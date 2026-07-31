#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich.py - Enrichissement MAXIMAL de la base AskData, avec reprise automatique.

Pour chaque entreprise de data/base_askdata.csv, ajoute 59 colonnes :

 1. FICHE OFFICIELLE (API Recherche d'entreprises, 1 appel par SIREN, en cache) :
    effectif + annee de la donnee, categorie (PME/ETI/GE), nature juridique,
    sigle, nom commercial / enseignes, date de creation, caractere employeur,
    nb d'etablissements (total, ouverts, communes des autres sites),
    dirigeants (personne PHYSIQUE en priorite + 4 autres, avec fonction),
    finances INPI (CA, CA n-1, resultat net, exercice), labels officiels
    (RGE, Qualiopi, ESS, societe a mission...), convention collective,
    adresse complete du siege, region, latitude/longitude, et deux drapeaux
    de conformite : entrepreneur individuel et diffusion protegee (INSEE).
 2. TVA INTRACOMMUNAUTAIRE : calcul local, formule officielle.
 3. SIGNAUX BODACC (option --bodacc) : annonces legales recentes + exclusion
    des societes en procedure collective.
 4. CONTACTS : site officiel (devine depuis le nom puis verifie, repli
    DuckDuckGo), scraping des pages contact / mentions legales : emails
    GENERIQUES (contact@, info@...), telephone, page LinkedIn ; puis
    verification MX du domaine. AUCUN email nominatif n'est devine.
 5. SCORE 0-100 et TIER A/B/C, avec le detail des raisons.
 6. ETAT administratif (les cessees passent en tier "exclu"), 14 LIENS prets
    a cliquer (site, Pappers, Annuaire des Entreprises + ses onglets annonces
    BODACC / finances / dirigeants, societe.com, Pages Jaunes, Google Maps,
    recherche Google, Google News, recherche LinkedIn entreprise ET dirigeant,
    Trustpilot) et drapeaux derives : a_site, email_perso, anciennete.

OPT-OUT RGPD (data/optout.csv) : a chaque execution, AVANT toute chose, les
lignes deja enrichies qui correspondent a un SIREN / domaine / email de la
liste sont PURGEES de la sortie ; et aucune ligne en opt-out n'est traitee.
Ajouter une ligne a data/optout.csv suffit : le run suivant nettoie tout.

REPRISE AUTOMATIQUE : chaque execution complete la sortie, les lignes deja
faites sont sautees. Relancez autant de fois que necessaire :

    python enrich.py --limit 500
    python enrich.py --limit 2000 --types saas_web comptabilite --departements 69 38
    python enrich.py --bodacc --limit 300
    python enrich.py --stats            # ou en suis-je ?

Optimal en LOCAL (meilleur taux d'emails). Fonctionne aussi entierement via
GitHub Actions (workflow enrich.yml) : phase fiche avec --sans-web, puis
phase contacts avec --completer-web, sauvegarde compressee sur la branche
"enrichi".
"""
import argparse
import csv
import gzip
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from urllib.parse import quote_plus

from common import (load_config, strip_acc, slugify, norm_phone, load_optout,
                    is_optout, ensure_optout_file, USER_AGENT, BASE_COLS)

try:
    import requests
except ImportError:
    requests = None
try:
    import dns.resolver
except ImportError:
    dns = None

API_URL = "https://recherche-entreprises.api.gouv.fr/search"
BODACC_URL = ("https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/"
              "catalog/datasets/annonces-commerciales/records")
DDG_URL = "https://html.duckduckgo.com/html/"

ENRICH_COLS = ["score", "tier", "raisons", "etat",
               "sigle", "nom_commercial",
               "effectif", "annee_effectif", "categorie", "nature_juridique",
               "date_creation", "anciennete",
               "employeur", "entrepreneur_individuel", "diffusion_protegee",
               "nb_etablissements", "nb_etablissements_ouverts", "communes_etablissements",
               "ca", "ca_prev", "resultat_net", "annee_finances",
               "adresse", "cp", "ville", "region", "latitude", "longitude", "tva_intra",
               "dirigeant", "qualite", "autres_dirigeants", "labels", "idcc",
               "domain", "a_site", "email", "email_perso", "email_source", "email_status",
               "autres_emails", "linkedin",
               "lien_site", "lien_linkedin_rech", "lien_linkedin_dirigeant",
               "lien_pappers", "lien_annuaire", "lien_annonces", "lien_finances",
               "lien_dirigeants", "lien_maps", "lien_google", "lien_google_news",
               "lien_societe", "lien_pagesjaunes", "lien_trustpilot",
               "signaux_bodacc", "date_enrichi", "web_fait"]
OUT_COLS = BASE_COLS + ENRICH_COLS

# Colonnes conservees quand une ligne passe en opt-out (le reste est vide) :
# juste de quoi identifier la ligne pour que purge_optout() la retire au
# prochain run. Aucune donnee de contact, aucun dirigeant, aucun lien.
OPTOUT_KEEP = {"siren", "siret", "nom", "type", "naf", "libelle", "commune",
               "departement", "source", "score", "tier", "raisons",
               "date_enrichi", "web_fait"}


def opencsv(path, mode):
    """Ouvre un CSV, compresse (.gz) ou non, en texte utf-8."""
    if path.endswith(".gz"):
        return gzip.open(path, mode + "t", encoding="utf-8", newline="")
    return open(path, mode, newline="", encoding="utf-8")

EFFECTIFS = {"00": "0", "01": "1-2", "02": "3-5", "03": "6-9", "11": "10-19",
             "12": "20-49", "21": "50-99", "22": "100-199", "31": "200-249",
             "32": "250-499", "41": "500-999", "42": "1000-1999",
             "51": "2000-4999", "52": "5000-9999", "53": "10000+"}
NATURES = {"1000": "Entrepreneur individuel", "5498": "EURL", "5499": "SARL",
           "5599": "SA", "5710": "SAS", "5720": "SASU",
           "9220": "Association declaree", "9230": "Association RUP"}
LABELS_MAP = {"est_rge": "RGE", "est_qualiopi": "Qualiopi",
              "est_organisme_formation": "Organisme de formation",
              "est_ess": "ESS", "est_bio": "Bio",
              "est_societe_mission": "Societe a mission",
              "est_service_public": "Service public", "est_finess": "FINESS"}
REGIONS = {"01": "Guadeloupe", "02": "Martinique", "03": "Guyane",
           "04": "La Reunion", "06": "Mayotte", "11": "Ile-de-France",
           "24": "Centre-Val de Loire", "27": "Bourgogne-Franche-Comte",
           "28": "Normandie", "32": "Hauts-de-France", "44": "Grand Est",
           "52": "Pays de la Loire", "53": "Bretagne", "75": "Nouvelle-Aquitaine",
           "76": "Occitanie", "84": "Auvergne-Rhone-Alpes",
           "93": "Provence-Alpes-Cote d'Azur", "94": "Corse"}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
CF_HEX = re.compile(r'data-cfemail="([0-9a-fA-F]{8,})"')
PHONE_RE = re.compile(r"(?<!\d)(?:\+33\s?|0033\s?|0)[1-9](?:[\s.\-]?\d{2}){4}(?!\d)")
LINKEDIN_RE = re.compile(r"https?://[a-z]{0,3}\.?linkedin\.com/company/[A-Za-z0-9_\-%.]+", re.I)
_AT = r'(?:\s*\[at\]\s*|\s*\(at\)\s*|\s+arobase\s+|@)'
_DOT = r'(?:\s*\[dot\]\s*|\s*\(dot\)\s*|\s+point\s+|\.)'
OBF_RE = re.compile(r'([a-z0-9._%+\-]+)' + _AT + r'([a-z0-9.\-]+)' + _DOT + r'([a-z]{2,})', re.I)
PLACEHOLDER = ("example", "domain", "votre", "your", "yourname", "sentry", "wix",
               "@2x", ".png", ".jpg", ".jpeg", ".gif", ".webp", "email@")
HREF_RE = re.compile(r'href="(https?://[^"]+)"')

_session = None
_api_cache, _cache_lock = {}, threading.Lock()
_write_lock = threading.Lock()


class RateLimiter:
    def __init__(self, min_interval):
        self.min_interval, self.lock, self.next_t = min_interval, threading.Lock(), 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if now < self.next_t:
                time.sleep(self.next_t - now)
                now = time.monotonic()
            self.next_t = now + self.min_interval


_limiter = RateLimiter(0.15)


def session():
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def get_json(url, params, timeout=25, retries=3):
    for attempt in range(retries):
        try:
            _limiter.wait()
            r = session().get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return {}


def http_get(url, timeout):
    try:
        r = session().get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and "html" in r.headers.get("content-type", "").lower():
            return r.text
    except Exception:
        pass
    return ""


# ----------------------------------------------------------------------------
#  1. Fiche officielle
# ----------------------------------------------------------------------------
def api_fetch(siren):
    """Fiche complete d'un SIREN (mise en cache, 1 seul appel par SIREN)."""
    with _cache_lock:
        if siren in _api_cache:
            return _api_cache[siren]
    res = None
    for q in (f"siren:{siren}", siren):     # syntaxe documentee, puis repli texte
        data = get_json(API_URL, {"q": q, "page": 1, "per_page": 3})
        for r in (data.get("results") or []):
            if r.get("siren") == siren:
                res = r
                break
        if res:
            break
    with _cache_lock:
        _api_cache[siren] = res
    return res


def parse_fiche(res):
    out = {c: "" for c in ("etat", "sigle", "nom_commercial",
                           "effectif", "annee_effectif", "categorie", "nature_juridique",
                           "date_creation", "employeur", "entrepreneur_individuel",
                           "diffusion_protegee",
                           "nb_etablissements", "nb_etablissements_ouverts",
                           "communes_etablissements",
                           "ca", "ca_prev", "resultat_net", "annee_finances",
                           "adresse", "cp", "ville", "region", "latitude", "longitude",
                           "dirigeant", "qualite", "autres_dirigeants",
                           "labels", "idcc", "nom", "naf", "libelle", "commune_maj")}
    if not res:
        return out
    etat = str(res.get("etat_administratif")
               or (res.get("siege") or {}).get("etat_administratif") or "").upper()
    out["etat"] = {"A": "active", "C": "cessee"}.get(etat, etat.lower())
    out["nom"] = res.get("nom_raison_sociale") or res.get("nom_complet") or ""
    out["sigle"] = res.get("sigle") or ""
    out["naf"] = res.get("activite_principale") or ""
    out["libelle"] = res.get("libelle_activite_principale") or ""
    out["effectif"] = EFFECTIFS.get(res.get("tranche_effectif_salarie") or "", "")
    out["annee_effectif"] = str(res.get("annee_tranche_effectif_salarie") or "")
    out["categorie"] = res.get("categorie_entreprise") or ""
    nj = str(res.get("nature_juridique") or "")
    out["nature_juridique"] = NATURES.get(nj, nj)
    out["date_creation"] = res.get("date_creation") or ""
    out["employeur"] = {"O": "oui", "N": "non"}.get(
        str(res.get("caractere_employeur") or "").upper(), "")
    out["diffusion_protegee"] = "oui" if str(res.get("statut_diffusion") or "").upper() == "P" else "non"
    out["nb_etablissements"] = str(res.get("nombre_etablissements") or "")
    out["nb_etablissements_ouverts"] = str(res.get("nombre_etablissements_ouverts") or "")
    # dirigeants : PERSONNE PHYSIQUE en priorite (pour personnaliser un email on
    # veut un humain, pas la holding), registre officiel public, jamais d'email devine
    pp, pm = [], []
    for d in (res.get("dirigeants") or []):
        if d.get("denomination"):
            pm.append((d["denomination"], d.get("qualite") or ""))
        else:
            nomd = (" ".join(filter(None, [d.get("prenoms"), d.get("nom")]))).strip()
            if nomd:
                pp.append((nomd, d.get("qualite") or ""))
    dirs = pp + pm
    if dirs:
        out["dirigeant"], out["qualite"] = dirs[0]
        out["autres_dirigeants"] = "; ".join(f"{n} ({q})" if q else n for n, q in dirs[1:5])
    # finances
    fin = res.get("finances") or {}
    years = sorted(fin.keys())
    if years:
        last = years[-1]
        out["annee_finances"] = last
        out["ca"] = str(fin[last].get("ca") or "")
        out["resultat_net"] = str(fin[last].get("resultat_net") or "")
        if len(years) > 1:
            out["ca_prev"] = str(fin[years[-2]].get("ca") or "")
    # labels + drapeaux de conformite
    comp = res.get("complements") or {}
    out["labels"] = ", ".join(v for k, v in LABELS_MAP.items() if comp.get(k))
    out["entrepreneur_individuel"] = "oui" if comp.get("est_entrepreneur_individuel") else "non"
    # siege
    siege = res.get("siege") or {}
    out["nom_commercial"] = (siege.get("nom_commercial")
                             or "; ".join(e for e in (siege.get("liste_enseignes") or []) if e))
    out["adresse"] = siege.get("adresse") or ""
    out["cp"] = siege.get("code_postal") or ""
    out["ville"] = siege.get("libelle_commune") or ""
    out["region"] = REGIONS.get(str(siege.get("region") or ""), "")
    out["commune_maj"] = f"{out['cp']} {out['ville']}".strip()
    out["latitude"] = str(siege.get("latitude") or "")
    out["longitude"] = str(siege.get("longitude") or "")
    idcc = siege.get("liste_idcc") or []
    out["idcc"] = ", ".join(str(i) for i in idcc) if isinstance(idcc, list) else str(idcc)
    # autres etablissements : empreinte geographique (multi-sites = pitch BI)
    vus, communes = {out["ville"].upper()}, []
    for e in (res.get("matching_etablissements") or []):
        c = (e.get("libelle_commune") or "").strip()
        if c and c.upper() not in vus:
            vus.add(c.upper())
            communes.append(c)
        if len(communes) >= 5:
            break
    out["communes_etablissements"] = "; ".join(communes)
    return out


def tva_intra(siren):
    """Numero de TVA intracommunautaire, formule officielle (calcul local, fiable)."""
    if not siren or len(siren) != 9 or not siren.isdigit():
        return ""
    key = (12 + 3 * (int(siren) % 97)) % 97
    return f"FR{key:02d}{siren}"


# ----------------------------------------------------------------------------
#  2. BODACC
# ----------------------------------------------------------------------------
def bodacc_signals(siren):
    """Annonces legales recentes. Renvoie (resume, distress)."""
    if not siren:
        return "", False
    data = get_json(BODACC_URL, {"where": f'registre LIKE "%{siren}%"',
                                 "order_by": "dateparution desc", "limit": 8,
                                 "select": "dateparution,familleavis_lib"})
    items, distress = [], False
    for rec in (data.get("results") or []):
        fam = rec.get("familleavis_lib") or ""
        dt = rec.get("dateparution") or ""
        if fam:
            items.append(f"{dt} {fam}".strip())
        if "collective" in strip_acc(fam) or "radiation" in strip_acc(fam):
            distress = True
    return "; ".join(items[:5]), distress


# ----------------------------------------------------------------------------
#  3. Contacts (site, emails generiques, telephone, LinkedIn)
# ----------------------------------------------------------------------------
def cf_decode(hexstr):
    try:
        b = bytes.fromhex(hexstr)
        return "".join(chr(c ^ b[0]) for c in b[1:])
    except Exception:
        return ""


def emails_from_html(html):
    found = set()
    for m in EMAIL_RE.findall(html or ""):
        found.add(m.lower())
    for a, b, c in OBF_RE.findall(html or ""):
        found.add(f"{a}@{b}.{c}".lower())
    for hx in CF_HEX.findall(html or ""):
        d = cf_decode(hx)
        if "@" in d:
            found.add(d.lower())
    return {e for e in found if not any(p in e for p in PLACEHOLDER) and 5 <= len(e) <= 60}


def phones_from_html(html):
    out = []
    for m in PHONE_RE.findall(html or ""):
        n = norm_phone(m)
        if n and n not in out:
            out.append(n)
    return out


def plausible(html, nom, ville=""):
    h = strip_acc(html)
    toks = [t for t in re.split(r"[^a-z0-9]+", strip_acc(nom)) if len(t) >= 4]
    if any(t in h for t in toks):
        return True
    v = strip_acc(ville)
    return bool(v) and v.split()[-1] in h if v else False


def ddg_candidates(nom, ville, cfg):
    html = http_get(DDG_URL + "?q=" + requests.utils.quote(f"{nom} {ville}".strip()),
                    cfg["timeout_http"])
    if not html:
        return []
    doms, seen = [], set()
    for url in HREF_RE.findall(html):
        m = re.search(r"https?://(?:www\.)?([^/\"]+)", url)
        if not m:
            continue
        d = m.group(1).lower()
        if d in seen or "duckduckgo" in d or any(x in d for x in cfg["domaines_ignores"]):
            continue
        seen.add(d)
        doms.append(d)
        if len(doms) >= 3:
            break
    return doms


def resolve_domain(nom, ville, cfg):
    stem = slugify(nom)
    cands = [f"{stem}.{t}" for t in cfg["tlds"]] if len(stem) >= 3 else []
    if cfg.get("duckduckgo_fallback", True):
        cands += [d for d in ddg_candidates(nom, ville, cfg) if d not in cands]
    for dom in cands:
        for scheme in ("https://", "http://"):
            html = http_get(scheme + dom, cfg["timeout_http"])
            if html and plausible(html, nom, ville):
                return dom, scheme + dom, html
    return "", "", ""


def pick_email(emails, domain, role_prefixes):
    same = [e for e in emails if e.split("@")[-1].endswith(domain)] or sorted(emails)
    for e in same:
        local = e.split("@")[0]
        if any(local == p or local.startswith(p) for p in role_prefixes):
            return e, "site-generique"
    return (same[0], "site") if same else ("", "")


def verify_mx(domain):
    if not domain:
        return ""
    if dns is None:
        return "unknown"
    try:
        return "mx_ok" if dns.resolver.resolve(domain, "MX", lifetime=4) else "no_mx"
    except Exception:
        return "no_mx"


def scrape_contacts(nom, ville, cfg):
    out = {"domain": "", "email": "", "email_source": "", "email_status": "",
           "autres_emails": "", "linkedin": "", "telephone": ""}
    domain, base, home = resolve_domain(nom, ville, cfg)
    if not domain:
        return out
    out["domain"] = domain
    emails, phones, linkedin = set(), [], ""
    for path in cfg["paths_a_scanner"]:
        html = home if path == "/" else http_get(base + path, cfg["timeout_http"])
        if not html:
            continue
        emails |= emails_from_html(html)
        phones += [p for p in phones_from_html(html) if p not in phones]
        if not linkedin:
            lk = LINKEDIN_RE.search(html)
            if lk:
                linkedin = lk.group(0)
    emails = {e for e in emails if not any(d in e.split("@")[-1] for d in cfg["domaines_ignores"])}
    email, src = pick_email(emails, domain, cfg["role_prefixes"])
    out["email"], out["email_source"] = email, src
    out["email_status"] = verify_mx(email.split("@")[-1]) if email else ""
    out["autres_emails"] = "; ".join(sorted(e for e in emails if e != email)[:5])
    out["linkedin"] = linkedin
    out["telephone"] = phones[0] if phones else ""
    return out


# ----------------------------------------------------------------------------
#  4. Score
# ----------------------------------------------------------------------------
def compute_score(row, sc, distress):
    pts, why = 0, []

    def add(p, label):
        nonlocal pts
        if p:
            pts += p
            why.append(f"{label}+{p}")

    if row.get("email_source") == "site-generique":
        add(sc["poids_email"], "email")
    elif row.get("email"):
        add(max(sc["poids_email"] - 8, 0), "email")
    elif row.get("domain"):
        add(sc["poids_site"], "site")
    if row.get("telephone"):
        add(sc["poids_telephone"], "tel")
    try:
        ca = float(row.get("ca") or 0)
        if sc["ca_min_ideal"] <= ca <= sc["ca_max_ideal"]:
            add(sc["poids_finances"], "ca")
        if ca and float(row.get("ca_prev") or 0) and ca > float(row["ca_prev"]):
            add(sc["poids_croissance"], "croissance")
    except ValueError:
        pass
    if row.get("effectif") and row["effectif"] not in ("0", "1-2"):
        add(sc["poids_effectif"], "effectif")
    if row.get("categorie") in ("PME", "ETI", "GE"):
        add(sc["poids_categorie"], "categorie")
    try:
        if int(row.get("nb_etablissements_ouverts") or 0) >= 2:
            add(sc.get("poids_multi_etab", 0), "multi-sites")
    except (ValueError, TypeError):
        pass
    dc = row.get("date_creation") or ""
    if len(dc) >= 4 and dc[:4].isdigit():
        age = date.today().year - int(dc[:4])
        if sc["age_min"] <= age <= sc["age_max"]:
            add(sc["poids_anciennete"], "anciennete")
    if row.get("dirigeant"):
        add(sc["poids_dirigeant"], "dirigeant")
    if row.get("labels"):
        add(sc["poids_labels"], "labels")
    cessee = (row.get("etat") == "cessee")
    if distress:
        pts = max(pts - sc["penalite_distress"], 0)
        why.append("PROCEDURE COLLECTIVE")
    if cessee:
        pts = 0
        why.append("ENTREPRISE CESSEE")
    score = min(pts, 100)
    if distress or cessee:
        tier = "exclu"
    elif score >= sc["seuil_tier_A"]:
        tier = "A"
    elif score >= sc["seuil_tier_B"]:
        tier = "B"
    else:
        tier = "C"
    return score, tier, "|".join(why)


# ----------------------------------------------------------------------------
#  Pipeline
# ----------------------------------------------------------------------------
def row_key(r):
    return r.get("siret") or r.get("siren") or f"{r.get('nom','')}|{r.get('commune','')}"


def fill_links(row):
    """14 liens prets a cliquer, construits localement (aucun appel reseau)."""
    siren = (row.get("siren") or "").strip()
    if len(siren) == 9 and siren.isdigit():
        row["lien_pappers"] = f"https://www.pappers.fr/entreprise/{siren}"
        row["lien_annuaire"] = f"https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"
        row["lien_annonces"] = f"https://annuaire-entreprises.data.gouv.fr/annonces/{siren}"
        row["lien_finances"] = f"https://annuaire-entreprises.data.gouv.fr/donnees-financieres/{siren}"
        row["lien_dirigeants"] = f"https://annuaire-entreprises.data.gouv.fr/dirigeants/{siren}"
        row["lien_societe"] = f"https://www.societe.com/cgi-bin/search?champs={siren}"
    lat, lon = (row.get("latitude") or "").strip(), (row.get("longitude") or "").strip()
    if lat and lon:
        row["lien_maps"] = f"https://www.google.com/maps?q={lat},{lon}"
    else:
        q = f"{row.get('nom', '')} {row.get('ville') or row.get('commune', '')}".strip()
        row["lien_maps"] = "https://www.google.com/maps/search/?api=1&query=" + quote_plus(q) if q else ""
    q2 = f"{row.get('nom', '')} {row.get('ville') or row.get('commune', '')}".strip()
    row["lien_google"] = "https://www.google.com/search?q=" + quote_plus(q2) if q2 else ""
    nom = (row.get("nom") or "").strip()
    ville = (row.get("ville") or row.get("commune") or "").strip()
    if nom:
        row["lien_google_news"] = ("https://news.google.com/search?hl=fr&q="
                                   + quote_plus(f'"{nom}"'))
        row["lien_linkedin_rech"] = ("https://www.linkedin.com/search/results/companies/?keywords="
                                     + quote_plus(nom))
        row["lien_pagesjaunes"] = ("https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui="
                                   + quote_plus(nom) + ("&ou=" + quote_plus(ville) if ville else ""))
    dirigeant = (row.get("dirigeant") or "").strip()
    if dirigeant and nom:
        row["lien_linkedin_dirigeant"] = ("https://www.linkedin.com/search/results/people/?keywords="
                                          + quote_plus(f"{dirigeant} {nom}"))
    dom = (row.get("domain") or "").strip()
    if dom:
        row["lien_site"] = f"https://{dom}"
        row["lien_trustpilot"] = f"https://fr.trustpilot.com/review/{dom}"


PERSO_DOMAINS = {"gmail.com", "hotmail.com", "hotmail.fr", "outlook.com", "outlook.fr",
                 "yahoo.com", "yahoo.fr", "orange.fr", "wanadoo.fr", "free.fr", "sfr.fr",
                 "laposte.net", "live.fr", "icloud.com", "aol.com", "bbox.fr", "neuf.fr", "gmx.fr"}


def derive_flags(row):
    """Colonnes derivees, calcul local : a_site, email_perso, anciennete."""
    row["a_site"] = "oui" if (row.get("domain") or "").strip() else "non"
    em = (row.get("email") or "").strip().lower()
    row["email_perso"] = ("oui" if em.split("@")[-1] in PERSO_DOMAINS else "non") if em else ""
    dc = row.get("date_creation") or ""
    row["anciennete"] = str(max(date.today().year - int(dc[:4]), 0)) if len(dc) >= 4 and dc[:4].isdigit() else ""


def apply_optout(row, opt):
    """Si la ligne correspond a l'opt-out : ne garde que l'identite (pour que
    purge_optout la retire au prochain run), efface contacts, dirigeants et
    liens, marque exclu. Renvoie True si la ligne a ete traitee en opt-out."""
    if not is_optout(opt, *(row.get(c, "") for c in OUT_COLS)):
        return False
    for c in OUT_COLS:
        if c not in OPTOUT_KEEP:
            row[c] = ""
    row["telephone"] = ""
    row["score"], row["tier"] = 0, "exclu"
    row["raisons"] = "OPT-OUT RGPD (data/optout.csv)"
    return True


def migrate_out(path):
    """Si la sortie existante a un ancien schema, la reecrit avec les colonnes actuelles."""
    if not os.path.exists(path):
        return
    with opencsv(path, "r") as f:
        try:
            header = next(csv.reader(f))
        except StopIteration:
            return
    if header == OUT_COLS:
        return
    with opencsv(path, "r") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        for c in OUT_COLS:
            row.setdefault(c, "")
        fill_links(row)
        derive_flags(row)
    tmp = (path[:-3] + ".tmp.gz") if path.endswith(".gz") else path + ".tmp"
    with opencsv(tmp, "w") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)
    print(f"MIGRATION: {path} passe de {len(header)} a {len(OUT_COLS)} colonnes "
          f"({len(rows)} lignes conservees, liens recalcules)", file=sys.stderr)


def purge_optout(path, opt):
    """Retire de la sortie TOUTES les lignes correspondant a l'opt-out.
    Tourne a CHAQUE execution : ajouter une ligne a data/optout.csv suffit,
    le run suivant nettoie retroactivement la base enrichie."""
    if not os.path.exists(path) or not (opt["siren"] or opt["domain"] or opt["email"]):
        return
    with opencsv(path, "r") as f:
        rows = list(csv.DictReader(f))
    kept = [r for r in rows if not is_optout(opt, *r.values())]
    if len(kept) == len(rows):
        return
    tmp = (path[:-3] + ".tmp.gz") if path.endswith(".gz") else path + ".tmp"
    with opencsv(tmp, "w") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(kept)
    os.replace(tmp, path)
    print(f"OPT-OUT: {len(rows) - len(kept)} ligne(s) purgee(s) de {path}", file=sys.stderr)


def enrich_row(base_row, cfg, sc, use_bodacc, use_web, opt):
    row = {c: base_row.get(c, "") for c in BASE_COLS}
    row.update({c: "" for c in ENRICH_COLS})
    siren = (row.get("siren") or "").strip()
    fiche = parse_fiche(api_fetch(siren)) if siren else {}
    if fiche:
        for c in ("etat", "sigle", "nom_commercial",
                  "effectif", "annee_effectif", "categorie", "nature_juridique",
                  "date_creation", "employeur", "entrepreneur_individuel",
                  "diffusion_protegee",
                  "nb_etablissements", "nb_etablissements_ouverts", "communes_etablissements",
                  "ca", "ca_prev", "resultat_net", "annee_finances",
                  "adresse", "cp", "ville", "region", "latitude", "longitude",
                  "dirigeant", "qualite", "autres_dirigeants", "labels", "idcc"):
            row[c] = fiche.get(c, "")
        for c in ("nom", "naf", "libelle"):        # complete la base si vide
            if not row.get(c) and fiche.get(c):
                row[c] = fiche[c]
        if not row.get("commune") and fiche.get("commune_maj"):
            row["commune"] = fiche["commune_maj"]
    row["tva_intra"] = tva_intra(siren)
    distress = False
    if use_bodacc and siren:
        row["signaux_bodacc"], distress = bodacc_signals(siren)
    if use_web:
        contacts = scrape_contacts(row.get("nom", ""), row.get("ville") or row.get("commune", ""), cfg)
        for c in ("domain", "email", "email_source", "email_status", "autres_emails", "linkedin"):
            row[c] = contacts[c]
        if contacts["telephone"] and not row.get("telephone"):
            row["telephone"] = contacts["telephone"]
    fill_links(row)
    derive_flags(row)
    row["score"], row["tier"], row["raisons"] = compute_score(row, sc, distress)
    row["date_enrichi"] = date.today().isoformat()
    row["web_fait"] = "oui" if use_web else "non"
    # opt-out : attrape aussi un domaine/email exclu decouvert PENDANT le scraping
    apply_optout(row, opt)
    return row


def load_done(path):
    if not os.path.exists(path):
        return set()
    with opencsv(path, "r") as f:
        return {row_key(r) for r in csv.DictReader(f)}


def completer_web(a, cfg, sc, opt):
    """Complete le web (contacts) des lignes deja fichees (web_fait=non), sur place."""
    if not os.path.exists(a.out):
        print("Rien a completer: la sortie n'existe pas encore.", file=sys.stderr)
        return
    with opencsv(a.out, "r") as f:
        rows = list(csv.DictReader(f))
    idxs = []
    for i, r in enumerate(rows):
        if r.get("web_fait") == "oui" and not (a.retenter_vides and not (r.get("email") or "").strip()):
            continue
        if r.get("tier") == "exclu":
            continue
        if a.types and r.get("type") not in a.types:
            continue
        if a.departements and r.get("departement") not in a.departements:
            continue
        idxs.append(i)
        if len(idxs) >= a.limit:
            break
    if not idxs:
        print("Rien a completer (web deja fait pour ces filtres).", file=sys.stderr)
        return
    print(f"{len(idxs)} lignes a completer (web) sur {len(rows)}", file=sys.stderr)

    def work(i):
        r = rows[i]
        c = scrape_contacts(r.get("nom", ""), r.get("ville") or r.get("commune", ""), cfg)
        for k in ("domain", "email", "email_source", "email_status", "autres_emails", "linkedin"):
            r[k] = c[k]
        if c["telephone"] and not r.get("telephone"):
            r["telephone"] = c["telephone"]
        fill_links(r)
        derive_flags(r)
        r["score"], r["tier"], r["raisons"] = compute_score(r, sc, r.get("tier") == "exclu")
        r["date_enrichi"] = date.today().isoformat()
        r["web_fait"] = "oui"
        apply_optout(r, opt)

    done_n = 0
    with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
        for _ in as_completed([ex.submit(work, i) for i in idxs]):
            done_n += 1
            if done_n % 20 == 0:
                print(f"  {done_n}/{len(idxs)}", file=sys.stderr)
    tmp = (a.out[:-3] + ".tmp.gz") if a.out.endswith(".gz") else a.out + ".tmp"
    with opencsv(tmp, "w") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, a.out)
    n_mail = sum(1 for r in rows if r.get("email"))
    print(f"COMPLETER-WEB: +{done_n} lignes completees -> {a.out} (emails totaux: {n_mail}). "
          f"Relancez pour continuer.", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="Enrichissement maximal de la base AskData (reprise auto).")
    ap.add_argument("--in", dest="inp", default=None,
                    help="Base a enrichir (defaut: data/base_askdata.csv sinon base_askdata.csv).")
    ap.add_argument("--out", default="data/base_askdata_enrichi.csv")
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--limit", type=int, default=500, help="Lignes traitees par execution (defaut 500).")
    ap.add_argument("--types", nargs="+", help="Filtrer: saas_web conseil_it comptabilite ...")
    ap.add_argument("--departements", nargs="+")
    ap.add_argument("--bodacc", action="store_true", help="Ajouter les signaux BODACC (plus lent).")
    ap.add_argument("--sans-web", action="store_true", help="Sauter le scraping (fiche officielle seule, rapide).")
    ap.add_argument("--completer-web", action="store_true",
                    help="Completer le web (contacts) des lignes deja fichees avec --sans-web.")
    ap.add_argument("--retenter-vides", action="store_true",
                    help="Avec --completer-web : retente aussi les lignes deja faites mais sans email trouve.")
    ap.add_argument("--workers", type=int)
    ap.add_argument("--stats", action="store_true", help="Afficher l'avancement et sortir.")
    a = ap.parse_args()
    if requests is None:
        sys.exit("Le module 'requests' est requis (pip install -r requirements.txt).")

    inp = a.inp or ("data/base_askdata.csv" if os.path.exists("data/base_askdata.csv") else "base_askdata.csv")
    conf = load_config(a.config)
    cfg, sc = conf["enrichissement"], conf["scoring"]
    if a.workers:
        cfg["workers"] = a.workers

    # opt-out d'abord : purge retroactive de la sortie, puis filtre des entrants
    ensure_optout_file()
    opt = load_optout()
    migrate_out(a.out)
    purge_optout(a.out, opt)

    if a.completer_web:
        completer_web(a, cfg, sc, opt)
        return

    with open(inp, encoding="utf-8") as f:
        base = list(csv.DictReader(f))
    done = load_done(a.out)
    if a.stats:
        web = 0
        if os.path.exists(a.out):
            with opencsv(a.out, "r") as f:
                web = sum(1 for r in csv.DictReader(f) if r.get("web_fait") == "oui")
        print(f"{len(done)} / {len(base)} lignes enrichies dans {a.out} (dont web fait: {web})", file=sys.stderr)
        return
    pending, seen = [], set(done)
    for r in base:
        if a.types and r.get("type") not in a.types:
            continue
        if a.departements and r.get("departement") not in a.departements:
            continue
        if is_optout(opt, r.get("siren"), r.get("nom"), r.get("telephone")):
            continue
        k = row_key(r)
        if k in seen:
            continue
        seen.add(k)
        pending.append(r)
    pending = pending[: a.limit]
    if not pending:
        print("Rien a faire (tout est deja enrichi pour ces filtres).", file=sys.stderr)
        return
    print(f"{len(done)} deja faites | {len(pending)} a enrichir ce run "
          f"(web={'non' if a.sans_web else 'oui'}, bodacc={'oui' if a.bodacc else 'non'})", file=sys.stderr)

    new_file = not os.path.exists(a.out)
    d = os.path.dirname(a.out)
    if d:
        os.makedirs(d, exist_ok=True)
    with opencsv(a.out, "a") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore", restval="")
        if new_file:
            w.writeheader()
        done_n = 0
        with ThreadPoolExecutor(max_workers=cfg["workers"]) as ex:
            futs = [ex.submit(enrich_row, r, cfg, sc, a.bodacc, not a.sans_web, opt) for r in pending]
            for fut in as_completed(futs):
                row = fut.result()
                with _write_lock:
                    w.writerow(row)
                    f.flush()
                done_n += 1
                if done_n % 20 == 0:
                    print(f"  {done_n}/{len(pending)}", file=sys.stderr)
    with opencsv(a.out, "r") as f:
        n_tot = sum(1 for _ in f) - 1
    print(f"ENRICH: +{done_n} lignes ce run -> {a.out} ({n_tot} au total). "
          f"Relancez la meme commande pour continuer.", file=sys.stderr)


if __name__ == "__main__":
    main()
