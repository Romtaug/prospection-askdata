#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AskData - Pipeline de prospection intelligent
=============================================
Construit une base de prospects B2B qualifiés à partir de sources 100% officielles :

  1. SIRENE (API Recherche d'entreprises, data.gouv)  -> ciblage NAF / région / effectif
  2. BODACC (API DILA)                                -> signaux (dépôt comptes, modifs, création)
                                                         + exclusion des sociétés en difficulté
  3. Enrichissement email (cascade)                   -> site + Hunter/Dropcontact + devinette
  4. Scoring 0-100                                    -> priorisation + tier A/B/C
  5. Sortie                                           -> CSV + JSON + résumé Markdown, + push Brevo (option)

Robustesse : le nom du paramètre NAF de l'API est auto-détecté (activite_principale
ou code_naf). Tout fonctionne sans clé payante. Les clés Hunter / Dropcontact / Brevo
sont facultatives (variables d'environnement) et améliorent la récupération d'emails.

RGPD : cible B2B uniquement (intérêt légitime, régime opt-out). Chaque ligne trace la source.
Les SIREN et emails présents dans data/suppression.csv ne ressortent jamais (respect opt-out).
Un email seulement "deviné" n'est jamais poussé vers Brevo (protection de la réputation).

Usage :
    python prospect.py                 # run complet (config.yml)
    python prospect.py --limit 150     # limite le nombre de candidats enrichis
    python prospect.py --no-enrich     # ciblage + scoring seuls (rapide, sans email)
    python prospect.py --no-bodacc     # sans les signaux BODACC
    python prospect.py --push          # pousse les prospects A/B (email fiable) vers Brevo
"""

import os
import re
import csv
import json
import time
import argparse
import unicodedata
import html
import datetime
from urllib.parse import urljoin

import requests
import yaml

try:
    import dns.resolver
    HAS_DNS = True
except Exception:
    HAS_DNS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False

# --------------------------------------------------------------------------- #
#  Endpoints et constantes
# --------------------------------------------------------------------------- #
SIRENE_URL = "https://recherche-entreprises.api.gouv.fr/search"
BODACC_URL = ("https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/"
              "catalog/datasets/annonces-commerciales/records")
HUNTER_BASE = "https://api.hunter.io/v2"
DROPCONTACT = "https://api.dropcontact.io/batch"
BREVO_URL = "https://api.brevo.com/v3/contacts"

USER_AGENT = ("AskData-Prospect/1.1 (+https://askdata-bi.netlify.app; "
              "contact romtaug+askdata@gmail.com)")

# Clés API facultatives (secrets GitHub / variables d'environnement)
HUNTER_KEY = os.getenv("HUNTER_API_KEY", "").strip()
DROPCONTACT_KEY = os.getenv("DROPCONTACT_API_KEY", "").strip()
BREVO_KEY = os.getenv("BREVO_API_KEY", "").strip()

# Nom du paramètre NAF de l'API (auto-détecté au démarrage)
NAF_PARAM = "activite_principale"

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
ROLE_PREFIXES = ("contact", "info", "hello", "bonjour", "commercial",
                 "direction", "compta", "rh", "sav", "accueil")
LEGAL_SUFFIXES = (" sas", " sasu", " sarl", " eurl", " sa ", " sci",
                  " scop", " snc", " selarl", " ei ", " eirl")
# sources d'email "fiables" (vs devinées)
SOURCES_FIABLES = ("site", "hunter", "dropcontact")
STATUTS_OK = ("valid", "accept_all", "webmail", "mx_ok")

EFFECTIF_LABELS = {
    "NN": "non employeur", "00": "0 salarié", "01": "1-2", "02": "3-5",
    "03": "6-9", "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
    "31": "200-249", "32": "250-499", "41": "500-999", "42": "1000-1999",
    "51": "2000-4999", "52": "5000-9999", "53": "10000+",
}
EFFECTIF_MAX = {"NN": 0, "00": 0, "01": 2, "02": 5, "03": 9, "11": 19, "12": 49,
                "21": 99, "22": 199, "31": 249, "32": 499, "41": 999, "42": 1999,
                "51": 4999, "52": 9999, "53": 20000}

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# --------------------------------------------------------------------------- #
#  Utilitaires
# --------------------------------------------------------------------------- #
def log(msg):
    print(f"[{datetime.datetime.now():%H:%M:%S}] {msg}", flush=True)


def strip_acc(s):
    return (unicodedata.normalize("NFKD", s or "")
            .encode("ascii", "ignore").decode().lower())


def slugify(name):
    """Nom d'entreprise -> base de domaine plausible (sans accent ni forme juridique)."""
    n = " " + name.lower() + " "
    for suf in LEGAL_SUFFIXES:
        n = n.replace(suf, " ")
    n = strip_acc(n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def get_json(url, params=None, headers=None, timeout=15, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429:
                time.sleep(2 + attempt * 3)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                log(f"  ! requête échouée ({url.split('/')[2]}): {e}")
                return None
            time.sleep(1 + attempt)
    return None


# --------------------------------------------------------------------------- #
#  1. SIRENE - ciblage des entreprises
# --------------------------------------------------------------------------- #
def detect_naf_param(sample_naf):
    """Certaines versions de l'API attendent 'activite_principale', d'autres 'code_naf'.
    On teste avec un seul code NAF (sans autre filtre) et on garde celui qui répond."""
    global NAF_PARAM
    for pname in ("activite_principale", "code_naf"):
        d = get_json(SIRENE_URL, params={pname: sample_naf, "per_page": 1, "page": 1})
        if d and d.get("results"):
            NAF_PARAM = pname
            log(f"  paramètre NAF détecté : {pname}")
            return pname
    log("  paramètre NAF non confirmé, on garde 'activite_principale'")
    return NAF_PARAM


def sirene_search(cfg):
    t = cfg["targeting"]
    if t.get("naf_codes"):
        detect_naf_param(t["naf_codes"][0])
    companies = {}
    for naf in t["naf_codes"]:
        got, page = 0, 1
        while got < t["max_par_naf"]:
            params = {NAF_PARAM: naf, "etat_administratif": "A",
                      "per_page": 20, "page": page}
            if t.get("departements"):
                params["departement"] = t["departements"]
            elif t.get("region"):
                params["region"] = t["region"]
            if t.get("tranche_effectif"):
                params["tranche_effectif_salarie"] = ",".join(t["tranche_effectif"])
            if t.get("categorie_entreprise"):
                params["categorie_entreprise"] = t["categorie_entreprise"]

            data = get_json(SIRENE_URL, params=params)
            if not data or not data.get("results"):
                break
            for r in data["results"]:
                c = normalize_company(r, naf)
                if c and c["siren"] not in companies:
                    companies[c["siren"]] = c
                    got += 1
            if page >= (data.get("total_pages") or 1):
                break
            page += 1
            time.sleep(0.15)
        log(f"  NAF {naf}: {got} entreprises")
    return list(companies.values())


def normalize_company(r, naf_query):
    siren = r.get("siren")
    if not siren:
        return None
    siege = r.get("siege") or {}
    fin = r.get("finances") or {}
    years = sorted([y for y in fin.keys() if str(y).isdigit()], reverse=True)
    ca = (fin.get(years[0]) or {}).get("ca") if years else None
    ca_prev = (fin.get(years[1]) or {}).get("ca") if len(years) > 1 else None

    dpre = dnom = dqual = ""
    for d in (r.get("dirigeants") or []):
        typ = (d.get("type_dirigeant") or "").lower()
        if typ.startswith("personne physique") or d.get("nom"):
            dpre = (d.get("prenoms") or d.get("prenom") or "").split(",")[0].strip().title()
            dnom = (d.get("nom") or "").strip().title()
            dqual = (d.get("qualite") or "").strip()
            if dnom:
                break

    return {
        "siren": siren,
        "nom": r.get("nom_complet") or r.get("nom_raison_sociale") or "",
        "raison_sociale": r.get("nom_raison_sociale") or r.get("nom_complet") or "",
        "naf": r.get("activite_principale") or naf_query,
        "naf_label": (r.get("libelle_activite_principale")
                      or siege.get("libelle_activite_principale") or ""),
        "effectif_code": r.get("tranche_effectif_salarie") or "",
        "categorie": r.get("categorie_entreprise") or "",
        "date_creation": r.get("date_creation") or "",
        "ca": ca, "ca_prev": ca_prev,
        "ville": siege.get("libelle_commune") or "",
        "cp": siege.get("code_postal") or "",
        "dir_prenom": dpre, "dir_nom": dnom, "dir_qualite": dqual,
        # remplis plus tard
        "domain": "", "emails": [], "email_sources": {},
        "best_email": "", "best_source": "", "email_status": "",
        "bodacc": {}, "score": 0, "tier": "", "exclu": False, "raisons": [],
    }


# --------------------------------------------------------------------------- #
#  2. BODACC - signaux et exclusion des sociétés en difficulté
# --------------------------------------------------------------------------- #
def bodacc_signals(siren, delay=0.25):
    # Le SIREN apparaît parfois avec espaces dans le texte : on cherche les deux formes.
    spaced = f"{siren[:3]} {siren[3:6]} {siren[6:]}"
    params = {"where": f'"{siren}" OR "{spaced}"', "limit": 100}
    data = get_json(BODACC_URL, params=params, timeout=20)
    time.sleep(delay)
    sig = {"distress": False, "distress_label": "", "depot_comptes": None,
           "modif_recente": None, "creation": None, "n_events": 0}
    if not data:
        return sig
    results = data.get("results") or []
    sig["n_events"] = len(results)
    today = datetime.date.today()

    def recent(dstr, months=24):
        try:
            d = datetime.date.fromisoformat((dstr or "")[:10])
            return (today - d).days <= months * 31
        except Exception:
            return False

    for rec in results:
        dp = rec.get("dateparution") or ""
        famille = strip_acc(rec.get("familleavis_lib") or rec.get("familleavis") or "")
        typeavis = strip_acc(rec.get("typeavis_lib") or rec.get("typeavis") or "")
        jug = rec.get("jugement")
        blob = strip_acc(f"{famille} {typeavis} "
                         + (json.dumps(jug, ensure_ascii=False) if jug else ""))

        # Société en difficulté -> exclusion. Toute décision de justice (jugement)
        # ou mot-clé de procédure collective déclenche l'exclusion.
        distress_kw = ("liquidation", "redressement", "sauvegarde",
                       "procedure collective", "cessation", "insuffisance d actif")
        if jug or any(k in blob for k in distress_kw):
            sig["distress"] = True
            sig["distress_label"] = (extract_nature(jug) or famille
                                     or typeavis or "procédure collective")[:90]
        # Dépôt des comptes annuels (signe d'une PME structurée)
        if rec.get("depot") or "depot" in famille or "compte" in famille:
            if not sig["depot_comptes"] or dp > (sig["depot_comptes"] or ""):
                sig["depot_comptes"] = dp
        # Modification (capital, dirigeant, adresse...) = activité récente
        if rec.get("modificationsgenerales") or "modif" in famille:
            if recent(dp) and (not sig["modif_recente"] or dp > sig["modif_recente"]):
                sig["modif_recente"] = dp
        # Création / immatriculation
        if "creation" in famille or "immatriculation" in famille:
            sig["creation"] = dp
    return sig


def extract_nature(jug):
    if not jug:
        return ""
    if isinstance(jug, str):
        try:
            jug = json.loads(jug)
        except Exception:
            return jug[:90]
    if isinstance(jug, dict):
        return jug.get("nature") or jug.get("famille") or ""
    return ""


# --------------------------------------------------------------------------- #
#  3. Enrichissement email (site -> Hunter -> Dropcontact -> devinette)
# --------------------------------------------------------------------------- #
def resolve_domain(company, cfg):
    if not cfg["enrichissement"].get("deviner_domaine", True):
        return ""
    base = slugify(company["raison_sociale"] or company["nom"])
    if not base:
        return ""
    words = base.split()
    stems = {"".join(words), "-".join(words)}
    if words and len(words[0]) >= 6:          # premier mot seul, seulement si assez distinctif
        stems.add(words[0])
    tlds = cfg["enrichissement"].get("tlds", ["fr", "com"])
    for stem in stems:
        if len(stem) < 4:
            continue
        for tld in tlds:
            dom = f"{stem}.{tld}"
            if domain_matches(dom, company, cfg):
                return dom
    return ""


def domain_matches(domain, company, cfg):
    """Le domaine résout ET la page parle bien de l'entreprise (nom ou SIREN présent)."""
    if HAS_DNS:
        ok = False
        for rtype in ("A", "MX"):
            try:
                if dns.resolver.resolve(domain, rtype):
                    ok = True
                    break
            except Exception:
                continue
        if not ok:
            return False
    for scheme in ("https://", "http://"):
        try:
            r = session.get(scheme + domain,
                            timeout=cfg["enrichissement"]["timeout_http"],
                            allow_redirects=True)
        except Exception:
            continue
        if r.status_code >= 400 or len(r.text) < 200:
            continue
        txt = strip_acc(r.text)
        keys = [w for w in slugify(company["raison_sociale"] or company["nom"]).split()
                if len(w) > 3]
        if keys and any(k in txt for k in keys):
            return True
        if company["siren"] in re.sub(r"\D", "", r.text):
            return True
        return False
    return False


CF_HEX = re.compile(r'data-cfemail="([0-9a-fA-F]{8,})"')
CF_HEX2 = re.compile(r'/cdn-cgi/l/email-protection#([0-9a-fA-F]{8,})')
# emails obfusqués : "nom [at] domaine [dot] fr", "nom arobase domaine point fr"
_AT = r'(?:\s*\[at\]\s*|\s*\(at\)\s*|\s*\{at\}\s*|\s+arobase\s+|@)'
_DOT = r'(?:\s*\[dot\]\s*|\s*\(dot\)\s*|\s*\{dot\}\s*|\s+point\s+|\.)'
OBF_RE = re.compile(r'([a-z0-9._%+\-]+)' + _AT + r'([a-z0-9.\-]+)' + _DOT + r'([a-z]{2,})', re.I)


def cf_decode(hexstr):
    """Décode un email protégé par Cloudflare (XOR avec le premier octet comme clé)."""
    try:
        key = int(hexstr[:2], 16)
        return "".join(chr(int(hexstr[i:i + 2], 16) ^ key)
                       for i in range(2, len(hexstr), 2))
    except Exception:
        return ""


def scrape_emails(domain, cfg):
    found = set()
    for path in cfg["enrichissement"]["paths_a_scanner"]:
        for scheme in ("https://", "http://"):
            url = urljoin(scheme + domain, path)
            try:
                r = session.get(url, timeout=cfg["enrichissement"]["timeout_http"],
                                allow_redirects=True)
            except Exception:
                continue
            if r.status_code >= 400:
                continue
            raw = r.text
            text = html.unescape(raw)          # décode &#64; &commat; etc.
            # 1) emails en clair
            for m in EMAIL_RE.findall(text):
                found.add(m.lower())
            # 2) emails protégés par Cloudflare
            for hx in CF_HEX.findall(raw) + CF_HEX2.findall(raw):
                dec = cf_decode(hx)
                if EMAIL_RE.fullmatch(dec):
                    found.add(dec.lower())
            # 3) emails obfusqués ([at] / [dot] / arobase / point)
            for g1, g2, g3 in OBF_RE.findall(text):
                cand = f"{g1}@{g2}.{g3}".lower()
                if EMAIL_RE.fullmatch(cand):
                    found.add(cand)
            # 4) liens mailto
            if HAS_BS4:
                try:
                    soup = BeautifulSoup(raw, "html.parser")
                    for a in soup.select('a[href^="mailto"]'):
                        addr = a.get("href", "").replace("mailto:", "").split("?")[0].strip().lower()
                        if EMAIL_RE.fullmatch(addr):
                            found.add(addr)
                except Exception:
                    pass
            break
    return [e for e in found
            if not e.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js"))
            and "example" not in e and "sentry" not in e
            and "wixpress" not in e and "@2x" not in e]


def hunter_emails(domain, prenom, nom):
    out = []
    if not HUNTER_KEY or not domain:
        return out
    d = get_json(f"{HUNTER_BASE}/domain-search",
                 params={"domain": domain, "api_key": HUNTER_KEY, "limit": 10})
    if d and d.get("data"):
        for e in d["data"].get("emails", []):
            if e.get("value"):
                out.append(e["value"].lower())
    if prenom and nom:
        d2 = get_json(f"{HUNTER_BASE}/email-finder",
                      params={"domain": domain, "first_name": prenom,
                              "last_name": nom, "api_key": HUNTER_KEY})
        if d2 and d2.get("data") and d2["data"].get("email"):
            out.insert(0, d2["data"]["email"].lower())
    return out


def dropcontact_email(company):
    if not DROPCONTACT_KEY:
        return []
    payload = {"data": [{"company": company["raison_sociale"] or company["nom"],
                         "first_name": company["dir_prenom"],
                         "last_name": company["dir_nom"],
                         "website": company["domain"]}],
               "siren": True, "language": "fr"}
    try:
        r = session.post(DROPCONTACT, json=payload,
                         headers={"X-Access-Token": DROPCONTACT_KEY,
                                  "Content-Type": "application/json"}, timeout=20)
        rid = r.json().get("request_id")
    except Exception:
        return []
    if not rid:
        return []
    for _ in range(8):
        time.sleep(4)
        res = get_json(f"{DROPCONTACT}/{rid}",
                       headers={"X-Access-Token": DROPCONTACT_KEY})
        if res and res.get("success") and res.get("data"):
            emails = []
            for row in res["data"]:
                for e in (row.get("email") or []):
                    if e.get("email"):
                        emails.append(e["email"].lower())
            return emails
    return []


def guess_dirigeant_email(company):
    dom = company["domain"]
    p = re.sub(r"[^a-z]", "", strip_acc(company["dir_prenom"]))
    n = re.sub(r"[^a-z]", "", strip_acc(company["dir_nom"]))
    if not dom or not n:
        return []
    cands = []
    if p and n:
        cands += [f"{p}.{n}@{dom}", f"{p[0]}.{n}@{dom}", f"{p}{n}@{dom}"]
    cands.append(f"{n}@{dom}")
    return cands


def rank_emails(sources, domain, dir_nom):
    """Trie par fiabilité de source (fiable avant deviné, garanti) puis pertinence.
    sources : {email: source}."""
    dn = strip_acc(dir_nom)

    def bonus(e):
        local = e.split("@")[0]
        s = 0
        if dn and dn in strip_acc(local):
            s += 100
        if any(local.startswith(pp) for pp in ROLE_PREFIXES):
            s += 40
        if domain and e.endswith("@" + domain):
            s += 20
        if "." in local:
            s += 5
        if sources.get(e, "") in ("hunter", "dropcontact"):
            s += 3      # légère préférence à l'intérieur des sources fiables
        return s

    def tier(e):
        return 0 if sources.get(e, "") in SOURCES_FIABLES else 1

    # (tier fiable=0 avant deviné=1), puis meilleur bonus d'abord
    return sorted(sources.keys(), key=lambda e: (tier(e), -bonus(e)))


def verify_email(email, source=""):
    if not email or not EMAIL_RE.fullmatch(email):
        return "invalide"
    domain = email.split("@")[1]
    if HUNTER_KEY:
        d = get_json(f"{HUNTER_BASE}/email-verifier",
                     params={"email": email, "api_key": HUNTER_KEY})
        if d and d.get("data") and d["data"].get("status"):
            return d["data"]["status"]
    if HAS_DNS:
        try:
            if dns.resolver.resolve(domain, "MX"):
                return "devine_mx" if source == "devine" else "mx_ok"
        except Exception:
            return "sans_mx"
    return "inconnu"


def enrich(company, cfg):
    company["domain"] = resolve_domain(company, cfg)
    sources = {}  # email -> source (première source vue conservée)

    def add(emails, src):
        for e in emails:
            e = (e or "").lower().strip()
            if e and EMAIL_RE.fullmatch(e) and e not in sources:
                sources[e] = src

    if company["domain"] and cfg["enrichissement"].get("scrape_site", True):
        add(scrape_emails(company["domain"], cfg), "site")
    add(hunter_emails(company["domain"], company["dir_prenom"], company["dir_nom"]), "hunter")
    add(dropcontact_email(company), "dropcontact")
    if cfg["enrichissement"].get("deviner_email_dirigeant", True):
        add(guess_dirigeant_email(company), "devine")

    ranked = rank_emails(sources, company["domain"], company["dir_nom"])
    company["emails"] = ranked
    company["email_sources"] = sources
    company["best_email"] = ranked[0] if ranked else ""
    company["best_source"] = sources.get(company["best_email"], "")
    company["email_status"] = (verify_email(company["best_email"], company["best_source"])
                               if company["best_email"] else "aucun")
    return company


# --------------------------------------------------------------------------- #
#  4. Scoring
# --------------------------------------------------------------------------- #
def score_company(c, cfg):
    w = cfg["scoring"]
    reasons = []

    # Exclusion dure : société en difficulté
    if c["bodacc"].get("distress"):
        c.update(exclu=True, score=0, tier="EXCLU",
                 raisons=[f"EXCLU: {c['bodacc'].get('distress_label', 'procédure collective')}"])
        return c

    score = 0.0
    # Secteur (NAF ciblé = plein score)
    score += w["poids_secteur"]
    reasons.append(f"secteur +{w['poids_secteur']}")

    # Taille
    emax = EFFECTIF_MAX.get(c["effectif_code"])
    if emax is None:
        score += w["poids_taille"] * 0.4
        reasons.append("taille inconnue")
    elif 3 <= emax <= 49:
        score += w["poids_taille"]
        reasons.append("taille idéale")
    elif emax <= 2:
        score += w["poids_taille"] * 0.3
        reasons.append("très petite")
    elif emax <= 99:
        score += w["poids_taille"] * 0.6
        reasons.append("un peu grande")
    else:
        score += w["poids_taille"] * 0.2
        reasons.append("grande")

    # Chiffre d'affaires
    ca = c["ca"]
    if ca is None:
        score += w["poids_ca"] * 0.5
        reasons.append("CA inconnu")
    elif w["ca_min_ideal"] <= ca <= w["ca_max_ideal"]:
        score += w["poids_ca"]
        reasons.append("CA idéal")
    elif ca < w["ca_min_ideal"]:
        score += w["poids_ca"] * 0.3
        reasons.append("CA faible")
    else:
        score += w["poids_ca"] * 0.5
        reasons.append("CA élevé")

    # Croissance
    if ca and c["ca_prev"]:
        if ca > c["ca_prev"] * 1.05:
            score += w["poids_croissance"]
            reasons.append("en croissance")
        elif ca < c["ca_prev"] * 0.95:
            reasons.append("en baisse")
        else:
            score += w["poids_croissance"] * 0.5

    # Activité récente (BODACC)
    if c["bodacc"].get("modif_recente"):
        score += w["poids_activite_bodacc"]
        reasons.append("activité récente (BODACC)")

    # Bonus : dépose ses comptes
    if c["bodacc"].get("depot_comptes"):
        score += w["bonus_depot_comptes"]
        reasons.append("dépose ses comptes")

    # Contactable (on valorise surtout un email FIABLE, pas un email deviné)
    if (c["best_email"] and c["best_source"] in SOURCES_FIABLES
            and c["email_status"] in STATUTS_OK):
        score += w["poids_contactable"]
        reasons.append("email fiable")
    elif c["best_email"] and c["email_status"] in STATUTS_OK + ("devine_mx",):
        score += w["poids_contactable"] * 0.4
        reasons.append("email à confirmer")
    elif c["best_email"]:
        score += w["poids_contactable"] * 0.2
        reasons.append("email incertain")
    else:
        reasons.append("pas d'email")

    # Âge
    try:
        age = datetime.date.today().year - int(c["date_creation"][:4])
        if w["age_min_ideal"] <= age <= w["age_max_ideal"]:
            reasons.append(f"âge {age} ans")
        elif age < w["age_min_ideal"]:
            score -= 3
            reasons.append("très jeune")
    except Exception:
        pass

    c["score"] = max(0, min(100, round(score)))
    c["tier"] = ("A" if c["score"] >= w["seuil_tier_A"]
                 else "B" if c["score"] >= w["seuil_tier_B"] else "C")
    c["raisons"] = reasons
    return c


# --------------------------------------------------------------------------- #
#  5. État (dédup / opt-out) et sorties
# --------------------------------------------------------------------------- #
def load_set(path, col):
    s = set()
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = (row.get(col) or "").strip()
                if v:
                    s.add(v)
    return s


def append_seen(path, sirens):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        if not exists:
            wr.writerow(["siren", "date_ajout"])
        for s in sirens:
            wr.writerow([s, datetime.date.today().isoformat()])


def bodacc_summary(b):
    parts = []
    if b.get("distress"):
        parts.append(f"DIFFICULTE: {b.get('distress_label')}")
    if b.get("depot_comptes"):
        parts.append(f"comptes {str(b['depot_comptes'])[:10]}")
    if b.get("modif_recente"):
        parts.append(f"modif {str(b['modif_recente'])[:10]}")
    if b.get("creation"):
        parts.append(f"création {str(b['creation'])[:10]}")
    return "; ".join(parts)


def write_outputs(rows, cfg):
    outdir = cfg["sortie"]["dossier"]
    os.makedirs(outdir, exist_ok=True)
    stamp = datetime.date.today().isoformat()
    cols = ["score", "tier", "siren", "nom", "naf", "naf_label", "effectif",
            "categorie", "date_creation", "ca", "ca_prev", "ville", "cp",
            "dirigeant", "qualite", "domain", "best_email", "email_source",
            "email_status", "autres_emails", "signaux_bodacc", "raisons",
            "source", "date_ajout"]
    csv_path = os.path.join(outdir, f"prospects_{stamp}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        wr = csv.DictWriter(f, fieldnames=cols)
        wr.writeheader()
        for c in rows:
            wr.writerow({
                "score": c["score"], "tier": c["tier"], "siren": c["siren"],
                "nom": c["nom"], "naf": c["naf"], "naf_label": c["naf_label"],
                "effectif": EFFECTIF_LABELS.get(c["effectif_code"], c["effectif_code"]),
                "categorie": c["categorie"], "date_creation": c["date_creation"],
                "ca": c["ca"] or "", "ca_prev": c["ca_prev"] or "",
                "ville": c["ville"], "cp": c["cp"],
                "dirigeant": f"{c['dir_prenom']} {c['dir_nom']}".strip(),
                "qualite": c["dir_qualite"], "domain": c["domain"],
                "best_email": c["best_email"], "email_source": c["best_source"],
                "email_status": c["email_status"],
                "autres_emails": "; ".join(c["emails"][1:5]),
                "signaux_bodacc": bodacc_summary(c["bodacc"]),
                "raisons": " | ".join(c["raisons"]),
                "source": "SIRENE (recherche-entreprises.api.gouv.fr) + BODACC (DILA) + site web",
                "date_ajout": stamp,
            })
    with open(os.path.join(outdir, f"prospects_{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
    write_summary(rows, cfg, stamp)
    return csv_path


def write_summary(rows, cfg, stamp):
    kept = [r for r in rows if not r["exclu"]]
    a = [r for r in kept if r["tier"] == "A"]
    b = [r for r in kept if r["tier"] == "B"]
    fiables = [r for r in kept if r["best_email"] and r["best_source"] in SOURCES_FIABLES]
    excl = [r for r in rows if r["exclu"]]
    lines = [
        f"# Prospects AskData - {stamp}", "",
        f"- Candidats analysés : {len(rows)}",
        f"- Retenus (hors difficulté) : {len(kept)}",
        f"- Exclus (procédure collective / liquidation) : {len(excl)}",
        f"- Avec email fiable (site/Hunter/Dropcontact) : {len(fiables)}",
        f"- Tier A (>= {cfg['scoring']['seuil_tier_A']}) : {len(a)}",
        f"- Tier B : {len(b)}", "",
        "## Top 15", "",
        "| Score | Tier | Entreprise | Ville | Email | Source | Signaux |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in kept[:15]:
        lines.append(f"| {r['score']} | {r['tier']} | {r['nom'][:32]} | "
                     f"{r['ville']} | {r['best_email'] or '-'} | "
                     f"{r['best_source'] or '-'} | {bodacc_summary(r['bodacc']) or '-'} |")
    with open(os.path.join(cfg["sortie"]["dossier"], f"resume_{stamp}.md"),
              "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def push_brevo(rows, cfg):
    if not BREVO_KEY:
        log("  ! BREVO_API_KEY absente, push ignoré")
        return
    list_id = cfg["sortie"].get("brevo_list_id") or 0
    pushed = 0
    for c in rows:
        if c["exclu"] or c["tier"] not in ("A", "B") or not c["best_email"]:
            continue
        # On ne pousse JAMAIS un email seulement deviné (protection réputation d'expéditeur)
        if c["best_source"] not in SOURCES_FIABLES and c["email_status"] != "valid":
            continue
        body = {"email": c["best_email"], "updateEnabled": True,
                "attributes": {"NOM": c["dir_nom"], "PRENOM": c["dir_prenom"],
                               "ENTREPRISE": c["nom"], "SIREN": c["siren"],
                               "SCORE": c["score"], "VILLE": c["ville"],
                               "NAF": c["naf"]}}
        if list_id:
            body["listIds"] = [int(list_id)]
        try:
            r = session.post(BREVO_URL, json=body,
                             headers={"api-key": BREVO_KEY,
                                      "Content-Type": "application/json"}, timeout=15)
            if r.status_code in (200, 201, 204):
                pushed += 1
        except Exception:
            pass
    log(f"  Brevo : {pushed} contacts poussés (emails fiables uniquement)")


# --------------------------------------------------------------------------- #
#  Orchestrateur
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Pipeline de prospection AskData")
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--limit", type=int, default=0, help="plafond de candidats à enrichir")
    ap.add_argument("--no-enrich", action="store_true")
    ap.add_argument("--no-bodacc", action="store_true")
    ap.add_argument("--push", action="store_true")
    args = ap.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if args.limit:
        cfg["targeting"]["candidats_max"] = args.limit

    log("1/5 Ciblage SIRENE...")
    companies = sirene_search(cfg)
    log(f"  {len(companies)} entreprises ciblées")
    if not companies:
        log("  Aucune entreprise : vérifie les codes NAF / la zone dans config.yml")
        return

    seen = load_set("data/seen.csv", "siren")
    supp_siren = load_set("data/suppression.csv", "siren")
    supp_email = load_set("data/suppression.csv", "email")
    companies = [c for c in companies if c["siren"] not in seen and c["siren"] not in supp_siren]
    log(f"  {len(companies)} après dédup / opt-out")

    # Pré-tri (CA connu d'abord) pour respecter le plafond de candidats
    companies.sort(key=lambda c: (c["ca"] or 0), reverse=True)
    companies = companies[: cfg["targeting"]["candidats_max"]]

    if not args.no_bodacc:
        log("2/5 Signaux BODACC...")
        for i, c in enumerate(companies, 1):
            c["bodacc"] = bodacc_signals(c["siren"])
            if i % 25 == 0:
                log(f"  BODACC {i}/{len(companies)}")

    if not args.no_enrich:
        log("3/5 Enrichissement emails...")
        for i, c in enumerate(companies, 1):
            enrich(c, cfg)
            if c["best_email"] and c["best_email"] in supp_email:
                c["best_email"], c["best_source"], c["email_status"] = "", "", "opt-out"
            if i % 25 == 0:
                log(f"  enrich {i}/{len(companies)}")

    log("4/5 Scoring...")
    for c in companies:
        score_company(c, cfg)
    companies.sort(key=lambda c: c["score"], reverse=True)

    log("5/5 Sortie...")
    csv_path = write_outputs(companies, cfg)
    kept = [c for c in companies if not c["exclu"]]
    append_seen("data/seen.csv", [c["siren"] for c in kept])
    log(f"  -> {csv_path}")
    log(f"  Retenus: {len(kept)} | Tier A: {sum(1 for c in kept if c['tier'] == 'A')} "
        f"| email fiable: {sum(1 for c in kept if c['best_email'] and c['best_source'] in SOURCES_FIABLES)}")

    if args.push or cfg["sortie"].get("pousser_vers_brevo"):
        push_brevo(companies, cfg)


if __name__ == "__main__":
    main()
