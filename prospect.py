#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AskData - Pipeline de prospection intelligent (enrichissement maximal, 100% gratuit)
====================================================================================
Sources officielles :
  1. SIRENE (API Recherche d'entreprises, data.gouv)  -> ciblage NAF / région / effectif
  2. BODACC (API DILA)                                -> signaux + exclusion des sociétés en difficulté
  3. Enrichissement (gratuit, sans clé) :
       - devinette du domaine, PUIS recherche web (DuckDuckGo) si échec
       - scraping du site : pages standard + liens contact/mentions découverts
       - décodage des emails cachés (Cloudflare, entités HTML, "[at]/[dot]/arobase")
       - emails de rôle (contact@, info@) et email dirigeant devinés en secours
       - récupération du téléphone et du LinkedIn de l'entreprise
  4. Scoring 0-100 -> tier A/B/C
  5. Sortie -> CSV + JSON + résumé Markdown

Les clés Hunter / Dropcontact sont facultatives (variables d'environnement) et améliorent
encore la récupération d'emails, mais tout fonctionne sans aucune clé.

RGPD : cible B2B (intérêt légitime, opt-out). Chaque ligne trace la source. Les SIREN et
emails de data/suppression.csv ne ressortent jamais.

Usage :
    python prospect.py                 # run complet
    python prospect.py --limit 150     # limite le nombre de candidats enrichis
    python prospect.py --no-enrich     # ciblage + scoring seuls
    python prospect.py --no-bodacc     # sans les signaux BODACC
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
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

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
DDG_URL = "https://html.duckduckgo.com/html/"
HUNTER_BASE = "https://api.hunter.io/v2"
DROPCONTACT = "https://api.dropcontact.io/batch"

USER_AGENT = ("Mozilla/5.0 (compatible; AskData-Prospect/2.0; "
              "+https://askdata-bi.netlify.app; romtaug+askdata@gmail.com)")

HUNTER_KEY = os.getenv("HUNTER_API_KEY", "").strip()
DROPCONTACT_KEY = os.getenv("DROPCONTACT_API_KEY", "").strip()

NAF_PARAM = "activite_principale"   # auto-détecté au démarrage

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(r"(?<!\d)(?:\+33\s?|0033\s?|0)[1-9](?:[\s.\-]?\d{2}){4}(?!\d)")
LINKEDIN_RE = re.compile(r"https?://[a-z]{0,3}\.?linkedin\.com/company/[A-Za-z0-9_\-%]+", re.I)
CF_HEX = re.compile(r'data-cfemail="([0-9a-fA-F]{8,})"')
CF_HEX2 = re.compile(r'/cdn-cgi/l/email-protection#([0-9a-fA-F]{8,})')
_AT = r'(?:\s*\[at\]\s*|\s*\(at\)\s*|\s*\{at\}\s*|\s+arobase\s+|@)'
_DOT = r'(?:\s*\[dot\]\s*|\s*\(dot\)\s*|\s*\{dot\}\s*|\s+point\s+|\.)'
OBF_RE = re.compile(r'([a-z0-9._%+\-]+)' + _AT + r'([a-z0-9.\-]+)' + _DOT + r'([a-z]{2,})', re.I)

ROLE_PREFIXES = ("contact", "info", "hello", "bonjour", "commercial",
                 "direction", "compta", "rh", "sav", "accueil")
ROLE_GUESS = ("contact", "info", "bonjour", "hello")
LEGAL_SUFFIXES = (" sas", " sasu", " sarl", " eurl", " sa ", " sci",
                  " scop", " snc", " selarl", " ei ", " eirl")
SOURCES_FIABLES = ("site", "hunter", "dropcontact")
STATUTS_OK = ("valid", "accept_all", "webmail", "mx_ok")

PLACEHOLDER_LOCAL = {"your", "you", "name", "email", "exemple", "example", "nom",
                     "prenom", "prenom.nom", "test", "user", "username", "sample",
                     "john", "jane", "firstname", "lastname", "noreply", "no-reply",
                     "donotreply", "votre", "vous"}
PLACEHOLDER_DOM = ("example.", "email.com", "domain.com", "yourdomain", "yourcompany",
                   "mondomaine", "monsite", "votredomaine", "sentry", "wixpress",
                   "godaddy", "wordpress.", "@2x")
# domaines à ignorer dans la recherche web (annuaires, réseaux sociaux, etc.)
DOMAIN_BLACKLIST = ("societe.com", "pappers.fr", "infogreffe.fr", "pagesjaunes.fr",
                    "verif.com", "linkedin.com", "facebook.com", "twitter.com", "x.com",
                    "instagram.com", "youtube.com", "wikipedia.org", "google.", "bing.com",
                    "yelp.", "mappy.", "kompass.com", "manageo.fr", "bodacc", "data.gouv.fr",
                    "indeed.", "glassdoor.", "leboncoin.fr", "score3.fr", "annuaire",
                    "dnb.com", "usinenouvelle.com", "corporama", "ellisphere", "hellowork",
                    "societeinfo", "pappers", "verif", "figaro", "lefigaro", "bfmtv")

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
session.headers.update({"User-Agent": USER_AGENT,
                        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"})


# --------------------------------------------------------------------------- #
#  Utilitaires
# --------------------------------------------------------------------------- #
def log(msg):
    print(f"[{datetime.datetime.now():%H:%M:%S}] {msg}", flush=True)


def strip_acc(s):
    return (unicodedata.normalize("NFKD", s or "")
            .encode("ascii", "ignore").decode().lower())


def slugify(name):
    n = " " + name.lower() + " "
    for suf in LEGAL_SUFFIXES:
        n = n.replace(suf, " ")
    n = strip_acc(n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def is_placeholder(email):
    local, _, dom = email.partition("@")
    if local in PLACEHOLDER_LOCAL:
        return True
    return any(pp in dom for pp in PLACEHOLDER_DOM)


def norm_phone(s):
    d = re.sub(r"[^\d+]", "", s)
    digits = re.sub(r"\D", "", d)
    if len(set(digits)) <= 1:      # 0000000000 etc.
        return ""
    return d


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


def http_get(url, cfg):
    try:
        r = session.get(url, timeout=cfg["enrichissement"]["timeout_http"],
                        allow_redirects=True)
        if r.status_code < 400 and len(r.text) > 100:
            return r.text
    except Exception:
        pass
    return None


# --------------------------------------------------------------------------- #
#  1. SIRENE - ciblage
# --------------------------------------------------------------------------- #
def detect_naf_param(sample_naf):
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
        "domain": "", "emails": [], "email_sources": {},
        "best_email": "", "best_source": "", "email_status": "",
        "telephone": "", "linkedin": "",
        "bodacc": {}, "score": 0, "tier": "", "exclu": False, "raisons": [],
    }


# --------------------------------------------------------------------------- #
#  2. BODACC - signaux + exclusion (vérifiée par SIREN)
# --------------------------------------------------------------------------- #
def bodacc_signals(siren, delay=0.2):
    spaced = f"{siren[:3]} {siren[3:6]} {siren[6:]}"
    params = {"where": f'"{siren}" OR "{spaced}"', "limit": 100}
    data = get_json(BODACC_URL, params=params, timeout=20)
    time.sleep(delay)
    sig = {"distress": False, "distress_label": "", "depot_comptes": None,
           "modif_recente": None, "creation": None, "n_events": 0}
    if not data:
        return sig
    results = data.get("results") or []
    today = datetime.date.today()
    siren_digits = re.sub(r"\D", "", siren)
    matched = 0

    def recent(dstr, months=24):
        try:
            d = datetime.date.fromisoformat((dstr or "")[:10])
            return (today - d).days <= months * 31
        except Exception:
            return False

    for rec in results:
        # La recherche plein-texte peut ramener des annonces d'AUTRES sociétés.
        # On ne garde que celles où le SIREN de l'entreprise apparaît réellement.
        if siren_digits not in re.sub(r"\D", "", json.dumps(rec, ensure_ascii=False)):
            continue
        matched += 1
        dp = rec.get("dateparution") or ""
        famille = strip_acc(rec.get("familleavis_lib") or rec.get("familleavis") or "")
        typeavis = strip_acc(rec.get("typeavis_lib") or rec.get("typeavis") or "")
        jug = rec.get("jugement")
        blob = strip_acc(f"{famille} {typeavis} "
                         + (json.dumps(jug, ensure_ascii=False) if jug else ""))
        distress_kw = ("liquidation judiciaire", "redressement judiciaire",
                       "sauvegarde", "procedure collective", "cessation des paiements",
                       "insuffisance d actif", "interdiction de gerer")
        if jug or any(k in blob for k in distress_kw):
            sig["distress"] = True
            sig["distress_label"] = (extract_nature(jug) or famille
                                     or typeavis or "procédure collective")[:90]
        if rec.get("depot") or "depot" in famille or "compte" in famille:
            if not sig["depot_comptes"] or dp > (sig["depot_comptes"] or ""):
                sig["depot_comptes"] = dp
        if rec.get("modificationsgenerales") or "modif" in famille:
            if recent(dp) and (not sig["modif_recente"] or dp > sig["modif_recente"]):
                sig["modif_recente"] = dp
        if "creation" in famille or "immatriculation" in famille:
            sig["creation"] = dp
    sig["n_events"] = matched
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
#  3. Enrichissement
# --------------------------------------------------------------------------- #
def domain_matches(domain, company, cfg):
    """Le domaine résout ET la page parle bien de l'entreprise (nom ou SIREN présent)."""
    if any(b in domain for b in DOMAIN_BLACKLIST):
        return False
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
        txt = http_get(scheme + domain, cfg)
        if not txt:
            continue
        low = strip_acc(txt)
        keys = [w for w in slugify(company["raison_sociale"] or company["nom"]).split()
                if len(w) > 3]
        if keys and any(k in low for k in keys):
            return True
        if company["siren"] in re.sub(r"\D", "", txt):
            return True
        return False
    return False


def guess_domains(company, cfg):
    base = slugify(company["raison_sociale"] or company["nom"])
    if not base:
        return []
    words = base.split()
    stems = {"".join(words), "-".join(words)}
    if words and len(words[0]) >= 6:
        stems.add(words[0])
    tlds = cfg["enrichissement"].get("tlds", ["fr", "com"])
    out = []
    for stem in stems:
        if len(stem) >= 4:
            for tld in tlds:
                out.append(f"{stem}.{tld}")
    return out


def ddg_domain(company, cfg):
    """Recherche web (DuckDuckGo) pour trouver le site quand la devinette échoue."""
    q = f'{company["raison_sociale"] or company["nom"]} {company["ville"]}'.strip()
    txt = None
    try:
        r = session.get(DDG_URL, params={"q": q, "kl": "fr-fr"},
                        timeout=cfg["enrichissement"]["timeout_http"])
        if r.status_code < 400:
            txt = r.text
    except Exception:
        return ""
    if not txt:
        return ""
    cands = [unquote(x) for x in re.findall(r'uddg=([^&"]+)', txt)]
    cands += re.findall(r'href="(https?://[^"]+)"', txt)
    seen = set()
    checked = 0
    for url in cands:
        try:
            dom = urlparse(url).netloc.lower()
        except Exception:
            continue
        dom = dom[4:] if dom.startswith("www.") else dom
        if not dom or dom in seen or any(b in dom for b in DOMAIN_BLACKLIST):
            continue
        seen.add(dom)
        checked += 1
        if checked > 8:
            break
        if domain_matches(dom, company, cfg):
            return dom
    return ""


def resolve_domain(company, cfg):
    if not cfg["enrichissement"].get("deviner_domaine", True):
        return ""
    for dom in guess_domains(company, cfg):
        if domain_matches(dom, company, cfg):
            return dom
    if cfg["enrichissement"].get("recherche_web", True):
        return ddg_domain(company, cfg)
    return ""


def discover_contact_links(domain, home_html):
    """Trouve les liens contact/mentions/à-propos sur la page d'accueil."""
    links = set()
    if not HAS_BS4:
        return links
    try:
        soup = BeautifulSoup(home_html, "html.parser")
        for a in soup.find_all("a", href=True):
            key = strip_acc(a["href"] + " " + a.get_text())
            if any(k in key for k in ("contact", "mention", "legal", "propos",
                                      "about", "equipe", "team", "impressum")):
                href = a["href"]
                if href.startswith("http") and domain in href:
                    links.add(href.split("#")[0])
                elif href.startswith("/"):
                    links.add("https://" + domain + href.split("#")[0])
    except Exception:
        pass
    return set(list(links)[:6])


def parse_page(text, domain):
    emails, phones, linkedin = set(), set(), ""
    t = html.unescape(text)
    for m in EMAIL_RE.findall(t):
        emails.add(m.lower())
    for hx in CF_HEX.findall(text) + CF_HEX2.findall(text):
        dec = cf_decode(hx)
        if EMAIL_RE.fullmatch(dec):
            emails.add(dec.lower())
    for g1, g2, g3 in OBF_RE.findall(t):
        cand = f"{g1}@{g2}.{g3}".lower()
        if EMAIL_RE.fullmatch(cand):
            emails.add(cand)
    for m in PHONE_RE.findall(t):
        ph = norm_phone(m)
        if ph:
            phones.add(ph)
    # numéros déclarés dans les données structurées (schema.org / JSON-LD)
    for m in re.findall(r'"telephone"\s*:\s*"([^"]{6,30})"', t):
        ph = norm_phone(m)
        if ph and len(re.sub(r"\D", "", ph)) >= 9:
            phones.add(ph)
    lk = LINKEDIN_RE.search(text)
    if lk:
        linkedin = lk.group(0)
    if HAS_BS4:
        try:
            soup = BeautifulSoup(text, "html.parser")
            for a in soup.select('a[href^="mailto"]'):
                addr = a.get("href", "").replace("mailto:", "").split("?")[0].strip().lower()
                if EMAIL_RE.fullmatch(addr):
                    emails.add(addr)
            for a in soup.select('a[href^="tel:"]'):
                ph = norm_phone(a.get("href", "").replace("tel:", ""))
                if ph and len(re.sub(r"\D", "", ph)) >= 9:
                    phones.add(ph)
        except Exception:
            pass
    emails = {e for e in emails
              if not e.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js"))
              and not is_placeholder(e)}
    return emails, phones, linkedin


def cf_decode(hexstr):
    try:
        key = int(hexstr[:2], 16)
        return "".join(chr(int(hexstr[i:i + 2], 16) ^ key)
                       for i in range(2, len(hexstr), 2))
    except Exception:
        return ""


def scrape_site(domain, cfg):
    emails, phones, linkedin = set(), set(), ""
    base = None
    home = None
    for sch in ("https://", "http://"):
        home = http_get(sch + domain + "/", cfg)
        if home:
            base = sch
            break
    if not home:
        return {"emails": [], "phones": [], "linkedin": ""}
    e, p, l = parse_page(home, domain)
    emails |= e
    phones |= p
    linkedin = linkedin or l
    urls = set(discover_contact_links(domain, home))
    for path in cfg["enrichissement"]["paths_a_scanner"]:
        urls.add(base + domain + path)
    for u in list(urls)[:10]:
        txt = http_get(u, cfg)
        if txt:
            e, p, l = parse_page(txt, domain)
            emails |= e
            phones |= p
            linkedin = linkedin or l
    return {"emails": sorted(emails), "phones": sorted(phones), "linkedin": linkedin}


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


def guess_role_emails(domain):
    return [f"{p}@{domain}" for p in ROLE_GUESS] if domain else []


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
            s += 3
        return s

    def tier(e):
        return 0 if sources.get(e, "") in SOURCES_FIABLES else 1

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
    sources = {}

    def add(emails, src):
        for e in emails:
            e = (e or "").lower().strip()
            if e and EMAIL_RE.fullmatch(e) and not is_placeholder(e) and e not in sources:
                sources[e] = src

    if company["domain"] and cfg["enrichissement"].get("scrape_site", True):
        data = scrape_site(company["domain"], cfg)
        add(data["emails"], "site")
        company["telephone"] = data["phones"][0] if data["phones"] else ""
        company["linkedin"] = data["linkedin"]
    add(hunter_emails(company["domain"], company["dir_prenom"], company["dir_nom"]), "hunter")
    add(dropcontact_email(company), "dropcontact")
    if company["domain"] and cfg["enrichissement"].get("deviner_email_role", True):
        add(guess_role_emails(company["domain"]), "devine")
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
    if c["bodacc"].get("distress"):
        c.update(exclu=True, score=0, tier="EXCLU",
                 raisons=[f"EXCLU: {c['bodacc'].get('distress_label', 'procédure collective')}"])
        return c

    score = 0.0
    score += w["poids_secteur"]
    reasons.append(f"secteur +{w['poids_secteur']}")

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

    if ca and c["ca_prev"]:
        if ca > c["ca_prev"] * 1.05:
            score += w["poids_croissance"]
            reasons.append("en croissance")
        elif ca < c["ca_prev"] * 0.95:
            reasons.append("en baisse")
        else:
            score += w["poids_croissance"] * 0.5

    if c["bodacc"].get("modif_recente"):
        score += w["poids_activite_bodacc"]
        reasons.append("activité récente (BODACC)")
    if c["bodacc"].get("depot_comptes"):
        score += w["bonus_depot_comptes"]
        reasons.append("dépose ses comptes")

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
#  5. État et sorties
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
            "email_status", "telephone", "linkedin", "autres_emails",
            "signaux_bodacc", "raisons", "source", "date_ajout"]
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
                "email_status": c["email_status"], "telephone": c["telephone"],
                "linkedin": c["linkedin"],
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
    tel = [r for r in kept if r["telephone"]]
    excl = [r for r in rows if r["exclu"]]
    lines = [
        f"# Prospects AskData - {stamp}", "",
        f"- Candidats analysés : {len(rows)}",
        f"- Retenus (hors difficulté) : {len(kept)}",
        f"- Exclus (procédure collective / liquidation) : {len(excl)}",
        f"- Avec email fiable : {len(fiables)}",
        f"- Avec téléphone : {len(tel)}",
        f"- Tier A (>= {cfg['scoring']['seuil_tier_A']}) : {len(a)}",
        f"- Tier B : {len(b)}", "",
        "## Top 15", "",
        "| Score | Tier | Entreprise | Ville | Email | Source | Tel |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in kept[:15]:
        lines.append(f"| {r['score']} | {r['tier']} | {r['nom'][:30]} | {r['ville']} | "
                     f"{r['best_email'] or '-'} | {r['best_source'] or '-'} | "
                     f"{r['telephone'] or '-'} |")
    with open(os.path.join(cfg["sortie"]["dossier"], f"resume_{stamp}.md"),
              "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# --------------------------------------------------------------------------- #
#  Orchestrateur
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Pipeline de prospection AskData")
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--limit", type=int, default=0, help="plafond de candidats à enrichir")
    ap.add_argument("--no-enrich", action="store_true")
    ap.add_argument("--no-bodacc", action="store_true")
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

    companies.sort(key=lambda c: (c["ca"] or 0), reverse=True)
    companies = companies[: cfg["targeting"]["candidats_max"]]

    def process_one(c):
        if not args.no_bodacc:
            c["bodacc"] = bodacc_signals(c["siren"])
        if not args.no_enrich:
            enrich(c, cfg)
            if c["best_email"] and c["best_email"] in supp_email:
                c["best_email"], c["best_source"], c["email_status"] = "", "", "opt-out"
        return c

    workers = int(cfg["enrichissement"].get("workers", 8))
    log(f"2/5 + 3/5 BODACC + enrichissement (en parallèle, {workers} threads)...")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in as_completed([ex.submit(process_one, c) for c in companies]):
            done += 1
            if done % 25 == 0:
                log(f"  traité {done}/{len(companies)}")

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
        f"| email fiable: {sum(1 for c in kept if c['best_email'] and c['best_source'] in SOURCES_FIABLES)} "
        f"| tel: {sum(1 for c in kept if c['telephone'])}")


if __name__ == "__main__":
    main()
