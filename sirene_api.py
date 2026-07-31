#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sirene_api.py - Collecte SIRENE des cibles AskData via l'API publique
recherche-entreprises.api.gouv.fr (aucune cle, aucun telechargement).

Cible : TPE/PME qui ont de la donnee a exploiter mais pas de profil data
(editeurs, ESN, e-commerce, comptables, agences...). Le filtre combine :
 - les codes NAF de config.yml (regroupes en 10 verticales) ;
 - la tranche d'effectif salarie de l'unite legale (3-499 par defaut,
   voir cle "effectifs" de config.yml) ;
 - l'etat administratif actif.

Concu pour tenir des heures sans casser :
 - chaque requete est retentee 4 fois (timeouts, coupures reseau, 429) avec
   attente progressive ; une page definitivement en echec est SAUTEE, jamais fatale ;
 - une seule requete par departement et par profil d'effectif (tous les NAF du
   profil combines), avec decoupage automatique par NAF si un departement
   depasse le plafond de l'API (10 000 resultats par recherche).

OPT-OUT : les entreprises listees dans data/optout.csv (SIREN, domaine ou
email) n'entrent JAMAIS dans la base, des la collecte.

    python sirene_api.py
    python sirene_api.py --departements 69 38 01 42 73 74
    python sirene_api.py --out sirene_askdata.csv --sleep 0.2
"""
import argparse
import csv
import sys
import time
from collections import Counter

from common import (load_config, naf_type_map, effectif_groups, all_departements,
                    clean_siret, siren_of, naf_label, load_optout, is_optout,
                    ensure_optout_file, USER_AGENT, BASE_COLS)

try:
    import requests
except ImportError:
    requests = None

API = "https://recherche-entreprises.api.gouv.fr/search"
PER_PAGE = 25
MAX_PAGES = 400      # plafond API : page * per_page <= 10 000
TIMEOUT = 60
RETRIES = 4


def fetch(session, params, sleep):
    """GET avec retries sur TOUT (timeout, reseau, 429, 5xx). Renvoie {} si echec definitif."""
    for attempt in range(RETRIES):
        try:
            r = session.get(API, params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ! {type(e).__name__} sur {params.get('departement')} p{params.get('page')} "
                  f"(tentative {attempt + 1}/{RETRIES}), retry dans {wait}s", file=sys.stderr)
            time.sleep(wait)
    print(f"  !! page abandonnee: dep {params.get('departement')} p{params.get('page')}", file=sys.stderr)
    return {}


def parse_result(res, tmap):
    if str(res.get("etat_administratif") or "").upper() == "C":
        return None          # societe cessee : hors prospection
    naf = res.get("activite_principale") or ""
    typ = tmap.get(naf.replace(".", "").upper(), "autre")
    siege = res.get("siege") or {}
    siret = clean_siret(siege.get("siret"))
    siren = res.get("siren") or siren_of(siret)
    if not siren or len(siren) != 9:
        return None
    dep = siege.get("departement") or (siege.get("code_commune") or "")[:2]
    return {"siren": siren, "siret": siret,
            "nom": res.get("nom_raison_sociale") or res.get("nom_complet") or "",
            "type": typ, "naf": naf,
            "libelle": naf_label(naf),   # l'API ne renvoie pas le libelle : rempli localement
            "commune": siege.get("libelle_commune") or "",
            "departement": dep, "telephone": "", "source": "sirene"}


def collect_query(sess, base_params, rows, tmap, sleep, force=False):
    """Pagine une recherche. Renvoie None si trop de resultats (a decouper), sinon le nb de pages lues."""
    page, pages = 1, 1
    while page <= pages:
        data = fetch(sess, {**base_params, "page": page, "per_page": PER_PAGE}, sleep)
        if not data:
            return page - 1          # echec definitif de cette page : on passe a la suite
        total = int(data.get("total_pages", 1) or 1)
        if total > MAX_PAGES and not force:
            return None              # trop gros pour une requete combinee : decouper par NAF
        pages = min(total, MAX_PAGES)
        for res in data.get("results", []):
            row = parse_result(res, tmap)
            if row:
                rows[row["siren"]] = row
        page += 1
        time.sleep(sleep)
    return page - 1


def collect(cfg, deps, sleep):
    if requests is None:
        sys.exit("Le module 'requests' est requis (pip install requests).")
    tmap = naf_type_map(cfg)
    rows, sess = {}, requests.Session()
    for tranches, nafs in effectif_groups(cfg):
        base = {"activite_principale": ",".join(nafs), "etat_administratif": "A"}
        if tranches:
            base["tranche_effectif_salarie"] = ",".join(tranches)
        label = ",".join(tranches) if tranches else "toutes"
        print(f"Profil effectifs [{label}] : {len(nafs)} codes NAF", file=sys.stderr)
        for dep in deps:
            r = collect_query(sess, {**base, "departement": dep}, rows, tmap, sleep)
            if r is None:
                print(f"  dep {dep}: volumineux, decoupage par NAF", file=sys.stderr)
                for naf in nafs:
                    collect_query(sess, {**base, "activite_principale": naf, "departement": dep},
                                  rows, tmap, sleep, force=True)
            print(f"  dep {dep} -> cumul {len(rows)}", file=sys.stderr)
    return list(rows.values())


def main():
    ap = argparse.ArgumentParser(description="Collecte SIRENE AskData via API (sans telechargement).")
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--departements", nargs="+")
    ap.add_argument("--out", default="sirene_askdata.csv")
    ap.add_argument("--sleep", type=float, default=0.2, help="Pause entre requetes (s).")
    a = ap.parse_args()

    cfg = load_config(a.config)
    deps = a.departements or (cfg.get("departements") or all_departements())
    rows = collect(cfg, deps, a.sleep)

    # opt-out RGPD : jamais dans la base, meme pas une fois
    ensure_optout_file()
    opt = load_optout()
    before = len(rows)
    rows = [r for r in rows if not is_optout(opt, *r.values())]
    if before - len(rows):
        print(f"Opt-out : {before - len(rows)} entreprise(s) ecartee(s) des la collecte", file=sys.stderr)

    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BASE_COLS)
        w.writeheader()
        w.writerows(rows)
    print(f"SIRENE (api): {len(rows)} entreprises -> {a.out}", file=sys.stderr)
    for typ, n in Counter(r["type"] for r in rows).most_common():
        print(f"  {typ}: {n}", file=sys.stderr)


if __name__ == "__main__":
    main()
