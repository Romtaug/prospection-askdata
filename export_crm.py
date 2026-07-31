#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_crm.py - Genere le CRM de prospection AskData au format CSV.

Prend la base enrichie et en fait un fichier de travail commercial :
 - ajoute 12 colonnes de suivi : prospection (statut, date_contact, canal,
   date_relance, notes) et vente (produit, montant_vendu, date_vente, paye,
   montant_encaisse, commission, statut_livraison) ;
 - remet les colonnes dans un ordre utile (identite, priorite, suivi, contact,
   liens, firmographie, identifiants) au lieu de l'ordre technique ;
 - contient TOUTE la base : les lignes fichees avec leurs donnees, les autres
   en attente avec les colonnes vides (option --enrichies-seulement) ;
 - trie par score decroissant : les meilleures lignes en haut ;
 - CONSERVE le suivi deja saisi. Le script relit le CRM existant et reapplique
   statut, dates, canal et notes par SIREN. Tes annotations ne sont jamais
   perdues, meme apres un nouvel enrichissement ou un refresh mensuel ;
 - applique l'OPT-OUT (data/optout.csv) : une entreprise en opt-out n'apparait
   dans AUCUN export, meme si elle traine encore dans une vieille base.

Tout est en CSV : lisible directement sur GitHub, versionne ligne par ligne,
et ouvrable dans Excel ou LibreOffice sans rien installer.

    python export_crm.py
    python export_crm.py --types saas_web comptabilite --out crm/crm_prio.csv
    python export_crm.py --tier A --dep 69 38 01 42 73 74
    python export_crm.py --par-type            # un fichier CSV par verticale
"""
import argparse
import csv
import gzip
import os
import sys
from collections import Counter
from datetime import date

from common import load_optout, is_optout, ensure_optout_file
from enrich import OUT_COLS as COLS_BASE_ENRICHIE   # schema complet : en-tete CRM stable

# Colonnes de suivi commercial (ajoutees ici, jamais produites par enrich.py).
# Bloc 1 : la prospection. Bloc 2 : la vente et l'encaissement.
SUIVI_COLS = ["statut", "date_contact", "canal", "date_relance", "notes",
              "produit", "montant_vendu", "date_vente", "paye",
              "montant_encaisse", "commission", "statut_livraison"]
STATUT_DEFAUT = "a contacter"
STATUTS = ["a contacter", "contacte", "relance", "interesse", "rdv", "client",
           "refuse", "injoignable", "hors cible"]
CANAUX = ["email", "telephone", "linkedin", "courrier", "visite"]
PRODUITS = ["POC", "Abonnement", "Deploiement", "Formation"]
LIVRAISON = ["a faire", "en cours", "livre"]
TAUX_COMMISSION = 0.40        # commission closer, sur l'encaisse
SANS_COMMISSION = {"formation"}   # la formation est de la prestation, pas de la vente

# Ordre d'affichage des colonnes : ce qu'on lit en premier est a gauche.
ORDRE = (["nom", "tier", "score"] + SUIVI_COLS +
         ["type", "libelle", "commune", "cp", "departement", "region",
          "telephone", "email", "email_status", "dirigeant", "qualite", "raisons",
          "lien_site", "lien_pappers", "lien_annuaire", "lien_annonces",
          "lien_finances", "lien_dirigeants", "lien_maps", "lien_google",
          "lien_google_news", "lien_linkedin_rech", "lien_linkedin_dirigeant",
          "lien_pagesjaunes", "lien_societe", "lien_trustpilot",
          "linkedin", "domain",
          "effectif", "annee_effectif", "categorie", "nb_etablissements_ouverts",
          "nb_etablissements", "communes_etablissements",
          "ca", "ca_prev", "resultat_net", "annee_finances", "anciennete", "date_creation",
          "nature_juridique", "sigle", "nom_commercial", "employeur",
          "entrepreneur_individuel", "diffusion_protegee",
          "labels", "idcc", "tva_intra", "etat", "signaux_bodacc",
          "siren", "siret", "naf", "adresse", "latitude", "longitude",
          "email_source", "email_perso", "autres_emails", "autres_dirigeants",
          "a_site", "web_fait", "date_enrichi", "source"])


def lire(chemin):
    op = gzip.open if chemin.endswith(".gz") else open
    with op(chemin, "rt", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def premier_existant(*chemins):
    for c in chemins:
        if os.path.exists(c):
            return c
    return None


def charger(inp, enrichies_seulement=False):
    """Renvoie (lignes, description de la source).

    Par defaut le CRM contient TOUTE la base : les lignes deja fichees le sont
    avec leurs donnees, les autres attendent leur tour avec les colonnes vides.
    C'est ce qu'on veut d'un fichier de prospection : le commercial voit
    l'ensemble du marche, pas seulement la part que le pipeline a traitee.
    """
    if inp:
        if not os.path.exists(inp):
            sys.exit(f"Introuvable : {inp}")
        return lire(inp), inp

    brute = premier_existant("data/base_askdata.csv", "base_askdata.csv")
    enrichie = premier_existant("data/base_askdata_enrichi.csv",
                                "data/base_askdata_enrichi.csv.gz",
                                "base_askdata_enrichi.csv")
    if not brute and not enrichie:
        sys.exit("Aucune base trouvee. Lance d'abord 'Build base askdata', "
                 "puis 'Enrichissement base askdata'.")
    if enrichies_seulement:
        if not enrichie:
            sys.exit("Aucune base enrichie : lance 'Enrichissement base askdata'.")
        return lire(enrichie), enrichie
    if not enrichie:
        return lire(brute), brute
    if not brute:
        return lire(enrichie), enrichie

    rows_e = lire(enrichie)
    par_siren = {}
    for r in rows_e:
        si = (r.get("siren") or "").strip()
        if si:
            par_siren[si] = r
    out, fusion = [], 0
    for r in lire(brute):
        e = par_siren.pop((r.get("siren") or "").strip(), None)
        if e:
            r.update({k: v for k, v in e.items() if v not in (None, "")})
            fusion += 1
        out.append(r)
    out.extend(par_siren.values())      # lignes enrichies absentes de la base brute
    print(f"Fusion : {len(out)} lignes au total, dont {fusion} deja fichees "
          f"({100 * fusion / max(1, len(out)):.0f} %)", file=sys.stderr)
    return out, f"{brute} + {enrichie}"


def relire_suivi(chemins):
    """Recupere les colonnes de suivi deja saisies, indexees par SIREN.

    Accepte plusieurs fichiers (le CRM principal et les CRM par verticale) : le
    suivi survit donc meme si tu as travaille dans un fichier filtre.
    """
    out = {}
    for chemin in chemins:
        if not chemin or not os.path.exists(chemin):
            continue
        try:
            for r in lire(chemin):
                siren = (r.get("siren") or "").strip()
                if not siren:
                    continue
                vals = {c: r[c] for c in SUIVI_COLS
                        if r.get(c) and r[c] != STATUT_DEFAUT}
                if vals:
                    out.setdefault(siren, {}).update(vals)
        except Exception as e:
            print(f"! suivi non relu depuis {chemin} ({type(e).__name__}): {e}",
                  file=sys.stderr)
    if out:
        print(f"Suivi conserve : {len(out)} lignes deja travaillees", file=sys.stderr)
    return out


def num(v):
    try:
        return float(str(v).strip())
    except (ValueError, TypeError):
        return 0.0


def ecrire(chemin, rows, cols):
    d = os.path.dirname(chemin)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = chemin + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, chemin)          # remplacement atomique : jamais de fichier tronque
    return os.path.getsize(chemin) / 1024


def resume(rows):
    """Affiche un resume dans le log : c'est le tableau de bord du workflow."""
    n = len(rows)
    def cnt(f):
        return sum(1 for r in rows if f(r))
    fiche = cnt(lambda r: r.get("date_enrichi"))
    mail = cnt(lambda r: r.get("email"))
    tel = cnt(lambda r: r.get("telephone"))
    print(f"\n--- CRM : {n} lignes ---", file=sys.stderr)
    print(f"  fichees            {fiche:6d}  ({100*fiche/n:.1f}%)", file=sys.stderr)
    print(f"  avec email         {mail:6d}  ({100*mail/n:.1f}%)", file=sys.stderr)
    print(f"  email verifie MX   {cnt(lambda r: r.get('email_status')=='mx_ok'):6d}", file=sys.stderr)
    print(f"  avec telephone     {tel:6d}  ({100*tel/n:.1f}%)", file=sys.stderr)
    print(f"  multi-sites        {cnt(lambda r: num(r.get('nb_etablissements_ouverts'))>=2):6d}", file=sys.stderr)
    for t in ("A", "B", "C"):
        c = cnt(lambda r, t=t: (r.get("tier") or "").upper() == t)
        if c:
            print(f"  tier {t}             {c:6d}", file=sys.stderr)
    print("  par verticale :", file=sys.stderr)
    for t, c in Counter(r.get("type", "") for r in rows).most_common():
        a = cnt(lambda r, t=t: r.get("type") == t and (r.get("tier") or "").upper() == "A")
        print(f"    {t:16s} {c:6d}   (tier A : {a})", file=sys.stderr)
    print("  suivi commercial :", file=sys.stderr)
    for s, c in Counter(r.get("statut", "") for r in rows).most_common():
        print(f"    {s:14s} {c:6d}", file=sys.stderr)
    vendu = sum(num(r.get("montant_vendu")) for r in rows)
    encaisse = sum(num(r.get("montant_encaisse")) for r in rows)
    if vendu or encaisse:
        print(f"  vendu       {vendu:10.0f} euros", file=sys.stderr)
        print(f"  encaisse    {encaisse:10.0f} euros", file=sys.stderr)
        com = sum(num(r.get("commission")) for r in rows)
        print(f"  commission  {com:10.0f} euros "
              f"({TAUX_COMMISSION:.0%} de l'encaisse, hors formation)", file=sys.stderr)
        print(f"  clients     {sum(1 for r in rows if r.get('statut') == 'client'):10d}",
              file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="Genere le CRM de prospection AskData (CSV).")
    ap.add_argument("--in", dest="inp", default=None, help="CSV source (auto-detecte par defaut).")
    ap.add_argument("--out", default="crm/crm_askdata.csv")
    ap.add_argument("--types", nargs="+", help="Filtre verticales, ex: saas_web comptabilite")
    ap.add_argument("--tier", nargs="+", help="Filtre tiers, ex: A B")
    ap.add_argument("--dep", nargs="+", help="Filtre departements, ex: 69 38 01")
    ap.add_argument("--min-score", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None, help="Garde les N mieux scorees.")
    ap.add_argument("--par-type", action="store_true",
                    help="Ecrit aussi un CSV par verticale a cote du fichier principal.")
    ap.add_argument("--sans-fusion", action="store_true",
                    help="Ignorer le suivi deja saisi (repart de zero).")
    ap.add_argument("--enrichies-seulement", action="store_true",
                    help="Ne garder que les lignes deja fichees (par defaut : toute la base).")
    a = ap.parse_args()

    rows, src = charger(a.inp, a.enrichies_seulement)
    if not rows:
        sys.exit(f"{src} est vide.")
    print(f"Source : {src} ({len(rows)} lignes)", file=sys.stderr)

    # opt-out : filet final, aucune entreprise exclue ne sort dans un CRM
    ensure_optout_file()
    opt = load_optout()
    before = len(rows)
    rows = [r for r in rows if not is_optout(opt, *r.values())]
    if before - len(rows):
        print(f"Opt-out : {before - len(rows)} ligne(s) ecartee(s) du CRM", file=sys.stderr)

    fiche = sum(1 for r in rows if r.get("date_enrichi"))
    if not fiche:
        print("! Aucune ligne enrichie : contact, finances, score et liens seront vides.",
              file=sys.stderr)
    elif fiche < len(rows):
        print(f"  {len(rows) - fiche} lignes encore en attente d'enrichissement "
              f"(elles apparaissent avec les colonnes vides).", file=sys.stderr)

    # --- filtres
    if a.types:
        g = {t.lower() for t in a.types}
        rows = [r for r in rows if (r.get("type") or "").lower() in g]
    if a.tier:
        g = {t.upper() for t in a.tier}
        rows = [r for r in rows if (r.get("tier") or "").upper() in g]
    if a.dep:
        g = set(a.dep)
        rows = [r for r in rows if (r.get("departement") or "") in g]
    if a.min_score is not None:
        rows = [r for r in rows if num(r.get("score")) >= a.min_score]
    if not rows:
        sys.exit("Aucune ligne apres filtrage.")

    # --- tri : score decroissant, multi-sites avant, puis nom
    rows.sort(key=lambda r: (-num(r.get("score")),
                             -num(r.get("nb_etablissements_ouverts")),
                             (r.get("nom") or "").upper()))
    if a.limit:
        rows = rows[:a.limit]

    # --- suivi : on relit le CRM principal et les eventuels CRM par verticale
    base_out = os.path.splitext(a.out)[0]
    dossier = os.path.dirname(a.out) or "."
    candidats = [os.path.join(dossier, "suivi.csv"), a.out]
    candidats += [f"{base_out}_{t}.csv" for t in {(r.get("type") or "") for r in rows} if t]
    suivi = {} if a.sans_fusion else relire_suivi(candidats)
    for r in rows:
        anc = suivi.get((r.get("siren") or "").strip(), {})
        for c in SUIVI_COLS:
            r[c] = anc.get(c, "") or ""
        if not r["statut"]:
            r["statut"] = STATUT_DEFAUT
        enc = num(r.get("montant_encaisse"))
        sans = (r.get("produit") or "").strip().lower() in SANS_COMMISSION
        r["commission"] = "0" if (enc and sans) else (f"{enc * TAUX_COMMISSION:.0f}" if enc else "")

    # --- colonnes : schema complet TOUJOURS present (en-tete stable d'un export
    # a l'autre, meme si une colonne est vide partout), ordre voulu d'abord,
    # puis tout ajout futur trouve dans les donnees
    presentes = set(COLS_BASE_ENRICHIE) | set(SUIVI_COLS)
    for r in rows[:200]:
        presentes.update(r.keys())
    cols = [c for c in ORDRE if c in presentes]
    cols += [c for c in sorted(presentes) if c not in cols]

    ko = ecrire(a.out, rows, cols)
    print(f"\nCRM ecrit : {a.out}  ({len(rows)} lignes, {len(cols)} colonnes, {ko:.0f} Ko)",
          file=sys.stderr)

    if a.par_type:
        for t in sorted({(r.get("type") or "") for r in rows}):
            if not t:
                continue
            sub = [r for r in rows if r.get("type") == t]
            p = f"{base_out}_{t}.csv"
            k = ecrire(p, sub, cols)
            print(f"  {p}  ({len(sub)} lignes, {k:.0f} Ko)", file=sys.stderr)

    resume(rows)
    print(f"\nStatuts autorises   : {' | '.join(STATUTS)}", file=sys.stderr)
    print(f"Canaux autorises    : {' | '.join(CANAUX)}", file=sys.stderr)
    print(f"Produits autorises  : {' | '.join(PRODUITS)}", file=sys.stderr)
    print(f"Livraison autorisee : {' | '.join(LIVRAISON)}", file=sys.stderr)
    print(f"commission = montant_encaisse x {TAUX_COMMISSION:.0%}, sauf "
          f"{'/'.join(sorted(SANS_COMMISSION))} (0 %). Recalculee a chaque export.", file=sys.stderr)
    print(f"Genere le {date.today().strftime('%d/%m/%Y')}", file=sys.stderr)


if __name__ == "__main__":
    main()
