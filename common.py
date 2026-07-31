# -*- coding: utf-8 -*-
"""Helpers partages du pipeline de prospection AskData.

Contient tout ce qui est utilise par plusieurs scripts : chargement de la
config, table des libelles NAF, normalisation (noms, telephones, SIRET), et
le moteur d'OPT-OUT (suppression RGPD) qui est ici volontairement partage
pour qu'aucun script ne puisse l'oublier.
"""
import csv
import os
import re
import unicodedata

import yaml

USER_AGENT = "prospection-askdata/2.0 (base B2B AskData; +voir mentions legales)"

# Colonnes de la base brute produite par sirene_api.py.
BASE_COLS = ["siren", "siret", "nom", "type", "naf", "libelle",
             "commune", "departement", "telephone", "source"]


# ---------------------------------------------------------------------------
#  Libelles NAF officiels
#  L'API recherche-entreprises ne renvoie PAS le libelle dans sa reponse de
#  recherche : on le remplit donc localement depuis le code NAF.
# ---------------------------------------------------------------------------
NAF_LABELS = {
    "45.11Z": "Commerce de voitures et de vehicules automobiles legers",
    "46.19B": "Autres intermediaires du commerce en produits divers",
    "46.51Z": "Commerce de gros d'ordinateurs, d'equipements informatiques peripheriques et de logiciels",
    "46.90Z": "Commerce de gros non specialise",
    "47.41Z": "Commerce de detail d'ordinateurs, d'unites peripheriques et de logiciels en magasin specialise",
    "47.91A": "Vente a distance sur catalogue general",
    "47.91B": "Vente a distance sur catalogue specialise",
    "49.41A": "Transports routiers de fret interurbains",
    "52.29A": "Messagerie, fret express",
    "58.29C": "Edition de logiciels applicatifs",
    "62.01Z": "Programmation informatique",
    "62.02A": "Conseil en systemes et logiciels informatiques",
    "62.03Z": "Gestion d'installations informatiques",
    "62.09Z": "Autres activites informatiques",
    "63.11Z": "Traitement de donnees, hebergement et activites connexes",
    "63.12Z": "Portails Internet",
    "63.99Z": "Autres services d'information n.c.a.",
    "66.19B": "Autres activites auxiliaires de services financiers, hors assurance et caisses de retraite",
    "66.22Z": "Activites des agents et courtiers d'assurances",
    "68.31Z": "Agences immobilieres",
    "69.20Z": "Activites comptables",
    "70.10Z": "Activites des sieges sociaux",
    "70.21Z": "Conseil en relations publiques et communication",
    "70.22Z": "Conseil pour les affaires et autres conseils de gestion",
    "71.12B": "Ingenierie, etudes techniques",
    "72.19Z": "Recherche-developpement en autres sciences physiques et naturelles",
    "73.11Z": "Activites des agences de publicite",
    "73.12Z": "Regie publicitaire de medias",
    "73.20Z": "Etudes de marche et sondages",
    "78.10Z": "Activites des agences de placement de main-d'oeuvre",
    "82.20Z": "Activites de centres d'appels",
}


def naf_label(code):
    """Libelle officiel d'un code NAF ('62.01Z' ou '6201Z'), '' si inconnu."""
    if not code:
        return ""
    c = str(code).upper().replace(".", "")
    if len(c) >= 5:
        c = f"{c[:2]}.{c[2:5]}"
    return NAF_LABELS.get(c, "")


def load_config(path="config.yml"):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def naf_type_map(cfg):
    """{NAF_sans_point_majuscule: type}. Ex: {'6201Z': 'edition_saas'}."""
    m = {}
    for typ, codes in (cfg.get("naf") or {}).items():
        for code in codes:
            m[code.replace(".", "").upper()] = typ
    return m


def effectif_groups(cfg):
    """Regroupe les NAF par profil de tranches d'effectif salarie (INSEE).

    Lit config.yml :
      effectifs:
        defaut: ["11", ...]            # tranches appliquees a tous les types
        par_type:
          ecommerce: ["03", ...]       # exceptions par type

    Renvoie une liste triee de tuples (tranches, nafs). Un tuple de tranches
    vide = aucun filtre effectif (on prend tout, le scoring gere la taille).
    """
    eff = cfg.get("effectifs") or {}
    defaut = tuple(str(t).zfill(2) for t in (eff.get("defaut") or []))
    par_type = eff.get("par_type") or {}
    groups = {}
    for typ, codes in (cfg.get("naf") or {}).items():
        tr = tuple(str(t).zfill(2) for t in (par_type[typ] or [])) if typ in par_type else defaut
        groups.setdefault(tr, []).extend(codes)
    return sorted(groups.items())


def all_departements():
    """Tous les departements FR (metropole hors '20' -> 2A/2B, + DROM)."""
    deps = [f"{i:02d}" for i in range(1, 96) if i != 20]
    deps += ["2A", "2B", "971", "972", "973", "974", "976"]
    return deps


# ---------------------------------------------------------------------------
#  OPT-OUT (suppression RGPD)
#  Une seule source de verite : data/optout.csv, colonnes siren / domain /
#  email. Tout script qui produit ou reecrit une ligne passe par is_optout().
#  Consequence : une entreprise ajoutee ici disparait de la base au run
#  suivant, sans intervention manuelle et sans modification de code.
# ---------------------------------------------------------------------------
OPTOUT_PATH = os.path.join("data", "optout.csv")
OPTOUT_COLS = ["siren", "domain", "email", "raison", "date"]


def load_optout(path=OPTOUT_PATH):
    """Lit data/optout.csv -> {'siren': set, 'domain': set, 'email': set}."""
    out = {"siren": set(), "domain": set(), "email": set()}
    if not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            for col in out:
                v = (row.get(col) or "").strip().lower()
                if v:
                    out[col].add(v)
    return out


def is_optout(opt, *values):
    """True si l'un des champs fournis contient un siren/domaine/email exclu.

    On teste sur la concatenation des champs, pas sur un champ precis : un
    domaine en opt-out est ainsi attrape meme s'il apparait dans une colonne
    inattendue (email scrappe, URL LinkedIn, autres_emails...).
    """
    if not (opt["siren"] or opt["domain"] or opt["email"]):
        return False
    blob = " ".join(str(v or "") for v in values).lower()
    if not blob.strip():
        return False
    for key in ("siren", "domain", "email"):
        for needle in opt[key]:
            if needle in blob:
                return True
    return False


def ensure_optout_file(path=OPTOUT_PATH):
    """Cree data/optout.csv avec son en-tete s'il n'existe pas."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(OPTOUT_COLS)


# ---------------------------------------------------------------------------
#  Normalisation
# ---------------------------------------------------------------------------
def strip_acc(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    return "".join(c for c in s if unicodedata.category(c) != "Mn").lower().strip()


_LEGAL = (" sasu", " sas", " sarl", " eurl", " sa", " sci", " scm", " selarl",
          " selas", " snc", " scop", " association", " asso", " groupe", " ste",
          " societe", " holding", " france", " group")


def slugify(name):
    """Nom d'entreprise -> radical de domaine plausible (forme juridique retiree)."""
    s = strip_acc(name)
    for suf in _LEGAL:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return re.sub(r"[^a-z0-9]+", "", s)


def norm_phone(s):
    """Normalise un numero FR au format '0X XX XX XX XX', sinon ''."""
    if not s:
        return ""
    d = re.sub(r"\D", "", str(s))
    if d.startswith("0033"):
        d = "0" + d[4:]
    elif d.startswith("33") and len(d) == 11:
        d = "0" + d[2:]
    if len(d) == 10 and d[0] == "0":
        return " ".join([d[0:2], d[2:4], d[4:6], d[6:8], d[8:10]])
    return ""


def clean_siret(raw):
    d = "".join(c for c in str(raw or "") if c.isdigit())
    return d if len(d) == 14 else ""


def siren_of(siret, fallback=""):
    s = clean_siret(siret)
    if s:
        return s[:9]
    d = "".join(c for c in str(fallback or "") if c.isdigit())
    return d[:9] if len(d) >= 9 else ""
