# Prospection AskData

Base de prospection des TPE/PME francaises qui ont de la donnee a exploiter
mais pas de profil data en interne : editeurs, ESN, e-commerce, comptables,
agences, courtiers... Ce sont les acheteurs potentiels d'AskData (BI en
langage naturel).

Meme architecture que le pipeline tournees : la base se construit toute seule
(workflow mensuel), l'enrichissement tourne chaque nuit avec reprise
automatique, le CRM se regenere chaque lundi, tout se pilote depuis l'onglet
Actions de GitHub. Aucune app, que du CSV lisible.

## Cibles (31 codes NAF, 10 verticales)

| Verticale (`--types`) | Codes NAF | Qui c'est |
|---|---|---|
| saas_web | 62.01Z, 58.29C, 63.12Z | Editeurs, SaaS, portails internet |
| conseil_it | 62.02A, 62.03Z, 62.09Z | ESN, infogerance, services IT |
| data_etudes | 63.11Z, 63.99Z, 73.20Z, 72.19Z | Data, hebergement, etudes de marche, R&D |
| conseil_gestion | 70.22Z, 70.10Z, 71.12B | Conseil en gestion, sieges, ingenierie |
| marketing_com | 73.11Z, 73.12Z, 70.21Z | Agences de pub, regies, RP |
| comptabilite | 69.20Z | Experts-comptables |
| ecommerce_negoce | 47.91A, 47.91B, 46.19B, 46.90Z, 46.51Z, 47.41Z, 45.11Z | E-commerce, negoce, distribution |
| logistique | 49.41A, 52.29A | Fret interurbain, messagerie express |
| immobilier | 68.31Z | Agences immobilieres |
| finance_rh | 66.22Z, 66.19B, 78.10Z, 82.20Z | Courtiers, aux. financiers, recrutement, centres d'appels |

Filtre effectifs (unite legale) : 3 a 499 salaries. Etablissements actifs
uniquement. France entiere. Tout se regle dans `config.yml`.

## Regle emails (politique de donnees)

- UNIQUEMENT des adresses GENERIQUES publiees sur le site officiel de
  l'entreprise (contact@, info@, commercial@...).
- AUCUNE adresse nominative (prenom.nom@) n'est devinee ni construite :
  donnee personnelle.
- Le nom du dirigeant vient du registre officiel public (annuaire des
  entreprises) et sert uniquement a personnaliser le message.
- Verification MX du domaine avant tout envoi.

## Votre entreprise figure dans cette base ?

Ce depot est public. Si vous souhaitez retirer votre entreprise : ouvrez une
issue avec votre SIREN ou votre domaine. La ligne sera ajoutee a
`data/optout.csv` et la fiche disparaitra de la base, du CRM et de toute
sortie future des le run suivant. Aucune justification demandee. Les emails
collectes sont exclusivement des adresses generiques publiees sur votre
propre site (jamais d'adresse nominative construite).

## Opt-out RGPD : `data/optout.csv`

Une seule source de verite pour les demandes de suppression. Colonnes :
`siren`, `domain`, `email` (une seule suffit), `raison`, `date`.

Ce qu'une ligne declenche, sans toucher au code :
1. `sirene_api.py` n'integre jamais l'entreprise a la base ;
2. `enrich.py` PURGE retroactivement la base enrichie a chaque run et ne
   traite jamais la ligne ;
3. `export_crm.py` l'ecarte de tous les CRM, meme si une vieille base
   trainait encore.

Exemple :

    siren,domain,email,raison,date
    429033913,netcracker.com,,demande de suppression (issue GitHub #1),2026-07-31

## Workflows GitHub Actions

| Workflow | Quand | Ce qu'il fait |
|---|---|---|
| Build base askdata (`refresh.yml`) | 1er du mois, 03:00 UTC | Collecte SIRENE -> `data/base_askdata.csv` (commit sur main) |
| Enrichissement (`enrich.yml`) | chaque nuit, 01:00 UTC | Purge opt-out, fiche officielle, contacts web -> branche `enrichi` (gz) + artefact |
| CRM prospection (`crm.yml`) | lundi, 06:30 UTC | `crm/crm_askdata.csv` + un CSV par verticale (commit sur main) |

Prerequis une seule fois : Settings > Actions > General > Workflow
permissions > "Read and write permissions".

Premier lancement : Actions > "Build base askdata" > Run workflow, puis
laisser l'enrichissement nocturne tourner (il s'arrete tout seul quand tout
est fait), puis "CRM prospection".

## Donnees produites (69 colonnes)

10 colonnes de base (siren, siret, nom, type/verticale, naf, libelle,
commune, departement, telephone, source) + 59 colonnes d'enrichissement :

- Structure : effectif + annee de la donnee, categorie (PME/ETI/GE), nature
  juridique, sigle, nom commercial / enseignes, date de creation, anciennete,
  caractere employeur, nb d'etablissements ET nb d'etablissements OUVERTS,
  communes des autres etablissements (multi-sites = donnees eclatees = besoin
  BI, bonus de score).
- Finances INPI : CA, CA n-1, resultat net, annee d'exercice.
- Contact : adresse, CP, ville, REGION, latitude/longitude, telephone, TVA
  intra, domaine, email generique du site, statut MX, autres emails, flag
  email perso.
- Dirigeants : la premiere PERSONNE PHYSIQUE du registre (pas la holding) +
  fonction + 4 autres. Registre officiel public, jamais d'email construit.
- Conformite : drapeaux entrepreneur_individuel et diffusion_protegee
  (statut INSEE "P") pour ecarter ces lignes du cold email de masse : ce sont
  des donnees de personnes physiques.
- Labels officiels + convention collective (IDCC).
- 14 liens prets a cliquer par ligne : site officiel, Pappers, Annuaire des
  Entreprises + ses onglets annonces BODACC / donnees financieres /
  dirigeants, societe.com, Pages Jaunes, Google Maps, recherche Google,
  Google News (actualite de la boite avant un call), recherche LinkedIn
  entreprise ET dirigeant, Trustpilot. Plus la page LinkedIn officielle
  quand elle est trouvee sur le site.
- Signaux BODACC (procedures collectives) en option.
- Score 0-100 + tier A/B/C + raisons detaillees.

## CRM

`export_crm.py` ajoute 12 colonnes de suivi (statut, canal, relance, notes,
produit, montants, commission closer a 40 %...) et conserve tes annotations a
chaque regeneration (fusion par SIREN). Statuts : a contacter, contacte,
relance, interesse, rdv, client, refuse, injoignable, hors cible.
Produits : POC, Abonnement, Deploiement, Formation (la formation ne
commissionne pas).

NB depot public : les colonnes de suivi committees dans `crm/` (statuts,
notes, montants, commissions) sont visibles de tous. Garde les notes neutres,
ou travaille le suivi dans une copie locale du CSV.

## Fichiers

- `config.yml` : verticales NAF, effectifs, enrichissement, scoring.
- `common.py` : helpers partages + moteur d'opt-out.
- `sirene_api.py` : collecte via l'API recherche-entreprises (aucune cle).
- `enrich.py` : enrichissement complet avec reprise automatique + purge opt-out.
- `export_crm.py` : CRM CSV avec suivi conserve.
- `data/optout.csv` : demandes de suppression (siren / domaine / email).

## Usage local (plus rapide que GitHub pour le web)

    pip install -r requirements.txt
    python sirene_api.py --departements 69          # tester sur le Rhone
    python enrich.py --limit 200                    # fiche + web
    python enrich.py --stats                        # avancement
    python export_crm.py --par-type                 # CRM + un CSV par verticale

## Migration depuis l'ancien pipeline (prospect.py)

A supprimer du repo : `prospect.py`, `.github/workflows/prospect.yml`, le
dossier `output/` entier, `data/seen.csv`, `data/suppression.csv`.
Pourquoi : `output/` contient des emails nominatifs devines par l'ancien
code (donnee personnelle) ; la dedup `seen.csv` n'a plus de sens (la base
est desormais complete et enrichie une seule fois, la reprise s'en charge) ;
`suppression.csv` est remplace par `data/optout.csv` (deja pre-rempli).
L'historique Git conserve les anciens fichiers : passer le repo en prive
regle le passe et le present d'un coup.
