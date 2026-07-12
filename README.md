# AskData - Pipeline de prospection intelligent

Un seul script qui construit, chaque semaine et en automatique, une **base de prospects B2B qualifiés et scorés** (avec emails, téléphone et LinkedIn quand ils sont trouvés). À partir de sources **100% officielles et gratuites**, sans aucune clé API, et conforme RGPD (cible B2B, régime opt-out).

## Ce que fait le pipeline

1. **Ciblage SIRENE** - via l'API Recherche d'entreprises (data.gouv). Filtre par code NAF, région/département, tranche d'effectif. Récupère nom, dirigeant, CA, effectif, adresse.
2. **Signaux BODACC** - via l'API DILA. Détecte dépôt des comptes, modifications, création, et **exclut les sociétés en difficulté** (liquidation, redressement). Chaque annonce est vérifiée par SIREN, donc une entreprise n'est jamais exclue à cause de la procédure d'une AUTRE société.
3. **Enrichissement maximal (gratuit)** :
   - devine le domaine du site depuis le nom, PUIS le cherche sur le web (DuckDuckGo) si la devinette échoue,
   - scrappe le site : pages standard + liens contact/mentions/à-propos découverts sur l'accueil,
   - décode les emails cachés (Cloudflare, entités HTML, "nom [at] domaine [point] fr"),
   - devine les emails de rôle (contact@, info@) et du dirigeant (prenom.nom@) en secours,
   - récupère le **téléphone** et le **LinkedIn** de l'entreprise.
4. **Scoring 0-100** - secteur, taille, CA, croissance, activité BODACC, présence d'un email exploitable. Classe en **tier A / B / C**.
5. **Sortie** - un CSV et un JSON triés par score, plus un résumé Markdown.

Les clés Hunter / Dropcontact sont **facultatives** (variables d'environnement) et améliorent encore la récupération d'emails, mais tout fonctionne sans aucune clé.

## Installation

```bash
pip install -r requirements.txt
python prospect.py
```

Les résultats atterrissent dans `output/`.

## Options en ligne de commande

```bash
python prospect.py                 # run complet (config.yml)
python prospect.py --limit 150     # limite le nombre de candidats enrichis
python prospect.py --no-enrich     # ciblage + scoring seuls (rapide, sans email)
python prospect.py --no-bodacc     # sans les signaux BODACC
```

## Configuration (`config.yml`)

Tout se règle sans toucher au code : les **codes NAF** visés, la **zone** (région 84 = Auvergne-Rhône-Alpes, ou une liste de départements), les **tranches d'effectif**, les poids du **scoring**, la fourchette de **CA idéale**, les pages scannées, et le nombre de **threads** (`workers`). Le fichier est commenté.

## Automatisation GitHub Actions

Le workflow `.github/workflows/prospect.yml` :
- tourne **tous les lundis à 6h** (et à la demande via "Run workflow", avec un réglage de la limite),
- **enregistre les résultats directement dans le repo** : `output/prospects_AAAA-MM-JJ.csv` et le résumé sont commités à chaque run (le JSON reste local). Une copie est aussi téléchargeable dans l'onglet Actions.
- met à jour `data/seen.csv` pour **ne jamais ressortir deux fois** la même entreprise.

> **Important - repo privé obligatoire.** Les résultats (emails de prospects) sont enregistrés dans le repo : il doit rester **privé** (sinon des données personnelles seraient publiques, ce qui est interdit par le RGPD). Pense aussi à supprimer régulièrement les vieux fichiers `output/` (la donnée reste sinon dans l'historique Git).

## Sortie (colonnes du CSV)

`score`, `tier`, `siren`, `nom`, `naf`, `naf_label`, `effectif`, `categorie`, `date_creation`, `ca`, `ca_prev`, `ville`, `cp`, `dirigeant`, `qualite`, `domain`, `best_email`, `email_source` (`site`/`hunter`/`dropcontact` = fiable, `devine` = deviné), `email_status`, `telephone`, `linkedin`, `autres_emails`, `signaux_bodacc`, `raisons`, `source`, `date_ajout`. Plus un `resume_AAAA-MM-JJ.md` avec le top 15.

## Dédup et opt-out

- `data/seen.csv` : les SIREN déjà sortis (rempli automatiquement).
- `data/suppression.csv` : ta **liste d'opposition RGPD**. Ajoute un SIREN ou un email (colonnes `siren,email,raison,date`) et il ne sera plus jamais contacté. Reporte ici chaque désinscription.

## Conformité RGPD (à lire une fois)

Le cold email **B2B est légal en France** sans consentement préalable (régime opt-out), à quatre conditions que ce pipeline respecte ou prépare :
1. **Email professionnel** et message **en lien avec la fonction** du destinataire.
2. **Source tracée** : chaque ligne indique d'où vient la donnée. Garde ce CSV.
3. **Identité claire de l'expéditeur** + **lien de désinscription en 1 clic** : à mettre dans ton email (dans ton outil d'emailing).
4. **Opt-out respecté** : chaque désinscription va dans `data/suppression.csv`, et les données des prospects inactifs se purgent au bout de **3 ans**.

Garde aussi une courte **LIA** (analyse d'intérêt légitime) dans ton registre des traitements. Modèle minimal :

> Intérêt : développer AskData en proposant un service de BI aux PME.
> Nécessité : la prospection par email pro est le moyen le plus proportionné pour toucher des dirigeants.
> Mise en balance : données strictement professionnelles, sources publiques, message pertinent, opt-out immédiat, conservation limitée à 3 ans.
> Mesures : liste d'opposition, traçabilité des sources, désinscription en 1 clic.

## Robustesse et limites honnêtes

- **Auto-détection du paramètre NAF** : le script teste `activite_principale` et `code_naf` au démarrage et garde celui qui répond.
- **BODACC fiable** : filtre strict par SIREN, donc plus d'exclusion par contamination d'une autre société.
- **Emails d'exemple filtrés** : `your@email.com`, `name@example.com`, `test@...` sont écartés.
- **Confiance des emails** : les emails `site`/`hunter`/`dropcontact` sont fiables ; les `devine` (contact@ ou prenom.nom@ devinés) sont à confirmer avant un envoi de masse - contacte-les à la main d'abord.
- **Recherche web (DuckDuckGo)** : améliore la découverte des sites, mais peut être limitée par moments ; en cas de blocage, le script continue sans (pas de plantage).
- **Vitesse** : enrichissement en parallèle (`workers` dans `config.yml`) ; un run de 300 prend environ 10 minutes.
- **Effectif** : filtrer par tranche d'effectif exclut les ~50% d'entreprises sans effectif renseigné. Vide `tranche_effectif` pour élargir.
- **Quotas API** : SIRENE ~7 req/s, BODACC limité côté anonyme. Le script gère les 429 ; garde `candidats_max` autour de 300-400 par run.
- **NAF 2025** : la bascule officielle des codes APE est au 1er janvier 2027 ; d'ici là les codes NAF rév.2 de `config.yml` sont valides.

## Envoi

Le CSV s'importe dans l'outil d'emailing de ton choix. Chaque ligne contient l'email (avec sa source et son statut) et, quand ils sont trouvés, le téléphone et le LinkedIn de l'entreprise - autant de canaux de repli si l'email manque.

## Piste v2 (plus tard)

Une fois la prospection validée : un **scoring d'intention** (offres d'emploi "data analyst", stack e-commerce détectée, croissance d'effectif) pour prioriser encore mieux. Plus lourd à fiabiliser, à garder pour quand le socle actuel aura fait ses preuves.
