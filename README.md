# AskData - Pipeline de prospection intelligent

Un seul script qui construit, chaque semaine et en automatique, une **base de prospects B2B qualifiés et scorés**, prête à charger dans ton outil d'emailing (Brevo). Le tout à partir de sources **100% officielles et gratuites**, et conforme RGPD (cible B2B, régime opt-out).

## Ce que fait le pipeline

1. **Ciblage SIRENE** - via l'API Recherche d'entreprises (data.gouv). Filtre les entreprises par code NAF, région/département, tranche d'effectif et catégorie (PME).
2. **Signaux BODACC** - via l'API DILA. Pour chaque entreprise : détecte le dépôt des comptes, les modifications récentes (capital, dirigeant), la création. Et surtout **exclut les sociétés en difficulté** (procédure collective, liquidation, redressement).
3. **Enrichissement email** en cascade : devine le site depuis le nom et le vérifie, scrappe les emails du site, interroge Hunter et Dropcontact si tu as des clés, et devine `prenom.nom@domaine` à partir du dirigeant. Chaque email est vérifié (MX ou Hunter).
4. **Scoring 0-100** - combine secteur, taille, chiffre d'affaires, croissance, activité BODACC, et surtout la présence d'un email exploitable. Classe chaque prospect en **tier A / B / C**.
5. **Sortie** - un CSV et un JSON triés par score, plus un résumé Markdown. Option : pousse les tiers A/B directement dans une liste Brevo.

## Installation

```bash
pip install -r requirements.txt
python prospect.py
```

Le mode gratuit fonctionne sans aucune clé. Les résultats atterrissent dans `output/`.

## Options en ligne de commande

```bash
python prospect.py                 # run complet (config.yml)
python prospect.py --limit 150     # limite le nombre de candidats enrichis
python prospect.py --no-enrich     # ciblage + scoring seuls (rapide, sans email)
python prospect.py --no-bodacc     # sans les signaux BODACC
python prospect.py --push          # pousse les prospects A/B vers Brevo
```

## Clés API facultatives (améliorent le taux d'emails trouvés)

À définir en variables d'environnement, ou en **secrets GitHub** (Settings -> Secrets and variables -> Actions) :

| Secret | Rôle | Sans la clé |
|---|---|---|
| `HUNTER_API_KEY` | trouve et vérifie les emails par domaine | on scrappe le site + vérif MX |
| `DROPCONTACT_API_KEY` | enrichissement français, orienté RGPD | ignoré |
| `BREVO_API_KEY` | pousse les contacts dans Brevo | export CSV seulement |

## Configuration (`config.yml`)

Tout se règle sans toucher au code : les **codes NAF** visés, la **zone** (région 84 = Auvergne-Rhône-Alpes, ou une liste de départements), les **tranches d'effectif**, les **poids du scoring**, la fourchette de **CA idéale**, et les seuils des tiers. Le fichier est commenté.

## Automatisation GitHub Actions

Le workflow `.github/workflows/prospect.yml` :
- tourne **tous les lundis à 6h** (et à la demande via "Run workflow"),
- publie les résultats en **artefact téléchargeable** (onglet Actions),
- met à jour `data/seen.csv` pour **ne jamais ressortir deux fois** la même entreprise.

Pour le lancer à la main : onglet **Actions** -> "Prospection AskData" -> **Run workflow** (tu peux régler la limite et activer le push Brevo).

## Sortie

`output/prospects_AAAA-MM-JJ.csv` (ouvre-le dans Excel), avec pour chaque prospect : score, tier, SIREN, nom, NAF, effectif, CA, ville, dirigeant, domaine, **meilleur email**, statut de l'email, signaux BODACC, et la source de la donnée. Plus un `resume_AAAA-MM-JJ.md` avec le top 15.

## Dédup et opt-out

- `data/seen.csv` : les SIREN déjà sortis (rempli automatiquement, pour ne pas les reproposer).
- `data/suppression.csv` : ta **liste d'opposition RGPD**. Ajoute un SIREN ou un email dans ce fichier (colonnes `siren,email,raison,date`) et il ne sera plus jamais contacté. C'est ici que tu reportes chaque désinscription.

## Conformité RGPD (à lire une fois)

Le cold email **B2B est légal en France** sans consentement préalable (régime opt-out), à quatre conditions que ce pipeline respecte ou prépare :
1. **Email professionnel** et message **en lien avec la fonction** du destinataire (ta cible, ce sont des dirigeants de PME, sur un sujet pro).
2. **Source tracée** : chaque ligne indique d'où vient la donnée (SIRENE, BODACC, site web). Garde ce CSV.
3. **Identité claire de l'expéditeur** + **lien de désinscription en 1 clic** : à mettre dans ton email (côté Brevo).
4. **Opt-out respecté** : chaque désinscription va dans `data/suppression.csv`, et les données des prospects inactifs se purgent au bout de **3 ans**.

Pense aussi à rédiger une courte **LIA** (analyse d'intérêt légitime, 1-2 pages) et à la garder dans ton registre des traitements. Un modèle minimal :

> Intérêt poursuivi : développer l'activité d'AskData en proposant un service de BI aux PME.
> Nécessité : la prospection par email pro est le moyen le plus proportionné pour toucher des dirigeants.
> Mise en balance : données strictement professionnelles, issues de sources publiques, message pertinent, opt-out immédiat, conservation limitée à 3 ans.
> Mesures : liste d'opposition, traçabilité des sources, désinscription en 1 clic.

## Robustesse et limites honnêtes

- **Auto-détection du paramètre NAF** : au démarrage, le script teste `activite_principale` et `code_naf` et garde celui qui répond. Si l'API évolue, le ciblage continue de fonctionner.
- **Confiance des emails** : chaque email est étiqueté par sa source (`site`, `hunter`, `dropcontact` = fiable ; `devine` = deviné). Un email seulement deviné ne compte quasiment pas dans le score et **n'est jamais poussé vers Brevo**, pour protéger ta réputation d'expéditeur. Contacte les emails devinés à la main, avec prudence.
- **Devinette de domaine** (mode gratuit) : ne trouve pas 100% des sites, surtout pour les noms génériques. Les clés Hunter/Dropcontact augmentent nettement le taux. Le domaine n'est retenu que si la page mentionne le nom de l'entreprise ou son SIREN (évite les faux domaines).
- **NAF 2025** : la bascule officielle des codes APE vers NAF 2025 a lieu le **1er janvier 2027**. D'ici là, les codes NAF rév.2 de `config.yml` (62.01Z, etc.) sont valides. Après, il faudra les mettre à jour avec la table de correspondance de l'INSEE.
- **Effectif** : filtrer par tranche d'effectif exclut les ~50% d'entreprises sans effectif renseigné. Pour élargir, vide `tranche_effectif` dans `config.yml` (le scoring gère alors la taille).
- **Quotas API** : SIRENE ~7 requêtes/seconde, BODACC limité côté anonyme. Le script gère les erreurs 429 avec des pauses ; garde `candidats_max` autour de 300-400 par run.
- **CA** : disponible seulement pour les sociétés qui déposent leurs comptes ; sinon le score reste neutre sur ce critère.

## Enchaînement avec ton envoi

Le CSV se branche directement sur ton pipeline d'emailing existant (Brevo + GitHub Actions). Deux options : soit `--push` pour créer les contacts Brevo automatiquement, soit tu importes le CSV dans une liste Brevo et tu lances ta séquence habituelle.
