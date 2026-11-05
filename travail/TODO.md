# TODO — SAÉ 5 · Observabilité et diagnostic d'incidents

> Fichier de pilotage des tâches à faire. Il fusionne le suivi global (`TODO-SAE5.md` à la racine) et les restes à compléter de l'inventaire (`notes/metriques-observabilite.md`).
> Ce qui est déjà fait est consigné dans [`avancement.md`](avancement.md).
> Légende : ⬜ à faire · 🟡 en cours · 🎁 bonus / facultatif

---

## Table des matières

1. [Phase 2 — Terminer l'état des lieux ✅](#1-phase-2--terminer-létat-des-lieux-✅)
2. [Phase 3 — Conception de l'outil de diagnostic](#2-phase-3--conception-de-loutil-de-diagnostic) (matrice ✅, automatisation ✅)
3. [Phase 4 — Automatisation des essais](#3-phase-4--automatisation-des-essais)
4. [Phase 5 — Vérification croisée ✅](#4-phase-5--vérification-croisée-✅)
5. [Phase 6 — Livrables et preuves](#5-phase-6--livrables-et-preuves)
6. [Phase 7 — Extension (si temps)](#6-phase-7--extension-si-temps)
7. [Phase 8 — Dimension théorique](#7-phase-8--dimension-théorique)
8. [Phase 9 — Rendu GitHub](#8-phase-9--rendu-github)
9. [Phase 0/1 — Restes d'organisation](#9-phase-01--restes-dorganisation)
10. [Règles à respecter pendant l'exécution](#10-règles-à-respecter-pendant-lexécution)

---

## 1. Phase 2 — Terminer l'état des lieux ✅

**✅ FAIT (pull 04/11/2026)** — l'inventaire `notes/metriques-observabilite.md` est complet :

- **§1 Météo** : sondes, compteurs (`persisted_*` vs `db_*`), écarts/durées, Kafka, alertes, ce qui manque ;
- **§2 Dashboards Grafana** : chaîne d'alimentation (collecteur → InfluxDB), les 4 dashboards panneau par panneau (`incidents`, `kafka`, `persistence`, `pipeline`), ce qui manque (aucun ne croise les signaux) ;
- **§3 `tp-kafka.sh`** : topics, facteur de réplication, les 5 consumer groups, lecture par application, apports/limites ;
- **§4 `tp-db.sh`** : les 2 tables, motifs de refus (`processing_timeout` = aide au diagnostic), apports/limites ;
- **Synthèse** : tableau des 4 sources (quelles sont les vérifications indépendantes : `tp-kafka.sh` et `tp-db.sh` **oui**, météo/dashboards non), ambiguïtés observées entre signaux, ce qui manque pour la mission (3 règles à créer + fraîcheur + témoin automatisé).

Les enseignements clés pour la suite :

- **Vérifications indépendantes** = `tp-kafka.sh` (Kafka en direct) + `tp-db.sh` (PostgreSQL en direct) — la météo et les dashboards lisent les mêmes sources qu'eux-mêmes ;
- **3 ambiguïtés à lever** (matière première de la Phase 3) : retard consommateur ↔ persistance qui se ressemblent ; réplication dégradée ↔ panne de cluster ; alertes indépendantes sans « cause probable » ni « vérification suivante » ;
- **Le verdict « indéterminé » n'existe pas** dans le socle — c'est notre contribution.

---

## 2. Phase 3 — Conception de l'outil de diagnostic

**Objectif : concevoir l'apport concret à partir de la matrice (livrables de preuve attendus).**
**Garantie visée** (candidate #1, retenue) : « Notre lecture d'incident ne doit pas confondre retard de consommateur, réplication dégradée et panne de persistance, prouvé par une matrice validée sur incidents injectés : bonne cause retenue sur 100 % des cas connus, état "indéterminé" + vérification suivante sinon. »
**Architecture cible** : voir [`docs/architecture/plan-architecture-sae5.md`](../docs/architecture/plan-architecture-sae5.md) §4 (moteur de diagnostic : garde-fou fraîcheur → règles de croisement → sortie à 4 états).

### 2.1 Matrice symptôme / cause probable / vérification ✅

**✅ FAIT (pull 04/11/2026, commit `05c67db`)** — [`notes/matrice-symptomes-causes.md`](notes/matrice-symptomes-causes.md) (v1) :

- **5 lignes de diagnostic** (symptôme vu en premier → 2 causes plausibles → comment trancher → cas indéterminé) :
  1. file d'attente qui grossit → retard de lecture vs persistance (trancher via `tp-kafka.sh` : quel groupe est en retard ?) ;
  2. URP > 0 → broker arrêté (grave) vs retard de copie (bénin) (trancher via `offline_partitions_count` / `active_controller_count`) ;
  3. plus aucune écriture en base → PostgreSQL indisponible vs amont arrêté (trancher via `counts.generated` vs `seconds_since_last_validated`) ;
  4. paiements trop lents (`sla_breaches`) → retard en amont vs délai trop court (trancher via lag par application + longueur de file) ;
  5. verdict `degraded` → service down vs alerte grave (trancher via `services.<nom>` + sonde indépendante).
- **Témoin nominal** intégré (contrôle des faux positifs, référence 13/13).
- **Procédure d'usage pendant un incident** en 4 pas (verdict → symptôme → ligne de la matrice → cause nommée ou « indéterminé »).
- **Classement probabilité / criticité** (ajout local 04/11/2026) :
  - échelles qualitatives (Probabilité {Élevée, Moyenne, Faible} × Criticité {Critique, Élevée, Moyenne, Faible}) ;
  - tableau ligne par ligne : les 10 causes notées avec **priorité d'examen** et justification fondée sur les mesures (rattrapage éclair, fin de scénario, TTL 10 s, RF=3/min ISR=2) ;
  - règle de lecture : écarter d'abord la cause la plus **critique** si sa vérification est rapide (`tp-kafka.sh` / `tp-db.sh` = instantanés), puis confirmer la cause la plus **probable** ;
  - **top 3 à surveiller** : PostgreSQL indisponible (seule cause critique) > broker arrêté (near-miss à min ISR=2) > retard de lecture qui dure (fréquent, impact SLA si > 30 s).

**⚠️ La matrice est MANUELLE** : la contribution attendue est de la rendre **automatique** (voir 2.2).
**⚠️ Probabilités = hypothèses de travail** : la Phase 4 les transformera en fréquences mesurées.

### 2.2 Automatiser la matrice (le cœur de la contribution) ✅

Apport choisi : **commande de diagnostic** (`scripts/diagnose.sh`) — moteur pur testable
(`scripts/lib/diagnose_engine.py`), CLI d'état (`scripts/lib/diagnose_cli.py`, cache
`runtime/diagnostic-cache.json`), conforme au plan d'architecture §6.

- ✅ Concevoir l'apport concret (au moins **un** parmi) :
  - ~~une métrique dérivée~~ (partie du travail du socle `service-health`, déjà présente) ;
  - ~~une alerte~~ (le socle alerte déjà sur `down`/`degraded`/lag : le moteur relie au lieu de doubler) ;
  - ~~une vue (dashboard/HTTP)~~ (différée : voir note plus bas) ;
  - ✅ une **commande de diagnostic** : `scripts/diagnose.sh` (`--json`, `--watch N`, `--single`, `--reset`).

  Exigences du minimum :
  - ✅ couvrir les situations **A, B et C** (lignes 1/3/5 de la matrice, dont l'indisponibilité
    PostgreSQL : `services.postgres down`, `details.postgres.status` dégradé, `db_*` figé ;
    et la dégradation : `persister_slow` via l'écart de persistance qui S'ACCROÎT) ;
  - ✅ distinguer **automatiquement** au moins **2 causes plausibles d'un même symptôme**
    : sortie `plausible_causes[]` (chaque cause = id/label/probabilité/criticité/priorité
    d'examen/status `confirmed|probable|excluded|hypothesis`), discriminants de la
    matrice codés (URP+offline/broker_count ; lag persisters + écart croissant vs lag
    lecteur ; file vs latence>SLA ; réseau actif vs service down) ;
  - ✅ si les données ne permettent pas de trancher, état **« indéterminé »** + vérification
    suivante (`indeterminate_reason` ∈ {needs_second_sample, probes_stale, data_missing,
    contradictory_signals, ambiguous_sla, ambiguous_non_dominated, probe_blinded,
    service_down_transversal} + `next_check[]` avec commandes concrètes).
- ✅ Intégrer le **garde-fou fraîcheur** (critère #6) : sortie `freshness` (âge du payload
  ET de l'instantané réplication affichés, limite 75 s) ; sondes périmées →
  `indeterminate/probes_stale`, jamais une conclusion (test `test_stale_probes_never_conclude…`).
  *Calibrage : seuil 75 s sur cadence collecteur mesurée ~20 s — à re-vérifier en phase 4.*
- ✅ Prévoir une **vérification indépendante** de la cause : chaque règle produit des
  `evidence[]` à double lecture (`source` = kafka / postgres / platform) et `next_check`
  pointe `tp-kafka.sh` / `tp-db.sh` + re-lecture `diagnose.sh --watch` (jamais un dashboard seul).
- ✅ Tester la machine à états par un **test unitaire** (critère #5) :
  `cd scripts/lib && python3 -m unittest test_diagnose_engine` — **26 tests OK**
  (chaque ligne × 2 causes, témoin nominal : pas de faux positif éclair/rattrapage/idle,
  probes_stale, data_missing, reset de l'environnement, ordres de priorité, sérialisation).
- ✅ Codifier les règles de la matrice v1 : les 5 lignes sont des règles de croisement ;
  toute conclusion sur un compteur en hausse exige **2 échantillons espacés ≥ 10 s**
  (anti « lag éclair » nominal) ; discriminateur « l'écart qui S'ACCROÎT » (≥ 5 entre 2
  lectures) pour la persistance lente ; rattrapage (écart décroissant) = healthy noté.
- ✅ Reporter le **classement probabilité / criticité** dans la sortie : champs
  `probability`/`criticality` de la cause dominante + `plausible_causes[]` dans l'ordre
  « priorité d'examen » de la matrice (vérification critique rapide d'abord, ex :
  broker arrêté vérifié avant de conclure au retard de copie).

**Note architecture** : l'intégration optionnelle côté `service-health` (vue HTTP/panneau)
est **différée après la campagne d'essais phase 4** — le moteur ne modifie PAS le socle
(aucun rebuild d'image nécessaire pendant la boucle d'essais). La suite phase 4 :
`scripts/diagnostic-campaign.sh` (enchaîner incidents + témoin + relevés).

---

## 3. Phase 4 — Automatisation des essais

**Objectif : scripter les incidents + le témoin, et mesurer les critères de la garantie (justesse, délai ≤ 30 s, faux positifs, fraîcheur).**

- ✅ **Scripter l'injection des 3 incidents** (`scripts/diagnostic-campaign.sh`) :
  - **(A) retard consommateur** : `consumer_lag` (décision 400 ms, persisters 0 ms) → **`consumer_backlog/reader_backlog` en 15 s** ✅
  - **(B) réplication dégradée** : `replication_lag` (kafka-3 arrêté 20 s) → **`kafka_replication/replication_catchup` en 25 s** ✅ (le délai inclut le cycle de rafraîchissement du snapshot réplication ~20-30 s — cohérent avec le budget de la garantie)
  - **(C) incident PostgreSQL** : les deux modes faits — dégradation (persisters 350 ms) → **`persister_slow` en 14 s** ✅ ; indisponibilité (`docker compose stop postgres` 40 s) → **`service_down:postgres` (famille persistence) en 3 s** ✅
- ✅ Ajouter un essai **NOMINAL témoin** (contrôle des faux positifs — critère #4) : **0 faux positif sur 5 témoins, 70 échantillons, 100 % `healthy`** ✅
- ✅ Automatiser l'enchaînement : scénarios + collecte des relevés (verdicts horodatés `verdicts.jsonl`, évènements `events.jsonl`, sorties scénario) — **rejouable par un autre groupe** : protocole dans [`notes/protocole-campagne-phase4.md`](notes/protocole-campagne-phase4.md), rapport via `scripts/lib/campaign_report.py` (critère #8) ✅
- ✅ Mesurer pour tout diagnostic déterminé :
  - **justesse** du diagnostic — critère #1 : **4/4 incidents** → la bonne famille nommée chez les 3 (relevés `preuves/phase4-campagne-2026-11-05/SYNTHESE.md`) ✅
  - **délai de détection** (injection → alerte) — critère #3 : **max 25 s ≤ 30 s** ✅
    (A 15 s ; B 25 s ; C-dégradé 14 s ; C-indispo 3 s)
  - **taux de faux positifs** — critère #4 : **0/70 échantillons** ✅
- 🟡 (partiel) Vérifier la **fraîcheur des sondes** (critère #6) : en live, l'indisponibilité du service observé est relevée `unreachable` et ne produit JAMAIS de conclusion ; le cas `probes_stale` (payload vieilli 300 s → `indeterminate`) est couvert par le test unitaire. Arrêter `metrics-collector` ne gèle PAS `/health/details` (le `timestamp` est ré-émis à chaque requête, cf. protocole « limites »).

**Bilan seuils du moteur** : tenus sans recalibration (lag 10, écart qui S'ACCROÎT ≥ 5, big-gap 20, fraîcheur 75 s, span ≥ 10 s). Leçon d'outil : la cadence du relevé doit être ≥ 10 s sinon les règles à delta restent à `needs_second_sample` (v1 à 5 s archivée dans `archives-rec5s/` du répertoire de campagne).

---

## 4. Phase 5 — Vérification croisée ✅

**Objectif : prouver que le diagnostic repose sur la donnée, pas sur un graphique.**
✅ FAIT (05/11/2026) — `scripts/cross-check.sh` (4 cas : `A-draine`,
`B-replication-croise`, `C-rattrapage`, `rejets-metier`) + analyse
`scripts/lib/crosscheck_report.py` (convergence `scripts/lib/convergence_check.py`),
protocole rejouable dans [`notes/protocole-phase5-croisee.md`](notes/protocole-phase5-croisee.md),
preuves dans `preuves/phase5-croisee-2026-11-05/`.

- ✅ Vérifier **CHAQUE conclusion contre Kafka ET PostgreSQL** (`tp-kafka.sh`, `tp-db.sh`,
  consumer groups, contenu des tables), pas seulement contre un dashboard Grafana :
  chaque cas capture `tp-kafka-<label>.txt` + `tp-db-<label>.txt` + payload
  `health-<label>.json` + compteurs `counts-<label>.json` à `t0` (baseline), pendant
  l'incident (`A-draine`, `C-rattrapage`) et à la fermeture (`final`).
- ✅ Démontrer que le diagnostic **NE CONFOND PAS** : backlog consommateur / rejet
  métier / perte de données — **4/4 PASS** (candidate de garantie **#2** démontrée) :
  - **backlog temporaire** (A-draine, `consumer_lag` décision 400 ms / persisters 0) :
    `indeterminate/needs_second_sample` au 1er échantillon (fusible anti-lag-éclair),
    puis **10×`consumer_backlog`** (lag jusqu'à 517) ; à la fermeture `db=20+580 /
    outcome=600 / écarts=0 / lag=0` → TOUTE la donnée finit persistée, **dont les 580
    rejets `processing_timeout`** (TTL de décision 10 s dépassé sous grand backlog) qui
    sont des rejets métier **persistés en base, PAS une perte** ;
  - **perte = écart stable** : suite des fermetures étudiée après drainage — jamais
    déclenchée en l'état, car les persisters du socle **rejouent toujours** (rattrapage
    complet de C : `db=800+0 / outcome=800 / écarts=0` après `stop postgres` 40 s →
    **0 perte mesurée**) ; la branche « perte mesurée » est codée dans
    `convergence_check.py`/rapport (fermeture non atteinte à l'échéance) et le résultat
    négatif est documenté honnêtement dans le protocole ;
  - **rejet métier attendu** : scénario `errors_simple` → **300 rejetés == 300 lignes
    en PG** (cohérence compteurs ↔ tables), moteur **AUCUN incident sonné** (8×
    `healthy`) → rejet ≠ incident ;
  - **B-replication-croise** (kafka-3 arrêté 20 s) : verdict `kafka_replication`
    révéré par la capture **Kafka en direct** (`tp-kafka-final.txt` : brokers, URP,
    contrôleur), pas par un dashboard.
- ✅ **Convergence en fermeture single-snapshot** : `run-scenario.sh` relance
  service-health à chaque cas (compteurs cumulés recalculés) → deltas inter-cas
  interdits ; fermeture = `generated == raw_end == validated+rejected == outcome ==
  db_validated+db_rejected`, écarts 0, lag ≤ 5 (leçon §1.8).
- ✅ Essai de preuve `errors_simple` en place (cas `rejets-metier` ci-dessus).

---

## 5. Phase 6 — Livrables et preuves

- ⬜ **Guide de diagnostic reproductible**, utilisable par un autre groupe pendant un incident (recette praticienne pas à pas) → cible : `docs/operations/guide-diagnostic.md`.
- ⬜ **Note de conception courte** (choix techniques + justification des seuils) → cible : `docs/architecture/diagnostic-engine-design.md`.
- ⬜ **Protocole d'essai exécutable par un autre groupe** (commandes, scénarios, valeurs attendues).
- ⬜ **Relevés pour au moins 3 situations distinctes** (+ le témoin nominal) → `preuves/phase4-*/`.
- ⬜ **L'amélioration d'observabilité réellement exploitable** (le produit : commande de diagnostic / vue / alerte).
- ⬜ Déposer les preuves **synthétiques et rejouables** dans `preuves/` (pas les sorties volumineuses de `runtime/`, qui restent exclues).
- ⬜ **Conclusion** qui dit ce que la garantie couvre et ne couvre pas ; un résultat négatif est recevable s'il est rigoureusement établi.

---

## 6. Phase 7 — Extension (si temps)

- 🎁 Comparer seuils **fixes** à une détection **adaptative** ; une règle explicite justifiée et évaluée suffit pour le minimum, l'apprentissage machine n'est pas exigé.
- 🎁 Candidate #7 (impact métier) : champ « impact métier » par incident (X Pix dépassent le SLA de décision de 10 s, issu des compteurs `*_sla_breaches`), vérifiable en base.

---

## 7. Phase 8 — Dimension théorique

**Encouragée**

- ⬜ Formaliser les hypothèses de cause, les observations qui les discriminent et le cas indéterminé.
- ⬜ Justifier les seuils ; mesurer délai de détection, faux positifs et erreurs de diagnostic sur des incidents connus.
- ⬜ Consulter `TP/SAé/analyse-recherche-limos.md` pour les pistes de lecture et préparer un échange avec l'enseignant/LIMOS.

---

## 8. Phase 9 — Rendu GitHub

**Tout au long, pas à la fin.**

- ⬜ Travailler exclusivement sur le dépôt dérivé privé (`https://github.com/TomPERRIN/ISIMA-TP-kafka-2026-...`).
- ⬜ Inviter `williamguyotlenat@icloud.com` comme collaborateur (dès le début).
- ⬜ Committer régulièrement avec des messages explicites ; documenter les contributions non-code de chaque membre.
- ⬜ Mettre à jour le **README racine** : sujet traité, garantie visée, démarche pour lancer la plateforme + rejouer les essais, mention « dérivé de simul-pix ».
- ⬜ Vérifier avant chaque publication : **pas de secrets** (`infra/kafka/secrets/`), **pas de `runtime/`** (volumes, caches, gros dumps) — preuves = fichiers synthétiques de `preuves/`.
- ⬜ Veille de la soutenance : passer le dépôt en **PUBLIC** et transmettre l'URL à l'enseignant (dépôt inaccessible = rendu manquant).

---

## 9. Phase 0/1 — Restes d'organisation

- ⬜ Former un groupe de 4 minimum, répartir les rôles (analyse, développement, essais, documentation), désigner un responsable d'intégration, prévoir un point de synchronisation par séance.
- ⬜ Confirmer/inviter le collaborateur GitHub (voir ci-dessus).
- 🎁 Creuser l'anomalie `validate-observability.sh` (« must specify org ID or org name », flag `--org` absent du script).

---

## 10. Règles à respecter pendant l'exécution

1. Un offset Kafka seul **ne prouve pas** que la donnée métier est persistée : toujours vérifier le résultat dans PostgreSQL.
2. Un flux nominal OK **ne prouve pas** la résistance aux pannes/charge.
3. Toute évolution de contrat/topic/base doit être documentée et testée.
4. Distinguer **socle fourni / contribution minimale / extension** dans les livrables.
5. Lancer les batteries d'essais **tôt et régulièrement** (les essais demandent du temps machine).
6. Ne jamais conclure sur des sondes périmées (critère #6) : afficher l'âge des mesures.
7. Le rendu doit rester exécutable **sans dépendre de l'achèvement d'un autre groupe**.

---

## Outils utiles (rappels)

| Usage | Commande / URL |
|-------|----------------|
| Plateforme (démarrage / arrêt) | `./start.sh` · `./stop.sh` |
| Scénario | `./scripts/run-scenario.sh <scénario> <total> <débit>` |
| Arrêter / réinitialiser un scénario | `./scripts/stop-scenario.sh` · `./scripts/reset-scenario.sh` |
| Résumés d'état | `./scripts/tp-status.sh` · `./scripts/tp-kafka.sh` · `./scripts/tp-db.sh` |
| Validation observabilité | `./scripts/validate-observability.sh` |
| Perturbation réseau | `./scripts/network-perturb.sh <profil> <service>` (kafka_latency, kafka_loss, kafka_slow_link) · `./scripts/network-reset.sh` |
| Météo (service-health) | http://localhost:8082/ (JSON : `/health`, `/health/details`) |
| Grafana | http://localhost:3000 (admin / adminpass) |
| InfluxDB | http://localhost:8086 (admin / adminpass) |
| PostgreSQL | localhost:5432 (simulpix / simulpix) |
