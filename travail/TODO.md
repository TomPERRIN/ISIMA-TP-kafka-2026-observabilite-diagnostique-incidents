# TODO — SAÉ 5 · Observabilité et diagnostic d'incidents

> Fichier de pilotage des tâches à faire. Il fusionne le suivi global (`TODO-SAE5.md` à la racine) et les restes à compléter de l'inventaire (`notes/metriques-observabilite.md`).
> Ce qui est déjà fait est consigné dans [`avancement.md`](avancement.md).
> Légende : ⬜ à faire · 🟡 en cours · 🎁 bonus / facultatif

---

## Table des matières

1. [Phase 2 — Terminer l'état des lieux ✅](#1-phase-2--terminer-létat-des-lieux-✅)
2. [Phase 3 — Conception de l'outil de diagnostic](#2-phase-3--conception-de-loutil-de-diagnostic) (matrice ✅, automatisation ⬜)
3. [Phase 4 — Automatisation des essais](#3-phase-4--automatisation-des-essais)
4. [Phase 5 — Vérification croisée](#4-phase-5--vérification-croisée)
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

### 2.2 Automatiser la matrice (le cœur de la contribution) ⬜

- ⬜ Concevoir l'apport concret (au moins **un** parmi) :
  - une métrique dérivée ;
  - une alerte ;
  - une vue (dashboard/HTTP) ;
  - une commande de diagnostic.

  Exigences du minimum :
  - couvrir les situations **A, B et C** (dont une dégradation **OU** une indisponibilité PostgreSQL) ;
  - distinguer **automatiquement** au moins **2 causes plausibles d'un même symptôme** (la matrice v1 donne les 5 lignes à coder) ;
  - si les données ne permettent pas de trancher, afficher un état **« indéterminé »** + la vérification suivante à mener.
- ⬜ Intégrer le **garde-fou fraîcheur** (critère #6) : âge max des mesures utilisées affiché à côté du diagnostic ; sondes périmées → « indéterminé », jamais une conclusion fausse.
- ⬜ Prévoir une **vérification indépendante** de la cause (croiser offsets/API Kafka avec le contenu réel de PostgreSQL — `tp-kafka.sh` + `tp-db.sh` — pas seulement un graphique).
- ⬜ Tester la machine à états par un **test unitaire** (critère #5 : toute conclusion ≠ indéterminé exige la double lecture Kafka ET PostgreSQL).
- ⬜ Codifier les règles de la matrice v1 : chaque ligne (5 symptômes × 2 causes) devient une règle de croisement du moteur (voir architecture cible §4).
- ⬜ Reporter le **classement probabilité / criticité** dans la sortie du moteur : présenter les causes dans l'ordre « priorité d'examen » (critique d'abord si vérification rapide), pas seulement « probable d'abord ».

---

## 3. Phase 4 — Automatisation des essais

**Objectif : scripter les incidents + le témoin, et mesurer les critères de la garantie (justesse, délai ≤ 30 s, faux positifs, fraîcheur).**

- ⬜ **Scripter l'injection des 3 incidents** :
  - **(A) retard consommateur** : `./scripts/run-scenario.sh consumer_lag` ou `./scripts/network-perturb.sh <profil> <service>` ;
  - **(B) réplication dégradée** : `./scripts/run-scenario.sh replication_lag` (arrêt temporaire d'un broker) ;
  - **(C) incident PostgreSQL** : dégradation (`SIMULPIX_PERSIST_VALID_DELAY_MS` / `SIMULPIX_PERSIST_REJECTED_DELAY_MS`) ou indisponibilité (`docker stop postgres`).
- ⬜ Ajouter un essai **NOMINAL témoin** (contrôle des faux positifs — critère #4 : 0 alerte sur ≥ 5 nominaux).
- ⬜ Automatiser l'enchaînement : scénarios + collecte des relevés (commandes + sorties horodatées), **rejouable par un autre groupe** (critère #8).
- ⬜ Mesurer pour tout diagnostic déterminé :
  - **justesse** du diagnostic (bonne cause identifiée — critère #1 : 100 % des cas connus) ;
  - **délai de détection** (injection → alerte — critère #3 : ≤ 30 s) ;
  - **taux de faux positifs** (critère #4).
- ⬜ Vérifier la **fraîcheur des sondes** : injecter un incident sur le système d'observation lui-même (arrêt `metrics-collector`, gel du `history`) et vérifier que le diagnostic bascule en « indéterminé » (critère #6).

---

## 4. Phase 5 — Vérification croisée

**Objectif : prouver que le diagnostic repose sur la donnée, pas sur un graphique.**

- ⬜ Vérifier **CHAQUE conclusion contre Kafka ET PostgreSQL** (`tp-kafka.sh`, `tp-db.sh`, consumer groups, contenu des tables), pas seulement contre un dashboard Grafana.
- ⬜ Démontrer que le diagnostic **NE CONFOND PAS** : backlog consommateur / rejet métier / perte de données.
  - backlog temporaire = lag > 0 **drainé** + convergence finale DB == métier + gap == 0 ;
  - perte = gap > 0 **stable** après drainage ;
  - rejet métier attendu = compteurs `rejected` cohérents avec les tables PG, **pas un incident**.
  - Essai de preuve : scénario `errors_simple` (rejets métier) comparé à un incident de persistance.

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
