# SAÉ n°5 — Observabilité et diagnostic d'incidents (Simul-Pix)

> **Fichier de suivi des choses à faire**
>
> Sources : `README.md`, `TP/README-SAe.md`, `TP/SAe/README.md`, `TP/SAe/sujet-05-observabilite-incidents.md`, `TP/SAe/limites-du-socle.md`.
>
> Légende : ✅ fait · ⬜ à faire · 🎁 bonus / facultatif

---

## Table des matières

1. [Le sujet en résumé](#1-le-sujet-en-résumé)
2. [Plan d'architecture](#2-plan-darchitecture) → [`docs/architecture/plan-architecture-sae5.md`](docs/architecture/plan-architecture-sae5.md)
3. [Phase 0 — Préparation](#3-phase-0--préparation)
4. [Phase 1 — Formulation de la garantie](#4-phase-1--formulation-de-la-garantie)
5. [Phase 2 — État des lieux / recensement des signaux](#5-phase-2--état-des-lieux)
6. [Phase 3 — Conception de l'outil de diagnostic](#6-phase-3--conception-de-loutil)
7. [Phase 4 — Automatisation des essais](#7-phase-4--automatisation-des-essais)
8. [Phase 5 — Vérification croisée](#8-phase-5--vérification-croisée)
9. [Phase 6 — Livrables et preuves](#9-phase-6--livrables-et-preuves)
10. [Phase 7 — Extension facultative](#10-phase-7--extension)
11. [Phase 8 — Dimension théorique](#11-phase-8--dimension-théorique)
12. [Phase 9 — Rendu GitHub](#12-phase-9--rendu-github)
13. [Règles importantes](#13-règles-importantes)
14. [Outils utiles](#14-outils-utiles)
15. [Cohérences avec d'autres sujets](#15-cohérences-avec-dautres-sujets)

---

## 1. Le sujet en résumé

### 1.1 Le problème

Un pipeline peut paraître **disponible** (conteneurs démarrés) tout en accumulant du retard, en perdant la réplication Kafka ou en n'écrivant plus dans PostgreSQL. Une courbe isolée ne suffit pas : **il faut croiser les signaux**.

### 1.2 La mission

Construire une **lecture d'incident** qui distingue **au moins 3 situations** :

| # | Situation | Exemple concret |
|---|-----------|-----------------|
| **A** | Un consommateur prend du retard | `pix-validator` ralenti, backlog qui monte sur `raw`/`checked` |
| **B** | La réplication entre brokers Kafka est dégradée | broker `kafka-2` arrêté, partitions sous-répliquées |
| **C** | La persistance PostgreSQL ne suit plus le flux | `persister-valid` avec délai, ou conteneur `postgres` arrêté |

### 1.3 L'objectif

- Pas juste **afficher des métriques** : passer du **symptôme** au **diagnostic vérifiable** (cause + vérification indépendante).
- Socle fourni : météo (`service-health`), Grafana, sondes Kafka/PG, alertes existantes.
- La contribution : une règle/outil qui **relie** ces signaux pour lever une ambiguïté mesurée.
- Difficulté : ★★★ — durée : une SAÉ complète — groupe : 4 minimum.

---

## 2. Plan d'architecture

📄 Le plan d'architecture complet (socle, couche d'observabilité, chaîne applicative, architecture cible du moteur de diagnostic, 3 vues de lecture, intégration dans le dépôt) est dans un fichier dédié : [`docs/architecture/plan-architecture-sae5.md`](docs/architecture/plan-architecture-sae5.md).

---

## 3. Phase 0 — Préparation

**Objectif : être capable de lancer la plateforme et de lire ses signaux avant de toucher au code.**

### 3.1 Étude du socle

- ✅ Act. 08 (observabilité) relue + atelier avancé relu : scénario fil rouge `football_match_peak`, 3 vues (métier / transport / persistance), lecture croisée lag / sous-réplication / backlog.
- ✅ `docs/architecture/grafana-dashboards.md` + `service-health-http.md` lus : les **3 métriques du Kafka Dashboard à lire ENSEMBLE** (partitions sous-repliquées / lag total / ancienneté backlog) ; endpoints `/health`, `/health/details`, `/control` (run_scenario avec `producer_semantics`, `traffic_model`, `network_profile`, …).
- ✅ `docs/developpement/guide-developpeur.md` lu : anatomie d'un service, rebuild (`--build --no-deps`), ajout topic en 2 endroits, migrations SQL (`init/*.sql` créées au 1er volume), variables `SIMULPIX_*`, limites connues (attention : service-health fait des `count(*)` à CHAQUE appel de `/health` — précieux pour le sujet 5).
- ✅ Prérequis plateforme : Docker 29.1.3 + Compose v2.40.3, ~4 Go RAM alloués, tous les ports requis libres (3000, 5432, 8082, 8083, 8086, 9092-9094).

### 3.2 Plateforme opérationnelle

- ✅ Démarrer la plateforme : `./start.sh` — **FAIT 2026-11-03**.

  > **NOTE WINDOWS** (à conserver — coûteux à redécouvrir) :
  >
  > - lancer `./start.sh` **DEPUIS WSL (Ubuntu)** et pas Git Bash : le `docker-compose.yml` de service-health contient `${PWD}:${PWD}` (montage du dépôt dans le conteneur) ; sous Git Bash, `PWD` devient un chemin Windows à deux-points, Docker refuse (« too many colons »). Depuis WSL : `wsl -d Ubuntu -e bash -c "cd /mnt/c/Users/tompe/.../ISIMA-TP-kafka-2026-observabilite-diagnostique-incidents && ./start.sh"`.
  > - le start-up échouait aussi à cause des **CRLF Windows** sur les `.sh` (interpréteur `/bin/sh^M`) : tous les `.sh` du dépôt convertis en LF localement (invisible pour Git, blobs déjà en LF) ; un `.gitattributes` (`*.sh text eol=lf`) ajouté pour éviter la récidive au prochain clone.
  > - Git Bash reste utile : génération des secrets avec `MSYS_NO_PATHCONV=1` + `SIMULPIX_SECRETS_DIR=<chemin Windows>`.
  > - le dossier `infra/kafka/secrets/` manquait : recréé (+ `.gitkeep`), sinon `generate-secrets.sh` plante.
  > - secrets TLS générés avec succès.

- ✅ Lancer un flux nominal de contrôle : `./scripts/run-scenario.sh nominal 100 10` — **FAIT, run 1793704138**.

  Résultats observés :
  - `/health` => status `ok`, alerts `[]` ;
  - compteurs : 100 générés / 100 traités / 100 acceptés / 0 rejetés / 100 outcome / 100 persistés, taux de rejet 0 % ;
  - lag = 0 sur les 5 groupes applicatifs (à jour) ;
  - `tp-db.sh` : `validated_count=100`, `rejected_count=0` (PostgreSQL) ;
  - tous les services OK ;
  - `validate-observability.sh` : InfluxDB + Grafana + dashboards provisionnés OK (General, Kafka, Persistence, Incidents) ; seul le dernier pas (mesures Influx CLI) échoue avec « must specify org ID or org name » : anomalie mineure à creuser plus tard (flag `--org` absent du script).

  Commandes utiles (sur cette machine) :
  - scripts via `wsl -d Ubuntu -e bash -c "cd <chemin repo> && ./scripts/<script>.sh"` ;
  - résumés : `tp-status.sh` ; détails : `tp-kafka.sh`, `tp-db.sh`, `curl :8082/health` ou `/health/details`.

- ✅ Dépôt GitHub : fork `TomPERRIN/ISIMA-TP-kafka-2026-...-incidents` configuré (origin), 404 anonyme => visibilité privée probable. **Reste (utilisateur)** : confirmer/inviter `williamguyotlenat@icloud.com` comme collaborateur (GitHub > Settings > Collaborators).
- ✅ `.gitignore` durci : `infra/kafka/secrets/*` et `runtime/*` exclus en générique, `.gitkeep` préservés (vérifié par `git check-ignore`).

### 3.3 Organisation d'équipe

✅ Réparti le 05/11/2026 (groupe de 4, conforme à `TP/SAé/README.md` : analyse, développement, essais, documentation ; un responsable d'intégration ; un point de synchronisation à chaque séance).

| Membre | Rôle | Contributions |
|---|---|---|
| TomPERRIN | Développement + **responsable intégration** | moteur de diagnostic (`diagnose.sh`, `diagnose_engine.py` + tests), campagne automatisée phases 4-5, guide praticien, note de conception, impact métier ; relit et fusionne les PR, garantit `main` exécutable |
| Ayel04 | Analyse | états des lieux (météo, dashboards, métriques restantes), inventaire d'observabilité (`metriques-observabilite.md`), matrice symptômes/causes (v1) |
| Madiak5 | Documentation + revue | vérification croisée de l'inventaire contre les dashboards provisionnés (§2.6, 05/11), organisation d'équipe (cette section), README racine, relecture des livrables en « lecteur externe » (guide, note de conception), rejeu du protocole d'essai |
| Ania | Essais indépendants + documentation | rejeu de la campagne (preuve « protocole exécutable par un autre groupe »), test du guide praticien pendant un incident, vérification de la fraîcheur des sondes |

- **Responsable d'intégration** : TomPERRIN (PR #1, fusion des branches, `main` exécutable).
- **Point de synchronisation** : à chaque séance (bilan fait/reste, répartition des prochaines tâches, push de la branche de chacun).
- Les contributions non-code (revues, vérifications, rédaction) sont documentées dans ce TODO et les messages de commit, conformément aux attendus (« Rendez visibles les contributions individuelles »).

---

## 4. Phase 1 — Formulation de la garantie

**Objectif : formuler le problème et la garantie cible AVANT toute modification du code** (référence : `TP/README-SAe.md` §2.4, forme « [le comportement] ne doit pas [l'effet indésirable], prouvé par [la mesure] »).

### 4.1 Énoncé du problème

- ✅ Formulé : « Aider une personne qui découvre un incident à passer d'un symptôme visible (retard qui monte, réplication dégradée) à une cause vérifiée, en croisant les signaux déjà exposés par la météo, Grafana et les sondes Kafka/PostgreSQL — **sans ajouter de nouvelle sonde**. »

### 4.2 Décision (validée utilisateur, 03/11/2026)

| Candidate | Statut | Rôle |
|-----------|--------|------|
| **#1 Désambiguïsation** (3 familles) | ✅ retenue | **Garantie principale** |
| **#3 Délai de détection**, **#4 zéro faux positif**, **#6 fraîcheur des sondes**, **#5 jamais sans vérification croisée** | ✅ retenues | Critères d'acceptation mesurables de #1 |
| #2 non-confusion backlog / rejet / perte | 🎁 bonus | **MAIS** la fiche exige quand même « la démonstration que le diagnostic ne confond pas backlog, rejet métier et perte » → la *démonstration* reste un livrable (matrice + essai errors_simple), seul le critère quantifié est bonus |
| #7 impact métier | 🎁 extension facultative | |
| #8 reproductibilité inter-groupes | ✅ inhérent au rendu | Le protocole doit être exécutable par un autre groupe : ce n'est pas une option |

### 4.3 Garantie principale (candidate #1, version condensée pour le README)

> « Notre lecture d'incident ne doit pas confondre retard de consommateur, réplication dégradée et panne de persistance, prouvé par une matrice validée sur incidents injectés : bonne cause retenue sur **100 % des cas connus**, état **« indéterminé » + vérification suivante** sinon. »

Critères d'acceptation mesurables branchés dessus :

- justesse **100 %** sur incidents connus (#1) ;
- faux positifs = **0** sur ≥ 5 nominaux (#4) ;
- délai de détection **≤ 30 s**, à fixer définitivement après mesure du pas du collecteur Influx (#3) ;
- refus de conclure sans double lecture **Kafka + PostgreSQL** (#5) et fraîcheur des sondes affichée (#6).

Points tranchés : dire « 100 % des cas connus » (vérifiable) plutôt que « jamais d'erreur » ; seuil de détection = 30 s (voir justification §4.4).

### 4.4 Justification du seuil ≤ 30 s (mesures du 03/11/2026, sondes réelles)

- Collecteur InfluxDB (`metrics-collector`) : cadence fixe **10 s** (`SIMULPIX_OBS_INTERVAL_SECONDS: 10` dans `docker-compose.yml`).
- Historique service-health (`/health/details`) :
  - en idle : ne se rafraîchit **plus** (dernier échantillon observé de 710 s après la fin du nominal) → **piège à surveiller par le critère #6** (fraîcheur des sondes) ;
  - sous charge : intervalles observés 5,7 – 12,3 s (médiane ~6-8 s).
- Conclusion : 30 s ≈ 2-3 cycles de collecte + latence acteur. Seuil tenable et justifiable. **DÉCISION : garder 30 s.**

### 4.5 Les 8 candidates de formulation (proposées le 03/11/2026)

#### #1 Désambiguïsation (candidate principale — retenue)

> « Notre lecture d'incident ne doit pas confondre un retard de consommateur, une dégradation de réplication Kafka et une défaillance de persistance PostgreSQL, prouvé par une matrice "symptôme / cause / vérification" validée sur des incidents réellement injectés : la bonne cause est retenue dans 100 % des essais connus, et un état "indéterminé" + la vérification suivante lorsque les sondes ne permettent pas de trancher. »

- **Preuve** : ≥ 3 incidents injectés + 1 témoin nominal, relevés archivés dans `preuves/` ; couvre les 3 familles de la mission.

#### #2 Non-confusion backlog / rejet / perte (🎁 bonus, démonstration obligatoire)

> « Le diagnostic ne doit pas présenter comme perte de données ce qui est un simple backlog en rattrapage, ni appeler incident un rejet métier attendu, prouvé par l'égalité entre compteurs métier (outcome, rejected) et contenu réel de PostgreSQL après drainage, sur chaque essai. »

- **Preuve** : comparaison Kafka ↔ PostgreSQL à chaque campagne.

#### #3 Détection dans le temps de l'incident (retenue)

> « La détection ne doit pas se produire seulement après la fin de l'incident, prouvé par un délai de détection ≤ 30 s mesuré sur au moins 3 essais scriptés par famille d'incident. »

- **Preuve** : timestamps (injection → alerte) extraits du protocole.

#### #4 Zéro faux positif sur le nominal (retenue)

> « Le diagnostic ne doit pas lever d'alerte pendant un flux nominal, prouvé par 0 alerte intempestive sur au moins 5 essais nominaux témoins avec le même protocole. »

- **Preuve** : le témoin nominal devient test de régression.

#### #5 Jamais de conclusion sans vérification croisée (retenue)

> « Le diagnostic ne doit jamais affirmer une cause sans vérification indépendante, prouvé par un état de sortie à 4 valeurs (retard consommateur / réplication / persistance / indéterminé) tel que toute conclusion ≠ indéterminé exige la double lecture Kafka ET PostgreSQL. »

- **Preuve** : test unitaire sur la machine à états + contrôle dans chaque relevé de campagne.

#### #6 Fraîcheur des sondes (retenue — exigée par la fiche)

> « Le système ne doit pas conclure sur des sondes périmées, prouvé par un indicateur de fraîcheur (âge maximal des mesures utilisées) affiché à côté du diagnostic ; quand metrics-collector est arrêté, le diagnostic bascule en "indéterminé", pas en conclusion fausse. »

- **Preuve** : injection d'incident sur le collecteur lui-même.

#### #7 Diagnostic traduit en impact métier (🎁 extension facultative)

> « Le diagnostic ne doit pas rester purement technique, prouvé par un champ "impact métier" par incident (ex. : ces X Pix dépassent le SLA de décision de 10 s, issu des compteurs processing_timeout de PostgreSQL), vérifiable en base. »

- **Preuve** : lien signal technique → compteur métier en base.

#### #8 Reproductibilité inter-groupes (inhérent au rendu)

> « Le diagnostic ne doit pas dépendre de l'opérateur, prouvé par un protocole entièrement scripté qu'un groupe tiers exécute sans préparation et dont il obtient la même conclusion sur des incidents de la matrice. »

- **Preuve** : le guide de diagnostic s'exécute sans création manuelle (exigence du protocole d'essai SAÉ).

---

## 5. Phase 2 — État des lieux

**Objectif : recenser précisément les signaux existants avant de concevoir le croisement.**

### 5.1 Inventaire détaillé des métriques — `travail/notes/metriques-observabilite.md`

L'inventaire complet **mètre par mètre** (source, signification, où la lire, fraîcheur, limites) est dans [`travail/notes/metriques-observabilite.md`](travail/notes/metriques-observabilite.md). Il couvre :

- **§1 Météo (`service-health`)** :
  - *sondes* : `services.<nom>` (`ok`/`down`/`degraded`/`error`/`not_probed`), `status` global — une sonde qui répond prouve que le conteneur tourne, **pas qu'il travaille** ;
  - *compteurs* : `counts.generated/processed/validated/rejected`, `counts.persisted_*` (compteur interne des persisters) vs `counts.db_validated_count` / `db_rejected_count` (**réalité vérifiée en base** — à ne pas confondre), `counts.pending_result_count` (file d'attente), `counts.validated_sla_breaches` (Pix traités trop lentement) ;
  - *écarts et durées* : `gaps.persistence_gap_valid/_rejected` (« traité mais non sauvegardé » — un écart qui **grossit** est suspect), `timings.estimated_oldest_lag_seconds` (estimation ratio file/débit, **disparaît quand le générateur s'arrête**), `avg_*_latency_ms`, `seconds_since_last_*` ;
  - *Kafka* : `broker_count`, `topics.<topic>.end_offsets_total`, `partition_details`, `consumer_groups.<groupe>.lag_total` (5 groupes suivis), `replication.under_replicated_partitions_total` (**LE signal** de réplication dégradée), `replication.partitions[]`, `replication.brokers[]` ;
  - *alertes existantes* : 8-9 familles (`service_status`, `kafka_lag`, `kafka_replication`, `reject_rate`, `persistence_gap_valid/_rejected`, `decision_sla`, `oldest_lag`, `network_perturbation`), réglables par `SIMULPIX_WARNING_*` ;
  - *ce qui manque* : alertes **indépendantes les unes des autres** (rien ne dit laquelle explique l'autre), **fraîcheur des mesures non indiquée**, estimation de l'ancienneté qui disparaît si le générateur s'arrête alors que la file existe toujours.
- **§2 Dashboards Grafana**, **§3 `tp-kafka.sh`**, **§4 `tp-db.sh`**, **§5 Synthèse** : sections à compléter (voir 5.4 ci-dessous).

Cet inventaire alimentera la **matrice symptôme / cause / vérification** (Phase 3).

### 5.2 Recensement des signaux — ✅ FAIT 03/11/2026

Documenté dans `preuves/phase2-recensement/recensement-signaux.md` :

- `/health` + `/health/details` : structure complète (`details.kafka` / `details.postgres` / `metrics.counts`, `gaps`, `timings` / `history`) ;
- **7 mesures InfluxDB** : `kafka_broker`, `kafka_replication_partition`, `kafka_consumer_group`, `kafka_topic`, `simulpix_platform`, `simulpix_semantics`, `postgres_runtime` (pas de 10 s) ;
- **9 familles d'alertes** déjà codées dans `alerts.py` (seuils via variables `SIMULPIX_WARNING_*`) : toutes **monocausales**, aucune ne fait le croisement → **la marge d'apport du sujet 5 est là** ;
- rôles des dashboards et des scripts `tp-*`.

Observations structurantes :

- `history` de service-health **gèle en idle** (dernier échantillon observé : 710 s) ; sous charge : ~5,7-12,3 s ;
- `replication.captured_at` existe (fraîcheur implicite transport), et Postgres a `seconds_since_last_*` : mais **aucune clé d'âge global de sonde** → l'indicateur de fraîcheur (critère #6) sera bien une **contribution nouvelle** ;
- `lag_total == 1` en nominal est **normal** (rattrapage éclair) → toute règle brute « lag > 0 = incident » se vaudrait des faux positifs.

### 5.3 Mesure de référence sur flux nominal — ✅ FAIT

Archivée dans `preuves/phase2-recensement/reference-nominale-2026-11-03/` (run-scenario.log, health-details-mid/final.json, tp-status/tp-kafka/tp-db final, protocole.txt rejouable).

`verification-attendus.txt` : **13/13 vérifications OK** (status ok, 0 alerte, convergence 100/100/100/0/outcome/DB, gaps 0, lag 0 sur 5 groupes, sous-réplication 0 sur 3 brokers).

### 5.4 Limites du socle notées

(dans `recensement-signaux.md` §5/§7)

- `estimated_oldest_lag_seconds` = ratio courant/débit, **pas un âge** ;
- moyennes sans p95/p99 ;
- l'observateur peut être affecté par l'incident.

### 5.5 Reste à compléter dans l'inventaire (métriques-observabilite.md)

- ✅ **§2 Dashboards Grafana** : les 4 dashboards provisionnés (`simul-pix-incidents`, `simul-pix-kafka`, `simul-pix-persistence`, `simul-pix-pipeline`) — panneaux principaux, métriques sous-jacentes, seuils. Complété puis **revérifié panneau par panneau contre les JSON provisionnés le 05/11/2026** (Madiak5) : contenu conforme, écarts corrigés (panneau « Aide de lecture », courbe « Retard de réplication », variantes « période affichée », absence de courbe dédiée pour l'« Ancienneté backlog ») — voir `metriques-observabilite.md` §2.6.
- ✅ **§3 `tp-kafka.sh`** : informations lisibles (offsets, lag, réplication, …).
- ✅ **§4 `tp-db.sh`** : compteurs PostgreSQL, doublons, écritures.
- ✅ **§5 Synthèse** : ce qui manque pour distinguer les 3 incidents de la mission, ambiguïtés observées entre signaux.

### 5.6 Questions de conception — réponses de travail

(mémoire dans `recensement-signaux.md` §6)

- **Backlog temporaire vs perte** : backlog = lag > 0 **drainé** + convergence finale DB == métier + gap == 0 ; perte = gap > 0 **stable** après drainage.
- **Cluster vs applicatif** : lire **ENSEMBLE** sous-réplication/offline/contrôleur (brokers) **ET** lag des groupes applicatifs.
- **Impact métier** : compteurs SLA (breaches/within_sla) : un Pix arrivé après le TTL de 10 s.
- **Persistance** : gap + db counts vs compteurs métier + offsets topic vs outcome (**jamais un seul offset**).
- **Signal d'alerte fiable** : under_replicated > 0, gap stable après drainage, dérive persistante du lag ; `lag == 1` seul = bruité.

---

## 6. Phase 3 — Conception de l'outil de diagnostic

**Objectif : définir la matrice symptôme/cause/vérification et concevoir l'apport concret (livrables de preuve attendus).**

- ⬜ Pour chaque famille d'incident (A, B, C), définir : **symptômes attendus**, **hypothèses de cause possibles**, **vérification suivante**.
- ⬜ Construire la **MATRICE « symptôme / cause probable / vérification »** (livrable de preuve attendu).
- ⬜ Concevoir l'apport concret (au moins **un** parmi) :
  - une métrique dérivée ;
  - une alerte ;
  - une vue (dashboard/HTTP) ;
  - une commande de diagnostic.

  Exigences du minimum :
  - couvrir les situations **A, B et C** (dont une dégradation **OU** une indisponibilité PostgreSQL) ;
  - distinguer **automatiquement** au moins **2 causes plausibles d'un même symptôme** ;
  - si les données ne permettent pas de trancher, afficher un état **« indéterminé »** + la vérification suivante à mener.

  - ✅ **v2 (09/11/2026) — deux symptômes distincts** : le sujet demande aussi de séparer
    la **file d'attente qui grossit** (famille `consumer_backlog`) du **paiement traité
    trop lentement** (famille `slow_processing`), avec l'analyse causale : **un traitement
    long engendre une attente grossissante**, mais les causes possibles d'un traitement
    lent (persistance lente, délai SLA trop court, retard amont) sont tranchées
    automatiquement par l'état de la file (`matrice v2` +
    `scripts/lib/test_diagnose_engine.py`, section « symptôme 2 »).

- ⬜ Prévoir une **vérification indépendante** de la cause (ex. croiser offsets/API Kafka avec le contenu réel de PostgreSQL, pas seulement un graphique).

---

## 7. Phase 4 — Automatisation des essais

**Objectif : scripter les incidents + le témoin, et mesurer les critères de la garantie.**

- ⬜ **Scripter l'injection des 3 incidents** :
  - **(A) retard consommateur** : `./scripts/run-scenario.sh consumer_lag` ou `network-perturb.sh` sur un service ;
  - **(B) réplication dégradée** : `./scripts/run-scenario.sh replication_lag` (arrêt temporaire d'un broker) ;
  - **(C) incident PostgreSQL** : dégradation (délais de persistance) ou indisponibilité, p.ex. `SIMULPIX_PERSIST_*_DELAY_MS` ou arrêt du conteneur `postgres`.
  - ✅ **(D) traitement lent — symptôme 2 (v2, 09/11/2026)** : cas `D-traitement-lent` dans
    `scripts/diagnostic-campaign.sh` (famille attendue `slow_processing`) — scénario
    **nominal lancé via l'UI** (`POST /control`, `action=run_scenario`,
    `decision_sla_seconds=3`, minimum 3) + perturbation réseau `kafka_latency`
    (250 ms/requête sur `pix-decision-engine`) injectée au milieu du flux :
    traitement long à **file vide** ; si la file grossit ensuite → `consumer_backlog`
    (lien causal : un traitement long engendre une attente grossissante). Le moteur
    lit le SLA de l'UI (`decision_sla_seconds`) dans `/health/details?decision_sla_seconds=3`.
- ⬜ Ajouter un essai **NOMINAL témoin** (contrôle des faux positifs).
- ⬜ Automatiser l'enchaînement : scénarios + collecte des relevés (commandes + sorties), **rejouable par un autre groupe**.
- ⬜ Mesurer pour tout diagnostic déterminé :
  - **justesse** du diagnostic (bonne cause identifiée) ;
  - **délai de détection** ;
  - **taux de faux positifs**.
- ⬜ Vérifier la **fraîcheur des sondes** : le système d'observation peut être affecté par l'incident (âge de collecte InfluxDB, etc.).

---

## 8. Phase 5 — Vérification croisée

**Objectif : prouver que le diagnostic repose sur la donnée, pas sur un graphique.**

- ⬜ Vérifier **CHAQUE conclusion contre Kafka ET PostgreSQL** (`tp-kafka.sh`, `tp-db.sh`, consumer groups, contenu des tables), pas seulement contre un dashboard Grafana.
- ⬜ Démontrer que le diagnostic **NE CONFOND PAS** : backlog consommateur / rejet métier / perte de données.

---

## 9. Phase 6 — Livrables et preuves

- ⬜ **Guide de diagnostic reproductible**, utilisable par un autre groupe pendant un incident (recette praticienne pas à pas).
- ⬜ **Note de conception courte** (choix techniques + justification des seuils).
- ⬜ **Protocole d'essai exécutable par un autre groupe** (commandes, scénarios, valeurs attendues).
- ⬜ **Relevés pour au moins 3 situations distinctes** (+ le témoin nominal).
- ⬜ **L'amélioration d'observabilité réellement exploitable** (le produit).
- ⬜ Déposer les preuves **synthétiques et rejouables** dans `preuves/` (pas les sorties volumineuses de `runtime/`, qui restent exclues).
- ⬜ **Conclusion** qui dit ce que la garantie couvre et ne couvre pas ; un résultat négatif est recevable s'il est rigoureusement établi.

---

## 10. Phase 7 — Extension

**🎁 Facultative (si temps)**

- ⬜ Comparer seuils **fixes** à une détection **adaptative** ; une règle explicite justifiée et évaluée suffit pour le minimum, l'apprentissage machine n'est pas exigé.

---

## 11. Phase 8 — Dimension théorique

**Encouragée**

- ⬜ Formaliser les hypothèses de cause, les observations qui les discriminent et le cas indéterminé.
- ⬜ Justifier les seuils ; mesurer délai de détection, faux positifs et erreurs de diagnostic sur des incidents connus.
- ⬜ Consulter `TP/SAé/analyse-recherche-limos.md` pour les pistes de lecture et préparer un échange avec l'enseignant/LIMOS.

---

## 12. Phase 9 — Rendu GitHub

**Tout au long, pas à la fin.**

- ⬜ Dépôt dérivé privé déjà créé : y travailler exclusivement (`https://github.com/TomPERRIN/ISIMA-TP-kafka-2026-...`).
- ⬜ Inviter `williamguyotlenat@icloud.com` comme collaborateur (dès le début).
- ⬜ Committer régulièrement avec des messages explicites ; documenter les contributions non-code de chaque membre.
- ⬜ Mettre à jour le **README racine** : sujet traité, garantie visée, démarche pour lancer la plateforme + rejouer les essais, mention « dérivé de simul-pix ».
- ⬜ Vérifier avant chaque publication : **pas de secrets** (`infra/kafka/secrets/`), **pas de `runtime/`** (volumes, caches, gros dumps) — preuves = fichiers synthétiques de `preuves/`.
- ⬜ Veille de la soutenance : passer le dépôt en **PUBLIC** et transmettre l'URL à l'enseignant (dépôt inaccessible = rendu manquant).

---

## 13. Règles importantes

1. Un offset Kafka seul **ne prouve pas** que la donnée métier est persistée : toujours vérifier le résultat dans PostgreSQL.
2. Un flux nominal OK **ne prouve pas** la résistance aux pannes/charge.
3. Ne pas nommer une garantie exactly-once sans préciser son périmètre (pas concerné en direct par ce sujet, mais principe général).
4. Toute évolution de contrat/topic/base doit être documentée et testée.
5. Distinguer **socle fourni / contribution minimale / extension**.
6. Lancer les batteries d'essais **tôt et régulièrement** (les essais demandent du temps machine).

---

## 14. Outils utiles

| Usage | Commande / URL |
|-------|----------------|
| Plateforme (démarrage / arrêt) | `./start.sh` · `./stop.sh` |
| Scénario | `./scripts/run-scenario.sh <scénario> <total> <débit>` |
| Arrêter / réinitialiser un scénario | `./scripts/stop-scenario.sh` · `./scripts/reset-scenario.sh` |
| Résumés d'état | `./scripts/tp-status.sh` · `./scripts/tp-kafka.sh` · `./scripts/tp-db.sh` |
| Validation observabilité | `./scripts/validate-observability.sh` |
| Perturbation réseau | `./scripts/network-perturb.sh <profil> <service>` (kafka_latency, kafka_loss, kafka_slow_link) · `./scripts/network-reset.sh` |
| Météo (service-health) | http://localhost:8082/ (JSON : `/health`, `/health/details`) |
| Parcours TP | http://localhost:8083/ |
| Grafana | http://localhost:3000 (admin / adminpass) |
| InfluxDB | http://localhost:8086 (admin / adminpass) |
| PostgreSQL | localhost:5432 (simulpix / simulpix) |

---

## 15. Cohérences avec d'autres sujets

- **sujet 2 + sujet 5** : traitement des rejets **ET** preuves d'observabilité (l'enjeu du sujet 5 est justement de distinguer backlog / rejet métier / perte).
- Rappel : le rendu doit rester exécutable **sans dépendre de l'achèvement d'un autre groupe**.
