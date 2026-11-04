# Recensement des signaux observables — SAÉ 5

Recensement réalisé le 2026-11-03 sur la plateforme démarrée en nominal
(voir `reference-nominale-2026-11-03/` pour la mesure de référence et
son protocole). Ce document alimente la note de conception : notre
règle de diagnostic devra RELIER ces signaux existants, sans sonde
nouvelle.

## 1. Cibles de lecture disponibles

| Cible | Contenu |
|---|---|
| `http://localhost:8082/health` | statut global synthétique + `alerts[]` + control |
| `http://localhost:8082/health/details` | tout le détail JSON (voir §2) |
| `./scripts/tp-status.sh` | lecture guidée 6 sections |
| `./scripts/tp-kafka.sh` | topics, réplication/groupes Kafka |
| `./scripts/tp-db.sh` | comptes PostgreSQL (validés/rejetés/sans décision) |
| Dashboards Grafana (4) | séries temporelles via InfluxDB (§3) |

## 2. Structure de `/health/details` (profondeur utile)

- Racine : `status`, `timestamp`, `run`, `alerts`, `services`, `history`,
  `metrics`, `details`, `control`.
- `details.<service>` : un bloc par service (generator, validator,
  decision-engine, outcome-publisher, persister-*, scenario-planner,
  traffic-shaper, grafana, influxdb, postgres).
- `details.kafka` :
  - `broker_count`, `bootstrap_servers`, `topics` ;
  - `consumer_groups` : 5 groupes applicatifs du run courant
    (`group_id`, `lag_total`) ;
  - `replication` : **`captured_at`** (fraîcheur implicite !) + `brokers[3]`
    (`under_replicated_partitions`, `active_controller_count`,
    `offline_partitions_count`) + `partitions[]`
    (`topic`, `partition`, `leader`, `broker`, `lag_offsets`,
    `lag_seconds_estimate(null souvent)`, `log_dir_offsets`);
  - `lag_alert` : libellé texte.
- `details.postgres` : `validated_count`, `rejected_count`,
  `rejected_sla_breaches`, `validated_within_sla_count`, latences
  moyennes, tailles de bases/tables, `top_rejection_reasons`,
  `rejections_by_scenario`, `seconds_since_last_validated/rejected`,
  `status`, `active_connections`, `last_*_at`.
- `details.network` : état de la perturbation réseau active.
- `metrics.counts` : compteurs métier (generated, processed, validated,
  rejected, outcome_count, db_*, endpoint offsets par topic), sémantiques
  codes (producer_acks_code, consumer_commit_strategy_code...),
  lag par étape (`pix_validator_lag_total`,
  `pix_decision_engine_lag_total`, `pix_outcome_publisher_lag_total`,
  `persister_valid_lag_total`, `persister_rejected_lag_total`),
  tailles PostgreSQL.
- `metrics.gaps` : `persistence_gap_valid`, `persistence_gap_rejected`
  (écart "Kafka -> BDD").
- `metrics.timings` : `avg_validation_latency_ms`,
  `avg_rejection_latency_ms`, `estimated_oldest_lag_seconds` (ratio
  lag/débit — PAS un âge du plus ancien message), 
  `seconds_since_last_validated/rejected` (fraîcheur métier PG !).
- `history` : échantillons append-on-change (cf. §5).

## 3. Mesures InfluxDB (bucket `simulpix_observability`)

Rafraîchies toutes les **10 s** par `metrics-collector`
(`SIMULPIX_OBS_INTERVAL_SECONDS: 10`).

| Measurement | Champs utilisés par les dashboards |
|---|---|
| `kafka_broker` | `under_replicated_partitions`, `offline_partitions_count`, `active_controller_count` |
| `kafka_replication_partition` | `lag_offsets` (retard de réplication par partition) |
| `kafka_consumer_group` | `lag_total` (retard par groupe) |
| `kafka_topic` | `end_offsets_total` (volume observé par topic) |
| `simulpix_platform` | `generated`, `processed`, `validated`, `rejected`, `outcome_count`, `persistence_gap_valid/rejected`, `avg_validation_latency_ms`, `validated/rejected_within_sla_count`, `validated/rejected_sla_breaches`, `db_validated_count`, `db_rejected_count`, `estimated_oldest_lag_seconds`, `pending_result_count` |
| `simulpix_semantics` | `producer_acks_code`, `producer_retries`, `consumer_commit_strategy_code`, `consumer_auto_commit_code`, `kafka_exactly_once_chain_count` |
| `postgres_runtime` | `validated_count`, `rejected_count`, `database_size_bytes`, tailles de tables |

Dashboards : `Simul-Pix - General Dashboard` (métier), 
`Simul-Pix - Kafka Dashboard` (cluster + lag + backlog, avec vues
actuel/max 5 min/max 30 min), `Simul-Pix - Persistence Dashboard`
(écarts Kafka=>BDD, débit et latence d'écriture, capacité), 
`Simul-Pix Incidents` (sous-réplication, partitions offline,
contrôleur, décisions hors SLA).

## 4. Alertes existantes (`service-health/alerts.py`)

Toutes en niveau `warning` sauf service `down` (`error`) ; elles
conduisent à `status` = `warning`/`degraded` :

| kind | Déclencheur | Seuil (variable d'env) |
|---|---|---|
| `service_status` | service down/degraded | — |
| `kafka_lag` | `lag_total >= 1` par groupe | `SIMULPIX_WARNING_LAG_TOTAL=1` |
| `kafka_replication` | `under_replicated_partitions_total > 0` | — |
| `reject_rate` | taux rejet récent >= 30 % | `SIMULPIX_WARNING_REJECT_RATE_PERCENT=30` (min 10 traités) |
| `persistence_gap_valid` | écart validated >= 1 | `SIMULPIX_WARNING_PERSISTENCE_GAP=1` |
| `persistence_gap_rejected` | écart rejected >= 1 | idem |
| `decision_sla` | fuite SLA acceptée/rejetée > 0/s | — |
| `oldest_lag` | backlog estimé >= 20 s | `SIMULPIX_WARNING_OLDEST_LAG_SECONDS=20` |
| `network_perturbation` | perturbation tc active | — |

Lecture critique (notre marge d'apport) : chaque alerte signale un
symptôme **d'une famille**, aucune ne fait le croisement (ex. :
distinguer `kafka_lag` pur d'un `kafka_lag` + `kafka_replication`
concurrent, ou un `persistence_gap` transitoire pendant le flux d'une
absence d'écriture persistante après drainage).

## 5. Observations structurantes (mesurées le 03/11/2026)

1. **Cadences** : collecteur Influx = 10 s fixe ; historique 
   `service-health` = ~5.7–12.3 s sous charge ; **l'historique gèle en
   idle** (observé : dernier échantillon de 710 s lors d'inactivité)
   — il faut donc un indicateur de fraîcheur pour ne PAS conclure sur
   des données mortes (c'est le critère d'acceptation #6).
2. **Fraîcheur partielle** : `replication.captured_at` est horodaté ;
   `postgres.last_validated_at` existe ; mais **aucune clé d'âge
   global de sonde** n'existe → contribution nouvelle (horloge de
   fraîcheur composite).
3. **`estimated_oldest_lag_seconds`** est un ratio lag agrégé/débit —
   pas l'âge du plus ancien message (limite notée aussi dans
   `TP/SAé/limites-du-socle.md`) ; à ne pas réutiliser telle quelle.
4. **Bruit nominal** : au repos le lag repasse à 0 (la plateforme est
   bien dimensionnée) — toute règle "lag > 0 = incident" serait trop
   brutale ; c'est pourquoi la candidate #1 impose le croisement.

## 6. Réponses aux questions de conception (brouillons)

- **Backlog temporaire vs perte** : backlog = `lag_total > 0` MAIS
  drainé ensuite, et, après le rattrapage, les compteurs convergent
  (réel DB == compteurs métier, `persistence_gap_* == 0`) ; perte =
  `persistence_gap_* > 0` **stables après drainage** (ou offsets du
  topic applicatif `outcome` < compteurs métie attendus). Scénario de
  preuve : `consumer_lag` (rattrapage) contre incident persistance.
- **Applicatif vs cluster** : lire les deux familles ENSEMBLE.
  Cluster => `details.kafka.replication.brokers[]` (under_replicated,
  offline, active_controller). Applicatif => `kafka.consumer_groups`
  lag + compteurs d'étape. Cluster sain + lag qui monte = souci
  applicatif ; sous-réplication > 0 = transport.
- **Indicateur -> impact métier Pix** : les compteurs
  `*_sla_breaches` / `*_within_sla_count` (breaches = décisions après
  TTL de 10 s) relient le signal technique (lag) à "un paiement Pix
  arrivé trop tard" — aussi visibles en PostgreSQL et en Influx.
- **Conclure sur la persistance → lire ENSEMBLE** :
  `persistence_gap_valid/rejected` (persistants) +
  `db_validated_count / db_rejected_count` par rapport aux compteurs
  métie `validated / rejected` + `topic end offsets`
  (outcome/validated/rejected). Un offset seul ne prouve rien.
- **Signal fiable pour déclencher une alerte** : les signaux
  `under_replicated > 0`, `persistence_gap > 0` stabilisés (écart
  constant après drainage) ou dérive persistante du lag au-delà d'un
  seuil ; à l'inverse `lag_total >= 1` seul est trop bruité
  (normal pendant une émission).

## 7. Limites du socle relevées (TP/SAé/limites-du-socle.md)

- `estimated_oldest_lag_seconds` : estimation non bornée (cf. §5).
- moyennes affichées sans p95/p99 ; décisions absentes restent dans
  le bilan (pas d'état « en attente » explicite ;
  `pending_result_count` l'expose partiellement).
- JSON des contrats événements : des exemples, à relever côté code.
- Note : quand on éprouve la famille persistance, l'incident peut
  affecter les sondes elles-mêmes (compteurs, PostgreSQL) → d'où le
  critère #6 (fraîcheur) et l'état « indéterminé » recommandé.
