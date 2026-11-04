# Plan d'architecture — SAÉ 5 (Observabilité et diagnostic d'incidents)

> Document séparé du `TODO-SAE5.md` pour lisibilité. Il décrit **le socle existant** (ce que le sujet appelle « socle fourni ») et **l'architecture cible de la contribution** (le moteur de diagnostic).
>
> Sources : `docker-compose.yml`, `docs/architecture/container-interactions.md`, `travail/notes/metriques-observabilite.md`, `preuves/phase2-recensement/recensement-signaux.md`.

---

## 1. Architecture du socle (existant)

Le pipeline Simul-Pix repose sur le `docker-compose.yml` (projet Compose fixe : `simulpix`) :

```text
              COUCHE GÉNÉRATION              COUCHE TRAITEMENT             COUCHE PERSISTANCE
   ┌─────────────────────────────┐   ┌───────────────────────────┐   ┌────────────────────────────┐
   │ pix-scenario-planner :8090  │   │ pix-validator :8086       │   │ persister-valid :8084 ──┐  │
   │            │                │   │  raw + retry → checked    │   │                        ▼  │
   │            ▼                │   │            │              │   │           postgres :5432  │
   │ pix-traffic-shaper :8089    │   │            ▼              │   │            ▲ ▲            │
   │            │                │   │ pix-decision-engine :8087 │   │           ┌─┘ └─┐          │
   │            ▼                │   │  checked → validated /    │   │ persister-rejected :8085   │
   │ generator :8081             │   │  rejected / decision      │   │                            │
   └─────────────┬───────────────┘   └─────────────┬─────────────┘   └────────────────────────────┘
                 │                                 │                             ▲
                 ▼                                 ▼                             │
   ╔═══════════════════════════════════════════════════════════════════════════════════════╗
   ║ ZONE KAFKA — cluster 3 brokers (kafka-1 :9092, kafka-2 :9093, kafka-3 :9094)           ║
   ║ topics : raw → checked → validated / rejected / decision → outcome (+ retry)           ║
   ║ replication factor = 3, min.insync.replicas = 2, 6 partitions                          ║
   ╚═══════════════════════════════════════════════════════════════════════════════════════╝
```

## 2. Couche d'observabilité (existant — le socle du sujet 5)

```text
   ┌─────────────────────┐    /health, /health/details, /control    ┌──────────────────────┐
   │ service-health      │◄─────────────────────────────────────────│ opérateur / scripts  │
   │ (météo) :8082/:8083 │                                          └──────────────────────┘
   └─────────┬───────────┘
             │ sondes : Kafka (admin API), PostgreSQL (count/gaps), HTTP de chaque service,
             │ history (échantillons), alerts.py (9 familles monocausales),
             │ lit /runtime/observability/latest-replication.json
             ▼
   ┌─────────────────────┐  1 échantillon / 10 s                ┌────────────┐  dashboards  ┌─────────┐
   │ metrics-collector   │─────────────────────────────────────►│ InfluxDB   │◄─────────────│ Grafana  │
   │ (shell + JMX + HTTP)│  (SIMULPIX_OBS_INTERVAL_SECONDS)     │ :8086      │  provisionnée│ :3000    │
   └─────────────────────┘                                      └────────────┘              └─────────┘
```

Points clés :

- **service-health** agrège tout et expose la météo ; `alerts.py` contient 9 familles d'alertes **monocausales** (seuils `SIMULPIX_WARNING_*`) — aucune ne croise les signaux : **c'est là que se situe l'apport du sujet 5**.
- **metrics-collector** écrit 7 mesures dans InfluxDB (pas de 10 s) : `kafka_broker`, `kafka_replication_partition`, `kafka_consumer_group`, `kafka_topic`, `simulpix_platform`, `simulpix_semantics`, `postgres_runtime`.
- **Grafana** : 4 dashboards provisionnés (General, Kafka, Persistence, Incidents).

## 3. Chaîne applicative (le trajet d'un Pix)

```text
[1] pix-scenario-planner    construit le plan d'émission
        │
        ▼
[2] pix-traffic-shaper      calcule le trafic réel (modèles bursty, football, …)
        │
        ▼
[3] generator               fabrique et publie les Pix          ──► topic raw
        │
        ▼
[4] pix-validator           contrôle d'entrée                   ──► topic checked
        │
        ▼
[5] pix-decision-engine     décision métier (SLA 10 s)          ──► validated / rejected / decision
        │                                          │
        │            ┌─────────────────────────────┘
        ▼            ▼
[6] persister-valid / persister-rejected                ──► PostgreSQL (tables durables)
        │
        ▼
[7] pix-outcome-publisher   décision finale client             ──► topic outcome
        │
        ▼
[8] service-health + Grafana                                (lecture croisée des 3 vues)
```

## 4. Architecture cible de la contribution (ce que le sujet 5 ajoute)

**Principe** : ne pas ajouter de nouvelle sonde, mais **relier** les signaux existants dans un moteur de diagnostic.

```text
   Signaux existants (AUCUNE nouvelle sonde)
   ┌────────────────────────────────────────────────────────────────┐
   │ • /health/details : counts, gaps, timings, history, alerts     │
   │ • sondes Kafka    : under_replicated, offline, lag par groupe  │
   │ • sondes PG       : seconds_since_last_*, db counts            │
   │ • InfluxDB        : 7 séries, pas de 10 s                      │
   │ • latest-replication.json (fraîcheur transport)                │
   └──────────────┬─────────────────────────────────────────────────┘
                  │ agrégation (curl unique + requêtes Influx/PG ciblées)
                  ▼
   ┌────────────────────────────────────────────────────────────────┐
   │  MOTEUR DE DIAGNOSTIC (nouveau)                                 │
   │                                                                  │
   │  0) GARDE-FOU FRAÎCHEUR : âge max des mesures utilisées          │
   │     → si sondes périmées (ex. collecteur arrêté) : INDÉTERMINÉ   │
   │                                                                  │
   │  1) RÈGLES DE CROISEMENT (machine à états) :                     │
   │     • lag groupes applicatifs ↑ + sous-réplication = 0           │
   │       + DB converge            → A : RETARD CONSOMMATEUR         │
   │     • under_replicated > 0 ou broker offline                     │
   │       + persistance normale   → B : RÉPLICATION DÉGRADÉE         │
   │     • gap persistance stable + db counts ≠ compteurs métier      │
   │       + offsets outcome cohérents → C : PERSISTANCE DÉFAILLANTE  │
   │     • aucun critère tranché   → INDÉTERMINÉ + vérif. suivante    │
   │                                                                  │
   │  2) SORTIE À 4 ÉTATS :                                           │
   │     {retard_consommateur | réplication | persistance |           │
   │      indéterminé} + cause + preuves + âge des sondes             │
   │     → toute conclusion ≠ indéterminé exige la double lecture     │
   │       Kafka ET PostgreSQL (critère #5)                           │
   └──────────────┬─────────────────────────────────────────────────┘
                  │
       ┌──────────┼─────────────────┬──────────────────┐
       ▼          ▼                 ▼                  ▼
  commande CLI   vue HTTP        panneau Grafana    journal preuves/
  (diagnostic)   (extension)     (extension)        (relevés JSON)
```

**Flux de la garantie** (lecture d'un incident) :

```text
symptôme visible (Grafana / météo)
        │
        ▼
moteur de diagnostic ──► état {A, B, C, indéterminé} + fraîcheur des sondes
        │
        ▼
vérification indépendante : tp-kafka.sh (offsets/groupes) + tp-db.sh (tables PG)
        │
        ▼
cause confirmée ou invalidée ──► relevé archivé dans preuves/
```

## 5. Les 3 vues de lecture (métier / transport / persistance)

| Vue | Signaux lus | Incident détecté | Faux amis à éviter |
|-----|-------------|------------------|--------------------|
| **Métier** | counts (generated/processed/accepted/rejected/outcome), taux de rejet, SLA breaches | impact Pix (rejets `PROCESSING_TIMEOUT`, dépassement TTL 10 s) | rejet métier ≠ incident ; `lag_total = 1` en nominal est **normal** |
| **Transport** | under_replicated_partitions, brokers offline, lag par consumer group, ancienneté backlog | **B** (réplication dégradée), retards consommateurs | backlog ≠ perte ; moyennes sans p95/p99 |
| **Persistance** | persistence gap, db counts vs compteurs métier, offsets topic vs outcome | **C** (persistance défaillante) | un offset Kafka seul ne prouve rien : toujours lire PG |

## 6. Intégration dans le dépôt

| Élément | Emplacement | Rôle |
|---------|-------------|------|
| Moteur de diagnostic | `scripts/diagnose.sh` (+ éventuellement un endpoint `service-health`) | règle de croisement, sortie à 4 états |
| Machine à états / règles | module Python (`scripts/lib/` ou `tests/`) | testable unitairement (critère #5) |
| Incidents injectés | `scripts/run-scenario.sh`, `network-perturb.sh`, `SIMULPIX_PERSIST_*_DELAY_MS`, `docker stop postgres` | familles A, B, C |
| Batterie d'essais | `scripts/diagnostic-campaign.sh` | enchaîne incidents + témoin, archive les relevés |
| Preuves | `preuves/phase3-*/`, `preuves/phase4-*/` | matrice, relevés par incident |
| Guide praticien | `docs/operations/guide-diagnostic.md` | recette pas à pas pendant un incident |
| Note de conception | `docs/architecture/diagnostic-engine-design.md` | choix techniques + justification des seuils |
