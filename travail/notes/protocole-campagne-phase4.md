# Protocole de campagne d'incidents provoqués (phase 4)

> Objectif : mesurer les critères de la garantie #1 sur des incidents réels —
> **justesse** (#1 : la bonne famille est nommée), **délai de détection** (#3 : ≤ 30 s),
> **faux positifs** (#4 : 0 sur ≥ 5 nominaux) — avec des commandes **rejouables par un
> autre groupe**. Ce protocole décrit l'outil, les cas, et la lecture des relevés.
> La spécification du moteur reste la matrice v2 (2 symptômes distincts : file qui
> grossit / traitement lent)
> ([matrice-symptomes-causes.md](matrice-symptomes-causes.md)).

## L'outil : trois commandes

```bash
# 0) UNE commande, UN terminal : exporteur Grafana (frise en direct) + campagne
./scripts/demo-diagnostic.sh

# 1) la campagne seule (démarrage plateforme inclus si /health KO)
./scripts/diagnostic-campaign.sh

# 2) le rapport seul, sur des relevés existants
python3 scripts/lib/campaign_report.py preuves/phase4-campagne-<date>
```

`demo-diagnostic.sh` démarre l'exporteur Grafana **en arrière-plan** (cache dédié
`runtime/diagnostic-cache-grafana.json` : la frise ne se mélange pas aux 2 échantillons
de la campagne, sinon les lectures entrelacées tombent sous le span de 10 s et le
moteur refuse de conclure), affiche l'URL du dashboard, lance la campagne puis arrête
l'exporteur (trap EXIT). Toutes les options de la campagne sont transmises.

Options de `diagnostic-campaign.sh` :

| Option | Effet |
|---|---|
| `--quick` | 2 témoins au lieu de 5 (débug) |
| `--skip-start` | ne relance pas `./start.sh` (plateforme déjà up) |
| `--pause` | attend une **Entrée** entre les cas (observation de la frise pendant l'incident) |
| `--cases A,B-replication` | sous-ensemble de cas |
| `SIMULPIX_CAMPAIGN_DIR=...` | répertoire des relevés (défaut `preuves/phase4-campagne-<date>`) |
| `REC_INTERVAL=...` | cadence du relevé — **11 s par défaut** (le moteur exige 2 échantillons espacés ≥ 10 s : une cadence plus courte bloque les règles à delta sur `needs_second_sample`) |
| `SIMULPIX_EXPORT_INTERVAL=...` | cadence de l'exporteur Grafana dans la démo — 15 s par défaut |
| `SIMULPIX_GRAFANA_URL=...` | URL du dashboard affichée par la démo (défaut `http://localhost:3000`) |

## Les cas d'essai

| Cas | Injection | Effet attendu sur les signaux | Famille attendue | Cause nommée attendue |
|---|---|---|---|---|
| `temoin-1..5` | aucune (`run-scenario.sh nominal 200 10`) | lag éclair possible, écart de persistance léger en début de flux | **aucun diagnostic d'incident** (0 faux positif) | — |
| `A-retard-lecture` | `consumer_lag` : décision 350 ms, persisters 0 ms | lag lecteur qui monte, écart de persistance stable | `consumer_backlog` | `reader_backlog` |
| `B-replication` | `replication_lag` : kafka-3 arrêté 20 s | URP > 0, un broker absent de l'instantané réplication | `kafka_replication` | `broker_down` ou `replication_catchup` |
| `C-postgres-indispo` | `docker compose stop postgres` 40 s au milieu du flux | `db_*` figé / sonde PG en échec alors que `generated` monte | `persistence` | `postgres_unavailable` |
| `C-postgres-degrade` | `consumer_lag` : persisters 350 ms, décision 0 ms | lag persisters + écart de persistance qui S'ACCROÎT | `persistence` | `persister_slow` |
| `D-traitement-lent` | nominal via l'UI (`POST /control`, `decision_sla_seconds=3`) + `kafka_latency` (250 ms sur `pix-decision-engine`) au milieu du flux | latence > SLA × 80 % sur 2 échantillons, file vide | `slow_processing` | `sla_config` |

Profils : `A_PROCESSING_MS` (400 ms par défaut), `C_PERSIST_MS` (350),
`PG_OUTAGE_S` (40), `PG_INJECT_DELAY_S` (8), `PROFILE_TEMOIN` (« 200 10 »),
`PROFILE_INCIDENT` (« 600 20 »), `PROFILE_C_INDISPO` (« 800 15 ») ;
cas D : `D_SLA_SECONDS` (3), `D_TOTAL_MESSAGES` (1500), `D_RATE_PER_SECOND` (20),
`D_WARMUP_S` (25).

## Problèmes étudiés → diagnostic en sortie

| Problème (symptôme) | Cas de campagne | Verdict/cause attendue | Preuve archivée |
|---|---|---|---|
| File d'attente qui grossit (symptôme 1) | `A-retard-lecture` | `consumer_backlog` / `reader_backlog` | `preuves/phase4-campagne-2026-11-05/A-retard-lecture/` |
| Réplication dégradée | `B-replication` | `kafka_replication` / `broker_down` ou `replication_catchup` | `preuves/phase4-campagne-2026-11-05/B-replication/` |
| PostgreSQL indisponible | `C-postgres-indispo` | `persistence` / `postgres_unavailable` | `preuves/phase4-campagne-2026-11-05/C-postgres-indispo/` |
| PostgreSQL dégradé | `C-postgres-degrade` | `persistence` / `persister_slow` | `preuves/phase4-campagne-2026-11-05/C-postgres-degrade/` |
| Paiement traité trop lentement (symptôme 2) | `D-traitement-lent` | `slow_processing` / `sla_config` | à archiver au rejeu live (`./scripts/demo-diagnostic.sh --cases D-traitement-lent`) |
| Aucun incident (témoin) | `temoin-1..5` | `healthy`, 0 faux positif | `preuves/phase4-campagne-2026-11-05/temoin-*/` |

Le rapport (`campaign_report.py`) **vérifie automatiquement cette couverture** :
section « Couverture des problèmes étudiés » de `SYNTHESE.md` — une famille absente
des verdicts de son cas attendu est signalée ❌, même sur un run partiel (`--cases`).
Protégé par les tests `scripts/lib/test_campaign_report.py` (chaque famille d'incident
doit rester attendue par au moins un cas).

## Comment le relevé est pris

Pendant chaque cas, le script enregistre **toutes les 11 s** le verdict complet du moteur
(`diagnose.sh --json`, compacté) dans `<cas>/verdicts.jsonl`, ajouté à un journal
d'évènements horodatés `<cas>/events.jsonl` (`scenario_launch`, `scenario_ready`,
`fault_injected`, `fault_recovered`, …) et à la sortie brute du scénario `<cas>/scenario.log`.

Définition des mesures (appliquées par `campaign_report.py`) :

- **délai de détection** = `t_première_conclusion_conforme − t_injection` (cas PG-arrêté),
  sinon `− t_scenario_ready` (incidents injectés par le scénario lui-même : A, B, C-dégradé) ;
  ⚠️ ce délai est **quantifié par la cadence du relevé** (11 s) : le délai réel du moteur est
  majoré d'au plus une cadence ;
- **justesse** = première conclusion dont la **famille** (`consumer_backlog`,
  `slow_processing`, `kafka_replication`, `persistence`) est celle attendue — l'apport
  du sujet est de nommer la bonne famille ; la cause nommée est reportée à titre
  d'information ;
- **faux positifs** = sur les témoins, tout verdict de famille incidente
  (`consumer_backlog` / `slow_processing` / `kafka_replication` / `persistence`)
  compte pour un ;
- les verdicts `indeterminate` sont **attendus et sains** au début de chaque cas
  (`needs_second_sample` : le moteur ne conclut pas sur une seule lecture) et sont listés
  dans la synthèse.

## Limites connues et points d'attention

- **Critère #6 (fraîcheur)** : arrêter `metrics-collector` ne gèle PAS `/health/details`
  (le `timestamp` du payload est ré-émis à chaque requête, `captured_at` vient du cache
  interne de service-health). La démonstration en live du critère #6 se fait donc par
  l'indisponibilité du service observé (le relevé note `unreachable`, le moteur ne conclut
  jamais) ; le cas `probes_stale` (payload vieilli de 300 s) est couvert par le test
  unitaire `test_stale_probes_never_conclude...`.
- Les incidents A/B/C-dégradé sont injectés **par les variables du scénario** : le délai
  mesuré inclut les quelques secondes de démarrage du générateur.
- Les témoins génèrent un scénario de 20 s (`nominal 200 10`) : en fin de flux, un verdict
  `healthy/fin de scénario` (`upstream_stopped_normal`) est NORMAL.
- Chaque cas resette les consommateurs et le cache de diagnostic
  (`runtime/diagnostic-cache.json`) : aucun état ne fuit d'un cas à l'autre.
