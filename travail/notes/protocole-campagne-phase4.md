# Protocole de campagne d'incidents provoqués (phase 4)

> Objectif : mesurer les critères de la garantie #1 sur des incidents réels —
> **justesse** (#1 : la bonne famille est nommée), **délai de détection** (#3 : ≤ 30 s),
> **faux positifs** (#4 : 0 sur ≥ 5 nominaux) — avec des commandes **rejouables par un
> autre groupe**. Ce protocole décrit l'outil, les cas, et la lecture des relevés.
> La spécification du moteur reste la matrice v1
> ([matrice-symptomes-causes.md](matrice-symptomes-causes.md)).

## L'outil : deux commandes

```bash
# 1) la campagne complète (démarrage plateforme inclus si /health KO)
./scripts/diagnostic-campaign.sh

# 2) le rapport seul, sur des relevés existants
python3 scripts/lib/campaign_report.py preuves/phase4-campagne-<date>
```

Options de `diagnostic-campaign.sh` :

| Option | Effet |
|---|---|
| `--quick` | 2 témoins au lieu de 5 (débug) |
| `--skip-start` | ne relance pas `./start.sh` (plateforme déjà up) |
| `--cases A,B-replication` | sous-ensemble de cas |
| `SIMULPIX_CAMPAIGN_DIR=...` | répertoire des relevés (défaut `preuves/phase4-campagne-<date>`) |
| `REC_INTERVAL=...` | cadence du relevé — **11 s par défaut** (le moteur exige 2 échantillons espacés ≥ 10 s : une cadence plus courte bloque les règles à delta sur `needs_second_sample`) |

## Les cas d'essai

| Cas | Injection | Effet attendu sur les signaux | Famille attendue |
|---|---|---|---|
| `temoin-1..5` | aucune (`run-scenario.sh nominal 200 10`) | lag éclair possible, écart de persistance léger en début de flux | **aucun diagnostic d'incident** (0 faux positif) |
| `A-retard-lecture` | `consumer_lag` : décision 350 ms, persisters 0 ms | lag lecteur qui monte, écart de persistance stable | `consumer_backlog` |
| `B-replication` | `replication_lag` : kafka-3 arrêté 20 s | URP > 0, un broker absent de l'instantané réplication | `kafka_replication` |
| `C-postgres-indispo` | `docker compose stop postgres` 40 s au milieu du flux | `db_*` figé / sonde PG en échec alors que `generated` monte | `persistence` |
| `C-postgres-degrade` | `consumer_lag` : persisters 350 ms, décision 0 ms | lag persisters + écart de persistance qui S'ACCROÎT | `persistence` |

Profils : `A_PROCESSING_MS` (400 ms par défaut), `C_PERSIST_MS` (350),
`PG_OUTAGE_S` (40), `PG_INJECT_DELAY_S` (8), `PROFILE_TEMOIN` (« 200 10 »),
`PROFILE_INCIDENT` (« 600 20 »), `PROFILE_C_INDISPO` (« 800 15 »).

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
  `kafka_replication`, `persistence`) est celle attendue — l'apport du sujet est de nommer
  la bonne famille ; la cause nommée est reportée à titre d'information ;
- **faux positifs** = sur les témoins, tout verdict de famille incidente
  (`consumer_backlog` / `kafka_replication` / `persistence`) compte pour un ;
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
