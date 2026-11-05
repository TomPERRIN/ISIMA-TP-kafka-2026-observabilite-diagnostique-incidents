# Protocole de phase 5 — vérification croisée et non-confusion

> Objectif : prouver que le diagnostic repose sur la **donnée**, pas sur un graphique —
> chaque conclusion est retranchée par Kafka ET PostgreSQL en direct (`tp-kafka.sh`,
> `tp-db.sh`), et le moteur **ne confond pas** trois réalités qui se ressemblent côté
> métriques : un *backlog transitoire*, un *rejet métier attendu*, une *perte de données*.
> À l'issue : démonstration de la candidate de garantie **#2**.

## L'outil

```bash
./scripts/cross-check.sh                 # les 4 démonstrations (plateforme démarrée si besoin)
./scripts/cross-check.sh --cases A-draine,rejets-metier   # sous-ensemble
python3 scripts/lib/crosscheck_report.py preuves/phase5-croisee-<date>   # rapport seul
```

Réglages : `REC_INTERVAL` (11 s ≥ span moteur), `A_WINDOW` (120 s), `C_WINDOW` (150 s),
`WA_DRAIN_MAX` (420 s), `C_DRAIN_MAX` (600 s), `C_OUTAGE_S` (40 s), fenêtre de fin
`TAIL_WINDOW` (40 s) pour capturer le verdict du moteur APRÈS convergence.

## Les 4 démonstrations

| Cas | Levier | Ce qui est prouvé |
|---|---|---|
| `A-draine` | `consumer_lag` décision 400 ms / persisters 0 ms (`600 20`) | le moteur nomme `consumer_backlog` pendant l'incident ; après drainage : lag ~0, écarts de persistance 0, **delta compteurs == delta écrits en base** → backlog transitoire, **0 donnée perdue** |
| `B-replication-croise` | `replication_lag` kafka-3 arrêté 20 s | verdict `kafka_replication` retranché par l'état Kafka **en direct** (brokers, offline, contrôleur) capture `tp-kafka-final.txt`, pas par un dashboard |
| `C-rattrapage` | `docker compose stop postgres` 40 s au milieu d'un flux `nominal 800 15` | verdict `persistence` pendant la panne ; au retour de PG, **rattrapage complet** (convergence deltas) ou, si le socle committe sans écrire, **écart stable = perte mesurée** — la réalité est relevée, jamais supposée |
| `rejets-metier` | scénario `errors_simple` (`300 10`) | le diagnostic sonne **AUCUN incident** bien que des rejets existent ; les compteurs `rejected` sont cohérents avec les tables PG (`tp-db.sh`) → rejet métier ≠ incident |

## Captures croisées par cas

`capture <cas> <label>` enregistre dans `<cas>/` :

| Fichier | Contenu |
|---|---|
| `tp-kafka-<label>.txt` | lecture Kafka en direct (tp-kafka.sh horodaté) |
| `tp-db-<label>.txt` | lecture PostgreSQL en direct (tp-db.sh : totaux + motifs de rejet) |
| `health-<label>.json` | payload `/health/details` complet |
| `counts-<label>.json` | compteurs clés du payload (généré, offsets topics, outcome, db, écarts, lag_max) |
| `convergence-latest.json` | contrôle de convergence (deltas depuis t0 : `generated==raw_end`, `outcome==db_total`, écarts 0) |

Labels : `t0` (baseline avant scénario), `pendant-backlog` / `pendant-panne` (pendant
l'incident), `final` (après convergence). Le moteur : verdicts horodatés dans
`verdicts.jsonl` (cadence 11 s), journal d'évènements `events.jsonl`.

## Définition des contrôles de non-confusion

- **backlog ≠ perte** : pendant l'incident, le moteur conclut la famille à retard
  (A) ; à la fin, `delta(outcome) == delta(db_validated + db_rejected)` ET écarts de
  persistance 0 : chaque décision finit écrite en base (validée **ou** rejetée métier).
  Les rejets métier éventuels pendant un long backlog (`processing_timeout`) sont des
  rejets **persistés en base**, pas une perte.
- **rejet métier ≠ incident** : un scénario à erreurs implémentées ne provoque
  aucun verdict de famille incidente ; les compteurs `rejected` == lignes en PG.
- **perte = écart stable** : un écart de persistance qui ne se résorbe pas après
  drainage (deltas figés) est la signature d'une perte — de la matrice v1
  (« l'écart qui S'ACCROÎT sans jamais se résorber »).
- **fraîcheur** (critère #6) : les captures `health-*.json` portent le timestamp du
  payload ; toute lecture sur données gelées est visible dans les verdicts.

## Limites connues

- Les durées de convergence dépendent de la vitesse des consommateurs : le drainage
  A (~210 s théorique) est couvert par `WA_DRAIN_MAX=420`, le rattrapage C par
  `C_DRAIN_MAX=600` ; au timeout le cas est reporté « non convergé » avec ses deltas
  (aucune conclusion déguisée).
- La campagne de phase 4 doit avoir été menée (postes, compteurs globaux cumulés depuis
  le début des essais) : tous les contrôles utilisent des **deltas depuis t0**.
