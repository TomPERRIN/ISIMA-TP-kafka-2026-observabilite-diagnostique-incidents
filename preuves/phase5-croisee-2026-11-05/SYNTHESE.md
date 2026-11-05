# Phase 5 — vérification croisée et non-confusion

| Cas | Verdicts moteur | Causes | Convergence (état final) | Non-confusion |
|---|---|---|---|---|
| A-draine | consumer_backlog×10, healthy×3, indeterminate×2 | reader_backlog, upstream_stopped_normal | db=20+580 / outcome=600 / écarts=0 / lagMax=0 (fermeture ✔) | PASS |
| B-replication-croise | healthy×7, kafka_replication×1 | replication_catchup, upstream_stopped_normal | db=600+0 / outcome=600 / écarts=0 / lagMax=0 (fermeture ✔) | PASS |
| C-rattrapage | healthy×13, persistence×3 | postgres_unavailable, service_down:postgres, upstream_stopped_normal | db=800+0 / outcome=800 / écarts=0 / lagMax=0 (fermeture ✔) | PASS |
| rejets-metier | healthy×8 | upstream_stopped_normal | db=0+300 / outcome=300 / écarts=0 / lagMax=0 (fermeture ✔) | PASS |

**A-draine** : le moteur a nommé `consumer_backlog` pendant l'incident, puis la convergence finale (generated==raw_end, outcome==db_total, écarts 0) prouve que le backlog était transitoire : TOUTE la donnée est finalement persistée, AUCUNE perdue (rejetés métier en base : 580)
  - contrôles : {"backlog_détecté": true, "rattrapage_complet": true, "db_rejected": 580, "lag_max_final": 0}
**B-replication-croise** : verdict `kafka_replication` retranché par la capture tp-kafka-final.txt (brokers/contrôleur/URP en direct), pas par un dashboard
  - contrôles : {"réplication_détectée": true}
**C-rattrapage** : le moteur a nommé `persistence` pendant la panne ; convergence finale (outcome==db_total, écarts 0) : PostgreSQL rejoint après retour, rattrapage complet, AUCUNE perte mesurée
  - contrôles : {"panne_détectée": true, "rattrapage_complet": true, "db_total": 800, "outcome": 800}
**rejets-metier** : rejets métier = 300 vus / 300 en PG, cohérents ; le diagnostic n'a sonné AUCUN incident (rejet métier ≠ incident)
