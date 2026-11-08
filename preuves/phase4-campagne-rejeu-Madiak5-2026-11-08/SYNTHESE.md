# Campagne d'incidents provoqués — synthèse

| Critère | Résultat |
|---|---|
| #1 justesse (3 familles) | 4/4 |
| #4 faux positifs (témoin) | 0 (sur 5 témoins, 30 échantillons) |
| #3 délai max | 20.0 |

| Cas | Échant. | Verdicts | Cause nommée | Délai | PASS |
|---|---|---|---|---|---|
| A-retard-lecture | 9 | consumer_backlog×8, indeterminate×1 | reader_backlog | 15.0 s | ✅ |
| B-replication | 9 | healthy×7, kafka_replication×2 | replication_catchup | 20.0 s | ✅ |
| C-postgres-degrade | 9 | healthy×6, indeterminate×1, persistence×2 | persister_slow | 16.0 s | ✅ |
| C-postgres-indispo | 9 | healthy×5, persistence×4 | service_down:postgres | 5.0 s | ✅ |
| temoin-1 | 6 | healthy×6 | FP=0 | — | ✅ |
| temoin-2 | 6 | healthy×6 | FP=0 | — | ✅ |
| temoin-3 | 6 | healthy×6 | FP=0 | — | ✅ |
| temoin-4 | 6 | healthy×6 | FP=0 | — | ✅ |
| temoin-5 | 6 | healthy×6 | FP=0 | — | ✅ |

- A-retard-lecture : états indéterminés = needs_second_sample
- C-postgres-degrade : états indéterminés = needs_second_sample
