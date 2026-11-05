# Campagne d'incidents provoqués — synthèse

| Critère | Résultat |
|---|---|
| #1 justesse (3 familles) | 4/4 |
| #4 faux positifs (témoin) | 0 (sur 5 témoins, 70 échantillons) |
| #3 délai max | 25.0 |

| Cas | Échant. | Verdicts | Cause nommée | Délai | PASS |
|---|---|---|---|---|---|
| A-retard-lecture | 11 | consumer_backlog×10, indeterminate×1 | reader_backlog | 15.0 s | ✅ |
| B-replication | 10 | healthy×8, kafka_replication×2 | replication_catchup | 25.0 s | ✅ |
| C-postgres-degrade | 10 | healthy×6, indeterminate×1, persistence×3 | persister_slow | 14.0 s | ✅ |
| C-postgres-indispo | 9 | healthy×5, indeterminate×1, persistence×3 | service_down:postgres | 3.0 s | ✅ |
| temoin-1 | 14 | healthy×14 | FP=0 | — | ✅ |
| temoin-2 | 14 | healthy×14 | FP=0 | — | ✅ |
| temoin-3 | 14 | healthy×14 | FP=0 | — | ✅ |
| temoin-4 | 14 | healthy×14 | FP=0 | — | ✅ |
| temoin-5 | 14 | healthy×14 | FP=0 | — | ✅ |

- A-retard-lecture : états indéterminés = needs_second_sample
- C-postgres-degrade : états indéterminés = needs_second_sample
- C-postgres-indispo : états indéterminés = needs_second_sample
