# Analyse théorique du diagnostic — SAÉ 5

## 1. Objet et portée
Ce document formalise le modèle théorique sous-jacent au moteur de diagnostic (`scripts/lib/diagnose_engine.py`), la discrimination formelle des hypothèses de cause et la justification mathématique de nos choix d'échantillonnage temporel.

## 2. Vocabulaire : symptôme, hypothèse, observation discriminante, abstention
* **Symptôme** : manifestation observable au niveau des compteurs ou de l'API (ex. hausse de la file d'attente, partitions sous-répliquées).
* **Hypothèse ($H$)** : cause racine latente suspectée (retard consommateur, panne de réplication, défaillance PostgreSQL).
* **Observation discriminante** : règle logique croisant plusieurs métriques pour invalider une cause concurrente.
* **Abstention ($\Omega$)** : état "indéterminé" obligatoire lorsque les données sont incomplètes ou périmées, garantissant l'absence de faux diagnostic.

## 3. Formalisation
- $\mathcal{H} = \{\text{consumer\_backlog}, \text{kafka\_replication}, \text{persistence}, \text{healthy}\}$
- $\mathbf{O} = \text{observations disponibles via les sondes et les API}$
- $f: \mathbf{O} \to \mathcal{H} \cup \{\bot\}$, avec $\bot = \text{« indéterminé »}$
- L'indéterminé est une sortie de première classe, pas un échec.

## 4. Hypothèses et observations discriminantes

| Symptôme | Hypothèse 1 | Hypothèse 2 | Observation qui tranche | Référence |
|---|---|---|---|---|
| Hausse du lag | Retard consommateur ($H_A$) | Persistance lente ($H_C$) | Évolution de l'écart de persistance (`persistence_gap`) entre deux lectures | `diagnose_engine.py` |
| URP > 0 | Panne de broker ($H_B$) | Rattrapage de réplication | Nombre de brokers actifs et partitions offline (`offline_partitions_count`) | `diagnose_engine.py` |

## 5. Le cas indéterminé
- Les 7 raisons d'abstention : `needs_second_sample`, `probes_stale`, `data_missing`, `contradictory_signals`, `ambiguous_sla`, `ambiguous_non_dominated`, `probe_blinded`.

## 6. Justification des seuils

| Seuil | Valeur | Mesure de référence | Justification |
|---|---|---|---|
| `lag` | 10 | 0 en nominal | Évite de déclencher une alerte sur un retard transitoire mineur (valeur 1). |
| `gap step` | 5 | 0 en nominal | Caractérise un décrochage net de l'écriture en base de données. |
| `big gap` | 20 | 0 en nominal | Seuil critique d'accumulation non résorbée. |
| `fraîcheur` | 75 s | Cadence $\approx 10\text{ s}$ | Empêche de conclure sur des métriques figées si un collecteur tombe. |
| `span` | $\ge 10\text{ s}$ | Période d'échantillonnage | Respect de la condition de Shannon pour observer une dérivée discrète. |

## 7. Analyse d'erreurs sur les campagnes
### 7.1 Niveau cas
- Justesse : 4 / 4 sur les scénarios d'incidents injectés.
### 7.2 Niveau échantillon
- 40 échantillons d'incident, 3 abstentions (7,5 %).
### 7.3 Délais de détection
- 3 s, 14 s, 15 s, 25 s → médiane 14,5 s, max 25 s $\le 30\text{ s}$.
### 7.4 Faux positifs et intervalle de confiance
- 0/70 faux positifs ; borne supérieure Wilson 95 % $\approx 5,2\text{ \%}$.
- Donc on ne prétend pas « 0 % absolu ».

## 8. Limites et menaces de validité
- La période d'échantillonnage fixe borne inférieurement le délai de détection des incidents transitoires très courts.

## 9. Ce que la garantie ne couvre pas
- Résultat négatif assumé : aucune perte de données n'a pu être provoquée par un simple arrêt de PostgreSQL, car les persisters du socle rejouent leurs lots tant que Kafka conserve les messages.

## 10. Pistes de lecture (facultatif)
- Exploration future vers une détection adaptative basée sur le débit dynamique plutôt que sur des seuils absolus.

### Sources citées :
- `preuves/phase4-campagne-2026-11-05/SYNTHESE.md`
- `preuves/phase4-campagne-2026-11-05/campaign-summary.json`
- `preuves/phase5-croisee-2026-11-05/SYNTHESE.md`
- `travail/notes/matrice-symptomes-causes.md`
- `docs/architecture/diagnostic-engine-design.md`
