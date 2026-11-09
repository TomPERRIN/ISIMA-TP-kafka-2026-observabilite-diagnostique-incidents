# Catalogue des cas d'incidents

Inventaire maillon par maillon des incidents possibles. Pour chaque cas : ce qui se voit,
la cause nommée par le moteur, comment le provoquer, et s'il est prouvé en campagne.
C'est le complément de la [matrice](matrice-symptomes-causes.md) — les règles de diagnostic
(comment trancher) y restent — et la **checklist des essais restants**.

Sources : vocabulaire du moteur (`docs/architecture/diagnostic-engine-design.md` §4),
preuves archivées (`preuves/phase4-…`, `preuves/phase5-…`, `preuves/phase7-…`),
leviers d'injection (`scripts/inject-fault.sh`, `scripts/run-scenario.sh`).

## 1. Les incidents des 3 familles, déclinés finement

### 1A — Retard de consommateur, groupe par groupe (5 cas)

La frontière avec la famille C : le lag vu **en amont** (validator, decision-engine) ou
**en aval** (persisters). Un lag aval + un écart de persistance qui grossit → persistance,
pas lecture (règle de la matrice, ligne 1).

| # | Cas | Ce qui se voit | Cause nommée (moteur) | Comment le provoquer | Preuve |
|---|---|---|---|---|---|
| A1 | `pix-validator` en retard | raw s'accumule, checked stagne | `reader_backlog` (même règle, quel que soit le groupe) | `./scripts/inject-fault.sh chain_kill pix-validator` | même règle, pas d'essai isolé archivé |
| A2 | `pix-decision-engine` en retard | checked s'accumule, validated/rejected stagnent, dépassements SLA | `reader_backlog` | `./scripts/run-scenario.sh consumer_lag 600 20` | ✅ phase 4 `A-retard-lecture` + phase 5 `A-draine` + phase 7 |
| A3 | `pix-outcome-publisher` en retard | decision s'accumule, outcome stagne | `reader_backlog` | `./scripts/inject-fault.sh chain_kill pix-outcome-publisher` | même règle, pas d'essai isolé archivé |
| A4 | `persister-valid` en retard | validated s'accumule, `persistence_gap_valid` grossit | `persister_slow` (écart qui S'ACCROÎT) | `SIMULPIX_PERSIST_VALID_DELAY_MS=350` | ✅ phase 4 `C-postgres-degrade` (écart 3 → 319) |
| A5 | `persister-rejected` en retard | rejected s'accumule, `persistence_gap_rejected` grossit | `persister_slow` | `SIMULPIX_PERSIST_REJECTED_DELAY_MS=350` | même mécanisme, variante non isolée en campagne |

### 1B — Réplication dégradée (4 cas)

| # | Cas | Ce qui se voit | Cause nommée (moteur) | Comment le provoquer | Preuve |
|---|---|---|---|---|---|
| B1 | Broker arrêté | 2 brokers sur 3 (`broker_count < 3` → chemin grave), URP | `broker_down` | `./scripts/inject-fault.sh broker_outage kafka-3` (variante silencieuse : `broker_isolate`) | règle codée (`EXPECTED_BROKERS=3`), pas d'essai archivé |
| B2 | Copie lente entre brokers | retard de copie monte, brokers tous présents | `replication_catchup` (bénin, se résorbe) | `SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE=20 SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE=kafka-3 ./scripts/run-scenario.sh replication_lag 600 20` | ✅ phase 4 `B-replication` + phase 5 `B-replication-croise` (≤ 30 s) |
| B3 | Rééquilibrage de partitions | pics passagers d'URP, se résorbent seuls | `replication_catchup` (il se résorbe seul) | à définir | pas d'essai |
| B4 | Contrôleur perdu ou doublon | `active_controller_count` = 0 ou > 1 | — (pas de cause nommée) | à définir | **angle mort** : problème du cluster lui-même |

### 1C — Persistance PostgreSQL (4 cas)

| # | Cas | Ce qui se voit | Cause nommée (moteur) | Comment le provoquer | Preuve |
|---|---|---|---|---|---|
| C1 | Persister ralenti, base qui répond | gap grossit, `services.postgres` ok | `persister_slow` | voir A4/A5 (même phénomène vu côté base) | ✅ phase 4 `C-postgres-degrade` |
| C2 | PostgreSQL indisponible | `services.postgres` down, `tp-db.sh` échoue, `seconds_since_last_*` monte | `postgres_unavailable` (criticité **Critique**, première à écarter) | `docker compose stop postgres` 40 s en plein flux | ✅ phase 4 `C-postgres-indispo` (détection 3 s) + phase 5 `C-rattrapage` (0 perte) |
| C3 | PostgreSQL saturé | connexions actives au max, écritures lentes (`postgres_runtime`) | — (se rapproche de `persister_slow`) | à définir (aucun scénario dédié) | **angle mort** |
| C4 | Écriture échouée mais service « ok » | conteneurs ok, statut ok, `persisted_*` ≠ `db_*_count` | — (le discriminateur « écart qui S'ACCROÎT » le couvrirait) | à définir | jamais déclenché (aucune perte provoquée, cf. phase 5) |

### Bilan de couverture (section 1, d'un seul regard)

- **Prouvés en campagne** : A2, A4, B2, C1, C2 — la garantie mesurée (4/4, délai max 25 s,
  0 faux positif) porte sur ces cas.
- **Couverts par la même règle, sans essai isolé archivé** : A1, A3, A5.
- **Règle codée, jamais déclenchée en campagne** : B1 (`broker_down`).
- **Angles morts à assumer** : B3, B4, C3, C4 — pas de cause nommée. À lister tel quel
  dans le registre des risques (§5) et dans la conclusion du rendu (limites honnêtes).
  **Démontrés mécaniquement** par `./scripts/demo-angles-morts.sh` : chaque cas est
  simulé (payloads synthétiques frais) et le verdict du vrai moteur est affiché.

## 2. Les cas à ne pas confondre

La plateforme produit aussi des états qui **ressemblent** à des incidents sans en être.
Le moteur est construit pour ne PAS sonner sur ces cas — chacun a une signature qui
permet de l'écarter.

| # | Cas | Pourquoi ce n'est PAS un incident | Comment l'écarter | Preuve/statut |
|---|---|---|---|---|
| 1 | Rejets métier (`errors_simple`) | un rejet est une donnée persistée : les lignes partent en base, rien n'est perdu | cohérence compteur ↔ base : `rejected` == lignes dans `rejected_transactions` (`tp-db.sh`) | ✅ phase 5 `rejets-metier` (300 rejets, verdicts `healthy`) |
| 2 | Rejets `processing_timeout` | conséquence d'un retard (TTL de décision dépassé), pas une cause | la donnée est persistée, pas perdue ; un motif massif en base pointe un backlog amont | ✅ phase 7 (6/6 : moteur 278 == base 278) |
| 3 | Générateur arrêté (fin de scénario) | la base se fige parce que le flux s'arrête : cause `upstream_stopped_normal` | `counts.generated` stagne + file vide ; pièges : `estimated_oldest_lag_seconds` disparaît, `history` gèle en idle | ✅ phase 4 (fin des 5 témoins) |
| 4 | Pic de charge (`football_match_peak`) | le lag monte pendant la montée puis se draine : transitoire | laisser `--watch` tourner : le drainage ramène `healthy` | pas injecté en campagne |
| 5 | Perturbations réseau volontaires (`kafka_latency`, `kafka_loss`, `kafka_slow_link`) | provoquées à la main, signalées par l'alerte réseau | pendant une perturbation active, aucun service down n'est retenu (voir §3, cas 4) | règle codée, pas d'essai |

## 3. Les défaillances du système d'observation

Le système d'observation lui-même peut tomber — la mission demande de le vérifier.
Quatre cas, avec le comportement du moteur et ce qui reste fiable.

| # | Cas | Comportement du moteur / des signaux | Preuve/statut |
|---|---|---|---|
| 1 | `service-health` arrêté | `/health/details` injoignable : le relevé note `unreachable`, le moteur ne conclut JAMAIS — c'est la démonstration en live du critère fraîcheur | ✅ phase 4 |
| 2 | Collecteur Influx arrêté | `/health/details` reste frais (le `timestamp` du payload est ré-émis à chaque requête) — MAIS les courbes Grafana se figent sans le dire (InfluxDB n'est plus alimentée) | ✅ vérifié phase 4 |
| 3 | Sondes périmées (> 75 s) | verdict `indeterminate/probes_stale` : rien ne conclut, relire après rétablissement ; couvert par test unitaire (payload vieilli 300 s) | ✅ test unitaire |
| 4 | Sonde aveuglée par une perturbation réseau | un service down pendant une perturbation active n'est pas retenu comme cause : `indeterminate/probe_blinded` — la sonde peut être aveuglée, pas le service | règle codée |

Conséquence pratique : **un dashboard figé ne prouve rien**. En cas de doute, comparer la
fraîcheur de la courbe avec l'état des services (`tp-status.sh`) et confirmer sur les
sources (`tp-kafka.sh` / `tp-db.sh`).

## 4. Les combinaisons simultanées

Deux incidents en même temps : le moteur nomme UNE famille (verdict unique) mais garde
toutes les candidates dans `plausible_causes[]`, ordonnées par le tri probabilité ×
criticité — **la cause la plus critique d'abord** si sa vérification est rapide
(`tp-kafka.sh`/`tp-db.sh` sont des instantanés), puis confirmation de la cause la plus
probable.

| Combinaison | Comportement attendu | Statut |
|---|---|---|
| Retard consommateur + réplication dégradée | les deux familles candidates ; le verdict suit le tri (criticité d'abord), l'autre reste dans `plausible_causes[]` | règle codée, pas d'essai |
| Signaux contradictoires entre sources | `indeterminate/contradictory_signals` ou `ambiguous_non_dominated` : refus explicite, vérification à la main (`tp-kafka.sh`/`tp-db.sh`) | ✅ 7 raisons cataloguées (design doc §5) |
| PostgreSQL down pendant une perturbation réseau active | `indeterminate/probe_blinded` : la panne empêche de la VOIR — trancher avec `docker ps` et `tp-db.sh` après rétablissement | règle codée |

**Statut honnête** : la campagne n'a injecté que des incidents mono-cause. Les
combinaisons sont couvertes par construction (le refus de conclure est explicite et la
vérification suivante est toujours indiquée) mais jamais déclenchées en essai — à
assumer comme limite dans le rendu.

## 5. Le registre des risques

Ce qu'il faut énoncer tel quel au rendu — la mission exige de distinguer ce que la
solution garantit de ce qu'elle ne garantit pas.

**Faux positifs — mesurés et écartés par construction :**

- mesuré : **0/70 échantillons** sur 5 témoins (`nominal 200 10`, phase 4) ;
- les 3 pièges connus, ignorés par le moteur :
  - `lag_total = 1` en nominal (rattrapage éclair) — d'où le garde-fou « 2 échantillons
    espacés ≥ 10 s » ;
  - écart de persistance léger en début de flux (le temps d'écrire) — seul l'écart qui
    S'ACCROÎT conclut ;
  - fin de scénario : base figée nommée `upstream_stopped_normal`, PAS un incident.

**Angles morts — à assumer :**

- les 4 cas sans cause nommée du §1 : B3 (rééquilibrage), B4 (contrôleur perdu/doublon),
  C3 (PostgreSQL saturé), C4 (écriture échouée mais service « ok ») ;
- offset non initialisé ≠ retard (colonne « lecture » de `tp-kafka.sh`) ;
- verdict `degraded` de la météo trop grossier : un service transversal down n'est pas un
  incident pipeline (témoin `service_down_transversal`).

Ces angles morts ne sont pas seulement listés : ils sont **démontrés mécaniquement** par
`./scripts/demo-angles-morts.sh` — chaque cas est simulé par des payloads `/health/details`
synthétiques (sondes FRAÎCHES) passés au vrai moteur, qui affiche ce qu'il répond :

| Cas simulé | Verdict du moteur | Limite démontrée |
|---|---|---|
| B4a contrôleur perdu sans URP | `healthy` | invisible : sans URP, aucune règle ne regarde le contrôleur |
| B4b contrôleur perdu avec URP | `broker_down` | couvert — seulement parce qu'URP l'accompagne |
| B3 rééquilibrage (URP 3→0) | `replication_catchup` | cause fine non nommée (même cause générique que la copie lente) |
| C3a PG saturé léger | `healthy` | invisible : les connexions actives ne sont pas remontées |
| C3b PG saturé fort | `persister_slow` | cause voisine, pas « saturation » |
| C4a écriture échouée silencieuse | `contradictory_signals` | refus explicite mais cause non nommée |
| C4b sonde qui ment, fraîche | `healthy` | **LA limite** : le garde-fou fraîcheur ne voit que le PÉRIMÉ, jamais le mensonge |
| A0 offset non initialisé | `healthy` | lecture absente = invisible (0 lag lu quand la valeur manque) |
| T0 service transversal down | `service_down_transversal` | témoin sans incident pipeline (assumé) |

Le harnais ne modifie pas le moteur et fonctionne **sans Docker** (pur Python) —
rejouable tel quel en soutenance. Protégé par `scripts/lib/test_blind_spots.py` :
si un angle mort se ferme (le moteur évolue), les tests lèvent l'alerte.

**Garantit / ne garantit pas :** la garantie mesurée (justesse 4/4, délai max 25 s,
0 faux positif) porte sur les **5 cas prouvés du §1** (A2, A4, B2, C1, C2). Le détail
complet est tenu à jour dans le guide (§10) et la note de conception (§9) — ne pas le
dupliquer ici. Le cas « indéterminé » en une ligne : **7 raisons cataloguées** (design
doc §5), le refus de conclure est une sortie attendue, toujours accompagnée de la
vérification suivante.
