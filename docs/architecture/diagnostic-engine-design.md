# Note de conception — moteur de diagnostic Simul-Pix (SAÉ 5)

> **Garantie** : « Notre lecture d'incident ne doit pas confondre retard de consommateur,
> réplication dégradée et panne de persistance, prouvée par une matrice validée sur
> incidents injectés : bonne cause retenue sur 100 % des cas connus, état "indéterminé" +
> vérification suivante sinon — et le diagnostic ne confond pas backlog consommateur,
> rejet métier attendu et perte de données. »
>
> Vue d'ensemble du socle et de l'architecture cible :
> [`plan-architecture-sae5.md`](plan-architecture-sae5.md) (§2/§4). Usage pratique
> pendant un incident : [`../operations/guide-diagnostic.md`](../operations/guide-diagnostic.md).
> Code : `scripts/lib/diagnose_engine.py` (moteur pur), `scripts/lib/diagnose_cli.py`
> (cache 2 échantillons), `scripts/diagnose.sh` (CLI).

---

## 1. Les trois décisions structurantes

1. **Moteur PUR, hors socle** — module Python sans dépendance, entrée unique : un
   payload `/health/details`. Aucune sonde ajoutée, aucune modification de
   service-health (pas de rebuild pendant la boucle d'essais). Les signaux sont
   **reliés, non dupliqués** — c'est exactement la lacune observée dans l'état des
   lieux : 9 familles d'alertes monocausales qui ne se croisent jamais.
2. **CLI + cache à 2 échantillons** — la commande garde la lecture précédente
   (`runtime/diagnostic-cache.json`) ; les règles à deltas comparent 2 lectures
   espacées ≥ 10 s. Le praticien ne gère rien : `diagnose.sh --watch 15`.
3. **Le relevé fait partie du produit** — enregistreur mutualisé
   `scripts/lib/record-verdicts.sh` (cadence 11 s ≥ span moteur), utilisé par la
   campagne phase 4 ET le croisé phase 5 : une seule implémentation, preuves
   rejouables à l'identique.

---

## 2. Le pipeline de décision

```text
payload /health/details          ──►  GARDE-FOU FRAÎCHEUR      ──►  RÈGLES DE CROISEMENT    ──►  VERDICT
(counts, rates, timings, gaps,        (âge payload ET âge de       5 lignes de la matrice,      4 états + healthy
 details.kafka, details.postgres,      l'instantané réplication ;  2 échantillons espacés
 services, alerts)                     ≥ 75 s → probes_stale)      pour les règles à delta
```

Ordre et garde-fous, tels que codés :

- **Fraîcheur d'abord** (critère #6) : sondes périmées → `indeterminate/probes_stale`,
  JAMAIS une conclusion. Inversement, un service **à famille** (postgres, kafka,
  persisters, lecteurs) qui ne répond plus est une cause conclusive
  (`service_down:<nom>` — c'est ainsi que C a été détecté en 3 s) ; les deux autres
  cas n'inventent rien : service **transversal** down →
  `indeterminate/service_down_transversal` (simple témoin), service down **pendant
  une perturbation réseau active** → `indeterminate/probe_blinded` (la sonde peut être
  aveuglée, pas le service — trancher sur la sonde indépendante + `docker ps`).
- **Anti « lag éclair »** : toute conclusion sur un compteur qui monte exige 2
  échantillons espacés ≥ 10 s (`span < 10 s → "relire encore"`). Motivation mesurée :
  un lag de 1 en nominal est NORMAL (rattrapage éclair) ; la v1 à 5 s ne terminait
  jamais les règles à delta (leçon phase 4, relevés archivés).
- **Ancienneté de l'échantillon précédent** : au-delà de 900 s (15 min), le delta est
  refusé (« échantillon précédent trop ancien ») — pas de comparaison entre deux époques
  sans lien.
- **Détection de reset** : si un compteur monotone BAISSE (nouveau run : service-health
  relancé), l'échantillon précédent est écarté — même logique que la fermeture
  single-snapshot de la phase 5 (les deltas inter-runs sont interdits).
- **Discriminateur « l'écart qui S'ACCROÎT »** (cœur de la matrice v1, ligne 1/3) : un
  écart de persistance qui **grossit** de ≥ 5 en un span ⇒ `persister_slow` (persister
  lent / base lente) ; un écart qui **décroît** ⇒ rattrapage, noté `healthy`. Validé en
  direct sur C-dégradé : écart 3 → 319 entre deux lectures.
- **Un symptôme → au moins 2 causes plausibles**, chacune avec un discriminateur
  mesuré (les 5 lignes de la matrice) :

  | Symptôme vu en premier | Causes plausibles | Discriminateur mesuré |
  |---|---|---|
  | file qui grossit (lag ≥ 10) | lecteur en retard vs persister lent | **quel groupe** est en retard + écart qui S'ACCROÎT |
  | URP > 0 | broker arrêté vs retard de copie | **offline_partitions_count + active_controller_count + broker_count/3** |
  | plus aucune écriture en base | PG indisponible vs amont arrêté | **generated stagne ?** (2 lectures) |
  | paiements trop lents (`sla_breaches` ↑) | retard en amont vs délai trop court | **file présente ?** (`pending_result_count`, lags) |
  | `service` dégradé | panne de son service vs alerte grave | **sonde indépendante** du service |

- **Tri d'examen** (classement probabilité/criticité de la matrice v1, table
  `CAUSE_TABLE`) : la cause la plus **critique** d'abord si sa vérification est rapide
  (tp-kafka/tp-db sont instantanés). L'ordre complet est reflété par
  `plausible_causes[]` (status `confirmed | probable | excluded | hypothesis`).

---

## 3. Sortie (schéma `simulpix-diagnostic/1`)

| Champ | Contenu | Écho des livrables |
|---|---|---|
| `verdict` | `healthy`, `consumer_backlog`, `kafka_replication`, `persistence`, `indeterminate` | les états de la garantie |
| `cause` + `cause_label`, `probability`, `criticality` | cause nommée (null si `healthy`/`indeterminate`) | classement de la matrice |
| `plausible_causes[]` | causes candidates ordonnées par priorité d'examen, avec status | le praticien suit l'ordre |
| `evidence[]` | preuves à double lecture par source (`kafka` / `postgres` / `platform`) — inclut les deltas (`gap 3 → 319`, `db total 300 → 300 figé`) | le verdict cite ses chiffres |
| `freshness` | âge payload + âge snapshot réplication + violations | critère #6 |
| `indeterminate_reason` | raison cataloguée (§5) | le cas indéterminé |
| `next_check[]` | vérifications suivantes CONCRÈTES (tp-kafka.sh/tp-db.sh, relire à 15 s) | jamais un dashboard seul |
| `based_on` | `{samples, span_s}` | certification que le verdict repose sur N lectures espacées |
| `summary`, `notes[]` | explication, situation normale nommée (ex. fin de scénario) | lisibilité du praticien |

---

## 4. Catalogue des causes (extrait `CAUSE_TABLE`)

| Cause | Famille | Probabilité | Criticité | Vérification indépendante |
|---|---|---|---|---|
| `reader_backlog` | consumer_backlog | Élevée | Moyenne | `./scripts/tp-kafka.sh` |
| `upstream_backlog_sla` | consumer_backlog | Élevée | Moyenne | `./scripts/tp-kafka.sh` |
| `sla_config` | consumer_backlog | Faible | Moyenne | matrice ligne 4 |
| `persister_slow` | persistence | Moyenne | Élevée | `tp-kafka.sh` puis `tp-db.sh` |
| `postgres_unavailable` | persistence | Moyenne | **Critique** | `./scripts/tp-db.sh` |
| `broker_down` | kafka_replication | Faible | Élevée | `./scripts/tp-kafka.sh` |
| `replication_catchup` | kafka_replication | Élevée | Faible | `tp-kafka.sh` + relire ~30 s |
| `upstream_stopped_normal` | healthy | Élevée | Faible | `tp-db.sh` (base figée expliquée) |

Exemple retrouvé pendant les essais : sur `URP > 0`, le moteur vérifie **d'abord**
`broker_down` (critique, vérification instantanée) avant de conclure
`replication_catchup` (bénin) — exactement la règle de priorité de la matrice.

---

## 5. Catalogue de l'état `indeterminate`

| Raison | Quand | Vérification suivante |
|---|---|---|
| `needs_second_sample` | une seule lecture, delta requis | relire à ~15 s (`--watch 15`) |
| `probes_stale` | sondes > 75 s | inspecter la collecte puis relire |
| `data_missing` | champ attendu absent du payload | inspecter la sonde |
| `contradictory_signals` | sources se contredisent | relire, croiser tp-kafka/tp-db à la main |
| `ambiguous_sla` | SLA en hausse avec file vide ET latence dans le seuil | relire (le mouvement tranche) |
| `ambiguous_non_dominated` | aucune cause dominante | attendre/relire |
| `probe_blinded` / `service_down_transversal` | la panne empêche de la VOIR | remettre la sonde |

Vérifié en phase 4 (protocole « limites ») : arrêter `metrics-collector` ne gèle PAS
`/health/details` (le `timestamp` est ré-émis à chaque requête) — les violations de
fraîcheur détectables restent celles du snapshot réplication et de l'activité réelle.

---

## 6. Justification des seuils (tous réglables via `SIMULPIX_DIAG_*`)

| Seuil | Clé (suffixe de `SIMULPIX_DIAG_*`) | Justification (mesures) |
|---|---|---|
| âge max des sondes (75 s) | `MAX_PROBE_AGE_SECONDS` | cadence réelle du collecteur ~20 s, rafraîchissement `history` 5,7–12,3 s mesurés : 75 s ≈ 3–4 cycles d'écart |
| lag total d'incident (10) | `LAG_TOTAL_THRESHOLD` | lag éclair nominal = 1 ; incident A monté à 325–517 : le seuil 10 sépare nettement |
| gros écart immédiat (20) | `BIG_GAP_THRESHOLD` | un écart de persistance ≥ 20 déclenche déjà la ligne « persisters en retard » sans attendre leur lag — à UNE lecture, ça reste une hypothèse (`needs_second_sample`) |
| pas de S'ACCROÎT (5) | `GAP_STEP` | l'écart doit gagner ≥ 5 en un span pour conclure `persister_slow` — mesure C-dégradé : 3 → 319, aucun doute |
| span minimal (10 s) | `MIN_SAMPLE_SPAN` | anti lag éclair (2 lectures trop proches ne disent rien) ; v1 à 5 s restait bloquée en `needs_second_sample` |
| âge max de l'échantillon précédent (900 s) | `PREVIOUS_MAX_AGE_SECONDS` | au-delà de 15 min, le cache n'est pas une série fiable : pas de delta entre époques sans lien |
| brokers attendus (3) | `EXPECTED_BROKERS` | RF=3 / min ISR=2 : `broker_count < 3` déclenche déjà le chemin grave (`broker_down`), sans attendre offline>0 |
| marge latence / SLA (0.8) | `SLA_LATENCY_MARGIN` | SLA de décision 10 s (matrice ligne 4) : affichée en preuve (`latence vs SLA, marge 80 %`) pour aider à trancher retard amont vs délai trop court |

---

## 7. Le contrôle de fermeture (non-confusion, phase 5)

`scripts/lib/convergence_check.py` définit la **fermeture** sur UN seul snapshot :
`generated == raw_end == validated+rejected == outcome == db_validated+db_rejected`,
écarts de persistance 0, lag ≤ 5. Les deltas inter-cas sont **interdits**
(`run-scenario.sh` relance service-health à chaque cas : compteurs cumulés recalculés ;
le rapport v1 a produit des « deltas » aberrants, corrigé en v2). C'est le juge
mécanique des trois réalités : backlog résorbé ≠ rejet TTL persisté ≠ écart stable
(= perte). Campagne croisée : `scripts/cross-check.sh` — **4/4 PASS**
(`preuves/phase5-croisee-2026-11-05/SYNTHESE.md`).

---

## 8. Tests et preuves

- **26 tests unitaires** : `cd scripts/lib && python3 -m unittest test_diagnose_engine`
  — chaque ligne × 2 causes, témoin nominal (0 FP : lag éclair, rattrapage, idle
  résiduel), probes_stale, lectures manquantes, reset, priorités, sérialisation.
- **Campagne phase 4** (`preuves/phase4-campagne-2026-11-05/SYNTHESE.md`) : justesse
  **4/4**, délai **max 25 s ≤ 30 s** (A 15 s ; B 25 s — inclut le cycle de
  rafraîchissement du snapshot réplication ; C-lent 14 s ; C-indispo 3 s), faux
  positifs **0/70 échantillons** sur 5 témoins.
- **Croisé phase 5** (4/4 PASS) : chaque conclusion retranchée par Kafka et PostgreSQL
  en direct ; le discriminateur « écart qui S'ACCROÎT » validé en direct (3 → 319) ;
  rejets métier (300 en PG) ≠ incident ; fermeture activée sur les 4 cas.

---

## 9. Portée et limites (la « conclusion »)

**Couvert (mesuré, pas promis)** : nomination de la famille en ≤ 30 s sur A/B/C +
variante lente ; 0 faux positif en nominal (5 témoins) ; refus de conclure EXPLICITE
(7 raisons cataloguées) quand la donnée ne le permet pas ; fraîcheur des sondes avant
toute conclusion ; vérification indépendante pointée dans chaque verdict ;
démonstration de non-confusion backlog/rejet métier/perte en campagne croisée.

**Non couvert (honnêteté)** :

- **aucune perte n'a pu être provoquée** : les persisters du socle rejouent toujours —
  la branche « écart stable = perte » est codée (fermeture non atteinte à l'échéance)
  mais jamais déclenchée en campagne ; documenté tel quel, pas supposé ;
- seuils **fixes** (extension adaptative non implémentée ; le sujet ne l'exige pas) ;
- pas de vue HTTP/Grafana du moteur — la CLI est le produit livré, l'extension est
  différée (aucun rebuild nécessaire grâce à l'architecture « hors socle ») ;
- le moteur joue **au-dessus du socle** : il hérite de ses défauts — si une sonde
  ment tout en restant « fraîche », le garde-fou fraîcheur ne le voit pas (l'inverse,
  une sonde périmée, EST détectée). La vérification indépendante tp-kafka/tp-db reste
  le dernier rempart — c'est pour cela qu'elle est imprimée dans chaque verdict.

---

## 10. Prolongements ouverts (phase 7+, facultatifs)

1. **Impact métier** (candidate de garantie #7) : par incident, « X Pix dépassent le
   SLA de 10 s » issu des compteurs `*_sla_breaches`, vérifiable en base ;
2. **Détection adaptative** : remplacer des seuils fixes par une estimation liée au
   débit courant — une règle explicite, justifiée et évaluée suffit déjà au minimum ;
3. **Vue HTTP / panneau Grafana** : le même module peut être exposé par service-health
   ou rendu en dashboard (extension différée après la phase 6).
