# SYNTHESE — phase 7 : impact métier (candidate de garantie #7)

> **Candidate démontrée** : le verdict du moteur transporte un bloc `business_impact`
> (« 278 Pix ont dépassé le SLA de décision de 10 s — 278 rejet(s) métier "délai
> dépassé" (cette donnée est persistée, pas perdue) »), et le SQL direct confirme
> à l'identique : moteur == base, **6/6 checks PASS**. De plus, un rejet métier
> traité DANS le SLA n'est jamais compté à tort comme dépassement (compteur par
> drapeau `decision_within_sla`).
>
> Date : 2026-11-05 · Scénario : `consumer_lag 300 @ 20/s` (situation A)
> Scripts : `scripts/impact-metier-proof.sh` + `scripts/lib/impact_report.py`
> (protocole rejouable, ~6 min) · Moteur : bloc `business_impact` ajouté,
> 32 tests unitaires OK.

---

## 1. Design de l'extension (ce qui a été codé)

- **Bloc `business_impact` dans le verdict** (`scripts/lib/diagnose_engine.py`,
  calculé dans `build_verdict` après `route()` — présent sur tout verdict à
  sondes fraîches, `null` sur `probes_stale`/`data_missing`) :
  `total_sla_breaches`, `delta_sla_breaches` (entre 2 lectures espacées ≥ 10 s),
  répartition `validated_sla_breaches` / `rejected_sla_breaches`, `sla_seconds`,
  phrase lisible (`label`) et `independent_check` = `tp-db.sh`.
- **Source de vérité en base** : la sonde PostgreSQL du socle compte les colonnes
  `decision_within_sla` des deux tables (`probes.py`) ; la plateforme relaie ces
  compteurs dans `metrics.counts.*` ET `details.postgres.*` — le moteur croise,
  il n'ajoute aucune sonde.
- **Discipline inchangée** : le bloc est FACTUEL (impact mesuré), la branche-cause
  reste le travail des règles de la matrice ; le garde-fou anti « lag éclair »
  (span ≥ 10 s) s'applique aussi au delta d'impact ; la détection de run
  (compteurs décroissants) interdit un delta entre deux époques sans lien.

## 2. Scénario et fermeture

```
./scripts/impact-metier-proof.sh 300 20    # consumer_lag, 300 messages à 20/s
```

Fermeture de la chaîne atteinte (`convergence.json`, tous les checks à `true`) :
`generated == raw == decisions == outcome == db = 300` (22 validés + 278 rejets),
écarts de persistance 0, lag 0. **0 perte** — les 278 rejets sont en base.

## 3. Confrontation moteur ⇄ payload ⇄ SQL direct (rapport-impact-metier.json)

| Instant | Verdict moteur | Impact moteur (total / delta) | Payload `metrics.counts` | Sonde socle `details.postgres` | SQL direct | span moteur |
|---|---|---|---|---|---|---|
| t0 (sain, avant l'incident) | healthy | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 12,6 s (2 lectures) |
| pendant-1 (croissance) | indeterminate[1] | 105 / — | 106 | 106 | 102 | — (garde-fou) |
| pendant-2 (drain) | indeterminate[1] | 245 / — | 246 | 246 | 239 | — (garde-fou) |
| pré-delta (état fermé) | — | — | — | — | **278** | — |
| **final** (après fermeture) | **healthy** (fin de scénario expliquée) | **278 / 0** | **278** | **278** | **278** | 12,4 s |

[1] Captures script prises entre deux écritures de l'enregistreur (span 9,3 s /
2,6 s < 10 s) : le garde-fou anti « lag éclair » refuse le delta — **le moteur
défend sa règle sur son propre outil de preuve** (verdict `needs_second_sample`,
note « Échantillon précédent écarté »). Ce pendant, le TOTAL d'impact reste
affiché : l'information factuelle est disponible même quand la cause ne peut
pas trancher.

**Écarts pendant la croissance (105↔102, 245↔239)** : le SQL direct est pris
quelques secondes AVANT la lecture du payload ; à une vitesse d'incrémentation
de ~2,8/s dans l'incident, ce décalage de mesure vaut 3 à 6 unités — il devient
égalité exacte à l'état fermé (278 == 278 == 278).

**Deltas observés (§4)** ≈ +31 / 11 s ≈ 2,8/s ≈ le débit réel du decision-engine
(délai 350 ms) : chaque message de la file qui a dépassé son TTL est rejeté AU
MOMENT où il est traité — le « débit du renvoi » est celui du traitement, pas
celui de la génération (20/s).

## 4. Timeline moteur (verdicts.jsonl, cadence 11 s)

| t (UTC) | verdict | cause / motif | impact (total / delta) |
|---|---|---|---|
| 16:44:42 | healthy | témoin pré-scénario | 0 / — |
| 16:44:54 → 16:46:01 | `unreachable` ×7 | recréation de service-health par run-scenario : le relevé trace l'indispo, il ne conclut RIEN | — |
| 16:46:13 | healthy | nouveau run (compteurs remis à zéro) | 0 / — |
| 16:46:25 | consumer_backlog | reader_backlog (« lag ≥ 10 ») | 0 / 0 — l'incident est détecté AVANT le premier dépassement |
| 16:46:37 → 16:48:00 | consumer_backlog ×7 | reader_backlog | **13 → 48 → 79 → 142 → 173 → 205 → 238** (deltas +35, +31, +31, … ≈ 2,8/s) |
| 16:47:14 | indeterminate | needs_second_sample (garde-fou sur un écart de fenêtre) | 111 / — |
| 16:48:12 | healthy | fin de scénario (retard complètement absorbé) | 269 / — (historique, plus rien de nouveau) |
| final (script) | healthy | upstream_stopped_normal | **278 / 0** |

Séquence anti-faux-positifs visible en direct : `0/0` quand l'incident s'ouvre
(le lag précède le TTL — l'impact réel est encore nul), puis +13, +35, +31…
pendant la croissance, puis stabilisation (269 historique, 278 final) quand la
file est vidée : **le total d'impact monte pendant l'incident et s'immobilise
après la fermeture**.

## 5. Checks (6/6 PASS)

1. `moteur_total_equals_sql_final` — moteur **278** == SQL **278** ;
2. `sonde_socle_equals_sql_final` — `details.postgres` 278/0 == SQL 278/0 ;
3. `moteur_equals_sonde_socle_final` — le relais payload ne déforme rien ;
4. `delta_controle_zero_apres_fermeture` — 2 lectures espacées de 12,4 s à l'état
   fermé : le moteur affiche **delta = 0** (aucun nouveau dépassement) ;
5. `sql_stable_apres_fermeture` — SQL 278 (pré-delta) == 278 (final) ;
6. `delta_controle_reproduit_par_sql` — delta moteur 0 == variation SQL 0 entre
   les mêmes instants.

## 6. Non-confusion — la candidate #7 ne fausse rien

- **X Pix impactés == X rejets en base** : `tp-db-final.txt` montre 278 rejets,
  **100 % de motif `processing_timeout`** — le bloc d'impact porte exactement sur
  ces deux faits : 278 dépassements TTL et « cette donnée est persistée, pas
  perdue » (rappel imprimé DANS la phrase d'impact) ; la fermeture (§2) le prouve.
- **Rejet métier ≠ dépassement SLA** : à t0, la base contenait encore les 300
  rejets métier de l'essai `errors_simple` précédent (motifs
  `amount_must_be_positive`, etc.) avec `rejected_breaches == 0` — un rejet
  métier traité DANS le SLA ne compte PAS comme dépassement ; le compteur ne
  s'embrase que sur de vrais dépassements de TTL (100 % `processing_timeout` ici).
- **Retard ≠ impact** : à 16:46:25 l'incident est détecté (verdict
  `consumer_backlog`) alors que l'impact est encore 0/0 — le retard précède le
  comptage d'impact ; le bloc sépare donc la cause (le retard) et sa conséquence
  chiffrée (l'impact réel compté en base).

## 7. Reproduction

```bash
./scripts/impact-metier-proof.sh 300 20     # ~6 min, preuves incluses
# (individuellement : ./scripts/run-scenario.sh consumer_lag 300 20
#  avec, en parallèle avant le lancement :
#  ./scripts/diagnose.sh --reset && ./scripts/lib/record-verdicts.sh <dossier> 620 11)

cd scripts/lib && python3 -m unittest test_diagnose_engine    # 32 tests OK
python3 scripts/lib/impact_report.py preuves/phase7-impact-metier-2026-11-05 300 20
```

## 8. Fichiers de cette preuve

- `verdicts.jsonl` — timeline complète du moteur (cadence 11 s, mutualisé) ;
- `verdict-{t0,pendant-1,pendant-2,final}.json` + `health-*.json` — instantanés ;
- `db-sla-{t0,pendant-1,pendant-2,pre-delta,final}.txt` — SQL direct (psql) ;
- `counts-*.json`, `convergence.json` — fermeture (extraction plateforme) ;
- `tp-{kafka,db}-final.txt` — vérifications indépendantes de fin ;
- `rapport-impact-metier.json` — confrontation moteur ↔ sonde ↔ SQL (6/6).
