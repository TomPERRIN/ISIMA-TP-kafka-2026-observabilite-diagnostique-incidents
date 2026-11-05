# Avancement — SAÉ 5 · Observabilité et diagnostic d'incidents

> Fichier de suivi d'avancement détaillé (ce qui est fait, résultats, décisions).
> Les tâches restantes sont pilotées dans [`TODO.md`](TODO.md).
> Dernière mise à jour : 2026-11-05.

---

## Vue d'ensemble

| Étape | État | Commentaire |
|-------|------|-------------|
| Choix du sujet + répertoire de travail | ✅ fait | `travail/` créé (`notes/`, `essais/`) |
| Plateforme opérationnelle | ✅ fait | `start.sh` OK, flux nominal validé (13/13) |
| Formulation de la garantie | ✅ fait | Garantie #1 retenue, seuil 30 s justifié |
| État des lieux des métriques | ✅ fait | Inventaire complet (§1-§4 + synthèse, pull 04/11) |
| Matrice symptôme / cause / vérification | ✅ fait (v1) | `notes/matrice-symptomes-causes.md` — 5 lignes + classement probabilité/criticité, **manuelle** (commit `05c67db`) |
| Automatisation de la matrice (le produit) | ✅ fait (05/11) | `scripts/diagnose.sh` + moteur `scripts/lib/diagnose_engine.py` (4 états + 26 tests unitaires) — couverture A/B/C, double lecture, fraîcheur ; vue HTTP différée |
| Incidents provoqués et mesurés | ✅ fait (05/11) | Campagne phase 4 (`scripts/diagnostic-campaign.sh`) : **justesse 4/4, délai max 25 s, 0 FP sur 5 témoins** — `preuves/phase4-campagne-2026-11-05/` |
| Vérification croisée + non-confusion (phase 5) | ✅ fait (05/11) | `scripts/cross-check.sh` : 4 cas → **4/4 PASS**, chaque conclusion cross-checkée Kafka+PG, fermeture single-snapshot compteurs==PG — candidate #2 démontrée (`preuves/phase5-croisee-2026-11-05/`) |
| Livrables finaux (guide, note de conception, conclusion) | ✅ fait (05/11) | `docs/operations/guide-diagnostic.md` + `docs/architecture/diagnostic-engine-design.md` (portée/limites = conclusion) ; protocoles + relevés déjà en place (§1.7/§1.8) |
| Extension : impact métier (candidate #7, phase 7) | ✅ fait (05/11) | Bloc `business_impact` dans chaque verdict, vérifiable en base ; preuve 6/6 (moteur == sonde PG == SQL : 278==278==278) — `preuves/phase7-impact-metier-2026-11-05/` + `scripts/impact-metier-proof.sh` |

---

## 1. Ce qui est fait

### 1.1 Plateforme opérationnelle (03/11/2026)

- `./start.sh` lancé avec succès. Points durs résolus (documentés dans `TODO-SAE5.md` §3.2) :
  - démarrage **depuis WSL** obligatoire (montage `${PWD}:${PWD}` illisible depuis Git Bash : « too many colons ») ;
  - **CRLF Windows** sur les `.sh` corrigés en LF + `.gitattributes` (`*.sh text eol=lf`) pour éviter la récidive ;
  - dossier `infra/kafka/secrets/` recréé (+ `.gitkeep`), secrets TLS générés.
- Flux nominal de contrôle : `./scripts/run-scenario.sh nominal 100 10` — **run 1793704138** :
  - `/health` => `ok`, aucune alerte ;
  - 100 générés / 100 traités / 100 acceptés / 0 rejetés / 100 outcome / 100 persistés, rejet 0 % ;
  - lag = 0 sur les 5 groupes applicatifs ; `tp-db.sh` : `validated_count=100`, `rejected_count=0` ;
  - dashboards Grafana provisionnés (General, Kafka, Persistence, Incidents) ;
  - anomalie mineure : `validate-observability.sh` échoue au dernier pas (« must specify org ID or org name », flag `--org` absent) — à creuser plus tard.
- Dépôt GitHub : fork `TomPERRIN/ISIMA-TP-kafka-2026-...-incidents` configuré (origin), visibilité privée probable.
- `.gitignore` durci : `infra/kafka/secrets/*` et `runtime/*` exclus, `.gitkeep` préservés (vérifié par `git check-ignore`).

### 1.2 Garantie formulée (03/11/2026)

- Énoncé du problème : « Aider une personne qui découvre un incident à passer d'un symptôme visible à une cause vérifiée, en croisant les signaux déjà exposés par la météo, Grafana et les sondes Kafka/PostgreSQL — sans ajouter de nouvelle sonde. »
- **8 candidates de formulation** rédigées et arbitées (décision validée utilisateur le 03/11/2026) :
  - **#1 Désambiguïsation** = garantie principale ;
  - **#3 délai de détection, #4 zéro faux positif, #5 jamais sans vérification croisée, #6 fraîcheur des sondes** = critères d'acceptation de #1 ;
  - #2 non-confusion backlog/rejet/perte = bonus **mais** sa démonstration reste un livrable (exigé par la fiche) ;
  - #7 impact métier = extension facultative ; #8 reproductibilité = inhérent au rendu.
- **Seuil de détection ≤ 30 s justifié** par des mesures réelles : collecteur Influx = 10 s fixe ; `history` service-health = 5,7-12,3 s sous charge ; 30 s ≈ 2-3 cycles + latence acteur.

### 1.3 État des lieux (03/11/2026)

- **Recensement des signaux** : `preuves/phase2-recensement/recensement-signaux.md`
  - structure complète de `/health` + `/health/details` (`details.kafka`, `details.postgres`, `metrics.counts/gaps/timings`, `history`) ;
  - **7 mesures InfluxDB** (pas de 10 s) : `kafka_broker`, `kafka_replication_partition`, `kafka_consumer_group`, `kafka_topic`, `simulpix_platform`, `simulpix_semantics`, `postgres_runtime` ;
  - **9 familles d'alertes** dans `alerts.py`, toutes **monocausales** → la marge d'apport du sujet 5 est là ;
  - réponses de travail aux 5 questions de conception (§6).
- **Mesure de référence nominale** : `preuves/phase2-recensement/reference-nominale-2026-11-03/` — protocole rejouable, **13/13 vérifications OK** (convergence complète compteurs/DB, gaps 0, lag 0, sous-réplication 0).
- **Inventaire métriques — COMPLET** : `travail/notes/metriques-observabilite.md` (commits `95556a5` + `a394db6`, pull 04/11/2026)
  - §1 (météo service-health) : sondes, compteurs (`persisted_*` vs `db_*` à ne pas confondre), gaps/timings, Kafka (lag, réplication), alertes existantes, ce qui manque ;
  - §2 (dashboards Grafana) : chaîne d'alimentation (collecteur → InfluxDB → dashboards), les 4 dashboards panneau par panneau (`incidents`, `kafka`, `persistence`, `pipeline`), limites (aucun ne croise les signaux, `incidents` = simple somme d'indicateurs) ;
  - §3 (`tp-kafka.sh`) : topics, facteur de réplication, les 5 consumer groups avec lecture par application, apports (vérification indépendante + discrimination par application) et limites (instantané, pas de réplication ni PG) ;
  - §4 (`tp-db.sh`) : les 2 tables (`validated_transactions`, `rejected_transactions`), motifs de refus (`processing_timeout` = typique d'un retard de consommateur, aide au diagnostic), apports/limites (totaux depuis le début → travailler en différence) ;
  - Synthèse : tableau des 4 sources — **vérifications indépendantes = `tp-kafka.sh` (Kafka en direct) et `tp-db.sh` (PG en direct)**, la météo et les dashboards lisent les mêmes sources qu'eux-mêmes ; 3 ambiguïtés observées ; verdict « indéterminé » inexistant dans le socle.

### 1.4 Documentation de pilotage (04/11/2026)

- `TODO-SAE5.txt` réécrit en **`TODO-SAE5.md`** : Markdown lisible (TOC, tableaux, décisions mises en évidence, candidats de garantie détaillés).
- **Plan d'architecture extrait** dans un fichier dédié : `docs/architecture/plan-architecture-sae5.md` (socle, couche d'observabilité, chaîne applicative, architecture cible du moteur de diagnostic, 3 vues de lecture, intégration dans le dépôt).
- `TODO-SAE5.md` §2 remplacé par un lien vers le plan ; sa Phase 2 fusionnée avec l'inventaire métriques.
- **`travail/avancement.md`** (ce fichier) + **`travail/TODO.md`** (pilotage fusionné TODO-SAE5 + inventaire métriques).

### 1.5 Matrice symptôme / cause probable / vérification (04/11/2026, commit `05c67db`)

**`travail/notes/matrice-symptomes-causes.md` (v1)** — construite à partir de l'état des lieux, c'est la **spécification de la règle de diagnostic** :

- **5 lignes de diagnostic** : chaque ligne = un symptôme vu en premier → 2 causes plausibles → la vérification qui tranche → le cas « indéterminé » :
  1. **file d'attente qui grossit** (`pending_result_count`, lag ↑) → retard de lecture vs persistance ; trancher : quel groupe est en retard (`tp-kafka.sh`) + `persistence_gap_*` ;
  2. **URP > 0** (tranches mal répliquées) → broker arrêté (grave) vs retard de copie (bénin) ; trancher : `offline_partitions_count` > 0 ou contrôleur = 0 → panne, sinon retard qui se résorbe seul ;
  3. **plus aucune écriture en base** (`db_*` figé) → PostgreSQL indisponible vs amont arrêté ; trancher : `counts.generated` qui stagne → amont, qui monte sans écriture → PostgreSQL ;
  4. **paiements trop lents** (`sla_breaches` ↑) → retard en amont vs délai trop court ; trancher : file longue au moment du dépassement → amont, file vide → délai trop court ;
  5. **verdict `degraded`** → service down vs alerte grave ; trancher : `services.<nom>` + sonde indépendante.
- **Témoin nominal** intégré (contrôle des faux positifs, référence 13/13) : `lag_total = 1` passager normal, léger écart de persistance normal en début de flux, alerte au nominal = faux positif.
- **Procédure d'usage en 4 pas** pendant un incident : verdict → symptôme (1re colonne) → ligne de la matrice (toujours `tp-kafka.sh` ou `tp-db.sh`, jamais un dashboard seul) → cause nommée ou « indéterminé » explicite.
- **⚠️ La matrice est manuelle** : la contribution attendue est de la rendre **automatique** (métrique dérivée / alerte / vue / commande), évaluée sur incidents provoqués.
- **Classement probabilité / criticité ajouté** (04/11/2026) :
  - échelles qualitatives : Probabilité {Élevée, Moyenne, Faible} × Criticité {Critique, Élevée, Moyenne, Faible} ;
  - les 10 causes notées ligne par ligne avec **priorité d'examen** et justification par les mesures connues (rattrapage éclair = lag passager normal, `generated` qui stagne = fin de scénario normale, TTL 10 s, RF=3 / min ISR=2) ;
  - règle de lecture : **écarter d'abord la cause la plus critique** quand sa vérification est peu coûteuse (les instantanés `tp-kafka.sh` / `tp-db.sh` le permettent), puis confirmer la cause la plus probable ;
  - top 3 à surveiller : **PostgreSQL indisponible** (seule cause **Critique** du lot, perte durable) > **broker arrêté** (near-miss : à min ISR=2, un 2e incident rend les écritures impossibles) > **retard de lecture qui dure** (fréquent, devient impact SLA massif si non détecté dans les 30 s) ;
  - ⚠️ hypothèses de travail : la Phase 4 transformera ces probabilités en fréquences **mesurées**.

### 1.6 Automatisation de la matrice — moteur de diagnostic (05/11/2026)

La matrice v1 est maintenant **codée dans un moteur** (apport = commande de diagnostic,
conformément au plan d'architecture §4 et §6). ⚠️ Contexte : l'essai du 03/11 (diagnose.py,
intégration service-health, scripts d'essais) n'avait pas été commité et a été perdu au
pull du 04/11 — le moteur est reconstruit **hors service-health** (module + CLI), décision
consolidée ci-dessous dans le journal.

- `scripts/lib/diagnose_engine.py` — module **pur stdlib**, sans effet de bord :
  - `build_verdict(payload, now, previous, config)` : un payload `/health/details`
    (+ l'échantillon précédent du cache) → verdict JSON ; aucune nouvelle sonde.
  - Les **5 lignes de la matrice** = règles de croisement, chaque symptôme aboutit à
    `plausible_causes[]` (libellés matrice + probabilité/criticité/**priorité d'examen**,
    status `confirmed|probable|excluded|hypothesis`) et à la cause dominante.
  - **4 familles + healthy** : `consumer_backlog` (A) / `kafka_replication` (B) /
    `persistence` (C) / `indeterminate` (+ `healthy`, cause expliquée : fin de scénario
    normale `upstream_stopped_normal`, rattrapage éclair…) — cf. plan §4.
  - **Discriminants codés** : ligne 2 = `offline_partitions_count`/contrôleur/`broker_count`
    < 3 → broker arrêté (vérifié avant de conclure au retard de copie) ; ligne 1 =
    lag sur persisters + **écart de persistance qui S'ACCROÎT** (≥ 5) → PostgreSQL ne suit
    plus, lag lecteur persistant/montant → retard de lecture ; ligne 3 = `generated` stagne
    vs monte ; ligne 4 = file longue vs file vide + `avg_validation_latency_ms` > SLA
    (comparé à `ui.decision_sla_seconds`, chaîne à parser) ; ligne 5 = `services.<nom>`
    + `details.network.active` (sonde éventuellement aveuglée).
  - **Garde-fous** : fraîcheur (critère #6 : âge payload + âge `replication.captured_at`,
    limite 75 s calibrée sur la cadence collecteur mesurée ~20 s → sondes périmées =
    `indeterminate/probes_stale`, jamais une conclusion) ; double lecture (critère #5 :
    `evidence[]` taguée kafka/postgres/platform, lectures manquantes = `data_missing`) ;
    2 échantillons espacés ≥ 10 s pour conclure sur un compteur en hausse (anti « lag
    éclair » nominal) ; détection d'un nouvel environnement (compteurs qui baissent =
    cache rejeté) ; `indeterminate_reason` + `next_check[]` (commandes concrètes :
    `tp-kafka.sh`, `tp-db.sh`, `diagnose.sh --watch 15`) — jamais de silence.
- `scripts/lib/diagnose_cli.py` — CLI d'état : fusionne l'échantillon courant dans
  `runtime/diagnostic-cache.json` (2 lectures max), rend le verdict en JSON (``--json``)
  ou en **texte français** (état/cause/preuves/causes plausibles/fraîcheur/prochains contrôles).
- `scripts/diagnose.sh` — la commande de diagnostic : `--json`, `--watch N` (deltas
  réels), `--single` (sans état : les règles à deltas tombent sur « indéterminé : relire »),
  `--reset` ; URL via `SIMULPIX_HEALTH_URL` ; seuils via env `SIMULPIX_DIAG_*`.
- `scripts/lib/test_diagnose_engine.py` — **26 tests OK** (`cd scripts/lib &&
  python3 -m unittest test_diagnose_engine`) : chaque ligne × 2 causes, témoin nominal
  (lag éclair 1, rattrapage, idle résiduel = healthy, **0 faux positif sur ces cas**),
  sondes périmées, lectures manquantes, reset d'environnement, sérialisation JSON.
- Vérification bout en bout (CLI, payload nominal de référence décalé « maintenant») :
  lag 132 à 1 lecture → **indeterminate/needs_second_sample** ; re-lecture 11,2 s plus tard
  → **retard de consommateur (A) / `reader_backlog` avec 2 lectures** — cohérent avec la
  garantie (≤ 30 s) ; `probes_stale` confirmé sur un payload vieilli de 300 s.
- ✅ Calibration faite en phase 4 (voir §1.7) : les seuils (fraîcheur 75 s, lag 10,
  gap 5/20, span ≥ 10 s) sont **tenus sans recalibration** sur la plateforme réelle ; les
  fixtures unitaires restent synthétiques mais les mesures live ont validé les comportements.

### 1.7 Campagne d'incidents provoqués — phase 4 (05/11/2026)

L'outil : `scripts/diagnostic-campaign.sh` (+ `scripts/lib/compact_verdict.py` pour le
relevé JSONL horodaté, `scripts/lib/campaign_report.py` pour l'analyse). Protocole
rejouable documenté dans [`notes/protocole-campagne-phase4.md`](notes/protocole-campagne-phase4.md)
(critère #8). Chaque cas : lancement du scénario, relevés du moteur toutes les 11 s
(`verdicts.jsonl`), journal d'évènements horodatés (`events.jsonl`), sortie scénario.

**Résultats** (`preuves/phase4-campagne-2026-11-05/SYNTHESE.md`) :

| Cas | Injection | Événement concluant | Famille attendue | Délai | PASS |
|---|---|---|---|---|---|
| temoin-1..5 | aucune | 5×0 FP (`healthy` sur 70 échantillons) | — | — | ✅ critère #4 |
| A-retard-lecture | `consumer_lag` décision 400 ms/pers 0 | `reader_backlog` (lag 326 ≥ 10) | consumer_backlog | **15 s** | ✅ #1/#3 |
| B-replication | `replication_lag` kafka-3 out 20 s | `replication_catchup` (URP=86, offline=0, contrôleur actif, 3/3 brokers) | kafka_replication | **25 s** | ✅ #1/#3 |
| C-postgres-indispo | `docker compose stop postgres` 40 s | `service_down:postgres` (famille persistence) | persistence | **3 s** | ✅ #1/#3 |
| C-postgres-degrade | `consumer_lag` pers 350 ms/décision 0 | `persister_slow` (**écart S'ACCROÎT 3 → 319** en 2 lectures) | persistence | **14 s** | ✅ #1/#3 |

- **Critères de la garantie tous tenus** : justesse **4/4** (#1), délai **max 25 s ≤ 30 s**
  (#3), faux positifs **0/70** (#4). Le discriminateur « écart qui S'ACCROÎT » (conçu
  mardi) a fonctionné en direct sur C-dégradé.
- **Seuils du moteur validés sans recalibration** : lag 10, gap_step 5, big_gap 20,
  fraîcheur 75 s, span ≥ 10 s.
- **Timing B instructif** : l'arrêt du broker s'est produit pendant la latence d'armement
  du relevé ; la détection (25 s) inclut le cycle de rafraîchissement du snapshot
  réplication de la météo (~20-30 s) — exactement le budget que la garantie ≤ 30 s avait
  fondé sur des mesures réelles (collecteur 10 s, history 5,7-12,3 s). Le moteur a par
  ailleurs bien distingué *broker tous présent, offline=0, contrôleur actif* → retard de
  copie (probable), pas une panne.
- **Leçon d'outil — cadence de relevé ≥ 10 s** : la campagne v1 relevait toutes les 5 s,
  sous le span minimal du moteur (2 échantillons ≥ 10 s) : les règles à delta (A, C-dégradé)
  restaient à `indeterminate/needs_second_sample` (fusible anti-lag-éclair, pas un bug
  moteur). Relevés v1 archivés dans `archives-rec5s/`, campagne relancée à 11 s → 4/4.
- Le comportement dégradé du système d'observation (critère #6) est couvert : service
  injoignable → relevés `unreachable`, jamais une conclusion ; `probes_stale` verrouillé
  par test unitaire (arrêter `metrics-collector` ne gèle pas `/health/details`, cf. protocole).

---

### 1.8 Vérification croisée et non-confusion — phase 5 (05/11/2026)

L'outil : `scripts/cross-check.sh` — 4 cas (`A-draine`, `B-replication-croise`,
`C-rattrapage`, `rejets-metier`), contrôle de fermeture `scripts/lib/convergence_check.py`,
rapport `scripts/lib/crosscheck_report.py`, enregistreur de verdicts **mutualisé**
`scripts/lib/record-verdicts.sh` (la campagne phase 4 est refactorée dessus, comportement
inchangé). Protocole rejouable : [`notes/protocole-phase5-croisee.md`](notes/protocole-phase5-croisee.md).
Chaque cas capture, à `t0` (baseline) / pendant l'incident / à la fermeture :
`tp-kafka.sh` et `tp-db.sh` EN DIRECT (croisement par définition indépendant des
dashboards), le payload `/health/details` (`health-*.json`) et les compteurs clés
(`counts-*.json`) — le moteur est relevé toutes les 11 s dans `verdicts.jsonl`
(events horodatés dans `events.jsonl`).

**Résultats** (`preuves/phase5-croisee-2026-11-05/SYNTHESE.md`) : **4/4 PASS** — la
candidate de garantie **#2** est démontrée :

| Cas | Levier | Verdicts moteur | Fermeture (état final) | Conclusion |
|---|---|---|---|---|
| A-draine | `consumer_lag` décision 400 ms / persisters 0, 600 msgs | 1×`indeterminate` (anti-lag-éclair) → **10×`consumer_backlog`** (lag pic 517) → 1×`indeterminate/ambiguous_sla` (transition) → 3×healthy | db=20+580 / outcome=600 / écarts=0 / lag=0 ✔ | backlog transitoire, la totalité est en base — dont **580 `processing_timeout` = rejets métier PERSISTÉS** (un rejet persisté ≠ une perte) |
| B-replication-croise | kafka-3 arrêté 20 s | `kafka_replication` (cause `replication_catchup`) puis healthy | db=600+0 / outcome=600 ✔ | verdict **révéré par Kafka en direct** (`tp-kafka-final.txt` : brokers/URP/contrôleur), pas par un dashboard |
| C-rattrapage | `stop postgres` 40 s puis relance (flux 800) | **3×`persistence`** (`service_down:postgres`, cause `postgres_unavailable`) | db=800+0 / outcome=800 / écarts=0 ✔ | PostgreSQL rejoint au retour : **rattrapage complet, 0 perte mesurée** |
| rejets-metier | `errors_simple` (300 msgs → 300 rejets métier) | **AUCUN incident sonné** (8×healthy) | db=0+300 == 300 `rejected` ✔ | rejet métier ≠ incident (cohérence compteurs ↔ tables PG) |

**Leçon de méthode — fermeture single-snapshot** : `run-scenario.sh` relance
service-health à chaque cas → tous les compteurs cumulés sont recalculés de zéro ; un
delta entre deux instants (t0 → final) croise donc deux runs DIFFÉRENTES (le rapport v1
a produit un « db=200/outcome=200 » aberrant sur C : 800 final − 600 d'une autre run).
La convergence est redéfinie sur **UN seul snapshot** : `generated == raw_end ==
validated+rejected == outcome == db_validated+db_rejected`, écarts de persistance 0,
lag ≤ 5 — implantée dans `convergence_check.py` (attente active) et recalculée par le
rapport. Les états non fermés sont des sorties attendues et documentées
(`pendant-backlog` de A : lag 235, décision en cours de rattrapage) ; la branche
« perte mesurée » (écarts qui refusent de se résorber à l'échéance) est codée dans le
rapport mais n'a jamais déclenché : **les persisters du socle rejouent toujours** — ce
résultat (0 perte provoquable par une simple panne de PG) est documenté tel quel.

**Deux bugs d'outillage trouvés et corrigés avant de conclure** (v1 relancée intégralement) :

- `record-verdicts.sh` : `REPO_DIR` calculé un cran trop haut (parent de `lib` au lieu
  de la racine) → dossier d'échantillon inexistant → **100 % de `unreachable`** dans
  les verdicts — corrigé (scripts/lib → racine à 2 niveaux), vérifié en direct ;
- le rapport v1 était calé sur des deltas inter-cas (voir leçon de méthode) — v2
  sur la fermeture single-snapshot.

---

### 1.9 Livrables et conclusions — phase 6 (05/11/2026)

Deux documents finaux, alignés sur les preuves des phases 2-5 (chaque affirmation cite
un relevé) :

- **[`docs/operations/guide-diagnostic.md`](../docs/operations/guide-diagnostic.md)**
  — le guide praticien demandé par le sujet (« ce qu'un autre groupe peut suivre pendant
  un incident ») : prérequis, la commande et ses 5 modes, lecture d'une sortie (copie
  d'un verdict RÉEL), les 5 verdicts × 8 causes nommées + vérifications indépendantes,
  la procédure en 4 pas, les 3 situations de la mission rejouées (timelines réelles de
  la phase 5, dont la transition `indeterminate/ambiguous_sla` — l'exemple parfait du
  « ne pas conclure »), la règle de non-confusion (fermeture), les pièges anti-pattern,
  le dépannage, et §10 « ce que la garantie couvre — et ne couvre pas » (résultat
  négatif : aucune perte provoquable, assumé) ;
- **[`docs/architecture/diagnostic-engine-design.md`](../docs/architecture/diagnostic-engine-design.md)**
  — la note de conception : 3 décisions structurantes (moteur pur hors socle, cache 2
  échantillons, relevé mutualisé), pipeline (garde-fou fraîcheur → règles croissantes →
  4 états), catalogue des 8 causes et des 7 raisons `indeterminate`, chaque seuil
  justifié par une mesure (`SIMULPIX_DIAG_*`), le contrôle de fermeture (fermeture
  single-snapshot), les 26 tests + campagnes, et §9 « portée et limites » (conclusion).

La « conclusion » du livrable (ce que la garantie couvre / ne couvre pas) est donc
doublée côté praticien (guide §10) et côté conception (note §9) ; les limites validées
en campagne (collecteur arrêté ne gèle pas le payload ; délai B inclut le snapshot
réplication ; aucune perte provoquable) y sont citées avec leur source.

**Relecture croisée des deux documents (05/11)** — corrections issues de la vérification
dans le code : (1) traitement d'un service down précisé — cause conclusive
`service_down:<nom>` pour un service **à famille** (c'est ainsi que C est détecté en 3 s),
`indeterminate/service_down_transversal` pour un service transversal,
`indeterminate/probe_blinded` pendant une perturbation réseau (sonde potentiellement
aveuglée, croiser l'indépendante + `docker ps`) ; (2) timeline d'A-draine calée sur les
horodatages réels (pause du relevé 113→247 s pendant l'attente de fermeture de
`cross-check.sh`) ; (3) entrées du pipeline et table des seuils clarifiées (clé d'env
vs valeur par défaut, `PREVIOUS_MAX_AGE` = âge de l'échantillon précédent du cache).

Avec phases 2-5 closes, les preuves attendues du sujet sont toutes couvertes : matrice
(`notes/matrice-symptomes-causes.md`), relevés ≥ 3 situations + témoin
(`preuves/phase4-…`, `preuves/phase5-…`), amélioration exploitée (`diagnose.sh`),
démonstration de non-confusion (`SYNTHESE.md` phase 5), guide reproductible.

---

### 1.10 Impact métier — phase 7, candidate #7 implémentée (05/11/2026)

**Choix** : parmi les extensions (impact métier, détection adaptative, vue
HTTP/Grafana), la **candidate de garantie #7** est retenue et implémentée — champ
« impact métier » par incident, vérifiable en base.

**Code** :
- moteur : bloc **`business_impact`** attaché dans `build_verdict` après les règles —
  total des dépassements (compteurs `validated/rejected_sla_breaches`), `delta` entre
  2 lectures espacées ≥ 10 s (même garde-fou anti lag éclair), répartition
  « acceptés en retard / rejets TTL », phrase métier imprimée dans le verdict et
  `independent_check` = `tp-db.sh` ; `null` sur sondes périmées / données manquantes ;
- CLI : ligne `impact` dans la sortie humaine ; cache/enregistreur inchangés ;
- rapports/protocole : `scripts/impact-metier-proof.sh` + `scripts/lib/impact_report.py`.

**Source en base** (le socle ne change pas) : `probes.py` compte les colonnes
`decision_within_sla` des deux tables et l'expose deux fois (`metrics.counts.*` et
`details.postgres.*`) — la preuve confronte ces trois couches.

**Preuve live** (`preuves/phase7-impact-metier-2026-11-05/SYNTHESE.md`) : scénario
`consumer_lag 300 @ 20/s`, 3 couches comparées à 5 instants (t0 / pendant ×2 / état
fermé ×2) : moteur **278** == sonde PG **278** == SQL direct **278** (rejets 100 %
`processing_timeout`), delta contrôlé moteur **0** == variation SQL **0** à l'état
fermé, total montant pendant l'incident (13 → 238, ≈ débit réel du decision-engine)
puis immobilisé après la fermeture : **6/6 checks PASS**. Non-confusion maintenue :
les 300 rejets métier « dans le SLA » restés de l'essai errors_simple ne comptent pas
comme dépassements ; l'incident est détecté à impact **0/0** (le retard précède le
TTL) ; `unreachable` pendant le redémarrage de la plateforme ne conclut rien.

**Tests** : 26 → **32 tests unitaires OK** (6 nouveaux : nominal 0, comptage pendant
incident, delta inter-échantillons, présent sur `indeterminate`, absent si données
manquantes, `null` sur sondes périmées).

**Docs** : guide §2 (ligne `impact` du verdict), §5.1 (impact pendant un retard),
§8 (rejouage phase 7), §10 (couvert) ; note de conception §2/§3/§8/§9 (couvert +
limite de lecture), §10 (candidate #7 passe de « prolongement » à « implémentée »).

---

## 2. Journal des décisions

| Date | Décision |
|------|----------|
| 03/11/2026 | Garantie principale = candidate **#1** (désambiguïsation des 3 familles) ; critères #3, #4, #6, #5 rattachés |
| 03/11/2026 | Candidate #2 = bonus, **mais** sa démonstration reste un livrable obligatoire |
| 03/11/2026 | Seuil de détection **≤ 30 s** (2-3 cycles du collecteur + latence acteur) |
| 04/11/2026 | Plan d'architecture dans un fichier dédié (`docs/architecture/plan-architecture-sae5.md`) |
| 04/11/2026 | Pilotage fusionné dans `travail/` : `avancement.md` + `TODO.md` |
| 04/11/2026 | État des lieux **terminé** : inventaire métriques complet (pull `95556a5` + `a394db6`) — Phase 2 close |
| 04/11/2026 | **Matrice symptôme / cause / vérification v1** (`05c67db`, auteur anayel) : 5 lignes de diagnostic, manuelle — à automatiser |
| 04/11/2026 | **Classement probabilité / criticité** ajouté à la matrice : échelles qualitatives, priorité d'examen (critique d'abord si vérification rapide), top 3 = PostgreSQL > broker > retard qui dure — à valider en Phase 4 |
| 05/11/2026 | **Perte du travail du 03/11** (non commité : diagnose.py dans service-health, scripts d'essais, preuves phase 3/4) au pull du 04/11 ; leçons validées conservées (cadence ~20 s, règle 2 échantillons, discriminateur « écart qui S'ACCROÎT ») et ré-appliquées |
| 05/11/2026 | Architecture du moteur : **module pur `scripts/lib/diagnose_engine.py` + CLI `scripts/diagnose.sh`**, PAS d'intégration dans `service-health` (rebuild image à chaque itération pénaliserait la campagne) ; vue HTTP/panneau différée après la phase 4 |
| 05/11/2026 | Sortie du moteur à **4 états + healthy** avec cause nommée, `plausible_causes[]` ordonnée par priorité d'examen, `evidence[]` double lecture, garde-fou fraîcheur 75 s, 2 échantillons ≥ 10 s pour les deltas |
| 05/11/2026 | **Moteur écrit + 26 tests OK** ; essai CLI bout en bout validé hors plateforme (fixtures du payload nominal de référence) ; campagne live (phase 4) en attente de Docker/WSL réactivé |
| 05/11/2026 | **Campagne phase 4 tournée** (`scripts/diagnostic-campaign.sh` + rapport) : justesse **4/4**, délai **max 25 s**, faux positifs **0/70** sur 5 témoins ; discriminateur « écart qui S'ACCROÎT » validé en direct (3 → 319) ; seuils tenus sans recalibration |
| 05/11/2026 | Cadence de relevé fixée à **11 s** (≥ span moteur 10 s) : la v1 à 5 s bloquait les règles à delta sur `needs_second_sample` — relevés v1 archivés (`archives-rec5s/`), protocole documentant la quantification du délai (`notes/protocole-campagne-phase4.md`) |
| 05/11/2026 | **Phase 5 (vérification croisée) tournée** : 4/4 PASS — A : backlog drainé, 580 rejets TTL **persistés** (≠ perte) ; C : rattrapage complet (0 perte mesurée) ; rejets-metier : rejet ≠ incident (0 verdict d'incident) ; B : verdict cross-checké sur Kafka en direct. Candidate **#2** démontrée |
| 05/11/2026 | Convergence redéfinie en **fermeture single-snapshot** (`generated==raw_end==validated+rejected==outcome==db_total`, écarts 0, lag ≤ 5) : les deltas inter-cas sont interdits — `run-scenario.sh` relance service-health (compteurs re-calculés à chaque cas) |
| 05/11/2026 | Enregistreur de verdicts **mutualisé** (`scripts/lib/record-verdicts.sh`, campagne phase 4 refactorée dessus) ; bug `REPO_DIR` à un cran corrigé (v1 : 100 % `unreachable`) |
| 05/11/2026 | **Phase 6 (livrables) écrite** : guide praticien + note de conception (seuils justifiés par les mesures) ; la « conclusion » est double : guide §10 (praticien) + note §9 (conception), incluant le résultat négatif « aucune perte provoquable, les persisters rejouent » |
| 05/11/2026 | Vue HTTP/panneau Grafana du moteur **définitivement différée** indiquée dans le livrable (décision phase 4 maintenue) ; la CLI est le produit |
| 05/11/2026 | **Relecture croisée guide + note de conception** : imprécision corrigée sur le service down (conclusif pour un service à famille, `transversal`/`probe_blinded` sinon — vérifié dans le code), timeline A calée sur les horodatages, table des seuils clarifiée |
| 05/11/2026 | **Phase 7 (impact métier, candidate #7) implémentée** : bloc `business_impact` factuel dans le verdict (moteur croise les compteurs SLA du socle, branche-cause intacte, garde-fous identiques) ; 32 tests OK ; preuve live moteur==sonde==SQL 278==278 (6/6) dans `preuves/phase7-impact-metier-…` ; adaptative et vue HTTP restent en réserve |

---

## 3. Résultats clés à retenir (pièges connus)

- **`lag_total == 1` en nominal est normal** (rattrapage éclair) : toute règle brute « lag > 0 = incident » produirait des faux positifs.
- **`history` gèle en idle** (dernier échantillon observé : 710 s) → d'où le critère #6 (fraîcheur des sondes).
- **`estimated_oldest_lag_seconds`** = ratio lag/débit, **pas un âge** ; disparaît quand le générateur s'arrête alors que la file existe toujours.
- **`persisted_*`** (compteur interne des persisters) ≠ **`db_validated_count` / `db_rejected_count`** (réalité vérifiée en base).
- **Un offset Kafka seul ne prouve rien** : toujours lire PostgreSQL.
- Alertes existantes **indépendantes les unes des autres** : rien ne dit laquelle explique l'autre → c'est l'ambiguïté à lever.

---

## 4. En cours

Rien en cours — phases 2 à 7 closes. La garantie #1 est mesurée et tenue (justesse
4/4, délai max 25 s, 0 faux positif), la candidate #2 est démontrée (4/4 PASS), les
livrables finaux sont édités et la candidate #7 (impact métier) est implémentée et
prouvée (6/6). Il reste la dimension théorique (facultative, encouragée) et le rendu
GitHub final.

---

## 5. Prochaines étapes

1. **Pusher la phase 7** : moteur + CLI + tests + scripts de preuve + preuves + docs
   + pilotage (liste fournie à l'utilisateur).
2. **Phase 9 — rendu GitHub final** (à faire en continu) : README racine (sujet,
   garantie, démarche de rejeu incluant `impact-metier-proof.sh`), vérifier pas de
   secrets ni de `runtime/` déposé, collaborateur invité, dépôt en PUBLIC la veille
   de la soutenance.
3. **Phase 8 — dimension théorique (encouragée)** : formaliser les hypothèses de
   cause / observations discriminantes / cas indéterminé ; consulter
   `TP/SAé/analyse-recherche-limos.md` et préparer l'échange avec l'enseignant/LIMOS.
4. Extensions restées en réserve (si surplus de temps) : détection adaptative, vue
   HTTP/panneau Grafana du moteur.

