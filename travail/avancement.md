# Avancement — SAÉ 5 · Observabilité et diagnostic d'incidents

> Fichier de suivi d'avancement détaillé (ce qui est fait, résultats, décisions).
> Les tâches restantes sont pilotées dans [`TODO.md`](TODO.md).
> Dernière mise à jour : 2026-11-04.

---

## Vue d'ensemble

| Étape | État | Commentaire |
|-------|------|-------------|
| Choix du sujet + répertoire de travail | ✅ fait | `travail/` créé (`notes/`, `essais/`) |
| Plateforme opérationnelle | ✅ fait | `start.sh` OK, flux nominal validé (13/13) |
| Formulation de la garantie | ✅ fait | Garantie #1 retenue, seuil 30 s justifié |
| État des lieux des métriques | ✅ fait | Inventaire complet (§1-§4 + synthèse, pull 04/11) |
| Matrice symptôme / cause / vérification | ✅ fait (v1) | `notes/matrice-symptomes-causes.md` — 5 lignes + classement probabilité/criticité, **manuelle** (commit `05c67db`) |
| Automatisation de la matrice (le produit) | ⬜ à faire | Rendre les règles automatiques : métrique dérivée / alerte / vue / commande |
| Incidents provoqués et mesurés | ⬜ à faire | Familles A, B, C + témoin nominal |
| Livrables finaux | ⬜ à faire | Note de conception, protocole, guide |

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

Rien en cours — la matrice v1 est posée ; l'étape suivante est son automatisation (Phase 3).

---

## 5. Prochaines étapes

1. **Automatiser la matrice v1** : coder les 5 règles de croisement (métrique dérivée / alerte / vue / commande de diagnostic) → Phase 3.
2. Intégrer le garde-fou fraîcheur (critère #6) et le test unitaire de la machine à états (critère #5) → Phase 3.
3. Scripter les 3 incidents + témoin, lancer la campagne de mesures (justesse, délai ≤ 30 s, faux positifs) → Phase 4.
4. Vérification croisée Kafka ↔ PostgreSQL sur chaque conclusion → Phase 5.
5. Rédiger les livrables (note de conception, protocole, guide praticien) → Phase 6.
