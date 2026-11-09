# Étude des trois familles de problèmes — arbre de causalité, solutions, planning

Réponse aux quatre demandes de l'enseignant (arbre de causalité, étude des trois
familles de problèmes, solutions, planning de la semaine). Ce document **assemble**
les livrables existants du dépôt — il ne remplace ni la matrice
([matrice-symptomes-causes.md](matrice-symptomes-causes.md)), ni l'analyse théorique
([docs/architecture/analyse-theorique-diagnostic.md](../../docs/architecture/analyse-theorique-diagnostic.md)),
ni la note de conception ([docs/architecture/diagnostic-engine-design.md](../../docs/architecture/diagnostic-engine-design.md)).

---

## 1. L'arbre de causalité

L'arbre complet (racine = symptôme visible, branches = causes probables classées,
feuilles = vérification discriminante) est tracé dans
[matrice-risques-arbre.svg](matrice-risques-arbre.svg). Sa structure vient de la
matrice de diagnostic (5 symptômes × 2 causes × 1 vérification) :

| Symptôme (racine) | Cause probable n°1 | Cause probable n°2 | Vérification qui tranche |
|---|---|---|---|
| La file d'attente grossit (`pending_result_count` ↑, lag ↑) | retard de lecture applicatif (**Élevée**/Moyenne) | PostgreSQL ne suit plus (Moyenne/**Élevée**) | sur quelle application porte le lag (`tp-kafka.sh`) + `persistence_gap_*` |
| Des tranches de topics mal répliquées (URP > 0) | retard de copie entre brokers (**Élevée**/Faible) | broker arrêté (Faible/**Élevée**) | `offline_partitions_count` et `active_controller_count` (`replication.brokers[]`) |
| Plus aucune écriture en base | amont arrêté, générateur fini (**Élevée**/Faible) | PostgreSQL indisponible (Moyenne/**Critique**) | `counts.generated` stagne → amont ; monte sans écriture → PostgreSQL |
| Des paiements traités trop lentement (`sla_breaches` ↑) | retard de lecture en amont (**Élevée**/Moyenne) | délai trop court pour la charge (Faible/Moyenne) | longueur de la file au moment du dépassement |
| Verdict global `degraded` | service ne répond plus (Faible/**Élevée** selon service) | sonde en échec, faux positif (Moyenne/Faible) | `services.<nom>` + sonde indépendante |

**Règle de lecture de l'arbre** : on écarte d'abord la cause la plus **critique** quand
sa vérification est peu coûteuse (les vérifications `tp-kafka.sh` / `tp-db.sh` sont des
instantanés de quelques secondes), puis on confirme la cause la plus **probable**.
Exemple : sur un URP > 0, on vérifie la panne de broker (critique, ~5 s) avant de
conclure au retard de copie (probable, bénin).

---

## 2. Étude des trois familles de problèmes

Les trois situations de la mission, chacune traitée de bout en bout : symptôme →
causes → observations discriminantes → solution → preuve chiffrée.

### Famille A — le retard de consommateur (lecture)

- **Symptôme visible** : la file d'attente grossit (`pending_result_count` en hausse,
  lag des groupes consommateurs en hausse), puis des paiements dépassent le SLA de
  décision de 10 s (`validated_sla_breaches` ↑).
- **Causes probables** : retard de lecture applicatif (probabilité élevée, criticité
  moyenne — rattrapable mais réel si le lag dure) vs PostgreSQL qui ne suit plus
  (probabilité moyenne, criticité élevée — durabilité menacée).
- **Observations discriminantes** : le lag porté par `pix-validator` / `pix-decision-engine`
  pointe la **lecture** ; un lag concentré sur les persisters avec `persistence_gap_*`
  qui grossit pointe la **persistance**. Un `processing_timeout` massif pointe l'amont.
- **Solution apportée** : le moteur lit le lag **par application** (et non un lag global)
  et sépare automatiquement les deux familles ; le verdict transporte la cause nommée
  (`reader_backlog`) et l'impact métier.
- **Preuve chiffrée** : cas `A-retard-lecture` de la campagne (délai 400 ms injecté sur
  le traitement) — cause `reader_backlog` nommée en **15,0 s**, 8 échantillons
  `consumer_backlog` sur 9, l'état `indeterminate` du 9e étant le cas normal
  `needs_second_sample`. Rejeu indépendant du 08/11 conforme.

### Famille B — la réplication Kafka dégradée

- **Symptôme visible** : des tranches de topics mal répliquées (`under_replicated_partitions`
  > 0), visibles dans `replication.brokers[]`.
- **Causes probables** : retard de copie entre brokers (probabilité élevée, criticité
  faible — bénin, passager, se résorbe seul) vs **broker arrêté** (probabilité faible,
  criticité élevée — à RF=3 et min ISR=2, un second incident rend les écritures
  impossibles).
- **Observations discriminantes** : `offline_partitions_count` > 0 ou
  `active_controller_count` = 0 → **panne de broker**. Sinon, si `broker_count` = 3 et
  les tranches se resynchronisent → simple **retard de copie**.
- **Solution apportée** : le moteur croise les trois compteurs (URP, offline, contrôleur)
  et distingue `replication_catchup` (bénin) de `broker_down` (grave), avec l'avertissement
  min ISR explicite dans le verdict.
- **Preuve chiffrée** : cas `B-replication` (panne de 20 s sur `kafka-3`) — cause
  `replication_catchup` nommée en **20,0 s** (rejeu) / 25,0 s (origine), 7 échantillons
  sains sur 9 ; cas voisin `broker_down` vérifié en campagne d'exploitation (URP = 86,
  2 brokers sur 3, `diagnose.sh` a nommé la cause immédiatement).

### Famille C — la persistance PostgreSQL qui ne suit plus

- **Symptôme visible** : plus aucune écriture en base (`db_*` compteurs figés) ou des
  traitements qui s'accumulent côté base, sans rejet métier.
- **Causes probables** : PostgreSQL indisponible/saturé (probabilité moyenne, criticité
  **critique** — perte durable potentielle) vs amont arrêté, générateur fini (probabilité
  élevée, criticité faible — situation **normale** en fin de scénario).
- **Observations discriminantes** : `counts.generated` qui **stagne** avec les écritures
  arrêtées → amont (normal) ; `counts.generated` qui **continue de monter** sans écriture
  → PostgreSQL. Confirmation directe par `tp-db.sh` et `services.postgres`.
- **Solution apportée** : le moteur sépare `service_down:postgres` (indisponibilité) de
  `persister_slow` (désaturation progressive) et de `upstream_stopped_normal` (fin de
  scénario), et vérifie la fraîcheur des sondes avant toute conclusion.
- **Preuve chiffrée** : cas `C-postgres-indispo` (arrêt de PostgreSQL) — cause
  `service_down:postgres` nommée en **5,0 s**, 4 échantillons `persistence` sur 9 ;
  cas `C-postgres-degrade` (350 ms injectés sur les persisters) — cause `persister_slow`
  nommée en **16,0 s**. Bilan famille C : 2/2, zéro confusion avec la famille A.

### Les faux amis à ne pas confondre (les 3 vues de lecture)

| Vue | Signaux lus | Incident détecté | Faux amis à éviter |
|-----|-------------|------------------|--------------------|
| **Métier** | counts, taux de rejet, SLA breaches | impact Pix (rejets `PROCESSING_TIMEOUT`, TTL 10 s) | rejet métier ≠ incident ; `lag_total = 1` en nominal est **normal** |
| **Transport** | URP, brokers offline, lag par application, ancienneté backlog | familles A et B | backlog ≠ perte ; moyennes sans p95/p99 |
| **Persistance** | persistence gap, db counts vs compteurs métier | famille C | **un offset Kafka seul ne prouve rien : toujours lire PostgreSQL** |

---

## 3. Solutions apportées

### La chaîne de diagnostic

1. **La règle** — la matrice symptôme / cause probable / vérification
   ([matrice-symptomes-causes.md](matrice-symptomes-causes.md)), tracée depuis l'arbre
   de causalité, formalisée théoriquement dans
   [l'analyse](../../docs/architecture/analyse-theorique-diagnostic.md) (le diagnostic vu
   comme une fonction, le cas indéterminé défini) ;
2. **L'automatisation** — `scripts/diagnose.sh` + `scripts/lib/diagnose_engine.py` : le
   moteur lit `/health/details`, garde deux échantillons successifs (deltas), applique
   la matrice avec les seuils `SIMULPIX_DIAG_*`, contrôle la fraîcheur des sondes, et
   sort un verdict à 4 valeurs (`healthy` / cause nommée / `indeterminate`) avec
   vérification indépendante pointée et impact métier ;
3. **Les essais** — `scripts/diagnostic-campaign.sh` : 5 témoins nominaux + 4 incidents
   injectés (les 3 familles), rapport de justesse / faux positifs / délai de détection ;
4. **Le transfert** — le guide praticien
   ([docs/operations/guide-diagnostic.md](../../docs/operations/guide-diagnostic.md)) et la
   note de conception
   ([docs/architecture/diagnostic-engine-design.md](../../docs/architecture/diagnostic-engine-design.md)).

### Ce qui est garanti (mesuré, pas promis)

- nomination de la famille en **≤ 30 s** sur les 3 familles + variante lente ;
- **0 faux positif** en nominal (5 témoins, campagne du 05/11 et rejeu indépendant du
  08/11 : justesse 4/4, délai max 20 s) ;
- refus de conclure **explicite** (7 raisons cataloguées) quand la donnée ne le permet pas ;
- fraîcheur des sondes vérifiée avant toute conclusion (critère #6) ;
- **impact métier par incident** : « X Pix dépassent le SLA de décision », vérifié en
  base à la ligne près (278 == 278).

### Ce qui n'est pas garanti (honnêteté)

- « impact métier » = nombre de dépassements du SLA de décision — pas une estimation
  monétaire ni un pourcentage du trafic ;
- **aucune perte n'a pu être provoquée** : les persisters du socle rejouent toujours ;
  la branche « écart stable = perte » est codée mais jamais déclenchée en campagne —
  documenté tel quel ;
- seuils **fixes** (extension adaptative non implémentée ; le sujet ne l'exige pas) ;
- pas de vue HTTP/Grafana du moteur — la CLI est le produit livré ;
- causes **hors matrice** (un symptôme non répertorié conduit à `indeterminate`, pas à
  une conjecture).

---

## 4. Planning de la semaine (du 09/11 au 14/11)

| Membre | Tâches de la semaine | Livrable attendu |
|---|---|---|
| **TomPERRIN** (développement + intégration) | fusionner les PR en attente (README/section 3.3, rejeu, correctifs) ; rester réactif aux écarts trouvés par le rejeu ; écrire la conclusion finale | `main` à jour et exécutable ; conclusion validée |
| **Ayel04** (analyse) | relecture de l'analyse théorique et de la matrice ; préparation de sa partie soutenance (méthodologie d'analyse) | relectures documentées ; soutenance prête |
| **Madiak5** (documentation + revue + rejeu) | ~~rejeu indépendant du protocole~~ **fait (08/11, conforme)** ; test du guide praticien en lecteur externe pendant un incident ; vérification de la conclusion sur les limites ; préparation soutenance | preuves de rejeu archivées ; corrections du guide ; soutenance prête |
| **Ania** (essais indépendants + documentation) | test du guide praticien pendant un incident ; vérification de la fraîcheur des sondes (arrêt de `metrics-collector` → verdict `indeterminate`) | scénario documenté ; fraîcheur vérifiée |

**Point de synchronisation** : chaque séance — bilan fait/reste, répartition des
tâches suivantes, push de la branche de chacun (section 3.3 du TODO).

---

## Références

- Arbre : [matrice-risques-arbre.svg](matrice-risques-arbre.svg) · Matrice : [matrice-symptomes-causes.md](matrice-symptomes-causes.md)
- Théorie : [docs/architecture/analyse-theorique-diagnostic.md](../../docs/architecture/analyse-theorique-diagnostic.md)
- Produit : [docs/architecture/diagnostic-engine-design.md](../../docs/architecture/diagnostic-engine-design.md) · Guide : [docs/operations/guide-diagnostic.md](../../docs/operations/guide-diagnostic.md)
- Preuves : [preuves/phase4-campagne-2026-11-05/](../../preuves/phase4-campagne-2026-11-05/) (origine) · [preuves/phase4-campagne-rejeu-Madiak5-2026-11-08/](../../preuves/phase4-campagne-rejeu-Madiak5-2026-11-08/) (rejeu indépendant)
- Inventaire des signaux : [metriques-observabilite.md](metriques-observabilite.md)
