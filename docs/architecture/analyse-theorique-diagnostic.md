# Analyse théorique du diagnostic — SAÉ 5

> **Garantie étudiée** : « Notre lecture d'incident ne doit pas confondre retard de
> consommateur, réplication dégradée et panne de persistance, prouvée par une matrice
> validée sur incidents injectés : bonne cause retenue sur 100 % des cas connus, état
> “indéterminé” + vérification suivante sinon. »
>
> Ce document formalise le problème de diagnostic, explicite les hypothèses et les
> observations qui les départagent, justifie les seuils, puis analyse les erreurs
> observées sur les campagnes des phases 4, 5 et 7. Il ne remplace ni la note de
> conception ([`diagnostic-engine-design.md`](diagnostic-engine-design.md)) ni le guide
> praticien ([`../operations/guide-diagnostic.md`](../operations/guide-diagnostic.md)) :
> il prend du recul sur ce qui est **prouvé**, **mesuré** et **seulement supposé**.

---

## 1. Objet et portée

Le sujet demande de passer d'un **symptôme** (une courbe qui monte, un service qui ne
répond plus) à une **cause vérifiable**, sans ajouter de nouvelle sonde. La difficulté
n'est pas de collecter des métriques — le socle le fait déjà — mais de **lever une
ambiguïté** : plusieurs causes produisent le même symptôme.

Cette analyse poursuit quatre objectifs :

1. **formaliser** le diagnostic comme une inférence à partir d'observations, avec un
   état d'abstention explicite ;
2. **expliciter** les hypothèses de cause et les observations qui les discriminent ;
3. **justifier** chaque seuil par une mesure, et non par une intuition ;
4. **quantifier** les erreurs sur les incidents connus, en distinguant ce qui est
   établi de ce qui reste une extrapolation.

Elle ne prétend pas démontrer la sûreté du diagnostic sur tous les incidents possibles.
Elle établit une propriété sur un **ensemble fini de cas connus**, et borne
l'incertitude qui reste.

---

## 2. Vocabulaire et formalisation

### 2.1 Observations, symptômes, hypothèses

- **Observation** `o` : un payload `/health/details` à un instant donné (compteurs,
  débits, écarts de persistance, lag par groupe, état de réplication, statut des
  services, alertes).
- **Symptôme** `s` : la première anomalie visible dans `o` (file qui grossit, URP > 0,
  écritures figées, dépassements SLA, service `degraded`).
- **Hypothèse de cause** `h` : l'explication que l'on cherche à confirmer ou écarter.
- **Verdict** `v` : la conclusion du moteur, éventuellement « indéterminé ».

Les familles de verdict sont :

```
H = { A = consumer_backlog,
      B = kafka_replication,
      C = persistence,
      H0 = healthy }
```

et l'abstention est notée `⊥` (« indéterminé »).

### 2.2 Le diagnostic comme fonction

Le moteur implémente une fonction :

```
f : O  ⟶  H ∪ {⊥}
```

avec une règle de décision du type :

```
f(o) = h        si les observations discriminantes de h sont présentes
f(o) = ⊥        sinon, avec une raison et une vérification suivante
```

Trois propriétés sont recherchées :

1. **Justesse** : sur un incident connu, `f(o)` doit nommer la bonne famille.
2. **Spécificité** : sur un flux nominal, `f(o)` doit rester `H0`.
3. **Abstention** : quand la donnée ne permet pas de trancher, `f(o) = ⊥` plutôt
   qu'une conclusion fausse.

L'abstention n'est pas une erreur de diagnostic : c'est une **sortie de première
classe**, qui déplace le coût de l'erreur vers un coût de délai. Dans un incident, une
conclusion fausse peut orienter vers la mauvaise correction ; une abstention explicite
coûte quelques secondes et indique quoi faire ensuite.

### 2.3 Deux niveaux d'évaluation

Il faut distinguer :

- le **niveau cas** : un cas d'essai (un incident injecté, ou un témoin nominal) est
  correct si le moteur finit par nommer la bonne famille — ou reste `healthy` pour un
  témoin — avant la fin de la fenêtre ;
- le **niveau échantillon** : chaque relevé du moteur (toutes les 11 s) est comparé à
  l'état attendu à cet instant.

Le niveau cas mesure la **capacité à conclure** ; le niveau échantillon mesure le
**comportement transitoire** (abstentions, retours à `healthy`, éventuels faux
positifs). Les deux sont nécessaires : un moteur qui ne conclut jamais aurait 0 % de
faux positifs mais serait inutile ; un moteur qui conclut trop vite aurait de bons
délais mais des erreurs.

---

## 3. Hypothèses et observations discriminantes

### 3.1 Les cinq lignes de la matrice

La matrice de la phase 3 ([`../../travail/notes/matrice-symptomes-causes.md`](../../travail/notes/matrice-symptomes-causes.md))
est la spécification du diagnostic. Chaque ligne part d'un symptôme et oppose au moins
deux causes plausibles, avec l'observation qui tranche.

| Symptôme vu en premier | Hypothèse 1 | Hypothèse 2 | Observation discriminante | Réf. |
|---|---|---|---|---|
| File d'attente qui grossit (`pending_result_count` ↑, lag ↑) | retard de lecture applicatif (`reader_backlog`) | PostgreSQL ne suit plus (`persister_slow`) | **quel groupe** est en retard + **tendance de l'écart de persistance** : un écart qui **s'accroît** de ≥ 5 entre deux lectures ⇒ persistance ; un lag sur les lecteurs ⇒ retard de lecture | matrice ligne 1 ; conception §2 |
| URP > 0 (tranches mal répliquées) | broker arrêté (`broker_down`) | simple retard de copie (`replication_catchup`) | `offline_partitions_count`, `active_controller_count`, `broker_count < 3` : une panne est vérifiée **avant** de conclure au retard bénin | matrice ligne 2 ; conception §2/§4 |
| Plus aucune écriture en base (`db_*` figé) | PostgreSQL indisponible/saturé (`postgres_unavailable`) | amont arrêté, cas normal de fin de scénario (`upstream_stopped_normal`) | `counts.generated` **stagne** (2 lectures) ⇒ amont ; `generated` monte sans écriture ⇒ PostgreSQL | matrice ligne 3 ; conception §2 |
| Paiements trop lents (`sla_breaches` ↑) | retard de lecture en amont (`upstream_backlog_sla`) | délai de décision trop court (`sla_config`) | file présente (`pending_result_count`, lags) au moment du dépassement ; sinon latence moyenne > 80 % du SLA avec file vide | matrice ligne 4 ; conception §2 |
| Verdict `degraded` | panne d'un service **à famille** (`service_down:<nom>`) | sonde aveuglée ou service transversal (`probe_blinded`, `service_down_transversal`) | famille du service + `details.network.active` + sonde indépendante (`tp-kafka.sh`, `tp-db.sh`, `docker ps`) | matrice ligne 5 ; conception §2 |

### 3.2 Pourquoi deux causes par symptôme

Le point clé est qu'un symptôme n'est **pas** une cause. Un lag qui monte peut venir
d'un lecteur lent ou d'un persister lent ; un URP peut venir d'un broker mort ou d'une
copie en retard ; une base figée peut venir de PostgreSQL ou de l'arrêt normal du
générateur. La contribution du sujet 5 n'est donc pas « afficher le lag », mais
**ajouter l'observation qui départage** :

- pour la ligne 1, la **dérivée** de l'écart de persistance (pas son niveau) ;
- pour la ligne 2, l'**état du cluster** (offline, contrôleur, nombre de brokers) ;
- pour la ligne 3, la **tendance** de `generated` sur deux lectures ;
- pour la ligne 4, la **présence d'une file** au moment du dépassement ;
- pour la ligne 5, la **famille** du service et l'état du réseau.

C'est cette structure qui permet de dire, dans la sortie, non seulement « quelle
famille », mais aussi **quelles causes ont été écartées et pourquoi**.

### 3.3 La conservation des Pix : backlog ≠ rejet ≠ perte

La non-confusion demandée par le sujet repose sur une identité de conservation. À la
fermeture d'un essai, on doit avoir :

```
generated == raw_end
          == validated + rejected
          == outcome
          == db_validated + db_rejected
```

avec, en plus, un écart de persistance nul et un lag résorbé. Cette identité sépare
mécaniquement trois réalités :

- **backlog** : une partie des Pix est encore **en vol** (`in-flight > 0`), mais la
  fermeture finit par être atteinte — la donnée n'est pas perdue ;
- **rejet métier** : `rejected > 0` et `db_rejected == rejected` — le rejet est
  **persisté**, ce n'est pas une perte ;
- **perte** : un écart de persistance **stable** qui refuse de se résorber après
  drainage — la fermeture n'est pas atteinte à l'échéance.

C'est un critère **mécanique**, pas une opinion : il est implémenté dans
[`../../scripts/lib/convergence_check.py`](../../scripts/lib/convergence_check.py) et
utilisé par la campagne croisée de la phase 5.

---

## 4. Le cas indéterminé

### 4.1 Définition

`f(o) = ⊥` lorsque l'une des conditions suivantes est vraie :

- les observations sont **insuffisantes** (une seule lecture pour une règle qui exige
  une variation) ;
- les observations sont **périmées** (sondes plus vieilles que le seuil de fraîcheur) ;
- les observations sont **contradictoires** (deux sources ne disent pas la même chose) ;
- l'observation est **aveuglée** (une perturbation réseau empêche de voir l'état réel) ;
- aucune cause ne **domine** les autres.

### 4.2 Catalogue des sept raisons

| Raison | Déclencheur | Vérification suivante |
|---|---|---|
| `needs_second_sample` | une seule lecture, alors que la règle exige un delta | relire à ~15 s (`./scripts/diagnose.sh --watch 15`) |
| `probes_stale` | âge du payload ou du snapshot réplication > 75 s | inspecter la collecte, puis relire |
| `data_missing` | un champ attendu est absent du payload | inspecter la sonde / le service |
| `contradictory_signals` | les sources se contredisent | relire et croiser `tp-kafka.sh` + `tp-db.sh` à la main |
| `ambiguous_sla` | SLA en hausse avec file vide **et** latence dans le seuil | relire : le mouvement tranche |
| `ambiguous_non_dominated` | aucune cause n'est dominante | attendre / relire |
| `probe_blinded` / `service_down_transversal` | la panne empêche de voir l'état, ou le service tombé n'appartient à aucune famille | remettre la sonde, vérifier `docker ps` et la sonde indépendante |

### 4.3 Pourquoi l'abstention est une fonctionnalité

Trois arguments :

1. **Sécurité** : une conclusion fausse peut envoyer l'opérateur vers la mauvaise
   correction (redémarrer PostgreSQL alors que le problème est un lecteur lent).
2. **Honnêteté** : le système ne prétend pas savoir ce qu'il ne sait pas ; il affiche
   la raison et la vérification suivante.
3. **Mesurabilité** : l'abstention est observable et quantifiable (3 échantillons sur
   40 en phase 4, soit 7,5 %), au lieu d'être un silence.

Le prix est un **délai** : sur un incident qui exige deux lectures, la conclusion
arrive au deuxième échantillon, soit ~11–15 s après le premier. Ce délai est inclus
dans la mesure de la garantie (≤ 30 s).

---

## 5. Justification des seuils

### 5.1 Principe

Un seuil `θ` sépare le **bruit nominal** du **signal d'incident**. Il gouverne un
compromis :

- `θ` **trop bas** ⇒ détection précoce mais faux positifs (le nominal déclenche) ;
- `θ` **trop haut** ⇒ pas de faux positifs mais détection tardive, voire manquée.

Le choix n'est pas arbitraire : il est contraint par la **cadence de collecte** et par
l'**amplitude mesurée** des signaux. Tous les seuils sont réglables via
`SIMULPIX_DIAG_*` ; les valeurs par défaut sont celles qui ont été évaluées en campagne.

### 5.2 Table des seuils

| Seuil | Valeur | Mesure / justification | Compromis |
|---|---|---|---|
| Âge maximal des sondes | **75 s** | collecteur configuré à 10 s, cadence réelle observée ~20 s ; `history` 5,7–12,3 s ; 75 s ≈ 3–4 cycles | trop bas ⇒ `probes_stale` à tort ; trop haut ⇒ conclusion sur données périmées |
| Lag total d'incident | **10** | lag éclair nominal = **1** ; incident A monté à **325–517** | 10 sépare le bruit du signal de plus d'un ordre de grandeur |
| Grand écart immédiat | **20** | écart de persistance ≥ 20 déjà suspect à une lecture, mais reste une **hypothèse** sans deuxième échantillon | trop bas ⇒ suspicion sur un début de flux normal ; trop haut ⇒ attente inutile |
| Pas d'accroissement | **5** | sur C-dégradé, l'écart est passé de **3 à 319** entre deux lectures ; 5 est au-dessus du bruit d'arrondi | trop bas ⇒ `persister_slow` sur un écart instable ; trop haut ⇒ détection tardive |
| Span minimal | **10 s** | la v1 à 5 s restait bloquée en `needs_second_sample` ; 10 s = un cycle de collecte | trop bas ⇒ lags éclairs pris pour des incidents ; trop haut ⇒ délai |
| Âge max de l'échantillon précédent | **900 s** | au-delà de 15 min, le cache n'est plus une série fiable | trop bas ⇒ deltas impossibles ; trop haut ⇒ comparaison entre deux époques |
| Brokers attendus | **3** | RF = 3, min ISR = 2 ; `broker_count < 3` est déjà grave | — |
| Marge latence / SLA | **0,8** | SLA de décision = 10 s ; latence > 8 s avec file vide suggère un SLA trop court | trop bas ⇒ `sla_config` à tort ; trop haut ⇒ `upstream_backlog_sla` à tort |
| Débit « au repos » | **0,01 /s** | distingue un amont arrêté d'un flux lent | — |

### 5.3 Le compromis délai / faux positifs, en pratique

La campagne fournit une illustration directe :

- avec un seuil de lag à 10, le nominal (`lag = 1`) ne déclenche pas, et l'incident A
  (lag 325–517) est détecté au premier échantillon concluant, **15 s** après le début
  du scénario ;
- avec un span de 10 s, l'abstention `needs_second_sample` dure **un seul** échantillon
  sur A et C-dégradé, puis la conclusion tombe ;
- avec un seuil de fraîcheur à 75 s, les payloads de la campagne (âges observés
  ~0,1–0,2 s pour le payload, ~13–23 s pour le snapshot réplication) restent **frais**,
  sans faux `probes_stale`.

Autrement dit, les seuils retenus sont dans une **zone de séparation large** : le bruit
nominal est à 1, le signal incident à plusieurs centaines. C'est ce qui rend le
diagnostic robuste sans être trop sensible.

### 5.4 Pourquoi des seuils fixes

Le sujet autorise une détection adaptative, mais ne l'exige pas. Les seuils fixes ont
été préférés pour trois raisons :

1. **Explicabilité** : chaque seuil est justifié par une mesure et peut être discuté ;
2. **Testabilité** : les tests unitaires peuvent injecter des valeurs connues ;
3. **Risque d'adaptation à l'incident** : une baseline apprise pendant un incident
   pourrait « absorber » l'anomalie et masquer la panne.

L'extension adaptative reste une piste (voir §10), mais elle devrait être évaluée
contre les mêmes critères, pas supposée meilleure.

---

## 6. Analyse d'erreurs sur les campagnes

### 6.1 Protocole

- **Phase 4** : 4 incidents injectés (A, B, C-dégradé, C-indisponible) + **5 témoins
  nominaux**. Le moteur est relevé toutes les **11 s** (≥ span minimal de 10 s). Les
  relevés sont dans [`../../preuves/phase4-campagne-2026-11-05/`](../../preuves/phase4-campagne-2026-11-05/).
- **Phase 5** : 4 cas de vérification croisée (A drainé, B croisé, C rattrapé, rejets
  métier), avec captures `tp-kafka.sh` / `tp-db.sh` en direct et contrôle de fermeture.
  Les relevés sont dans [`../../preuves/phase5-croisee-2026-11-05/`](../../preuves/phase5-croisee-2026-11-05/).
- **Phase 7** : preuve d'impact métier sur un scénario A, avec confrontation
  moteur ⇄ sonde PostgreSQL ⇄ SQL direct.

Le délai de détection est mesuré entre l'événement d'injection (`fault_injected`, ou
`scenario_ready` pour les scénarios qui injectent dès le départ) et le **premier verdict
concluant de la bonne famille**.

### 6.2 Niveau cas : matrice de confusion

| Cas attendu | Première conclusion observée | Délai | Résultat |
|---|---|---|---|
| A — retard de lecture | `consumer_backlog` (`reader_backlog`) | 15 s | ✅ |
| B — réplication dégradée | `kafka_replication` (`replication_catchup`) | 25 s | ✅ |
| C — PostgreSQL dégradé | `persistence` (`persister_slow`) | 14 s | ✅ |
| C — PostgreSQL indisponible | `persistence` (`service_down:postgres`) | 3 s | ✅ |
| Témoins nominaux (×5) | `healthy` | — | ✅ 0 faux positif |

**Justesse au niveau cas : 4/4.** Aucune famille erronée n'a été nommée. Les témoins
n'ont produit aucun verdict d'incident.

### 6.3 Niveau échantillon : stabilité et abstentions

| Cas | Échantillons | A | B | C | `healthy` | `⊥` |
|---|---:|---:|---:|---:|---:|---:|
| A — retard de lecture | 11 | 10 | 0 | 0 | 0 | 1 |
| B — réplication | 10 | 0 | 2 | 0 | 8 | 0 |
| C — PostgreSQL dégradé | 10 | 0 | 0 | 3 | 6 | 1 |
| C — PostgreSQL indisponible | 9 | 0 | 0 | 3 | 5 | 1 |
| **Total incidents** | **40** | **10** | **2** | **6** | **19** | **3** |
| Témoins (×5) | 70 | 0 | 0 | 0 | 70 | 0 |

Lecture :

- **18 échantillons concluants** sur 40 (10 + 2 + 6) nomment la bonne famille ;
- **3 abstentions** (7,5 %), toutes `needs_second_sample` : le moteur refuse de conclure
  sur une seule lecture, puis conclut au suivant ;
- **19 `healthy`** : ils ne sont **pas** des faux négatifs. Ils correspondent à
  l'échantillon de B antérieur à l'arrêt effectif du broker (le scénario l'arrête
  après quelques secondes) et aux échantillons postérieurs au rétablissement
  (7 pour B, 6 pour C-dégradé, 5 pour C-indisponible). Autrement dit, le moteur
  **revient à `healthy`** après la fin de l'incident, sans alarme collante ;
- **aucune famille erronée** sur les 40 échantillons d'incident.

Les 70 échantillons des témoins sont tous `healthy` : **0 faux positif**.

### 6.4 Délais de détection

| Cas | Délai |
|---|---:|
| C — indisponibilité PostgreSQL | 3 s |
| C — dégradation PostgreSQL | 14 s |
| A — retard de lecture | 15 s |
| B — réplication | 25 s |

- minimum : **3 s** ;
- médiane : **14,5 s** ;
- moyenne : **14,25 s** ;
- maximum : **25 s** ≤ **30 s** (garantie).

Le cas B est le plus lent parce que le snapshot de réplication se rafraîchit toutes les
~20–30 s : le délai inclut ce cycle, pas seulement la décision du moteur. C'est un
résultat **structurel** du socle, pas un défaut du moteur ; il laisse une marge de 5 s
sous la garantie.

### 6.5 Faux positifs et intervalle de confiance

**0 faux positif sur 70 échantillons nominaux.** Il faut résister à la tentation d'en
déduire « taux de faux positifs = 0 % ». Avec 0 succès sur 70, l'intervalle de confiance
à 95 % de Wilson pour le taux de faux positifs est :

```
[0 % ; 5,2 %]
```

(Clopper-Pearson exact : borne supérieure ≈ 4,2 %.) Autrement dit : les données sont
**compatibles** avec un taux nul, mais aussi avec un taux allant jusqu'à ~5 %. La
garantie « 0 faux positif » doit donc être lue comme « aucun faux positif observé sur
70 échantillons », pas comme une preuve d'impossibilité.

De même, **4/4 cas corrects** a un intervalle de confiance large :

```
Wilson 95 % : [51 % ; 100 %]
Clopper-Pearson 95 % : [47,3 % ; 100 %]
```

La formulation « 100 % des cas **connus** » est donc la bonne : elle porte sur les
4 cas testés, pas sur tous les incidents futurs. Pour resserrer l'intervalle, il
faudrait répéter les essais et multiplier les cas — c'est une limite explicite (§8).

### 6.6 Deuxième jeu de validation : phase 5

La phase 5 apporte une validation **indépendante** par le croisement Kafka + PostgreSQL,
et une démonstration de non-confusion :

| Cas | Échantillons exploitables | Verdicts | Fermeture |
|---|---:|---|---|
| A-draine | 15 (15 `unreachable`) | 10 A, 3 `healthy`, 2 `⊥` | `db = 20 + 580`, `outcome = 600`, écarts 0, lag 0 |
| B-replication-croise | 8 (8 `unreachable`) | 1 B, 7 `healthy` | `db = 600`, `outcome = 600`, écarts 0, lag 0 |
| C-rattrapage | 16 (16 `unreachable`) | 3 C, 13 `healthy` | `db = 800`, `outcome = 800`, écarts 0, lag 0 |
| rejets-metier | 8 (9 `unreachable`) | 8 `healthy` | `db = 0 + 300`, `outcome = 300`, écarts 0, lag 0 |

Résultats : **4/4 PASS**. Points saillants :

- **backlog ≠ perte** : A-draine finit avec **600 Pix** en base (20 validés + 580
  rejetés TTL), soit exactement le total généré ; le backlog était transitoire ;
- **rejet ≠ incident** : `rejets-metier` produit 300 rejets et **aucun** verdict
  d'incident ; les 300 rejets sont bien en base ;
- **panne PostgreSQL ≠ perte** : C-rattrapage finit avec 800/800 en base après le
  retour de PostgreSQL ;
- **réplication** : B est confirmé par `tp-kafka.sh` en direct (brokers, URP,
  contrôleur), pas par un dashboard.

Les nombreux `unreachable` (15, 8, 16, 9) correspondent aux redémarrages de
service-health et aux arrêts volontaires de la plateforme pendant les essais. Ils ne
produisent **aucune conclusion** : c'est le garde-fou de fraîcheur qui joue son rôle.
En revanche, le nombre d'échantillons concluants par cas est faible (notamment 1 pour
B), donc la phase 5 est une confirmation **qualitative**, pas une deuxième estimation
statistique robuste.

---

## 7. Discussion : ce que les résultats prouvent

### 7.1 Ce qui est établi

Sur les cas testés :

- le moteur nomme la bonne **famille** pour les 4 incidents (A, B, C-dégradé,
  C-indisponible) et reste `healthy` sur les 5 témoins ;
- il **s'abstient** quand la donnée est insuffisante (3 échantillons, 7,5 %) et fournit
  une vérification suivante ;
- il **revient à `healthy`** après rétablissement, sans alarme collante ;
- il respecte le budget de **30 s** (max observé 25 s) ;
- il ne confond pas **backlog**, **rejet métier** et **perte** : la fermeture
  single-snapshot le vérifie mécaniquement (phase 5, 4/4) ;
- l'**impact métier** est vérifié à la ligne près : moteur **278** == sonde PG **278**
  == SQL **278**, delta contrôlé **0** == **0** (phase 7, 6/6).

### 7.2 Ce qui n'est pas établi

- **Généralisation** : les résultats portent sur une topologie, une machine, un modèle
  de trafic et 4 incidents scriptés. Rien ne prouve que le moteur serait aussi précis
  sur une panne réseau réelle, un disque lent, un broker qui se rééquilibre, etc.
- **Sous-famille** : l'évaluation est au niveau **famille**. Distinguer
  `reader_backlog` de `upstream_backlog_sla` à l'intérieur de A est utile, mais n'a pas
  été mesuré séparément avec un nombre suffisant de cas.
- **Taux de faux positifs** : 0/70 ne prouve pas 0 % ; l'IC monte à ~5 %.
- **Justesse universelle** : 4/4 a un IC bas à ~47–51 % ; « 100 % des cas connus » ne
  signifie pas « 100 % de tous les cas ».
- **Détection de perte** : aucune perte n'a pu être provoquée ; la branche « écart
  stable = perte » est codée mais jamais déclenchée. C'est un **résultat négatif**
  assumé, pas une preuve que la perte est impossible.
- **Détection adaptative** : non implémentée ; les seuils sont fixes.

### 7.3 Falsifiabilité

La garantie serait **falsifiée** par l'un des événements suivants :

1. un témoin nominal produisant un verdict d'incident (faux positif) ;
2. un incident injecté produisant une **famille erronée** avec confiance ;
3. une conclusion émise sur des sondes **périmées** ;
4. une conclusion **sans** vérification indépendante Kafka + PostgreSQL ;
5. un backlog présenté comme une perte, ou un rejet métier présenté comme un incident.

Les campagnes n'ont observé aucun de ces événements. C'est un argument de **réfutabilité
par essai** : le protocole est rejouable, et un groupe tiers peut tenter de produire un
contre-exemple. C'est exactement le critère de reproductibilité demandé (§8 du sujet).

---

## 8. Limites et menaces de validité

| Menace | Effet | Atténuation |
|---|---|---|
| **Petit échantillon** (4 incidents, 5 témoins) | intervalles de confiance larges ; pas de puissance statistique | assumé et quantifié (§6.5) ; protocole rejouable pour accumuler des cas |
| **Une seule machine / topologie** | généralisation externe limitée | documenté ; la plateforme est pédagogique, pas de production |
| **Incidents scriptés** | les pannes réelles peuvent être plus ambiguës | les cas couvrent les 3 familles + 2 modes PostgreSQL (dégradé, indisponible) |
| **Calibration sur la même plateforme** | risque de circularité (seuils choisis sur les données d'évaluation) | séparation large bruit/signal (1 vs 325 ; 3 vs 319) ; seuils réglables |
| **Redémarrage de service-health** | compteurs remis à zéro, deltas inter-cas invalides | fermeture **single-snapshot** ; détection de reset dans le moteur |
| **Système d'observation affecté** | une sonde peut mentir ou être aveuglée | garde-fou fraîcheur (75 s) + `probe_blinded` + sonde indépendante |
| **Fenêtre d'essai** | les échantillons `healthy` avant/après l'incident gonflent le dénominateur | analyse au niveau **cas** en priorité ; lecture temporelle des séquences (§6.3) |
| **Délai B** | 25 s laisse 5 s de marge sous 30 s | dépend du snapshot réplication du socle ; documenté |

---

## 9. Ce que la garantie ne couvre pas

- Elle ne promet pas **0 % de faux positifs** : elle promet **0 faux positif observé**
  sur 70 échantillons nominaux, avec une borne supérieure d'environ 5 %.
- Elle ne promet pas **100 % de justesse universelle** : elle promet **100 % des cas
  connus** (4/4), avec un intervalle large.
- Elle ne couvre pas les **pannes non testées** (réseau, disque, rééquilibrage,
  combinaisons de pannes).
- Elle ne couvre pas la **perte de données** : aucune perte n'a été provoquée ; le
  résultat négatif est documenté.
- Elle ne couvre pas l'**impact métier monétaire** : le bloc `business_impact` compte
  des dépassements de SLA en base, pas un montant en euros.
- Elle ne couvre pas une **vue HTTP/Grafana** : le produit livré est la CLI
  `diagnose.sh` ; l'extension est différée.
- Elle ne couvre pas les **seuils adaptatifs** : les seuils sont fixes et réglables.

---

## 10. Pistes de lecture (facultatif)

Pour approfondir :

- **Théorie de la détection du signal** : compromis seuil / sensibilité / spécificité,
  courbes ROC — éclaire le choix des seuils (§5).
- **Logique à trois valeurs / abstention** : le verdict `⊥` comme sortie explicite,
  plutôt qu'un faux choix binaire.
- **Théorie des files d'attente** : backlog, temps de séjour, loi de Little — pourquoi
  un lag n'est pas un âge, et pourquoi `estimated_oldest_lag_seconds` est un ratio.
- **Injection de fautes et validité expérimentale** : menaces internes/externes,
  répétition, randomisation — pour aller au-delà de 4 cas.
- **Axes LIMOS** : SIC (données, échanges, réseaux) et ODPS (simulation, performance)
  sont les plus proches du sujet ; voir
  [`../../TP/SAé/analyse-recherche-limos.md`](../../TP/SAé/analyse-recherche-limos.md).

---

## Références internes

- Garantie et candidates : [`../../TODO-SAE5.md`](../../TODO-SAE5.md) §4.
- Matrice symptôme / cause / vérification :
  [`../../travail/notes/matrice-symptomes-causes.md`](../../travail/notes/matrice-symptomes-causes.md).
- Note de conception (pipeline, causes, seuils, portée) :
  [`diagnostic-engine-design.md`](diagnostic-engine-design.md).
- Guide praticien : [`../operations/guide-diagnostic.md`](../operations/guide-diagnostic.md).
- Campagne phase 4 :
  [`../../preuves/phase4-campagne-2026-11-05/SYNTHESE.md`](../../preuves/phase4-campagne-2026-11-05/SYNTHESE.md).
- Croisé phase 5 :
  [`../../preuves/phase5-croisee-2026-11-05/SYNTHESE.md`](../../preuves/phase5-croisee-2026-11-05/SYNTHESE.md).
- Impact métier phase 7 :
  [`../../preuves/phase7-impact-metier-2026-11-05/SYNTHESE.md`](../../preuves/phase7-impact-metier-2026-11-05/SYNTHESE.md).
- Code : [`../../scripts/lib/diagnose_engine.py`](../../scripts/lib/diagnose_engine.py),
  [`../../scripts/diagnose.sh`](../../scripts/diagnose.sh).
