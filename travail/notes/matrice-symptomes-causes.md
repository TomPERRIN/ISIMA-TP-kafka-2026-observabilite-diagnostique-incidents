# Matrice symptôme / cause probable / vérification

Première version, construite à partir de l'état des lieux
([metriques-observabilite.md](metriques-observabilite.md)). C'est la spécification de notre
règle de diagnostic : chaque ligne dit comment passer d'un symptôme à une cause nommée,
avec une vérification indépendante.

## La matrice

| Symptôme vu en premier | Cause probable n°1 | Cause probable n°2 | Comment trancher | Si on ne peut pas trancher |
|---|---|---|---|---|
| La file d'attente grossit (`pending_result_count` en hausse, lag en hausse) | une application qui lit les topics est en retard | PostgreSQL ne suit plus le flux | `tp-kafka.sh` : si le lag est sur `persister-valid`/`persister-rejected` et que `persistence_gap_*` grossit -> persistance. Si le lag est sur `validator` ou `decision-engine` -> retard de lecture | indéterminé : vérifier les motifs de refus (`tp-db.sh`), un `processing_timeout` massif pointe l'amont |
| Des tranches de topics sont mal répliquées (URP > 0) | un broker est arrêté (grave) | la copie entre brokers est simplement en retard (bénin, passager) | `replication.brokers[]` : `offline_partitions_count` > 0 ou `active_controller_count` = 0 -> panne de broker. Sinon, si `broker_count` = 3 et les tranches se resynchronisent -> retard de copie | indéterminé : relancer la lecture 30 s plus tard, un retard de copie se résorbe seul, pas une panne |
| Plus aucune écriture en base (`db_*` ne bouge plus) | PostgreSQL est indisponible ou saturé | l'amont ne produit plus rien (le générateur s'est arrêté) | `seconds_since_last_validated` en hausse **et** `counts.generated` qui stagne -> c'est l'amont. `counts.generated` qui continue de monter sans écriture -> PostgreSQL | indéterminé : vérifier directement avec `tp-db.sh` et le statut `services.postgres` |
| Des paiements sont traités trop lentement (`validated_sla_breaches` en hausse) | un retard de lecture en amont (la file a grossi, les paiements attendent) | le délai accordé est trop court pour la charge | le lag vu par application (`tp-kafka.sh`) + la longueur de la file : si la file est longue au moment du dépassement -> retard en amont. Si la file est vide et qu'on dépasse quand même -> délai trop court | indéterminé : comparer le `decision_latency_ms` moyen en base avec le `decision_sla_seconds` du scénario |
| Le verdict global passe à `degraded` | un service ne répond plus (sonde) | une alerte grave s'est déclenchée | regarder `services.<nom>` : quel service est `down` ? Puis confirmer avec la sonde indépendante (`tp-kafka.sh` ou `tp-db.sh`) | indéterminé : la sonde peut être en échec à cause du réseau, vérifier le conteneur avec `docker ps` |

### Classement des causes : probabilité et criticité (v1)

Échelles utilisées — estimation **qualitative à valider par les essais** (Phase 4 : les
fréquences mesurées sur incidents provoqués affineront ces notes) :

| Probabilité (que cette cause soit la bonne) | Criticité (gravité si cette cause est la bonne) |
|---|---|
| **Élevée** : signal fréquent, vu en charge et même en nominal (rattrapages éclair, pics) | **Critique** : perte de données durable, réponses client fausses |
| **Moyenne** : arrive en charge soutenue ou après une action (rejeu, redémarrage) | **Élevée** : durabilité menacée mais récupérable, ou impact SLA massif |
| **Faible** : rare, nécessite un évènement exceptionnel (arrêt de conteneur, panne) | **Moyenne** : impact SLA limité, retards rattrapables |
| | **Faible** : pas d'impact métier, phénomène transitoire attendu |

Classement ligne par ligne (priorité d'examen = dans quel ordre vérifier les deux causes) :

| # | Symptôme | Cause | Probabilité | Criticité | Priorité d'examen | Justification |
|---|---|---|---|---|---|---|
| 1 | File d'attente qui grossit | retard de lecture applicatif | **Élevée** | Moyenne | **1re** | le rattrapage éclair est fréquent (lag passager documenté en nominal) ; criticité moyenne car rattrapable, mais devient réelle si le lag dure (TTL 10 s) |
| 1 | File d'attente qui grossit | PostgreSQL ne suit plus | Moyenne | **Élevée** | **2e** | plus rare (conteneur stable) mais durabilité menacée : à écarter vite car la vérification est rapide (`persistence_gap_*` + `tp-db.sh`) |
| 2 | URP > 0 | retard de copie entre brokers | **Élevée** | Faible | **2e** | survient brièvement à chaque rééquilibrage/redémarrage et se résorbe seul ; bénin si `offline = 0` et contrôleur actif |
| 2 | URP > 0 | broker arrêté | Faible | **Élevée** | **1re** | rare mais grave (RF=3, min ISR=2 : un 2e incident provoquerait l'indisponibilité) ; vérification immédiate (`offline_partitions_count`, `active_controller_count`) |
| 3 | Plus aucune écriture en base | amont arrêté (générateur fini) | **Élevée** | Faible | **1re** | en fin de scénario c'est le cas **normal** (`generated` stagne) ; aucun risque de perte |
| 3 | Plus aucune écriture en base | PostgreSQL indisponible/saturé | Moyenne | **Critique** | **2e** | perte durable potentielle ; à vérifier dès que `generated` continue de monter sans écriture (`services.postgres`, `tp-db.sh`) |
| 4 | Paiements trop lents (`sla_breaches` ↑) | retard de lecture en amont | **Élevée** | Moyenne | **1re** | conséquence directe et fréquente d'un backlog : les Pix dépassent le TTL de 10 s (impact client réel mais explicable) |
| 4 | Paiements trop lents (`sla_breaches` ↑) | délai trop court pour la charge | Faible | Moyenne | **2e** | problème de configuration du scénario, pas un incident de plateforme ; à soupçonner si la file est vide |
| 5 | Verdict `degraded` | service ne répond plus | Faible | **Élevée** (selon service) | **1re** | dépend du service : un persister down = criticité élevée, un shaper down = cosmétique ; vérification directe (`services.<nom>`) |
| 5 | Verdict `degraded` | sonde en échec (réseau, faux positif) | Moyenne | Faible | **2e** | la sonde peut être aveuglée par la perturbation réseau elle-même — croiser avec la sonde indépendante avant de conclure |

Lecture du classement : **on écarte d'abord la cause la plus critique** quand sa vérification
est peu coûteuse (c'est le cas ici : `tp-kafka.sh` / `tp-db.sh` sont des instantanés rapides),
puis on confirme la cause la plus probable. Exemple : sur un URP > 0, on vérifie la panne de
broker (critique, 5 s) avant de conclure au retard de copie (probable, bénin).

**Top 3 des combinaisons à surveiller en priorité** (criticité × facilité de vérification) :

1. **PostgreSQL ne suit plus / indisponible** (lignes 1 et 3) — seule cause **critique** du lot : c'est elle qui menace la durabilité, et c'est la 3e situation de la mission ;
2. **Broker arrêté** (ligne 2) — critique de near-miss : à min ISR=2, un second incident rend les écritures impossibles ;
3. **Retard de lecture qui dure** (ligne 1) — la plus fréquente : bénigne au départ, elle devient un impact SLA massif si elle n'est pas détectée dans le délai de 30 s.

> ⚠️ Ces probabilités sont des hypothèses de travail (Phase 3). La Phase 4 les transformera
> en fréquences **mesurées** : chaque essai provoqué confirme ou corrige la ligne concernée.

### Le témoin nominal (contrôle des faux positifs)

Avant de conclure à un incident, vérifier qu'un flux **sain** ne déclenche pas la règle.
Référence mesurée : `preuves/phase2-recensement/reference-nominale-2026-11-03/` (13/13 OK).

- pendant un scénario `nominal`, tout doit rester à jour : lag qui retombe à 0 partout
  (un `lag_total = 1` passager est **normal**, c'est un rattrapage éclair), écart de
  persistance = 0 en régime stabilisé, URP = 0, aucune écriture manquante ;
- si une alerte sonne pendant le nominal, c'est un **faux positif** : le seuil est trop
  sensible, à ajuster (par exemple `SIMULPIX_WARNING_LAG_TOTAL`) ;
- à noter : un léger écart de persistance est **normal en début de flux** (le temps
  d'écrire) ; c'est un écart qui grossit qui est suspect.

## Comment on utilisera la matrice pendant un incident

1. **On regarde le verdict** (`status` de la météo) et les alertes actives.
2. **On identifie le symptôme en premier** : file d'attente ? tranches mal répliquées ?
   écritures arrêtées ? paiements trop lents ? C'est la 1re colonne de la matrice.
3. **On suit la ligne** : les deux causes plausibles, puis la vérification qui tranche
   (toujours avec `tp-kafka.sh` ou `tp-db.sh`, jamais un dashboard seul).
4. **On nomme la cause** ou on conclut explicitement **« indéterminé »** avec la
   vérification suivante indiquée — jamais de silence.

## Ce qu'il reste à faire (lien avec la contribution)

Cette matrice est **manuelle**. La contribution attendue est de la rendre **automatique** :
une métrique dérivée, une alerte ou une vue qui applique ces règles toute seule, évaluée
sur des incidents provoqués (justesse du diagnostic, délai de détection, faux positifs).
Les essais correspondants se feront dans `essais/`.
