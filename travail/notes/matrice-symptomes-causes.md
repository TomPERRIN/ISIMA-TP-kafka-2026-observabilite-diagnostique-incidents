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
