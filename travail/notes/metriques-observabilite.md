# État des lieux — métriques d'observabilité existantes

Recensement des informations déjà visibles dans la météo (`service-health`), les dashboards
Grafana, `tp-kafka.sh` et `tp-db.sh`, avant de construire quoi que ce soit.

Pour chaque métrique : **source, signification, où la lire, fraîcheur, limites**.
Cet inventaire alimentera la matrice symptôme / cause / vérification.

## 1. Météo — service `service-health`

Deux vues HTML servies par le même composant : météo générale sur `http://localhost:8082`,
vue atelier orientée pilotage sur `http://localhost:8083`. La page se rafraîchit
toutes les 10 s. Les données JSON viennent de :

- `GET /health` : la synthèse (`status`, `services`, `metrics`, `alerts`, `control`) ;
- `GET /health/details` : tout, plus les détails Kafka, PostgreSQL, l'historique, l'état réseau
  et les groupes de consommateurs. C'est la route à utiliser pour nos vérifications.

Pour bien lire : un paiement simulé voyage dans la plateforme dans cet ordre —

```
envoyé (raw) -> vérifié (checked) -> accepté (validated) ou refusé (rejected)
            -> décision publiée (decision) -> résultat publié (outcome) -> écrit en base (PostgreSQL)
```

### 1.1 Est-ce que les services sont vivants ? (sondes)

| Champ | Signification | Limite |
|---|---|---|
| `services.<nom>` | pour chaque service : `ok` (il répond), `down` (il ne répond pas), `degraded`, `error`, `not_probed`. La vérification est simple : « est-ce que le service répond sur son port ? » | une sonde qui répond prouve que le conteneur tourne, pas qu'il travaille vraiment. Un service peut répondre « ok » et pourtant ne plus rien traiter |
| `status` global | le verdict d'ensemble : `ok` (tout va bien), `warning` (au moins une alerte), `degraded` (un service est down) | dit juste qu'il y a un problème quelque part, pas lequel ni pourquoi |

### 1.2 Où en sont les paiements ? (compteurs)

| Champ | Signification | Limite |
|---|---|---|
| `counts.generated` | le nombre de paiements envoyés dans la simulation depuis le début | compte seulement ce qui part du générateur |
| `counts.processed` | le nombre de paiements passés dans le moteur de décision | — |
| `counts.validated` | le nombre de paiements **acceptés** par la vérification métier | — |
| `counts.rejected` | le nombre de paiements **refusés** par la vérification métier (deux compteurs séparés : accepted + refusés = traités) | — |
| `counts.persisted_valid` / `counts.persisted_rejected` | le nombre d'écritures que les deux services de sauvegarde ont effectuées (un pour les acceptés, un pour les refusés) | — |
| `counts.db_validated_count` | le nombre de paiements **acceptés** réellement présents dans la base PostgreSQL. La base contient deux tables distinctes : une pour les acceptés (`validated_transactions`), une pour les refusés (`rejected_transactions`) — ce champ compte la première | c'est un total depuis le tout début : pour comparer avec une exécution précise, il faut noter la valeur avant de lancer le scénario |
| `counts.db_rejected_count` | le nombre de paiements **refusés** réellement présents dans la base PostgreSQL (compte la deuxième table) | même remarque : total depuis le tout début |
| `counts.pending_result_count` | le nombre de paiements reçus mais qui n'ont **ni** été acceptés **ni** refusés : ils attendent leur tour. C'est ce qui permet de voir s'il y a une file d'attente qui s'allonge | la valeur repart à 0 en cas de rejeu ou de reset des topics |
| `counts.validated_sla_breaches` / `_rejected` | le nombre de paiements traités trop lentement (délai Pix dépassé) | compté côté PostgreSQL : si c'est lui qui ne suit plus, ce compteur ne bouge plus et ne révèle pas le problème |

À ne pas confondre : `persisted_*` est le compteur interne des services de sauvegarde, `db_*` est la réalité vérifiée en base.

### 1.3 Ce qui est déjà calculé pour nous (écarts et durées)

| Champ | Signification | Limite |
|---|---|---|
| `gaps.persistence_gap_valid` / `_rejected` | le nombre de paiements acceptés (ou refusés) mais **pas encore écrits en base**. Pratique : ça montre déjà « traité mais non sauvegardé » | un écart normal apparaît brièvement tout le temps (le temps d'écrire) ; c'est un écart qui grossit qui est suspect |
| `timings.estimated_oldest_lag_seconds` | l'ancienneté estimée du paiement le plus en retard dans la file d'attente : on divise la taille de la file par le débit d'arrivée des paiements | une estimation : elle n'est valable que si les paiements arrivent à un rythme régulier, et elle disparaît (valeur nulle) dès que le générateur s'arrête |
| `timings.avg_validation_latency_ms` / `avg_rejection_latency_ms` | le temps moyen mis pour sauvegarder un paiement sur les 5 dernières minutes | — |
| `timings.seconds_since_last_validated` / `_rejected` | depuis combien de temps aucun paiement n'a été écrit en base | — |

### 1.4 Côté Kafka : topics, retard, réplication

| Champ | Signification | Limite |
|---|---|---|
| `broker_count` | le nombre de serveurs Kafka (brokers) visibles depuis la météo | — |
| `topics.<topic>.end_offsets_total` | la fin du topic : le nombre total de messages écrits dedans | — |
| `topics.<topic>.partition_details` | pour chaque tranche (partition) du topic : quel broker la dirige et le nombre de messages | — |
| `consumer_groups.<groupe>.lag_total` | le **retard** de chaque application qui lit les topics : combien de messages ont été écrits mais pas encore lus par cette application. 5 groupes sont suivis : validator, decision-engine, outcome-publisher, persister-valid, persister-rejected | mesuré au moment précis de la sonde : la valeur peut être vielle de quelques secondes quand vous la lisez |
| `replication.under_replicated_partitions_total` | le nombre de tranches de topics qui ne sont plus copiées sur les 3 brokers comme prévu : c'est LE signal d'une réplication dégradée | ce chiffre vient d'un collecteur séparé (voir `captured_at`) : il peut être moins frais que le reste |
| `replication.partitions[]` | le détail du retard de copie, tranche par tranche : nombre de messages pas encore copiés et estimation du retard en secondes | l'estimation en secondes dépend du débit courant des messages |
| `replication.brokers[]` | pour chaque broker : nombre de tranches mal répliquées, contrôleur actif, tranches hors ligne | — |

### 1.5 Alertes déjà en place (`alerts[]`)

Chaque alerte a un `kind` (son type) et un `level` (`warning` ou `error`).

| Type d'alerte | Quand elle se déclenche | Réglable par variable d'environnement |
|---|---|---|
| `service_status` | un service ne répond pas | — |
| `kafka_lag` | une application a au moins 1 message de retard | `SIMULPIX_WARNING_LAG_TOTAL` |
| `kafka_replication` | au moins une tranche de topic mal répliquée | — |
| `reject_rate` | plus de 30 % de refus récemment (et au moins 10 paiements traités) | `SIMULPIX_WARNING_REJECT_RATE_PERCENT`, `SIMULPIX_WARNING_REJECT_MIN_PROCESSED` |
| `persistence_gap_valid` / `_rejected` | au moins 1 paiement traité mais pas écrit en base | `SIMULPIX_WARNING_PERSISTENCE_GAP` |
| `decision_sla` | des paiements traités trop lentement viennent d'être détectés | — |
| `oldest_lag` | le paiement le plus en retard attend depuis plus de 20 s | `SIMULPIX_WARNING_OLDEST_LAG_SECONDS` |
| `network_perturbation` | une perturbation réseau est volontairement active | — |

### 1.6 Ce qui manque, visible dès cet état des lieux

- Les alertes sont **indépendantes les unes des autres** : une alerte de retard (`kafka_lag`)
  et une alerte d'écriture manquante (`persistence_gap_valid`) peuvent apparaître en même temps
  sans que rien ne dise laquelle explique l'autre, ni s'il s'agit d'un seul et même incident.
- **La fraîcheur des mesures n'est pas indiquée** : quand on lit `lag_total`, rien ne dit si le
  chiffre vient d'être mesuré ou date de plusieurs secondes. Or l'incident lui-même peut
  ralentir le système qui observe (c'est un des points à vérifier dans la mission).
- L'estimation de l'ancienneté de la file d'attente repose sur le débit d'arrivée : si le
  générateur s'arrête, l'estimation disparaît alors que la file existe toujours.

## 2. Dashboards Grafana

<!-- À remplir : les 4 dashboards provisionnés dans infra/observability/grafana/dashboards/
     (simul-pix-incidents, simul-pix-kafka, simul-pix-persistence, simul-pix-pipeline) :
     panneaux principaux, métriques sous-jacentes, seuils et alertes existants. -->

## 3. Scripts `tp-kafka.sh`

<!-- À remplir : informations lisibles via scripts/tp-kafka.sh (offsets, lag, réplication, ...). -->

## 4. Scripts `tp-db.sh`

<!-- À remplir : informations lisibles via scripts/tp-db.sh (compteurs PostgreSQL, doublons,
     écritures, ...). -->

## Synthèse

<!-- À remplir : ce qui manque pour distinguer les trois incidents de la mission
     (retard consommateur / réplication dégradée / persistance PostgreSQL),
     ambiguïtés observées entre signaux. -->
