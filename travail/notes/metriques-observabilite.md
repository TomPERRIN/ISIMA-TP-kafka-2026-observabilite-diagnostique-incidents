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

### 2.0 Comment les dashboards sont alimentés

Les dashboards ne lisent pas les services directement. La chaîne est :

```
service-health (météo) -> collect-metrics.sh (toutes les 10 s) -> base InfluxDB -> dashboards Grafana
```

Le collecteur (`infra/observability/scripts/collect-metrics.sh`, intervalle réglable par
`SIMULPIX_OBS_INTERVAL_SECONDS`, 10 s par défaut) enregistre dans InfluxDB :

| mesure InfluxDB | Contenu |
|---|---|
| `simulpix_platform` | les compteurs du pipeline (émis, traités, acceptés, refusés, écrits en base, file d'attente, écarts, latences, hors-délai...) |
| `kafka_consumer_group` | le retard de chaque application, un point par groupe |
| `kafka_topic` | le nombre de messages par topic |
| `kafka_broker` | par broker : tranches mal répliquées, contrôleur actif, tranches hors ligne (lu via JMX sur les brokers) |
| `kafka_replication_partition` | le retard de copie, tranche de topic par tranche (via JMX) |
| `postgres_runtime` | lignes en base, connexions actives, taille, fraîcheur des écritures |
| `simulpix_semantics` | la configuration du moment (acks, retries, mode commit, mode transactionnel...) |

Conséquence pour nos essais : un incident peut aussi ralentir cette chaîne (service-health,
collecteur ou InfluxDB). Dans ce cas les courbes **se figent sans le dire** : à vérifier
pendant chaque incident.

### 2.1 Dashboard `simul-pix-incidents`

Le plus proche de notre sujet : rassemble les signaux d'incident.

| Panneau | Ce qu'il montre | Remarque |
|---|---|---|
| Partitions applicatives sous-répliquées (stat) | le maximum de tranches mal répliquées sur la période affichée | garde le pic : utile car la fin d'un incident revient souvent au normal |
| Lag de réplication par partition (courbe) | le retard de copie, tranche par tranche | signal principal d'une réplication dégradée |
| Partitions indisponibles par broker (courbe) | les tranches devenues inaccessibles | signal plus grave que « mal répliqué » |
| Contrôleur actif du cluster (courbe) | le nombre de contrôleurs Kafka (1 en temps normal) | 0 ou >1 = problème du cluster lui-même |
| Décisions hors SLA par minute (courbe) | les paiements traités trop lentement | l'impact métier visible |

### 2.2 Dashboard `simul-pix-kafka`

Côté cluster et consommateurs. Plusieurs panneaux existent en version « max 5 min »,
« max 30 min » et courbe temps réel — le max retient le pic même si l'incident est déjà fini.

| Panneau | Ce qu'il montre |
|---|---|
| Partitions sous-répliquées — max 5 min / 30 min / courbe | le pic de tranches mal répliquées |
| Lag consommateur — max 5 min / 30 min / courbe | le pic de retard des applications |
| Ancienneté backlog — max 5 min / 30 min / courbe | le pic d'ancienneté de la file d'attente |
| Retard par étapes générales (courbe) | le retard, application par application, sur une même courbe |
| Volume observé par topic (courbe) | le nombre de messages par topic dans le temps |
| Partitions offline, contrôleur actif (courbes) | l'état de santé du cluster |
| Acks actifs, retries actifs, commit SUB, auto commit, étapes en transaction (stats) | la configuration Kafka du moment (utile pour savoir dans quel mode l'essai a tourné) |
| Résultats finaux logiques vs offset technique (stats) | compare le résultat vu par l'application et le compteur brut du topic |

### 2.3 Dashboard `simul-pix-persistence`

Tout ce qui concerne PostgreSQL — le dashboard à regarder pour la 3e situation de la mission.

| Panneau | Ce qu'il montre | Remarque |
|---|---|---|
| Écarts Kafka => BDD (courbe + stats actuel / 5 min / 30 min) | les paiements traités mais pas encore écrits en base, pour les acceptés et les refusés | le signal « persistance qui ne suit plus » ; stats en max, donc garde le pic |
| Débit d'écriture PostgreSQL (courbe, lignes/s) | le rythme d'écriture en base | une écriture qui tombe à 0 pendant que le flux continue = alerte |
| Temps moyen des dernières écritures (courbe) | le délai entre l'événement Pix et son écriture en base | — |
| Décisions SLA / hors SLA (courbe) | les paiements dans les temps et hors délai | — |
| PostgreSQL — valides / rejetés (stats total / 5 min / 30 min) | le nombre de lignes en base, en cumul et par fenêtre | la version « par fenêtre » montre si ça bouge pendant l'essai |
| BDD — taille, taille des 2 tables (stats) | le volume occupé | secondaire pour notre sujet |

### 2.4 Dashboard `simul-pix-pipeline` (général)

La vue d'ensemble du flux, utile pour le témoin nominal.

| Panneau | Ce qu'il montre |
|---|---|
| Volumes cumulés du pipeline (courbe) | émis, traités, acceptés, refusés, écrits — superposés : les écarts entre courbes sautent aux yeux |
| Débits du pipeline (courbe, msg/s) | le rythme à chaque étape |
| Pix acceptés ou rejetés / s (courbe) | les décisions en temps réel |
| Temps moyen de validation d'un Pix sur 1 minute (courbe) | le délai de traitement |
| Pix rejetés pour dépassement du TTL / s (courbe) | les paiements trop lents |
| Longueur de la file d'attente (courbe) | les paiements en attente de décision |
| Stats émis / traités / acceptés / rejetés — 5 min, 30 min, période affichée | les volumes par fenêtre |

### 2.5 Ce qui manque, visible dès cet état des lieux

- Les dashboards **affichent les mêmes données que la météo**, mais sous forme de courbes et
  de max par fenêtre. Aucun ne croise les signaux : on peut voir « retard » sur un panneau et
  « écriture en retard » sur un autre sans qu'un lien soit fait entre les deux.
- Le dashboard `incidents` rassemble les bons signaux mais reste une **somme d'indicateurs** :
  il ne dit pas « c'est une réplication dégradée » ou « c'est la persistance », c'est au lecteur
  d'interpréter — exactement l'ambiguïté que notre contribution doit lever.

## 3. Scripts `tp-kafka.sh`

Script à lancer depuis la racine : `./scripts/tp-kafka.sh`. Il interroge Kafka en direct
(via le broker `kafka-1`), donc **sans passer par la météo ni Grafana** : c'est notre
vérification indépendante. Sortie en 4 parties :

### 3.1 Les topics applicatifs

Pour chaque topic, une ligne : nombre de tranches, facteur de réplication, rôle.

```
simulpix.transactions.raw        Pix émis par generator
simulpix.transactions.checked    Pix contrôlés par pix-validator
simulpix.transactions.decision   Décisions publiées par pix-decision-engine
simulpix.transactions.validated  Pix acceptés
simulpix.transactions.rejected   Pix rejetés / topic de rejet métier
simulpix.transactions.retry      Messages republiés pour rejeu
simulpix.transactions.outcome    Réponses finales client
```

`partitions=6` : jusqu'à 6 lecteurs du même groupe peuvent travailler en parallèle.
`replication=3` : chaque tranche est copiée sur les 3 brokers. Si `replication` passe
sous 3 dans la sortie, c'est un signal de dégradation du cluster.

### 3.2 Tous les topics visibles dans le cluster

Liste brute de tout ce que le cluster contient (y compris les topics internes).

### 3.3 Les 5 groupes de lecteurs (consumer groups)

Pour chaque application, un tableau par topic : position de lecture, fin du topic, retard.

| Groupe | Rôle |
|---|---|
| validator | lit raw et retry, publie checked |
| decision-engine | lit checked, publie validated / rejected / decision |
| outcome-publisher | lit decision, publie outcome |
| persister-valid | lit validated, persiste les Pix acceptés |
| persister-rejected | lit rejected, persiste les Pix rejetés |

Pour chaque ligne, la colonne « lecture » résume la situation :

- `à jour` : le groupe a tout lu (LAG=0) ;
- `retard à traiter` : LAG>0, des messages attendent d'être lus ;
- `offset non initialisé sur N partition(s)` : rien n'a encore été committé, souvent une
  partition vide ou pas encore utilisée — à ne pas confondre avec un retard.

Le détail brut est disponible avec `SIMULPIX_TP_KAFKA_VERBOSE=1 ./scripts/tp-kafka.sh`.

### 3.4 Ce que ce script apporte pour nos essais

- **Vérification indépendante** : il lit Kafka directement, donc permet de confirmer (ou
  d'infirmer) ce qu'annoncent la météo et les dashboards — exigence de la mission.
- **Vue par application** : contrairement au lag global des dashboards, on voit *quelle*
  application est en retard (validator ? persister-valid ?), ce qui discrimine déjà un
  retard de lecture d'un problème d'écriture en base.
- **Limites** : c'est un instantané à la main (pas d'historique) ; il ne montre pas le retard
  de réplication (il faut passer par JMX/les dashboards pour ça) ; il ne montre rien sur
  PostgreSQL — pour la 3e situation, il faut `tp-db.sh`.

## 4. Scripts `tp-db.sh`

Script à lancer depuis la racine : `./scripts/tp-db.sh`. Il interroge PostgreSQL en direct
(psql sur le conteneur), donc **sans passer par la météo ni Grafana** : c'est la vérification
indépendante pour la 3e situation de la mission. Sortie :

```sql
select count(*) as validated_count from validated_transactions;
select count(*) as rejected_count from rejected_transactions;
select rejection_reason, count(*) as occurrences
from rejected_transactions
group by rejection_reason
order by occurrences desc, rejection_reason asc;
```

Trois résultats :

1. le nombre total de paiements **acceptés** en base (table `validated_transactions`) ;
2. le nombre total de paiements **refusés** en base (table `rejected_transactions`) ;
3. la liste des **motifs de refus**, du plus fréquent au moins fréquent
   (par exemple `processing_timeout` quand un paiement a été traité trop lentement).

### 4.1 Ce qu'il y a dans les deux tables

- `validated_transactions` : un paiement accepté y est enregistré avec son `transaction_id`
  (identifiant unique), l'heure de l'événement, le délai accordé (`decision_sla_seconds`),
  le temps effectif de traitement (`decision_latency_ms`) et s'il a été traité dans les temps
  (`decision_within_sla`).
- `rejected_transactions` : un refus y est enregistré avec son motif (`rejection_reason`),
  le même suivi de délai, le message d'origine complet (`original_payload`) et une empreinte
  unique (`rejection_fingerprint`) qui empêche d'enregistrer deux fois le même refus.

### 4.2 Ce que ce script apporte pour nos essais

- **Vérification indépendante côté données** : les chiffres viennent de la base elle-même,
  pas des compteurs des services — c'est ce qui permet de confirmer qu'un paiement a
  réellement été enregistré (règle du sujet : ne jamais conclure depuis Kafka seul).
- **Les motifs de refus aident au diagnostic** : par exemple, un `processing_timeout` massif
  indique que des paiements ont dépassé leur délai (typique d'un retard de consommateur),
  alors que des refus métier (`invalid_*`) indiquent autre chose.
- **Limites** : les compteurs sont des **totaux depuis le tout début** — pour mesurer un essai,
  il faut noter la valeur avant de lancer le scénario, ou travailler en différence.
  Le script ne montre pas la fraîcheur des écritures (dernier enregistrement) ni les
  connexions : ces informations sont dans la météo (`timings.seconds_since_last_*`) et
  le dashboard persistence.

## Synthèse

### Ce qu'on a en résumé

| Source | Ce qu'elle apporte | Vérification indépendante ? |
|---|---|---|
| Météo `service-health` | l'état des services, les compteurs, les alertes, l'état réseau | non : elle lit Kafka et PostgreSQL elle-même |
| Dashboards Grafana | les mêmes mesures en courbes, avec historique et max par fenêtre | non : elles lisent la base InfluxDB alimentée par la météo |
| `tp-kafka.sh` | topics, réplication, retard par application | **oui** : interroge Kafka en direct |
| `tp-db.sh` | lignes en base, motifs de refus | **oui** : interroge PostgreSQL en direct |

### Les ambiguïtés observées entre signaux

- **Un retard consommateur et une dégradation de persistance se ressemblent** : dans les deux
  cas la file d'attente (`pending_result_count`, lag) grossit et le débit de sortie baisse.
  Les signaux qui les séparent existent bien — le retard vu par application (`tp-kafka.sh`,
  `consumer_groups`), l'écart de persistance (`persistence_gap_*`), les motifs de refus
  (`processing_timeout` dans `tp-db.sh`) — mais **rien ne les croise automatiquement**.
- **Une réplication dégradée ressemble à une panne de cluster** : des tranches mal répliquées
  (URP>0) peuvent venir d'un broker arrêté (grave) ou d'un simple retard de copie passager
  (bénin). Les signaux `offline_partitions` et `active_controller_count` permettent de
  trancher, mais ne sont affichés qu'à côté, sans règle de décision.
- **Les alertes se déclenchent indépendamment** : `kafka_lag`, `persistence_gap_valid`,
  `oldest_lag` peuvent sonner en même temps pour un seul et même incident, ou inversement
  une seule alerte sonner pour deux problèmes distincts. Aucune alerte ne dit « cause
  probable » ni « vérification suivante ».
- **Le verdict « indéterminé » n'existe pas** : quand les données ne permettent pas de
  trancher (par exemple générateur arrêté, donc plus de débit pour estimer l'ancienneté),
  le système n'a pas de réponse explicite — il reste simplement silencieux.

### Ce qui manque pour la mission (notre matière première)

Pour les trois situations à distinguer :

1. **Retard consommateur** : les signaux existent (lag par application, file d'attente,
   ancienneté), il manque une règle qui les combine et pointe l'application en faute.
2. **Réplication dégradée** : les signaux existent (URP, retard de copie, état des brokers),
   il manque une règle qui sépare « broker arrêté » de « copie en retard ».
3. **Persistance PostgreSQL** : les signaux existent (écarts de persistance, débit
   d'écriture, fraîcheur, `processing_timeout` en base), il manque une règle qui distingue
   « PostgreSQL ne suit plus » d'un simple retard consommateur en amont.

En plus : la **fraîcheur des sondes** n'est exposée nulle part alors que la mission demande
de la vérifier (le système d'observation peut être affecté par l'incident), et un témoin
nominal automatisé n'existe pas (les scénarios `nominal` se lancent à la main).

C'est exactement l'espace de notre contribution : **une règle ou un outil qui croise les
signaux existants pour donner un diagnostic nommé, avec sa vérification indépendante et son
cas « indéterminé » explicite.**
