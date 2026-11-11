# Campagne d'incidents provoqués — synthèse

| Critère | Résultat |
|---|---|
| #1 justesse (4 familles) | 0/1 |
| Couverture familles (problèmes étudiés) | 0/4 |
| #4 faux positifs (témoin) | 0 (sur 0 témoins, 0 échantillons) |
| #3 délai max | None |

| Cas | Échant. | Verdicts | Cause nommée | Délai | PASS |
|---|---|---|---|---|---|
| D-traitement-lent | 7 | consumer_backlog×6, healthy×1 | — | — | ❌ |


## Couverture des problèmes étudiés

| Famille | Cas qui l'attendent | Statut |
|---|---|---|
| `consumer_backlog` | aucun cas attendu | ❌ |
| `slow_processing` | D-traitement-lent (famille absente des verdicts) | ❌ |
| `kafka_replication` | aucun cas attendu | ❌ |
| `persistence` | aucun cas attendu | ❌ |

- témoin nominal (0 faux positif) : ❌ (aucun témoin exploitable)
- verdicts indéterminés explicites : — (aucun)

---

## Cas D-traitement-lent — angle mort documenté : verdict `slow_processing` à file vide

**Ce que cette campagne prouve.** La perturbation a été réellement appliquée :
tc netem `delay 850ms` vérifié dans le conteneur pix-decision-engine (event
`fault_verified`), et le moteur a produit des verdicts réels : la file du
decision-engine croît linéairement (lag 5 → 65, ≈ 0,66 msg/s) et le verdict
bascule de `healthy` à `consumer_backlog` (cause nommée `reader_backlog`) dès
que le lag franchit le seuil 10, soit +14 s après l'injection.

**Ce que cette campagne ne peut pas prouver en live.** Le verdict
`slow_processing` (traitement long à FILE VIDE, cause `sla_config`) est
structurellement hors de portée de la plateforme. Le calibrage du 11/11
(6 essais) établit quatre contraintes :

| # | Contrainte structurelle | Mesure |
|---|---|---|
| 1 | Débit minimum de l'API = 1 msg/s (`sanitize_positive_int` min 1 ; `rate=0` déclenche un burst de 500 messages, `traffic_model.py` l.137) | impossible de réduire le flux sous 1 msg/s |
| 2 | Boucle du decision-engine : 1 message/cycle (poll 1.0 s + produce synchrone `flush(10.0)` + commit `asynchronous=False`) | période réelle ≈ 3 s/msg → à D=850 ms, le lag croît ≈ 0,66 msg/s |
| 3 | Métrique déclencheuse `avg_validation_latency_ms` : moyenne SQL sur 5 min (`validated_at − event_time`, `probes.py`) | 20-25 s de traitement lent pour dépasser le seuil SLA×80 % = 2,4 s |
| 4 | Sonde `/health` du service : timeout 1.0 s (`probes.py` l.83) | un tc ≥ 1 s rend le service « down » → `probe_blinded` → indéterminé |

La course est perdue d'avance : le lag franchit son seuil (10) en ~15 s quand
la moyenne SQL ne dépasse le sien qu'après 20-25 s. Avec l'ordre du moteur
(persister > reader > sla), `reader_backlog` l'emporte toujours. De plus,
`breaches_rising` seul + file vide conduit à `ambiguous_sla` (indéterminé) :
seul `latency_high` peut conclure `slow_processing` (contrainte 3), et forcer
D ≥ 1 s aveugle la sonde (contrainte 4).

**Ce n'est pas un défaut du moteur.** La matrice v2
(`travail/notes/matrice-symptomes-causes.md`) l'énonce : « un traitement long
engendre une attente grossissante, mais la réciproque est fausse » — quand la
file grossit, conclure `slow_processing` serait une erreur logique ; le moteur
la nomme `reader_backlog` et c'est le verdict correct du point de vue des
données observées. Le tranchage file vide/latence haute (`sla_config`) reste
couvert par les tests unitaires du moteur (37 tests, dont 4 dédiés à
slow_processing). Couvrir ce verdict en live exigerait un levier absent de
l'API de pilotage (ex. `processing_delay_ms_override` utilisé par les
expériences R4/R6, qui incluait une file de 1122 messages).

**Déroulé réel** (events.jsonl) : flux 100×1 SLA UI=3 s → pause du generator à
generated=7 (fenêtre SQL quasi vide) → conteneurs stables → tc netem
delay=850 ms jitter=40 ms vérifié → restart generator + pix-traffic-shaper (la
pause du generator fait crasher le shaper : DNS `generator` non résolu pendant
l'arrêt) → sondes toutes ok avant le relevé → relevé 90 s (fenêtre 11 s) →
reset. Verdicts observés : healthy (lag 5) puis consumer_backlog/reader_backlog
×6 (lag 15 → 65) ; aucun indeterminate.

**Runs préliminaires de calibrage** (relevés écrasés par celui-ci, scripts de
calibrage sous /tmp) : (1) run du matin — injection tombée pendant la
recréation des conteneurs par le reset du `run_scenario` → tc jamais appliqué
(« No such container »), campagne = nominal déguisé (healthy×7) ; (2) 1er run
du soir — crash du shaper pendant la pause du generator laissé sans relance →
sondes aveuglées → verdicts indeterminate×5 (probe_blinded). Ces deux échecs
ont motivé le déroulé final : pause → stabilité → tc vérifié → restart
generator+shaper → sondes ok avant relevé.
