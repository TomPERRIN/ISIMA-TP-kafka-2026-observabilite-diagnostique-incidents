# SAÉ 5 — Diagnostic d'incidents à partir des signaux du pipeline

Sujet : [`TP/SAé/sujet-05-observabilite-incidents.md`](../TP/SAé/sujet-05-observabilite-incidents.md)
Attendus et évaluation : [`TP/SAé/README.md`](../TP/SAé/README.md)

## Notre but

Un pipeline peut sembler en bonne santé alors qu'il ne l'est pas : les conteneurs tournent,
mais un consommateur prend du retard, la réplication Kafka se dégrade ou PostgreSQL n'écrit
plus. Vu d'un dashboard, ces trois incidents se ressemblent.

Sujet : construire un diagnostic qui, à partir des signaux déjà présents sur la
plateforme, identifie **quel incident est en cours**, indique la vérification qui permet de
le confirmer — et répond clairement « indéterminé » quand les métriques ne suffisent pas à
trancher.

## Organisation

- [`avancement.md`](avancement.md) — ce qui est fait : résultats, décisions, pièges connus.
- [`TODO.md`](TODO.md) — ce qu'il reste à faire : pilotage fusionné du suivi global et de l'inventaire métriques.
- [`notes/`](notes/) — tout ce qu'on trouve : métriques existantes, docs utiles, idées, trucs
  intéressants.
- [`essais/`](essais/) — tout ce qu'on teste : comment on a provoqué un incident, ce que ça a
  donné, captures d'écran et résultats.


## Avancement

- [x] Choix du sujet et mise en place du répertoire de travail.
- [x] État des lieux des métriques d'observabilité existantes ([`notes/metriques-observabilite.md`](notes/metriques-observabilite.md)).
- [x] Matrice symptôme / cause / vérification ([`notes/matrice-symptomes-causes.md`](notes/matrice-symptomes-causes.md), v1 manuelle).
- [ ] Automatisation de la matrice (métrique dérivée, alerte, vue ou commande de diagnostic).
- [ ] Incidents provoqués et mesurés.
- [ ] Rédaction des livrables finaux (note de conception, protocole d'essai, guide de diagnostic).
