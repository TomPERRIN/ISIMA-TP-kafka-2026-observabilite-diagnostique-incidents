# État des lieux — métriques d'observabilité existantes

Mission (étape 1 du sujet) : recenser les informations déjà visibles dans la météo
(`service-health`), les dashboards Grafana, `tp-kafka.sh` et `tp-db.sh`.

Pour chaque métrique : **source, signification, où la lire, fraîcheur, limites**.
Cet inventaire alimentera la matrice symptôme / cause / vérification pour la conception.

## 1. Météo — service `service-health`

<!-- À remplir : indicateurs exposés par l'API HTTP (voir docs/architecture/service-health-http.md),
     format des payloads, fraîcheur des sondes, ce que chaque indicateur couvre et ne couvre pas. -->

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
