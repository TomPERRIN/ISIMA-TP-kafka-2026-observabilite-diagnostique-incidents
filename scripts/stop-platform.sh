#!/bin/sh
set -eu

# Arrêt SIMPLE de la plateforme : `docker compose stop` (les conteneurs et les
# volumes sont conservés, données incluses). Ne pas confondre avec ./stop.sh
# (racine) qui PURGE : conteneurs + volumes + toutes les données persistées.

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

echo "[simulpix] arrêt général de la plateforme"
docker_compose stop
echo "[simulpix] plateforme arrêtée"
