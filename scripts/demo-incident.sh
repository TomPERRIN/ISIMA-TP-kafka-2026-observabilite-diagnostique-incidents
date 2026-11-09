#!/usr/bin/env bash
# Démonstration d'un incident injecté (SAÉ 5) — UNE commande, UN terminal :
# l'incident tourne pendant que la frise du dashboard Grafana s'alimente.
#
# Usage :
#   ./scripts/demo-incident.sh [A|B|C]     (défaut : A)
#
#   A  retard consommateur  : la décision est ralentie, la file grossit    (orange)
#   B  réplication dégradée : un broker Kafka est en pause                 (jaune)
#   C  panne PostgreSQL     : la base est arrêtée au milieu du flux        (rouge)
#
# Ce que fait le script :
#   1) démarre l'exporteur Grafana en arrière-plan AVANT le scénario : il
#      réessaie tout seul pendant la recréation des conteneurs (« /health
#      injoignable — nouvel essai au prochain tour »), puis capture l'incident ;
#   2) affiche l'URL du dashboard et la couleur attendue ;
#   3) lance le scénario d'incident (recréation + injection + émission) ;
#   4) laisse tourner l'exporteur pour observer le retour au vert (le panneau
#      « Causes examinées » se vide) — Ctrl+C arrête proprement (trap EXIT).
#
# Variables : SIMULPIX_GRAFANA_URL (défaut http://localhost:3000),
#             SIMULPIX_EXPORT_INTERVAL (défaut 15 s, >= 10 s : span du moteur),
#             SIMULPIX_PG_OUTAGE_SECONDS (défaut 40 s, cas C),
#             journal exporteur : runtime/diagnostic-export-incident.log (gitignore).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
GRAFANA_URL="${SIMULPIX_GRAFANA_URL:-http://localhost:3000}"
DASHBOARD_URL="$GRAFANA_URL/d/simulpix-diagnostic"
EXPORT_INTERVAL="${SIMULPIX_EXPORT_INTERVAL:-15}"
PG_OUTAGE="${SIMULPIX_PG_OUTAGE_SECONDS:-40}"
CASE="${1:-A}"

case "$CASE" in
  A) COULEUR="orange — retard consommateur : la décision est ralentie à 350 ms, la file grossit" ;;
  B) COULEUR="jaune — réplication dégradée : le broker kafka-3 est en pause" ;;
  C) COULEUR="rouge — panne PostgreSQL : la base ne répond plus ${PG_OUTAGE}s" ;;
  *)
    echo "[demo] cas inconnu : '$CASE' — usage : ./scripts/demo-incident.sh [A|B|C]" >&2
    exit 2
    ;;
esac

EXPORT_PID=""
stop_exporter() {
  if [ -n "$EXPORT_PID" ] && kill -0 "$EXPORT_PID" 2>/dev/null; then
    kill "$EXPORT_PID" 2>/dev/null
    wait "$EXPORT_PID" 2>/dev/null || true
  fi
}
trap stop_exporter EXIT

mkdir -p "$REPO_DIR/runtime"
EXPORT_LOG="$REPO_DIR/runtime/diagnostic-export-incident.log"

echo "[demo] exporteur Grafana en arrière-plan (toutes les ${EXPORT_INTERVAL}s)"
echo "[demo] journal exporteur : $EXPORT_LOG"
"$SCRIPT_DIR/diagnostic-export.sh" --watch "$EXPORT_INTERVAL" >"$EXPORT_LOG" 2>&1 &
EXPORT_PID=$!

echo "[demo] ouvrez le dashboard dans le navigateur : $DASHBOARD_URL"
echo "[demo] incident attendu : $COULEUR"
echo "[demo] frise : vert=sain · violet=indéterminé · orange=A · jaune=B · rouge=C · bleu=D"
echo "[demo] pendant l'incident le panneau « Causes examinées » se remplit ; au retour au vert il se vide"

cd "$REPO_DIR"

case "$CASE" in
  A)
    echo "[demo] 1/2 scénario consumer_lag : recréation + décision ralentie + 600 messages"
    ./scripts/run-scenario.sh consumer_lag 600 20 | tail -1
    echo "[demo] 2/2 le backlog se vide (~3-4 min) : phase orange puis retour au vert"
    ;;
  B)
    echo "[demo] 1/2 scénario replication_lag : réplication dégradée sur kafka-3"
    SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE=20 \
    SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE=kafka-3 \
      ./scripts/run-scenario.sh replication_lag 600 20 | tail -1
    echo "[demo] 2/2 la réplication se resynchronise : retour au vert"
    ;;
  C)
    echo "[demo] 1/3 flux nominal : 800 messages à 15/s"
    ./scripts/run-scenario.sh nominal 800 15 | tail -1
    echo "[demo] 2/3 arrêt de PostgreSQL ${PG_OUTAGE}s au milieu du flux — regardez le dashboard"
    docker compose stop postgres >/dev/null 2>&1
    sleep "$PG_OUTAGE"
    docker compose start postgres >/dev/null 2>&1
    echo "[demo] 3/3 PostgreSQL relancé : rattrapage puis retour au vert"
    ;;
esac

echo "[demo] l'exporteur continue de tourner pour observer le retour au vert — Ctrl+C pour arrêter"
wait "$EXPORT_PID" 2>/dev/null || true
echo "[demo] terminé — journal : $EXPORT_LOG, dashboard : $DASHBOARD_URL"
