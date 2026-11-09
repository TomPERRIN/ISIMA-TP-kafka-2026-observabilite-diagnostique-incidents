#!/usr/bin/env bash
# Démo observabilité (SAÉ 5) — UNE commande, UN terminal : la campagne d'incidents
# tourne automatiquement pendant que la frise du dashboard Grafana s'alimente.
#
# Usage :
#   ./scripts/demo-diagnostic.sh                            campagne complète (~35 min)
#   ./scripts/demo-diagnostic.sh --quick                    2 témoins au lieu de 5
#   ./scripts/demo-diagnostic.sh --cases D-traitement-lent  un cas seul (~3 min)
#   ./scripts/demo-diagnostic.sh --pause                    + Entrée entre les cas
#   (toutes les options de diagnostic-campaign.sh sont transmises telles quelles)
#
# Ce que fait le script :
#   1) démarre l'exporteur Grafana en arrière-plan (cache DÉDIÉ : la frise ne
#      se mélange pas aux 2 échantillons de la campagne — un cache partagé
#      entrelacerait les lectures, span < 10 s, et le moteur refuserait de
#      conclure pendant toute la campagne) ;
#   2) affiche l'URL du dashboard à ouvrir dans le navigateur ;
#   3) lance la campagne (démarrage de la plateforme inclus si besoin) ;
#   4) arrête l'exporteur proprement, même en cas d'interruption (trap EXIT).
#
# Variables : SIMULPIX_GRAFANA_URL (défaut http://localhost:3000),
#             SIMULPIX_EXPORT_INTERVAL (défaut 15 s, >= 10 s : span du moteur),
#             journal exporteur : runtime/diagnostic-export.log (gitignore).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
GRAFANA_URL="${SIMULPIX_GRAFANA_URL:-http://localhost:3000}"
DASHBOARD_URL="$GRAFANA_URL/d/simulpix-diagnostic"
EXPORT_INTERVAL="${SIMULPIX_EXPORT_INTERVAL:-15}"
EXPORT_PID=""

stop_exporter() {
  if [ -n "$EXPORT_PID" ] && kill -0 "$EXPORT_PID" 2>/dev/null; then
    kill "$EXPORT_PID" 2>/dev/null
    wait "$EXPORT_PID" 2>/dev/null || true
  fi
}
trap stop_exporter EXIT

mkdir -p "$REPO_DIR/runtime"
EXPORT_LOG="$REPO_DIR/runtime/diagnostic-export.log"

echo "[demo] exporteur Grafana en arrière-plan (toutes les ${EXPORT_INTERVAL}s, cache dédié)"
echo "[demo] journal exporteur : $EXPORT_LOG"
"$SCRIPT_DIR/diagnostic-export.sh" --watch "$EXPORT_INTERVAL" >"$EXPORT_LOG" 2>&1 &
EXPORT_PID=$!

echo "[demo] ouvrez le dashboard dans le navigateur : $DASHBOARD_URL"
echo "[demo] frise : vert=sain · violet=indéterminé · orange=A (file grossit) · jaune=B (réplication) · rouge=C (persistance) · bleu=D (traitement lent)"

"$SCRIPT_DIR/diagnostic-campaign.sh" "$@"
RC=$?

stop_exporter
echo "[demo] campagne terminée (code $RC) — preuves dans preuves/phase4-…, frise consultable dans Grafana"
exit "$RC"
