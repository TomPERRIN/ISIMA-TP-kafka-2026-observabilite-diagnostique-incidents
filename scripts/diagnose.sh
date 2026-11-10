#!/usr/bin/env bash
# Diagnostic Simul-Pix (SAÉ 5) : lit /health/details, tient le cache 2 échantillons
# et exécute le moteur qui code la matrice symptômes/causes v1.
#
# Usage :
#   ./scripts/diagnose.sh              diagnostic ponctuel (cache partagé)
#   ./scripts/diagnose.sh --watch 15   boucle toutes les 15 s (2 lectures = deltas)
#   ./scripts/diagnose.sh --json       sortie JSON brute
#   ./scripts/diagnose.sh --single     sans état (règles à 2 échantillons -> indéterminé)
#   ./scripts/diagnose.sh --reset      vide le cache d'état
#
# Variables : SIMULPIX_HEALTH_URL (défaut http://localhost:8082/health/details),
#             SIMULPIX_DIAG_CACHE (défaut runtime/diagnostic-cache.json),
#             seuils moteur : SIMULPIX_DIAG_MAX_PROBE_AGE_SECONDS (75),
#             SIMULPIX_DIAG_LAG_TOTAL_THRESHOLD (10), SIMULPIX_DIAG_GAP_STEP (5)...
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
HEALTH_URL="${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}"
CACHE="${SIMULPIX_DIAG_CACHE:-$REPO_DIR/runtime/diagnostic-cache.json}"
CLI="$SCRIPT_DIR/lib/diagnose_cli.py"

JSON=0
WATCH=0
INTERVAL=15
SINGLE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --json) JSON=1 ;;
    --watch) WATCH=1; INTERVAL="${2:-15}"; shift ;;
    --single) SINGLE=1 ;;
    --reset) mkdir -p "$(dirname "$CACHE")"; rm -f "$CACHE"; echo "Cache réinitialisé : $CACHE"; exit 0 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Option inconnue : $1 (voir --help)" >&2; exit 1 ;;
  esac
  shift
done

mkdir -p "$(dirname "$CACHE")"
SAMPLE_FILE="$REPO_DIR/runtime/.diagnose-sample.json"
trap 'rm -f "$SAMPLE_FILE"' EXIT

fetch_sample() {
  if ! curl -fsS "$HEALTH_URL" -o "$SAMPLE_FILE" 2>/dev/null; then
    echo "diagnose: /health/details injoignable ($HEALTH_URL) — plateforme éteinte ?" >&2
    echo "  relancer : ./start.sh  et attendre que service-health réponde" >&2
    exit 2
  fi
}

run_once() {
  fetch_sample
  local flags=()
  [ "$JSON" = 1 ] && flags+=(--json)
  [ "$SINGLE" = 1 ] && flags+=(--single)
  python3 "$CLI" --sample "$SAMPLE_FILE" --cache "$CACHE" "${flags[@]+"${flags[@]}"}"
}

if [ "$WATCH" = 1 ]; then
  # Ctrl+C pour sortir ; 2 lectures espacées (>= 10 s) sont nécessaires pour
  # conclure sur les compteurs qui montent (garde anti « lag éclair »).
  while true; do
    run_once
    echo "(prochaine lecture dans ${INTERVAL}s — Ctrl+C pour arrêter)"
    sleep "$INTERVAL"
  done
else
  run_once
fi
