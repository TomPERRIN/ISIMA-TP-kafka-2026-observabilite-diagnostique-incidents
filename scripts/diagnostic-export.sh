#!/usr/bin/env bash
# Export Grafana du diagnostic Simul-Pix (SAÉ 5) : pousse les verdicts du moteur
# dans InfluxDB (métrique simulpix_diagnostic) pour le dashboard provisionné
# « Simul-Pix - Diagnostic (SAÉ 5) ».
#
# Usage :
#   ./scripts/diagnostic-export.sh              un verdict -> InfluxDB (une fois)
#   ./scripts/diagnostic-export.sh --watch 15   boucle toutes les 15 s (Ctrl+C pour arrêter)
#   ./scripts/diagnostic-export.sh --print      affiche la ligne au lieu de l'envoyer
#
# Variables : SIMULPIX_HEALTH_URL (défaut http://localhost:8082/health/details),
#             SIMULPIX_DIAG_CACHE (défaut runtime/diagnostic-cache.json, partagé
#             avec diagnose.sh — les règles à 2 échantillons restent valables),
#             SIMULPIX_INFLUXDB_URL / _ORG / _BUCKET / _TOKEN (idem socle).
#
# Le moteur et le socle ne sont PAS modifiés : l'export est un adaptateur de
# sortie (extension « vue », note de conception §9). La CLI (diagnose.sh) reste
# la source de vérité : le dashboard affiche, la preuve reste tp-kafka.sh/tp-db.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
HEALTH_URL="${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}"
CACHE="${SIMULPIX_DIAG_CACHE:-$REPO_DIR/runtime/diagnostic-cache.json}"
CLI="$SCRIPT_DIR/lib/diagnose_cli.py"
EXPORT="$SCRIPT_DIR/lib/diagnostic_export.py"

WATCH=0
INTERVAL=15
PRINT=0

while [ $# -gt 0 ]; do
  case "$1" in
    --watch) WATCH=1; INTERVAL="${2:-15}"; shift ;;
    --print) PRINT=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Option inconnue : $1 (voir --help)" >&2; exit 1 ;;
  esac
  shift
done

mkdir -p "$(dirname "$CACHE")"
SAMPLE_FILE="$REPO_DIR/runtime/.diagnose-sample.json"
VERDICT_FILE="$REPO_DIR/runtime/.diagnose-verdict.json"
trap 'rm -f "$SAMPLE_FILE" "$VERDICT_FILE"' EXIT

run_once() {
  if ! curl -fsS --max-time 10 "$HEALTH_URL" -o "$SAMPLE_FILE" 2>/dev/null; then
    if [ "$WATCH" = 1 ]; then
      # En mode watch, un hic passager (service-health qui redémarre, charge
      # NFS) ne doit pas tuer la boucle : on réessaie au prochain tour.
      echo "diagnostic-export: /health/details injoignable ($HEALTH_URL) — nouvel essai au prochain tour" >&2
      return 0
    fi
    echo "diagnostic-export: /health/details injoignable ($HEALTH_URL) — plateforme éteinte ?" >&2
    echo "  relancer : ./start.sh  et attendre que service-health réponde" >&2
    exit 2
  fi
  # Le verdict du moteur est produit en JSON (moteur inchangé), puis converti
  # en ligne InfluxDB par l'adaptateur de sortie.
  if ! python3 "$CLI" --sample "$SAMPLE_FILE" --cache "$CACHE" --json > "$VERDICT_FILE"; then
    if [ "$WATCH" = 1 ]; then
      echo "diagnostic-export: verdict illisible ce tour-ci — nouvel essai au prochain tour" >&2
      return 0
    fi
    return 2
  fi
  local flags=()
  [ "$PRINT" = 1 ] && flags+=(--print)
  if [ "$WATCH" = 1 ]; then
    python3 "$EXPORT" --verdict "$VERDICT_FILE" "${flags[@]+"${flags[@]}"}" \
      || echo "diagnostic-export: export InfluxDB échoué ce tour-ci — nouvel essai au prochain tour" >&2
  else
    python3 "$EXPORT" --verdict "$VERDICT_FILE" "${flags[@]+"${flags[@]}"}"
  fi
}

if [ "$WATCH" = 1 ]; then
  # Ctrl+C pour sortir ; cadence conseillée 15 s (≥ 10 s : les règles à delta
  # du moteur ont besoin de 2 échantillons espacés pour conclure).
  # Tolérant aux hics passagers (service-health injoignable, écriture InfluxDB
  # échouée) : la boucle continue et réessaie au prochain tour.
  while true; do
    run_once
    echo "(prochain verdict dans ${INTERVAL}s — Ctrl+C pour arrêter)"
    sleep "$INTERVAL"
  done
else
  run_once
fi
