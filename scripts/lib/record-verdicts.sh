#!/usr/bin/env bash
#
# record-verdicts.sh — boucle d'enregistrement des verdicts du moteur de diagnostic.
#
# Utilisée par scripts/diagnostic-campaign.sh (phase 4) et scripts/cross-check.sh
# (phase 5) : toutes les <cadence> s, lit /health/details, exécute le moteur
# (diagnose_cli.py) et ajoute une ligne compacte dans <dossier>/verdicts.jsonl :
#   {"t_epoch": ..., "verdict": {...}}
# Si le service est injoignable : {"t_epoch": ..., "error": "unreachable"}.
#
# Usage : record-verdicts.sh <dossier> <durée_s> [cadence_s (défaut 11)]
# Variables : SIMULPIX_HEALTH_URL (défaut http://localhost:8082/health/details)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"   # scripts/lib -> racine du dépôt

DIR="${1:?usage : record-verdicts.sh <dossier> <durée_s> [cadence_s]}"
WINDOW="${2:?durée en secondes requise}"
INTERVAL="${3:-11}"   # >= 10 s : span minimal entre les 2 échantillons du moteur
HEALTH_URL="${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}"

mkdir -p "$DIR"
deadline=$(( $(date +%s) + WINDOW ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  ts="$(date +%s)"
  sample="$REPO_DIR/runtime/.diagnose-sample.json"
  if curl -fsS "$HEALTH_URL" -o "$sample" 2>/dev/null; then
    if out="$(python3 "$SCRIPT_DIR/diagnose_cli.py" --sample "$sample" \
                     --cache "$REPO_DIR/runtime/diagnostic-cache.json" --json 2>/dev/null)"; then
      T_EPOCH="$ts" python3 "$SCRIPT_DIR/compact_verdict.py" <<<"$out" >>"$DIR/verdicts.jsonl"
    else
      printf '{"t_epoch":%s,"verdict_error":"cli_failed"}\n' "$ts" >>"$DIR/verdicts.jsonl"
    fi
  else
    printf '{"t_epoch":%s,"error":"unreachable"}\n' "$ts" >>"$DIR/verdicts.jsonl"
  fi
  sleep "$INTERVAL"
done
