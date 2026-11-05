#!/usr/bin/env bash
#
# cross-check.sh — phase 5 : vérification croisée + non-confusion (SAÉ 5)
#
# Démontre, en croisant chaque conclusion du diagnostic avec les sources
# indépendantes (tp-kafka.sh : Kafka en direct ; tp-db.sh : PostgreSQL en direct)
# et les compteurs du payload, que le diagnostic NE CONFOND PAS trois réalités :
#
#   1) A-draine : un retard de lecture (consumer_backlog) qui SE RESORBE.
#      Après drainage : lag ~0, convergence compteurs == PostgreSQL, écart de
#      persistance 0 — le backlog était transitoire, AUCUNE donnée perdue.
#   2) B-replication-croise : URP / retard de copie vérifié contre l'état Kafka
#      en direct (brokers, offline, contrôleur) — pas contre un dashboard.
#   3) C-rattrapage : PostgreSQL indisponible pendant le flux, puis rattrapage ;
#      convergence finale compteurs == PG (ou, si l'outil du socle committe sans
#      écrire, écart STABLE = perte — la réalité est mesurée, pas supposée).
#   4) rejets-metier : le scénario errors_simple produit des rejets métier ;
#      le diagnostic ne sonne AUCUN incident (rejet métier ≠ incident) et les
#      compteurs `rejected` sont cohérents avec les tables PG.
#
# Pour chaque cas : relevés du moteur (record-verdicts.sh), captures croisées
# horodatées (tp-kafka-<label>.txt, tp-db-<label>.txt, health-<label>.json,
# counts-<label>.json), puis rapport scripts/lib/crosscheck_report.py.
#
# Usage :
#   ./scripts/cross-check.sh                les 4 démonstrations
#   ./scripts/cross-check.sh --cases A-draine,rejets-metier   sous-ensemble
#   ./scripts/cross-check.sh --skip-start   plateforme déjà up
#
# Variables : SIMULPIX_CROSSCHECK_DIR (défaut preuves/phase5-croisee-<date>),
#             REC_INTERVAL (11 s), A_WINDOW (120), C_WINDOW (150),
#             WA_DRAIN_MAX (420), C_DRAIN_MAX (600), C_OUTAGE_S (40),
#             C_INJECT_DELAY_S (8), CONV_POLL_S (10).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

OUT_DIR="${SIMULPIX_CROSSCHECK_DIR:-$REPO_DIR/preuves/phase5-croisee-$(date +%Y-%m-%d)}"
REC_INTERVAL="${REC_INTERVAL:-11}"
REC_WINDOW="${REC_WINDOW:-90}"
A_WINDOW="${A_WINDOW:-120}"
C_WINDOW="${C_WINDOW:-150}"
TAIL_WINDOW="${TAIL_WINDOW:-40}"
WA_DRAIN_MAX="${WA_DRAIN_MAX:-420}"
C_DRAIN_MAX="${C_DRAIN_MAX:-600}"
C_OUTAGE_S="${C_OUTAGE_S:-40}"
C_INJECT_DELAY_S="${C_INJECT_DELAY_S:-8}"
CONV_POLL_S="${CONV_POLL_S:-10}"
SKIP_START=0
CASES_PLAN="A-draine,B-replication-croise,C-rattrapage,rejets-metier"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

health_ok() {
  curl -fsS "${SIMULPIX_HEALTH_URL_BASE:-http://127.0.0.1:8082}/health" >/dev/null 2>&1
}

wait_health() {
  local attempts="${1:-90}"
  while [ "$attempts" -gt 0 ]; do
    if health_ok; then return 0; fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

log_event() {
  # log_event <cas> <evenement> [detail]
  local name="$1" event="$2" detail="${3:-}"
  printf '{"t_epoch":%s,"event":"%s","detail":"%s"}\n' \
    "$(date +%s)" "$event" "${detail//\"/\\\"}" >> "$OUT_DIR/$name/events.jsonl"
}

snapshot_counts() {
  curl -fsS "${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}" 2>/dev/null \
    | python3 -c '
import json, sys
p = json.load(sys.stdin)
c = (p.get("metrics") or {}).get("counts") or {}
g = (p.get("metrics") or {}).get("gaps") or {}
out = {}
for k in ("generated","validated","rejected","outcome_count",
          "raw_topic_end_offsets","validated_topic_end_offsets",
          "rejected_topic_end_offsets","outcome_topic_end_offsets",
          "db_validated_count","db_rejected_count"):
    out[k] = c.get(k, 0)
for k in ("persistence_gap_valid","persistence_gap_rejected"):
    out[k] = g.get(k, 0)
lag = 0
for k in ("pix_validator_lag_total","pix_decision_engine_lag_total",
          "pix_outcome_publisher_lag_total","persister_valid_lag_total",
          "persister_rejected_lag_total"):
    lag = max(lag, c.get(k) or 0)
out["lag_max"] = lag
out["payload_timestamp"] = p.get("timestamp")
print(json.dumps(out, ensure_ascii=False, separators=(",",":")))
'
}

capture() {
  # capture <cas> <label> : tp-kafka.sh + tp-db.sh + payload -> fichiers
  local name="$1" label="$2" dir="$OUT_DIR/$name"
  echo "        [capture] $label"
  { echo "### $label — $(date -u +%FT%TZ)"; ./scripts/tp-kafka.sh; } > "$dir/tp-kafka-$label.txt"
  { echo "### $label — $(date -u +%FT%TZ)"; ./scripts/tp-db.sh; } > "$dir/tp-db-$label.txt"
  curl -fsS "${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}" \
    > "$dir/health-$label.json" 2>/dev/null || true
  snapshot_counts > "$dir/counts-$label.json" 2>/dev/null || true
}

wait_convergence() {
  # attend la fermeture complète de la chaîne (compteurs == base, écarts 0, lag ~0)
  local name="$1" max_s="$2"
  local check_file="$OUT_DIR/$name/counts-check.json"
  local conv_file="$OUT_DIR/$name/convergence-latest.json"
  local waited=0
  sleep 5
  while [ "$waited" -lt "$max_s" ]; do
    if snapshot_counts > "$check_file" 2>/dev/null; then
      python3 "$SCRIPT_DIR/lib/convergence_check.py" "$check_file" > "$conv_file" 2>/dev/null || true
      if grep -q '"converged":true' "$conv_file" 2>/dev/null; then
        log_event "$name" "convergence_reached" "waited=${waited}s"
        snapshot_counts > "$OUT_DIR/$name/counts-final.json" 2>/dev/null || true
        return 0
      fi
    fi
    waited=$((waited + CONV_POLL_S))
    sleep "$CONV_POLL_S"
  done
  snapshot_counts > "$OUT_DIR/$name/counts-final.json" 2>/dev/null || true
  log_event "$name" "convergence_timeout" "waited=${max_s}s (non convergé)"
  return 1
}

pg_outage_background() {
  local name="$1"
  (
    sleep "$C_INJECT_DELAY_S"
    log_event "$name" "fault_injected" "docker compose stop postgres (${C_OUTAGE_S}s)"
    docker_compose stop postgres >/dev/null
    sleep "$C_OUTAGE_S"
    log_event "$name" "fault_recovered" "docker compose start postgres"
    docker_compose start postgres >/dev/null
    wait_health 60 && log_event "$name" "platform_ok_after_recovery" ""
  ) &
}

run_case() {
  # run_case <nom> [commande du scénario]
  local name="$1"
  shift
  local dir="$OUT_DIR/$name"
  mkdir -p "$dir"
  : > "$dir/events.jsonl"
  echo "[cross-check] ============ cas $name ============"
  log_event "$name" "case_start" "$*"
  rm -f "$REPO_DIR/runtime/diagnostic-cache.json"
  if ! health_ok; then
    wait_health 60 || { echo "[cross-check] plateforme KO" >&2; return 1; }
  fi
  capture "$name" t0
  log_event "$name" "baseline_captured" "t0"

  if ! "$@" >"$dir/scenario.log" 2>&1; then
    log_event "$name" "scenario_failed" "voir scenario.log"
    echo "[cross-check] ! scénario en échec ($name)" >&2
    return 1
  fi
  log_event "$name" "scenario_ready" ""
  sleep 3

  local rec_pid inj_pid
  case "$name" in
    A-draine)
      bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$A_WINDOW" "$REC_INTERVAL" &
      rec_pid=$!
      wait "$rec_pid" || true
      capture "$name" "pendant-backlog"
      if wait_convergence "$name" "$WA_DRAIN_MAX"; then
        bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$TAIL_WINDOW" "$REC_INTERVAL" || true
      fi
      capture "$name" final
      ;;
    C-rattrapage)
      pg_outage_background "$name"
      inj_pid=$!
      bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$C_WINDOW" "$REC_INTERVAL" &
      rec_pid=$!
      wait "$rec_pid" || true
      wait "$inj_pid" || true
      capture "$name" "pendant-panne"
      if wait_convergence "$name" "$C_DRAIN_MAX"; then
        bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$TAIL_WINDOW" "$REC_INTERVAL" || true
      fi
      capture "$name" final
      ;;
    *)
      bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$REC_WINDOW" "$REC_INTERVAL"
      capture "$name" final
      ;;
  esac
  log_event "$name" "case_end" ""
  echo "[cross-check] cas $name terminé"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --skip-start) SKIP_START=1 ;;
    --cases) CASES_PLAN="$2"; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Option inconnue : $1" >&2; exit 1 ;;
  esac
  shift
done

mkdir -p "$OUT_DIR"
printf '{"t_epoch":%s,"event":"crosscheck_start","cases":"%s"}\n' \
  "$(date +%s)" "$CASES_PLAN" > "$OUT_DIR/crosscheck-log.jsonl"

if ! health_ok; then
  if [ "$SKIP_START" = 1 ]; then
    echo "[cross-check] /health KO et --skip-start : abandon" >&2
    exit 1
  fi
  echo "[cross-check] /health KO : démarrage de la plateforme (./start.sh)"
  ./start.sh
fi

IFS=', ' read -r -a CASES <<< "$CASES_PLAN"
for case_name in "${CASES[@]}"; do
  case "$case_name" in
    A-draine)
      run_case "$case_name" \
        env SIMULPIX_PROCESSING_DELAY_MS_OVERRIDE=400 \
            SIMULPIX_PERSIST_VALID_DELAY_MS_OVERRIDE=0 \
            SIMULPIX_PERSIST_REJECTED_DELAY_MS_OVERRIDE=0 \
        ./scripts/run-scenario.sh consumer_lag 600 20
      ;;
    B-replication-croise)
      run_case "$case_name" \
        env SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE=20 \
            SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE=kafka-3 \
        ./scripts/run-scenario.sh replication_lag 600 20
      ;;
    C-rattrapage)
      run_case "$case_name" ./scripts/run-scenario.sh nominal 800 15
      ;;
    rejets-metier)
      run_case "$case_name" ./scripts/run-scenario.sh errors_simple 300 10
      ;;
    *)
      echo "[cross-check] cas inconnu : $case_name (ignoré)" >&2
      ;;
  esac
done

printf '{"t_epoch":%s,"event":"crosscheck_end"}\n' "$(date +%s)" >> "$OUT_DIR/crosscheck-log.jsonl"
echo "[cross-check] campagne terminée, génération du rapport"
python3 "$SCRIPT_DIR/lib/crosscheck_report.py" "$OUT_DIR"
