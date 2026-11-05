#!/usr/bin/env bash
#
# diagnostic-campaign.sh — campagne d'incidents provoqués (SAÉ 5, phase 4)
#
# Exécute la matrice d'essais du pilotage (travail/TODO.md §3) :
#
#   temoin-1..N    scénario nominal SANS incident — contrôle des faux positifs
#                  (critère #4 : 0 diagnostic d'incident sur ≥ 5 nominaux) ;
#   A-retard-lecture  consumer_lag avec retard de décision (350 ms) et
#                  persisters rapides -> famille attendue consumer_backlog ;
#   B-replication  replication_lag : kafka-3 arrêté 20 s par le scénario
#                  -> famille attendue kafka_replication ;
#   C-postgres-indispo   nominal avec docker compose stop postgres (40 s)
#                  au milieu du flux -> famille attendue persistence ;
#   C-postgres-degrade   consumer_lag avec persisters ralentis (350 ms),
#                  décision rapide -> famille attendue persistence.
#
# Chaque cas :
#   1) lance le scénario (run-scenario.sh resette les consommateurs et
#      relance le générateur) ;
#   2) enregistre les verdicts horodatés de ``./scripts/diagnose.sh --json``
#      toutes les REC_INTERVAL s dans <cas>/verdicts.jsonl ;
#   3) journalise les évènements (t_epoch) dans <cas>/events.jsonl ;
#   4) sauvegarde la sortie du scénario dans <cas>/scenario.log.
#
# En fin de campagne, le rapport est produit par
# ``scripts/lib/campaign_report.py`` (tableau + synthèse JSON + SYNTHESE.md).
#
# Usage :
#   ./scripts/diagnostic-campaign.sh                 campagne complète
#   ./scripts/diagnostic-campaign.sh --skip-start    ne pas relancer ./start.sh
#                                                    (défaut : démarré si /health KO)
#   ./scripts/diagnostic-campaign.sh --quick         2 témoins au lieu de 5
#   ./scripts/diagnostic-campaign.sh --cases A,C-postgres-indispo   sous-ensemble
#
# Variables d'environnement utiles :
#   SIMULPIX_CAMPAIGN_DIR   répertoire des relevés
#                           (défaut preuves/phase4-campagne-<date>)
#   REC_INTERVAL/REC_WINDOW Witness/REC_WINDOW_INCIDENT   cadence et fenêtre du
#                           relevé (défaut 5 s / 75 s / 115 s)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

REC_INTERVAL="${REC_INTERVAL:-11}"   # >= 10 s : span minimal des 2 échantillons du moteur
REC_WINDOW_WITNESS="${REC_WINDOW_WITNESS:-75}"
REC_WINDOW_INCIDENT="${REC_WINDOW_INCIDENT:-115}"
CAMPAIGN_T0="$(date +%s)"
DEFAULT_OUT="$REPO_DIR/preuves/phase4-campagne-$(date +%Y-%m-%d)"
OUT_DIR="${SIMULPIX_CAMPAIGN_DIR:-$DEFAULT_OUT}"
SKIP_START=0
QUICK=0
PROFILE_TEMOIN="${PROFILE_TEMOIN:-200 10}"
PROFILE_INCIDENT="${PROFILE_INCIDENT:-600 20}"
PROFILE_C_INDISPO="${PROFILE_C_INDISPO:-800 15}"
A_PROCESSING_MS="${A_PROCESSING_MS:-400}"
C_PERSIST_MS="${C_PERSIST_MS:-350}"
PG_OUTAGE_S="${PG_OUTAGE_S:-40}"
PG_INJECT_DELAY_S="${PG_INJECT_DELAY_S:-8}"

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

ensure_platform() {
  if health_ok; then
    echo "[campaign] plateforme déjà up (usage : --skip-start pour sauter ce contrôle)"
    return 0
  fi
  if [ "$SKIP_START" = 1 ]; then
    echo "[campaign] /health KO et --skip-start : abandon" >&2
    return 1
  fi
  echo "[campaign] /health KO : démarrage de la plateforme (./start.sh)"
  ./start.sh || return 1
}

run_recorder() {
  # run_recorder <sous-dossier du cas> <fenêtre en s>
  local dir="$1" window="$2"
  bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$dir" "$window" "$REC_INTERVAL"
}

pg_outage_background() {
  # Injecte l'indisponibilité PostgreSQL au milieu du flux, puis la retire.
  local name="$1"
  (
    sleep "$PG_INJECT_DELAY_S"
    log_event "$name" "fault_injected" "docker compose stop postgres (${PG_OUTAGE_S}s)"
    docker_compose stop postgres >/dev/null
    sleep "$PG_OUTAGE_S"
    log_event "$name" "fault_recovered" "docker compose start postgres"
    docker_compose start postgres >/dev/null
    if wait_health 60; then
      log_event "$name" "platform_ok_after_recovery" ""
    else
      log_event "$name" "platform_ko_after_recovery" ""
    fi
  ) &
}

run_case() {
  # run_case <nom de cas> [commande scénario complète, sans redirection]
  local name="$1"
  shift
  local dir="$OUT_DIR/$name"
  mkdir -p "$dir"
  : > "$dir/events.jsonl"
  : > "$dir/verdicts.jsonl"
  echo "[campaign] ========== cas $name : $* =========="
  log_event "$name" "case_start" "$*"
  rm -f "$REPO_DIR/runtime/diagnostic-cache.json" "$REPO_DIR/runtime/.diagnose-sample.json"

  local t_launch
  t_launch="$(date +%s)"
  log_event "$name" "scenario_launch" "run-scenario.sh $*"

  if ! "$@" >"$dir/scenario.log" 2>&1; then
    echo "[campaign] ! scénario en échec pour $name (voir scenario.log)" >&2
    log_event "$name" "scenario_failed" "$(tail -n 3 "$dir/scenario.log" | tr '\n' ' ' | tr -d '"')"
    return 1
  fi
  local t_ready
  t_ready="$(date +%s)"
  log_event "$name" "scenario_ready" "t_launch=$t_launch"
  sleep 3

  local window="$REC_WINDOW_INCIDENT"
  case "$name" in
    temoin-*) window="$REC_WINDOW_WITNESS" ;;
  esac

  if [ "$name" = "C-postgres-indispo" ]; then
    pg_outage_background "$name"
  fi
  run_recorder "$dir" "$window"
  wait 2>/dev/null || true

  log_event "$name" "case_end" "window=${window}s"
  echo "[campaign] cas $name terminé (${window}s de relevé)"
}

# ----------------------------------------------------------------- arguments
CASES_PLAN="temoin,A-retard-lecture,B-replication,C-postgres-indispo,C-postgres-degrade"
WITNESSES=5
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-start) SKIP_START=1 ;;
    --quick) QUICK=1 ;;
    --cases) CASES_PLAN="$2"; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Option inconnue : $1" >&2; exit 1 ;;
  esac
  shift
done
[ "$QUICK" = 1 ] && WITNESSES=2

# ----------------------------------------------------------------- campagne
mkdir -p "$OUT_DIR"
printf '{"t_epoch":%s,"event":"campaign_start","cases":"%s"}\n' \
  "$CAMPAIGN_T0" "$CASES_PLAN" > "$OUT_DIR/campaign-log.jsonl"
echo "[campaign] relevés dans $OUT_DIR"

ensure_platform
log_event_sync() {
  printf '{"t_epoch":%s,"event":"%s"}\n' "$(date +%s)" "$1" >> "$OUT_DIR/campaign-log.jsonl"
}

IFS=', ' read -r -a CASES <<< "$CASES_PLAN"
for case_name in "${CASES[@]}"; do
  case "$case_name" in
    temoin)
      witnesses_i=1
      while [ "$witnesses_i" -le "$WITNESSES" ]; do
        ARGS=(./scripts/run-scenario.sh nominal $PROFILE_TEMOIN)
        run_case "temoin-$witnesses_i" "${ARGS[@]}" || true
        witnesses_i=$((witnesses_i + 1))
      done
      ;;
    A-retard-lecture)
      ARGS=(env SIMULPIX_PROCESSING_DELAY_MS_OVERRIDE="$A_PROCESSING_MS" \
                 SIMULPIX_PERSIST_VALID_DELAY_MS_OVERRIDE=0 \
                 SIMULPIX_PERSIST_REJECTED_DELAY_MS_OVERRIDE=0 \
            ./scripts/run-scenario.sh consumer_lag $PROFILE_INCIDENT)
      run_case "$case_name" "${ARGS[@]}" || true
      ;;
    B-replication)
      ARGS=(env SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE=20 \
                 SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE=kafka-3 \
            ./scripts/run-scenario.sh replication_lag $PROFILE_INCIDENT)
      run_case "$case_name" "${ARGS[@]}" || true
      ;;
    C-postgres-indispo)
      ARGS=(./scripts/run-scenario.sh nominal $PROFILE_C_INDISPO)
      run_case "$case_name" "${ARGS[@]}" || true
      ;;
    C-postgres-degrade)
      ARGS=(env SIMULPIX_PROCESSING_DELAY_MS_OVERRIDE=0 \
                 SIMULPIX_PERSIST_VALID_DELAY_MS_OVERRIDE="$C_PERSIST_MS" \
                 SIMULPIX_PERSIST_REJECTED_DELAY_MS_OVERRIDE="$C_PERSIST_MS" \
            ./scripts/run-scenario.sh consumer_lag $PROFILE_INCIDENT)
      run_case "$case_name" "${ARGS[@]}" || true
      ;;
    *)
      echo "[campaign] cas inconnu : $case_name (ignoré)" >&2
      ;;
  esac
done

log_event_sync "campaign_end"
echo "[campaign] campagne terminée, génération du rapport"
python3 "$SCRIPT_DIR/lib/campaign_report.py" "$OUT_DIR"
