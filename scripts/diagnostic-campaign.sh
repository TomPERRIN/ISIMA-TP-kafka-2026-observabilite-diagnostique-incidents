#!/usr/bin/env bash
#
# diagnostic-campaign.sh — campagne d'incidents provoqués (SAÉ 5, phase 4)
#
# Exécute la matrice d'essais de la phase 4 (pilotage : TODO-SAE5.md §7) :
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
#                  décision rapide -> famille attendue persistence ;
#   D-traitement-lent  nominal lancé via l'UI (POST /control) avec
#                  decision_sla_seconds=3, perturbation réseau kafka_latency
#                  sur pix-decision-engine au milieu du flux -> traitement
#                  long à file vide -> famille attendue slow_processing
#                  (symptôme 2) ; si la file grossit ensuite -> consumer_backlog
#                  (le lien causal traitement long -> attente grossissante).
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
#   ./scripts/diagnostic-campaign.sh --pause         attendre Entrée entre les cas
#                                                    (observation de la frise Grafana)
#
# Démo couplée (exporteur Grafana + campagne, un seul terminal) :
#   ./scripts/demo-diagnostic.sh                     (voir ce script)
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
# Cas D (traitement lent) : le SLA lu par le moteur est celui de l'UI
# (decision_sla_seconds, minimum 3) ; perturbation kafka_latency sur
# pix-decision-engine, injectée au milieu du flux (file encore vide).
HEALTH_URL_BASE="${SIMULPIX_HEALTH_URL_BASE:-http://127.0.0.1:8082}"
D_SLA_SECONDS="${D_SLA_SECONDS:-3}"
D_TOTAL_MESSAGES="${D_TOTAL_MESSAGES:-1500}"
D_RATE_PER_SECOND="${D_RATE_PER_SECOND:-20}"
D_WARMUP_S="${D_WARMUP_S:-25}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

health_ok() {
  curl -fsS "$HEALTH_URL_BASE/health" >/dev/null 2>&1
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

control_post() {
  # control_post <corps form-urlencoded> : soumet le formulaire de pilotage de
  # service-health (POST /control, comme le fait l'UI) — le service renvoie 303.
  curl -fsS -o /dev/null -X POST -d "$1" "$HEALTH_URL_BASE/control"
}

d_scenario_and_fault_background() {
  # Cas D : lance le scénario nominal via l'UI (POST /control avec
  # decision_sla_seconds=$D_SLA_SECONDS) en tâche de fond, puis injecte la
  # perturbation réseau kafka_latency sur pix-decision-engine au milieu du
  # flux. Le SLA lu par le moteur est celui de l'UI : la perturbation rend le
  # traitement long à FILE VIDE -> slow_processing, sans attendre les breaches.
  local name="$1"
  (
    sleep 2
    if ! control_post "action=run_scenario&scenario=nominal&total_messages=$D_TOTAL_MESSAGES&rate_per_second=$D_RATE_PER_SECOND&traffic_model=linear&decision_sla_seconds=$D_SLA_SECONDS"; then
      log_event "$name" "scenario_post_failed" "POST /control run_scenario"
      return 1
    fi
  ) &
  # warm-up : laisser le reset (le POST est bloquant ~20 s) puis le flux démarrer
  sleep "$D_WARMUP_S"
  log_event "$name" "fault_injected" \
    "kafka_latency sur pix-decision-engine (POST /control, SLA UI=$D_SLA_SECONDS s)"
  if ! control_post "action=apply_network_profile&confirm_network_apply=on&network_profile=kafka_latency&network_target_service=pix-decision-engine"; then
    log_event "$name" "fault_failed" "POST /control apply_network_profile"
    return 1
  fi
}

d_reset_network() {
  # retire la perturbation réseau en fin de cas (quel que soit le résultat du relevé)
  local name="$1"
  if control_post "action=reset_network_profile"; then
    log_event "$name" "fault_recovered" "POST /control reset_network_profile"
  else
    log_event "$name" "fault_recover_failed" "POST /control reset_network_profile"
  fi
}

verdict_attendu() {
  # rappel affiché à chaque cas : quelle sortie diagnostic il doit produire
  # (le rapport vérifie la couverture, cf. campaign_report.py coverage())
  case "$1" in
    temoin-*) echo "healthy (0 faux positif)" ;;
    A-*)      echo "consumer_backlog (file d'attente qui grossit)" ;;
    B-*)      echo "kafka_replication (réplication dégradée)" ;;
    C-*)      echo "persistence (PostgreSQL ne suit plus)" ;;
    D-*)      echo "slow_processing (traitement lent, file vide)" ;;
    *)        echo "—" ;;
  esac
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
  echo "[campaign] sortie attendue : $(verdict_attendu "$name")"
  log_event "$name" "case_start" "$*"
  rm -f "$REPO_DIR/runtime/diagnostic-cache.json" "$REPO_DIR/runtime/.diagnose-sample.json"

  if [ "$name" = "D-traitement-lent" ]; then
    # déroulé spécifique : le scénario est lancé via l'UI (POST /control) en
    # tâche de fond, la perturbation réseau est injectée au milieu du flux et
    # le relevé démarre pendant que le flux tourne (pas après la fin du scénario).
    local window="$REC_WINDOW_INCIDENT"
    : > "$dir/scenario.log"
    log_event "$name" "scenario_launch" \
      "POST /control run_scenario nominal ${D_TOTAL_MESSAGES}x${D_RATE_PER_SECOND}, SLA UI=${D_SLA_SECONDS}s"
    d_scenario_and_fault_background "$name"
    # le moteur lit le SLA de l'UI (decision_sla_seconds) dans /health/details
    local health_url="${SIMULPIX_HEALTH_URL:-http://127.0.0.1:8082/health/details}"
    SIMULPIX_HEALTH_URL="${health_url}?decision_sla_seconds=$D_SLA_SECONDS" \
      run_recorder "$dir" "$window"
    d_reset_network "$name"
    wait 2>/dev/null || true
    log_event "$name" "case_end" "window=${window}s"
    echo "[campaign] cas $name terminé (${window}s de relevé)"
    return 0
  fi

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
CASES_PLAN="temoin,A-retard-lecture,B-replication,C-postgres-indispo,C-postgres-degrade,D-traitement-lent"
WITNESSES=5
PAUSE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-start) SKIP_START=1 ;;
    --quick) QUICK=1 ;;
    --pause) PAUSE=1 ;;
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
  if [ "$PAUSE" = 1 ]; then
    printf '[campaign] --pause : Entrée pour lancer « %s » (Ctrl+C pour arrêter) : ' "$case_name"
    read -r _
  fi
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
    D-traitement-lent)
      # déroulé spécifique géré par run_case (POST /control + perturbation
      # réseau au milieu du flux) : pas de commande scénario
      run_case "$case_name" || true
      ;;
    *)
      echo "[campaign] cas inconnu : $case_name (ignoré)" >&2
      ;;
  esac
done

log_event_sync "campaign_end"
echo "[campaign] campagne terminée, génération du rapport"
python3 "$SCRIPT_DIR/lib/campaign_report.py" "$OUT_DIR"
