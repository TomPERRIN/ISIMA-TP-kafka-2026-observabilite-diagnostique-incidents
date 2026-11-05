#!/usr/bin/env bash
# Preuve live de la candidate #7 « impact métier » — SAÉ 5, phase 7.
#
# Champ couvert : scénario A (retard de consommateur). Pendant l'incident, le
# verdict du moteur transporte un bloc ``business_impact`` (« X Pix ont dépassé
# le SLA de décision ») issu des compteurs ``validated/rejected_sla_breaches``,
# eux-mêmes comptés EN BASE par la sonde PostgreSQL du socle (colonnes
# ``decision_within_sla``). La preuve confronte TROIS couches aux instants
# t0 / pendant / final :
#
#     1. moteur     : ``business_impact`` du verdict (diagnose.sh --json)
#     2. sonde socle: details.postgres.*_sla_breaches (payload /health/details)
#     3. SQL direct : psql count(*) ... where not decision_within_sla
#
# et vérifie de plus que le delta moteur sur un span contrôlé (2 lectures
# espacées de ~12 s à l'état fermé, enregistreur arrêté) égal(e) la différence
# des totaux SQL pris aux deux mêmes instants.
#
# Usage :
#   ./scripts/impact-metier-proof.sh [messages (300)] [rate (20)] [max_convergence_s (300)]
# Résultats : preuves/phase7-impact-metier-<date>/ — verdicts.jsonl (timeline),
#             snapshots, SQL, rapport-impact-metier.json (moteur == SQL).
set -euo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
SCRIPT_DIR="$PWD/scripts"

MESSAGES="${1:-300}"
RATE="${2:-20}"
MAX_CONVERGENCE_S="${3:-300}"
HEALTH_URL="${SIMULPIX_HEALTH_URL:-http://localhost:8082/health/details}"
EVID="preuves/phase7-impact-metier-$(date -u +%F)"
mkdir -p "$EVID"

db_sla() {
  # SQL direct (source de vérité) : totaux des dépassements SLA + motifs de rejet
  { echo "### db-sla — instant=$(date -u +%FT%TZ)"
    echo "sql: validated = count(*) validated_transactions where not decision_within_sla"
    docker compose exec -T postgres psql -U simulpix -d simulpix -At -P pager=off -c \
      "select 'validated_breaches|'||count(*) from validated_transactions where coalesce(decision_within_sla,true)=false"
    echo "sql: rejected = count(*) rejected_transactions where not decision_within_sla"
    docker compose exec -T postgres psql -U simulpix -d simulpix -At -P pager=off -c \
      "select 'rejected_breaches|'||count(*) from rejected_transactions where coalesce(decision_within_sla,true)=false"
    echo "sql: motifs de rejet (tous les rejets, colonne rejection_reason)"
    docker compose exec -T postgres psql -U simulpix -d simulpix -At -P pager=off -c \
      "select 'reason|'||rejection_reason||'|'||count(*) from rejected_transactions group by rejection_reason order by 1"
  } > "$EVID/db-sla-$1.txt"
}

snapshot() {
  # extraction plate des compteurs de fermeture (réutilise le format cross-check)
  curl -fsS "$HEALTH_URL" 2>/dev/null | python3 -c '
import json, sys
p = json.load(sys.stdin)
c = (p.get("metrics") or {}).get("counts") or {}
g = (p.get("metrics") or {}).get("gaps") or {}
out = {}
for k in ("generated", "validated", "rejected", "outcome_count",
          "raw_topic_end_offsets", "db_validated_count", "db_rejected_count",
          "validated_sla_breaches", "rejected_sla_breaches"):
    out[k] = c.get(k, 0)
pg = (p.get("details") or {}).get("postgres") or {}
out["pg_validated_sla_breaches"] = pg.get("validated_sla_breaches")
out["pg_rejected_sla_breaches"] = pg.get("rejected_sla_breaches")
for k in ("persistence_gap_valid", "persistence_gap_rejected"):
    out[k] = g.get(k, 0)
lag = 0
for k in ("pix_validator_lag_total", "pix_decision_engine_lag_total",
          "pix_outcome_publisher_lag_total", "persister_valid_lag_total",
          "persister_rejected_lag_total"):
    lag = max(lag, c.get(k) or 0)
out["lag_max"] = lag
out["payload_timestamp"] = p.get("timestamp")
print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
' > "$EVID/counts-$1.json"
}

verdict_capture() {
  # Le cache est partagé avec l'enregistreur : si la dernière écriture est trop
  # récente (span < 10 s), le garde-fou anti « lag éclair » refuse le delta — on
  # réessaie en avançant dans le temps, MAX 4 tentatives espacées de 7 s.
  local label="$1" attempt
  for attempt in 1 2 3 4; do
    curl -fsS "$HEALTH_URL" 2>/dev/null > "$EVID/health-$label.json" || true
    python3 "$SCRIPT_DIR/lib/diagnose_cli.py" \
      --sample "$EVID/health-$label.json" \
      --cache runtime/diagnostic-cache.json --json \
        > "$EVID/verdict-$label.json"
    if python3 -c "import json; vc=json.load(open('$PWD/$EVID/verdict-$label.json')); span=(vc.get('based_on') or {}).get('span_s'); raise SystemExit(0 if span and span >= 10 else 1)" 2>/dev/null; then
      return 0
    fi
    sleep 7
  done
}

echo "== [1/6] baseline t0 après reset du cache =="
./scripts/diagnose.sh --reset >/dev/null
./scripts/diagnose.sh >/dev/null                       # lecture 1 (ancre)
sleep 12
verdict_capture t0
db_sla t0
snapshot t0

echo "== [2/6] relevé mutualisé (cadence 11 s) puis scénario consumer_lag ${MESSAGES} @ ${RATE}/s =="
bash "$SCRIPT_DIR/lib/record-verdicts.sh" "$EVID" 620 11 &
REC_PID=$!

./scripts/run-scenario.sh consumer_lag "$MESSAGES" "$RATE"

echo "== [3/6] captures pendant l'incident =="
sleep 45
db_sla pendant-1
verdict_capture pendant-1
snapshot pendant-1
sleep 45
db_sla pendant-2
verdict_capture pendant-2
snapshot pendant-2

echo "== [4/6] attente de la fermeture (<= ${MAX_CONVERGENCE_S} s) =="
waited=0
sleep 5
while [ "$waited" -lt "$MAX_CONVERGENCE_S" ]; do
  if snapshot check && python3 "$SCRIPT_DIR/lib/convergence_check.py" "$EVID/counts-check.json" \
      > "$EVID/convergence.json" 2>/dev/null \
      && grep -q '"converged":true' "$EVID/convergence.json"; then
    echo "    fermeture atteinte après ${waited} s"
    break
  fi
  sleep 10
  waited=$((waited + 10))
done
cp "$EVID/counts-check.json" "$EVID/counts-final.json" 2>/dev/null || true

echo "== [5/6] delta contrôlé (enregistreur arrêté, 2 lectures espacées ~12 s) =="
kill "$REC_PID" 2>/dev/null || true
wait "$REC_PID" 2>/dev/null || true
db_sla pre-delta
./scripts/diagnose.sh >/dev/null                       # lecture d'ancre du delta contrôlé
sleep 12
verdict_capture final
db_sla final
{ echo "### final — $(date -u +%FT%TZ)"; ./scripts/tp-kafka.sh; } > "$EVID/tp-kafka-final.txt"
{ echo "### final — $(date -u +%FT%TZ)"; ./scripts/tp-db.sh; }     > "$EVID/tp-db-final.txt"

echo "== [6/6] rapport moteur vs sonde vs SQL =="
python3 "$SCRIPT_DIR/lib/impact_report.py" "$EVID" "$MESSAGES" "$RATE"
echo
echo "Preuves écrites dans $EVID (timeline verdicts.jsonl + rapport-impact-metier.json)."
