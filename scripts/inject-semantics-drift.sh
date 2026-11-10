#!/bin/sh
set -eu

# inject-semantics-drift.sh — simule un client ancien (famille D, contrat).
#
# Le socle ne vérifie AUCUNE version de contrat : un client qui n'a pas mis à
# jour son application envoie un format qui n'est plus supporté, et le pipeline
# le traite comme un message métier normal — ou pire, boucle dessus.
#
#   ./scripts/inject-semantics-drift.sh missing    champ obligatoire retiré
#   ./scripts/inject-semantics-drift.sh enum       valeur d'énumération inconnue
#   ./scripts/inject-semantics-drift.sh extra      champ nouveau (dérive silencieuse)
#   ./scripts/inject-semantics-drift.sh malformed  payload non-JSON
#   ./scripts/inject-semantics-drift.sh poison     type changé (amount = "12,50")
#   ./scripts/inject-semantics-drift.sh cleanup    débloque le groupe après un poison
#
# Usage : ./scripts/inject-semantics-drift.sh <mode> [nombre_de_messages]
# Défaut : 1 message. Le script publie sur simulpix.transactions.raw, attend 6 s,
# puis affiche l'évolution des compteurs et les derniers motifs de rejet.
#
# ATTENTION : en mode `poison`, le validator relit le message indéfiniment
# (commit après traitement). Terminer par `cleanup` pour ne pas laisser la
# plateforme bloquée.

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

MODE="${1:-}"
COUNT="${2:-1}"
RAW_TOPIC="${SIMULPIX_TOPIC_RAW:-simulpix.transactions.raw}"
VALIDATOR_GROUP="${SIMULPIX_PIX_VALIDATOR_GROUP:-simulpix-validator-v1}"

if [ -z "$MODE" ]; then
  echo "Usage : $0 <missing|enum|extra|malformed|poison|cleanup> [nombre]" >&2
  exit 1
fi

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "[simulpix] docker introuvable : l'injection nécessite la plateforme démarrée." >&2
    exit 1
  fi
}

health_block() {
  curl -fsS "http://127.0.0.1:8082/health/details" 2>/dev/null | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("  (service-health indisponible)")
    raise SystemExit(0)
c = d.get("metrics", {}).get("counts", {})
g = d.get("metrics", {}).get("gaps", {})
for k in ("generated", "validated", "rejected", "outcome_count",
          "db_validated_count", "db_rejected_count"):
    print("  %-22s = %s" % (k, c.get(k)))
print("  ecart valid/reject     = %s/%s" % (
    g.get("persistence_gap_valid"), g.get("persistence_gap_rejected")))
svc = d.get("services", {})
print("  pix-validator          = %s" % svc.get("pix-validator"))
' || echo "  (service-health indisponible)"
}

rejections_block() {
  docker_compose exec -T postgres psql -U simulpix -d simulpix -At -F '|' -c "
select coalesce(rejection_reason, '(aucun)'), count(*)
from rejected_transactions
group by 1
order by 2 desc, 1 asc
limit 8;
" 2>/dev/null | sed 's/^/  /' || echo "  (postgres indisponible)"
}

publish() {
  # $1 = fichier temporaire contenant un message JSON par ligne
  payload_file="$1"
  docker_compose exec -T kafka-1 sh -lc '
    producer_bin="${KAFKA_PRODUCER_BIN:-/opt/kafka/bin/kafka-console-producer.sh}"
    client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"
    "$producer_bin" \
      --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
      --producer.config "$client_config" \
      --topic "$1"
  ' sh "$RAW_TOPIC" < "$payload_file"
}

build_payloads() {
  # $1 = mode, $2 = nombre, $3 = fichier de sortie
  MODE="$1" COUNT="$2" OUT="$3" python3 - <<'PY'
import json
import os
from datetime import datetime, timezone

mode = os.environ["MODE"]
count = int(os.environ["COUNT"])
out = os.environ["OUT"]
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

lines = []
for i in range(max(count, 1)):
    tx = f"tx-drift-{mode}-{stamp}-{i:04d}"
    message = {
        "transaction_id": tx,
        "event_time": now,
        "emitter_account_id": "acc-0001",
        "beneficiary_account_id": "acc-0003",
        "emitter_pix_client_id": "pixc-0001",
        "beneficiary_pix_client_id": "pixc-0002",
        "emitter_tax_id": "TAX-0001",
        "beneficiary_tax_id": "TAX-0002",
        "amount": 125.5,
        "currency": "BRL",
        "status": "RECEIVED",
        "scenario_type": "semantics_drift",
        "producer_id": "old-client-v1",
        "trace_id": f"trace-{tx}",
        "decision_sla_seconds": 5,
    }

    if mode == "missing":
        # client v1 : le champ n'existait pas encore
        del message["emitter_pix_client_id"]
    elif mode == "enum":
        # client v2 : enum ajouté côté serveur, inconnu du client
        message["status"] = "PENDING_V2"
    elif mode == "extra":
        # client v3 : champ que le serveur ne connaît pas encore
        message["client_version"] = "3.1"
        message["legacy_routing_hint"] = "batch-2024"
    elif mode == "poison":
        # type changé : le client envoie une chaîne à virgule décimale
        message["amount"] = "12,50"
    elif mode == "malformed":
        lines.append('{"transaction_id": "%s", "amount": ' % tx)
        continue
    elif mode == "cleanup":
        raise SystemExit("cleanup ne publie rien")
    else:
        raise SystemExit(f"mode inconnu : {mode}")

    lines.append(json.dumps(message, ensure_ascii=False))

with open(out, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines) + ("\n" if lines else ""))
PY
}

cleanup_poison() {
  require_docker
  echo "[simulpix] arrêt du validator pour libérer le groupe ${VALIDATOR_GROUP}"
  docker_compose stop pix-validator >/dev/null
  sleep 2

  echo "[simulpix] remise du groupe à la fin du topic ${RAW_TOPIC}"
  if ! docker_compose exec -T kafka-1 sh -lc '
    groups_bin="${KAFKA_GROUPS_BIN:-/opt/kafka/bin/kafka-consumer-groups.sh}"
    client_config="${KAFKA_CLIENT_CONFIG:-/config/client.properties}"
    "$groups_bin" \
      --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
      --command-config "$client_config" \
      --group "$1" \
      --topic "$2" \
      --reset-offsets --to-latest --execute
  ' sh "$VALIDATOR_GROUP" "$RAW_TOPIC"; then
    echo "[simulpix] échec de la remise à jour du groupe (groupe absent ?) — redémarrage quand même" >&2
  fi

  echo "[simulpix] redémarrage du validator"
  docker_compose start pix-validator >/dev/null
  echo "[simulpix] groupe remis à jour — les messages en attente ont été sautés"
}

require_docker

if [ "$MODE" = "cleanup" ]; then
  cleanup_poison
  exit 0
fi

if ! curl -fsS "http://127.0.0.1:8082/health" >/dev/null 2>&1; then
  echo "[simulpix] plateforme non démarrée : lancer ./start.sh d'abord." >&2
  exit 1
fi

tmp_payload="$(mktemp)"
cleanup_tmp() {
  rm -f "$tmp_payload"
}
trap cleanup_tmp EXIT

build_payloads "$MODE" "$COUNT" "$tmp_payload"

echo "=== avant injection (${MODE} x${COUNT}) ==="
health_block
echo "  motifs de rejet :"
rejections_block

echo
echo "[simulpix] publication sur ${RAW_TOPIC}"
publish "$tmp_payload"

echo "[simulpix] attente de 6 s (traitement + relevé)"
sleep 6

echo
echo "=== après injection ==="
health_block
echo "  motifs de rejet :"
rejections_block

echo
case "$MODE" in
  missing|enum)
    cat <<'EOF'
Attendu : un rejet dont le motif est contractuel (missing_*, invalid_initial_status),
mais le client est informé d'un rejet BANCAIRE (BANK_REJECTED) — c'est le défaut.
À vérifier : ./scripts/tp-db.sh  puis  ./scripts/diagnose.sh --json
EOF
    ;;
  extra)
    cat <<'EOF'
Attendu : AUCUN signal. Le champ nouveau est recopié silencieusement.
C'est la dérive sémantique : la donnée a changé de sens sans alerte.
EOF
    ;;
  poison)
    cat <<'EOF'
Attendu : le validator passe en error et relit le même message (commit après
traitement). Le lag du groupe monte, aucun rejet n'est produit, et le moteur
conclut consumer_backlog (famille A) au lieu de contrat (famille D).

IMPORTANT : terminer par
  ./scripts/inject-semantics-drift.sh cleanup
sinon la plateforme reste bloquée sur le message empoisonné.
EOF
    ;;
  malformed)
    cat <<'EOF'
Attendu : erreur de décodage JSON, donc poison ou rejet selon l'endroit du décodage.
Si le validator boucle, terminer par ./scripts/inject-semantics-drift.sh cleanup
EOF
    ;;
esac
