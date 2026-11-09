#!/usr/bin/env bash
# Démo des angles morts du diagnostic Simul-Pix (SAÉ 5).
#
# Rejoue les cas SANS cause nommée du catalogue d'incidents
# (travail/notes/cas-incidents.md : §1 B3/B4/C3/C4, §5 registre des risques) :
# chaque cas fabrique des payloads /health/details simulés — sondes FRAÎCHES —
# et les passe au VRAI moteur (build_verdict). Le verdict affiché est calculé à
# l'exécution : la limite est DÉMONTRÉE, pas déclarée.
#
# Sans plateforme : le moteur tourne en pur Python (aucun Docker, aucune sonde
# réelle) — rejouable tel quel en soutenance, même sans la stack.
#
# Usage :
#   ./scripts/demo-angles-morts.sh               tous les cas + récapitulatif
#   ./scripts/demo-angles-morts.sh --case B4a    un seul cas (B4a/B4/B3/C3/C4/A0/T0)
#   ./scripts/demo-angles-morts.sh --json        verdicts bruts (machine)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/lib/blind_spots.py" "$@"
