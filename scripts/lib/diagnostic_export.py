#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export Grafana du diagnostic Simul-Pix (SAÉ 5) : verdict -> InfluxDB.

Rôle :
  - lire UN verdict JSON produit par le moteur (``diagnose_cli.py --json``) ;
  - le convertir en ligne InfluxDB (métrique ``simulpix_diagnostic``) :
    tag ``verdict_family`` (sain / A / B / C / indéterminé) + champs lisibles ;
  - l'écrire dans InfluxDB avec les MÊMES réglages que le socle
    (``infra/observability/scripts/collect-metrics.sh`` : URL, org, bucket,
    jeton — rien à configurer de plus).

Le moteur N'EST PAS modifié : c'est un adaptateur de sortie (extension « vue »,
en réserve dans la note de conception, implémentée le 06/11/2026).
La CLI reste la source de vérité : le dashboard affiche, il ne prouve pas —
toute conclusion d'incident se vérifie toujours avec ``tp-kafka.sh`` / ``tp-db.sh``.

Usage (via scripts/diagnostic-export.sh, pas besoin d'appeler ce fichier à la main) :
    python3 diagnostic_export.py --verdict verdict.json            # envoie dans InfluxDB
    python3 diagnostic_export.py --verdict verdict.json --print    # affiche la ligne seulement
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

# Les 6 états du moteur (mêmes clés que FAMILY_LABELS de diagnose_cli.py).
STATE_CODES = {
    "healthy": 0,
    "consumer_backlog": 1,
    "slow_processing": 4,
    "kafka_replication": 2,
    "persistence": 3,
    "indeterminate": -1,
}

MEASUREMENT = "simulpix_diagnostic"

# L'exporteur tourne sur l'hôte (comme diagnose.sh) : InfluxDB est exposé en
# localhost:8086. Dans le réseau Docker (nom « influxdb »), passer
# SIMULPIX_INFLUXDB_URL=http://influxdb:8086 — idem socle collect-metrics.sh.
DEFAULT_URL = "http://localhost:8086"
DEFAULT_ORG = "simulpix"
DEFAULT_BUCKET = "simulpix_observability"
DEFAULT_TOKEN = "simulpix-observability-token"

FALLBACK_CAUSE = "aucune"
FALLBACK_CAUSE_LABEL = "flux sain (aucune cause nommée)"
FALLBACK_REASON = "aucun"
# Étiquette affichée sur la vue Grafana quand le verdict est sain : on ne nomme
# pas d'« incident » tant que l'état est sain (le détail moteur reste visible
# via ./scripts/diagnose.sh et le champ cause=).
HEALTHY_CAUSE_LABEL = "État sain — aucun incident actif"


def _escape_tag(value: str) -> str:
    """Échappement d'une valeur de tag InfluxDB (virgule, =, espace, antislash)."""
    return (str(value).replace("\\", "\\\\")
            .replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ "))


def _escape_str_field(value: str) -> str:
    """Échappement d'un champ texte InfluxDB (guillemets et antislash)."""
    return (str(value).replace("\\", "\\\\")
            .replace('"', '\\"').replace("\n", " ").replace("\r", " "))


def _num(value: Any) -> Optional[float]:
    """Nombre flottant si la valeur s'y prête, sinon None."""
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def verdict_to_line(verdict: Dict[str, Any], timestamp_ns: Optional[int] = None) -> Optional[str]:
    """Convertit un verdict du moteur en ligne InfluxDB (line protocol, precision=ns).

    Retourne None si le verdict est illisible (pas de champ ``verdict``).
    """
    family = verdict.get("verdict")
    if family not in STATE_CODES:
        return None
    if timestamp_ns is None:
        timestamp_ns = time.time_ns()

    freshness = verdict.get("freshness") or {}
    based_on = verdict.get("based_on") or {}

    # Champs texte toujours présents (les requêtes Flux du dashboard restent stables).
    cause = verdict.get("cause") or FALLBACK_CAUSE
    cause_label = verdict.get("cause_label") or FALLBACK_CAUSE_LABEL
    if family == "healthy":
        # Quand l'état redevient sain, la vue affiche « état sain » au lieu du
        # libellé moteur (« Amont arrêté… » ou cause résiduelle d'un incident passé).
        cause_label = HEALTHY_CAUSE_LABEL
    probability = verdict.get("probability") or "—"
    criticality = verdict.get("criticality") or "—"
    reason = verdict.get("indeterminate_reason") or FALLBACK_REASON

    # Liste des causes candidates du moteur (plausible_causes), triées par ordre
    # d'examen : « [probable] label (probabilité/criticité) • [écartée] ... ».
    candidates = verdict.get("plausible_causes") or []
    if candidates:
        parts = []
        for candidate in sorted(candidates, key=lambda item: item.get("exam_order", 99)):
            candidate_status = candidate.get("status") or "?"
            candidate_label = candidate.get("label") or candidate.get("cause") or "?"
            candidate_prob = candidate.get("probability") or "—"
            candidate_crit = candidate.get("criticality") or "—"
            parts.append("[%s] %s (%s/%s)" % (
                candidate_status, candidate_label, candidate_prob, candidate_crit))
        causes_list = " • ".join(parts)
    else:
        # Pas de cause candidate : « — » (non vide — le last() de Flux saute les
        # valeurs vides) ; la vue le masque pour n'afficher rien à l'état sain.
        causes_list = "—"

    fields = [
        "state_code=%di" % STATE_CODES[family],
        'cause="%s"' % _escape_str_field(cause),
        'cause_label="%s"' % _escape_str_field(cause_label),
        'probability="%s"' % _escape_str_field(probability),
        'criticality="%s"' % _escape_str_field(criticality),
        'indeterminate_reason="%s"' % _escape_str_field(reason),
        'causes_list="%s"' % _escape_str_field(causes_list),
        "freshness_ok=%di" % (1 if freshness.get("ok") else 0),
    ]
    for name in ("payload_age_s", "replication_age_s"):
        value = _num(freshness.get(name))
        if value is not None:
            fields.append("%s=%.2f" % (name, value))
    samples = _num(based_on.get("samples"))
    if samples is not None:
        fields.append("samples=%di" % int(samples))

    # Protocole InfluxDB : champs séparés par des VIRGULES (l'espace marque la
    # fin du jeu de champs et le début du timestamp — « bad timestamp » sinon).
    line = "%s,verdict_family=%s %s %d" % (
        MEASUREMENT, _escape_tag(family), ",".join(fields), timestamp_ns)
    return line


def send_line(line: str, url: str, org: str, bucket: str, token: str,
              timeout: float = 5.0) -> bool:
    """Écrit la ligne dans InfluxDB (même API v2 que le socle). Retourne True si OK."""
    endpoint = "%s/api/v2/write?org=%s&bucket=%s&precision=ns" % (url, org, bucket)
    request = urllib.request.Request(
        endpoint, data=line.encode("utf-8"), method="POST",
        headers={
            "Authorization": "Token %s" % token,
            "Content-Type": "text/plain; charset=utf-8",
        })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError) as error:
        print("diagnostic-export: écriture InfluxDB impossible (%s) : %s"
              % (endpoint, error), file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Grafana du diagnostic Simul-Pix")
    parser.add_argument("--verdict", required=True,
                        help="fichier JSON du verdict ('-' pour l'entrée standard)")
    parser.add_argument("--print", dest="print_line", action="store_true",
                        help="affiche la ligne InfluxDB au lieu de l'envoyer")
    parser.add_argument("--url", default=os.environ.get("SIMULPIX_INFLUXDB_URL", DEFAULT_URL))
    parser.add_argument("--org", default=os.environ.get("SIMULPIX_INFLUXDB_ORG", DEFAULT_ORG))
    parser.add_argument("--bucket",
                        default=os.environ.get("SIMULPIX_INFLUXDB_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--token",
                        default=os.environ.get("SIMULPIX_INFLUXDB_TOKEN", DEFAULT_TOKEN))
    args = parser.parse_args()

    try:
        if args.verdict == "-":
            verdict = json.load(sys.stdin)
        else:
            with open(args.verdict, "r", encoding="utf-8") as handle:
                verdict = json.load(handle)
    except (OSError, ValueError) as error:
        print("diagnostic-export: impossible de lire le verdict (%s) : %s"
              % (args.verdict, error), file=sys.stderr)
        return 2

    line = verdict_to_line(verdict)
    if line is None:
        print("diagnostic-export: verdict illisible (champ « verdict » inconnu)",
              file=sys.stderr)
        return 2

    if args.print_line:
        print(line)
        return 0

    if send_line(line, args.url, args.org, args.bucket, args.token):
        print("diagnostic-export: verdict exporté (%s)" % verdict.get("verdict"))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
