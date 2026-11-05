#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI du diagnostic Simul-Pix (SAÉ 5) : relie diagnose.sh au moteur.

Rôles :
  - lire UN échantillon JSON de /health/details (fichier récupéré par le shell) ;
  - maintenir le cache d'état ``runtime/diagnostic-cache.json`` (2 échantillons
    espacés d'au moins SIMULPIX_DIAG_MIN_SAMPLE_SPAN, par défaut 10 s) — c'est ce
    qui rend possible la règle anti « lag éclair » et la détection des deltas ;
  - exécuter le moteur (scripts/lib/diagnose_engine.py) et imprimer le verdict
    en JSON (``--json``) ou en texte français lisible.

Usage (via scripts/diagnose.sh, pas besoin d'appeler ce fichier à la main) :
    python3 diagnose_cli.py --sample <fichier.json> --cache <cache.json> [--json] [--single]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from diagnose_engine import (  # noqa: E402
    DEFAULT_CONFIG,
    RERUN_CMD,
    build_verdict,
)

CACHE_VERSION = 1

FAMILY_LABELS = {
    "healthy": "flux sain (aucune cause nommée)",
    "consumer_backlog": "retard de consommateur (état A)",
    "kafka_replication": "réplication dégradée (état B)",
    "persistence": "persistance défaillante (état C)",
    "indeterminate": "INDÉTERMINÉ (pas de conclusion)",
}


def load_sample(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_cache(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            cache = json.load(handle)
        if isinstance(cache, dict) and isinstance(cache.get("samples"), list):
            return cache
    except (OSError, ValueError):
        pass
    return {"version": CACHE_VERSION, "samples": []}


def save_cache(path: str, cache: Dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(cache, handle, ensure_ascii=False, indent=1)


def merge_sample(cache: Dict[str, Any], payload: Dict[str, Any], wall: float) -> Dict[str, Any]:
    """Ajoute l'échantillon courant : on garde au maximum 2 lectures (ancre + récente)."""
    samples = [entry for entry in cache.get("samples") or []
               if isinstance(entry, dict) and "wall" in entry and "payload" in entry]
    samples.append({"wall": wall, "payload": payload})
    samples = samples[-2:]
    return {"version": CACHE_VERSION, "samples": samples}


def pick_previous(samples: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Le 1er échantillon (l'ancre) quand 2 sont en cache ; le moteur validera le span."""
    if len(samples) >= 2:
        return samples[0]
    return None


def render_text(verdict: Dict[str, Any]) -> str:
    lines: List[str] = []
    provenance = verdict.get("based_on") or {}
    lines.append("=" * 62)
    lines.append("DIAGNOSTIC SIMUL-PIX — échantillon %s" % verdict.get("sample_timestamp"))
    lines.append("état      : %s" % FAMILY_LABELS.get(verdict.get("verdict"),
                                                       verdict.get("verdict")))
    if verdict.get("cause"):
        lines.append("cause     : %s — %s" % (verdict["cause"], verdict.get("cause_label")))
        meta = []
        if verdict.get("probability"):
            meta.append("probabilité %s" % verdict["probability"])
        if verdict.get("criticality"):
            meta.append("criticité %s" % verdict["criticality"])
        if meta:
            lines.append("            %s" % " · ".join(meta))
    if verdict.get("indeterminate_reason"):
        lines.append("motif     : %s" % verdict["indeterminate_reason"])
    if verdict.get("summary"):
        lines.append("résumé    : %s" % verdict["summary"])
    for note in verdict.get("notes") or []:
        lines.append("note      : %s" % note)
    if verdict.get("plausible_causes"):
        lines.append("causes plausibles (ordre « priorité d'examen » de la matrice) :")
        for entry in verdict["plausible_causes"]:
            lines.append("  - %-28s [%s] probabilité %s, criticité %s" % (
                entry.get("cause"), entry.get("status"),
                entry.get("probability"), entry.get("criticality")))
            if entry.get("note"):
                lines.append("      %s" % entry["note"])
            lines.append("      vérification : %s" % entry.get("check"))
    if verdict.get("evidence"):
        lines.append("preuves (double lecture) :")
        for entry in verdict["evidence"]:
            lines.append("  [%-8s] %s : %s" % (entry.get("source"),
                                                entry.get("metric"), entry.get("detail")))
    freshness = verdict.get("freshness") or {}
    if freshness:
        probe_status = "OK" if freshness.get("ok") else "PÉRIMÉ(E)S"
        lines.append("fraîcheur : payload %s s · réplication %s s (limite %s s) — %s" % (
            freshness.get("payload_age_s"), freshness.get("replication_age_s"),
            freshness.get("threshold_s"), probe_status))
        for violation in freshness.get("violations") or []:
            lines.append("           ! %s" % violation)
    if provenance.get("samples"):
        lines.append("échantillons : %s (%s)" % (
            provenance.get("samples"), "span %s s" % provenance["span_s"]
            if provenance.get("span_s") else "lecture unique"))
    if verdict.get("next_check"):
        lines.append("vérification à faire ensuite :")
        for command in verdict["next_check"]:
            lines.append("  $ %s" % command)
    lines.append("=" * 62)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="CLI du moteur de diagnostic Simul-Pix")
    parser.add_argument("--sample", required=True, help="fichier JSON /health/details")
    parser.add_argument("--cache", required=True, help="fichier cache d'état (2 échantillons)")
    parser.add_argument("--json", action="store_true", help="sortie JSON brute")
    parser.add_argument("--single", action="store_true",
                        help="ignorer le cache (mode single-shot : les règles à 2 échantillons "
                             "tombent sur « indéterminé : relire »)")
    args = parser.parse_args()

    try:
        payload = load_sample(args.sample)
    except (OSError, ValueError) as error:
        print("diagnose: impossible de lire l'échantillon (%s) : %s" % (args.sample, error),
              file=sys.stderr)
        return 2

    now = time.time()
    cache = load_cache(args.cache)
    cache = merge_sample(cache, payload, now)
    previous = None if args.single else pick_previous(cache["samples"])
    verdict = build_verdict(payload, now=now, previous=previous)
    save_cache(args.cache, cache)

    if args.json:
        print(json.dumps(verdict, ensure_ascii=False, indent=1))
    else:
        print(render_text(verdict))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
