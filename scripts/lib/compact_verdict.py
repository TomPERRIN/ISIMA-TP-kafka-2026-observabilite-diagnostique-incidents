#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compacte la sortie JSON du moteur de diagnostic en une ligne JSONL.

Usage (utilisé par scripts/diagnostic-campaign.sh) :
    T_EPOCH=1710000000 python3 compact_verdict.py < sortie_diagnose_json

L'entrée est le verdict multi-lignes imprimé par ``diagnose.sh --json`` ;
la sortie est ``{"t_epoch": ..., "verdict": {...}}`` sur une seule ligne,
acceptable pour un fichier ``verdicts.jsonl`` analysé ligne par ligne.
"""

import json
import os
import sys


def main() -> int:
    try:
        verdict = json.load(sys.stdin)
    except ValueError as error:
        print("compact_verdict: sortie JSON illisible (%s)" % error, file=sys.stderr)
        return 1
    record = {
        "t_epoch": float(os.environ.get("T_EPOCH", "0") or 0),
        "verdict": verdict,
    }
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
