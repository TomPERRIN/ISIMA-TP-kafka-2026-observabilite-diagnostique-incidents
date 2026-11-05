#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Convergence d'un état de la plateforme = fermeture complète de la chaîne (SAÉ 5).

Usage :
    convergence_check.py <counts.json>   # compteurs d'UN seul snapshot

Vérifie sur ce snapshot (et PAS sur des deltas entre deux instants : les
compteurs cumulés sont recalculés à chaque redémarrage de service-health quand
un scénario est relancé) :

  generated == raw_topic_end_offsets            (le générateur a tout publié)
  validated + rejected == generated             (chaque Pix aune décision)
  outcome_count == generated                    (chaque décision a un état final)
  db_validated_count + db_rejected_count == outcome_count
                                                (tout est persisté en base)
  persistence_gap_valid + persistence_gap_rejected == 0   (écarts nuls)
  lag_max <= 5                                          (consommateurs à jour)

Sortie : JSON compact {"converged": bool, "checks": {...}, "values": {...}}
(Toujours exit 0 : l'appelant interprète le JSON.)
"""

import json
import sys


def main() -> int:
    try:
        with open(sys.argv[1], "r", encoding="utf-8") as handle:
            now = json.load(handle)
    except (IndexError, OSError, ValueError) as error:
        print(json.dumps({"converged": False, "error": str(error)}))
        return 0

    generated = int(now.get("generated") or 0)
    validated = int(now.get("validated") or 0)
    rejected = int(now.get("rejected") or 0)
    outcome = int(now.get("outcome_count") or 0)
    raw = int(now.get("raw_topic_end_offsets") or 0)
    db_v = int(now.get("db_validated_count") or 0)
    db_r = int(now.get("db_rejected_count") or 0)
    gaps = int(now.get("persistence_gap_valid") or 0) + int(now.get("persistence_gap_rejected") or 0)
    lag = int(now.get("lag_max") or 0)

    checks = {
        "generated_vs_raw": generated == raw and generated > 0,
        "decisions_vs_generated": (validated + rejected) == generated and generated > 0,
        "outcome_vs_decisions": outcome == (validated + rejected) and outcome > 0,
        "db_vs_outcome": (db_v + db_r) == outcome and outcome > 0,
        "persistence_gaps_zero": gaps == 0,
        "consumers_caught_up": lag <= 5,
    }
    print(json.dumps({
        "converged": all(checks.values()),
        "checks": checks,
        "values": {
            "generated": generated, "validated": validated, "rejected": rejected,
            "outcome_count": outcome, "db_validated": db_v, "db_rejected": db_r,
            "persistence_gaps": gaps, "lag_max": lag,
            "payload_timestamp": now.get("payload_timestamp"),
        },
    }, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
