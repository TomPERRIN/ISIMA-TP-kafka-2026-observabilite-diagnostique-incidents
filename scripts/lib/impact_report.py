#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rapport « impact métier » (phase 7 — candidate de garantie #7).

Consolide les captures produites par ``scripts/impact-metier-proof.sh`` et
confronte, à chaque instant t0 / pendant / final, trois couches :

    1. le moteur     : champ ``business_impact`` du verdict (diagnose.sh --json) ;
    2. la sonde socle: ``details.postgres.*_sla_breaches`` du payload ;
    3. le SQL direct : count(*) ... where not decision_within_sla (psql à la main).

Vérifications (checks) :
    - moteur total == SQL à la fermeture ;
    - sonde socle == SQL (le relais payload ne déforme rien) ;
    - moteur == sonde socle ;
    - delta moteur sur le span contrôlé (2 lectures espacées ~12 s, enregistreur
      arrêté) == différence des totaux SQL pris aux deux mêmes instants.

Usage :
    python3 impact_report.py <dossier preuves> [messages] [rate]
Sortie :
    <dossier>/rapport-impact-metier.json + affichage des checks.
"""

import json
import os
import sys


def load(evid, name):
    return json.loads((evid / name).read_text(encoding="utf-8"))


def sql_totals(evid, name):
    validated = rejected = None
    reasons = {}
    for line in (evid / name).read_text(encoding="utf-8").splitlines():
        if line.startswith("validated_breaches|"):
            validated = int(line.split("|")[1])
        elif line.startswith("rejected_breaches|"):
            rejected = int(line.split("|")[1])
        elif line.startswith("reason|"):
            _, reason, count = line.split("|", 2)
            reasons[reason] = int(count)
    return {"validated_breaches": validated, "rejected_breaches": rejected,
            "rejection_reasons": reasons}


def point(evid, label):
    verdict = load(evid, f"verdict-{label}.json")
    counts = load(evid, f"counts-{label}.json")
    impact = verdict.get("business_impact") or {}
    return {
        "verdict": verdict.get("verdict"),
        "cause": verdict.get("cause"),
        "indeterminate_reason": verdict.get("indeterminate_reason"),
        "samples": (verdict.get("based_on") or {}).get("samples"),
        "span_s": (verdict.get("based_on") or {}).get("span_s"),
        "moteur": {
            "total": impact.get("total_sla_breaches"),
            "delta": impact.get("delta_sla_breaches"),
            "validated": impact.get("validated_sla_breaches"),
            "rejected": impact.get("rejected_sla_breaches"),
            "label": impact.get("label"),
        },
        "payload_metrics_counts": {
            "validated": counts.get("validated_sla_breaches"),
            "rejected": counts.get("rejected_sla_breaches"),
        },
        "payload_details_pg": {
            "validated": counts.get("pg_validated_sla_breaches"),
            "rejected": counts.get("pg_rejected_sla_breaches"),
        },
        "sql": sql_totals(evid, f"db-sla-{label}.txt"),
    }


def main() -> int:
    evid = os.path.normpath(sys.argv[1])
    evid = __import__("pathlib").Path(evid)
    messages = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    rate = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    t0 = point(evid, "t0")
    pendant1 = point(evid, "pendant-1")
    pendant2 = point(evid, "pendant-2")
    final = point(evid, "final")

    def engine_total_sum(p):
        return (p["moteur"]["validated"] or 0) + (p["moteur"]["rejected"] or 0)

    def sql_total_sum(p):
        return ((p["sql"]["validated_breaches"] or 0)
                + (p["sql"]["rejected_breaches"] or 0))

    sql_pre_delta = sql_totals(evid, "db-sla-pre-delta.txt")
    sql_pre_total = ((sql_pre_delta["validated_breaches"] or 0)
                     + (sql_pre_delta["rejected_breaches"] or 0))

    checks = {
        "moteur_total_equals_sql_final":
            engine_total_sum(final) == sql_total_sum(final),
        "sonde_socle_equals_sql_final":
            final["payload_details_pg"]["validated"] == final["sql"]["validated_breaches"]
            and final["payload_details_pg"]["rejected"] == final["sql"]["rejected_breaches"],
        "moteur_equals_sonde_socle_final":
            final["payload_metrics_counts"]["validated"] == final["payload_details_pg"]["validated"]
            and final["payload_metrics_counts"]["rejected"] == final["payload_details_pg"]["rejected"],
        "delta_controle_zero_apres_fermeture":
            final["moteur"]["delta"] in (0, 0.0),
        # à l'état fermé, le SQL ne doit plus bouger entre pre-delta et final ;
        # le pendant-2 (239) était un instant AVANT la fin du drain : exclut.
        "sql_stable_apres_fermeture": sql_total_sum(final) == sql_pre_total,
        # le delta moteur sur le span contrôlé == la variation SQL du même intervalle
        "delta_controle_reproduit_par_sql":
            (final["moteur"]["delta"] or 0) == (sql_total_sum(final) - sql_pre_total),
    }

    rapport = {
        "generated_by": "scripts/impact-metier-proof.sh + scripts/lib/impact_report.py",
        "scenario": {"type": "consumer_lag", "messages": messages, "rate": rate},
        "instants": {"t0": t0, "pendant_1": pendant1,
                     "pendant_2": pendant2, "final": final,
                     "sql_pre_delta": sql_pre_delta},
        "checks": checks,
        "checks_ok": all(checks.values()),
    }
    out = evid / "rapport-impact-metier.json"
    out.write_text(json.dumps(rapport, ensure_ascii=False, indent=1) + "\n",
                   encoding="utf-8")
    print("checks :", json.dumps(checks, ensure_ascii=False))
    print("checks_ok:", rapport["checks_ok"])
    print("rapport :", str(out))
    return 0 if rapport["checks_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
