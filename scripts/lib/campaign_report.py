#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rapport de la campagne d'incidents provoqués (SAÉ 5, phase 4).

Lit ``<campagne>/<cas>/{verdicts.jsonl,events.jsonl}`` et produit :
  - un tableau par cas : verdicts observés, cause nommée, délai de détection,
    faux positifs ;
  - une synthèse globale : justesse (critère #1), délais (critère #3),
    faux positifs sur témoins (critère #4), états « indéterminé » assumés ;
  - ``campaign-summary.json`` et ``SYNTHESE.md`` dans le répertoire de campagne.

Usage :
    python3 campaign_report.py <répertoire-de-campagne>
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

INCIDENT_FAMILIES = ("consumer_backlog", "slow_processing", "kafka_replication", "persistence")

#: famille attendue par cas (préfixe du nom de répertoire)
EXPECTED_BY_PREFIX: List[Tuple[str, Optional[str]]] = [
    ("temoin-", None),
    ("A-retard-lecture", "consumer_backlog"),
    ("B-replication", "kafka_replication"),
    ("C-postgres-indispo", "persistence"),
    ("C-postgres-degrade", "persistence"),
    ("D-traitement-lent", "slow_processing"),
]

EVENT_DELAY_BASE = "fault_injected"   # sinon scenario_ready


def expected_family(case_name: str) -> Optional[str]:
    for prefix, family in EXPECTED_BY_PREFIX:
        if case_name.startswith(prefix):
            return family
    return None


def coverage(analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Couverture « problèmes étudiés » : chaque famille d'incident doit avoir
    été VUE dans les verdicts d'un cas qui l'attend — pas seulement lancée.

    Un run partiel (``--cases``) ne doit pas ressembler à une campagne complète :
    la synthèse affiche ce qui manque et ``complete`` passe à False. Le témoin
    (0 faux positif) et l'indéterminé explicite sont rapportés en complément.
    """
    per_family: Dict[str, List[Dict[str, Any]]] = {}
    for a in analyses:
        fam = a.get("expected_family")
        if not fam:
            continue
        counts = a.get("verdict_counts") or {}
        per_family.setdefault(fam, []).append({
            "case": a["case"],
            "vue": fam in counts,
            "verdicts": counts.get(fam, 0),
        })
    rows = []
    for fam in INCIDENT_FAMILIES:
        attendus = per_family.get(fam) or []
        rows.append({
            "family": fam,
            "cases": attendus,
            "exerciced": any(e["vue"] for e in attendus),
        })
    ok = sum(1 for r in rows if r["exerciced"])
    witnesses = [a for a in analyses if a.get("is_witness")]
    return {
        "families": rows,
        "families_exerciced": "%d/%d" % (ok, len(INCIDENT_FAMILIES)),
        "complete": ok == len(INCIDENT_FAMILIES),
        "temoin_zero_fp": any(a.get("false_positives", 0) == 0
                              and a.get("reachable", 0) > 0 for a in witnesses),
        "indetermine_explicite": any("indeterminate" in (a.get("verdict_counts") or {})
                                     for a in analyses),
    }


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return records


def verdict_family(verdict: Dict[str, Any]) -> Optional[str]:
    value = verdict.get("verdict")
    return value if isinstance(value, str) else None


def case_events(case_dir: str) -> Dict[str, int]:
    events: Dict[str, int] = {}
    for record in load_jsonl(os.path.join(case_dir, "events.jsonl")):
        event = record.get("event")
        if isinstance(event, str) and "t_epoch" in record:
            events[event] = int(record["t_epoch"])
    return events


def analyse_case(root: str, case_name: str) -> Dict[str, Any]:
    case_dir = os.path.join(root, case_name)
    verdicts = load_jsonl(os.path.join(case_dir, "verdicts.jsonl"))
    events = case_events(case_dir)
    expected = expected_family(case_name)
    is_witness = expected is None

    reachable = [r for r in verdicts if "verdict" in r and isinstance(r["verdict"], dict)]
    unreachable = [r for r in verdicts if r.get("error") == "unreachable"]
    cli_failed = [r for r in verdicts if r.get("verdict_error")]

    families: List[str] = []
    reasons: List[str] = []
    for record in reachable:
        verdict = record["verdict"]
        family = verdict_family(verdict)
        if family:
            families.append(family)
            if family == "indeterminate":
                reason = verdict.get("indeterminate_reason")
                if reason:
                    reasons.append(reason)

    counts = {f: families.count(f) for f in set(families)}

    result: Dict[str, Any] = {
        "case": case_name,
        "expected_family": expected,
        "is_witness": is_witness,
        "samples": len(verdicts),
        "reachable": len(reachable),
        "unreachable": len(unreachable),
        "verdict_counts": dict(sorted(counts.items())),
        "indeterminate_reasons": sorted(set(reasons)),
        "cli_failed": len(cli_failed),
    }

    # --- cas témoin : faux positifs ----------------------------------------
    if is_witness:
        fp_families = [f for f in families if f in INCIDENT_FAMILIES]
        result["false_positives"] = len(fp_families)
        result["pass"] = (len(fp_families) == 0)
        result["fp_verdicts"] = fp_families
        return result

    # --- cas incident : conclusion attendue -------------------------------
    first_conclusive: Optional[Dict[str, Any]] = None
    needs_second_before = 0
    for record in reachable:
        verdict = record["verdict"]
        family = verdict_family(verdict)
        if family == "indeterminate" and verdict.get("indeterminate_reason") in (
                "needs_second_sample", "data_missing", "probes_stale"):
            if verdict.get("indeterminate_reason") == "needs_second_sample":
                needs_second_before += 1
        elif family == expected:
            first_conclusive = record
            break

    result["conclusive"] = first_conclusive is not None
    result["needs_second_sample_before"] = needs_second_before
    if first_conclusive is not None:
        verdict = first_conclusive["verdict"]
        result["cause"] = verdict.get("cause")
        result["cause_label"] = verdict.get("cause_label")
        result["probability"] = verdict.get("probability")
        result["criticality"] = verdict.get("criticality")
        base = events.get("fault_injected") or events.get("scenario_ready")
        if base is not None:
            result["detection_delay_s"] = round(first_conclusive["t_epoch"] - base, 1)
        result["detected_at"] = first_conclusive["t_epoch"]
    result["pass"] = bool(first_conclusive is not None and result.get("detection_delay_s", 999) <= 30)
    return result


def write_reports(root: str, analyses: List[Dict[str, Any]]) -> None:
    families_ok = sum(1 for a in analyses if not a.get("is_witness") and a.get("conclusive"))
    incidents_total = sum(1 for a in analyses if not a.get("is_witness"))
    fp_total = sum(a.get("false_positives", 0) for a in analyses if a.get("is_witness"))
    witnesses = [a for a in analyses if a.get("is_witness")]
    delays = [a["detection_delay_s"] for a in analyses
              if not a.get("is_witness") and a.get("detection_delay_s") is not None]
    cov = coverage(analyses)

    summary = {
        "justesse": "%d/%d" % (families_ok, incidents_total),
        "faux_positifs_temoins": "%d (sur %d témoins, %d échantillons)" % (
            fp_total, len(witnesses), sum(a["samples"] for a in witnesses)),
        "delai_detection": {
            "par_cas": {a["case"]: a.get("detection_delay_s")
                        for a in analyses if a.get("detection_delay_s") is not None},
            "max_s": max(delays) if delays else None,
            "garantie": "<= 30 s",
        },
        "critere1_100_pourcent": families_ok == incidents_total,
        "critere3_delai_30s": all(d is not None and d <= 30 for d in delays) if delays else False,
        "critere4_zero_fp": fp_total == 0,
        "couverture": cov,
        "couverture_familles": cov["complete"],
        "cases": analyses,
    }
    with open(os.path.join(root, "campaign-summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=1)

    lines: List[str] = []
    lines.append("# Campagne d'incidents provoqués — synthèse\n")
    lines.append("| Critère | Résultat |")
    lines.append("|---|---|")
    lines.append("| #1 justesse (%d familles) | %s |" % (len(INCIDENT_FAMILIES), summary["justesse"]))
    lines.append("| Couverture familles (problèmes étudiés) | %s |" % cov["families_exerciced"])
    lines.append("| #4 faux positifs (témoin) | %s |" % summary["faux_positifs_temoins"])
    lines.append("| #3 délai max | %s |" % summary["delai_detection"]["max_s"])
    lines.append("")
    lines.append("| Cas | Échant. | Verdicts | Cause nommée | Délai | PASS |")
    lines.append("|---|---|---|---|---|---|")
    for a in analyses:
        verdicts_txt = ", ".join("%s×%d" % (k, v) for k, v in sorted(a["verdict_counts"].items())) or "—"
        if a.get("is_witness"):
            cause_txt = "FP=%d" % a.get("false_positives", 0)
            delay_txt = "—"
        else:
            cause_txt = str(a.get("cause") or "—")
            delay_txt = ("%.1f s" % a["detection_delay_s"]
                         if a.get("detection_delay_s") is not None else "—")
        lines.append("| %s | %d | %s | %s | %s | %s |" % (
            a["case"], a.get("reachable", 0), verdicts_txt, cause_txt, delay_txt,
            "✅" if a.get("pass") else "❌"))
    lines.append("")
    for a in analyses:
        if a.get("indeterminate_reasons"):
            lines.append("- %s : états indéterminés = %s" % (
                a["case"], ", ".join(a["indeterminate_reasons"])))

    lines.append("")
    lines.append("## Couverture des problèmes étudiés\n")
    lines.append("| Famille | Cas qui l'attendent | Statut |")
    lines.append("|---|---|---|")
    for row in cov["families"]:
        cases_txt = ", ".join(
            "%s (%s×%d)" % (e["case"], row["family"], e["verdicts"])
            if e["vue"] else "%s (famille absente des verdicts)" % e["case"]
            for e in row["cases"]) or "aucun cas attendu"
        lines.append("| `%s` | %s | %s |" % (
            row["family"], cases_txt, "✅" if row["exerciced"] else "❌"))
    lines.append("")
    lines.append("- témoin nominal (0 faux positif) : %s" % (
        "✅" if cov["temoin_zero_fp"] else "❌ (aucun témoin exploitable)"))
    lines.append("- verdicts indéterminés explicites : %s" % (
        "✅" if cov["indetermine_explicite"] else "— (aucun)"))
    with open(os.path.join(root, "SYNTHESE.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print("\n".join(lines))


def main() -> int:
    if len(sys.argv) < 2:
        print("usage : campaign_report.py <répertoire-de-campagne>", file=sys.stderr)
        return 2
    root = sys.argv[1]
    if not os.path.isdir(root):
        print("répertoire introuvable : %s" % root, file=sys.stderr)
        return 2
    case_names = sorted(
        d for d in os.listdir(root)
        if os.path.isdir(os.path.join(root, d)) and os.path.isfile(os.path.join(root, d, "verdicts.jsonl"))
    )
    analyses = [analyse_case(root, name) for name in case_names]
    if not analyses:
        print("aucun cas trouvé (verdicts.jsonl) dans %s" % root, file=sys.stderr)
        return 1
    write_reports(root, analyses)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
