#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Démonstration des angles morts du diagnostic Simul-Pix (SAÉ 5).

Rejoue les cas SANS cause nommée du catalogue d'incidents
(`travail/notes/cas-incidents.md` : §1 B3/B4/C3/C4, §5 registre des risques) :
chaque cas fabrique des payloads ``/health/details`` synthétiques — sondes
FRAÎCHES, c'est le point — et les passe au vrai moteur
(``diagnose_engine.build_verdict``). Le verdict affiché est calculé à
l'exécution, jamais codé en dur : la limite est DÉMONTRÉE, pas déclarée.

Deux limites structurantes, à dire telles quelles en soutenance :
  - ce que le payload ne remonte pas est invisible (connexions PG saturées,
    contrôleur perdu sans URP, offset non initialisé) ;
  - une sonde qui MENT en restant fraîche n'est pas détectée : le garde-fou
    fraîcheur (critère #6) ne voit que le PÉRIMÉ — la vérification indépendante
    tp-kafka.sh / tp-db.sh reste le dernier rempart (imprimée dans chaque verdict).

Usage :
    python3 blind_spots.py                 tous les cas + récapitulatif
    python3 blind_spots.py --case B4a      un seul cas (id exact ou préfixe)
    python3 blind_spots.py --json          verdicts bruts (machine)
    python3 blind_spots.py --export        pousse les états SIMULÉS sur la frise
                                           Grafana (métrique simulpix_diagnostic,
                                           protocole de l'exporteur réutilisé —
                                           le dashboard n'est PAS modifié)
    python3 blind_spots.py --print         affiche les lignes InfluxDB sans envoyer
    scripts/demo-angles-morts.sh           enrobage CLI recommandé

Variables d'export : SIMULPIX_INFLUXDB_URL/_ORG/_BUCKET/_TOKEN (idem exporteur),
                     SIMULPIX_BLIND_HOLD_S (secondes entre deux états, défaut 5 :
                     chaque segment doit rester lisible sur la frise).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import diagnostic_export  # noqa: E402
from diagnose_engine import build_verdict  # noqa: E402

FAMILY_LABELS = {
    "healthy": "healthy",
    "consumer_backlog": "consumer_backlog",
    "slow_processing": "slow_processing",
    "kafka_replication": "kafka_replication",
    "persistence": "persistence",
    "indeterminate": "indeterminate",
}


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _env_float(name: str, default: float) -> float:
    """Flottant lu dans l'environnement, sinon la valeur par défaut."""
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _brokers(urp: float = 0.0, offline: float = 0.0, controller: float = 1.0,
             count: int = 3) -> List[Dict[str, Any]]:
    """Instantané réplication : RF=3 par défaut, chacun contrôleur actif."""
    return [{"broker": "kafka-%d" % i,
             "under_replicated_partitions": urp,
             "offline_partitions_count": offline,
             "active_controller_count": controller}
            for i in range(1, count + 1)]


def _payload(now: float, *, counts: Optional[Dict[str, Any]] = None,
             gaps: Optional[Dict[str, Any]] = None,
             timings: Optional[Dict[str, Any]] = None,
             rates: Optional[Dict[str, Any]] = None,
             groups: Optional[Dict[str, Any]] = None,
             replication: Optional[Dict[str, Any]] = None,
             postgres_status: str = "ok",
             services: Optional[Dict[str, str]] = None,
             sla: float = 10.0, status: str = "ok",
             alerts: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    """Payload nominal frais (flux actif, tout à zéro) + surcharges par cas."""
    services_default = {
        "generator": "ok", "pix-validator": "ok", "pix-decision-engine": "ok",
        "pix-outcome-publisher": "ok", "persister-valid": "ok",
        "persister-rejected": "ok", "postgres": "ok", "metrics-collector": "ok",
    }
    payload: Dict[str, Any] = {
        "status": status,
        "timestamp": _iso(now - 2),
        "metrics": {
            "counts": {
                "generated": 500, "validated": 420, "processed": 420,
                "pending_result_count": 0,
                "db_validated_count": 400, "db_rejected_count": 20,
                "validated_sla_breaches": 0, "rejected_sla_breaches": 0,
                "broker_count": 3,
            },
            "gaps": {"persistence_gap_valid": 0, "persistence_gap_rejected": 0},
            "timings": {"avg_validation_latency_ms": 1200,
                        "seconds_since_last_validated": 0.5},
            "rates": {"generated_per_second": 10.0,
                      "validated_per_second": 10.0,
                      "persisted_valid_per_second": 9.5},
        },
        "details": {
            "kafka": {
                "consumer_groups": {
                    "pix-validator": {"lag_total": 0},
                    "pix-decision-engine": {"lag_total": 0},
                    "pix-outcome-publisher": {"lag_total": 0},
                    "persister-valid": {"lag_total": 0},
                    "persister-rejected": {"lag_total": 0},
                },
                "replication": {
                    "under_replicated_partitions_total": 0,
                    "captured_at": _iso(now - 3),
                    "brokers": _brokers(),
                },
            },
            "postgres": {"status": postgres_status},
            "network": {"active": 0, "profile": None},
        },
        "ui": {"decision_sla_seconds": sla},
        "services": dict(services_default if services is None else services),
        "alerts": alerts or [],
    }
    if counts:
        payload["metrics"]["counts"].update(counts)
    if gaps:
        payload["metrics"]["gaps"].update(gaps)
    if timings:
        payload["metrics"]["timings"].update(timings)
    if rates:
        payload["metrics"]["rates"].update(rates)
    if groups:
        payload["details"]["kafka"]["consumer_groups"].update(groups)
    if replication:
        replication = dict(replication)
        replication.setdefault("captured_at", _iso(now - 3))
        payload["details"]["kafka"]["replication"] = replication
    return payload


def _signal_broker_controller(urp: float) -> str:
    return ("3 brokers présents, active_controller_count total = 0, URP = %g "
            "— sondes fraîches" % urp)


def _steps_b4a(now: float) -> List[Dict[str, Any]]:
    return [{"label": "lecture unique", "payload": _payload(
        now, replication={"under_replicated_partitions_total": 0,
                          "brokers": _brokers(controller=0)})}]


def _steps_b4b(now: float) -> List[Dict[str, Any]]:
    return [{"label": "lecture unique", "payload": _payload(
        now, replication={"under_replicated_partitions_total": 3,
                          "brokers": _brokers(urp=3, controller=0)})}]


def _steps_b3(now: float) -> List[Dict[str, Any]]:
    pic = _payload(now, replication={"under_replicated_partitions_total": 3,
                                     "brokers": _brokers(urp=3)})
    resorption = _payload(now, counts={"generated": 530, "validated": 450,
                                       "processed": 450,
                                       "db_validated_count": 412},
                          replication={
        "under_replicated_partitions_total": 0, "brokers": _brokers()})
    return [{"label": "lecture 1 — pic d'URP", "payload": pic},
            {"label": "lecture 2 — résorption", "payload": resorption}]


def _steps_c3a(now: float) -> List[Dict[str, Any]]:
    ancre = _payload(now, counts={"db_validated_count": 400},
                     gaps={"persistence_gap_valid": 2})
    deux = _payload(now, counts={"db_validated_count": 402},
                    gaps={"persistence_gap_valid": 2})
    return [{"label": "lecture 1 (ancre)", "payload": ancre},
            {"label": "lecture 2 (span 12 s)", "payload": deux,
             "previous": ancre}]


def _steps_c3b(now: float) -> List[Dict[str, Any]]:
    ancre = _payload(now, counts={"db_validated_count": 400},
                     gaps={"persistence_gap_valid": 2},
                     groups={"persister-valid": {"lag_total": 2},
                             "persister-rejected": {"lag_total": 2}})
    deux = _payload(now, counts={"db_validated_count": 405},
                    gaps={"persistence_gap_valid": 20},
                    groups={"persister-valid": {"lag_total": 15},
                            "persister-rejected": {"lag_total": 15}},
                    rates={"persisted_valid_per_second": 3.0})
    return [{"label": "lecture 1 (ancre)", "payload": ancre},
            {"label": "lecture 2 (span 12 s)", "payload": deux,
             "previous": ancre}]


def _steps_c4a(now: float) -> List[Dict[str, Any]]:
    ancre = _payload(now, counts={"generated": 500, "db_validated_count": 400,
                                  "db_rejected_count": 20})
    deux = _payload(now, counts={"generated": 530, "db_validated_count": 400,
                                 "db_rejected_count": 20, "pending_result_count": 2},
                    groups={"pix-validator": {"lag_total": 2}})
    return [{"label": "lecture 1 (ancre)", "payload": ancre},
            {"label": "lecture 2 (span 12 s)", "payload": deux,
             "previous": ancre}]


def _steps_c4b(now: float) -> List[Dict[str, Any]]:
    ancre = _payload(now, counts={"db_validated_count": 400})
    deux = _payload(now, counts={"db_validated_count": 405})
    return [{"label": "lecture 1 (ancre)", "payload": ancre},
            {"label": "lecture 2 (span 12 s)", "payload": deux,
             "previous": ancre}]


def _steps_a0(now: float) -> List[Dict[str, Any]]:
    return [{"label": "lecture unique", "payload": _payload(
        now, groups={"pix-outcome-publisher": {"lag_total": None}})}]


def _steps_t0(now: float) -> List[Dict[str, Any]]:
    services = {"generator": "ok", "pix-validator": "ok",
                "pix-decision-engine": "ok", "pix-outcome-publisher": "ok",
                "persister-valid": "ok", "persister-rejected": "ok",
                "postgres": "ok", "metrics-collector": "down"}
    return [{"label": "lecture unique", "payload": _payload(now, services=services)}]


CASES: List[Dict[str, Any]] = [
    {
        "id": "B4a", "source": "§1 (B4)",
        "title": "Contrôleur perdu, sans URP",
        "signal": _signal_broker_controller(0),
        "limit": ("le problème du cluster lui-même est invisible : sans URP, "
                  "aucune règle ne regarde active_controller_count — pas de "
                  "cause nommée pour B4 (registre des risques §5)"),
        "outcome": "❌ angle mort",
        "expected": {"verdict": "healthy", "cause": None},
        "steps": _steps_b4a,
    },
    {
        "id": "B4b", "source": "§1 (B4, contraste)",
        "title": "Contrôleur perdu AVEC URP",
        "signal": _signal_broker_controller(3),
        "limit": ("couvert — mais seulement parce que URP l'accompagne : le "
                  "chemin grave (broker_down) exige un symptôme visible"),
        "outcome": "✅ couvert (contraste)",
        "expected": {"verdict": "kafka_replication", "cause": "broker_down"},
        "steps": _steps_b4b,
    },
    {
        "id": "B3", "source": "§1 (B3)",
        "title": "Rééquilibrage de partitions (pic d'URP passager)",
        "signal": "URP 3 → 0 entre 2 lectures, brokers tous opérationnels",
        "limit": ("la famille est juste (bénin, se résorbe) mais la cause fine "
                  "n'est pas nommée : rééquilibrage et copie lente sont couverts "
                  "par la même cause générique replication_catchup"),
        "outcome": "⚠️ cause fine absente",
        "expected": {"verdict": "kafka_replication", "cause": "replication_catchup"},
        "steps": _steps_b3,
    },
    {
        "id": "C3a", "source": "§1 (C3)",
        "title": "PostgreSQL saturé — écritures lentes qui passent",
        "signal": ("services.postgres = ok (sonde fraîche), écart de persistance "
                   "stable à 2, écritures qui progressent lentement"),
        "limit": ("invisible : la sonde PostgreSQL du socle ne remonte pas les "
                  "connexions actives — la saturation légère reste sous le seuil "
                  "de toute règle de la matrice"),
        "outcome": "❌ angle mort",
        "expected": {"verdict": "healthy", "cause": None},
        "steps": _steps_c3a,
    },
    {
        "id": "C3b", "source": "§1 (C3, contraste)",
        "title": "PostgreSQL saturé — l'écart S'ACCROÎT",
        "signal": ("services.postgres = ok, écart de persistance 2 → 20 entre 2 "
                   "lectures, lag persisters = 15"),
        "limit": ("la cause nommée est « persister lent / base lente », pas "
                  "« PostgreSQL saturé » : impossible de distinguer saturation "
                  "et lenteur depuis le payload"),
        "outcome": "⚠️ cause voisine",
        "expected": {"verdict": "persistence", "cause": "persister_slow"},
        "steps": _steps_c3b,
    },
    {
        "id": "C4a", "source": "§1 (C4)",
        "title": "Écriture échouée silencieuse (service « ok »)",
        "signal": ("conteneurs ok, services.postgres = ok, db_validated_count "
                   "FIGÉ (400 → 400) alors que generated monte (+30)"),
        "limit": ("le moteur refuse de conclure (contradictory_signals) — c'est "
                  "le contrat — mais la cause « écriture échouée » n'est PAS "
                  "nommée : c'est au praticien d'aller voir tp-db.sh"),
        "outcome": "⚠️ refus explicite, cause non nommée",
        "expected": {"verdict": "indeterminate",
                     "reason": "contradictory_signals"},
        "steps": _steps_c4a,
    },
    {
        "id": "C4b", "source": "§1 (C4) · §9 note de conception",
        "title": "Sonde qui MENT en restant fraîche",
        "signal": ("services.postgres = ok, db qui monte normalement (la sonde "
                   "remonte des valeurs saines) — en réalité les écritures "
                   "échouent : tout le payload est faux mais FRAIS"),
        "limit": ("LA limite majeure : le garde-fou fraîcheur (critère #6) ne "
                  "voit que le PÉRIMÉ, jamais le mensonge — une sonde qui ment "
                  "en restant fraîche produit un healthy erroné ; seul le "
                  "croisement indépendant (tp-db.sh) peut la prendre en défaut"),
        "outcome": "❌ sonde fraîche qui ment : non détectée",
        "expected": {"verdict": "healthy", "cause": None},
        "steps": _steps_c4b,
    },
    {
        "id": "A0", "source": "§5 (registre des risques)",
        "title": "Offset non initialisé ≠ retard",
        "signal": ("pix-outcome-publisher : lag_total absent (offset jamais "
                   "commité) — le groupe ne lit pas encore, le payload ne "
                   "remonte aucune valeur"),
        "limit": ("l'absence de lecture est invisible : le moteur lit 0 lag "
                  "quand la valeur manque — il ne peut pas distinguer « pas de "
                  "retard » de « pas de mesure »"),
        "outcome": "❌ lecture absente = invisible",
        "expected": {"verdict": "healthy", "cause": None},
        "steps": _steps_a0,
    },
    {
        "id": "T0", "source": "§5 (registre des risques)",
        "title": "Service transversal down (météo « degraded » trop grossière)",
        "signal": "metrics-collector = down — service hors pipeline (aucune famille)",
        "limit": ("témoin sans incident pipeline nommé : verdict "
                  "service_down_transversal (indéterminé explicite) — assumé, "
                  "le moteur n'invente pas de famille"),
        "outcome": "⚠️ assumé (témoin, pas un incident)",
        "expected": {"verdict": "indeterminate",
                     "reason": "service_down_transversal"},
        "steps": _steps_t0,
    },
]


def run_case(case: Dict[str, Any], now: float) -> Dict[str, Any]:
    """Exécute les étapes d'un cas : verdicts calculés par le VRAI moteur."""
    steps: List[Dict[str, Any]] = []
    previous: Optional[Dict[str, Any]] = None
    for index, step in enumerate(case["steps"](now), start=1):
        verdict = build_verdict(step["payload"], now=now, previous=previous)
        steps.append({"label": step["label"], "verdict": verdict,
                      "signal": step.get("signal")})
        previous = {"wall": now - 12, "payload": step["payload"]}
    return {
        "id": case["id"], "title": case["title"], "source": case["source"],
        "outcome": case["outcome"], "limit": case["limit"],
        "signal": case["signal"],
        "steps": steps,
        "final": steps[-1]["verdict"],
    }


def _short(verdict: Dict[str, Any]) -> str:
    parts = ["%s" % FAMILY_LABELS.get(verdict.get("verdict"), verdict.get("verdict"))]
    if verdict.get("cause"):
        parts.append("cause=%s" % verdict["cause"])
    if verdict.get("indeterminate_reason"):
        parts.append("motif=%s" % verdict["indeterminate_reason"])
    based = verdict.get("based_on") or {}
    parts.append("%s échantillon(s)" % based.get("samples", 1))
    if based.get("span_s"):
        parts.append("span=%ss" % based["span_s"])
    return " · ".join(parts)


def render_case(case: Dict[str, Any]) -> str:
    bar = "─" * 64
    lines = [bar, "%s · %s  [%s]" % (case["id"], case["title"], case["source"])]
    lines.append("Signal vu  : %s" % case["signal"])
    last = case["steps"][-1]["verdict"]
    if last.get("summary"):
        lines.append("Résumé     : %s" % last["summary"].replace("\n", " ").strip())
    for step in case["steps"]:
        lines.append("Moteur (%s) : %s" % (step["label"], _short(step["verdict"])))
    lines.append("Limite     : %s" % case["limit"])
    lines.append("Statut     : %s" % case["outcome"])
    return "\n".join(lines)


PROVEN = ("A2 retard de lecture · A4 persister lent · B2 copie lente · "
          "C1 base lente · C2 PG indisponible")


def render_summary(runs: List[Dict[str, Any]]) -> str:
    lines = ["═" * 64,
             "RÉCAPITULATIF — ce que la garantie couvre / ne couvre pas",
             "═" * 64,
             "Cas prouvés en campagne (preuves archivées phase 4/5/7) : %s" % PROVEN,
             "  → justesse 4/4 · délai max 25 s · 0/70 faux positifs (5 témoins)",
             "",
             "Angles morts simulés (sondes fraîches, verdicts calculés par le moteur) :",
             "%-4s %-46s %-26s %s" % ("cas", "verdict moteur", "cause / motif", "statut")]
    for run in runs:
        verdicts = " → ".join(_short(s["verdict"]).split(" · ")[0]
                              for s in run["steps"])
        last = run["final"]
        cause = last.get("cause") or last.get("indeterminate_reason") or "—"
        lines.append("%-4s %-46s %-26s %s" % (run["id"], verdicts, cause,
                                              run["outcome"]))
    lines += ["",
              "Ne garantit pas (exprimé ci-dessus, pas déclaré) :",
              "  - ce que le payload ne remonte pas (connexions PG, contrôleurs "
              "sans URP, offsets absents) ;",
              "  - les causes fines au-delà des familles (B3 rééquilibrage, "
              "C3 saturation) ;",
              "  - une sonde qui MENT en restant fraîche (C4b) : le garde-fou "
              "fraîcheur ne voit que le PÉRIMÉ.",
              "Dernier rempart : la vérification indépendante imprimée dans "
              "chaque verdict (next_check → ./scripts/tp-kafka.sh, "
              "./scripts/tp-db.sh)."]
    return "\n".join(lines)


def _match(cases: List[Dict[str, Any]], wanted: str) -> List[Dict[str, Any]]:
    wanted = wanted.upper()
    for case in cases:
        if case["id"] == wanted:
            return [case]
    matches = [case for case in cases if case["id"].startswith(wanted)]
    if not matches:
        raise SystemExit("cas inconnu : %s (attendus : %s)" % (
            wanted, ", ".join(case["id"] for case in cases)))
    return matches


def collect_lines(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Un item par étape exécutée (14 au total, pas seulement les verdicts finaux).

    L'ordre est celui des cas puis des étapes : la frise montre les transitions
    dans l'ordre du catalogue. La ligne InfluxDB est produite par le protocole
    de l'exporteur (``diagnostic_export.verdict_to_line``, métrique
    ``simulpix_diagnostic``) ; à l'export, elle est reconstruite au moment du
    push pour que le timestamp espace les segments sur la frise.
    """
    items: List[Dict[str, Any]] = []
    for run in runs:
        for step in run["steps"]:
            items.append({"case": run["id"], "label": step["label"],
                          "verdict": step["verdict"],
                          "line": diagnostic_export.verdict_to_line(step["verdict"])})
    return items


def export_to_grafana(items: List[Dict[str, Any]], hold_s: float, url: str,
                      org: str, bucket: str, token: str) -> int:
    """Pousse chaque état sur la frise Grafana (métrique simulpix_diagnostic).

    Timestamp = moment du push, hold_s entre deux états (pas après le
    dernier) : chaque segment reste lisible sur la frise. Abandon à la
    première écriture en échec ; retourne le nombre d'états poussés.
    """
    pushed = 0
    for index, item in enumerate(items):
        if index and hold_s > 0:
            time.sleep(hold_s)
        line = diagnostic_export.verdict_to_line(item["verdict"])
        if line is None or not diagnostic_export.send_line(line, url, org, bucket, token):
            print("arrêt à l'état %d/%d (%s %s) : écriture InfluxDB en échec"
                  % (index + 1, len(items), item["case"], item["label"]),
                  file=sys.stderr)
            break
        pushed += 1
        print("%2d/%d  %-4s %-26s %s" % (
            pushed, len(items), item["case"], item["label"],
            FAMILY_LABELS.get(item["verdict"].get("verdict"), "?")))
    return pushed


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Démonstration des angles morts du diagnostic Simul-Pix")
    parser.add_argument("--case", help="un cas (B4a, B4, B3, C3, C4, A0, T0)")
    parser.add_argument("--json", action="store_true", help="sortie JSON brute")
    parser.add_argument("--export", action="store_true",
                        help="pousse les états SIMULÉS sur la frise Grafana")
    parser.add_argument("--print", dest="print_lines", action="store_true",
                        help="affiche les lignes InfluxDB sans envoyer")
    parser.add_argument("--hold", type=float,
                        default=_env_float("SIMULPIX_BLIND_HOLD_S", 5.0),
                        help="secondes entre deux états poussés (défaut 5)")
    args = parser.parse_args(argv)

    cases = CASES if not args.case else _match(CASES, args.case)
    now = time.time()
    runs = [run_case(case, now) for case in cases]

    if args.json:
        print(json.dumps({"schema": "simulpix-blind-spots/1",
                          "generated_at": _iso(now),
                          "cases": runs,
                          "proven_in_campaign": PROVEN},
                         ensure_ascii=False, indent=1))
        return 0

    if args.print_lines:
        for item in collect_lines(runs):
            print(item["line"])
        return 0

    if args.export:
        items = collect_lines(runs)
        print("états SIMULÉS (angles morts, sondes fraîches) : %d états, "
              "hold %.0f s — frise : http://localhost:3000/d/simulpix-diagnostic"
              % (len(items), args.hold))
        pushed = export_to_grafana(
            items, args.hold,
            os.environ.get("SIMULPIX_INFLUXDB_URL", diagnostic_export.DEFAULT_URL),
            os.environ.get("SIMULPIX_INFLUXDB_ORG", diagnostic_export.DEFAULT_ORG),
            os.environ.get("SIMULPIX_INFLUXDB_BUCKET", diagnostic_export.DEFAULT_BUCKET),
            os.environ.get("SIMULPIX_INFLUXDB_TOKEN", diagnostic_export.DEFAULT_TOKEN))
        print("%d/%d états poussés sur %s" % (
            pushed, len(items), diagnostic_export.MEASUREMENT))
        return 0 if pushed == len(items) else 1

    for index, run in enumerate(runs):
        if index:
            print()
        print(render_case(run))
    if not args.case or len(runs) > 1:
        print()
        print(render_summary(runs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
