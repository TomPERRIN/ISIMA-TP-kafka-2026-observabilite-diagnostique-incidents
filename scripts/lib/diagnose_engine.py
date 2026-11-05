#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Moteur de diagnostic Simul-Pix (SAÉ 5 · observabilité & diagnostic d'incidents).

Ce module CODE la matrice symptômes/causes v1
(`travail/notes/matrice-symptomes-causes.md`) : chaque ligne de la matrice devient
une règle de croisement, avec le discriminant qui tranche entre les deux causes
et une vérification indépendante (tp-kafka.sh / tp-db.sh).

Correspondance matrice -> règles du moteur :
  ligne 1  file d'attente grossit (pending_result_count / lag)
             ->  reader_backlog  (prob. Élevée, crit. Moyenne, 1re)
             |  persister_slow   (prob. Moyenne, crit. Élevée, 2e) [lag persisters + écart grossit]
  ligne 2  URP > 0
             ->  broker_down          (prob. Faible, crit. Élevée, 1re à examiner)
             |  replication_catchup   (prob. Élevée, crit. Faible, 2e)
  ligne 3  plus aucune écriture en base
             ->  upstream_stopped_normal  (prob. Élevée, crit. Faible, 1re) : fin de flux NORMALE
             |  postgres_unavailable      (prob. Moyenne, crit. Critique, 2e)
  ligne 4  validated_sla_breaches en hausse
             ->  upstream_backlog_sla  (prob. Élevée, crit. Moyenne, 1re)
             |  sla_config             (prob. Faible, crit. Moyenne, 2e) : file vide + latence > SLA
  ligne 5  verdict global degraded
             ->  service_down:<nom>    (crit. selon service, 1re)
             |  probe_blinded          (sonde aveuglée par une perturbation réseau, 2e)

Garanties de la mission implémentées :
  - critère #6 (fraîcheur) : sondes périmées -> INDÉTERMINÉ (probes_stale), jamais une conclusion ;
  - critère #5 (double lecture) : toute conclusion ≠ indéterminé repose sur les lectures
    Kafka (lags par groupe, réplication/brokers) ET PostgreSQL (db counts, gaps, timings) ;
  - critère #4 : les deux causes d'un même symptôme sont automatiquement séparées
    (plausible_causes, triées dans l'ordre « priorité d'examen » de la matrice) ;
  - critère #1 : pas de confusion retard de consommateur / réplication dégradée /
    panne de persistance.

Entrée : le payload JSON de ``/health/details`` de service-health (aucune nouvelle
sonde : le moteur relie uniquement les signaux existants, cf. plan §4).

Règle anti-faux-positifs (témoin nominal) : un ``lag_total`` éclair est NOMINAL
(matrice : « lag_total = 1 passager est normal », écart de persistance léger en
début de flux), donc toute conclusion sur un compteur en hausse exige DEUX
échantillons espacés d'au moins ``min_sample_span_seconds`` (cache d'état tenu
par le CLI ``scripts/diagnose.sh``).

Usage minimal :
    from diagnose_engine import build_verdict
    verdict = build_verdict(payload, now=time.time())
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["SCHEMA_VERSION", "DEFAULT_CONFIG", "VERDICTS", "CAUSE_TABLE", "build_verdict"]

SCHEMA_VERSION = "simulpix-diagnostic/1"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


#: Seuils par défaut (lisibles depuis l'environnement, surchargeables par ``config``).
DEFAULT_CONFIG: Dict[str, Any] = {
    # garde-fou fraîcheur (critère #6) : cadence réelle du collecteur ~20 s (mesurée), marge 75 s
    "max_probe_age_seconds": _env_float("SIMULPIX_DIAG_MAX_PROBE_AGE_SECONDS", 75.0),
    # un lag_total < 10 est un rattrapage éclair NOMINAL (témoin nominal de la matrice)
    "lag_total_threshold": _env_float("SIMULPIX_DIAG_LAG_TOTAL_THRESHOLD", 10.0),
    # discrimination « l'écart qui S'ACCROÎT » : >= 5 entre deux lectures à ~10 s
    "gap_step": _env_float("SIMULPIX_DIAG_GAP_STEP", 5.0),
    # écart jugé « gros » (au-delà, l'écart ne peut plus être un simple début de flux)
    "big_gap_threshold": _env_float("SIMULPIX_DIAG_BIG_GAP_THRESHOLD", 20.0),
    # espacement minimal des 2 échantillons (compense les lags éclairs)
    "min_sample_span_seconds": _env_float("SIMULPIX_DIAG_MIN_SAMPLE_SPAN", 10.0),
    # compose nominal : kafka-1..kafka-3 (RF=3, min ISR=2)
    "expected_brokers": _env_int("SIMULPIX_DIAG_EXPECTED_BROKERS", 3),
    # marge latence moyenne vs SLA pour suspecter un scénario trop court
    "sla_latency_margin": _env_float("SIMULPIX_DIAG_SLA_LATENCY_MARGIN", 0.8),
    # un échantillon précédent plus vieux que ça n'est plus exploitable
    "previous_max_age_seconds": _env_float("SIMULPIX_DIAG_PREVIOUS_MAX_AGE_SECONDS", 900.0),
    # débit « flux au repos » (par seconde)
    "idle_rate_threshold": _env_float("SIMULPIX_DIAG_IDLE_RATE", 0.01),
}

VERDICTS = ("healthy", "consumer_backlog", "kafka_replication", "persistence", "indeterminate")

READER_GROUPS = ("pix-validator", "pix-decision-engine", "pix-outcome-publisher")
PERSISTER_GROUPS = ("persister-valid", "persister-rejected")

OK_SERVICE_STATUS = {"ok", "pass"}

#: familles des services suivis (matrice ligne 5) ; les autres sont « transversaux »
SERVICE_FAMILY = {
    "kafka": "kafka_replication",
    "postgres": "persistence",
    "persister-valid": "persistence",
    "persister-rejected": "persistence",
    "pix-validator": "consumer_backlog",
    "pix-decision-engine": "consumer_backlog",
    "pix-outcome-publisher": "consumer_backlog",
}

#: criticité assignée à un service tombé (matrice ligne 5 : persister down = élevée...)
SERVICE_CRITICALITY = {
    "postgres": "Critique",
    "persister-valid": "Élevée",
    "persister-rejected": "Élevée",
    "kafka": "Élevée",
    "pix-validator": "Moyenne",
    "pix-decision-engine": "Moyenne",
    "pix-outcome-publisher": "Moyenne",
}

#: ordre d'examen pour choisir le service dominant si plusieurs tombent
SERVICE_EXAM_ORDER = (
    "postgres",
    "persister-valid",
    "persister-rejected",
    "kafka",
    "pix-validator",
    "pix-decision-engine",
    "pix-outcome-publisher",
)

#: catalogue des causes nommées (libellés + classement probabilité/criticité/priorité de la matrice)
CAUSE_TABLE: Dict[str, Dict[str, Any]] = {
    "reader_backlog": {
        "family": "consumer_backlog",
        "label": "Retard de lecture applicatif (les applications qui lisent les topics sont en retard)",
        "probability": "Élevée",
        "criticality": "Moyenne",
        "exam_order": 0,
        "independent_check": "./scripts/tp-kafka.sh",
    },
    "persister_slow": {
        "family": "persistence",
        "label": "PostgreSQL ne suit plus le flux (persister lent / base lente)",
        "probability": "Moyenne",
        "criticality": "Élevée",
        "exam_order": 1,
        "independent_check": "./scripts/tp-kafka.sh puis ./scripts/tp-db.sh",
    },
    "broker_down": {
        "family": "kafka_replication",
        "label": "Broker arrêté (grave : RF=3, min ISR=2 -> un 2e incident rend les écritures impossibles)",
        "probability": "Faible",
        "criticality": "Élevée",
        "exam_order": 0,
        "independent_check": "./scripts/tp-kafka.sh",
    },
    "replication_catchup": {
        "family": "kafka_replication",
        "label": "Retard de copie entre brokers (bénin, passager : se résorbe seul)",
        "probability": "Élevée",
        "criticality": "Faible",
        "exam_order": 1,
        "independent_check": "./scripts/tp-kafka.sh (+ relire ~30 s après)",
    },
    "upstream_stopped_normal": {
        "family": "healthy",
        "label": "Amont arrêté (générateur terminé) : aucune écriture attendue, situation normale",
        "probability": "Élevée",
        "criticality": "Faible",
        "exam_order": 0,
        "independent_check": "./scripts/tp-db.sh",
    },
    "postgres_unavailable": {
        "family": "persistence",
        "label": "PostgreSQL indisponible/saturé (perte durable potentielle : à écarter en premier)",
        "probability": "Moyenne",
        "criticality": "Critique",
        "exam_order": 1,
        "independent_check": "./scripts/tp-db.sh",
    },
    "upstream_backlog_sla": {
        "family": "consumer_backlog",
        "label": "Retard de lecture en amont (la file a grossi : les paiements attendent et dépassent le TTL)",
        "probability": "Élevée",
        "criticality": "Moyenne",
        "exam_order": 0,
        "independent_check": "./scripts/tp-kafka.sh",
    },
    "sla_config": {
        "family": "consumer_backlog",
        "label": "Délai accordé trop court pour la charge (configuration du scénario, pas un incident de plateforme)",
        "probability": "Faible",
        "criticality": "Moyenne",
        "exam_order": 1,
        "independent_check": "comparer avg_validation_latency_ms au decision_sla_seconds du scénario",
    },
    "probe_blinded": {
        "family": "indeterminate",
        "label": "Sonde en échec à cause de la perturbation réseau active (faux positif possible : croiser avec la sonde indépendante)",
        "probability": "Moyenne",
        "criticality": "Faible",
        "exam_order": 1,
        "independent_check": "./scripts/tp-kafka.sh ou ./scripts/tp-db.sh, puis docker ps",
    },
    "probe_stale": {
        "family": "indeterminate",
        "label": "Sondes périmées : le collecteur ne rafraîchit plus ses mesures",
        "probability": "Moyenne",
        "criticality": "Faible",
        "exam_order": 0,
        "independent_check": "curl -s http://localhost:8082/health/details (fraîcheur), docker ps",
    },
}

RERUN_CMD = "./scripts/diagnose.sh --watch 15"
INDEPENDENT_CHECK_REMINDER = ("Vérification indépendante : ./scripts/tp-kafka.sh et / ou "
                              "./scripts/tp-db.sh")

# --------------------------------------------------------------------------- #
# utilitaires de lecture du payload
# --------------------------------------------------------------------------- #


def _num(value: Any) -> Optional[float]:
    """Convertis un champ JSON numérique (int/float/str numérique) en float, sinon None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _parse_iso(timestamp: Any) -> Optional[float]:
    """Timestamp ISO 8601 ('...Z', '...+00:00') -> epoch flottant, sinon None."""
    if not isinstance(timestamp, str) or not timestamp.strip():
        return None
    text = timestamp.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _iso_utc(epoch: float) -> str:
    """Epoch -> chaîne ISO UTC (pour sample_timestamp)."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _merge_config(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(DEFAULT_CONFIG)
    if config:
        merged.update({k: v for k, v in config.items() if v is not None})
    return merged


def _extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise les lectures brutes du payload /health/details (aucune décision métier)."""
    payload = payload or {}
    metrics = payload.get("metrics") or {}
    counts = metrics.get("counts") or {}
    gaps = metrics.get("gaps") or {}
    timings = metrics.get("timings") or {}
    rates = metrics.get("rates") or {}
    details = payload.get("details") or {}
    kafka = details.get("kafka") or {}
    groups = kafka.get("consumer_groups") or {}
    replication = kafka.get("replication") or {}
    brokers = replication.get("brokers") or []
    postgres = details.get("postgres") or {}
    network = details.get("network") or {}
    services = payload.get("services") or {}
    ui = payload.get("ui") or {}

    group_lags: Dict[str, Optional[float]] = {}
    for label in READER_GROUPS + PERSISTER_GROUPS:
        direct = groups.get(label) or {}
        group_lags[label] = _num(direct.get("lag_total"))

    return {
        "status": payload.get("status"),
        "timestamp": payload.get("timestamp"),
        "generated": _num(counts.get("generated")),
        "validated": _num(counts.get("validated")),
        "processed": _num(counts.get("processed")),
        "pending_result_count": _num(counts.get("pending_result_count")),
        "db_validated": _num(counts.get("db_validated_count")),
        "db_rejected": _num(counts.get("db_rejected_count")),
        "validated_sla_breaches": _num(counts.get("validated_sla_breaches")),
        "rejected_sla_breaches": _num(counts.get("rejected_sla_breaches")),
        "broker_count": _num(counts.get("broker_count")),
        "gap_valid": _num(gaps.get("persistence_gap_valid")),
        "gap_rejected": _num(gaps.get("persistence_gap_rejected")),
        "avg_validation_latency_ms": _num(timings.get("avg_validation_latency_ms")),
        "seconds_since_last_validated": _num(timings.get("seconds_since_last_validated")),
        "rates_validated": _num(rates.get("validated_per_second")),
        "rates_generated": _num(rates.get("generated_per_second")),
        "rates_persisted": _num(rates.get("persisted_valid_per_second")),
        "group_lags": group_lags,
        "proxy_group_lags": {
            "pix-validator": _num(counts.get("pix_validator_lag_total")),
            "pix-decision-engine": _num(counts.get("pix_decision_engine_lag_total")),
            "pix-outcome-publisher": _num(counts.get("pix_outcome_publisher_lag_total")),
            "persister-valid": _num(counts.get("persister_valid_lag_total")),
            "persister-rejected": _num(counts.get("persister_rejected_lag_total")),
        },
        "urp_total": _num(replication.get("under_replicated_partitions_total")),
        "saw_any_broker": bool(brokers),
        "brokers": [
            {
                "broker": str(broker.get("broker", "unknown")),
                "urp": _num(broker.get("under_replicated_partitions")) or 0.0,
                "offline": _num(broker.get("offline_partitions_count")) or 0.0,
                "active_controller": _num(broker.get("active_controller_count")) or 0.0,
            }
            for broker in brokers
        ],
        "captured_at": replication.get("captured_at"),
        "pg_status": postgres.get("status"),
        "services": services,
        "network_active": _num(network.get("active")) or 0.0,
        "network_profile": network.get("profile"),
        "sla_seconds": _num(ui.get("decision_sla_seconds")),
        "alerts": payload.get("alerts") or [],
    }


def _group_lag(s: Dict[str, Any], label: str) -> Optional[float]:
    """lag_total d'un groupe via details.kafka.consumer_groups, à défaut via metrics.counts."""
    value = (s.get("group_lags") or {}).get(label)
    if value is None:
        value = (s.get("proxy_group_lags") or {}).get(label)
    return value


def _max_lag(s: Dict[str, Any], labels) -> float:
    """Le plus grand lag (>= 0) parmi les groupes ; 0.0 si aucune lecture lisible."""
    values = [_group_lag(s, label) for label in labels]
    values = [v for v in values if v is not None]
    if not values:
        return 0.0
    return max(values)


def _gaps_total(s: Dict[str, Any]) -> Optional[float]:
    a, b = s.get("gap_valid"), s.get("gap_rejected")
    if a is None and b is None:
        return None
    return (a or 0.0) + (b or 0.0)


def _breaches_total(s: Dict[str, Any]) -> Optional[float]:
    a, b = s.get("validated_sla_breaches"), s.get("rejected_sla_breaches")
    if a is None and b is None:
        return None
    return (a or 0.0) + (b or 0.0)


def _db_total(s: Dict[str, Any]) -> Optional[float]:
    a, b = s.get("db_validated"), s.get("db_rejected")
    if a is None and b is None:
        return None
    return (a or 0.0) + (b or 0.0)


def _idle_now(s: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """Le flux est-il au repos à cet instant (taux ~0) ? False si aucun taux lisible."""
    eps = float(config["idle_rate_threshold"])
    rates = [s.get("rates_validated"), s.get("rates_generated"), s.get("rates_persisted")]
    rates = [r for r in rates if r is not None]
    if not rates:
        return False
    return max(rates) < eps


def _failing_services(s: Dict[str, Any]) -> List[str]:
    """Services dont la sonde n'est pas OK, triés du plus critique au moins critique."""
    services = s.get("services") or {}
    if not isinstance(services, dict):
        return []
    failing = [name for name, status in services.items()
               if isinstance(status, str) and status.lower() not in OK_SERVICE_STATUS]
    ordered = [name for name in SERVICE_EXAM_ORDER if name in failing]
    ordered.extend(sorted(name for name in failing if name not in SERVICE_EXAM_ORDER))
    return ordered


def _ev(source: str, metric: str, detail: str) -> Dict[str, str]:
    """Une preuve : source (kafka | postgres | platform), métrique lue, détail."""
    return {"source": source, "metric": metric, "detail": detail}


def _cause(cause_id: str, status: str, note: Optional[str] = None) -> Dict[str, Any]:
    """Entrée plausible_causes issue du catalogue (/status : confirmed|probable|excluded|hypothesis)."""
    spec = CAUSE_TABLE[cause_id]
    entry = {
        "cause": cause_id,
        "label": spec["label"],
        "family": spec["family"],
        "probability": spec["probability"],
        "criticality": spec["criticality"],
        "exam_order": spec["exam_order"],
        "status": status,
        "check": spec["independent_check"],
    }
    if note:
        entry["note"] = note
    return entry


def _service_down_cause(name: str, family: Optional[str]) -> Dict[str, Any]:
    """Cause « service_down:<nom> » (la libellé exact n'est pas dans le catalogue)."""
    checked = ("./scripts/tp-db.sh" if family == "persistence"
               else "./scripts/tp-kafka.sh" if family == "kafka_replication"
               else "./scripts/tp-kafka.sh ou ./scripts/tp-db.sh (selon le service)")
    return {
        "cause": f"service_down:{name}",
        "label": f"Le service {name} ne répond plus (sonde en échec)",
        "family": family or "indeterminate",
        "probability": "Faible",
        "criticality": SERVICE_CRITICALITY.get(name, "Faible"),
        "exam_order": 0,
        "status": "probable",
        "check": checked,
    }


# --------------------------------------------------------------------------- #
# finitions de verdict
# --------------------------------------------------------------------------- #


def _base_verdict() -> Dict[str, Any]:
    return {
        "schema": SCHEMA_VERSION,
        "verdict": "indeterminate",
        "cause": None,
        "cause_label": None,
        "probability": None,
        "criticality": None,
        "indeterminate_reason": None,
        "summary": "",
        "notes": [],
        "plausible_causes": [],
        "evidence": [],
        "freshness": {"ok": True, "threshold_s": None, "payload_age_s": None,
                      "replication_age_s": None, "violations": []},
        "next_check": [],
        "based_on": {"samples": 1, "span_s": None, "history_used": False},
        "sample_timestamp": None,
    }


def _stamped(verdict: Dict[str, Any], now: float, span: Optional[float], freshness: Dict[str, Any]) -> Dict[str, Any]:
    verdict["based_on"]["samples"] = 2 if span is not None else 1
    verdict["based_on"]["span_s"] = span
    verdict["sample_timestamp"] = _iso_utc(now)
    verdict["freshness"] = {
        "ok": freshness["ok"],
        "threshold_s": freshness["threshold_s"],
        "payload_age_s": freshness["payload_age_s"],
        "replication_age_s": freshness["replication_age_s"],
        "violations": freshness["violations"],
    }
    return verdict


def _concluded(family: str, cause_id: str, summary: str, evidence: List[Dict[str, str]],
               plausible: List[Dict[str, Any]], nexts: List[str], now: float,
               span: Optional[float], freshness: Dict[str, Any],
               notes: Optional[List[str]] = None) -> Dict[str, Any]:
    spec = CAUSE_TABLE[cause_id]
    verdict = _base_verdict()
    verdict["verdict"] = family
    verdict["cause"] = cause_id
    verdict["cause_label"] = spec["label"]
    verdict["probability"] = spec["probability"]
    verdict["criticality"] = spec["criticality"]
    verdict["summary"] = summary
    verdict["notes"] = notes or []
    verdict["plausible_causes"] = plausible
    verdict["evidence"] = evidence
    verdict["next_check"] = nexts
    return _stamped(verdict, now, span, freshness)


def _healthy(summary: str, evidence: List[Dict[str, str]], notes: List[str],
             now: float, span: Optional[float], freshness: Dict[str, Any],
             cause_id: Optional[str] = "upstream_stopped_normal",
             nexts: Optional[List[str]] = None) -> Dict[str, Any]:
    verdict = _base_verdict()
    verdict["verdict"] = "healthy"
    verdict["summary"] = summary
    verdict["notes"] = notes
    verdict["evidence"] = evidence
    verdict["next_check"] = nexts or ["Surveillance continue : " + RERUN_CMD]
    if cause_id:
        spec = CAUSE_TABLE[cause_id]
        verdict["cause"] = cause_id
        verdict["cause_label"] = spec["label"]
        verdict["probability"] = spec["probability"]
        verdict["criticality"] = spec["criticality"]
        if spec["family"] != "healthy":
            verdict["notes"].append("Cause associée : " + spec["label"])
    return _stamped(verdict, now, span, freshness)


def _undetermined(kind: str, summary: str, evidence: List[Dict[str, str]], now: float,
                  span: Optional[float], freshness: Dict[str, Any],
                  plausible: Optional[List[Dict[str, Any]]] = None,
                  nexts: Optional[List[str]] = None) -> Dict[str, Any]:
    verdict = _base_verdict()
    verdict["verdict"] = "indeterminate"
    verdict["indeterminate_reason"] = kind
    verdict["summary"] = summary
    verdict["evidence"] = evidence
    verdict["plausible_causes"] = sorted(
        plausible or [], key=lambda item: item.get("exam_order", 9))
    verdict["evidence"] = evidence
    verdict["next_check"] = nexts or [
        "Relire l'état pour suivre l'évolution : " + RERUN_CMD,
        INDEPENDENT_CHECK_REMINDER,
    ]
    return _stamped(verdict, now, span, freshness)


# --------------------------------------------------------------------------- #
# preuves typiques
# --------------------------------------------------------------------------- #


def _evidence_kafka_lags(s: Dict[str, Any], labels) -> List[Dict[str, str]]:
    parts = []
    for label in labels:
        lag = _group_lag(s, label)
        if lag is not None:
            parts.append(f"{label} lag_total={int(lag)}")
    if not parts:
        return [_ev("kafka", "consumer_groups", "lecture des groupes indisponible")]
    return [_ev("kafka", "consumer_groups lag_total", " ; ".join(parts))]


def _evidence_pg(s: Dict[str, Any]) -> List[Dict[str, str]]:
    return [
        _ev("postgres", "counts",
            f"db_validated_count={s.get('db_validated')} db_rejected_count={s.get('db_rejected')}"),
        _ev("postgres", "gaps",
            f"persistence_gap_valid={s.get('gap_valid')} persistence_gap_rejected={s.get('gap_rejected')}"),
    ]


def _evidence_flow(s: Dict[str, Any]) -> List[Dict[str, str]]:
    return [
        _ev("platform", "counts",
            f"generated={s.get('generated')} pending_result_count={s.get('pending_result_count')} "
            f"validated={s.get('validated')}"),
        _ev("platform", "rates",
            f"rates: generated/s={s.get('rates_generated')} validated/s={s.get('rates_validated')} "
            f"persisted/s={s.get('rates_persisted')}"),
    ]


def _evidence_replication(s: Dict[str, Any]) -> List[Dict[str, str]]:
    brokers = s.get("brokers") or []
    parts = [f"{b['broker']}: urp={int(b['urp'])} offline={int(b['offline'])} "
             f"controller={int(b['active_controller'])}" for b in brokers]
    return [
        _ev("kafka", "replication",
            f"under_replicated_partitions_total={s.get('urp_total')} ; brokers: " +
            (" ; ".join(parts) if parts else "aucun")),
    ]


def _evidence_services(s: Dict[str, Any], failing: List[str]) -> List[Dict[str, str]]:
    return [_ev("platform", f"services.{name}", f"status = {s.get('services', {}).get(name)}")
            for name in failing]


def _evidence_alerts(s: Dict[str, Any]) -> List[Dict[str, str]]:
    alerts = s.get("alerts") or []
    if not alerts:
        return []
    texts = []
    for alert in alerts[:5]:
        if isinstance(alert, dict):
            texts.append(f"[{alert.get('level', '?')}] {alert.get('message', alert)}")
        else:
            texts.append(str(alert))
    return [_ev("platform", "alerts", " ; ".join(texts))]


# --------------------------------------------------------------------------- #
# garde-fous
# --------------------------------------------------------------------------- #


def _structural_reason(payload: Dict[str, Any]) -> Optional[str]:
    """None si les grandes lectures existent, sinon la raison data_missing."""
    if not isinstance(payload, dict) or not payload:
        return "payload vide ou non JSON"
    metrics = payload.get("metrics") or {}
    counts = metrics.get("counts") or {}
    details = payload.get("details") or {}
    missing = []
    if not counts:
        missing.append("metrics.counts")
    if (details.get("kafka") or {}).get("consumer_groups") is None:
        missing.append("details.kafka.consumer_groups")
    if not details.get("postgres"):
        missing.append("details.postgres")
    if missing:
        return "lectures absentes : " + ", ".join(missing)
    return None


def _freshness(s: Dict[str, Any], now: float, config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
    """Garde-fou fraîcheur (critère #6) : âge du payload et de l'instantané réplication."""
    threshold = float(config["max_probe_age_seconds"])
    msg_epoch = _parse_iso(s.get("timestamp"))
    captured_epoch = _parse_iso(s.get("captured_at"))
    violations: List[str] = []
    if msg_epoch is not None:
        msg_age = round(max(now - msg_epoch, 0.0), 2)
        if msg_age > threshold:
            violations.append(f"payload timestamp age {msg_age:.1f}s > {threshold:.0f}s")
    else:
        msg_age = None
        violations.append("payload timestamp absent ou illisible")
    if captured_epoch is not None:
        captured_age = round(max(now - captured_epoch, 0.0), 2)
        if captured_age > threshold:
            violations.append(f"instantané réplication age {captured_age:.1f}s > {threshold:.0f}s")
    else:
        captured_age = None
        violations.append("instantané réplication (captured_at) absent ou illisible")
    freshness = {
        "ok": not violations,
        "threshold_s": threshold,
        "payload_age_s": msg_age,
        "replication_age_s": captured_age,
        "violations": violations,
    }
    return (not violations), freshness, violations


def _previous_sample(previous: Optional[Dict[str, Any]], now: float, config: Dict[str, Any],
                     s_now: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[float], Optional[str]]:
    """Valide l'échantillon précédent : span >= min, âge <= max, pas de réinitialisation.

    Retourne (extrait précédent ou None, span_s ou None, raison d'écartement).
    """
    if not previous:
        return None, None, None
    payload_prev = previous.get("payload") or {}
    wall = _num(previous.get("wall"))
    if not payload_prev or wall is None:
        return None, None, "échantillon précédent incomplet (wall/payload manquants)"
    span = now - wall
    if span < float(config["min_sample_span_seconds"]):
        return None, None, f"span {span:.1f}s < seuil {config['min_sample_span_seconds']:.0f}s (relire encore)"
    if span > float(config["previous_max_age_seconds"]):
        return None, None, f"échantillon précédent trop ancien ({span:.0f}s)"
    s_prev = _extract(payload_prev)
    # détection de réinitialisation de l'environnement (compteurs monotones qui baissent)
    for key in ("generated", "db_validated", "db_rejected"):
        a, b = s_now.get(key), s_prev.get(key)
        if a is not None and b is not None and a < b:
            return (None, None,
                    "compteurs décroissants (nouveau run détecté) : échantillon précédent écarté")
    return s_prev, round(span, 2), None


# --------------------------------------------------------------------------- #
# règles de la matrice
# --------------------------------------------------------------------------- #


def _rule_services(s: Dict[str, Any], now: float, span: Optional[float],
                   freshness: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Ligne 5 : verdict degraded -> service down (1re) vs sonde aveuglée by réseau (2e)."""
    failing = _failing_services(s)
    evidence = _evidence_services(s, failing) + _evidence_alerts(s)
    primary = failing[0]
    family = SERVICE_FAMILY.get(primary)

    if s.get("network_active"):
        blinded = _cause("probe_blinded", "probable",
                         note=f"perturbation réseau active (profil {s.get('network_profile')}) : "
                              "la sonde peut être aveuglée, pas forcément le service")
        service = _service_down_cause(primary, family)
        service["status"] = "hypothesis"
        return _undetermined(
            "probe_blinded",
            (f"Verdict global « {s.get('status')} » et service « {primary} » en échec pendant une "
             f"perturbation réseau active : la sonde peut être aveuglée (le service peut tourner). "
             "Croiser avec la sonde indépendante avant de conclure."),
            evidence, now, span, freshness,
            plausible=[service, blinded],
            nexts=[blinded["check"], "Relire après essai : " + RERUN_CMD],
        )

    evidence.append(_ev("platform", "network", "aucune perturbation réseau active (profil "
                       f"{s.get('network_profile')})"))
    plausible = [_service_down_cause(primary, family),
                 _cause("probe_blinded", "excluded",
                        note="aucune perturbation réseau active")]
    if family is None:
        summary = (f"Verdict global « {s.get('status')} » : service transversal « {primary} » "
                   "ne répond plus (sonde). Pas d'impact pipeline direct, mais un témoin à noter.")
        nexts = [plausible[0]["check"], "Relire ensuite : " + RERUN_CMD]
        return _undetermined("service_down_transversal", summary, evidence, now, span,
                             freshness, plausible, nexts)
    summary = (f"Verdict global « {s.get('status')} » : le service {primary} ne répond plus "
               "(sonde). À confirmer avec la sonde indépendante.")
    return _rule_services_concluded(primary, family, s, evidence, plausible, summary,
                                    [plausible[0]["check"], "Relire ensuite : " + RERUN_CMD],
                                    now, span, freshness)


def _rule_services_concluded(primary: str, family: str, s: Dict[str, Any], evidence,
                             plausible, summary, nexts, now, span, freshness) -> Dict[str, Any]:
    """Conclusion de la ligne 5 pour un service avec famille (A/B/C) — cause ad hoc."""
    verdict = _base_verdict()
    verdict["verdict"] = family
    verdict["cause"] = f"service_down:{primary}"
    verdict["cause_label"] = plausible[0]["label"]
    verdict["probability"] = plausible[0]["probability"]
    verdict["criticality"] = plausible[0]["criticality"]
    verdict["summary"] = summary
    verdict["plausible_causes"] = plausible
    verdict["evidence"] = evidence
    verdict["next_check"] = nexts
    return _stamped(verdict, now, span, freshness)


def _rule_urp(s: Dict[str, Any], now: float, span: Optional[float], freshness: Dict[str, Any],
              config: Dict[str, Any]) -> Dict[str, Any]:
    """Ligne 2 : URP > 0 -> broker arrêté (1re à vérifier, 5 s) vs retard de copie (2e)."""
    urp = s.get("urp_total") or 0.0
    broker_count = s.get("broker_count")
    brokers = s.get("brokers") or []
    offline_names = [b["broker"] for b in brokers if (b.get("offline") or 0.0) > 0]
    controllers = sum((b.get("active_controller") or 0.0) for b in brokers)
    count_low = broker_count is not None and broker_count < int(config["expected_brokers"])

    evidence = _evidence_replication(s) + [
        _ev("kafka", "broker_count", f"broker_count={broker_count} (attendu "
                                     f"{int(config['expected_brokers'])})")]
    how = ", ".join(filter(None, [
        (f"offline_partitions_count>0 sur {', '.join(offline_names)}" if offline_names else ""),
        ("active_controller_count total = 0" if controllers == 0 else ""),
        (f"broker_count={broker_count:.0f} < {int(config['expected_brokers'])}" if count_low else ""),
    ]))

    if offline_names or controllers == 0 or count_low:
        status = "confirmed" if (offline_names or controllers == 0) else "probable"
        return _concluded(
            "kafka_replication", "broker_down",
            summary=(f"URP={int(urp)} avec signes de panne broker ({how}) : cluster en intégrité "
                     "réduite (RF=3, min ISR=2 : un 2e incident bloque les écritures). "
                     "Vérifier immédiatement avec tp-kafka.sh."),
            evidence=evidence,
            plausible=[
                _cause("broker_down", status, note=how),
                _cause("replication_catchup", "excluded", note="bénin seulement si brokers tous OK"),
            ],
            nexts=[CAUSE_TABLE["broker_down"]["independent_check"],
                   "Surveiller la résorption : " + RERUN_CMD],
            now=now, span=span, freshness=freshness)

    return _concluded(
        "kafka_replication", "replication_catchup",
        summary=(f"URP={int(urp)} mais brokers tous opérationnels (offline=0, contrôleur actif, "
                 f"broker_count={broker_count:.0f}/{int(config['expected_brokers'])}) : retard de "
                 "copie probable. On relit pour confirmer la résorption (un retard de copie se "
                 "résorbe seul, pas une panne)."),
        evidence=evidence,
        plausible=[
            _cause("replication_catchup", "probable"),
            _cause("broker_down", "excluded",
                   note=f"exclu par la vérification immédiate ({how or 'brokers OK'})"),
        ],
        nexts=[CAUSE_TABLE["replication_catchup"]["independent_check"],
               "Relire ~30 s après : " + RERUN_CMD],
        now=now, span=span, freshness=freshness)


def _rule_persister(s: Dict[str, Any], s_prev: Optional[Dict[str, Any]], span: Optional[float],
                    now: float, freshness: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ligne 1 (branche persistance) : lag persisters + écart grossit -> PostgreSQL ne suit plus.

    Retourne None si la règle ne se déclenche pas.
    """
    threshold = float(config["lag_total_threshold"])
    gap_step = float(config["gap_step"])
    big_gap = float(config["big_gap_threshold"])

    lag_p = _max_lag(s, PERSISTER_GROUPS)
    gap_now = _gaps_total(s)
    gap_prev = _gaps_total(s_prev) if s_prev else None
    db_now = _db_total(s)
    db_prev = _db_total(s_prev) if s_prev else None
    idle = _idle_now(s, config)

    if lag_p < threshold and (gap_now or 0.0) < big_gap:
        return None
    if s_prev is None:
        # UNE seule lecture : on ne peut pas trancher (l'écart doit être vu S'ACCROÎT)
        evidence = _evidence_kafka_lags(s, PERSISTER_GROUPS) + _evidence_pg(s) + _evidence_flow(s)
        plausible = [
            _cause("reader_backlog", "hypothesis",
                   note="cause 1 de la matrice ligne 1 (à trancher avec l'écart de persistance)"),
            _cause("persister_slow", "hypothesis",
                   note=f"lag persisters={int(lag_p)} >= seuil {int(threshold)} : à confirmer par "
                        "un écart qui S'ACCROÎT"),
        ]
        return _undetermined(
            "needs_second_sample",
            ("Les persisters ont du retard (lag) : à UNE lecture on ne peut pas trancher entre "
             "retard de lecture et PostgreSQL (il faut voir l'écart grossir entre 2 lectures). "
             "Relire puis trancher (si l'écart grossit -> persistance)."),
            evidence, now, span, freshness, plausible,
            nexts=[RERUN_CMD, INDEPENDENT_CHECK_REMINDER])

    gap_delta = None
    if gap_now is not None and gap_prev is not None:
        gap_delta = gap_now - gap_prev
    db_delta = None
    if db_now is not None and db_prev is not None:
        db_delta = db_now - db_prev
    evidence = _evidence_kafka_lags(s, PERSISTER_GROUPS) + _evidence_pg(s) + _evidence_flow(s)
    evidence.append(_ev("kafka|postgres", "écart cross-check",
                        f"persistence_gap {gap_prev if gap_prev is not None else '?'} -> "
                        f"{gap_now} (delta {gap_delta}) ; db total {db_prev} -> {db_now}"))

    if gap_delta is not None and gap_delta >= gap_step and (lag_p > 0 or (db_delta or 0.0) <= 0):
        # l'écart S'ACCROÎT : le discriminateur C2 de mardi, donc PostgreSQL ne suit plus
        return _concluded(
            "persistence", "persister_slow",
            summary=(f"Les persisters ont du lag (>= {int(threshold)}) ET l'écart de persistance "
                     f"entre 2 lectures S'ACCROÎT ({gap_prev:g} -> {gap_now:g}) : PostgreSQL ne "
                     "suit plus le flux (écarter vite : durabilité, via tp-db.sh)."),
            evidence=evidence,
            plausible=[
                _cause("reader_backlog", "excluded",
                       note="le lag est sur les persisters, pas sur validator/decision (tp-kafka.sh)"),
                _cause("persister_slow", "probable"),
            ],
            nexts=["./scripts/tp-db.sh", "Relire pour tableau : " + RERUN_CMD],
            now=now, span=span, freshness=freshness)

    if gap_delta is not None and gap_delta < 0:
        return _healthy(
            summary=("	L'écart de persistance DÉCROÎT entre les 2 lectures : rattrapage en cours "
                     "(les persisters lisent le retard, ils ne sont pas bloqués)."),
            evidence=evidence,
            notes=["Rattrapage détecté : écart qui décroît, en cours de résorption"],
            now=now, span=span, freshness=freshness, cause_id=None)

    if abs(gap_delta or 0.0) < gap_step:
        if idle:
            return _healthy(
                summary=("Le flux est au repos et le lag persisters résiduel ne bouge pas : "
                         "amont arrêté (fin de scénario), aucune écriture attendue."),
                evidence=evidence,
                notes=["idle : lag résiduel, amont arrêté (nominal en fin de flux)"],
                now=now, span=span, freshness=freshness, cause_id="upstream_stopped_normal")
        return _undetermined(
            "contradictory_signals",
            ("Lag persisters élevé mais l'écart de persistance n'évolue pas alors que le flux "
             "tourne : début de blocage, rattrapage ou purge — à relire sur une fenêtre."),
            evidence, now, span, freshness,
            plausible=[
                _cause("reader_backlog", "hypothesis"),
                _cause("persister_slow", "hypothesis", note="écart stable : à re-confirmer"),
            ],
            nexts=[RERUN_CMD, INDEPENDENT_CHECK_REMINDER])
    return None


def _rule_db_frozen(s: Dict[str, Any], s_prev: Dict[str, Any], span: Optional[float],
                    now: float, freshness: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ligne 3 : db figé entre 2 lectures -> amont arrêté (normal) vs PostgreSQL indisponible."""
    threshold = float(config["lag_total_threshold"])
    gap_step = float(config["gap_step"])
    db_now, db_prev = _db_total(s), _db_total(s_prev)
    generated, generated_prev = s.get("generated"), s_prev.get("generated")
    idle = _idle_now(s, config)
    lag_r = _max_lag(s, READER_GROUPS)
    lag_p = _max_lag(s, PERSISTER_GROUPS)
    if db_now is None or db_prev is None or db_now != db_prev:
        return None
    gen_delta = None
    if generated is not None and generated_prev is not None:
        gen_delta = generated - generated_prev

    evidence = _evidence_pg(s) + _evidence_flow(s) + _evidence_kafka_lags(s, READER_GROUPS)
    evidence.append(_ev("postgres", "db counters delta",
                        f"db total {db_prev:g} -> {db_now:g} (figé) ; "
                        f"generated {generated_prev} -> {generated}"))

    if gen_delta == 0 or (gen_delta is None and idle):
        # situation « fin de scénario » possible : mais si du travail est résiduel
        # (file non vide, lag, SLA qui monte) on laisse les règles suivantes trancher
        pending = s.get("pending_result_count") or 0.0
        b_now, b_prev = _breaches_total(s), _breaches_total(s_prev)
        breach_delta = None if (b_now is None or b_prev is None) else b_now - b_prev
        residual = (pending >= threshold) or lag_r >= threshold or lag_p >= threshold
        if residual or (breach_delta is not None and breach_delta > 0):
            return None
        return _healthy(
            summary=("Plus aucune écriture en base ET l'amont stagne : fin de scénario. "
                     "C'est la situation normale (matrice ligne 3, cause 1 : amont arrêté)."),
            evidence=evidence,
            notes=["Base figée expliquée : generated stagne, aucun risque de perte"],
            now=now, span=span, freshness=freshness, cause_id="upstream_stopped_normal")
    if gen_delta and gen_delta > 0 and lag_r >= threshold:
        # l'amont produit, la base n'écrit pas : les lecteurs n'ont pas encore propagé
        return _concluded(
            "consumer_backlog", "reader_backlog",
            summary=(f"db figé alors que generated monte (+{gen_delta:g}) et le lag de lecture est "
                     f"élevé ({int(lag_r)}) : le flux de lecture/commit en amont traîne."),
            evidence=evidence,
            plausible=[
                _cause("reader_backlog", "probable"),
                _cause("persister_slow", "hypothesis", note="surveiller l'écart de persistance"),
            ],
            nexts=[CAUSE_TABLE["reader_backlog"]["independent_check"],
                   "Relire : " + RERUN_CMD],
            now=now, span=span, freshness=freshness)

    # l'amont monte, la base figée sans lag lisible : contradiction => indéterminé
    return _undetermined(
        "contradictory_signals",
        ("La base n'écrit plus pendant que l'amont produit, sans lag lisible ni service en échec : "
         "le payload ne permet pas de trancher entre les causes de la matrice ligne 3."),
        evidence, now, span, freshness,
        plausible=[
            _cause("upstream_stopped_normal", "hypothesis", note="generated semble stagner/finir"),
            _cause("postgres_unavailable", "hypothesis",
                   note="écarter via tp-db.sh (critique : perte durable potentielle)"),
        ],
        nexts=[CAUSE_TABLE["postgres_unavailable"]["independent_check"], RERUN_CMD])


def _rule_reader(s: Dict[str, Any], s_prev: Optional[Dict[str, Any]], span: Optional[float],
                 now: float, freshness: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ligne 1 (branche lecture) : lag lecteur persistant/montant -> retard de lecture."""
    threshold = float(config["lag_total_threshold"])
    lag_r = _max_lag(s, READER_GROUPS)
    if lag_r < threshold:
        return None
    evidence = _evidence_kafka_lags(s, READER_GROUPS) + _evidence_pg(s) + _evidence_flow(s)
    if s_prev is None:
        plausible = [
            _cause("reader_backlog", "hypothesis",
                   note=f"lag lecteur = {int(lag_r)} >= seuil {int(threshold)} : un flash est "
                        "nominal, relire à 2 échantillons pour conclure"),
            _cause("persister_slow", "hypothesis"),
        ]
        return _undetermined(
            "needs_second_sample",
            ("Retard possible de lecture mais ne pas conclure sur une seule lecture (lag éclair "
             "nominal) : relire à ~15 s via --watch puis trancher."),
            evidence, now, span, freshness, plausible,
            nexts=[RERUN_CMD, CAUSE_TABLE["reader_backlog"]["independent_check"]])

    gap_now, gap_prev = _gaps_total(s), _gaps_total(s_prev)
    gap_delta = (gap_now - gap_prev) if (gap_now is not None and gap_prev is not None) else None
    lag_p = _max_lag(s, PERSISTER_GROUPS)
    persister_status = "excluded" if (lag_p or 0.0) < threshold else "hypothesis"
    persister_note = ("lag persisters faible") if persister_status == "excluded" else \
        "les persisters traînent aussi : surveiller l'écart de persistance"
    return _concluded(
        "consumer_backlog", "reader_backlog",
        summary=(f"Lag de lecture persistant/montant ({int(lag_r)} >= seuil {int(threshold)}) : "
                 "les applications qui lisent les topics sont en retard (file qui grossit)."),
        evidence=evidence,
        plausible=[
            _cause("reader_backlog", "probable"),
            _cause("persister_slow", persister_status, note=persister_note),
        ],
        nexts=[CAUSE_TABLE["reader_backlog"]["independent_check"],
               "Suivre la résorption : " + RERUN_CMD],
        now=now, span=span, freshness=freshness)


def _rule_sla(s: Dict[str, Any], s_prev: Dict[str, Any], span: Optional[float], now: float,
              freshness: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ligne 4 : validated_sla_breaches en hausse -> retard amont (1re) vs délai trop court (2e)."""
    breaches_now = _breaches_total(s)
    breaches_prev = _breaches_total(s_prev)
    if breaches_now is None or breaches_prev is None or breaches_now <= breaches_prev:
        return None
    threshold = float(config["lag_total_threshold"])
    delta = breaches_now - breaches_prev
    latency = s.get("avg_validation_latency_ms")
    sla = s.get("sla_seconds")
    pending = s.get("pending_result_count") or 0.0
    lag_r = _max_lag(s, READER_GROUPS)
    lag_p = _max_lag(s, PERSISTER_GROUPS)
    queue_long = (pending >= threshold) or lag_r >= threshold or lag_p >= threshold
    evidence = [
        _ev("platform", "sla_breaches",
            f"validated+rejected SLA breaches {breaches_prev:g} -> {breaches_now:g} (delta +{delta:g})"),
        _ev("postgres", "sla", "breaches comptés en base (details.postgres)") ,
        _ev("platform", "pending_result_count", f"file (pending_result_count) = {int(pending)}"),
    ]
    if latency is not None and sla is not None:
        evidence.append(_ev("postgres", "latency vs SLA",
                            f"avg_validation_latency_ms={int(latency)} ms vs "
                            f"decision_sla_seconds={int(sla)} s (marge "
                            f"{int(config['sla_latency_margin']*100)}%)"))

    if queue_long:
        return _concluded(
            "consumer_backlog", "upstream_backlog_sla",
            summary=(f"Dépassements SLA en hausse (+{delta:g}) avec une file"
                     f" (« pending={int(pending)} », lag lecteur "
                     f"{int(lag_r)}) : retard amont — les paiements attendent et dépassent le TTL."),
            evidence=evidence,
            plausible=[
                _cause("upstream_backlog_sla", "probable"),
                _cause("sla_config", "hypothesis", note="à suspecter seulement si la file était vide"),
            ],
            nexts=[CAUSE_TABLE["upstream_backlog_sla"]["independent_check"],
                   "Relire : " + RERUN_CMD],
            now=now, span=span, freshness=freshness)

    if latency is not None and sla is not None and latency > sla * 1000 * float(config["sla_latency_margin"]):
        return _concluded(
            "consumer_backlog", "sla_config",
            summary=(f"Dépassements SLA en hausse (+{delta:g}) AVEC la file vide et une latence "
                     f"{int(latency)} ms > SLA {int(sla)} s : délai trop court pour la charge "
                     "(problème de configuration du scénario, pas un incident de plateforme)."),
            evidence=evidence,
            plausible=[
                _cause("upstream_backlog_sla", "hypothesis", note="file vide : peu probable"),
                _cause("sla_config", "probable"),
            ],
            nexts=["Comparer latence moyenne vs SLA : "
                   f"{int(latency)} ms / {int(sla)} s (marge "
                   f"{int(config['sla_latency_margin']*100)}%)",
                   "./scripts/tp-db.sh", RERUN_CMD],
            now=now, span=span, freshness=freshness)

    return _undetermined(
        "ambiguous_sla",
        ("Dépassements SLA en hausse avec file vide et latence dans le seuil : on ne peut "
         "pas trancher retard amont/délai trop court — comparer la latence moyenne au SLA "
         "et vérifier les motifs en base."),
        evidence, now, span, freshness,
        plausible=[
            _cause("upstream_backlog_sla", "hypothesis"),
            _cause("sla_config", "hypothesis"),
        ],
        nexts=["Comparer avg_validation_latency_ms au decision_sla_seconds",
               "./scripts/tp-db.sh", RERUN_CMD])


def _rule_calm(s: Dict[str, Any], s_prev: Optional[Dict[str, Any]], span: Optional[float],
               now: float, freshness: Dict[str, Any], config: Dict[str, Any],
               extra_evidence: List[Dict[str, str]]) -> Dict[str, Any]:
    """Zone nominale : healthy avec notes sur les signaux « légers » attendus."""
    threshold = float(config["lag_total_threshold"])
    notes: List[str] = []
    evidence = list(extra_evidence) + _evidence_kafka_lags(s, READER_GROUPS + PERSISTER_GROUPS) \
        + _evidence_pg(s) + _evidence_flow(s)
    lags = {label: _group_lag(s, label) for label in READER_GROUPS + PERSISTER_GROUPS}
    lags = {k: v for k, v in lags.items() if v}
    if lags:
        worst = max(lags.items(), key=lambda kv: kv[1])
        notes.append(f"lag passager {worst[0]}={int(worst[1])} < seuil {int(threshold)} : "
                     "rattrapage éclair (nominal)")
    gap = _gaps_total(s)
    if gap is not None and 0.0 < gap <= float(config["big_gap_threshold"]):
        notes.append(f"écart de persistance léger ({int(gap)}) : nominal en début de flux")
    if s_prev is not None:
        b_now, b_prev = _breaches_total(s), _breaches_total(s_prev)
        if b_now and b_prev is not None and b_now == b_prev and b_now > 0:
            notes.append(f"dépassements SLA cumulés = {int(b_now)} sans progression entre lectures : "
                         "historique, pas de nouveau dépassement")
    if s.get("status") and s.get("status") != "ok":
        notes.append(f"verdict global de la plateforme : {s.get('status')} (aucun moteur n'a "
                     "détaché de cause : croiser avec les alertes)")
    evidence += _evidence_alerts(s)
    return _healthy(
        summary="Aucune règle de la matrice ne se déclenche : flux nominal, aucune cause nommée.",
        evidence=evidence, notes=notes, now=now, span=span, freshness=freshness, cause_id=None,
        nexts=["Surveillance continue : " + RERUN_CMD])


# --------------------------------------------------------------------------- #
# machine à états
# --------------------------------------------------------------------------- #


def route(payload: Dict[str, Any], s: Dict[str, Any], s_prev: Optional[Dict[str, Any]],
          span: Optional[float], now: float, freshness: Dict[str, Any],
          config: Dict[str, Any], notes: List[str]) -> Dict[str, Any]:
    # LIGNE 5 : service(s) down
    if _failing_services(s):
        return _rule_services(s, now, span, freshness, config)
    # C1 : probe PostgreSQL non OK malgré services ok -> indisponibilité probable
    if isinstance(s.get("pg_status"), str) and s["pg_status"].lower() not in OK_SERVICE_STATUS:
        evidence = _evidence_pg(s) + [
            _ev("platform", "details.postgres.status", f"status = {s['pg_status']}")]
        return _concluded(
            "persistence", "postgres_unavailable",
            summary=(f"details.postgres.status = {s['pg_status']} : PostgreSQL indisponible ou "
                     "saturé ; la sonde/service dit OK, à croiser avec tp-db.sh "
                     "(critique : perte durable potentielle)."),
            evidence=evidence,
            plausible=[
                _cause("postgres_unavailable", "probable"),
            ],
            nexts=[CAUSE_TABLE["postgres_unavailable"]["independent_check"],
                   "docker compose logs --tail=50 postgres", "Relire : " + RERUN_CMD],
            now=now, span=span, freshness=freshness)
    # LIGNE 2 : URP
    if (s.get("urp_total") or 0.0) > 0:
        return _rule_urp(s, now, span, freshness, config)
    if s_prev is not None:
        # LIGNE 3 : db figé
        verdict = _rule_db_frozen(s, s_prev, span, now, freshness, config)
        if verdict:
            return verdict
    # Ligne 1 : persister slow
    verdict = _rule_persister(s, s_prev, span, now, freshness, config)
    if verdict:
        return verdict
    # ligne 1 : lecteur en retard
    verdict = _rule_reader(s, s_prev, span, now, freshness, config)
    if verdict:
        return verdict
    if s_prev is not None:
        verdict = _rule_sla(s, s_prev, span, now, freshness, config)
        if verdict:
            return verdict
    return _rule_calm(s, s_prev, span, now, freshness, config, [])


def build_verdict(payload: Dict[str, Any], now: Optional[float] = None,
                  previous: Optional[Dict[str, Any]] = None,
                  config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Un échantillon (+ éventuellement le précédent) -> verdict JSON sérialisable.

    Args:
      payload: dict JSON de ``/health/details``.
      now: horloge unix de la lecture (secondes flottantes).
      previous: ``{"wall": float, "payload": dict}`` de la lecture précédente
        (maintenu par le CLI), ou None en mode sans état.
      config: surcharge des seuils (clés de ``DEFAULT_CONFIG``).

    Returns:
      dict à clés stables (verdict, cause, plausible_causes, evidence, freshness,
      next_check, based_on, sample_timestamp).
    """
    config = _merge_config(config)
    now = float(now) if now is not None else 0.0

    structural = _structural_reason(payload)
    s = _extract(payload)
    prev_payload = previous

    if structural:
        verdict = _undetermined(
            "data_missing",
            "Lectures incomplètes : impossible de croiser Kafka ET PostgreSQL (critère #5).",
            [_ev("platform", "payload", structural)], now, None,
            {"ok": False, "threshold_s": float(config["max_probe_age_seconds"]),
             "payload_age_s": None, "replication_age_s": None, "violations": []},
            nexts=["Vérifier service-health : curl -s "
                   "${SIMULPIX_HEALTH_URL:-http://localhost:8082}/health/details | head -c 400",
                   "docker compose ps service-health"])
        return verdict

    fresh, freshness, violations = _freshness(s, now, config)
    if not fresh:
        return _undetermined(
            "probes_stale",
            "Sondes périmées (" + " ; ".join(violations) +
            ") : toute conclusion serait une conjecture — état indéterminé (critère #6).",
            [_ev("platform", "freshness", " ; ".join(violations))],
            now, span=None, freshness=freshness)

    s_prev, span, rejection = _previous_sample(prev_payload, now, config, s)
    notes: List[str] = []
    extra_evidence: List[Dict[str, str]] = []
    if rejection:
        notes.append("Échantillon précédent écarté : " + rejection)
        extra_evidence.append(_ev("platform", "state", "échantillon précédent écarté : " + rejection))

    verdict = route(payload, s, s_prev, span, now, freshness, config, notes)
    for note in notes:
        if note and note not in verdict["notes"]:
            verdict["notes"].append(note)
    for item in extra_evidence:
        if item not in verdict["evidence"]:
            verdict["evidence"].append(item)
    return verdict
