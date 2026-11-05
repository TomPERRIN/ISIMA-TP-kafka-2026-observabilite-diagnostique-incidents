#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests unitaires du moteur de diagnostic Simul-Pix (SAÉ 5).

Exécution :
    cd scripts/lib && python3 -m unittest test_diagnose_engine -v

Les fixtures produisent des payloads au format ``/health/details`` (synthétiques,
conformes à la référence nominale mesurée). Chaque test encode une case de la
matrice v1 (travail/notes/matrice-symptomes-causes.md) et/ou un critère du sujet :
  - critère #1 : pas de confusion retard de consommateur / réplication / persistance
    (chaque incident d'essai est testé séparément, famille par famille).
  - critère #4 : les 2 causes d'un symptôme sont séparées automatiquement ;
  - critère #5 : toute conclusion ≠ indéterminé exige la double lecture Kafka+PG ;
  - critère #6 : sondes périmées -> indéterminé, jamais une conclusion ;
  - témoin nominal : un rattrapage éclair, un écart de persistance léger
    ou un lag passager ne produisent PAS d'incident (0 faux positif).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from diagnose_engine import build_verdict  # noqa: E402
import diagnose_engine as eng  # noqa: E402,F401  (helpers _utc/_iso)


def _iso(epoch):
    return eng._iso_utc(epoch)

T0 = 1794464000.0  # instant de test arbitraire (2026-11-05 ~ 00:00 UTC)
SPAN = 12.0        # espacement des 2 échantillons (>= seuil 10 s)

ALL_SERVICES = {
    "kafka": "ok", "pix-scenario-planner": "ok", "pix-traffic-shaper": "ok",
    "generator": "ok", "pix-validator": "ok", "pix-decision-engine": "ok",
    "pix-outcome-publisher": "ok", "persister-valid": "ok", "persister-rejected": "ok",
    "postgres": "ok", "influxdb": "ok", "grafana": "ok",
}

READERS = ("pix-validator", "pix-decision-engine", "pix-outcome-publisher")
PERSISTERS = ("persister-valid", "persister-rejected")


def base_payload(timestamp):
    """Échantillon nominal synthétique, conforme au témoin nominal (matrice)."""
    groups = {}
    for label in READERS + PERSISTERS:
        groups[label] = {"group_id": "simulpix-" + label, "lag_total": 0}
    brokers = [{"broker": "kafka-%d" % i, "under_replicated_partitions": 0,
                "active_controller_count": 1, "offline_partitions_count": 0}
               for i in (1, 2, 3)]
    return {
        "status": "ok",
        "timestamp": _iso(timestamp),
        "alerts": [],
        "services": dict(ALL_SERVICES),
        "metrics": {
            "counts": {
                "generated": 100, "validated": 100, "rejected": 0,
                "persisted_valid": 100, "persisted_rejected": 0,
                "db_validated_count": 100, "db_rejected_count": 0,
                "outcome_count": 100, "pending_result_count": 0,
                "pix_validator_lag_total": 0, "pix_decision_engine_lag_total": 0,
                "pix_outcome_publisher_lag_total": 0,
                "persister_valid_lag_total": 0, "persister_rejected_lag_total": 0,
                "broker_count": 3,
                "validated_sla_breaches": 0, "rejected_sla_breaches": 0,
            },
            "gaps": {"persistence_gap_valid": 0, "persistence_gap_rejected": 0},
            "timings": {"avg_validation_latency_ms": 982.29,
                        "seconds_since_last_validated": 2.0},
            "rates": {"generated_per_second": 0.0, "validated_per_second": 0.0,
                      "persisted_valid_per_second": 0.0},
        },
        "details": {
            "kafka": {
                "broker_count": 3,
                "consumer_groups": groups,
                "replication": {
                    "captured_at": _iso(timestamp - 20.0),
                    "brokers": brokers,
                    "under_replicated_partitions_total": 0,
                },
            },
            "postgres": {"status": "ok", "seconds_since_last_validated": 2.0,
                         "validated_sla_breaches": 0, "rejected_sla_breaches": 0},
            "network": {"active": 0, "profile": "none", "target_service": "none"},
        },
        "ui": {"decision_sla_seconds": "10"},
    }


def previous_of(payload, now):
    return {"wall": now - SPAN, "payload": payload}


def verdict(payload, now, previous=None, config=None):
    return build_verdict(payload, now=now, previous=previous, config=config)


class DiagnoseEngineTest(unittest.TestCase):
    """Chaque test = une case de la matrice ou un garde-fou du sujet."""

    # ---------- ligne 1 : file d'attente grossit ----------

    def test_nominal_healthy_single_sample(self):
        v = verdict(base_payload(T0), now=T0)
        self.assertEqual(v["verdict"], "healthy")

    def test_flash_lag_single_sample_never_concludes(self):
        # témoin nominal : lag éclair -> AUCUNE conclusion sur une seule lecture
        p = base_payload(T0)
        counts(p, pending_result_count=132, pix_validator_lag_total=132)
        group_lag(p, "pix-validator", 132)
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "needs_second_sample")

    def test_reader_backlog_two_samples_concluded(self):
        p0 = with_lag(base_payload(T0), reader=120)
        p1 = with_lag(base_payload(T0), reader=132)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        self.assertEqual(v["verdict"], "consumer_backlog")
        self.assertEqual(v["cause"], "reader_backlog")
        sources = {e["source"] for e in v["evidence"]}
        self.assertIn("kafka", sources)
        self.assertIn("postgres", sources)   # critère #5 : double lecture
        self.assertTrue(len(v["next_check"]) >= 2)

    def test_reader_backlog_excludes_persister_when_persister_calm(self):
        p0 = with_lag(base_payload(T0), reader=120)
        p1 = with_lag(base_payload(T0), reader=132)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        plausible = {c["cause"]: c["status"] for c in v["plausible_causes"]}
        self.assertEqual(plausible.get("reader_backlog"), "probable")
        self.assertEqual(plausible.get("persister_slow"), "excluded")

    def test_persister_slow_rising_gap_two_samples(self):
        # discriminateur : lag persisters >= seuil ET écart qui S'ACCROÎT -> persistance
        p0 = with_persister(base_payload(T0), lag=4, gap=0, db=94)
        p1 = with_persister(base_payload(T0), lag=81, gap=78, db=100)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        self.assertEqual(v["verdict"], "persistence")
        self.assertEqual(v["cause"], "persister_slow")
        sources = {e["source"] for e in v["evidence"]}
        self.assertIn("kafka", sources)
        self.assertIn("postgres", sources)

    def test_rattrapage_is_healthy_not_false_positive(self):
        # écart DÉCROISSANT + lag persisters : rattrapage = healthy, jamais un incident
        p0 = with_persister(base_payload(T0), lag=20, gap=60, db=92)
        p1 = with_persister(base_payload(T0), lag=20, gap=40, db=100)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        self.assertEqual(v["verdict"], "healthy")
        self.assertTrue(any("rattrapage" in note.lower() for note in v["notes"]))

    def test_idle_residual_persister_lag_is_normal_end_of_flow(self):
        # lag persisters résiduel + flux au repos : fin de scénario (nominal)
        p0 = with_persister(base_payload(T0 - SPAN), lag=15, gap=5, db=100)
        p1 = with_persister(base_payload(T0), lag=15, gap=5, db=100)
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "healthy")
        self.assertEqual(v["cause"], "upstream_stopped_normal")

    # ---------- ligne 2 : URP > 0 ----------

    def test_broker_down_when_offline_partitions(self):
        p = with_urp(base_payload(T0), total=15, offline=7)
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "kafka_replication")
        self.assertEqual(v["cause"], "broker_down")
        plausible = {c["cause"]: c["status"] for c in v["plausible_causes"]}
        self.assertEqual(plausible.get("broker_down"), "confirmed")
        self.assertEqual(plausible.get("replication_catchup"), "excluded")

    def test_broker_down_when_low_broker_count(self):
        # Tuesday : broker_count = 2 sur un compose à 3 brokers = broker tombé
        p = with_urp(base_payload(T0), total=6, offline=0, count=2)
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "kafka_replication")
        self.assertEqual(v["cause"], "broker_down")

    def test_urp_with_all_brokers_up_is_catchup(self):
        # le discriminant de 5 s de la matrice : offline=0 + contrôleur actif -> retard copie
        p = with_urp(base_payload(T0), total=2, offline=0, count=3)
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "kafka_replication")
        self.assertEqual(v["cause"], "replication_catchup")
        causes = [c["cause"] for c in v["plausible_causes"]]
        self.assertEqual(causes, ["replication_catchup", "broker_down"])  # critère #4
        self.assertEqual(v["plausible_causes"][1]["status"], "excluded")

    def test_catchup_next_check_asks_reread_30s(self):
        p = with_urp(base_payload(T0), total=2, offline=0, count=3)
        v = verdict(p, now=T0)
        self.assertTrue(any("30" in next or "relire" in next.lower() for next in v["next_check"]))

    # ---------- ligne 3 : plus aucune écriture en base ----------

    def test_frozen_db_and_stalled_upstream_is_normal(self):
        # matrice ligne 3 cause 1 : generated stagne + db figée -> amont arrêté (NORMAL)
        p0 = with_flow(base_payload(T0 - SPAN), generated=100, database=100, pending=0)
        p1 = with_flow(base_payload(T0), generated=100, database=100, pending=0)
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "healthy")
        self.assertEqual(v["cause"], "upstream_stopped_normal")

    def test_generated_rising_with_stuck_db_and_reader_lag_is_backlog(self):
        # l'amont monte, la base n'écrit pas, les lecteurs traînent -> file (A)
        p0 = with_flow(base_payload(T0 - SPAN), generated=100, database=100, pending=30)
        p1 = with_flow(with_lag(base_payload(T0), reader=60), generated=132, database=100, pending=30)
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "consumer_backlog")
        self.assertEqual(v["cause"], "reader_backlog")

    def test_postgres_service_down_is_critical_persistence(self):
        p = with_services(base_payload(T0), {"postgres": "down"})
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "persistence")
        self.assertEqual(v["cause"], "service_down:postgres")
        self.assertEqual(v["criticality"], "Critique")
        self.assertTrue(any("tp-db.sh" in n for n in v["next_check"]))

    def test_pg_probe_status_bad_concludes_unavailable(self):
        p = base_payload(T0)
        p["details"]["postgres"]["status"] = "error"
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "persistence")
        self.assertEqual(v["cause"], "postgres_unavailable")

    # ---------- ligne 4 : dépassements SLA ----------

    def test_sla_breaches_with_long_queue_is_upstream_backlog(self):
        p0 = with_flow(base_payload(T0 - SPAN), generated=140, database=100, pending=240)
        counts(p0, validated_sla_breaches=0)
        p1 = with_flow(base_payload(T0), generated=140, database=100, pending=240)
        counts(p1, validated_sla_breaches=12)
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "consumer_backlog")
        self.assertEqual(v["cause"], "upstream_backlog_sla")

    def test_sla_breaches_empty_queue_and_high_latency_is_config(self):
        # matrice ligne 4 cause 2 : file vide + latence > SLA -> configuration trop courte
        p0 = base_payload(T0 - SPAN)
        counts(p0, validated_sla_breaches=0)
        p1 = base_payload(T0)
        counts(p1, validated_sla_breaches=20)
        p1["metrics"]["timings"]["avg_validation_latency_ms"] = 12000.0
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "consumer_backlog")
        self.assertEqual(v["cause"], "sla_config")
        self.assertTrue(any("latence" in s.lower() or "config" in s.lower()
                            for s in (v["summary"],)))

    def test_sla_breaches_cannot_settle_without_signals(self):
        # file vide + latence dans le seuil : on ne tranche pas (dépassés historiques)
        p0 = base_payload(T0 - SPAN)
        counts(p0, validated_sla_breaches=0)
        p1 = base_payload(T0)
        counts(p1, validated_sla_breaches=5)
        v = verdict(p1, now=T0, previous=previous_of(p0, T0))
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "ambiguous_sla")

    # ---------- ligne 5 : verdict degraded ----------

    def test_reader_service_down_named(self):
        p = with_services(base_payload(T0), {"pix-decision-engine": "down"})
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "consumer_backlog")
        self.assertEqual(v["cause"], "service_down:pix-decision-engine")
        plausible = {c["cause"]: c["status"] for c in v["plausible_causes"]}
        self.assertEqual(plausible.get("probe_blinded"), "excluded")

    def test_probe_blinded_when_network_perturbation_active(self):
        # matrice ligne 5 cause 2 : sonde aveuglée par la perturbation -> on ne conclut pas
        p = with_network(with_services(base_payload(T0), {"pix-validator": "down"}), profile="loss")
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "probe_blinded")
        causes = [c["cause"] for c in v["plausible_causes"]]
        self.assertIn("service_down:pix-validator", causes)
        self.assertIn("probe_blinded", causes)

    def test_transversal_service_down_stays_indeterminate(self):
        p = with_services(base_payload(T0), {"influxdb": "down"})
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "service_down_transversal")
        self.assertEqual(v["plausible_causes"][0]["cause"], "service_down:influxdb")

    # ---------- garde-fou fraîcheur (critère #6) ----------

    def test_stale_probes_never_conclude__even_with_strong_symptoms(self):
        # symptôme fort mais sondes vieilles : la fraîcheur gagne (jamais conclure)
        p = base_payload(T0)
        p["timestamp"] = _iso(T0 - 10.0)
        p["details"]["kafka"]["replication"]["captured_at"] = _iso(T0 - 300.0)
        counts(p, pix_validator_lag_total=132)
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "probes_stale")
        self.assertFalse(v["freshness"]["ok"])
        self.assertIn("replication_age_s", v["freshness"])

    def test_missing_pg_details_is_data_missing(self):
        # critère #5 : sans lecture PostgreSQL pas de conclusion du tout
        p = base_payload(T0)
        del p["details"]["postgres"]
        v = verdict(p, now=T0)
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "data_missing")

    # ---------- robustesse du cache d'état ----------

    def test_previous_sample_rejected_when_environment_restarts(self):
        # compteurs qui baissent = nouvel environment : l'échantillon précédent est écarté
        p0 = base_payload(T0)
        counts(p0, generated=500, db_validated_count=500, validated_sla_breaches=40,
               pix_validator_lag_total=140)
        p1 = with_lag(base_payload(T0 + SPAN), reader=140)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        self.assertEqual(v["verdict"], "indeterminate")
        self.assertEqual(v["indeterminate_reason"], "needs_second_sample")

    # ---------- seuils sur chargeables ----------

    def test_threshold_config_can_silence_a_flashy_lag(self):
        # la configuration est lisible : un lag 132 peut être sous le seuil d'alarme
        config = {"lag_total_threshold": 200.0}
        p0 = with_lag(base_payload(T0 - SPAN), reader=132)
        p1 = with_lag(base_payload(T0), reader=133)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN), config=config)
        self.assertEqual(v["verdict"], "healthy")

    # ---------- cohérence de la sortie ----------

    def test_output_keys_and_serializable(self):
        import json
        p0 = with_lag(base_payload(T0 - SPAN), reader=50)
        p1 = with_lag(base_payload(T0), reader=60)
        v = verdict(p1, now=T0 + SPAN, previous=previous_of(p0, T0 + SPAN))
        for key in ("schema", "verdict", "cause", "summary", "notes", "plausible_causes",
                    "evidence", "freshness", "next_check", "based_on", "sample_timestamp"):
            self.assertIn(key, v)
        json.dumps(v, ensure_ascii=False)  # sérialisable
        self.assertEqual(v["based_on"]["samples"], 2)
        self.assertEqual(v["based_on"]["span_s"], SPAN)


# --------------------------------------------------------------------------- #
# helpers de mutation
# --------------------------------------------------------------------------- #


def counts(payload, **kw):
    payload["metrics"]["counts"].update(kw)
    return payload


def group_lag(payload, label, lag_total):
    payload["details"]["kafka"]["consumer_groups"][label]["lag_total"] = lag_total
    return payload


def with_lag(payload, reader=None, persister=None):
    if reader is not None:
        group_lag(payload, "pix-validator", reader)
    if persister is not None:
        group_lag(payload, "persister-valid", persister)
    return payload


def with_persister(payload, lag, gap, db):
    group_lag(payload, "persister-valid", lag)
    payload["metrics"]["gaps"]["persistence_gap_valid"] = gap
    payload["metrics"]["counts"]["db_validated_count"] = db
    payload["metrics"]["counts"]["pending_result_count"] = lag
    return payload


def with_flow(payload, generated, database, pending):
    payload["metrics"]["counts"]["generated"] = generated
    payload["metrics"]["counts"]["db_validated_count"] = database
    payload["metrics"]["counts"]["db_rejected_count"] = 0
    payload["metrics"]["counts"]["pending_result_count"] = pending
    return payload


def with_urp(payload, total, offline=0, count=3):
    rep = payload["details"]["kafka"]["replication"]
    rep["under_replicated_partitions_total"] = total
    payload["details"]["kafka"]["broker_count"] = count
    payload["metrics"]["counts"]["broker_count"] = count
    for i, broker in enumerate(rep["brokers"]):
        broker["under_replicated_partitions"] = max(total - i, 0)
        broker["offline_partitions_count"] = offline if i == 0 else 0
        broker["active_controller_count"] = 1
    return payload


def with_services(payload, services):
    payload["services"].update(services)
    return payload


def with_network(payload, profile="loss"):
    payload["details"]["network"] = {
        "active": 1, "profile": profile, "target_service": "pix-validator",
    }
    return payload


if __name__ == "__main__":
    unittest.main()
