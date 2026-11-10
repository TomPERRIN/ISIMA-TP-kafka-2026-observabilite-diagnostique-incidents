"""Tests des angles morts simulés (blind_spots.py).

Contrat : chaque cas du catalogue « angles morts » (cas-incidents.md §1 B3/B4/C3/C4,
§5 registre des risques) doit produire le verdict attendu du VRAI moteur. Si un
angle mort se FERME (le moteur évolue), ces tests lèvent l'alerte : il faudra
relire la limite correspondante dans les docs — c'est une bonne nouvelle, mais
elle doit être relayée, pas ignorée.

Tous les payloads de simulation portent des sondes FRAÎCHES : c'est le point de la
démonstration (le garde-fou fraîcheur ne voit que le PÉRIMÉ, pas le mensonge).
Chaque verdict est donc vérifié avec freshness.ok == True.

Exécuter depuis le dépôt : cd scripts/lib && python3 -m unittest test_blind_spots
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import blind_spots as bs  # noqa: E402
import diagnostic_export as de  # noqa: E402 (protocole réutilisé par l'export)


def _run(case_id: str) -> dict:
    case = next(c for c in bs.CASES if c["id"] == case_id)
    return bs.run_case(case, now=bs.time.time())


class BlindSpotVerdictTest(unittest.TestCase):
    """Chaque cas produit le verdict attendu du moteur (dérive = alerte)."""

    def test_b4a_controller_lost_without_urp_is_invisible(self):
        run = _run("B4a")
        self.assertEqual(run["final"]["verdict"], "healthy")
        self.assertIsNone(run["final"]["cause"])
        self.assertEqual(run["outcome"], "❌ angle mort")

    def test_b4b_controller_lost_with_urp_is_covered(self):
        run = _run("B4b")
        self.assertEqual(run["final"]["verdict"], "kafka_replication")
        self.assertEqual(run["final"]["cause"], "broker_down")

    def test_b3_rebalancing_family_right_but_cause_generic(self):
        run = _run("B3")
        first, second = run["steps"][0]["verdict"], run["steps"][1]["verdict"]
        self.assertEqual(first["verdict"], "kafka_replication")
        self.assertEqual(first["cause"], "replication_catchup")
        # la cause fine « rééquilibrage » n'existe pas dans le catalogue
        self.assertNotIn("rebalanc", json.dumps(first))
        self.assertEqual(second["verdict"], "healthy")

    def test_c3a_light_saturation_is_invisible(self):
        run = _run("C3a")
        self.assertEqual(run["final"]["verdict"], "healthy")
        self.assertIsNone(run["final"]["cause"])

    def test_c3b_growing_gap_names_persister_slow_not_saturation(self):
        run = _run("C3b")
        self.assertEqual(run["final"]["verdict"], "persistence")
        self.assertEqual(run["final"]["cause"], "persister_slow")

    def test_c4a_failed_write_refuses_but_does_not_name_the_cause(self):
        run = _run("C4a")
        self.assertEqual(run["final"]["verdict"], "indeterminate")
        self.assertEqual(run["final"]["indeterminate_reason"],
                         "contradictory_signals")
        self.assertIsNone(run["final"]["cause"])

    def test_c4b_lying_fresh_probe_is_undetected(self):
        run = _run("C4b")
        self.assertEqual(run["final"]["verdict"], "healthy")
        self.assertIsNone(run["final"]["cause"])

    def test_a0_missing_offset_is_invisible(self):
        run = _run("A0")
        self.assertEqual(run["final"]["verdict"], "healthy")
        self.assertIsNone(run["final"]["cause"])

    def test_t0_transversal_service_is_a_witness_not_an_incident(self):
        run = _run("T0")
        self.assertEqual(run["final"]["verdict"], "indeterminate")
        self.assertEqual(run["final"]["indeterminate_reason"],
                         "service_down_transversal")


class BlindSpotContractTest(unittest.TestCase):
    """Le harnais lui-même : sondes fraîches, mapping complet, rendu."""

    def test_every_case_has_an_expected_verdict_for_tests(self):
        for case in bs.CASES:
            with self.subTest(case=case["id"]):
                self.assertIn("expected", case)
                self.assertIn("verdict", case["expected"])

    def test_every_simulated_probe_is_fresh(self):
        for case in bs.CASES:
            run = bs.run_case(case, now=bs.time.time())
            with self.subTest(case=case["id"]):
                for step in run["steps"]:
                    self.assertTrue(step["verdict"]["freshness"]["ok"],
                                    "%s : sonde simulée périmée" % case["id"])

    def test_multi_step_cases_have_a_span_of_12s(self):
        for case_id in ("B3", "C3a", "C3b", "C4a", "C4b"):
            run = _run(case_id)
            with self.subTest(case=case_id):
                self.assertEqual(len(run["steps"]), 2)
                self.assertEqual(run["final"]["based_on"]["span_s"], 12.0)

    def test_all_nine_cases_are_declared(self):
        ids = [case["id"] for case in bs.CASES]
        self.assertEqual(ids, ["B4a", "B4b", "B3", "C3a", "C3b",
                               "C4a", "C4b", "A0", "T0"])

    def test_case_prefix_matching(self):
        self.assertEqual([c["id"] for c in bs._match(bs.CASES, "B4")],
                         ["B4a", "B4b"])
        self.assertEqual([c["id"] for c in bs._match(bs.CASES, "C3")],
                         ["C3a", "C3b"])

    def test_unknown_case_is_rejected(self):
        with self.assertRaises(SystemExit):
            bs._match(bs.CASES, "Z9")

    def test_render_text_and_summary(self):
        runs = [bs.run_case(case, now=bs.time.time()) for case in bs.CASES]
        for run in runs:
            text = bs.render_case(run)
            self.assertIn("Limite", text)
            self.assertIn("Statut", text)
        summary = bs.render_summary(runs)
        self.assertIn("garantit", summary)
        self.assertIn("C4b", summary)


class BlindSpotExportTest(unittest.TestCase):
    """Mode --export/--print : 14 lignes InfluxDB, protocole de l'exporteur."""

    def test_collect_lines_produces_one_valid_line_per_step(self):
        runs = [bs.run_case(case, now=bs.time.time()) for case in bs.CASES]
        items = bs.collect_lines(runs)
        self.assertEqual(len(items), 14)
        for item in items:
            with self.subTest(case=item["case"], label=item["label"]):
                self.assertTrue(item["line"].startswith(
                    "simulpix_diagnostic,verdict_family="))
                self.assertIn("state_code=", item["line"])
                self.assertRegex(item["line"], r" \d+$")  # timestamp final (ns)

    def test_export_covers_every_step_and_family_count(self):
        runs = [bs.run_case(case, now=bs.time.time()) for case in bs.CASES]
        counts: dict = {}
        for run in runs:
            for step in run["steps"]:
                family = step["verdict"]["verdict"]
                counts[family] = counts.get(family, 0) + 1
        self.assertEqual(counts, {"healthy": 9, "kafka_replication": 2,
                                  "persistence": 1, "indeterminate": 2})

    def test_print_mode_outputs_the_14_lines_without_sending(self):
        import io
        from contextlib import redirect_stdout
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = bs.main(["--print"])
        self.assertEqual(code, 0)
        lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 14)
        self.assertTrue(lines[0].startswith("simulpix_diagnostic,verdict_family="))

    def _quiet(self, items, hold_s, url, org, bucket, token):
        """export_to_grafana sans polluer la sortie des tests."""
        import io
        from contextlib import redirect_stdout
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            return bs.export_to_grafana(items, hold_s, url, org, bucket, token)

    def test_export_pushes_every_state(self):
        items = bs.collect_lines(
            [bs.run_case(case, now=bs.time.time()) for case in bs.CASES])
        calls: list = []
        original = de.send_line
        de.send_line = (lambda line, url, org, bucket, token, timeout=5.0:
                        calls.append(line) or True)
        try:
            pushed = self._quiet(items, 0, "http://localhost:8086",
                                 "simulpix", "simulpix_observability", "t")
        finally:
            de.send_line = original
        self.assertEqual(pushed, 14)
        self.assertEqual(len(calls), 14)

    def test_export_aborts_on_first_write_failure(self):
        items = bs.collect_lines(
            [bs.run_case(case, now=bs.time.time()) for case in bs.CASES])
        calls: list = []
        original = de.send_line

        def failing(line, url, org, bucket, token, timeout=5.0):
            calls.append(line)
            return len(calls) < 3  # échec à la 3e écriture

        de.send_line = failing
        try:
            pushed = self._quiet(items, 0, "http://localhost:8086",
                                 "simulpix", "simulpix_observability", "t")
        finally:
            de.send_line = original
        self.assertEqual(pushed, 2)
        self.assertEqual(len(calls), 3)

    def test_export_sleeps_between_states_not_after_the_last(self):
        items = bs.collect_lines(
            [bs.run_case(case, now=bs.time.time()) for case in bs.CASES])
        sleeps: list = []
        original_send, original_sleep = de.send_line, bs.time.sleep
        de.send_line = lambda line, url, org, bucket, token, timeout=5.0: True
        bs.time.sleep = lambda s: sleeps.append(s)
        try:
            pushed = self._quiet(items, 5.0, "http://localhost:8086",
                                 "simulpix", "simulpix_observability", "t")
        finally:
            de.send_line, bs.time.sleep = original_send, original_sleep
        self.assertEqual(pushed, 14)
        self.assertEqual(len(sleeps), 13)  # pas après le dernier état


if __name__ == "__main__":
    unittest.main()
