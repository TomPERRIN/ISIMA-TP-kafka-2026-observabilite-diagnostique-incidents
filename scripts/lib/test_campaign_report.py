"""Tests du mapping et de la couverture de campagne (campaign_report.py).

La couverture « problèmes étudiés » est un contrat : chaque famille d'incident
du sujet — A file qui grossit, B réplication, C persistance, D traitement lent
(symptôme 2 de la matrice v2) — doit rester attendue par au moins un cas de la
campagne, sans quoi le rapport tomberait à 3/4 sans qu'on s'en aperçoive.

Exécuter depuis le dépôt : cd scripts/lib && python3 -m unittest test_campaign_report
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import campaign_report as cr  # noqa: E402


class CampaignMappingTest(unittest.TestCase):
    """Le mapping « cas -> famille attendue » couvre toutes les familles."""

    def test_every_incident_family_is_expected_by_a_case(self):
        for family in cr.INCIDENT_FAMILIES:
            with self.subTest(family=family):
                matching = [p for p, f in cr.EXPECTED_BY_PREFIX if f == family]
                self.assertTrue(
                    matching,
                    "aucun cas de campagne n'attend la famille %s" % family)

    def test_expected_families_are_known(self):
        for prefix, family in cr.EXPECTED_BY_PREFIX:
            if family is not None:
                self.assertIn(family, cr.INCIDENT_FAMILIES,
                              "cas %s : famille %s inconnue" % (prefix, family))

    def test_expected_family_by_prefix(self):
        self.assertIsNone(cr.expected_family("temoin-1"))
        self.assertEqual(cr.expected_family("A-retard-lecture"), "consumer_backlog")
        self.assertEqual(cr.expected_family("B-replication"), "kafka_replication")
        self.assertEqual(cr.expected_family("C-postgres-indispo"), "persistence")
        self.assertEqual(cr.expected_family("C-postgres-degrade"), "persistence")
        self.assertEqual(cr.expected_family("D-traitement-lent"), "slow_processing")

    def test_unknown_case_expects_nothing(self):
        self.assertIsNone(cr.expected_family("Z-inconnu"))
        self.assertIsNone(cr.expected_family(""))


def _analysis(case, family, counts, witness=False, fp=None, reachable=1):
    """Analyse de cas synthétique (format de sortie d'analyse_case)."""
    result = {
        "case": case,
        "expected_family": family,
        "is_witness": witness,
        "verdict_counts": counts,
        "reachable": reachable,
    }
    if fp is not None:
        result["false_positives"] = fp
    return result


class CoverageTest(unittest.TestCase):
    """La couverture signale les familles non exercées (run partiel --cases)."""

    def test_complete_campaign_is_4_over_4(self):
        analyses = [
            _analysis("A-retard-lecture", "consumer_backlog", {"consumer_backlog": 9}),
            _analysis("B-replication", "kafka_replication", {"kafka_replication": 2}),
            _analysis("C-postgres-degrade", "persistence", {"persistence": 3}),
            _analysis("D-traitement-lent", "slow_processing", {"slow_processing": 2}),
        ]
        cov = cr.coverage(analyses)
        self.assertEqual(cov["families_exerciced"], "4/4")
        self.assertTrue(cov["complete"])

    def test_partial_run_reports_missing_families(self):
        # --cases A : la justesse dit 1/1, la couverture doit dire 1/4
        analyses = [_analysis("A-retard-lecture", "consumer_backlog",
                              {"consumer_backlog": 9})]
        cov = cr.coverage(analyses)
        self.assertEqual(cov["families_exerciced"], "1/4")
        self.assertFalse(cov["complete"])
        missing = sorted(r["family"] for r in cov["families"] if not r["exerciced"])
        self.assertEqual(missing,
                         ["kafka_replication", "persistence", "slow_processing"])

    def test_case_launched_but_family_absent_is_not_exerciced(self):
        # le cas a tourné mais la famille attendue n'est jamais sortie
        analyses = [_analysis("D-traitement-lent", "slow_processing",
                              {"indeterminate": 5})]
        cov = cr.coverage(analyses)
        row = next(r for r in cov["families"]
                   if r["family"] == "slow_processing")
        self.assertFalse(row["exerciced"])

    def test_expected_case_seen_in_a_foreign_case_is_not_enough(self):
        # la famille vue par un cas qui ne l'attend pas ne prouve rien
        analyses = [
            _analysis("A-retard-lecture", "consumer_backlog", {"consumer_backlog": 9}),
            _analysis("D-traitement-lent", "slow_processing",
                      {"consumer_backlog": 3, "indeterminate": 2}),
        ]
        cov = cr.coverage(analyses)
        row = next(r for r in cov["families"] if r["family"] == "slow_processing")
        self.assertFalse(row["exerciced"])
        self.assertEqual(cov["families_exerciced"], "1/4")

    def test_witness_and_indeterminate_are_reported(self):
        analyses = [
            _analysis("temoin-1", None, {"healthy": 14},
                      witness=True, fp=0, reachable=14),
            _analysis("A-retard-lecture", "consumer_backlog",
                      {"consumer_backlog": 9, "indeterminate": 1}),
        ]
        cov = cr.coverage(analyses)
        self.assertTrue(cov["temoin_zero_fp"])
        self.assertTrue(cov["indetermine_explicite"])

    def test_no_witness_is_reported_false(self):
        analyses = [_analysis("A-retard-lecture", "consumer_backlog",
                              {"consumer_backlog": 9})]
        cov = cr.coverage(analyses)
        self.assertFalse(cov["temoin_zero_fp"])


if __name__ == "__main__":
    unittest.main()
