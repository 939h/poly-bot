import csv
import io
import time
import unittest

import momentum_v3


class OptimizerHistoryTests(unittest.TestCase):
    def setUp(self):
        self.original = momentum_v3.optimizer_recommendation_history
        momentum_v3.optimizer_recommendation_history = []

    def tearDown(self):
        momentum_v3.optimizer_recommendation_history = self.original

    @staticmethod
    def golden(config, rate=0.75):
        return {
            "ready": True,
            "candidate_count": 12,
            "recommendation": {
                "config": config,
                "validation": {"samples": 8, "wins": 6, "rate": rate},
                "score": 4,
            },
            "current": {"validation": {"samples": 8, "wins": 4, "rate": 0.5}},
        }

    def test_records_only_recommendation_changes(self):
        first = self.golden({"lookback": 3, "min_high": 2, "threshold": 1.0, "gap_magnitude": 3.0})
        second = self.golden({"lookback": 4, "min_high": 3, "threshold": 1.1, "gap_magnitude": 2.0})
        off = {"ready": False}

        momentum_v3._record_optimizer_recommendations({"btc": first}, off, now=100_000)
        momentum_v3._record_optimizer_recommendations({"btc": first}, off, now=100_100)
        momentum_v3._record_optimizer_recommendations({"btc": second}, off, now=100_200)

        self.assertEqual(len(momentum_v3.optimizer_recommendation_history), 2)
        self.assertEqual(momentum_v3.optimizer_recommendation_history[-1]["config"], second["recommendation"]["config"])

    def test_prunes_records_outside_rolling_window_and_exports_csv(self):
        now = time.time()
        momentum_v3.optimizer_recommendation_history = [
            {"timestamp_unix": now - (momentum_v3.OPPO_OPTIMIZER_HISTORY_HOURS * 3600) - 1, "optimizer": "golden", "asset": "BTC", "config": {}},
        ]
        standard = {
            "ready": True,
            "candidate_count": 3,
            "good_pump_entry_ratio_samples": 12,
            "good_pump_entry_ratio_average": 2.975,
            "good_pump_entry_ratio_median": 1.7,
            "recommendation": {
                "config": {"min_rvol": 1.0, "max_kraken_gap_ratio": 3.0, "min_rebound_ratio": 1.5},
                "validation": {"trades": 6, "wins": 4, "win_rate": 2 / 3, "pnl": 1.25},
                "score": 1.25,
            },
            "current": {"validation": {"trades": 6, "wins": 3, "win_rate": 0.5, "pnl": 0.5}},
        }
        momentum_v3._record_optimizer_recommendations({}, standard, now=now)

        self.assertEqual(len(momentum_v3.optimizer_recommendation_history), 1)
        rows = list(csv.DictReader(io.StringIO(momentum_v3._optimizer_history_csv_bytes().decode())))
        self.assertEqual(rows[0]["optimizer"], "standard")
        self.assertEqual(rows[0]["min_rvol"], "1.0")
        self.assertEqual(rows[0]["validation_pnl"], "1.25")
        self.assertEqual(rows[0]["good_pump_entry_ratio_median"], "1.7")


if __name__ == "__main__":
    unittest.main()
