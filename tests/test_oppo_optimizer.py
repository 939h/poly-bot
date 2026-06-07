import unittest

import momentum_v3


class OppoTradeOptimizerTests(unittest.TestCase):
    def setUp(self):
        self.original_log = momentum_v3.pump_log
        self.original_min = momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES
        self.original_enabled = momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED
        self.original_equivalence = momentum_v3.OPPO_OPTIMIZER_SCORE_EQUIVALENCE
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = True
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = 2

    def tearDown(self):
        momentum_v3.pump_log = self.original_log
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = self.original_min
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = self.original_enabled
        momentum_v3.OPPO_OPTIMIZER_SCORE_EQUIVALENCE = self.original_equivalence

    def test_migrates_legacy_gap_magnitude_field_names(self):
        legacy = {
            "pump_log": [{"kraken_gap_magnitude": 1.2, "entry_gap_magnitude": 0.8}],
            "config": {"max_gap_magnitude": 5.0},
        }

        migrated = momentum_v3._migrate_gap_ratio_names(legacy)

        self.assertEqual(migrated["pump_log"][0]["kraken_gap_ratio"], 1.2)
        self.assertEqual(migrated["pump_log"][0]["entry_kraken_gap_ratio"], 0.8)
        self.assertEqual(migrated["config"]["max_kraken_gap_ratio"], 5.0)
        self.assertNotIn("entry_gap_magnitude", migrated["pump_log"][0])

    def test_recommends_filters_using_peak_multiple(self):
        momentum_v3.pump_log = []
        for index in range(12):
            strong = index % 2 == 0
            momentum_v3.pump_log.insert(0, {
                # A pump that ends FAILED can still be the strongest opportunity.
                "status": "FAILED",
                "entry_rvol": 1.5 if strong else 0.4,
                "entry_kraken_gap_ratio": 0.4 if strong else 4.0,
                "max_multiple": 14.0 if strong else 1.2,
            })

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["mode"], "shadow-recommend-only")
        self.assertTrue(result["ready"])
        self.assertEqual(result["samples"], 12)
        self.assertGreater(result["recommendation"]["validation"]["average_max_multiple"], 10.0)
        self.assertEqual(result["recommendation"]["validation"]["highest_max_multiple"], 14.0)
        config = result["recommendation"]["config"]
        self.assertTrue(config["min_rvol"] >= 0.5 or config["max_kraken_gap_ratio"] <= 4.0)

    def test_keeps_valid_one_x_samples_and_caps_outlier_influence(self):
        momentum_v3.pump_log = []
        for multiple in (30.0, 1.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0, 4.0, 4.0):
            momentum_v3.pump_log.insert(0, {
                "status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": 0.1,
                "max_multiple": multiple, "observation_secs": 120, "price_updates": 20,
            })

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()
        current = result["current"]["validation"]

        self.assertEqual(result["samples"], 10)
        self.assertGreater(result["current"]["train"]["no_pumps"] + current["no_pumps"], 0)
        self.assertLessEqual(current["capped_average_max_multiple"], momentum_v3.OPPO_OPTIMIZER_MAX_MULTIPLE_CAP)
        self.assertNotEqual(result["current"]["score"], result["current"]["validation"]["average_max_multiple"])

    def test_excludes_short_or_under_observed_pumps(self):
        momentum_v3.pump_log = [
            {"status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": 1.0, "max_multiple": 1.0, "observation_secs": 10, "price_updates": 20},
            {"status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": 1.0, "max_multiple": 1.0, "observation_secs": 120, "price_updates": 1},
        ]

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["samples"], 0)
        self.assertEqual(result["quality_excluded"], 2)


    def test_recommends_without_weak_validation_outcomes_but_reports_warning(self):
        momentum_v3.pump_log = [
            {"status": "SUCCESS", "entry_rvol": 0.2, "entry_kraken_gap_ratio": 1.0, "max_multiple": 4.0}
            for _ in range(12)
        ]

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertTrue(result["ready"])
        self.assertFalse(result["outcome_diverse"])
        self.assertIsNotNone(result["recommendation"])
        self.assertIn("no weak/failed pumps", result["outcome_warning"])
        self.assertIsNone(result["readiness_reason"])

    def test_recommendation_caps_ratio_at_good_pump_entry_median(self):
        ratios = (1.2, 0.5, 2.2, 8.0)
        chronological = []
        for index in range(12):
            ratio = ratios[index % len(ratios)]
            chronological.append({
                "status": "FAILED", "entry_rvol": 0.5,
                "entry_kraken_gap_ratio": ratio, "max_multiple": 4.0,
            })
        momentum_v3.pump_log = list(reversed(chronological))

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["good_pump_entry_ratio_median"], 1.7)
        self.assertEqual(result["good_pump_entry_ratio_average"], 2.975)
        self.assertTrue(result["ready"])
        self.assertLessEqual(result["recommendation"]["config"]["max_kraken_gap_ratio"], 1.7)

    def test_equivalent_scores_prefer_conservative_gap(self):
        momentum_v3.OPPO_OPTIMIZER_SCORE_EQUIVALENCE = 0.10
        chronological = []
        for index in range(12):
            chronological.append({
                "status": "FAILED", "entry_rvol": 0.2, "entry_kraken_gap_ratio": 4.0,
                "max_multiple": 1.2 if index in (9, 11) else 4.0,
            })
        momentum_v3.pump_log = list(reversed(chronological))

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertTrue(result["ready"])
        self.assertEqual(result["recommendation"]["config"]["max_kraken_gap_ratio"], 4.0)

    def test_pump_tracker_stops_at_dead_zone_and_preserves_prior_peak(self):
        originals = (momentum_v3.ASSETS, momentum_v3.live_prices, momentum_v3.pump_tracker,
                     momentum_v3.pump_log, momentum_v3.pump_finished_tracker_keys)
        try:
            momentum_v3.ASSETS = ["btc"]
            momentum_v3.live_prices = {"btc_yes": 0.03}
            momentum_v3.pump_log = []
            momentum_v3.pump_finished_tracker_keys = set()
            momentum_v3.pump_tracker = {
                "btc_yes": {
                    "asset": "btc", "side": "yes", "window_start": 1000,
                    "base_price": 0.05, "trough": 0.05, "current": 0.10,
                    "multiple": 2.0, "max_price": 0.10, "max_multiple": 2.0,
                    "entry_ts": momentum_v3.time.time() - 800, "price_updates": 10,
                    "entry_rvol": 0.5, "entry_kraken_gap_ratio": 1.0,
                }
            }

            momentum_v3.update_pump_trackers(1000, 850)

            self.assertNotIn("btc_yes", momentum_v3.pump_tracker)
            self.assertEqual(momentum_v3.pump_log[0]["finish_reason"], "DEAD-ZONE")
            self.assertEqual(momentum_v3.pump_log[0]["max_multiple"], 2.0)
            self.assertEqual(momentum_v3.pump_log[0]["current"], 0.03)
            self.assertTrue(momentum_v3._pump_tracker_already_finished(1000, "btc_yes"))
        finally:
            (momentum_v3.ASSETS, momentum_v3.live_prices, momentum_v3.pump_tracker,
             momentum_v3.pump_log, momentum_v3.pump_finished_tracker_keys) = originals


    def test_window_boundary_records_full_window_result(self):
        originals = (momentum_v3.live_prices, momentum_v3.pump_tracker, momentum_v3.pump_log,
                     momentum_v3.pump_finished_tracker_keys)
        try:
            momentum_v3.live_prices = {"btc_yes": 0.08}
            momentum_v3.pump_log = []
            momentum_v3.pump_finished_tracker_keys = set()
            momentum_v3.pump_tracker = {
                "btc_yes": {
                    "asset": "btc", "side": "yes", "window_start": 1000,
                    "base_price": 0.05, "trough": 0.05, "current": 0.05,
                    "multiple": 1.0, "max_price": 0.05, "max_multiple": 1.0,
                    "entry_ts": momentum_v3.time.time() - 900, "price_updates": 20,
                    "entry_rvol": 0.5, "entry_kraken_gap_ratio": 1.0,
                }
            }

            momentum_v3._finish_window_pump_trackers(1000)

            self.assertNotIn("btc_yes", momentum_v3.pump_tracker)
            self.assertEqual(momentum_v3.pump_log[0]["finish_reason"], "FULL-WINDOW")
            self.assertEqual(momentum_v3.pump_log[0]["max_multiple"], 1.6)
        finally:
            (momentum_v3.live_prices, momentum_v3.pump_tracker, momentum_v3.pump_log,
             momentum_v3.pump_finished_tracker_keys) = originals

    def test_all_configs_csv_exports_every_candidate(self):
        momentum_v3.pump_log = [
            {"status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": 1.0, "max_multiple": 2.0}
            for _ in range(12)
        ]

        csv_text = momentum_v3._optimizer_configs_csv_bytes().decode()

        self.assertIn("recommended,min_rvol,max_kraken_gap_ratio,good_pump_entry_ratio_median,good_pump_entry_ratio_average,good_pump_entry_ratio_samples,score", csv_text)
        self.assertEqual(len(csv_text.strip().splitlines()), 31)

    def test_excludes_tracking_milestones_and_incomplete_metrics(self):
        momentum_v3.pump_log = [
            {"status": "TRACKING", "entry_rvol": 2.0, "entry_kraken_gap_ratio": 0.2, "max_multiple": 14.0},
            {"status": "SUCCESS", "entry_rvol": None, "entry_kraken_gap_ratio": 0.2, "max_multiple": 4.0},
            {"status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": None, "max_multiple": 3.0},
            {"status": "FAILED", "entry_rvol": 1.0, "entry_kraken_gap_ratio": 1.0, "max_multiple": 0.0},
        ]

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["samples"], 0)
        self.assertFalse(result["ready"])


    def test_new_trough_resnapshots_entry_conditions_for_later_peak(self):
        original_prices = momentum_v3.live_prices
        original_snapshot = momentum_v3._get_pump_kraken_snapshot
        try:
            momentum_v3.live_prices = {"btc_yes": 0.05}
            momentum_v3._get_pump_kraken_snapshot = lambda asset: {
                "kraken_gap": 10.0, "kraken_gap_ratio": 1.5, "cvd_slope": 0.25, "rvol": 2.0,
            }
            tracker = {
                "asset": "btc", "trough": 0.06, "base_price": 0.06, "current": 0.06,
                "max_price": 0.12, "max_multiple": 2.0, "highest_milestone": 2,
                "entry_kraken_gap_ratio": 4.0, "entry_cvd_slope": -0.1, "entry_rvol": 0.4,
            }

            momentum_v3._refresh_pump_tracker_price("btc_yes", tracker)

            self.assertEqual(tracker["trough"], 0.05)
            self.assertEqual(tracker["entry_rvol"], 2.0)
            self.assertEqual(tracker["entry_kraken_gap_ratio"], 1.5)
            self.assertEqual(tracker["max_multiple"], 1.0)
        finally:
            momentum_v3.live_prices = original_prices
            momentum_v3._get_pump_kraken_snapshot = original_snapshot


if __name__ == "__main__":
    unittest.main()
