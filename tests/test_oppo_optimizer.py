import unittest

import momentum_v3


class OppoTradeOptimizerTests(unittest.TestCase):
    def setUp(self):
        self.original_log = momentum_v3.pump_log
        self.original_min = momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES
        self.original_enabled = momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = True
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = 2

    def tearDown(self):
        momentum_v3.pump_log = self.original_log
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = self.original_min
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = self.original_enabled

    def test_recommends_filters_using_peak_multiple(self):
        momentum_v3.pump_log = []
        for index in range(12):
            strong = index % 2 == 0
            momentum_v3.pump_log.insert(0, {
                # A pump that ends FAILED can still be the strongest opportunity.
                "status": "FAILED",
                "entry_rvol": 1.5 if strong else 0.4,
                "entry_gap_magnitude": 0.4 if strong else 4.0,
                "max_multiple": 14.0 if strong else 1.2,
            })

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["mode"], "shadow-recommend-only")
        self.assertTrue(result["ready"])
        self.assertEqual(result["samples"], 12)
        self.assertGreater(result["recommendation"]["validation"]["average_max_multiple"], 10.0)
        self.assertEqual(result["recommendation"]["validation"]["highest_max_multiple"], 14.0)
        config = result["recommendation"]["config"]
        self.assertTrue(config["min_rvol"] >= 0.5 or config["max_gap_magnitude"] <= 4.0)

    def test_excludes_tracking_milestones_and_incomplete_metrics(self):
        momentum_v3.pump_log = [
            {"status": "TRACKING", "entry_rvol": 2.0, "entry_gap_magnitude": 0.2, "max_multiple": 14.0},
            {"status": "SUCCESS", "entry_rvol": None, "entry_gap_magnitude": 0.2, "max_multiple": 4.0},
            {"status": "FAILED", "entry_rvol": 1.0, "entry_gap_magnitude": None, "max_multiple": 3.0},
            {"status": "FAILED", "entry_rvol": 1.0, "entry_gap_magnitude": 1.0, "max_multiple": 0.0},
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
                "kraken_gap": 10.0, "kraken_gap_magnitude": 1.5, "cvd_slope": 0.25, "rvol": 2.0,
            }
            tracker = {
                "asset": "btc", "trough": 0.06, "base_price": 0.06, "current": 0.06,
                "max_price": 0.12, "max_multiple": 2.0, "highest_milestone": 2,
                "entry_gap_magnitude": 4.0, "entry_cvd_slope": -0.1, "entry_rvol": 0.4,
            }

            momentum_v3._refresh_pump_tracker_price("btc_yes", tracker)

            self.assertEqual(tracker["trough"], 0.05)
            self.assertEqual(tracker["entry_rvol"], 2.0)
            self.assertEqual(tracker["entry_gap_magnitude"], 1.5)
            self.assertEqual(tracker["max_multiple"], 1.0)
        finally:
            momentum_v3.live_prices = original_prices
            momentum_v3._get_pump_kraken_snapshot = original_snapshot


if __name__ == "__main__":
    unittest.main()
