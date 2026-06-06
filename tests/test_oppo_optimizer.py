import unittest

import momentum_v3


class OppoTradeOptimizerTests(unittest.TestCase):
    def setUp(self):
        self.original_log = momentum_v3.trade_log
        self.original_min = momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES
        self.original_enabled = momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = True
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = 2

    def tearDown(self):
        momentum_v3.trade_log = self.original_log
        momentum_v3.OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = self.original_min
        momentum_v3.OPPO_TRADE_OPTIMIZER_ENABLED = self.original_enabled

    def test_recommends_filters_from_completed_standard_oppo_trades(self):
        momentum_v3.trade_log = []
        for index in range(12):
            profitable = index % 2 == 0
            momentum_v3.trade_log.insert(0, {
                "is_oppo": True,
                "is_golden_oppo": False,
                "entry_rvol": 1.5 if profitable else 0.4,
                "entry_gap_magnitude": 0.4 if profitable else 4.0,
                "entry_rebound_ratio": 2.0,
                "pnl": 1.0 if profitable else -1.0,
            })

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["mode"], "shadow-recommend-only")
        self.assertTrue(result["ready"])
        self.assertEqual(result["trades"], 12)
        self.assertGreater(result["recommendation"]["validation"]["pnl"], 0)
        config = result["recommendation"]["config"]
        self.assertTrue(config["min_rvol"] >= 0.5 or config["max_gap_magnitude"] <= 4.0)

    def test_excludes_golden_oppo_trades(self):
        momentum_v3.trade_log = [{
            "is_oppo": True,
            "is_golden_oppo": True,
            "entry_rvol": 2.0,
            "entry_gap_magnitude": 0.2,
            "entry_rebound_ratio": 2.0,
            "pnl": 5.0,
        }]

        result = momentum_v3._build_oppo_trade_optimizer_snapshot()

        self.assertEqual(result["trades"], 0)
        self.assertFalse(result["ready"])


if __name__ == "__main__":
    unittest.main()
