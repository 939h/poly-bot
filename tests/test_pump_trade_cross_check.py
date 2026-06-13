import unittest

import momentum_v3


class PumpTradeCrossCheckTests(unittest.TestCase):
    def setUp(self):
        self.original_trackers = momentum_v3.pump_tracker
        self.original_log = momentum_v3.pump_log
        self.original_finished = momentum_v3.pump_finished_tracker_keys
        momentum_v3.pump_tracker = {}
        momentum_v3.pump_log = []
        momentum_v3.pump_finished_tracker_keys = set()

    def tearDown(self):
        momentum_v3.pump_tracker = self.original_trackers
        momentum_v3.pump_log = self.original_log
        momentum_v3.pump_finished_tracker_keys = self.original_finished

    def _successful_tracker(self):
        return {
            "asset": "btc",
            "side": "yes",
            "window_start": 900,
            "base_price": 0.05,
            "trough": 0.05,
            "current": 0.12,
            "multiple": 2.4,
            "max_price": 0.12,
            "max_multiple": 2.4,
            "entry_ts": 1,
            "price_updates": 10,
            "trade_placed": False,
            "trade_cross_checks": [],
        }

    def test_successful_missed_pump_reports_gap_block_values(self):
        tracker = self._successful_tracker()
        momentum_v3.pump_tracker["btc_yes"] = tracker

        momentum_v3.record_oppo_trigger(
            "btc_yes", "btc", "yes", 0.10, "GAP-BLOCK",
            "0.0100>=0.0080 mag=1.20x ratio=1.50x",
        )
        momentum_v3._finish_pump_tracker("btc_yes", tracker, "FULL-WINDOW")

        event = momentum_v3.pump_log[0]
        self.assertEqual(event["status"], "SUCCESS")
        self.assertFalse(event["trade_placed"])
        self.assertIn("GAP-BLOCK", event["trade_cross_check"])
        self.assertIn("0.0100>=0.0080", event["trade_cross_check"])

    def test_successful_pump_with_buy_is_marked_as_traded(self):
        tracker = self._successful_tracker()
        tracker["trade_placed"] = True

        result = momentum_v3._pump_trade_cross_check(tracker)

        self.assertTrue(result["trade_placed"])
        self.assertEqual(result["reason"], "trade placed")

    def test_cvd_block_is_captured_even_when_suppressed_from_oppo_log(self):
        tracker = self._successful_tracker()
        momentum_v3.pump_tracker["btc_yes"] = tracker

        momentum_v3.record_oppo_trigger(
            "btc_yes", "btc", "yes", 0.10, "CVD-BLOCK",
            "slope=-0.123000 expected=positive win=-5.00",
        )

        result = momentum_v3._pump_trade_cross_check(tracker)
        self.assertIn("CVD-BLOCK", result["reason"])
        self.assertIn("slope=-0.123000", result["reason"])


if __name__ == "__main__":
    unittest.main()
