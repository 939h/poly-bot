import unittest

import momentum_v3


class PumpTradeCrossCheckTests(unittest.TestCase):
    def setUp(self):
        self.original_window = momentum_v3.active_window_start
        self.original_audit = momentum_v3.trade_decision_audit
        self.original_checks = momentum_v3.pump_cross_checks
        momentum_v3.active_window_start = 123
        momentum_v3.trade_decision_audit = {}
        momentum_v3.pump_cross_checks = []

    def tearDown(self):
        momentum_v3.active_window_start = self.original_window
        momentum_v3.trade_decision_audit = self.original_audit
        momentum_v3.pump_cross_checks = self.original_checks

    def test_successful_unbought_pump_reports_exact_gap_block(self):
        momentum_v3.record_oppo_trigger(
            "btc_yes", "btc", "yes", 0.08, "GAP-BLOCK",
            "100.0000>=80.0000 mag=2.00x ratio=2.50x",
        )

        momentum_v3._cross_check_successful_pump(
            "btc_yes", {"window_start": 123, "asset": "btc", "side": "yes", "max_multiple": 4.2},
        )

        check = momentum_v3.pump_cross_checks[0]
        self.assertEqual(check["reason"], "GAP-BLOCK")
        self.assertIn("100.0000>=80.0000", check["detail"])

    def test_matching_buy_excludes_pump_from_cross_check(self):
        momentum_v3.record_oppo_trigger("eth_no", "eth", "no", 0.09, "CVD-BLOCK", "slope positive")
        momentum_v3.record_oppo_trigger("eth_no", "eth", "no", 0.10, "BOUGHT", "success")

        momentum_v3._cross_check_successful_pump(
            "eth_no", {"window_start": 123, "asset": "eth", "side": "no", "max_multiple": 3.1},
        )

        self.assertEqual(momentum_v3.pump_cross_checks, [])

    def test_dashboard_contains_cross_check_card(self):
        self.assertIn("Pump / Trade Cross-Check", momentum_v3._DASHBOARD_HTML)
        self.assertIn("Why No Buy?", momentum_v3._DASHBOARD_HTML)


if __name__ == "__main__":
    unittest.main()
