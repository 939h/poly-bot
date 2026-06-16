import unittest

import momentum_v3


class PumpTradeCrossCheckTests(unittest.TestCase):
    def setUp(self):
        self.original_window = momentum_v3.active_window_start
        self.original_audit = momentum_v3.trade_decision_audit
        self.original_checks = momentum_v3.pump_cross_checks
        self.original_pump_log = momentum_v3.pump_log
        self.original_cvd_enabled = momentum_v3.CVD_OPPO_ENABLED
        momentum_v3.active_window_start = 123
        momentum_v3.trade_decision_audit = {}
        momentum_v3.pump_cross_checks = []
        momentum_v3.pump_log = []

    def tearDown(self):
        momentum_v3.active_window_start = self.original_window
        momentum_v3.trade_decision_audit = self.original_audit
        momentum_v3.pump_cross_checks = self.original_checks
        momentum_v3.pump_log = self.original_pump_log
        momentum_v3.CVD_OPPO_ENABLED = self.original_cvd_enabled

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

    def test_no_trigger_infers_specific_gap_block_at_first_2x_rebound(self):
        momentum_v3._cross_check_successful_pump(
            "btc_no",
            {
                "window_start": 123, "asset": "btc", "side": "no", "max_multiple": 18.09,
                "rebound_2x_at": "17:05:01", "rebound_2x_kraken_gap": 100.0,
                "rebound_2x_kraken_gap_ratio": 2.5, "rebound_2x_cvd_slope": -0.1,
                "rebound_2x_rvol": 0.5, "rebound_2x_decision_events": [],
            },
        )

        check = momentum_v3.pump_cross_checks[0]
        self.assertEqual(check["reason"], "GAP-BLOCK @ 2X")
        self.assertEqual(check["detail"], "at first 2x rebound: 100.0000 >= 48.0000 threshold")
        self.assertEqual(check["rebound_2x_at"], "17:05:01")

    def test_successful_unbought_pump_reports_golden_direction_block(self):
        momentum_v3.record_oppo_trigger(
            "btc_yes", "btc", "yes", 0.14, "GOLDEN-DIR-BLOCK",
            "normal OPPO side YES blocked; Golden direction=NO",
        )

        momentum_v3._cross_check_successful_pump(
            "btc_yes", {"window_start": 123, "asset": "btc", "side": "yes", "max_multiple": 5.7},
        )

        check = momentum_v3.pump_cross_checks[0]
        self.assertEqual(check["reason"], "GOLDEN-DIR-BLOCK")
        self.assertIn("Golden direction=NO", check["detail"])
        self.assertEqual(check["blockers"][0]["status"], "GOLDEN-DIR-BLOCK")

    def test_first_2x_rebound_freezes_gate_snapshot_and_decisions(self):
        momentum_v3.trade_decision_audit[(123, "btc_no")] = {
            "events": [{"status": "CVD-BLOCK", "detail": "slope positive"}],
        }
        original_snapshot = momentum_v3._get_pump_kraken_snapshot
        momentum_v3._get_pump_kraken_snapshot = lambda asset: {
            "kraken_gap": 100.0, "kraken_gap_ratio": 2.5, "cvd_slope": 0.1, "rvol": 0.5,
        }
        tracker = {"asset": "btc", "window_start": 123, "multiple": 2.01, "current": 0.12}
        try:
            momentum_v3._capture_pump_2x_cross_check("btc_no", tracker)
        finally:
            momentum_v3._get_pump_kraken_snapshot = original_snapshot

        self.assertEqual(tracker["rebound_2x_price"], 0.12)
        self.assertEqual(tracker["rebound_2x_kraken_gap"], 100.0)
        self.assertEqual(tracker["rebound_2x_decision_events"][0]["status"], "CVD-BLOCK")

    def test_disabled_cvd_guard_is_not_reported_as_missed_buy_reason(self):
        momentum_v3.CVD_OPPO_ENABLED = False
        momentum_v3._cross_check_successful_pump(
            "xrp_no",
            {
                "window_start": 123, "asset": "xrp", "side": "no", "max_multiple": 10.46,
                "rebound_2x_at": "19:00:01", "rebound_2x_cvd_slope": 0.0,
                "rebound_2x_decision_events": [
                    {"status": "CVD-BLOCK", "detail": "slope=0.000000 expected=negative"},
                ],
            },
        )

        check = momentum_v3.pump_cross_checks[0]
        self.assertEqual(check["reason"], "NO-TRIGGER @ 2X")
        self.assertNotIn("CVD", check["detail"])
        self.assertEqual(check["blockers"], [])

    def test_cross_check_csv_exports_specific_reason_and_history(self):
        momentum_v3.pump_cross_checks = [{
            "time": "17:15:00", "window_start": 123, "asset": "BTC", "side": "NO",
            "max_multiple": 18.09, "reason": "GAP-BLOCK",
            "detail": "at pump tracking start: 100.0000 >= 80.0000 threshold",
            "blockers": [{"status": "GAP-BLOCK", "detail": "100.0000 >= 80.0000 threshold"}],
        }]

        csv_text = momentum_v3._pump_cross_check_csv_bytes().decode()

        self.assertIn("rebound_2x_at,reason,detail,blocker_history", csv_text)
        self.assertIn("GAP-BLOCK,at pump tracking start: 100.0000 >= 80.0000 threshold", csv_text)

    def test_finished_pump_log_records_golden_direction_alignment(self):
        original_setup = momentum_v3._oppo_golden_rvol_setup
        original_update = momentum_v3._update_pump_kraken_snapshot
        momentum_v3._oppo_golden_rvol_setup = lambda asset, side: {
            "side": "yes", "armed": True, "qualified": False,
        }
        momentum_v3._update_pump_kraken_snapshot = lambda tracker: None
        try:
            momentum_v3._record_pump_event(
                "btc_yes",
                {"window_start": 123, "asset": "btc", "side": "yes", "base_price": 0.05,
                 "trough": 0.05, "current": 0.2, "multiple": 4.0, "max_price": 0.2,
                 "max_multiple": 4.0, "status": "SUCCESS"},
                "END",
                "SUCCESS",
            )
        finally:
            momentum_v3._oppo_golden_rvol_setup = original_setup
            momentum_v3._update_pump_kraken_snapshot = original_update

        self.assertEqual(momentum_v3.pump_log[0]["golden_direction"], "YES")
        self.assertEqual(momentum_v3.pump_log[0]["golden_alignment"], "ALIGNED")

    def test_dashboard_shows_finished_pump_golden_direction_not_tracking(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("<th>Status</th><th>Golden Direction</th>", html)
        self.assertIn("const showGoldenAlign=(status==='SUCCESS'||status==='FAILED')", html)
        self.assertIn("`${goldenAlign} ${goldenDir}`", html)

    def test_dashboard_contains_cross_check_card(self):
        self.assertIn("Pump / Trade Cross-Check", momentum_v3._DASHBOARD_HTML)
        self.assertIn("Why No Buy?", momentum_v3._DASHBOARD_HTML)
        self.assertIn('href="/pump-cross-check.csv"', momentum_v3._DASHBOARD_HTML)


if __name__ == "__main__":
    unittest.main()
