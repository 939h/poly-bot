import inspect
import unittest

import momentum_v3


class OppoFlexiConditionTests(unittest.TestCase):
    def setUp(self):
        self.original_open_positions = momentum_v3.open_positions
        self.original_last_entry_ts = momentum_v3.last_entry_ts
        self.original_stats = momentum_v3.stats
        self.original_trade_log = momentum_v3.trade_log
        self.original_rvol_enabled = momentum_v3.OPPO_RVOL_GUARD_ENABLED
        self.original_flexi_enabled = momentum_v3.FLEXI_RVOL_ENABLED
        self.original_volume_snapshot = momentum_v3.get_volume_snapshot
        momentum_v3.open_positions = {}
        momentum_v3.last_entry_ts = {}
        momentum_v3.stats = {"scans": 0, "triggers": 0, "buys": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        momentum_v3.trade_log = []

    def tearDown(self):
        momentum_v3.open_positions = self.original_open_positions
        momentum_v3.last_entry_ts = self.original_last_entry_ts
        momentum_v3.stats = self.original_stats
        momentum_v3.trade_log = self.original_trade_log
        momentum_v3.OPPO_RVOL_GUARD_ENABLED = self.original_rvol_enabled
        momentum_v3.FLEXI_RVOL_ENABLED = self.original_flexi_enabled
        momentum_v3.get_volume_snapshot = self.original_volume_snapshot

    def test_cvd_direction_confirms_matching_side_without_poll_wait(self):
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("yes", 0.01))
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("no", -0.01))

    def test_opposite_cvd_direction_fails_confirmation(self):
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("yes", -0.01))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("no", 0.01))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("yes", 0.0))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("no", 0.0))

    def test_golden_entries_also_run_cvd_guard(self):
        source = inspect.getsource(momentum_v3.scan_markets)
        cvd_guard_index = source.index("if CVD_OPPO_ENABLED:")
        golden_branch_index = source.index("if golden_opportunity:", cvd_guard_index)

        self.assertLess(cvd_guard_index, golden_branch_index)
        self.assertNotIn("and not golden_opportunity", source[cvd_guard_index:golden_branch_index])

    def test_low_rvol_returns_flexi_amount_and_out_flag(self):
        momentum_v3.OPPO_RVOL_GUARD_ENABLED = True
        momentum_v3.FLEXI_RVOL_ENABLED = True
        momentum_v3.get_volume_snapshot = lambda *args: {
            "rvol": 0.1, "average": 100.0, "current": 10.0,
        }

        ok, _, amount, is_out = momentum_v3._oppo_rvol_guard_ok("btc", "yes", 0.1, 600)

        self.assertTrue(ok)
        self.assertTrue(is_out)
        self.assertEqual(amount, momentum_v3.FLEXI_RVOL_BUY_AMOUNT)

    def test_high_rvol_returns_normal_amount_without_out_flag(self):
        momentum_v3.OPPO_RVOL_GUARD_ENABLED = True
        momentum_v3.FLEXI_RVOL_ENABLED = True
        momentum_v3.get_volume_snapshot = lambda *args: {
            "rvol": 10.0, "average": 100.0, "current": 1000.0,
        }

        ok, _, amount, is_out = momentum_v3._oppo_rvol_guard_ok("btc", "yes", 0.1, 600)

        self.assertTrue(ok)
        self.assertFalse(is_out)
        self.assertEqual(amount, momentum_v3.BUY_AMOUNT)

    def test_position_and_trade_log_preserve_ordered_out_conditions(self):
        momentum_v3.open_position(
            "btc_yes_oppo", "token", 0.1, filled_shares=10,
            buy_amount=momentum_v3.FLEXI_RVOL_BUY_AMOUNT,
            entry_out_conditions=["OUT-GAP", "UNKNOWN", "OUT-RVOL", "OUT-GAP", "OUT-CVD"],
        )
        position = momentum_v3.open_positions["btc_yes_oppo"]

        self.assertEqual(position["entry_out_conditions"], ["OUT-RVOL", "OUT-GAP"])
        self.assertEqual(position["cost"], momentum_v3.FLEXI_RVOL_BUY_AMOUNT)

        momentum_v3._record_trade_log("btc_yes_oppo", position, "OPPO-SELL", 0.2, 1.0)
        self.assertEqual(momentum_v3.trade_log[0]["entry_out_conditions"], ["OUT-RVOL", "OUT-GAP"])

    def test_oppo_scan_collects_only_rvol_and_gap_flexi_conditions(self):
        source = inspect.getsource(momentum_v3.scan_markets)

        for condition in momentum_v3.OPPO_OUT_CONDITIONS:
            self.assertIn(f'entry_out_conditions.append("{condition}")', source)
        self.assertIn("oppo_buy_amount = FLEXI_RVOL_BUY_AMOUNT", source)
        self.assertIn("entry_out_conditions=entry_out_conditions", source)
        self.assertIn("if CVD_OPPO_ENABLED:", source)
        self.assertNotIn("if CVD_OPPO_ENABLED and not golden_opportunity", source)
        self.assertIn("_oppo_cvd_slope_confirms(side, cvd_slope)", source)
        self.assertIn('record_oppo_trigger(opp_key, opp_asset, side, opp_price, "CVD-BLOCK", detail)', source)
        self.assertNotIn('entry_out_conditions.append("OUT-CVD")', source)
        self.assertNotIn("CVD-FLEXI", source)
        self.assertNotIn("CVD_OPPO_SLOPE_POLLS", source)

    def test_dashboard_renders_blue_out_condition_badges(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("t.entry_out_conditions", html)
        self.assertIn("p.entry_out_conditions", html)
        for condition in momentum_v3.OPPO_OUT_CONDITIONS:
            self.assertIn(condition, momentum_v3.OPPO_OUT_CONDITIONS)
        self.assertNotIn("OUT-CVD", momentum_v3.OPPO_OUT_CONDITIONS)
        self.assertIn("CVD always blocks", html)

    def test_trade_log_csv_exports_out_conditions(self):
        momentum_v3.trade_log = [{"entry_out_conditions": ["OUT-RVOL", "OUT-GAP"]}]

        csv_text = momentum_v3._trade_log_csv_bytes().decode()

        self.assertIn("entry_out_conditions", csv_text)
        self.assertIn("OUT-RVOL|OUT-GAP", csv_text)


if __name__ == "__main__":
    unittest.main()
