import inspect
import unittest

import momentum_v3


class OppoCvdStopCountdownTests(unittest.TestCase):
    def test_legacy_short_cvd_import_remains_available_for_mixed_deployments(self):
        import kraken_ws

        self.assertTrue(callable(kraken_ws.get_short_cvd_slope))
        self.assertEqual(kraken_ws.get_short_cvd_slope("btc"), kraken_ws.get_cvd_snapshot("btc")[2])

    def test_oppo_stop_snapshots_cvd_and_restarts_on_improvement_from_baseline(self):
        source = inspect.getsource(momentum_v3.manage_positions)

        self.assertIn("OPPO_STOP_LOSS_COUNTDOWN_SECS", source)
        self.assertIn('_, cvd_at_stop, _ = get_cvd_snapshot', source)
        self.assertIn('pos["force_stop_cvd_baseline"] = cvd_at_stop', source)
        self.assertIn("cvd_change = current_cvd - baseline_cvd", source)
        self.assertIn("cvd_improving = _oppo_cvd_slope_confirms(side, cvd_change)", source)
        self.assertIn('pos["force_stop_triggered"] = now', source)
        self.assertIn('pos["force_stop_cvd_baseline"] = current_cvd', source)
        self.assertIn("oppo_stop_active and current_price < entry", source)
        self.assertNotIn("get_short_cvd_slope", source)

    def test_successful_cvd_stop_recovery_sells_all_at_breakeven_and_logs_once(self):
        source = inspect.getsource(momentum_v3.manage_positions)

        self.assertIn('cvd_recovered = bool(pos.get("is_oppo") and cvd_restarts > 0 and current_price >= entry)', source)
        self.assertIn('client, pos["token_id"], shares, current_price, key.upper()', source)
        self.assertIn('_record_trade_log(key, pos, "CVD-BREAKEVEN-SELL", current_price, pnl)', source)
        self.assertIn('not pos.get("force_stop_cvd_recovery_logged", False)', source)
        self.assertIn('"CVD-RECOVERED"', source)
        self.assertIn('pos["force_stop_cvd_recovery_logged"] = True', source)
        self.assertIn("'CVD-RECOVERED'", momentum_v3._DASHBOARD_HTML)
        self.assertIn("'CVD-BREAKEVEN-SELL'", momentum_v3._DASHBOARD_HTML)

    def test_yes_and_no_use_change_from_stop_baseline(self):
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("yes", -0.25 - -0.40))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("yes", -0.50 - -0.40))
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("no", 0.25 - 0.40))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("no", 0.50 - 0.40))

    def test_dashboard_explains_stop_start_cvd_baseline(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("oppo_stop_loss_countdown_secs", html)
        self.assertIn("snapshots CVD at stop start", html)
        self.assertIn("sell all remaining shares at breakeven", html)
        self.assertNotIn("oppo_stop_cvd_window_secs", html)


if __name__ == "__main__":
    unittest.main()
