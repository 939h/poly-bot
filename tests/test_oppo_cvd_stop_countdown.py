import inspect
import unittest

import momentum_v3


class OppoCvdStopCountdownTests(unittest.TestCase):
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

    def test_yes_and_no_use_change_from_stop_baseline(self):
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("yes", -0.25 - -0.40))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("yes", -0.50 - -0.40))
        self.assertTrue(momentum_v3._oppo_cvd_slope_confirms("no", 0.25 - 0.40))
        self.assertFalse(momentum_v3._oppo_cvd_slope_confirms("no", 0.50 - 0.40))

    def test_dashboard_explains_stop_start_cvd_baseline(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("oppo_stop_loss_countdown_secs", html)
        self.assertIn("snapshots CVD at stop start", html)
        self.assertNotIn("oppo_stop_cvd_window_secs", html)


if __name__ == "__main__":
    unittest.main()
