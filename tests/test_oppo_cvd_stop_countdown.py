import inspect
import unittest

import kraken_ws
import momentum_v3


class OppoCvdStopCountdownTests(unittest.TestCase):
    def test_short_cvd_slope_uses_requested_recent_window(self):
        original_points = kraken_ws._cvd_points["btc"]
        original_time = kraken_ws.time.time
        try:
            kraken_ws._cvd_points["btc"] = kraken_ws.deque([
                (970.0, -100.0),
                (980.0, 0.0),
                (990.0, 5.0),
                (1000.0, 15.0),
            ], maxlen=300)
            kraken_ws.time.time = lambda: 1000.0

            self.assertEqual(kraken_ws.get_short_cvd_slope("btc", 15), 1.0)
            self.assertEqual(kraken_ws.get_short_cvd_slope("btc", 25), 0.75)
        finally:
            kraken_ws._cvd_points["btc"] = original_points
            kraken_ws.time.time = original_time

    def test_oppo_stop_uses_fixed_countdown_and_short_cvd_reversal_restart(self):
        source = inspect.getsource(momentum_v3.manage_positions)

        self.assertIn("OPPO_STOP_LOSS_COUNTDOWN_SECS", source)
        self.assertIn("get_short_cvd_slope", source)
        self.assertIn("if cvd_confirming and not was_confirming:", source)
        self.assertIn('pos["force_stop_triggered"] = now', source)
        self.assertIn("oppo_stop_active and current_price < entry", source)

    def test_dashboard_explains_oppo_cvd_stop_countdown(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("oppo_stop_loss_countdown_secs", html)
        self.assertIn("CVD reversal until entry recovery", html)


if __name__ == "__main__":
    unittest.main()
