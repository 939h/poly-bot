import inspect
import unittest

import momentum_v3


class EmaVisualizationOnlyTests(unittest.TestCase):
    def test_legacy_ema_guard_status_is_defined_and_always_disabled(self):
        self.assertIs(momentum_v3.EMA_CONFIRM_ENABLED, False)

    def test_scan_markets_does_not_gate_entries_with_ema(self):
        source = inspect.getsource(momentum_v3.scan_markets)

        self.assertNotIn("_ema_confirms_side", source)
        self.assertNotIn("ema-not-confirmed", source)

    def test_ema_data_is_still_in_dashboard_snapshot(self):
        source = inspect.getsource(momentum_v3._build_state_snapshot)

        self.assertIn("get_ema_snapshot", source)
        self.assertIn('"ema_now"', source)
        self.assertIn('"ema_history"', source)


if __name__ == "__main__":
    unittest.main()
