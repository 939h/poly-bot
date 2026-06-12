import inspect
import unittest

import momentum_v3


class OppoGapFlexiWindowTests(unittest.TestCase):
    def setUp(self):
        self.original_flexi = momentum_v3.FLEXI_RVOL_ENABLED
        self.original_max_mag = momentum_v3.OPPO_GAP_FLEXI_MAX_MAG
        momentum_v3.FLEXI_RVOL_ENABLED = True
        momentum_v3.OPPO_GAP_FLEXI_MAX_MAG = 2.0

    def tearDown(self):
        momentum_v3.FLEXI_RVOL_ENABLED = self.original_flexi
        momentum_v3.OPPO_GAP_FLEXI_MAX_MAG = self.original_max_mag

    def test_gap_flexi_is_allowed_from_zero_through_600_seconds(self):
        self.assertTrue(momentum_v3._oppo_gap_flexi_allowed(0, 2.0))
        self.assertTrue(momentum_v3._oppo_gap_flexi_allowed(600, 2.0))

    def test_gap_flexi_is_blocked_after_600_seconds(self):
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(601, 2.0))
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(899, 2.0))

    def test_gap_flexi_is_blocked_when_global_flexi_is_disabled(self):
        momentum_v3.FLEXI_RVOL_ENABLED = False

        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(300, 2.0))

    def test_gap_flexi_is_blocked_above_two_x(self):
        self.assertTrue(momentum_v3._oppo_gap_flexi_allowed(300, 2.0))
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(300, 2.0001))
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(300, 3.0))

    def test_gap_flexi_is_blocked_without_a_ratio(self):
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(300, None))

    def test_scanner_and_dashboard_use_same_gap_flexi_window(self):
        scan_source = inspect.getsource(momentum_v3.scan_markets)
        snapshot_source = inspect.getsource(momentum_v3._build_state_snapshot)

        self.assertIn("_oppo_gap_flexi_allowed(secs_into, gap_ratio)", scan_source)
        self.assertIn("_oppo_gap_flexi_allowed(secs_in, golden_gap_ratio)", snapshot_source)
        self.assertIn("oppo_gap_flexi_max_mag", momentum_v3._DASHBOARD_HTML)


if __name__ == "__main__":
    unittest.main()
