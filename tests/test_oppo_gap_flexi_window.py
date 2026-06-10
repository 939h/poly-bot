import inspect
import unittest

import momentum_v3


class OppoGapFlexiWindowTests(unittest.TestCase):
    def setUp(self):
        self.original_flexi = momentum_v3.FLEXI_RVOL_ENABLED
        momentum_v3.FLEXI_RVOL_ENABLED = True

    def tearDown(self):
        momentum_v3.FLEXI_RVOL_ENABLED = self.original_flexi

    def test_gap_flexi_is_allowed_from_zero_through_600_seconds(self):
        self.assertTrue(momentum_v3._oppo_gap_flexi_allowed(0))
        self.assertTrue(momentum_v3._oppo_gap_flexi_allowed(600))

    def test_gap_flexi_is_blocked_after_600_seconds(self):
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(601))
        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(899))

    def test_gap_flexi_is_blocked_when_global_flexi_is_disabled(self):
        momentum_v3.FLEXI_RVOL_ENABLED = False

        self.assertFalse(momentum_v3._oppo_gap_flexi_allowed(300))

    def test_scanner_and_dashboard_use_same_gap_flexi_window(self):
        scan_source = inspect.getsource(momentum_v3.scan_markets)
        snapshot_source = inspect.getsource(momentum_v3._build_state_snapshot)

        self.assertIn("_oppo_gap_flexi_allowed(secs_into)", scan_source)
        self.assertIn("_oppo_gap_flexi_allowed(secs_in)", snapshot_source)


if __name__ == "__main__":
    unittest.main()
