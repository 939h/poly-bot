import inspect
import unittest

import momentum_v3


class OppoModeTests(unittest.TestCase):
    def setUp(self):
        self.original_normal = momentum_v3.OPPO_NORMAL_ENABLED

    def tearDown(self):
        momentum_v3.OPPO_NORMAL_ENABLED = self.original_normal

    def test_normal_enabled_allows_standard_and_golden(self):
        momentum_v3.OPPO_NORMAL_ENABLED = True

        self.assertTrue(momentum_v3._oppo_entry_mode_allowed(False))
        self.assertTrue(momentum_v3._oppo_entry_mode_allowed(True))

    def test_normal_disabled_allows_only_golden(self):
        momentum_v3.OPPO_NORMAL_ENABLED = False

        self.assertFalse(momentum_v3._oppo_entry_mode_allowed(False))
        self.assertTrue(momentum_v3._oppo_entry_mode_allowed(True))

    def test_master_mode_still_wraps_shared_oppo_scanner(self):
        source = inspect.getsource(momentum_v3.scan_markets)

        self.assertIn("if OPPO_MODE_ENABLED and secs_into >= OPPO_WINDOW_START_SEC", source)
        self.assertIn("if not _oppo_entry_mode_allowed(golden_opportunity)", source)

    def test_dashboard_exposes_all_oppo_mode_switches(self):
        snapshot_source = inspect.getsource(momentum_v3._build_state_snapshot)
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn('"oppo_mode_enabled"', snapshot_source)
        self.assertIn('"oppo_normal_enabled"', snapshot_source)
        self.assertIn("Master ON, Normal OFF, Golden ON", html)


if __name__ == "__main__":
    unittest.main()
