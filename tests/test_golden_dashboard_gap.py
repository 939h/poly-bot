import inspect
import unittest

import momentum_v3


class GoldenDashboardGapTests(unittest.TestCase):
    def test_snapshot_reports_golden_gap_as_flexi_when_enabled(self):
        source = inspect.getsource(momentum_v3._build_state_snapshot)

        self.assertIn("golden_gap_flexi", source)
        self.assertIn('"gap_flexi": golden_gap_flexi', source)
        self.assertIn("golden_gap_passed or golden_gap_flexi", source)

    def test_dashboard_distinguishes_gap_flexi_from_gap_block(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("const gapFlexi=", html)
        self.assertIn("'GAP FLEXI'", html)
        self.assertIn("&& !gapFlexi", html)


if __name__ == "__main__":
    unittest.main()
