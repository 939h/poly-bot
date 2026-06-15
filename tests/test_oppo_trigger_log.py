import unittest

import momentum_v3


class OppoTriggerLogTests(unittest.TestCase):
    def setUp(self):
        self.original_log = momentum_v3.oppo_trigger_log
        self.original_once = momentum_v3.oppo_dashboard_once_per_window
        momentum_v3.oppo_trigger_log = []
        momentum_v3.oppo_dashboard_once_per_window = set()

    def tearDown(self):
        momentum_v3.oppo_trigger_log = self.original_log
        momentum_v3.oppo_dashboard_once_per_window = self.original_once

    def test_rvol_and_cvd_blocks_are_suppressed_from_trigger_logs(self):
        original_last = momentum_v3.oppo_last_trigger
        momentum_v3.oppo_last_trigger = {}
        try:
            for status in ("RVOL-BLOCK", "CVD-BLOCK"):
                momentum_v3.record_oppo_trigger("btc_yes_oppo", "btc", "yes", 0.05, status, "repeat")
                momentum_v3._record_oppo_trigger("btc", "yes", 0.05, status, "repeat")

            self.assertEqual(momentum_v3.oppo_last_trigger, {})
            self.assertEqual(momentum_v3.oppo_trigger_log, [])
            self.assertNotIn("'CVD-BLOCK'", momentum_v3._DASHBOARD_HTML)
            self.assertNotIn("'RVOL-BLOCK'", momentum_v3._DASHBOARD_HTML)
        finally:
            momentum_v3.oppo_last_trigger = original_last

    def test_golden_gap_block_is_logged_only_once_per_window(self):
        momentum_v3._record_oppo_trigger("btc", "yes", 0.05, "GOLDEN-GAP-BLOCK", "first")
        momentum_v3._record_oppo_trigger("btc", "yes", 0.06, "GOLDEN-GAP-BLOCK", "repeat")

        self.assertEqual(len(momentum_v3.oppo_trigger_log), 1)
        self.assertEqual(momentum_v3.oppo_trigger_log[0]["reason"], "first")

    def test_golden_is_logged_once_but_other_statuses_are_not_deduplicated(self):
        momentum_v3._record_oppo_trigger("eth", "no", 0.07, "GOLDEN", "first")
        momentum_v3._record_oppo_trigger("eth", "no", 0.08, "GOLDEN", "repeat")
        momentum_v3._record_oppo_trigger("eth", "no", 0.08, "BOUGHT", "first buy")
        momentum_v3._record_oppo_trigger("eth", "no", 0.09, "BOUGHT", "second buy")

        self.assertEqual([event["status"] for event in momentum_v3.oppo_trigger_log], ["BOUGHT", "BOUGHT", "GOLDEN"])

    def test_golden_gap_block_can_be_logged_again_after_window_reset(self):
        momentum_v3._record_oppo_trigger("sol", "yes", 0.05, "GOLDEN-GAP-BLOCK", "old window")
        momentum_v3.oppo_dashboard_once_per_window.clear()
        momentum_v3._record_oppo_trigger("sol", "yes", 0.06, "GOLDEN-GAP-BLOCK", "new window")

        self.assertEqual(len(momentum_v3.oppo_trigger_log), 2)
        self.assertEqual(momentum_v3.oppo_trigger_log[0]["reason"], "new window")

    def test_dashboard_uses_expected_boxes_for_tp2_cut_and_oppo_sell(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("'OPPO-SELL':'#0d2a1e'", html)
        self.assertIn("'TP2-CUT':'#2a1e08'", html)
        self.assertIn("'OPPO-SELL','CUT-LOSS','TP2-CUT'", html)
        self.assertIn('class="badge" style="${statusStyle}"', html)


if __name__ == "__main__":
    unittest.main()
