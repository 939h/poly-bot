import unittest

import momentum_v3


class DashboardScrollTests(unittest.TestCase):
    def test_trade_log_is_registered_for_horizontal_scroll_preservation(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn('id="tradeLogWrap"', html)
        self.assertIn("const horizontalScrollIds=['tradeLogWrap','pumpActiveWrap','pumpLogWrap'];", html)
        self.assertIn("captureHorizontalScroll();", html)
        self.assertIn("requestAnimationFrame(restoreHorizontalScroll);", html)

    def test_poll_does_not_rerender_during_horizontal_scroll_interaction(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("if(horizontalScrollIsActive())", html)
        self.assertIn("pendingScrollState=d", html)


if __name__ == "__main__":
    unittest.main()
