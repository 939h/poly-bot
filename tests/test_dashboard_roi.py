import inspect
import unittest

import momentum_v3


class DashboardRoiTests(unittest.TestCase):
    def test_closed_trade_records_position_cost_for_roi(self):
        original_trade_log = momentum_v3.trade_log
        momentum_v3.trade_log = []
        try:
            position = {"opened_at": "12:00", "entry_price": 0.1, "sell_price": 0.2, "cost": 2.5}
            momentum_v3._record_trade_log("btc_yes", position, "SELL", 0.2, 1.0)

            self.assertEqual(momentum_v3.trade_log[0]["cost"], 2.5)
        finally:
            momentum_v3.trade_log = original_trade_log

    def test_roi_card_is_immediately_after_net_pnl(self):
        html = momentum_v3._DASHBOARD_HTML
        net_pnl = '<div class="card"><div class="lbl">Net PnL</div>'
        roi = '<div class="card"><div class="lbl">ROI</div>'

        self.assertIn("const roiCost=tLog.reduce", html)
        self.assertIn("const roiPnl=tLog.reduce", html)
        self.assertIn("const roi=roiCost>0?roiPnl/roiCost*100:null", html)
        self.assertGreater(html.index(roi), html.index(net_pnl))
        self.assertNotIn(net_pnl + roi, html)  # Cards include their displayed values between labels.
        self.assertLess(html.index(roi) - html.index(net_pnl), 300)

    def test_bot_status_card_is_immediately_after_roi(self):
        html = momentum_v3._DASHBOARD_HTML
        roi = '<div class="card"><div class="lbl">ROI</div>'
        status = '<div class="card"><div class="lbl">Bot Status</div>'

        self.assertGreater(html.index(status), html.index(roi))
        self.assertLess(html.index(status) - html.index(roi), 300)
        self.assertIn("botStatus.active?'green':'amber'", html)
        self.assertIn("botStatus.detail", html)

    def test_trade_log_csv_exports_cost(self):
        source = inspect.getsource(momentum_v3._trade_log_csv_bytes)

        self.assertIn('"pnl", "cost", "entry_rvol"', source)
        self.assertIn('t.get("cost", "")', source)


if __name__ == "__main__":
    unittest.main()
