import unittest

import momentum_v3


class TradeLogEntryGapTests(unittest.TestCase):
    def setUp(self):
        self.original_open_positions = momentum_v3.open_positions
        self.original_last_entry_ts = momentum_v3.last_entry_ts
        self.original_stats = momentum_v3.stats
        self.original_trade_log = momentum_v3.trade_log
        self.original_candle_open = momentum_v3.candle_open.get("btc")
        self.original_live_close = momentum_v3.live_close.get("btc")
        momentum_v3.open_positions = {}
        momentum_v3.last_entry_ts = {}
        momentum_v3.stats = {"scans": 0, "triggers": 0, "buys": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        momentum_v3.trade_log = []
        momentum_v3.candle_open["btc"] = 100_000.0
        momentum_v3.live_close["btc"] = 100_125.25

    def tearDown(self):
        momentum_v3.open_positions = self.original_open_positions
        momentum_v3.last_entry_ts = self.original_last_entry_ts
        momentum_v3.stats = self.original_stats
        momentum_v3.trade_log = self.original_trade_log
        if self.original_candle_open is None:
            momentum_v3.candle_open.pop("btc", None)
        else:
            momentum_v3.candle_open["btc"] = self.original_candle_open
        if self.original_live_close is None:
            momentum_v3.live_close.pop("btc", None)
        else:
            momentum_v3.live_close["btc"] = self.original_live_close

    def test_open_position_snapshots_kraken_gap_at_buy_time(self):
        momentum_v3.open_position("btc_yes", "token", 0.5, filled_shares=2.0)

        position = momentum_v3.open_positions["btc_yes"]
        self.assertEqual(position["entry_kraken_gap"], 125.25)
        self.assertEqual(position["entry_kraken_gap_ratio"], 1.252)

        # Later Kraken movement must not change the recorded buy-time gap or ratio.
        momentum_v3.live_close["btc"] = 100_500.0
        momentum_v3._record_trade_log("btc_yes", position, "SELL", 0.75, 0.5)
        self.assertEqual(momentum_v3.trade_log[0]["entry_kraken_gap"], 125.25)
        self.assertEqual(momentum_v3.trade_log[0]["entry_kraken_gap_ratio"], 1.252)

    def test_dashboard_trade_log_shows_buy_time_kraken_gap_ratio(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("Buy Kraken Gap Ratio", html)
        self.assertIn("t.entry_kraken_gap_ratio", html)
        self.assertIn("krakenGapRatio.toFixed(3)+'x'", html)
        self.assertNotIn("<th>Buy Kraken Gap</th>", html)

    def test_dashboard_trade_log_marks_only_golden_trades(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("const goldenTag=t.is_golden_oppo?", html)
        self.assertIn(">GOLDEN</span>", html)

    def test_trade_log_preserves_golden_mode_flag(self):
        position = {
            "opened_at": "12:00", "entry_price": 0.1, "sell_price": 0.2,
            "is_golden_oppo": True,
        }

        momentum_v3._record_trade_log("btc_yes_oppo", position, "OPPO-SELL", 0.2, 1.0)

        self.assertTrue(momentum_v3.trade_log[0]["is_golden_oppo"])

    def test_normal_oppo_snapshots_actual_golden_status_and_side(self):
        original_setup = momentum_v3._oppo_golden_rvol_setup
        momentum_v3._oppo_golden_rvol_setup = lambda asset, side: {
            "armed": True, "qualified": False, "side": "yes",
        }
        try:
            status = momentum_v3._golden_status_at_buy("btc", "no", False)
        finally:
            momentum_v3._oppo_golden_rvol_setup = original_setup

        self.assertEqual(status, "ARMED YES")

    def test_trade_log_shows_recorded_buy_time_golden_status_after_gap_ratio(self):
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn("<th>Buy Kraken Gap Ratio</th><th>Golden Status</th>", html)
        self.assertIn("t.entry_golden_status", html)

    def test_trade_log_csv_exports_buy_time_kraken_gap(self):
        momentum_v3.trade_log = [{"entry_kraken_gap": 125.25, "entry_kraken_gap_ratio": 1.2525}]

        csv_text = momentum_v3._trade_log_csv_bytes().decode()

        self.assertIn("entry_kraken_gap,entry_kraken_gap_ratio", csv_text)
        self.assertIn("125.25,1.2525", csv_text)


if __name__ == "__main__":
    unittest.main()
