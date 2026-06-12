import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import momentum_v3


class TradingWindowTests(unittest.TestCase):
    @staticmethod
    def _utc_ts(hour, minute):
        return int(datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc).timestamp())

    def test_parses_compact_and_colon_multiple_windows(self):
        windows = momentum_v3._parse_trading_windows("630-830, 11:30-12:30")

        self.assertEqual(windows, ((6, 30, 8, 30), (11, 30, 12, 30)))
        self.assertEqual(momentum_v3._format_trading_windows(windows), "06:30-08:30, 11:30-12:30")

    def test_enabled_windows_allow_only_mentioned_hours(self):
        windows = ((6, 30, 8, 30), (11, 30, 12, 30))
        with patch.object(momentum_v3, "TRADING_WINDOWS_ENABLED", True), \
             patch.object(momentum_v3, "TRADING_TZ_OFFSET_HRS", 0), \
             patch.object(momentum_v3, "TRADING_WINDOWS", windows):
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(6, 30)))
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(8, 29)))
            self.assertFalse(momentum_v3.can_open_new_trades(self._utc_ts(8, 30)))
            self.assertFalse(momentum_v3.can_open_new_trades(self._utc_ts(10, 0)))
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(11, 30)))
            self.assertFalse(momentum_v3.can_open_new_trades(self._utc_ts(12, 30)))

    def test_timezone_offset_and_overnight_windows(self):
        with patch.object(momentum_v3, "TRADING_WINDOWS_ENABLED", True), \
             patch.object(momentum_v3, "TRADING_TZ_OFFSET_HRS", 8), \
             patch.object(momentum_v3, "TRADING_WINDOWS", ((23, 0, 1, 0),)):
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(15, 30)))  # local 23:30
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(16, 30)))  # local 00:30
            self.assertFalse(momentum_v3.can_open_new_trades(self._utc_ts(17, 0)))  # local 01:00

    def test_disabled_windows_allow_entries_at_any_hour(self):
        with patch.object(momentum_v3, "TRADING_WINDOWS_ENABLED", False):
            self.assertTrue(momentum_v3.can_open_new_trades(self._utc_ts(3, 0)))

    def test_invalid_window_is_rejected(self):
        for value in ("", "630", "630/830", "2500-2600", "0630-0860"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                momentum_v3._parse_trading_windows(value)

    def test_dashboard_snapshot_reports_active_or_idle_status(self):
        with patch.object(momentum_v3, "TRADING_WINDOWS_ENABLED", True), \
             patch.object(momentum_v3, "TRADING_TZ_OFFSET_HRS", 0), \
             patch.object(momentum_v3, "TRADING_WINDOWS", ((6, 30, 8, 30),)), \
             patch.object(momentum_v3.time, "time", return_value=self._utc_ts(10, 0)):
            status = momentum_v3._build_state_snapshot()["bot_status"]

        self.assertEqual(status, {
            "active": False,
            "label": "IDLE",
            "detail": "Outside trading hours",
        })

    def test_main_loop_keeps_idle_periods_silent(self):
        import inspect

        source = inspect.getsource(momentum_v3.main)
        self.assertIn("silent_idle  = idle_now and not open_positions", source)
        self.assertIn("if not silent_idle:", source)
        self.assertIn("if not (idle_now and not open_positions):", source)
        self.assertNotIn("[IDLE]", source)


if __name__ == "__main__":
    unittest.main()
