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
        self.assertIn("if not _oppo_entry_mode_allowed(golden_order_allowed)", source)

    def test_dashboard_exposes_all_oppo_mode_switches(self):
        snapshot_source = inspect.getsource(momentum_v3._build_state_snapshot)
        html = momentum_v3._DASHBOARD_HTML

        self.assertIn('"oppo_mode_enabled"', snapshot_source)
        self.assertIn('"oppo_normal_enabled"', snapshot_source)
        self.assertIn('"oppo_golden_order_enabled"', snapshot_source)
        self.assertIn("Golden orders ${cfg.oppo_golden_order_enabled?'ON':'OFF'}", html)
        self.assertIn("signal stays active when orders are OFF", html)

    def test_golden_entries_use_dedicated_buy_amount(self):
        source = inspect.getsource(momentum_v3.scan_markets)
        snapshot_source = inspect.getsource(momentum_v3._build_state_snapshot)

        self.assertIn("oppo_buy_amount = OPPO_GOLDEN_BUY_AMOUNT", source)
        self.assertIn("golden_order_allowed = bool(golden_opportunity and OPPO_GOLDEN_ORDER_ENABLED)", source)
        self.assertIn("if golden_order_allowed:", source)
        self.assertIn('"oppo_golden_buy_amount"', snapshot_source)
        self.assertIn("cfg.oppo_golden_buy_amount", momentum_v3._DASHBOARD_HTML)

    def test_golden_order_switch_does_not_disable_golden_signal_logic(self):
        source = inspect.getsource(momentum_v3.scan_markets)

        self.assertIn("golden_setup = _oppo_golden_rvol_setup(opp_asset, side)", source)
        self.assertIn("golden_opportunity = golden_setup.get(\"qualified\", False)", source)
        self.assertIn("golden_order_allowed = bool(golden_opportunity and OPPO_GOLDEN_ORDER_ENABLED)", source)
        self.assertIn("detail = \"normal OPPO disabled; Golden orders disabled\" if golden_opportunity", source)

    def test_normal_oppo_requires_matching_golden_direction(self):
        self.assertEqual(
            momentum_v3._golden_direction_allows_normal_oppo(
                "btc", "yes", {"side": "yes"},
            ),
            (True, "yes"),
        )
        self.assertEqual(
            momentum_v3._golden_direction_allows_normal_oppo(
                "btc", "yes", {"side": "no"},
            ),
            (False, "no"),
        )

    def test_scan_blacklists_normal_oppo_when_golden_direction_mismatches(self):
        source = inspect.getsource(momentum_v3.scan_markets)

        self.assertIn("if not golden_order_allowed:", source)
        self.assertIn("_golden_direction_allows_normal_oppo(opp_asset, side, golden_setup)", source)
        self.assertIn('record_oppo_trigger(opp_key, opp_asset, side, opp_price, "GOLDEN-DIR-BLOCK", detail)', source)
        self.assertIn("_clear_oppo_tracking_for_asset(opp_asset)", source)
        self.assertIn("'GOLDEN-DIR-BLOCK'", momentum_v3._DASHBOARD_HTML)


if __name__ == "__main__":
    unittest.main()
