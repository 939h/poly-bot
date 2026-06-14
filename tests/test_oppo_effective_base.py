import unittest

import momentum_v3


class OppoEffectiveBaseTests(unittest.TestCase):
    def test_trough_below_minimum_uses_minimum_effective_base(self):
        effective_base, entry_trigger, rebound_ratio = momentum_v3._oppo_rebound_state(0.05, 0.10)

        self.assertEqual(effective_base, 0.07)
        self.assertEqual(entry_trigger, 0.14)
        self.assertAlmostEqual(rebound_ratio, 0.10 / 0.07)
        self.assertLess(rebound_ratio, momentum_v3.OPPO_REBOUND_MULT)

    def test_trough_above_minimum_remains_the_effective_base(self):
        effective_base, entry_trigger, rebound_ratio = momentum_v3._oppo_rebound_state(0.08, 0.16)

        self.assertEqual(effective_base, 0.08)
        self.assertEqual(entry_trigger, 0.16)
        self.assertEqual(rebound_ratio, 2.0)

    def test_defaults_and_dashboard_expose_minimum_effective_base(self):
        self.assertEqual(momentum_v3.OPPO_MIN_PRICE, 0.07)
        self.assertEqual(momentum_v3.OPPO_MAX_PRICE, 0.15)
        self.assertEqual(momentum_v3.OPPO_MIN_EFFECTIVE_BASE, 0.07)
        self.assertIn("cfg.oppo_min_effective_base", momentum_v3._DASHBOARD_HTML)


if __name__ == "__main__":
    unittest.main()
