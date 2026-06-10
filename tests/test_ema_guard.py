import os
import unittest
from unittest.mock import patch

import momentum_v3


class EmaGuardTests(unittest.TestCase):
    def setUp(self):
        self.original_enabled = momentum_v3.EMA_CONFIRM_ENABLED
        self.original_snapshot = momentum_v3.get_ema_snapshot

    def tearDown(self):
        momentum_v3.EMA_CONFIRM_ENABLED = self.original_enabled
        momentum_v3.get_ema_snapshot = self.original_snapshot

    def test_false_environment_value_disables_flag(self):
        with patch.dict(os.environ, {"EMA_CONFIRM_ENABLED": "false"}):
            self.assertFalse(momentum_v3._env_bool("EMA_CONFIRM_ENABLED", True))

    def test_true_environment_value_enables_flag(self):
        with patch.dict(os.environ, {"EMA_CONFIRM_ENABLED": "true"}):
            self.assertTrue(momentum_v3._env_bool("EMA_CONFIRM_ENABLED", False))

    def test_disabled_guard_allows_entry_even_when_ema_disagrees(self):
        momentum_v3.EMA_CONFIRM_ENABLED = False
        momentum_v3.get_ema_snapshot = lambda asset: (90.0, 100.0)

        self.assertTrue(momentum_v3._ema_confirms_side("btc", "yes"))

    def test_enabled_guard_blocks_entry_when_ema_disagrees(self):
        momentum_v3.EMA_CONFIRM_ENABLED = True
        momentum_v3.get_ema_snapshot = lambda asset: (90.0, 100.0)

        self.assertFalse(momentum_v3._ema_confirms_side("btc", "yes"))


if __name__ == "__main__":
    unittest.main()
