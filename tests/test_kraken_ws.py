import unittest

import kraken_ws


class RvolReversalSnapshotTests(unittest.TestCase):
    def setUp(self):
        with kraken_ws._lock:
            kraken_ws.candle_history["btc"].clear()

    def tearDown(self):
        with kraken_ws._lock:
            kraken_ws.candle_history["btc"].clear()

    def _append(self, open_, close, quote_volume):
        with kraken_ws._lock:
            kraken_ws.candle_history["btc"].append({
                "ts": len(kraken_ws.candle_history["btc"]) * 900_000,
                "open": open_,
                "high": max(open_, close),
                "low": min(open_, close),
                "close": close,
                "quote_volume": quote_volume,
                "closed": True,
            })

    def _seed_declining_setup(self, volumes):
        for index in range(20):
            self._append(120 - index, 119 - index, 100)
        for index, volume in enumerate(volumes):
            open_ = 100 - (index * 2)
            self._append(open_, open_ - 2, volume)

    def test_every_two_of_three_ordering_arms_golden_mode(self):
        patterns = {
            "high-low-high": (130, 80, 130),
            "high-high-low": (130, 130, 80),
            "low-high-high": (80, 130, 130),
        }
        for name, volumes in patterns.items():
            with self.subTest(name=name):
                with kraken_ws._lock:
                    kraken_ws.candle_history["btc"].clear()
                self._seed_declining_setup(volumes)

                snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

                self.assertTrue(snapshot["armed"])
                self.assertEqual(snapshot["side"], "yes")
                self.assertEqual(snapshot["high_rvol_count"], 2)

    def test_three_of_three_high_rvol_arms_golden_mode(self):
        self._seed_declining_setup((130, 130, 130))

        snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

        self.assertTrue(snapshot["armed"])
        self.assertEqual(snapshot["side"], "yes")
        self.assertEqual(snapshot["high_rvol_count"], 3)

    def test_only_one_high_rvol_does_not_arm(self):
        self._seed_declining_setup((130, 80, 80))

        snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

        self.assertFalse(snapshot["armed"])
        self.assertEqual(snapshot["high_rvol_count"], 1)

    def test_probability_reports_historical_fourth_window_reversals(self):
        for index in range(20):
            self._append(120 - index, 119 - index, 100)
        self._append(100, 98, 130)
        self._append(98, 96, 130)
        self._append(96, 94, 80)
        self._append(94, 96, 100)  # YES reversal win after a declining setup
        self._append(96, 94, 130)
        self._append(94, 92, 130)
        self._append(92, 90, 80)
        self._append(90, 88, 100)  # YES reversal loss after a declining setup
        self._append(88, 86, 130)
        self._append(86, 84, 130)
        self._append(84, 82, 80)

        snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

        self.assertTrue(snapshot["armed"])
        self.assertEqual(snapshot["side"], "yes")
        self.assertGreaterEqual(snapshot["samples"], 2)
        self.assertGreater(snapshot["wins"], 0)
        self.assertEqual(snapshot["probability"], snapshot["wins"] / snapshot["samples"])


if __name__ == "__main__":
    unittest.main()
