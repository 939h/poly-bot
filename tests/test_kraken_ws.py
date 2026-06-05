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

    def test_two_of_previous_three_high_rvol_arms_opposite_side(self):
        for index in range(20):
            self._append(120 - index, 119 - index, 100)
        self._append(100, 98, 130)
        self._append(98, 96, 80)
        self._append(96, 94, 130)

        snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

        self.assertTrue(snapshot["armed"])
        self.assertEqual(snapshot["side"], "yes")
        self.assertEqual(snapshot["high_rvol_count"], 2)

    def test_only_one_high_rvol_does_not_arm(self):
        for index in range(20):
            self._append(120 - index, 119 - index, 100)
        self._append(100, 98, 130)
        self._append(98, 96, 80)
        self._append(96, 94, 80)

        snapshot = kraken_ws.get_rvol_reversal_snapshot("btc", period=20)

        self.assertFalse(snapshot["armed"])
        self.assertEqual(snapshot["high_rvol_count"], 1)


if __name__ == "__main__":
    unittest.main()
