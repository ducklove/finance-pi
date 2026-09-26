import unittest
from datetime import UTC, datetime

from finance_pi.sources.gold.prices import parse_months


class DataTests(unittest.TestCase):
    def test_excludes_incomplete_month_and_invalid_prices(self):
        stamps = [
            int(datetime(2023 + i // 12, i % 12 + 1, 1, tzinfo=UTC).timestamp()) for i in range(38)
        ]
        prices = [100.0] * 38
        prices[0] = None
        prices[1] = float("nan")
        prices[2] = -1
        result = {
            "meta": {"exchangeTimezoneName": "UTC"},
            "timestamp": stamps,
            "indicators": {"quote": [{"close": prices}]},
        }
        points = parse_months(result, datetime(2026, 2, 15, tzinfo=UTC))
        self.assertEqual(points[0]["date"], "2023-04")
        self.assertEqual(points[-1]["date"], "2026-01")
        self.assertEqual(len(points), 34)

    def test_rejects_short_data(self):
        with self.assertRaises(ValueError):
            parse_months(
                {"meta": {}, "timestamp": [], "indicators": {"quote": [{"close": []}]}},
                datetime.now(UTC),
            )
