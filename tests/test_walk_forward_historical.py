"""Tests for walk-forward historical evaluation split logic."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


def _make_fake_markets(n: int) -> list[dict]:
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "condition_id": f"mkt_{i:03d}",
            "resolution_time_utc": (base + timedelta(days=i)).isoformat(),
            "label": i % 2,
        }
        for i in range(n)
    ]


class WalkForwardSplitTests(unittest.TestCase):

    def test_split_proportions(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(100)
        train, calib, test = split_markets(markets)
        self.assertEqual(len(train), 60)
        self.assertEqual(len(calib), 20)
        self.assertEqual(len(test), 20)

    def test_split_is_temporally_ordered(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(30)
        train, calib, test = split_markets(markets)
        train_max = max(m["resolution_time_utc"] for m in train)
        calib_min = min(m["resolution_time_utc"] for m in calib)
        calib_max = max(m["resolution_time_utc"] for m in calib)
        test_min = min(m["resolution_time_utc"] for m in test)
        self.assertLessEqual(train_max, calib_min)
        self.assertLessEqual(calib_max, test_min)

    def test_no_market_in_multiple_splits(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(50)
        train, calib, test = split_markets(markets)
        train_ids = {m["condition_id"] for m in train}
        calib_ids = {m["condition_id"] for m in calib}
        test_ids = {m["condition_id"] for m in test}
        self.assertEqual(len(train_ids & calib_ids), 0)
        self.assertEqual(len(train_ids & test_ids), 0)
        self.assertEqual(len(calib_ids & test_ids), 0)

    def test_all_markets_accounted_for(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(50)
        train, calib, test = split_markets(markets)
        self.assertEqual(len(train) + len(calib) + len(test), 50)

    def test_minimum_30_test_markets(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(200)
        _, _, test = split_markets(markets)
        self.assertGreaterEqual(len(test), 30)
