"""Look-ahead audit for FeatureBuilder — HARD GATE.

These tests must pass before any evaluation results are considered valid.
They verify that FeatureBuilder.build_features() never uses data timestamped
after the as_of time.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from features.builder import FeatureBuilder


def _make_market_history(base_ts: datetime, n: int = 20) -> pd.DataFrame:
    """Create synthetic market history with timestamps at 1-minute intervals."""
    rows = []
    for i in range(n):
        rows.append({
            "timestamp": base_ts + timedelta(minutes=i),
            "market_id": "test_mkt",
            "p_market": 0.5 + i * 0.01,
            "volume": 100.0 + i * 10,
        })
    return pd.DataFrame(rows)


def _make_crypto_history(base_ts: datetime, n: int = 20) -> pd.DataFrame:
    """Create synthetic crypto history with timestamps at 1-minute intervals."""
    rows = []
    for i in range(n):
        rows.append({
            "timestamp": base_ts + timedelta(minutes=i),
            "btc_price": 50000.0 + i * 100,
            "eth_price": 3000.0,
            "funding_rate": 0.001,
        })
    return pd.DataFrame(rows)


class LookAheadAuditTests(unittest.TestCase):
    """Verify FeatureBuilder never uses future data."""

    def setUp(self) -> None:
        self.builder = FeatureBuilder(schema_version="v2")
        self.base_ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_market_source_max_ts_never_exceeds_as_of(self) -> None:
        """market_source_max_ts must be <= as_of for all valid as_of values."""
        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist = _make_crypto_history(self.base_ts, n=20)
        resolution_time = self.base_ts + timedelta(hours=1)

        for minutes_offset in [5, 10, 15, 19]:
            as_of = self.base_ts + timedelta(minutes=minutes_offset)
            row = self.builder.build_features(
                market_history=market_hist,
                crypto_history=crypto_hist,
                as_of=as_of,
                resolution_time=resolution_time,
                label=1,
                market_id="test_mkt",
            )
            self.assertLessEqual(
                row.market_source_max_ts,
                as_of,
                msg=f"market_source_max_ts {row.market_source_max_ts} > as_of {as_of}",
            )

    def test_crypto_source_max_ts_never_exceeds_as_of(self) -> None:
        """crypto_source_max_ts must be <= as_of for all valid as_of values."""
        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist = _make_crypto_history(self.base_ts, n=20)
        resolution_time = self.base_ts + timedelta(hours=1)

        for minutes_offset in [5, 10, 15, 19]:
            as_of = self.base_ts + timedelta(minutes=minutes_offset)
            row = self.builder.build_features(
                market_history=market_hist,
                crypto_history=crypto_hist,
                as_of=as_of,
                resolution_time=resolution_time,
                label=1,
                market_id="test_mkt",
            )
            self.assertLessEqual(
                row.crypto_source_max_ts,
                as_of,
                msg=f"crypto_source_max_ts {row.crypto_source_max_ts} > as_of {as_of}",
            )

    def test_future_data_injected_into_market_history_is_ignored(self) -> None:
        """Future rows in market_history must not affect the result."""
        as_of = self.base_ts + timedelta(minutes=10)
        resolution_time = self.base_ts + timedelta(hours=1)

        # Base market history: 10 minutes of data (past)
        market_hist_clean = _make_market_history(self.base_ts, n=10)

        # Inject a future row with a suspiciously high price
        future_row = pd.DataFrame([{
            "timestamp": as_of + timedelta(minutes=5),  # FUTURE
            "market_id": "test_mkt",
            "p_market": 9999.0,
            "volume": 9999.0,
        }])
        market_hist_with_future = pd.concat([market_hist_clean, future_row], ignore_index=True)

        crypto_hist = _make_crypto_history(self.base_ts, n=20)

        row_clean = self.builder.build_features(
            market_history=market_hist_clean,
            crypto_history=crypto_hist,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )
        row_injected = self.builder.build_features(
            market_history=market_hist_with_future,
            crypto_history=crypto_hist,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )

        # p_market must not be 9999.0 (the injected future value)
        self.assertAlmostEqual(
            row_clean.values["p_market"],
            row_injected.values["p_market"],
            places=6,
            msg="Future data contaminated p_market",
        )
        self.assertLessEqual(row_injected.market_source_max_ts, as_of)

    def test_future_data_injected_into_crypto_history_is_ignored(self) -> None:
        """Future rows in crypto_history must not affect the result."""
        as_of = self.base_ts + timedelta(minutes=10)
        resolution_time = self.base_ts + timedelta(hours=1)

        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist_clean = _make_crypto_history(self.base_ts, n=10)

        future_crypto = pd.DataFrame([{
            "timestamp": as_of + timedelta(minutes=5),  # FUTURE
            "btc_price": 999999.0,
            "eth_price": 0.0,
            "funding_rate": 99.0,
        }])
        crypto_hist_injected = pd.concat([crypto_hist_clean, future_crypto], ignore_index=True)

        row_clean = self.builder.build_features(
            market_history=market_hist,
            crypto_history=crypto_hist_clean,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )
        row_injected = self.builder.build_features(
            market_history=market_hist,
            crypto_history=crypto_hist_injected,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )

        self.assertAlmostEqual(
            row_clean.values["btc_return_1m"],
            row_injected.values["btc_return_1m"],
            places=6,
            msg="Future crypto data contaminated btc_return_1m",
        )
        self.assertLessEqual(row_injected.crypto_source_max_ts, as_of)


if __name__ == "__main__":
    unittest.main()
