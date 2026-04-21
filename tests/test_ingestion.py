"""Ingestion pipeline tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ingestion.recorder import ParquetRecorder, RawReplayStore
from utils.time_utils import parse_utc_timestamp
from utils.validation import validate_crypto_rows, validate_limitless_rows


class IngestionPipelineTests(unittest.TestCase):
    """Validate append-only raw storage and replay behavior."""

    def test_validate_limitless_rows_rejects_bad_prices(self) -> None:
        """Limitless validation should reject out-of-range prices."""

        frame = pd.DataFrame(
            [
                {"market_id": "m1", "timestamp": "2026-01-01T00:00:00Z", "yes_price": 0.6, "volume": 10, "liquidity": 100},
                {"market_id": "m1", "timestamp": "2026-01-01T00:00:01Z", "yes_price": 1.2, "volume": 10, "liquidity": 100},
            ]
        )
        valid, rejected = validate_limitless_rows(frame)
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(rejected), 1)

    def test_validate_crypto_rows_rejects_null_critical_fields(self) -> None:
        """Crypto validation should reject rows without required fields."""

        frame = pd.DataFrame(
            [
                {"symbol": "BTCUSDT", "timestamp": "2026-01-01T00:00:00Z", "price": 100000.0, "volume": 1.0},
                {"symbol": "ETHUSDT", "timestamp": None, "price": 4000.0, "volume": 2.0},
            ]
        )
        valid, rejected = validate_crypto_rows(frame)
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(rejected), 1)

    def test_parquet_recorder_appends_without_overwrite_and_replays_until_cutoff(self) -> None:
        """Recorder should write append-only parquet files and replay only rows up to T."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            recorder = ParquetRecorder(root)
            first_batch = [
                {
                    "market_id": "m1",
                    "timestamp": parse_utc_timestamp("2026-01-01T00:00:00Z"),
                    "yes_price": 0.55,
                    "volume": 100.0,
                    "liquidity": 1000.0,
                    "source": "test",
                    "ingested_at": parse_utc_timestamp("2026-01-01T00:00:00Z"),
                }
            ]
            second_batch = [
                {
                    "market_id": "m1",
                    "timestamp": parse_utc_timestamp("2026-01-01T00:00:05Z"),
                    "yes_price": 0.58,
                    "volume": 150.0,
                    "liquidity": 1010.0,
                    "source": "test",
                    "ingested_at": parse_utc_timestamp("2026-01-01T00:00:05Z"),
                }
            ]
            crypto_batch = [
                {
                    "symbol": "BTCUSDT",
                    "timestamp": parse_utc_timestamp("2026-01-01T00:00:02Z"),
                    "price": 100100.0,
                    "volume": 5.0,
                    "source": "test",
                    "ingested_at": parse_utc_timestamp("2026-01-01T00:00:02Z"),
                }
            ]

            recorder.append_limitless(first_batch)
            recorder.append_limitless(second_batch)
            recorder.append_crypto(crypto_batch)

            limitless_files = list((root / "limitless").rglob("*.parquet"))
            crypto_files = list((root / "crypto").rglob("*.parquet"))
            self.assertGreaterEqual(len(limitless_files), 2)
            self.assertEqual(len(crypto_files), 1)

            replay = RawReplayStore(root)
            market_frame = replay.get_market_data_until("2026-01-01T00:00:03Z", ["m1"])
            crypto_frame = replay.get_crypto_data_until("2026-01-01T00:00:03Z", ["BTCUSDT"])
            self.assertEqual(len(market_frame), 1)
            self.assertEqual(len(crypto_frame), 1)
            self.assertLessEqual(pd.to_datetime(market_frame["timestamp"], utc=True).max(), pd.Timestamp("2026-01-01T00:00:03Z"))


if __name__ == "__main__":
    unittest.main()
