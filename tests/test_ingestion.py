"""Ingestion pipeline tests."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ingestion.recorder import ParquetRecorder, RawReplayStore, run_ingestion_loop
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


class ParseUtcTimestampTests(unittest.TestCase):
    """Validate parse_utc_timestamp handles all supported input types."""

    def test_iso_string(self) -> None:
        from utils.time_utils import parse_utc_timestamp
        from datetime import timezone

        result = parse_utc_timestamp("2026-01-01T00:00:00Z")
        self.assertEqual(result.year, 2026)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_unix_seconds_int(self) -> None:
        from utils.time_utils import parse_utc_timestamp
        from datetime import timezone

        epoch_seconds = 1_609_459_200  # 2021-01-01 00:00:00 UTC
        result = parse_utc_timestamp(epoch_seconds)
        self.assertEqual(result.year, 2021)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_unix_seconds_float(self) -> None:
        from utils.time_utils import parse_utc_timestamp

        result = parse_utc_timestamp(1_609_459_200.5)
        self.assertEqual(result.year, 2021)

    def test_nan_raises(self) -> None:
        from utils.time_utils import parse_utc_timestamp

        with self.assertRaises(ValueError):
            parse_utc_timestamp(float("nan"))


class PartitionPruningTests(unittest.TestCase):
    """Validate that get_*_data_until skips date partitions beyond the cutoff."""

    def test_skips_future_date_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            recorder = ParquetRecorder(root)

            day1_row = {
                "market_id": "m1",
                "timestamp": parse_utc_timestamp("2026-01-01T12:00:00Z"),
                "yes_price": 0.5,
                "volume": 100.0,
                "liquidity": 1000.0,
                "source": "test",
                "ingested_at": parse_utc_timestamp("2026-01-01T12:00:00Z"),
            }
            day2_row = {
                "market_id": "m1",
                "timestamp": parse_utc_timestamp("2026-01-02T12:00:00Z"),
                "yes_price": 0.6,
                "volume": 200.0,
                "liquidity": 1100.0,
                "source": "test",
                "ingested_at": parse_utc_timestamp("2026-01-02T12:00:00Z"),
            }
            recorder.append_limitless([day1_row])
            recorder.append_limitless([day2_row])

            replay = RawReplayStore(root)
            # Cutoff is end of day 1 — day 2 partition should be skipped entirely.
            result = replay.get_market_data_until("2026-01-01T23:59:59Z", ["m1"])
            self.assertEqual(len(result), 1)
            self.assertAlmostEqual(float(result.iloc[0]["yes_price"]), 0.5)

    def test_cross_batch_monotonicity_is_enforced(self) -> None:
        """Recorder should reject rows that go backward in time across batches."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            recorder = ParquetRecorder(root)

            first = [
                {
                    "market_id": "m1",
                    "timestamp": parse_utc_timestamp("2026-01-01T00:00:10Z"),
                    "yes_price": 0.55,
                    "volume": 100.0,
                    "liquidity": 1000.0,
                    "source": "test",
                    "ingested_at": parse_utc_timestamp("2026-01-01T00:00:10Z"),
                }
            ]
            stale = [
                {
                    "market_id": "m1",
                    "timestamp": parse_utc_timestamp("2026-01-01T00:00:05Z"),  # older
                    "yes_price": 0.50,
                    "volume": 80.0,
                    "liquidity": 900.0,
                    "source": "test",
                    "ingested_at": parse_utc_timestamp("2026-01-01T00:00:05Z"),
                }
            ]
            accepted1, rejected1 = recorder.append_limitless(first)
            accepted2, rejected2 = recorder.append_limitless(stale)
            self.assertEqual(accepted1, 1)
            self.assertEqual(rejected2, 1)  # stale row must be rejected


class IngestionEnabledFlagTests(unittest.TestCase):
    """run_ingestion_loop should exit immediately when INGESTION_ENABLED=false."""

    def test_disabled_via_env_exits_without_writing(self) -> None:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from project.configuration import load_config

        with tempfile.TemporaryDirectory() as tmp:
            config = load_config(Path(__file__).parent.parent / "config" / "default.yaml")

            with patch.dict("os.environ", {"INGESTION_ENABLED": "false"}):
                # Should return immediately without hanging or writing files.
                asyncio.run(run_ingestion_loop(config))

            # No ingestion_status.json should have been written.
            data_dir = Path(config.data.market_metadata_path).parent
            status_path = data_dir / "ingestion_status.json"
            self.assertFalse(
                status_path.exists(),
                "ingestion_status.json must not be written when ingestion is disabled",
            )


if __name__ == "__main__":
    unittest.main()
