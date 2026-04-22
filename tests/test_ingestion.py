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
        from dataclasses import replace
        from project.configuration import load_config

        base_config = load_config(Path(__file__).parent.parent / "config" / "default.yaml")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            # Redirect data paths into the temp dir so the test is hermetic.
            data = replace(
                base_config.data,
                market_metadata_path=tmp_path / "market_metadata.csv",
            )
            config = replace(base_config, data=data)

            with patch.dict("os.environ", {"INGESTION_ENABLED": "false"}):
                asyncio.run(run_ingestion_loop(config))

            # No ingestion_status.json must appear in the redirected data dir.
            status_path = tmp_path / "ingestion_status.json"
            self.assertFalse(
                status_path.exists(),
                "ingestion_status.json must not be written when ingestion is disabled",
            )


class IngestionConfigDefaultsTests(unittest.TestCase):
    """Pagination and crypto-filter defaults on IngestionConfig."""

    def test_defaults_include_pagination_and_filter(self) -> None:
        from project.configuration import IngestionConfig

        cfg = IngestionConfig()
        self.assertEqual(cfg.pagination_page_size, 25)
        self.assertAlmostEqual(cfg.pagination_delay_seconds, 0.2)
        self.assertEqual(cfg.pagination_max_pages, 50)
        self.assertEqual(cfg.crypto_filter_mode, "auto")
        self.assertIsInstance(cfg.crypto_ticker_allowlist, list)
        self.assertIn("btc", cfg.crypto_ticker_allowlist)
        self.assertIn("eth", cfg.crypto_ticker_allowlist)
        self.assertEqual(cfg.max_snapshots_per_cycle, 300)

    def test_empty_allowlist_seeded_from_yaml_defaults(self) -> None:
        """An empty list from YAML should be replaced with the curated set."""
        from project.configuration import IngestionConfig

        cfg = IngestionConfig(crypto_ticker_allowlist=[])
        self.assertIn("btc", cfg.crypto_ticker_allowlist)

    def test_explicit_allowlist_preserved(self) -> None:
        from project.configuration import IngestionConfig

        cfg = IngestionConfig(crypto_ticker_allowlist=["foo", "bar"])
        self.assertEqual(cfg.crypto_ticker_allowlist, ["foo", "bar"])


class LimitlessPaginationTests(unittest.TestCase):
    """Pagination loop in list_active_markets."""

    @staticmethod
    def _client(pages, *, max_pages=10, total=None, filter_mode="off"):
        from ingestion.limitless_client import LimitlessClient
        from project.configuration import IngestionConfig

        cfg = IngestionConfig(
            limitless_rest_base_url="https://example.test",
            pagination_page_size=2,
            pagination_delay_seconds=0.0,
            pagination_max_pages=max_pages,
            crypto_filter_mode=filter_mode,
        )
        client = LimitlessClient(config=cfg)
        calls: list[tuple[str, dict]] = []

        declared = sum(len(p) for p in pages) if total is None else total

        def fake(path, query=None):
            calls.append((path, dict(query or {})))
            idx = (query or {}).get("page", 1) - 1
            items = pages[idx] if 0 <= idx < len(pages) else []
            return {"data": items, "totalMarketsCount": declared}

        client._request_json = fake  # type: ignore[assignment]
        return client, calls

    def test_stops_on_short_page(self) -> None:
        pages = [
            [{"market_id": "1", "slug": "btc-100k"}, {"market_id": "2", "slug": "eth-5k"}],
            [{"market_id": "3", "slug": "sol-500"}],
        ]
        client, calls = self._client(pages)
        out = client.list_active_markets()
        self.assertEqual(len(out), 3)
        self.assertEqual(len(calls), 2)

    def test_stops_on_total_reached(self) -> None:
        pages = [
            [{"market_id": "1", "slug": "btc-100k"}, {"market_id": "2", "slug": "eth-5k"}],
        ]
        client, calls = self._client(pages, total=2)
        out = client.list_active_markets()
        self.assertEqual(len(out), 2)
        self.assertEqual(len(calls), 1)

    def test_respects_max_pages_cap(self) -> None:
        pages = [
            [
                {"market_id": str(i), "slug": f"btc-{i}"},
                {"market_id": str(i + 100), "slug": f"eth-{i}"},
            ]
            for i in range(20)
        ]
        client, calls = self._client(pages, max_pages=3, total=999)
        out = client.list_active_markets()
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(out), 6)


class LimitlessCryptoFilterTests(unittest.TestCase):
    """Slug-based crypto filter."""

    @staticmethod
    def _client(mode="auto"):
        from ingestion.limitless_client import LimitlessClient
        from project.configuration import IngestionConfig

        return LimitlessClient(config=IngestionConfig(crypto_filter_mode=mode))

    def test_keeps_known_crypto_slugs(self) -> None:
        c = self._client()
        for slug in [
            "btc-above-dollar7922541-on-apr-22-1630-utc-1776874511835",
            "eth-above-dollar240539-on-apr-22-1630-utc-1776874512313",
            "sol-above-dollar8828-on-apr-22-1630-utc-1776874511936",
            "xrp-above-dollar14547-on-apr-22-1630-utc-1776874512314",
            "doge-price-on-apr-22-1700-utc-1776873600476",
            "mnt-above-dollar062899-on-apr-22-1900-utc-1776798002244",
        ]:
            self.assertTrue(c._is_crypto_market({"slug": slug}), slug)

    def test_rejects_non_crypto_slugs(self) -> None:
        c = self._client()
        for slug in [
            "tesla-tsla-above-dollar39119-on-apr-22-1700-utc-1776873966697",
            "nvidia-nvda-above-dollar20120-on-apr-22-1700-utc-1776873606041",
            "oil-ukoilspot-above-dollar97130-on-apr-22-1700-utc-1776873606107",
            "gold-paxg-above-dollar471612-on-apr-22-1700-utc-1776873604597",
            "south-america-rejects-vs-alis-ventorus-1776794410266",
            "aerospace-and-defense-etf-ita-above-dollar22287",
        ]:
            self.assertFalse(c._is_crypto_market({"slug": slug}), slug)

    def test_mode_off_keeps_everything(self) -> None:
        c = self._client(mode="off")
        self.assertTrue(c._is_crypto_market({"slug": "chiefs-win-super-bowl"}))


class LimitlessSnapshotCapTests(unittest.TestCase):
    """max_snapshots_per_cycle guards against runaway fetches."""

    def test_caps_iteration_and_warns(self) -> None:
        import logging as _logging

        from ingestion.limitless_client import LimitlessClient
        from project.configuration import IngestionConfig

        client = LimitlessClient(config=IngestionConfig(max_snapshots_per_cycle=5))
        call_count = {"n": 0}

        def fake_req(path, query=None):
            call_count["n"] += 1
            return {"market_id": path}

        def fake_norm(payload, market_id):
            return {"market_id": market_id}

        client._request_json = fake_req  # type: ignore[assignment]
        client._normalize_snapshot = fake_norm  # type: ignore[assignment]

        ids = [str(i) for i in range(20)]
        with self.assertLogs("ingestion.limitless_client", level=_logging.WARNING) as cm:
            out = client.fetch_market_snapshots(ids)
        self.assertEqual(call_count["n"], 5)
        self.assertEqual(len(out), 5)
        self.assertTrue(
            any("max_snapshots_per_cycle" in msg for msg in cm.output),
            cm.output,
        )


if __name__ == "__main__":
    unittest.main()
