"""Append-only raw data recorder and replay store."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path

import pandas as pd

from ingestion.crypto_client import CryptoClient
from ingestion.limitless_client import LimitlessClient
from project.configuration import AppConfig
from utils.time_utils import parse_utc_timestamp, utc_now
from utils.validation import validate_crypto_rows, validate_limitless_rows


LOGGER = logging.getLogger(__name__)


@dataclass
class ParquetRecorder:
    """Append-only Parquet writer partitioned by symbol/market and date."""

    root: Path
    last_timestamp_by_stream: dict[str, pd.Timestamp] = field(default_factory=dict)

    def append_limitless(self, rows: list[dict[str, object]]) -> tuple[int, int]:
        """Validate and append Limitless rows. Returns (accepted, rejected_total)."""

        stats = self.append_limitless_with_stats(rows)
        return stats["accepted"], stats["validation_rejected"] + stats["dedup_rejected"]

    def append_limitless_with_stats(self, rows: list[dict[str, object]]) -> dict[str, int]:
        """Validate and append Limitless rows.

        Returns a dict separating ``validation_rejected`` (real schema/range
        failures) from ``dedup_rejected`` (rows whose timestamp didn't advance
        a stream's running max — expected from REST polling).
        """

        frame = pd.DataFrame(rows)
        valid, validation_rejected = validate_limitless_rows(frame)
        valid, dedup_rejected = self._filter_monotonic(valid, "limitless", "market_id")
        self._write_rows(valid, dataset="limitless", key_column="market_id")
        self._log_rejections("limitless", validation_rejected, dedup_rejected)
        return {
            "accepted": len(valid),
            "validation_rejected": len(validation_rejected),
            "dedup_rejected": len(dedup_rejected),
        }

    def append_crypto(self, rows: list[dict[str, object]]) -> tuple[int, int]:
        """Validate and append crypto rows. Returns (accepted, rejected_total)."""

        stats = self.append_crypto_with_stats(rows)
        return stats["accepted"], stats["validation_rejected"] + stats["dedup_rejected"]

    def append_crypto_with_stats(self, rows: list[dict[str, object]]) -> dict[str, int]:
        """Validate and append crypto rows. See ``append_limitless_with_stats``."""

        frame = pd.DataFrame(rows)
        valid, validation_rejected = validate_crypto_rows(frame)
        valid, dedup_rejected = self._filter_monotonic(valid, "crypto", "symbol")
        self._write_rows(valid, dataset="crypto", key_column="symbol")
        self._log_rejections("crypto", validation_rejected, dedup_rejected)
        return {
            "accepted": len(valid),
            "validation_rejected": len(validation_rejected),
            "dedup_rejected": len(dedup_rejected),
        }

    def _write_rows(self, frame: pd.DataFrame, *, dataset: str, key_column: str) -> None:
        """Write validated rows into append-only Parquet partitions."""

        if frame.empty:
            return
        normalized = frame.copy()
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
        normalized["event_time"] = pd.to_datetime(
            normalized["event_time"] if "event_time" in normalized.columns else normalized["timestamp"],
            utc=True,
        )
        normalized["ingestion_time"] = pd.to_datetime(
            normalized["ingestion_time"] if "ingestion_time" in normalized.columns else normalized["ingested_at"],
            utc=True,
        )
        normalized["ingested_at"] = normalized["ingestion_time"]
        normalized["date"] = normalized["event_time"].dt.strftime("%Y-%m-%d")
        for (key_value, date_value), group in normalized.groupby([key_column, "date"], sort=True):
            partition_dir = self.root / dataset / f"{key_column}={key_value}" / f"date={date_value}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{utc_now().strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
            group.drop(columns=["date"]).to_parquet(partition_dir / file_name, index=False)

    def _filter_monotonic(self, frame: pd.DataFrame, dataset: str, key_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Reject rows that move backward in time within a stream."""

        if frame.empty:
            return frame.copy(), frame.copy()
        ordered = frame.sort_values([key_column, "timestamp"]).reset_index(drop=True).copy()
        # Pre-parse all timestamps in one pass to avoid per-row overhead inside the loop.
        ts_ns: list[int] = [
            pd.Timestamp(parse_utc_timestamp(v)).value for v in ordered["timestamp"]
        ]
        keep: list[bool] = [False] * len(ordered)
        _ts_min = pd.Timestamp.min.value

        for key, group in ordered.groupby(key_column, sort=True):
            stream_key = f"{dataset}:{key}"
            prev = self.last_timestamp_by_stream.get(stream_key)
            prev_ns: int = int(pd.Timestamp(prev).value) if prev is not None else _ts_min
            running_max = prev_ns

            for idx in group.index:
                current = ts_ns[idx]
                if current > running_max:
                    keep[idx] = True
                    running_max = current

            if running_max > prev_ns:
                self.last_timestamp_by_stream[stream_key] = pd.Timestamp(running_max, unit="ns", tz="UTC")

        keep_series = pd.Series(keep)
        filtered = ordered[keep_series].reset_index(drop=True)
        rejected = ordered[~keep_series].copy()
        if not rejected.empty:
            rejected["validation_error"] = "non_monotonic_timestamp"
        return filtered, rejected.reset_index(drop=True)

    @staticmethod
    def _log_rejections(
        dataset: str,
        validation_rejected: pd.DataFrame,
        dedup_rejected: pd.DataFrame,
    ) -> None:
        """Log rejected rows without stopping ingestion.

        Validation failures (schema/range/timestamp) are real problems and
        emit a WARNING with the breakdown. Dedup rejections (REST polling
        returning the same updated_at as last time) are routine and emit at
        DEBUG so logs remain readable.
        """

        if not validation_rejected.empty:
            if "validation_error" in validation_rejected.columns:
                breakdown = validation_rejected["validation_error"].value_counts().to_dict()
            else:
                breakdown = {"unknown": len(validation_rejected)}
            LOGGER.warning(
                "Rejected %d %s rows during validation: %s",
                len(validation_rejected),
                dataset,
                breakdown,
            )
        if not dedup_rejected.empty:
            LOGGER.debug(
                "Deduplicated %d %s rows (already-seen timestamps)",
                len(dedup_rejected),
                dataset,
            )


@dataclass
class RawReplayStore:
    """Replay raw Parquet partitions up to a cutoff time."""

    root: Path

    def read_all_market_data(self, market_ids: list[str] | None = None) -> pd.DataFrame:
        """Return all raw Limitless rows without a cutoff."""

        dataset_root = self.root / "limitless"
        frame = self._read_partitioned(dataset_root, market_ids, "market_id")
        if frame.empty:
            return frame
        return self._normalize_raw_times(frame).sort_values(["event_time", "market_id"]).reset_index(drop=True)

    def read_all_crypto_data(self, symbols: list[str] | None = None) -> pd.DataFrame:
        """Return all raw crypto rows without a cutoff."""

        dataset_root = self.root / "crypto"
        frame = self._read_partitioned(dataset_root, symbols, "symbol")
        if frame.empty:
            return frame
        return self._normalize_raw_times(frame).sort_values(["event_time", "symbol"]).reset_index(drop=True)

    def get_market_data_until(self, timestamp, market_ids: list[str] | None = None) -> pd.DataFrame:
        """Return Limitless rows with timestamps less than or equal to the cutoff."""

        cutoff = pd.Timestamp(parse_utc_timestamp(timestamp))
        frame = self._read_partitioned(
            self.root / "limitless", market_ids, "market_id", max_date=cutoff.strftime("%Y-%m-%d")
        )
        if frame.empty:
            return frame
        normalized = self._normalize_raw_times(frame).sort_values(["event_time", "market_id"]).reset_index(drop=True)
        return normalized[normalized["event_time"] <= cutoff].sort_values(["event_time", "market_id"]).reset_index(drop=True)

    def get_crypto_data_until(self, timestamp, symbols: list[str] | None = None) -> pd.DataFrame:
        """Return crypto rows with timestamps less than or equal to the cutoff."""

        cutoff = pd.Timestamp(parse_utc_timestamp(timestamp))
        frame = self._read_partitioned(
            self.root / "crypto", symbols, "symbol", max_date=cutoff.strftime("%Y-%m-%d")
        )
        if frame.empty:
            return frame
        normalized = self._normalize_raw_times(frame).sort_values(["event_time", "symbol"]).reset_index(drop=True)
        return normalized[normalized["event_time"] <= cutoff].sort_values(["event_time", "symbol"]).reset_index(drop=True)

    def get_market_data_grid(self, market_id: str, start, end, frequency: str = "10s") -> pd.DataFrame:
        """Return a resampled fixed-grid view of a market with forward-filled prices."""

        raw = self.get_market_data_until(end, [market_id])
        if raw.empty:
            return raw
        start_ts = pd.Timestamp(parse_utc_timestamp(start))
        end_ts = pd.Timestamp(parse_utc_timestamp(end))
        raw = raw[(raw["event_time"] >= start_ts) & (raw["event_time"] <= end_ts)].copy()
        if raw.empty:
            return raw
        grid = pd.date_range(start=start_ts, end=end_ts, freq=frequency, tz="UTC")
        reindexed = (
            raw.sort_values("event_time")
            .drop_duplicates(subset=["event_time"], keep="last")
            .set_index("event_time")
            .reindex(grid)
        )
        reindexed["market_id"] = market_id
        reindexed["yes_price"] = reindexed["yes_price"].ffill()
        reindexed["volume"] = reindexed["volume"].ffill()
        reindexed["liquidity"] = reindexed["liquidity"].ffill()
        reindexed["ingestion_time"] = reindexed["ingestion_time"].ffill()
        reindexed["timestamp"] = reindexed.index
        return reindexed.reset_index(names="event_time")

    def get_crypto_data_grid(self, symbol: str, start, end, frequency: str = "10s") -> pd.DataFrame:
        """Return a resampled fixed-grid view of a crypto symbol with forward-filled prices."""

        raw = self.get_crypto_data_until(end, [symbol])
        if raw.empty:
            return raw
        start_ts = pd.Timestamp(parse_utc_timestamp(start))
        end_ts = pd.Timestamp(parse_utc_timestamp(end))
        raw = raw[(raw["event_time"] >= start_ts) & (raw["event_time"] <= end_ts)].copy()
        if raw.empty:
            return raw
        grid = pd.date_range(start=start_ts, end=end_ts, freq=frequency, tz="UTC")
        reindexed = (
            raw.sort_values("event_time")
            .drop_duplicates(subset=["event_time"], keep="last")
            .set_index("event_time")
            .reindex(grid)
        )
        reindexed["symbol"] = symbol
        reindexed["price"] = reindexed["price"].ffill()
        reindexed["volume"] = reindexed["volume"].ffill()
        reindexed["ingestion_time"] = reindexed["ingestion_time"].ffill()
        reindexed["timestamp"] = reindexed.index
        return reindexed.reset_index(names="event_time")

    def replay_integrity_check(
        self,
        *,
        dataset: str,
        sample_count: int,
        keys: list[str] | None = None,
        seed: int = 7,
    ) -> dict[str, object]:
        """Randomly sample replay cutoffs and verify no rows are returned after T."""

        reader = self.get_market_data_until if dataset == "market" else self.get_crypto_data_until
        frame = self.read_all_market_data(keys) if dataset == "market" else self.read_all_crypto_data(keys)
        if frame.empty:
            return {"dataset": dataset, "samples": 0, "failures": 0, "passed": True}
        timestamps = frame["event_time"].dropna().sort_values().tolist()
        rng = random.Random(seed)
        sample_size = min(sample_count, len(timestamps))
        chosen = rng.sample(timestamps, sample_size)
        failures: list[dict[str, object]] = []
        for cutoff in chosen:
            replayed = reader(cutoff, keys)
            if replayed.empty:
                continue
            max_ts = pd.Timestamp(replayed["event_time"].max())
            if max_ts > pd.Timestamp(cutoff):
                failures.append({"cutoff": pd.Timestamp(cutoff).isoformat(), "max_returned": max_ts.isoformat()})
        return {"dataset": dataset, "samples": sample_size, "failures": len(failures), "passed": len(failures) == 0, "examples": failures[:3]}

    def _read_partitioned(
        self,
        dataset_root: Path,
        keys: list[str] | None,
        key_column: str,
        max_date: str | None = None,
    ) -> pd.DataFrame:
        """Read Parquet files from partitioned raw storage.

        When max_date is provided (YYYY-MM-DD), date partitions strictly after
        that date are skipped entirely, avoiding unnecessary I/O.
        """

        if not dataset_root.exists():
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        candidate_dirs: list[Path] = []
        if keys:
            candidate_dirs = [dataset_root / f"{key_column}={key}" for key in keys]
        else:
            candidate_dirs = sorted(p for p in dataset_root.iterdir() if p.is_dir())
        for candidate in candidate_dirs:
            if not candidate.exists():
                continue
            for date_dir in sorted(candidate.iterdir()):
                if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                    continue
                if max_date is not None and date_dir.name[5:] > max_date:
                    continue
                for file_path in sorted(date_dir.glob("*.parquet")):
                    frames.append(pd.read_parquet(file_path))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _normalize_raw_times(frame: pd.DataFrame) -> pd.DataFrame:
        """Backfill canonical raw time columns for legacy datasets."""

        normalized = frame.copy()
        normalized["event_time"] = pd.to_datetime(
            normalized["event_time"] if "event_time" in normalized.columns else normalized["timestamp"],
            utc=True,
        )
        normalized["ingestion_time"] = pd.to_datetime(
            normalized["ingestion_time"] if "ingestion_time" in normalized.columns else normalized["ingested_at"],
            utc=True,
        )
        normalized["timestamp"] = normalized["event_time"]
        normalized["ingested_at"] = normalized["ingestion_time"]
        return normalized


def _write_ingestion_status(status_path: Path, limitless: dict, crypto: dict) -> None:
    status = {"limitless": limitless, "crypto": crypto}
    tmp = status_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, indent=2))
    tmp.replace(status_path)


async def run_ingestion_loop(config: AppConfig) -> None:
    """Run the continuous market and crypto ingestion loop."""

    enabled_env = os.environ.get("INGESTION_ENABLED", str(config.runtime.ingestion_enabled))
    if enabled_env.strip().lower() in ("false", "0", "no"):
        LOGGER.info("Ingestion disabled (INGESTION_ENABLED=%s) — loop exiting.", enabled_env)
        return

    limitless_client = LimitlessClient(config.ingestion)
    crypto_client = CryptoClient(config.ingestion)
    recorder = ParquetRecorder(config.data.raw_storage_root)
    market_queue: asyncio.Queue = asyncio.Queue()
    active_market_ids: list[str] = []

    status_path = Path(config.data.market_metadata_path).parent / "ingestion_status.json"
    limitless_status: dict = {"last_fetch_utc": None, "rows_accepted": 0, "rows_rejected": 0}
    crypto_status: dict = {"last_fetch_utc": None, "rows_accepted": 0, "rows_rejected": 0}

    def _now_utc_str() -> str:
        return utc_now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    metadata_sidecar_path = (
        Path(config.data.raw_storage_root) / "limitless" / "market_metadata.parquet"
    )

    async def discovery_loop() -> None:
        nonlocal limitless_status
        while True:
            try:
                markets = limitless_client.list_active_markets()
                active_market_ids[:] = [market["market_id"] for market in markets]
                if markets:
                    limitless_client.upsert_metadata_sidecar(
                        markets, metadata_sidecar_path
                    )
                if active_market_ids:
                    snapshots = limitless_client.fetch_market_snapshots(active_market_ids)
                    stats = recorder.append_limitless_with_stats(snapshots)
                    limitless_status = {
                        "last_fetch_utc": _now_utc_str(),
                        "rows_accepted": stats["accepted"],
                        "rows_rejected": stats["validation_rejected"],
                        "rows_deduped": stats["dedup_rejected"],
                    }
                    _write_ingestion_status(status_path, limitless_status, crypto_status)
                    LOGGER.info(
                        '{"event":"limitless_flush","rows_accepted":%d,"rows_rejected":%d,'
                        '"rows_deduped":%d,"markets":%d}',
                        stats["accepted"],
                        stats["validation_rejected"],
                        stats["dedup_rejected"],
                        len(active_market_ids),
                    )
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.exception("Market discovery loop failed: %s", exc)
            await asyncio.sleep(config.ingestion.limitless_discovery_interval_seconds)

    async def stream_loop() -> None:
        while True:
            try:
                if not active_market_ids:
                    await asyncio.sleep(1)
                    continue
                await limitless_client.stream_market_snapshots(list(active_market_ids), market_queue)
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.exception("Limitless stream loop failed: %s", exc)
                await asyncio.sleep(config.ingestion.limitless_poll_interval_seconds)

    async def queue_flush_loop() -> None:
        """Drain the stream queue, flushing on timeout or when the buffer is full.

        Previously this flushed after every single item arrived (because
        ``if buffer:`` triggered on each successful ``wait_for``), producing
        one parquet file per row. Now we accumulate until either the
        ``flush_interval_seconds`` timeout fires or the buffer reaches
        ``flush_max_batch`` rows.
        """

        nonlocal limitless_status
        flush_max_batch = max(64, config.ingestion.max_snapshots_per_cycle)
        buffer: list[dict[str, object]] = []
        while True:
            should_flush = False
            try:
                item = await asyncio.wait_for(
                    market_queue.get(),
                    timeout=config.ingestion.flush_interval_seconds,
                )
                buffer.append(item)
                if len(buffer) >= flush_max_batch:
                    should_flush = True
            except asyncio.TimeoutError:
                should_flush = bool(buffer)
            if should_flush:
                stats = recorder.append_limitless_with_stats(buffer)
                limitless_status = {
                    "last_fetch_utc": _now_utc_str(),
                    "rows_accepted": stats["accepted"],
                    "rows_rejected": stats["validation_rejected"],
                    "rows_deduped": stats["dedup_rejected"],
                }
                _write_ingestion_status(status_path, limitless_status, crypto_status)
                LOGGER.info(
                    '{"event":"queue_flush","rows_submitted":%d,"rows_accepted":%d,'
                    '"rows_rejected":%d,"rows_deduped":%d}',
                    len(buffer),
                    stats["accepted"],
                    stats["validation_rejected"],
                    stats["dedup_rejected"],
                )
                buffer = []

    async def crypto_loop() -> None:
        nonlocal crypto_status
        while True:
            try:
                rows = crypto_client.fetch_quotes()
                stats = recorder.append_crypto_with_stats(rows)
                crypto_status = {
                    "last_fetch_utc": _now_utc_str(),
                    "rows_accepted": stats["accepted"],
                    "rows_rejected": stats["validation_rejected"],
                    "rows_deduped": stats["dedup_rejected"],
                }
                _write_ingestion_status(status_path, limitless_status, crypto_status)
                LOGGER.info(
                    '{"event":"crypto_flush","rows_accepted":%d,"rows_rejected":%d,'
                    '"rows_deduped":%d}',
                    stats["accepted"],
                    stats["validation_rejected"],
                    stats["dedup_rejected"],
                )
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.exception("Crypto ingestion loop failed: %s", exc)
            await asyncio.sleep(config.ingestion.crypto_poll_interval_seconds)

    await asyncio.gather(discovery_loop(), stream_loop(), queue_flush_loop(), crypto_loop())
