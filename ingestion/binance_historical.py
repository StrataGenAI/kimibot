"""Fetch and cache BTC/USDT 1-minute klines and funding rates from Binance."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd

from ingestion.subgraph_client import TokenBucket

LOGGER = logging.getLogger(__name__)

SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"
KLINES_DIR = Path("data/binance_raw")

# Binance public rate limit: 1200 weight/min; klines = 2 weight each → 600/min max.
# We cap at 30/min (well under).
_BINANCE_BUCKET = TokenBucket(rate=30.0 / 60.0)


def _binance_get(base: str, path: str, params: dict[str, Any]) -> Any:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base}{path}?{qs}"
    _BINANCE_BUCKET.acquire()
    req = Request(url, headers={"User-Agent": "kimibot-historical/0.1"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def _kline_cache_path(year: int, month: int) -> Path:
    return KLINES_DIR / f"btc_klines_{year:04d}_{month:02d}.parquet"


def _funding_cache_path(year: int, month: int) -> Path:
    return KLINES_DIR / f"btc_funding_{year:04d}_{month:02d}.parquet"


def _fetch_klines_range(start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch all 1m BTC/USDT klines between start_ms and end_ms (inclusive)."""
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        raw = _binance_get(
            SPOT_BASE,
            "/api/v3/klines",
            {"symbol": "BTCUSDT", "interval": "1m", "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not raw:
            break
        for candle in raw:
            open_time_ms = int(candle[0])
            rows.append({
                "timestamp": datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc),
                "btc_open": float(candle[1]),
                "btc_high": float(candle[2]),
                "btc_low": float(candle[3]),
                "btc_price": float(candle[4]),  # close
                "btc_volume": float(candle[5]),
            })
        last_open_ms = int(raw[-1][0])
        cursor = last_open_ms + 60_000  # next minute
        if len(raw) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "btc_open", "btc_high", "btc_low", "btc_price", "btc_volume"])
    return pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def _fetch_funding_range(start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch BTC perpetual funding rates between start_ms and end_ms."""
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        raw = _binance_get(
            FUTURES_BASE,
            "/fapi/v1/fundingRate",
            {"symbol": "BTCUSDT", "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not raw:
            break
        for item in raw:
            rows.append({
                "timestamp": datetime.fromtimestamp(int(item["fundingTime"]) / 1000.0, tz=timezone.utc),
                "funding_rate": float(item["fundingRate"]),
            })
        cursor = int(raw[-1]["fundingTime"]) + 1
        if len(raw) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "funding_rate"])
    return pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def _month_range(start: datetime, end: datetime) -> list[tuple[int, int]]:
    """Return list of (year, month) tuples covering start..end inclusive."""
    result: list[tuple[int, int]] = []
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur <= end:
        result.append((cur.year, cur.month))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return result


def ensure_btc_data(
    start: datetime,
    end: datetime,
    cache_dir: Path = KLINES_DIR,
) -> None:
    """
    Ensure BTC klines and funding rates are cached for the given date range.
    Fetches only months not already cached. Idempotent.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    # Add 1-hour buffer before start for momentum lookback
    buffered_start = start - timedelta(hours=1)

    for year, month in _month_range(buffered_start, end):
        klines_path = _kline_cache_path(year, month)
        if not klines_path.exists():
            LOGGER.info("Fetching BTC klines for %04d-%02d...", year, month)
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            else:
                month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            actual_start = max(buffered_start, month_start)
            actual_end = min(end, month_end)
            df = _fetch_klines_range(
                int(actual_start.timestamp() * 1000),
                int(actual_end.timestamp() * 1000),
            )
            df.to_parquet(klines_path, index=False)
            LOGGER.info("Cached %d klines for %04d-%02d", len(df), year, month)
        else:
            LOGGER.debug("Klines cache hit: %s", klines_path)

        funding_path = _funding_cache_path(year, month)
        if not funding_path.exists():
            LOGGER.info("Fetching BTC funding rates for %04d-%02d...", year, month)
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            else:
                month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            actual_start = max(buffered_start, month_start)
            actual_end = min(end, month_end)
            df = _fetch_funding_range(
                int(actual_start.timestamp() * 1000),
                int(actual_end.timestamp() * 1000),
            )
            df.to_parquet(funding_path, index=False)
            LOGGER.info("Cached %d funding rates for %04d-%02d", len(df), year, month)
        else:
            LOGGER.debug("Funding cache hit: %s", funding_path)


def load_btc_klines(start: datetime, end: datetime, cache_dir: Path = KLINES_DIR) -> pd.DataFrame:
    """Load cached BTC klines for the given range into a single DataFrame."""
    frames: list[pd.DataFrame] = []
    for year, month in _month_range(start - timedelta(hours=1), end):
        path = _kline_cache_path(year, month)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        raise FileNotFoundError(f"No BTC kline cache found for {start} to {end}. Run ensure_btc_data() first.")
    df = pd.concat(frames, ignore_index=True)
    df = df[df["timestamp"] >= start - timedelta(hours=1)]
    df = df[df["timestamp"] <= end]
    return df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def load_btc_funding(start: datetime, end: datetime, cache_dir: Path = KLINES_DIR) -> pd.DataFrame:
    """Load cached BTC funding rates for the given range."""
    frames: list[pd.DataFrame] = []
    for year, month in _month_range(start - timedelta(hours=1), end):
        path = _funding_cache_path(year, month)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame(columns=["timestamp", "funding_rate"])
    df = pd.concat(frames, ignore_index=True)
    df = df[df["timestamp"] >= start - timedelta(hours=1)]
    df = df[df["timestamp"] <= end]
    return df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def build_crypto_history(
    start: datetime,
    end: datetime,
    cache_dir: Path = KLINES_DIR,
) -> pd.DataFrame:
    """
    Build crypto_history DataFrame for FeatureBuilder from cached Binance data.

    Columns: timestamp, btc_price, eth_price (0.0), funding_rate
    """
    klines = load_btc_klines(start, end, cache_dir)
    funding = load_btc_funding(start, end, cache_dir)

    # Forward-fill funding rate onto kline timestamps using merge_asof
    if not funding.empty:
        merged = pd.merge_asof(
            klines.sort_values("timestamp"),
            funding.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )
        merged["funding_rate"] = merged["funding_rate"].fillna(0.0)
    else:
        merged = klines.copy()
        merged["funding_rate"] = 0.0

    merged["eth_price"] = 0.0
    return merged[["timestamp", "btc_price", "eth_price", "funding_rate"]].reset_index(drop=True)
