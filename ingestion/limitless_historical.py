"""Enumerate and cache resolved Limitless markets with their trade history."""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from ingestion.filters import is_crypto_market
from ingestion.subgraph_client import SubgraphClient, TokenBucket
from project.configuration import IngestionConfig

LOGGER = logging.getLogger(__name__)

RAW_DIR = Path("data/limitless_raw")
REST_BASE = "https://api.limitless.exchange"

# REST rate limiter: 1 req per 350ms (safe under 300ms minimum)
_REST_BUCKET = TokenBucket(rate=1.0 / 0.35)


def _rest_get(path: str, retries: int = 5) -> Any:
    """Fetch JSON from Limitless REST API with retries."""
    url = f"{REST_BASE}{path}"
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            _REST_BUCKET.acquire()
            req = Request(url, headers={"User-Agent": "kimibot-historical/0.1"})
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            last_err = exc
            LOGGER.warning("REST attempt %d failed for %s: %s", attempt + 1, url, exc)
            time.sleep(delay + random.uniform(0, 0.25))
            delay = min(delay * 2, 16.0)
    raise last_err  # type: ignore[misc]


def _resolve_outcome(payout_numerators: list[str | int]) -> int | None:
    """Return 1 if YES won, 0 if NO won, None if ambiguous."""
    if not payout_numerators or len(payout_numerators) < 2:
        return None
    try:
        yes_payout = int(payout_numerators[0])
        no_payout = int(payout_numerators[1])
    except (ValueError, TypeError):
        return None
    if yes_payout > 0 and no_payout == 0:
        return 1
    if no_payout > 0 and yes_payout == 0:
        return 0
    return None  # Both or neither non-zero — multi-outcome or invalid


def _fetch_rest_metadata(condition_id: str) -> dict[str, Any] | None:
    """Fetch market metadata from REST API."""
    try:
        payload = _rest_get(f"/markets/{condition_id}")
        if not isinstance(payload, dict):
            return None
        return payload
    except Exception as exc:
        LOGGER.warning("REST metadata fetch failed for %s: %s", condition_id, exc)
        return None


def _parse_resolution_time(market_payload: dict[str, Any]) -> datetime | None:
    """Extract resolution time from REST market payload."""
    for key in ("resolvingAt", "resolveAt", "resolution_time", "expirationDate"):
        val = market_payload.get(key)
        if val:
            try:
                if isinstance(val, (int, float)):
                    return datetime.fromtimestamp(val / 1000.0 if val > 1e10 else val, tz=timezone.utc)
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except Exception:
                continue
    return None


def run_historical_ingestion(
    graph_api_key: str,
    ingestion_config: IngestionConfig,
    cache_dir: Path = RAW_DIR,
) -> list[dict[str, Any]]:
    """
    Enumerate resolved Limitless markets and cache raw data to disk.

    Returns a list of market dicts ready for feature construction.
    Each dict has: condition_id, slug, label, resolution_time_utc, trades (list of {price, amountUSD, timestamp}).
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    cfg = ingestion_config
    min_trades = cfg.historical_min_trades

    client = SubgraphClient(
        api_key=graph_api_key,
        rate_per_second=cfg.subgraph_rate_per_second,
    )

    LOGGER.info(
        "Enumerating resolved markets from subgraph (min_trades>=%d, resolved_since=%d)...",
        min_trades, cfg.historical_resolved_since_unix,
    )
    conditions = client.get_all_resolved_markets(
        min_trades=min_trades,
        resolved_since_unix=cfg.historical_resolved_since_unix,
    )
    LOGGER.info("Found %d candidate resolved markets (server-filtered)", len(conditions))

    markets: list[dict[str, Any]] = []
    success = 0
    skipped = 0
    cached_hits = 0
    non_crypto_dropped = 0
    crypto_kept = 0

    for i, cond in enumerate(conditions):
        condition_id = cond["id"]
        cache_path = cache_dir / f"{condition_id}.json"

        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text())
                # Re-apply crypto filter against cached slug so old caches from
                # pre-filter runs don't leak non-crypto markets into training.
                if is_crypto_market(
                    cached.get("slug"),
                    cfg.crypto_ticker_allowlist,
                    mode=cfg.crypto_filter_mode,
                ):
                    markets.append(cached)
                    cached_hits += 1
                    crypto_kept += 1
                else:
                    non_crypto_dropped += 1
                continue
            except Exception:
                pass  # Re-fetch if cache is corrupt

        LOGGER.info("[%d/%d] Processing condition %s", i + 1, len(conditions), condition_id)

        # Determine outcome from subgraph payoutNumerators
        label = _resolve_outcome(cond.get("payoutNumerators", []))
        if label is None:
            LOGGER.warning("Ambiguous outcome for %s, skipping", condition_id)
            skipped += 1
            continue

        # Fetch REST metadata for resolution time and slug
        rest_meta = _fetch_rest_metadata(condition_id)
        resolution_time_utc: str | None = None
        slug = condition_id

        if rest_meta:
            slug = rest_meta.get("slug") or rest_meta.get("conditionId") or condition_id
            rt = _parse_resolution_time(rest_meta)
            if rt:
                resolution_time_utc = rt.isoformat()

        # Apply crypto filter BEFORE the expensive trade fetch.
        if not is_crypto_market(
            slug,
            cfg.crypto_ticker_allowlist,
            mode=cfg.crypto_filter_mode,
        ):
            non_crypto_dropped += 1
            continue
        crypto_kept += 1

        # Fall back to subgraph resolvedAt if REST doesn't have resolution time
        if resolution_time_utc is None:
            resolved_at = cond.get("resolvedAt")
            if resolved_at:
                try:
                    ts = float(resolved_at)
                    resolution_time_utc = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    pass

        if resolution_time_utc is None:
            LOGGER.warning("Cannot determine resolution time for %s, skipping", condition_id)
            skipped += 1
            continue

        # Fetch trade history from subgraph
        trades = client.get_market_trades(condition_id)
        if len(trades) < min_trades:
            LOGGER.warning("Market %s has only %d trades after fetch, skipping", condition_id, len(trades))
            skipped += 1
            continue

        market_record = {
            "condition_id": condition_id,
            "slug": slug,
            "label": label,
            "resolution_time_utc": resolution_time_utc,
            "trades_count": len(trades),
            "volume_usd": float(cond.get("market", {}).get("volumeUSD") or 0),
            "trades": [
                {
                    "price": float(t["price"]),
                    "amount_usd": float(t.get("amountUSD") or 0),
                    "timestamp": int(t["timestamp"]),
                }
                for t in trades
            ],
        }

        cache_path.write_text(json.dumps(market_record, indent=2))
        markets.append(market_record)
        success += 1

        LOGGER.info(
            "Cached market %s: slug=%s label=%d, %d trades, resolved=%s",
            condition_id, slug, label, len(trades), resolution_time_utc,
        )

    LOGGER.info(
        "historical filter: subgraph_returned=%d crypto_kept=%d non_crypto_dropped=%d",
        len(conditions), crypto_kept, non_crypto_dropped,
    )
    LOGGER.info(
        "Ingestion complete: %d cached hits, %d newly fetched, %d skipped",
        cached_hits, success, skipped,
    )
    return markets
