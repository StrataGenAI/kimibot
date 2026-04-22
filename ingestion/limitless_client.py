"""Limitless market discovery and snapshot collection."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pathlib import Path

import pandas as pd
import websockets

from project.configuration import IngestionConfig
from utils.time_utils import parse_utc_timestamp, utc_now


LOGGER = logging.getLogger(__name__)


@dataclass
class LimitlessClient:
    """REST and WebSocket client for Limitless market data collection."""

    config: IngestionConfig
    # Map market_id (numeric-as-string) -> slug. Single-market REST calls
    # require the slug; the id is kept as the canonical partition key.
    _slug_by_id: dict[str, str] = field(default_factory=dict)

    @property
    def is_mock_mode(self) -> bool:
        """Return True if no API key is configured."""
        return not bool(self.config.limitless_api_key)

    def list_active_markets(self) -> list[dict[str, Any]]:
        """Fetch and normalize active markets from the REST API.

        Paginates through ``/markets/active?page=N`` until an empty or short
        page is returned (or the declared ``totalMarketsCount`` is reached,
        or ``pagination_max_pages`` is hit). Applies the crypto filter then
        the allow/deny filter. Logs a single JSON-ish ``limitless_discovery``
        event per cycle with raw/crypto/final counts.
        """

        cfg = self.config
        raw: list[dict[str, Any]] = []
        total: int | None = None
        pages_fetched = 0
        for page in range(1, cfg.pagination_max_pages + 1):
            payload = self._request_json("/markets/active", query={"page": page})
            items = self._unwrap_list(payload)
            pages_fetched += 1
            raw.extend(items)
            if isinstance(payload, dict) and total is None:
                declared = payload.get("totalMarketsCount")
                if isinstance(declared, int):
                    total = declared
            if not items or len(items) < cfg.pagination_page_size:
                break
            if total is not None and len(raw) >= total:
                break
            if cfg.pagination_delay_seconds > 0:
                time.sleep(cfg.pagination_delay_seconds)

        normalized = self._normalize_market_entries(raw)
        crypto = [m for m in normalized if self._is_crypto_market(m)]
        final = self._filter_markets(crypto)

        LOGGER.info(
            '{"event":"limitless_discovery","pages":%d,"raw_markets":%d,'
            '"crypto_markets":%d,"after_allowlist":%d,"total_declared":%s}',
            pages_fetched,
            len(normalized),
            len(crypto),
            len(final),
            total if total is not None else "null",
        )
        return final

    def _normalize_market_entries(
        self, markets: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Project raw market dicts down to the canonical discovery shape."""

        normalized: list[dict[str, Any]] = []
        for market in markets:
            market_id = str(
                market.get("market_id") or market.get("id") or market.get("slug") or ""
            )
            if not market_id:
                continue
            slug = str(market.get("slug") or market_id)
            self._slug_by_id[market_id] = slug
            normalized.append(
                {
                    "market_id": market_id,
                    "slug": slug,
                    "status": market.get("status", "active"),
                    "resolution_time": market.get("resolution_time")
                    or market.get("resolveAt")
                    or market.get("expirationTimestamp")
                    or market.get("expirationDate"),
                    "tags": market.get("tags") or [],
                }
            )
        return normalized

    def _is_crypto_market(self, market: dict[str, Any]) -> bool:
        """Return True iff the market's slug begins with a known crypto ticker.

        Limitless does not expose a reliable category field or native filter
        (verified empirically 2026-04-22). Slug shape is consistently
        ``<ticker>-<...>`` for price markets, so we anchor the match to the
        slug start. Unknown slugs are rejected; rejections are logged at
        DEBUG to make list tuning easy.
        """

        mode = self.config.crypto_filter_mode
        if mode == "off":
            return True
        slug = str(market.get("slug") or "").lower()
        if not slug:
            return False
        for ticker in self.config.crypto_ticker_allowlist:
            t = str(ticker).lower()
            if not t:
                continue
            if slug == t or slug.startswith(f"{t}-"):
                return True
        LOGGER.debug("crypto filter rejected slug=%s", slug)
        return False

    @staticmethod
    def _unwrap_list(payload: Any) -> list[dict[str, Any]]:
        """Extract a market list from one of the common response shapes."""

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "markets", "items", "results", "list"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value
        return []

    def fetch_orderbook_depth(
        self, slug: str, price_tolerance: float = 0.02
    ) -> float:
        """Fetch effective orderbook depth in USDC within ``price_tolerance`` of mid.

        TODO(phase-b): Implement before live trading. Endpoint to probe:
        ``/markets/{slug}/orderbook``. Should return the total USDC fillable
        within ± ``price_tolerance`` of the mid price. Until this is wired up
        the DecisionEngine must either run with
        ``paper_mode_unsafe_liquidity=True`` (Phase A only) or be fed depth
        from another source.
        """

        raise NotImplementedError(
            "fetch_orderbook_depth — see Option A plan, Phase B blocker"
        )

    def upsert_metadata_sidecar(
        self, markets: list[dict[str, Any]], sidecar_path: Path
    ) -> None:
        """Merge discovered markets into a Parquet metadata sidecar.

        The sidecar is the canonical source of ``market_metadata`` for the
        live DataStore path. For each active market we record
        ``{market_id, slug, status, resolution_time, outcome_yes (NA),
        resolved (False), first_seen, last_seen}``. Repeated calls upsert
        by ``market_id``: ``last_seen`` and ``status`` advance; ``first_seen``
        is preserved.
        """

        if not markets:
            return
        now = utc_now()
        rows = []
        for market in markets:
            rows.append(
                {
                    "market_id": str(market.get("market_id", "")),
                    "slug": str(market.get("slug") or market.get("market_id", "")),
                    "status": str(market.get("status", "")),
                    "resolution_time": self._parse_resolution_time(
                        market.get("resolution_time")
                    ),
                    "outcome_yes": pd.NA,
                    "resolved": False,
                    "first_seen": now,
                    "last_seen": now,
                }
            )
        incoming = pd.DataFrame(rows)
        incoming = incoming[incoming["market_id"] != ""]
        if incoming.empty:
            return

        if sidecar_path.exists():
            try:
                existing = pd.read_parquet(sidecar_path)
            except Exception as exc:  # pragma: no cover - disk corruption
                LOGGER.warning(
                    "Failed to read metadata sidecar at %s: %s — overwriting.",
                    sidecar_path,
                    exc,
                )
                existing = None
        else:
            existing = None

        if existing is not None and not existing.empty:
            first_seen_lookup = (
                existing.groupby("market_id")["first_seen"].min().to_dict()
            )
            combined = pd.concat([existing, incoming], ignore_index=True)
            combined = combined.sort_values("last_seen").drop_duplicates(
                subset=["market_id"], keep="last"
            )
            combined["first_seen"] = combined["market_id"].map(first_seen_lookup).fillna(
                combined["first_seen"]
            )
        else:
            combined = incoming

        combined = combined.sort_values("market_id").reset_index(drop=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = sidecar_path.with_suffix(".tmp.parquet")
        combined.to_parquet(tmp, index=False)
        tmp.replace(sidecar_path)

    @staticmethod
    def _parse_resolution_time(raw: Any) -> pd.Timestamp:
        """Coerce the many resolution_time shapes into a UTC Timestamp (NaT on fail)."""

        if raw is None:
            return pd.NaT
        try:
            if pd.isna(raw):
                return pd.NaT
        except (TypeError, ValueError):
            pass
        if isinstance(raw, (int, float)):
            unit = "ms" if raw > 1e12 else "s"
            try:
                return pd.to_datetime(int(raw), unit=unit, utc=True)
            except (ValueError, OverflowError):
                return pd.NaT
        if isinstance(raw, str):
            try:
                return pd.to_datetime(raw, utc=True, errors="coerce")
            except Exception:
                return pd.NaT
        if isinstance(raw, pd.Timestamp):
            return raw.tz_convert("UTC") if raw.tzinfo else raw.tz_localize("UTC")
        return pd.NaT

    def fetch_market_snapshots(self, market_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch market snapshots over REST for polling fallback or bootstrap."""

        cap = self.config.max_snapshots_per_cycle
        if cap and len(market_ids) > cap:
            LOGGER.warning(
                "fetch_market_snapshots received %d ids, capping to "
                "max_snapshots_per_cycle=%d",
                len(market_ids),
                cap,
            )
            market_ids = market_ids[:cap]
        snapshots: list[dict[str, Any]] = []
        for market_id in market_ids:
            slug = self._slug_by_id.get(market_id, market_id)
            try:
                payload = self._request_json(f"/markets/{slug}")
            except Exception as exc:  # pragma: no cover - network path
                LOGGER.exception(
                    "Limitless market poll failed for %s: %s", market_id, exc
                )
                continue
            snapshots.append(self._normalize_snapshot(payload, market_id))
        return [row for row in snapshots if row["market_id"]]

    async def stream_market_snapshots(
        self, market_ids: list[str], output_queue: asyncio.Queue
    ) -> None:
        """Stream market snapshots into a queue, falling back to polling on failure."""

        if not self.config.limitless_ws_url:
            # No WS configured — run a REST polling loop with sleep so the
            # outer stream_loop() does not tight-spin when polling returns.
            while True:
                await self._poll_market_snapshots(market_ids, output_queue)
                await asyncio.sleep(self.config.limitless_poll_interval_seconds)

        # Build WebSocket connection headers
        ws_headers = []
        if self.config.limitless_api_key:
            ws_headers.append(("X-API-Key", self.config.limitless_api_key))

        backoff = self.config.retry_base_delay_seconds
        while True:  # pragma: no cover - event loop path
            try:
                async with websockets.connect(
                    self.config.limitless_ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    additional_headers=ws_headers if ws_headers else None,
                ) as websocket:
                    subscribe_message = json.dumps(
                        {
                            "type": "subscribe",
                            "channels": [{"name": "markets", "market_ids": market_ids}],
                        }
                    )
                    await websocket.send(subscribe_message)
                    backoff = self.config.retry_base_delay_seconds
                    async for message in websocket:
                        parsed = json.loads(message)
                        snapshot = self._normalize_snapshot(
                            parsed, parsed.get("market_id") or parsed.get("id") or ""
                        )
                        if snapshot["market_id"]:
                            await output_queue.put(snapshot)
            except Exception as exc:
                LOGGER.exception(
                    "Limitless websocket stream failed, falling back to polling: %s",
                    exc,
                )
                await self._poll_market_snapshots(market_ids, output_queue)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.config.retry_max_delay_seconds)

    async def _poll_market_snapshots(
        self, market_ids: list[str], output_queue: asyncio.Queue
    ) -> None:
        """Emit REST-polled snapshots into the queue."""

        snapshots = self.fetch_market_snapshots(market_ids)
        for snapshot in snapshots:
            await output_queue.put(snapshot)

    def _request_json(
        self, path: str, query: dict[str, Any] | None = None
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Fetch JSON with exponential backoff."""

        suffix = f"?{urlencode(query)}" if query else ""
        url = f"{self.config.limitless_rest_base_url.rstrip('/')}{path}{suffix}"
        delay = self.config.retry_base_delay_seconds
        last_error: Exception | None = None

        headers = {"User-Agent": "limitless-trader/0.1"}
        if self.config.limitless_api_key:
            headers["X-API-Key"] = self.config.limitless_api_key

        for _ in range(5):
            try:
                request = Request(url, headers=headers)
                with urlopen(request, timeout=10) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                LOGGER.warning("Limitless request failed for %s: %s", url, exc)
                time.sleep(delay + random.uniform(0.0, 0.25))
                delay = min(delay * 2.0, self.config.retry_max_delay_seconds)
        if last_error is None:
            raise RuntimeError(f"Limitless request failed for {url}")
        raise last_error

    def _normalize_snapshot(
        self, payload: dict[str, Any], market_id: str
    ) -> dict[str, Any]:
        """Normalize varying payload shapes into the raw Limitless schema."""

        yes_price = payload.get("yes_price") or payload.get("yesPrice")
        prices = payload.get("prices")
        if yes_price is None and isinstance(prices, dict):
            yes_price = prices.get("yes")
        if yes_price is None and isinstance(prices, list) and prices:
            # Limitless returns `prices: [yes, no]` in [0, 1]. API occasionally
            # returns them scaled to [0, 100] — detect and normalize.
            try:
                yes_price = float(prices[0])
                if yes_price > 1.0:
                    yes_price = yes_price / 100.0
            except (TypeError, ValueError):
                yes_price = None
        if yes_price is None:
            yes_price = payload.get("probability") or payload.get("price")
        timestamp = (
            payload.get("timestamp")
            or payload.get("updated_at")
            or payload.get("updatedAt")
            or payload.get("lastUpdatedAt")
            or utc_now()
        )
        volume = (
            payload.get("volumeFormatted")
            if payload.get("volumeFormatted") is not None
            else payload.get("volume")
            if payload.get("volume") is not None
            else payload.get("volume24h") or 0.0
        )
        return {
            "market_id": str(
                market_id or payload.get("market_id") or payload.get("id") or ""
            ),
            "timestamp": parse_utc_timestamp(timestamp),
            "yes_price": float(yes_price) if yes_price is not None else None,
            "volume": float(volume or 0.0),
            "liquidity": float(
                payload.get("liquidity")
                or payload.get("liquidity24h")
                or payload.get("open_interest")
                or 0.0
            ),
            "source": "limitless",
            "ingested_at": utc_now(),
        }

    def _filter_markets(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply allow/deny filters to discovered markets."""

        allow = set(self.config.market_allowlist)
        deny = set(self.config.market_denylist)
        filtered = []
        for market in markets:
            market_id = market["market_id"]
            if allow and market_id not in allow:
                continue
            if market_id in deny:
                continue
            filtered.append(market)
        return filtered
