"""Limitless market discovery and snapshot collection."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import websockets

from project.configuration import IngestionConfig
from utils.time_utils import parse_utc_timestamp, utc_now


LOGGER = logging.getLogger(__name__)


@dataclass
class LimitlessClient:
    """REST and WebSocket client for Limitless market data collection."""

    config: IngestionConfig

    @property
    def is_mock_mode(self) -> bool:
        """Return True if no API key is configured."""
        return not bool(self.config.limitless_api_key)

    def list_active_markets(self) -> list[dict[str, Any]]:
        """Fetch and normalize active markets from the REST API."""

        payload = self._request_json("/markets/active")
        markets = payload if isinstance(payload, list) else payload.get("markets", [])
        normalized: list[dict[str, Any]] = []
        for market in markets:
            market_id = str(
                market.get("market_id") or market.get("id") or market.get("slug") or ""
            )
            if not market_id:
                continue
            normalized.append(
                {
                    "market_id": market_id,
                    "slug": market.get("slug", market_id),
                    "status": market.get("status", "active"),
                    "resolution_time": market.get("resolution_time")
                    or market.get("resolveAt"),
                }
            )
        return self._filter_markets(normalized)

    def fetch_market_snapshots(self, market_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch market snapshots over REST for polling fallback or bootstrap."""

        snapshots: list[dict[str, Any]] = []
        for market_id in market_ids:
            try:
                payload = self._request_json(f"/markets/{market_id}")
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
            await self._poll_market_snapshots(market_ids, output_queue)
            return

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

        yes_price = payload.get("yes_price")
        if yes_price is None and isinstance(payload.get("prices"), dict):
            yes_price = payload["prices"].get("yes")
        if yes_price is None:
            yes_price = payload.get("probability") or payload.get("price")
        timestamp = (
            payload.get("timestamp")
            or payload.get("updated_at")
            or payload.get("updatedAt")
            or utc_now()
        )
        return {
            "market_id": str(
                market_id or payload.get("market_id") or payload.get("id") or ""
            ),
            "timestamp": parse_utc_timestamp(timestamp),
            "yes_price": float(yes_price) if yes_price is not None else None,
            "volume": float(payload.get("volume") or payload.get("volume24h") or 0.0),
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
