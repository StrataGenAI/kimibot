"""GraphQL client for The Graph subgraph with token-bucket rate limiter."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)

_SUBGRAPH_ID = "BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB"
_GATEWAY_BASE = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/" + _SUBGRAPH_ID

_RESOLVED_MARKETS_GQL = """
query ResolvedMarkets($skip: Int!, $first: Int!) {
  conditions(
    where: { resolved: true }
    orderBy: resolvedAt
    orderDirection: asc
    first: $first
    skip: $skip
  ) {
    id
    resolved
    payoutNumerators
    resolvedAt
    market {
      id
      tradesCount
      volumeUSD
    }
  }
}
"""

_MARKET_TRADES_GQL = """
query MarketTrades($conditionId: ID!, $skip: Int!, $first: Int!) {
  trades(
    where: { market: { id: $conditionId } }
    orderBy: timestamp
    orderDirection: asc
    first: $first
    skip: $skip
  ) {
    id
    type
    price
    amountUSD
    timestamp
  }
}
"""


class TokenBucket:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int = 1) -> None:
        self._rate = rate
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1.0:
                sleep_time = (1.0 - self._tokens) / self._rate
                time.sleep(sleep_time)
                self._last = time.monotonic()
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class SubgraphClient:
    """Read-only GraphQL client for the Limitless subgraph."""

    def __init__(self, api_key: str, rate_per_second: float = 1.0) -> None:
        if not api_key:
            raise ValueError(
                "GRAPH_API_KEY is required. Get a free key at thegraph.com/studio/apikeys "
                "and set it as the GRAPH_API_KEY environment variable."
            )
        self._endpoint = _GATEWAY_BASE.format(api_key=api_key)
        self._bucket = TokenBucket(rate=rate_per_second)

    def query(self, gql: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return the data dict."""
        self._bucket.acquire()
        payload = json.dumps({"query": gql, "variables": variables or {}}).encode()
        req = Request(
            self._endpoint,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
        if "errors" in result:
            raise RuntimeError(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def get_all_resolved_markets(self, min_trades: int = 20) -> list[dict[str, Any]]:
        """Paginate through all resolved conditions and return those with sufficient trades."""
        all_conditions: list[dict[str, Any]] = []
        skip = 0
        page_size = 100
        while True:
            data = self.query(
                _RESOLVED_MARKETS_GQL,
                {"skip": skip, "first": page_size},
            )
            page = data.get("conditions", [])
            LOGGER.info("Subgraph page skip=%d returned %d conditions", skip, len(page))
            all_conditions.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        filtered = [
            c for c in all_conditions
            if c.get("market") and int(c["market"].get("tradesCount", 0) or 0) >= min_trades
        ]
        LOGGER.info(
            "Resolved conditions: %d total, %d with >= %d trades",
            len(all_conditions), len(filtered), min_trades,
        )
        return filtered

    def get_market_trades(self, condition_id: str) -> list[dict[str, Any]]:
        """Fetch all trades for a market, paginating as needed."""
        all_trades: list[dict[str, Any]] = []
        skip = 0
        page_size = 1000
        while True:
            data = self.query(
                _MARKET_TRADES_GQL,
                {"conditionId": condition_id, "skip": skip, "first": page_size},
            )
            page = data.get("trades", [])
            all_trades.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return all_trades
