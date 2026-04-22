"""GraphQL client for The Graph subgraph with token-bucket rate limiter."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class SubgraphError(RuntimeError):
    """Raised when the subgraph gateway returns a non-success response."""


_DEFAULT_USER_AGENT = "kimibot/0.1"

LOGGER = logging.getLogger(__name__)

_SUBGRAPH_ID = "BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB"
_GATEWAY_BASE = "https://gateway.thegraph.com/api"

_RESOLVED_MARKETS_GQL = """
query ResolvedMarkets($skip: Int!, $first: Int!, $minTrades: BigInt!, $resolvedSince: BigInt!) {
  conditions(
    where: {
      resolved: true
      resolvedAt_gte: $resolvedSince
      market_: { tradesCount_gte: $minTrades }
    }
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

    def __init__(self, api_key: str, rate_per_second: float = 5.0) -> None:
        if not api_key:
            raise ValueError(
                "GRAPH_API_KEY is required. Get a free key at thegraph.com/studio/apikeys "
                "and set it as the GRAPH_API_KEY environment variable."
            )
        self._api_key = api_key
        self._endpoint = f"{_GATEWAY_BASE}/{api_key}/subgraphs/id/{_SUBGRAPH_ID}"
        self._endpoint_redacted = f"{_GATEWAY_BASE}/<redacted>/subgraphs/id/{_SUBGRAPH_ID}"
        self._user_agent = os.environ.get("GRAPH_USER_AGENT", _DEFAULT_USER_AGENT)
        self._bucket = TokenBucket(rate=rate_per_second)

    def query(self, gql: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return the data dict."""
        self._bucket.acquire()
        payload = json.dumps({"query": gql, "variables": variables or {}}).encode()
        req = Request(
            self._endpoint,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": self._user_agent,
            },
        )
        try:
            with urlopen(req, timeout=30) as resp:
                body_bytes = resp.read()
                status = resp.status
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500] if exc.fp else ""
            raise SubgraphError(
                f"Gateway returned HTTP {exc.code} for {self._endpoint_redacted}.\n"
                f"Response body: {body}\n"
                f"Common causes: User-Agent blocked by WAF (default Python-urllib is rejected; "
                f"this client sends '{self._user_agent}'), GRAPH_API_KEY invalid or domain-restricted, "
                f"subgraph not authorized for this key."
            ) from exc
        if status != 200:
            body = body_bytes.decode("utf-8", errors="replace")[:500]
            raise SubgraphError(
                f"Gateway returned HTTP {status} for {self._endpoint_redacted}.\n"
                f"Response body: {body}"
            )
        result = json.loads(body_bytes)
        if "errors" in result:
            raise SubgraphError(
                f"GraphQL errors from {self._endpoint_redacted}: {result['errors']}"
            )
        return result["data"]

    def get_all_resolved_markets(
        self,
        min_trades: int = 20,
        resolved_since_unix: int = 1735689600,
    ) -> list[dict[str, Any]]:
        """Paginate resolved conditions, filtering server-side by tradesCount and resolvedAt."""
        all_conditions: list[dict[str, Any]] = []
        skip = 0
        page_size = 100
        variables_base = {
            "minTrades": str(int(min_trades)),
            "resolvedSince": str(int(resolved_since_unix)),
        }
        while True:
            data = self.query(
                _RESOLVED_MARKETS_GQL,
                {"skip": skip, "first": page_size, **variables_base},
            )
            page = data.get("conditions", [])
            LOGGER.info("Subgraph page skip=%d returned %d conditions", skip, len(page))
            all_conditions.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        LOGGER.info(
            "Resolved conditions (server-filtered tradesCount>=%d, resolvedAt>=%d): %d",
            min_trades, resolved_since_unix, len(all_conditions),
        )
        return all_conditions

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
