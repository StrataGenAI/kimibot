"""BTC and ETH market data polling client."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from project.configuration import IngestionConfig
from utils.time_utils import parse_utc_timestamp, utc_now


LOGGER = logging.getLogger(__name__)


@dataclass
class CryptoClient:
    """Simple REST client for BTC and ETH price polling."""

    config: IngestionConfig

    @property
    def is_mock_mode(self) -> bool:
        """Return True if no API key is configured."""
        return not bool(self.config.binance_api_key)

    def fetch_quotes(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        """Fetch normalized quotes for the requested symbols."""

        target_symbols = symbols or ["BTCUSDT", "ETHUSDT"]
        rows: list[dict[str, Any]] = []
        for symbol in target_symbols:
            try:
                payload = self._request_json(f"/api/v3/ticker/24hr?symbol={symbol}")
                rows.append(
                    {
                        "symbol": symbol,
                        "timestamp": parse_utc_timestamp(
                            payload.get("closeTime") or utc_now()
                        ),
                        "price": float(payload["lastPrice"]),
                        "volume": float(payload.get("volume") or 0.0),
                        "source": "crypto_rest",
                        "ingested_at": utc_now(),
                    }
                )
            except Exception as exc:
                LOGGER.warning(f"Failed to fetch {symbol}: {exc}")
        return rows

    def _request_json(self, path: str) -> dict[str, Any]:
        """Fetch JSON with exponential backoff."""

        url = f"{self.config.crypto_rest_base_url.rstrip('/')}{path}"
        delay = self.config.retry_base_delay_seconds
        last_error: Exception | None = None

        headers = {"User-Agent": "limitless-trader/0.1"}

        # Add Binance API key header if available
        if self.config.binance_api_key:
            headers["X-MBX-APIKEY"] = self.config.binance_api_key

        for _ in range(5):
            try:
                request = Request(url, headers=headers)
                with urlopen(request, timeout=10) as response:
                    data = response.read().decode("utf-8")
                    return json.loads(data)
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                LOGGER.warning("Crypto request failed for %s: %s", url, exc)
                time.sleep(delay + random.uniform(0.0, 0.25))
                delay = min(delay * 2.0, self.config.retry_max_delay_seconds)
        if last_error is None:
            raise RuntimeError(f"Crypto request failed for {url}")
        raise last_error

    def _sign_request(self, params: dict[str, str]) -> str:
        """Create HMAC signature for Binance signed endpoints."""
        if not self.config.binance_api_secret:
            raise ValueError("Binance API secret not configured")

        query_string = urlencode(params)
        signature = hmac.new(
            self.config.binance_api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return query_string + "&signature=" + signature
