"""Deterministic, timestamp-safe feature generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from project.types import FeatureRow


@dataclass
class FeatureBuilder:
    """Build deterministic model features from historical context."""

    schema_version: str

    def build_features(
        self,
        market_history: pd.DataFrame,
        crypto_history: pd.DataFrame,
        as_of: datetime,
        resolution_time: datetime,
        label: int | None,
        market_id: str,
    ) -> FeatureRow:
        """Build features for a single market at a single timestamp."""

        safe_market = market_history[market_history["timestamp"] <= as_of].sort_values(
            "timestamp"
        )
        safe_crypto = crypto_history[crypto_history["timestamp"] <= as_of].sort_values(
            "timestamp"
        )
        if safe_market.empty or safe_crypto.empty:
            raise ValueError(
                f"Insufficient history for {market_id} at {as_of.isoformat()}"
            )

        current_market = safe_market.iloc[-1]
        current_crypto = safe_crypto.iloc[-1]
        market_prices = safe_market["p_market"]
        market_volumes = safe_market["volume"]
        btc_prices = safe_crypto["btc_price"]

        values = {
            "p_market": float(current_market["p_market"]),
            "momentum_1m": self._relative_change(
                safe_market, "p_market", as_of, timedelta(minutes=1)
            ),
            "momentum_5m": self._relative_change(
                safe_market, "p_market", as_of, timedelta(minutes=5)
            ),
            "volatility": float(market_prices.tail(5).std(ddof=0))
            if len(market_prices) > 1
            else 0.0,
            "volume_spike": self._volume_spike(market_volumes),
            "btc_return_1m": self._relative_change(
                safe_crypto, "btc_price", as_of, timedelta(minutes=1)
            ),
            "btc_return_5m": self._relative_change(
                safe_crypto, "btc_price", as_of, timedelta(minutes=5)
            ),
            "btc_return_15m": self._relative_change(
                safe_crypto, "btc_price", as_of, timedelta(minutes=15)
            ),
            "btc_volatility": float(
                btc_prices.pct_change().tail(5).fillna(0.0).std(ddof=0)
            ),
            "funding_rate": float(current_crypto.get("funding_rate", 0.0)),
            "time_to_resolution": max((resolution_time - as_of).total_seconds(), 0.0),
        }

        return FeatureRow(
            market_id=market_id,
            timestamp=as_of,
            resolution_time=resolution_time,
            label=label,
            values=values,
            market_source_max_ts=safe_market["timestamp"].max(),
            crypto_source_max_ts=safe_crypto["timestamp"].max(),
            schema_version=self.schema_version,
        )

    @staticmethod
    def _relative_change(
        history: pd.DataFrame, column: str, as_of: datetime, lookback: timedelta
    ) -> float:
        """Compute a relative change using only observations within the lookback window."""

        current = float(history.iloc[-1][column])
        cutoff = as_of - lookback
        prior_rows = history[history["timestamp"] <= cutoff]
        if prior_rows.empty:
            return 0.0
        prior = float(prior_rows.iloc[-1][column])
        if prior == 0.0:
            return 0.0
        return (current - prior) / prior

    @staticmethod
    def _volume_spike(volume_history: pd.Series) -> float:
        """Compute the current volume relative to recent average volume."""

        current_volume = float(volume_history.iloc[-1])
        baseline = float(volume_history.tail(5).mean())
        if baseline == 0.0:
            return 0.0
        return current_volume / baseline

    @staticmethod
    def to_frame(rows: list[FeatureRow]) -> pd.DataFrame:
        """Convert feature rows to a tabular dataframe for model training."""

        records = []
        for row in rows:
            record = {
                "market_id": row.market_id,
                "timestamp": row.timestamp,
                "resolution_time": row.resolution_time,
                "label": row.label,
                "market_source_max_ts": row.market_source_max_ts,
                "crypto_source_max_ts": row.crypto_source_max_ts,
                "schema_version": row.schema_version,
            }
            record.update(row.values)
            records.append(record)
        return pd.DataFrame.from_records(records)
