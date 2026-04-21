"""Historical data loading and normalized storage access."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from datetime import datetime, timezone

from project.configuration import AppConfig


def parse_utc_timestamp(value):
    """Parse timestamp from ISO-8601 string or Unix epoch integer into UTC."""

    if isinstance(value, (int, float)):
        return ensure_utc(datetime.fromtimestamp(value, timezone.utc))
    return ensure_utc(pd.Timestamp(value).to_pydatetime())


def ensure_utc(ts: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""

    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass
class DataBundle:
    """Normalized in-memory tables used by the pipeline."""

    market_metadata: pd.DataFrame
    market_snapshots: pd.DataFrame
    crypto_snapshots: pd.DataFrame


class DataStore:
    """Load normalized market and crypto data from disk."""

    def __init__(self, config: AppConfig) -> None:
        """Create a data store bound to application config."""

        self.config = config

    def load(self) -> DataBundle:
        """Load and normalize all configured datasets."""

        metadata = pd.read_csv(self.config.data.market_metadata_path)
        metadata["resolution_time"] = metadata["resolution_time"].map(
            parse_utc_timestamp
        )
        metadata = metadata.sort_values("resolution_time").reset_index(drop=True)

        market = pd.read_csv(self.config.data.market_snapshots_path)
        market["timestamp"] = market["timestamp"].map(parse_utc_timestamp)
        market = market.sort_values(["timestamp", "market_id"]).reset_index(drop=True)

        crypto = pd.read_csv(self.config.data.crypto_snapshots_path)
        crypto["timestamp"] = crypto["timestamp"].map(parse_utc_timestamp)
        crypto = crypto.sort_values("timestamp").reset_index(drop=True)

        return DataBundle(
            market_metadata=metadata,
            market_snapshots=market,
            crypto_snapshots=crypto,
        )
