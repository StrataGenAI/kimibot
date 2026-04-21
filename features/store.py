"""Cached feature store with strict timestamp alignment."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from project.types import FeatureRow
from utils.time import parse_utc_timestamp


class FeatureStore:
    """Persist and reload deterministic features keyed by market and timestamp."""

    def __init__(self, path: Path, schema_version: str) -> None:
        """Create a feature store backed by a CSV file."""

        self.path = path
        self.schema_version = schema_version
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache = self._load_existing()

    def _load_existing(self) -> pd.DataFrame:
        """Load an existing cache from disk if present."""

        if not self.path.exists():
            return pd.DataFrame()
        try:
            frame = pd.read_csv(self.path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        for column in ("timestamp", "resolution_time", "market_source_max_ts", "crypto_source_max_ts"):
            frame[column] = frame[column].map(parse_utc_timestamp)
        return frame

    def get_or_build(self, key_market_id: str, key_timestamp, builder: Callable[[], FeatureRow]) -> FeatureRow:
        """Return a cached feature row or build and persist it."""

        if not self._cache.empty:
            matches = self._cache[
                (self._cache["market_id"] == key_market_id)
                & (self._cache["timestamp"] == key_timestamp)
                & (self._cache["schema_version"] == self.schema_version)
            ]
            if not matches.empty:
                row = matches.iloc[0].to_dict()
                if pd.isna(row["label"]):
                    rebuilt = builder()
                    if rebuilt.label is not None:
                        self._cache.loc[matches.index[0], "label"] = rebuilt.label
                        self._persist()
                        return rebuilt
                values = {
                    key: float(value)
                    for key, value in row.items()
                    if key
                    not in {
                        "market_id",
                        "timestamp",
                        "resolution_time",
                        "label",
                        "market_source_max_ts",
                        "crypto_source_max_ts",
                        "schema_version",
                    }
                }
                label = None if pd.isna(row["label"]) else int(row["label"])
                return FeatureRow(
                    market_id=str(row["market_id"]),
                    timestamp=row["timestamp"],
                    resolution_time=row["resolution_time"],
                    label=label,
                    values=values,
                    market_source_max_ts=row["market_source_max_ts"],
                    crypto_source_max_ts=row["crypto_source_max_ts"],
                    schema_version=str(row["schema_version"]),
                )

        feature_row = builder()
        self._append(feature_row)
        return feature_row

    def _append(self, feature_row: FeatureRow) -> None:
        """Append a new feature row to the cache and flush it to disk."""

        record = {
            "market_id": feature_row.market_id,
            "timestamp": feature_row.timestamp,
            "resolution_time": feature_row.resolution_time,
            "label": feature_row.label,
            "market_source_max_ts": feature_row.market_source_max_ts,
            "crypto_source_max_ts": feature_row.crypto_source_max_ts,
            "schema_version": feature_row.schema_version,
        }
        record.update(feature_row.values)
        frame = pd.DataFrame([record])
        if self._cache.empty:
            self._cache = frame.copy()
        else:
            self._cache = pd.concat([self._cache, frame], ignore_index=True)
        self._persist()

    def _persist(self) -> None:
        """Persist the in-memory feature cache to disk."""

        persisted = self._cache.copy()
        for column in ("timestamp", "resolution_time", "market_source_max_ts", "crypto_source_max_ts"):
            persisted[column] = persisted[column].map(lambda value: value.isoformat())
        persisted.to_csv(self.path, index=False)

    def clear(self) -> None:
        """Remove cached features from memory and disk."""

        self._cache = pd.DataFrame()
        if self.path.exists():
            self.path.unlink()
