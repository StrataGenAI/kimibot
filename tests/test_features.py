"""Feature pipeline tests."""

from __future__ import annotations

import unittest

from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataStore
from project.configuration import load_config


class FeaturePipelineTests(unittest.TestCase):
    """Validate feature determinism and timestamp safety."""

    def setUp(self) -> None:
        """Load shared fixtures for feature tests."""

        self.config = load_config("config/default.yaml")
        self.store = DataStore(self.config).load()
        self.feature_store = FeatureStore(self.config.data.feature_cache_path, self.config.runtime.feature_schema_version)
        self.feature_store.clear()
        self.builder = FeatureBuilder(self.config.runtime.feature_schema_version)

    def test_time_to_resolution_is_non_negative_and_exact(self) -> None:
        """The required time-to-resolution feature should match metadata exactly."""

        metadata = self.store.market_metadata.set_index("market_id").loc["m5"]
        market_history = self.store.market_snapshots[self.store.market_snapshots["market_id"] == "m5"]
        as_of = market_history.iloc[0]["timestamp"]
        row = self.builder.build_features(
            market_history=market_history,
            crypto_history=self.store.crypto_snapshots,
            as_of=as_of,
            resolution_time=metadata["resolution_time"],
            label=1,
            market_id="m3",
        )
        expected = (metadata["resolution_time"] - as_of).total_seconds()
        self.assertAlmostEqual(row.values["time_to_resolution"], expected, places=0)

    def test_feature_store_returns_same_cached_row(self) -> None:
        """Feature caching should be deterministic across repeated calls."""

        metadata = self.store.market_metadata.set_index("market_id").loc["m6"]
        market_history = self.store.market_snapshots[self.store.market_snapshots["market_id"] == "m6"]
        as_of = market_history.iloc[1]["timestamp"]
        first = self.feature_store.get_or_build(
            "m4",
            as_of,
            lambda: self.builder.build_features(
                market_history=market_history,
                crypto_history=self.store.crypto_snapshots,
                as_of=as_of,
                resolution_time=metadata["resolution_time"],
                label=0,
                market_id="m4",
            ),
        )
        second = self.feature_store.get_or_build(
            "m4",
            as_of,
            lambda: self.builder.build_features(
                market_history=market_history,
                crypto_history=self.store.crypto_snapshots,
                as_of=as_of,
                resolution_time=metadata["resolution_time"],
                label=0,
                market_id="m4",
            ),
        )
        self.assertEqual(first.values, second.values)


if __name__ == "__main__":
    unittest.main()
