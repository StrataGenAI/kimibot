"""Walk-forward training tests."""

from __future__ import annotations

import unittest

from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataStore
from models.trainer import WalkForwardTrainer
from project.configuration import load_config


class WalkForwardTests(unittest.TestCase):
    """Validate fold construction and anti-leakage rules."""

    def setUp(self) -> None:
        """Load shared fixtures for walk-forward testing."""

        self.config = load_config("config/default.yaml")
        self.bundle = DataStore(self.config).load()
        self.feature_store = FeatureStore(self.config.data.feature_cache_path, self.config.runtime.feature_schema_version)
        self.feature_store.clear()
        self.trainer = WalkForwardTrainer(
            self.config,
            self.bundle,
            FeatureBuilder(self.config.runtime.feature_schema_version),
            self.feature_store,
        )

    def test_folds_use_only_past_resolved_markets_for_training(self) -> None:
        """Each fold should train on markets resolved before the test segment."""

        folds = self.trainer.build_folds()
        self.assertGreaterEqual(len(folds), 2)
        metadata = self.bundle.market_metadata.set_index("market_id")
        for fold in folds:
            for market_id in fold.train_market_ids:
                self.assertLessEqual(metadata.loc[market_id, "resolution_time"], fold.model_train_end_time)
            for market_id in fold.calibration_market_ids:
                self.assertGreaterEqual(metadata.loc[market_id, "resolution_time"], fold.model_train_end_time)
                self.assertLessEqual(metadata.loc[market_id, "resolution_time"], fold.calibration_end_time)
            for market_id in fold.test_market_ids:
                self.assertGreaterEqual(metadata.loc[market_id, "resolution_time"], fold.test_start_time)

    def test_trains_at_least_one_fold(self) -> None:
        """The sample dataset should support at least one trainable fold."""

        trained = self.trainer.train_folds()
        self.assertGreaterEqual(len(trained), 1)

    def test_strict_holdout_uses_later_markets_only(self) -> None:
        """Strict holdout should train once on early markets and test on later unseen markets."""

        trained = self.trainer.train_strict_holdout()
        self.assertEqual(len(trained), 1)
        fold = trained[0].fold
        metadata = self.bundle.market_metadata.set_index("market_id")
        latest_train_resolution = max(metadata.loc[market_id, "resolution_time"] for market_id in fold.calibration_market_ids)
        earliest_test_resolution = min(metadata.loc[market_id, "resolution_time"] for market_id in fold.test_market_ids)
        self.assertLessEqual(latest_train_resolution, earliest_test_resolution)


if __name__ == "__main__":
    unittest.main()
