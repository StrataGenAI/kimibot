"""Backtest integration tests."""

from __future__ import annotations

import unittest
from dataclasses import replace

from backtest.engine import BacktestEngine
from backtest.validation import ValidationRunner
from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataStore
from project.configuration import load_config


class BacktestIntegrationTests(unittest.TestCase):
    """Validate the end-to-end backtest flow."""

    def test_backtest_produces_metrics_and_trade_log(self) -> None:
        """The sample dataset should produce a stable non-empty result."""

        config = load_config("config/default.yaml")
        config = replace(config, data=replace(config.data, source_mode="synthetic"))
        bundle = DataStore(config).load()
        feature_store = FeatureStore(config.data.feature_cache_path, config.runtime.feature_schema_version)
        feature_store.clear()
        engine = BacktestEngine(
            config=config,
            bundle=bundle,
            feature_builder=FeatureBuilder(config.runtime.feature_schema_version),
            feature_store=feature_store,
        )
        result = engine.run(persist_artifacts=False)
        self.assertIn("total_return", result.metrics)
        self.assertIn("sharpe_ratio", result.metrics)
        self.assertIn("brier_score", result.metrics)
        self.assertTrue(result.trade_log)
        self.assertTrue(result.prediction_log)

    def test_validation_runner_writes_summary_and_reports(self) -> None:
        """Validation mode should produce a summary and baseline artifacts."""

        config = load_config("config/default.yaml")
        config = replace(config, data=replace(config.data, source_mode="synthetic"))
        result = ValidationRunner(config).run("stress")
        self.assertIn("baseline", result)
        self.assertIn("stress", result)
        self.assertTrue(config.data.validation_report_path.exists())
        baseline_metrics = config.data.validation_report_path.parent / "baseline" / "metrics.json"
        self.assertTrue(baseline_metrics.exists())


if __name__ == "__main__":
    unittest.main()
