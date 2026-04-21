"""Data quality audit tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ingestion.audit import DataQualityAuditor
from ingestion.recorder import ParquetRecorder, RawReplayStore
from project.configuration import clone_config, load_config


class DataQualityAuditTests(unittest.TestCase):
    """Validate read-only audit behavior over raw parquet samples."""

    def test_replay_integrity_check_passes_on_sample_data(self) -> None:
        """Replay integrity sampling should not return rows after cutoff."""

        store = RawReplayStore(Path("data/raw"))
        market_report = store.replay_integrity_check(dataset="market", sample_count=5)
        crypto_report = store.replay_integrity_check(dataset="crypto", sample_count=5)
        self.assertTrue(market_report["passed"])
        self.assertTrue(crypto_report["passed"])

    def test_market_resampling_forward_fills_last_price(self) -> None:
        """Resampled market grids should use fixed timestamps and forward-fill prices."""

        store = RawReplayStore(Path("data/raw"))
        grid = store.get_market_data_grid("m3", "2026-01-01T00:07:00Z", "2026-01-01T00:10:00Z", frequency="10s")
        self.assertFalse(grid.empty)
        self.assertEqual(grid.iloc[0]["market_id"], "m3")
        self.assertTrue(pd.notnull(grid.iloc[-1]["yes_price"]))

    def test_auditor_writes_report(self) -> None:
        """The auditor should emit a JSON report without mutating raw data."""

        config = load_config("config/default.yaml")
        with tempfile.TemporaryDirectory() as temp_dir:
            cloned = clone_config(
                config,
                data=config.data.__class__(
                    market_metadata_path=config.data.market_metadata_path,
                    market_snapshots_path=config.data.market_snapshots_path,
                    crypto_snapshots_path=config.data.crypto_snapshots_path,
                    raw_storage_root=config.data.raw_storage_root,
                    audit_report_path=Path(temp_dir) / "audit.json",
                    feature_cache_path=config.data.feature_cache_path,
                    trade_log_path=config.data.trade_log_path,
                    metrics_report_path=config.data.metrics_report_path,
                    prediction_report_path=config.data.prediction_report_path,
                    validation_report_path=config.data.validation_report_path,
                    model_artifact_path=config.data.model_artifact_path,
                    scaler_artifact_path=config.data.scaler_artifact_path,
                    calibrator_artifact_path=config.data.calibrator_artifact_path,
                    training_metadata_path=config.data.training_metadata_path,
                ),
            )
            report = DataQualityAuditor(cloned).run()
            self.assertIn("per_market", report)
            self.assertIn("replay_integrity", report)
            self.assertTrue(cloned.data.audit_report_path.exists())


if __name__ == "__main__":
    unittest.main()
