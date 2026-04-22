"""Tests for DataStore live-mode Parquet loading."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ingestion.data_store import DataStore
from project.configuration import (
    AppConfig,
    AuditConfig,
    CalibrationConfig,
    DataConfig,
    IngestionConfig,
    RuntimeConfig,
    TradingConfig,
    ValidationConfig,
    WalkForwardConfig,
)


def _make_config(tmp: Path, *, source_mode: str) -> AppConfig:
    data_dir = tmp / "data"
    raw_root = data_dir / "raw"
    data_dir.mkdir()
    raw_root.mkdir()
    (data_dir / "market_metadata.csv").write_text(
        "market_id,resolution_time,outcome_yes\n"
        "m1,2026-01-02T00:00:00+00:00,1\n"
    )
    (data_dir / "market_snapshots.csv").write_text(
        "market_id,timestamp,p_market,volume,liquidity\n"
        "m1,2026-01-01T00:00:00+00:00,0.5,100,1000\n"
    )
    (data_dir / "crypto_snapshots.csv").write_text(
        "timestamp,btc_price,eth_price\n"
        "2026-01-01T00:00:00+00:00,100000,3000\n"
    )
    data_cfg = DataConfig(
        market_metadata_path=data_dir / "market_metadata.csv",
        market_snapshots_path=data_dir / "market_snapshots.csv",
        crypto_snapshots_path=data_dir / "crypto_snapshots.csv",
        raw_storage_root=raw_root,
        audit_report_path=data_dir / "audit.json",
        feature_cache_path=data_dir / "features.csv",
        trade_log_path=data_dir / "trade_log.csv",
        metrics_report_path=data_dir / "metrics.json",
        prediction_report_path=data_dir / "predictions.csv",
        validation_report_path=data_dir / "validation.json",
        model_artifact_path=tmp / "m.pkl",
        scaler_artifact_path=tmp / "s.pkl",
        calibrator_artifact_path=tmp / "c.pkl",
        training_metadata_path=tmp / "tm.json",
        source_mode=source_mode,
    )
    return AppConfig(
        data=data_cfg,
        trading=TradingConfig(),
        walk_forward=WalkForwardConfig(),
        runtime=RuntimeConfig(),
        validation=ValidationConfig(),
        calibration=CalibrationConfig(),
        ingestion=IngestionConfig(),
        audit=AuditConfig(),
    )


def _write_live_fixtures(raw_root: Path) -> None:
    """Populate a minimal valid live parquet tree."""

    # Limitless snapshot
    mkt_dir = raw_root / "limitless" / "market_id=98715" / "date=2026-04-22"
    mkt_dir.mkdir(parents=True)
    now = pd.Timestamp("2026-04-22T12:00:00Z")
    snapshot = pd.DataFrame(
        [
            {
                "market_id": "98715",
                "timestamp": now,
                "yes_price": 0.62,
                "volume": 5.0,
                "liquidity": 0.0,
                "source": "limitless",
                "ingested_at": now,
                "event_time": now,
                "ingestion_time": now,
            }
        ]
    )
    snapshot.to_parquet(mkt_dir / "part-0.parquet", index=False)

    # Metadata sidecar
    meta = pd.DataFrame(
        [
            {
                "market_id": "98715",
                "slug": "sol-above-dollar8829",
                "status": "FUNDED",
                "resolution_time": pd.Timestamp("2026-04-22T15:00:00Z"),
                "outcome_yes": pd.NA,
                "resolved": False,
                "first_seen": now,
                "last_seen": now,
            }
        ]
    )
    meta.to_parquet(raw_root / "limitless" / "market_metadata.parquet", index=False)

    # Crypto parquet (BTCUSDT only — eth is optional downstream)
    crypto_dir = raw_root / "crypto" / "symbol=BTCUSDT" / "date=2026-04-22"
    crypto_dir.mkdir(parents=True)
    crypto_row = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": now,
                "price": 75000.0,
                "volume": 1234.5,
                "source": "crypto_rest",
                "ingested_at": now,
                "event_time": now,
                "ingestion_time": now,
            }
        ]
    )
    crypto_row.to_parquet(crypto_dir / "part-0.parquet", index=False)


class DataStoreTests(unittest.TestCase):

    def test_synthetic_mode_preserves_legacy_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = _make_config(Path(td), source_mode="synthetic")
            bundle = DataStore(cfg).load()
        self.assertEqual(list(bundle.market_metadata["market_id"]), ["m1"])
        self.assertIn("p_market", bundle.market_snapshots.columns)
        self.assertIn("btc_price", bundle.crypto_snapshots.columns)

    def test_datastore_loads_parquet_when_live_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            cfg = _make_config(tmp, source_mode="live")
            _write_live_fixtures(cfg.data.raw_storage_root)

            store = DataStore(cfg)
            snapshots = store.load_market_snapshots()
            metadata = store.load_market_metadata()
            crypto = store.load_crypto_snapshots()

        # Snapshots: live numeric market ids, yes_price→p_market rename.
        self.assertEqual(list(snapshots["market_id"].unique()), ["98715"])
        self.assertIn("p_market", snapshots.columns)
        self.assertNotIn("yes_price", snapshots.columns)
        self.assertAlmostEqual(float(snapshots.iloc[0]["p_market"]), 0.62)

        # Metadata: real slug, unresolved, resolution_time parsed.
        self.assertEqual(metadata.iloc[0]["market_id"], "98715")
        self.assertFalse(bool(metadata.iloc[0]["resolved"]))
        self.assertTrue(pd.isna(metadata.iloc[0]["outcome_yes"]))
        self.assertEqual(metadata.iloc[0]["slug"], "sol-above-dollar8829")

        # Crypto: long parquet pivoted to wide btc_price.
        self.assertIn("btc_price", crypto.columns)
        self.assertAlmostEqual(float(crypto.iloc[0]["btc_price"]), 75000.0)

    def test_live_mode_missing_sidecar_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            cfg = _make_config(tmp, source_mode="live")
            # Write snapshots but no metadata sidecar.
            _write_live_fixtures(cfg.data.raw_storage_root)
            (cfg.data.raw_storage_root / "limitless" / "market_metadata.parquet").unlink()
            with self.assertRaises(RuntimeError):
                DataStore(cfg).load_market_metadata()

    def test_live_mode_missing_parquet_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            cfg = _make_config(tmp, source_mode="live")
            # No raw/limitless directory populated at all.
            with self.assertRaises(RuntimeError):
                DataStore(cfg).load_market_snapshots()


if __name__ == "__main__":
    unittest.main()
