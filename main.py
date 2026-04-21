"""Primary CLI for backtest and live simulation."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from backtest.engine import BacktestEngine
from backtest.validation import ValidationRunner
from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.audit import DataQualityAuditor
from ingestion.data_store import DataStore
from ingestion.recorder import run_ingestion_loop
from project.configuration import load_config
from utils.logging import configure_logging


def run_backtest(config_path: str) -> None:
    """Run the full walk-forward backtest and persist reports."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    bundle = DataStore(config).load()
    feature_store = FeatureStore(
        config.data.feature_cache_path, config.runtime.feature_schema_version
    )
    engine = BacktestEngine(
        config=config,
        bundle=bundle,
        feature_builder=FeatureBuilder(config.runtime.feature_schema_version),
        feature_store=feature_store,
    )
    result = engine.run(persist_artifacts=True)
    print(result.metrics)


def run_live_sim(config_path: str) -> None:
    """Run a simulated live loop over the historical replay source."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    bundle = DataStore(config).load()
    feature_store = FeatureStore(
        config.data.feature_cache_path, config.runtime.feature_schema_version
    )
    engine = BacktestEngine(
        config=config,
        bundle=bundle,
        feature_builder=FeatureBuilder(config.runtime.feature_schema_version),
        feature_store=feature_store,
    )
    result = engine.run(persist_artifacts=False)
    for trade in result.trade_log:
        print(trade)
        if config.runtime.live_sim_sleep_seconds > 0.0:
            time.sleep(config.runtime.live_sim_sleep_seconds)
    print(result.metrics)


def run_validate(config_path: str, validation_mode: str) -> None:
    """Run the validation harness for one or more experiment modes."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    result = ValidationRunner(config).run(validation_mode)
    print(result)


def run_ingest(config_path: str) -> None:
    """Run the continuous raw ingestion loop."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    asyncio.run(run_ingestion_loop(config))


def run_audit_data(config_path: str) -> None:
    """Run the read-only raw data quality audit."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    report = DataQualityAuditor(config).run()
    print(report)


def run_sanity(config_path: str) -> None:
    """Run a quick sanity check on the predictor and write data/sanity_report.json."""

    import numpy as np
    from project.types import FeatureRow
    from models.predictor import LogisticRegressionPredictor, FEATURE_COLUMNS

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)

    model_path = Path(config.data.model_artifact_path)
    scaler_path = Path(config.data.scaler_artifact_path)
    calibrator_path = Path(config.data.calibrator_artifact_path)
    meta_path = Path(config.data.training_metadata_path)

    if not model_path.exists():
        print("ERROR: No trained model found. Run: python train.py", file=sys.stderr)
        sys.exit(1)

    predictor = LogisticRegressionPredictor.load(model_path, scaler_path, calibrator_path, meta_path)

    _CASES = [
        {"id": "high_yes_market",   "p_market": 0.90, "features": {c: 0.9 for c in FEATURE_COLUMNS}},
        {"id": "low_yes_market",    "p_market": 0.10, "features": {c: 0.1 for c in FEATURE_COLUMNS}},
        {"id": "neutral_market",    "p_market": 0.50, "features": {c: 0.5 for c in FEATURE_COLUMNS}},
        {"id": "high_vol_market",   "p_market": 0.60, "features": {c: 0.7 for c in FEATURE_COLUMNS}},
        {"id": "low_vol_market",    "p_market": 0.40, "features": {c: 0.3 for c in FEATURE_COLUMNS}},
        {"id": "all_zeros",         "p_market": 0.50, "features": {c: 0.0 for c in FEATURE_COLUMNS}},
        {"id": "all_ones",          "p_market": 0.50, "features": {c: 1.0 for c in FEATURE_COLUMNS}},
        {"id": "negative_features", "p_market": 0.50, "features": {c: -1.0 for c in FEATURE_COLUMNS}},
        {"id": "mixed_a",           "p_market": 0.55, "features": {c: float(i % 3) / 2.0 for i, c in enumerate(FEATURE_COLUMNS)}},
        {"id": "mixed_b",           "p_market": 0.45, "features": {c: float((i + 1) % 3) / 2.0 for i, c in enumerate(FEATURE_COLUMNS)}},
    ]

    cases_out = []
    failures = []
    all_passed = True

    for case in _CASES:
        row = FeatureRow(
            market_id="sanity",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            **case["features"],
        )
        p_raw = predictor.predict_raw(row)
        p_cal = predictor.predict(row)
        in_range = 0.05 <= p_raw <= 0.95 and 0.05 <= p_cal <= 0.95
        if not in_range:
            all_passed = False
            failures.append({"id": case["id"], "p_model_raw": p_raw, "p_model_calibrated": p_cal})
        cases_out.append({
            "id": case["id"],
            "p_market": case["p_market"],
            "p_model_raw": round(p_raw, 4),
            "p_model_calibrated": round(p_cal, 4),
            "expected_range": [0.05, 0.95],
            "passed": in_range,
        })

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "passed": all_passed,
        "cases": cases_out,
        "failures": failures,
    }

    data_dir = Path(config.data.market_metadata_path).parent
    report_path = data_dir / "sanity_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    if not all_passed:
        print(f"\nSANITY FAILED: {len(failures)} case(s) out of range", file=sys.stderr)
        sys.exit(1)
    print(f"\nSANITY PASSED: all {len(cases_out)} cases in [0.05, 0.95]")


def main() -> None:
    """Parse CLI arguments and dispatch to the requested mode."""

    parser = argparse.ArgumentParser(
        description="Limitless prediction market trading system."
    )
    parser.add_argument(
        "mode",
        choices=["backtest", "live-sim", "validate", "ingest", "audit-data", "sanity"],
        help="Execution mode.",
    )
    parser.add_argument(
        "--config", default="config/default.yaml", help="Path to YAML or JSON config."
    )
    parser.add_argument(
        "--validation-mode",
        choices=["stress", "shuffle", "holdout", "diagnostics", "all"],
        default="all",
        help="Validation mode when running `validate`.",
    )
    parser.add_argument(
        "--mock", action="store_true", help="Force mock mode (ignore API keys)."
    )
    args = parser.parse_args()

    # Check API keys and warn about mock mode
    from project.configuration import load_config

    config = load_config(args.config)

    if args.mock:
        import logging

        logging.getLogger("limitless").warning("Running in MOCK mode (--mock flag)")
    elif not config.ingestion.limitless_api_key:
        import logging

        logging.getLogger("limitless").warning(
            "No LIMITLESS_API_KEY found, using mock mode"
        )
    if not config.ingestion.binance_api_key:
        import logging

        logging.getLogger("crypto").warning(
            "No BINANCE_API_KEY found, using public Binance API"
        )

    if args.mode == "backtest":
        run_backtest(args.config)
    elif args.mode == "validate":
        run_validate(args.config, args.validation_mode)
    elif args.mode == "ingest":
        run_ingest(args.config)
    elif args.mode == "audit-data":
        run_audit_data(args.config)
    elif args.mode == "sanity":
        run_sanity(args.config)
    else:
        run_live_sim(args.config)


if __name__ == "__main__":
    main()
