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

    _SANITY_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _SANITY_RES_TS = datetime(2026, 1, 2, tzinfo=timezone.utc)

    for case in _CASES:
        row = FeatureRow(
            market_id="sanity",
            timestamp=_SANITY_TS,
            resolution_time=_SANITY_RES_TS,
            label=None,
            values=case["features"],
            market_source_max_ts=_SANITY_TS,
            crypto_source_max_ts=_SANITY_TS,
            schema_version=config.runtime.feature_schema_version,
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


def run_evaluate_limitless(config_path: str) -> None:
    """Run the Limitless historical ingestion and walk-forward evaluation."""

    import os
    from ingestion.limitless_historical import run_historical_ingestion
    from ingestion.binance_historical import ensure_btc_data, build_crypto_history
    from evaluation.walk_forward_evaluator import run_evaluation
    from evaluation.report_generator import generate_report, generate_charts
    from datetime import datetime, timezone

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)

    graph_api_key = config.ingestion.graph_api_key or os.environ.get("GRAPH_API_KEY", "")
    if not graph_api_key:
        print(
            "ERROR: GRAPH_API_KEY is required.\n"
            "Get a free key at https://thegraph.com/studio/apikeys\n"
            "Set it as: export GRAPH_API_KEY=your_key_here",
            file=sys.stderr,
        )
        sys.exit(1)

    # Step 1: Ingest resolved markets
    print("Step 1/4: Ingesting resolved Limitless markets...")
    markets = run_historical_ingestion(
        graph_api_key=graph_api_key,
        min_trades=20,
    )
    print(f"  Loaded {len(markets)} resolved markets")

    if len(markets) < 10:
        print(f"ERROR: Only {len(markets)} markets found. Need at least 10.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Ensure BTC data
    print("Step 2/4: Ensuring BTC price data is cached...")
    all_timestamps = []
    for m in markets:
        for t in m["trades"]:
            all_timestamps.append(t["timestamp"])
    if all_timestamps:
        data_start = datetime.fromtimestamp(min(all_timestamps), tz=timezone.utc)
        data_end = datetime.fromtimestamp(max(all_timestamps), tz=timezone.utc)
        ensure_btc_data(data_start, data_end)
        print(f"  BTC data covers {data_start.date()} to {data_end.date()}")
        crypto_df = build_crypto_history(data_start, data_end)
        print(f"  Loaded {len(crypto_df):,} BTC 1-minute bars")
    else:
        print("ERROR: No trade timestamps found in market data.", file=sys.stderr)
        sys.exit(1)

    # Step 3: Run evaluation
    print("Step 3/4: Running walk-forward evaluation...")
    results = run_evaluation(markets=markets, crypto_df=crypto_df)

    delta = results["headline"]["delta_brier_vs_market"]
    beats = results["headline"]["model_beats_market"]
    print(f"\n{'='*50}")
    print(f"RESULT: Model Brier = {results['model']['brier_score']:.4f}")
    print(f"        Market Brier = {results['market_baseline']['brier_score']:.4f}")
    print(f"        Delta = {delta:+.4f} ({'model beats market' if beats else 'model loses to market'})")
    print(f"{'='*50}\n")

    # Step 4: Generate reports
    print("Step 4/4: Generating reports...")
    generate_report(results)
    generate_charts(results)

    print("\nDone. See EVALUATION_REPORT.md and reports/")
    print(f"Test markets: {results['dataset']['test_markets']}")
    print(f"Test snapshots: {results['dataset']['test_snapshots']}")


def main() -> None:
    """Parse CLI arguments and dispatch to the requested mode."""

    parser = argparse.ArgumentParser(
        description="Limitless prediction market trading system."
    )
    parser.add_argument(
        "mode",
        choices=["backtest", "live-sim", "validate", "ingest", "audit-data", "sanity", "evaluate-limitless"],
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
    elif args.mode == "evaluate-limitless":
        run_evaluate_limitless(args.config)
    else:
        run_live_sim(args.config)


if __name__ == "__main__":
    main()
