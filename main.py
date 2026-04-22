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
    """Run a simulated live loop over the configured data source."""

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)
    mode = getattr(config.runtime, "live_sim_mode", "walk_forward")
    if mode == "infer_only":
        _run_live_sim_infer_only(config)
        return

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


def _run_live_sim_infer_only(config) -> None:
    """Load pre-trained artifacts and score every live snapshot.

    This path exists because live Limitless markets have no resolved
    outcomes yet, so WalkForwardTrainer cannot build folds from them. We
    load the existing synthetic-fixture-trained model, run it against the
    live feature stream, and persist predictions + trade intents so the
    dashboard refreshes on real markets. No training happens here.
    """

    import logging

    import pandas as pd

    from decision.engine import DecisionEngine
    from models.predictor import LogisticRegressionPredictor
    from project.types import PortfolioState, Prediction

    logger = logging.getLogger("main")

    model_path = Path(config.data.model_artifact_path)
    scaler_path = Path(config.data.scaler_artifact_path)
    calibrator_path = Path(config.data.calibrator_artifact_path)
    meta_path = Path(config.data.training_metadata_path)
    missing = [p for p in (model_path, scaler_path) if not p.exists()]
    if missing:
        raise RuntimeError(
            "live_sim_mode=infer_only requires pre-trained model artifacts. "
            f"Missing: {', '.join(str(p) for p in missing)}. "
            "Run `python train.py` first (or drop real artifacts in place)."
        )
    predictor = LogisticRegressionPredictor.load(
        model_path, scaler_path, calibrator_path, meta_path
    )

    # Each timer tick should produce one prediction per market describing
    # its CURRENT state. Narrow lookbacks both for speed (parquet is 1 file
    # per poll; 60K+ crypto files exist after an hour of ingest) and for
    # semantics (features only look 15m back).
    store = DataStore(config)
    snapshots = store.load_market_snapshots(lookback_hours=1)
    crypto = store.load_crypto_snapshots(lookback_hours=1)
    metadata = store.load_market_metadata().set_index("market_id")

    feature_builder = FeatureBuilder(config.runtime.feature_schema_version)
    decision_engine = DecisionEngine(config.trading)
    portfolio_state = PortfolioState(cash=config.trading.initial_capital)

    predictions_rows: list[dict] = []
    trade_rows: list[dict] = []
    skipped_no_meta = 0
    skipped_insufficient = 0

    history_by_market: dict[str, pd.DataFrame] = {
        mid: group.sort_values("timestamp")
        for mid, group in snapshots.groupby("market_id", sort=False)
    }
    # Infer-only produces "current state" predictions — one row per market
    # per tick, scored at its latest snapshot. Replaying all historical
    # snapshots would write thousands of stale predictions on every run.
    latest_per_market = (
        snapshots.sort_values("timestamp")
        .groupby("market_id", sort=False)
        .tail(1)
    )

    for _, row in latest_per_market.iterrows():
        market_id = str(row["market_id"])
        if market_id not in metadata.index:
            skipped_no_meta += 1
            continue
        meta_row = metadata.loc[market_id]
        resolution_time = meta_row["resolution_time"]
        if pd.isna(resolution_time):
            skipped_no_meta += 1
            continue
        as_of = row["timestamp"]
        try:
            feature_row = feature_builder.build_features(
                market_history=history_by_market[market_id],
                crypto_history=crypto,
                as_of=as_of,
                resolution_time=resolution_time,
                label=None,
                market_id=market_id,
            )
        except ValueError:
            skipped_insufficient += 1
            continue

        p_raw = predictor.predict_raw(feature_row)
        p_cal = predictor.predict(feature_row)
        p_market = float(row["p_market"])
        predictions_rows.append(
            {
                "market_id": market_id,
                "timestamp": as_of.isoformat(),
                "p_model_raw": p_raw,
                "p_model_calibrated": p_cal,
                "p_market": p_market,
                "label": "",
            }
        )
        prediction = Prediction(
            market_id=market_id,
            timestamp=as_of,
            p_model_raw=p_raw,
            p_model_calibrated=p_cal,
            p_market=p_market,
        )
        intent = decision_engine.evaluate(
            prediction,
            float(row["liquidity"]),
            portfolio_state,
            as_of,
        )
        trade_rows.append(
            {
                "market_id": market_id,
                "timestamp": as_of.isoformat(),
                "action": intent.action,
                "side": intent.side or "",
                "requested_notional": intent.requested_notional,
                "expected_value": intent.expected_value,
                "edge": intent.edge,
                "reason": intent.reason,
            }
        )

    predictions_path = Path(config.data.prediction_report_path)
    trade_log_path = Path(config.data.trade_log_path)
    predictions_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(predictions_rows).to_csv(predictions_path, index=False)
    pd.DataFrame(trade_rows).to_csv(trade_log_path, index=False)

    action_counts = (
        pd.Series([r["action"] for r in trade_rows]).value_counts().to_dict()
        if trade_rows
        else {}
    )
    logger.info(
        "infer_only complete: snapshots=%d predictions=%d skipped_no_meta=%d "
        "skipped_insufficient=%d actions=%s",
        len(snapshots),
        len(predictions_rows),
        skipped_no_meta,
        skipped_insufficient,
        action_counts,
    )
    print(
        f"infer_only: predictions={len(predictions_rows)} "
        f"skipped_no_meta={skipped_no_meta} "
        f"skipped_insufficient_history={skipped_insufficient} "
        f"actions={action_counts}"
    )


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


def run_scan_resolutions(config_path: str, *, dry_run: bool = False) -> None:
    """Run one pass of the resolution scanner."""

    import json as _json
    from ingestion.resolution_scanner import scan_resolutions

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)

    report = scan_resolutions(config, dry_run=dry_run)
    payload = report.as_dict()
    payload["dry_run"] = bool(dry_run)
    print(_json.dumps(payload, indent=2, default=str))

    # Zero resolutions is a legitimate success (idle cycle).
    if report.errors and report.resolved == 0 and report.fetched == 0:
        # Total failure (e.g. network down) should exit non-zero.
        sys.exit(2)


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
        ingestion_config=config.ingestion,
    )
    print(f"  Loaded {len(markets)} resolved crypto markets")

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
        choices=["backtest", "live-sim", "validate", "ingest", "audit-data", "sanity", "evaluate-limitless", "scan-resolutions"],
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run for scan-resolutions — performs fetches but skips the parquet write.",
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
    elif args.mode == "scan-resolutions":
        run_scan_resolutions(args.config, dry_run=args.dry_run)
    else:
        run_live_sim(args.config)


if __name__ == "__main__":
    main()
