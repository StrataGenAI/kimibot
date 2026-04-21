"""Primary CLI for backtest and live simulation."""

from __future__ import annotations

import argparse
import asyncio
import time

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


def main() -> None:
    """Parse CLI arguments and dispatch to the requested mode."""

    parser = argparse.ArgumentParser(
        description="Limitless prediction market trading system."
    )
    parser.add_argument(
        "mode",
        choices=["backtest", "live-sim", "validate", "ingest", "audit-data"],
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
    else:
        run_live_sim(args.config)


if __name__ == "__main__":
    main()
