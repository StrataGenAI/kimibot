"""Inference entrypoint for a single market and timestamp."""

from __future__ import annotations

import argparse

from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataStore
from models.predictor import LogisticRegressionPredictor
from project.configuration import load_config
from utils.logging import configure_logging
from utils.time import parse_utc_timestamp


def main() -> None:
    """Load the latest model artifact and score a single market snapshot."""

    parser = argparse.ArgumentParser(description="Run inference for one market and timestamp.")
    parser.add_argument("--config", default="config/default.yaml", help="Path to YAML or JSON config.")
    parser.add_argument("--market-id", required=True, help="Market identifier.")
    parser.add_argument("--timestamp", required=True, help="UTC timestamp to score.")
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.runtime.log_level)
    bundle = DataStore(config).load()
    metadata = bundle.market_metadata.set_index("market_id").loc[args.market_id]
    market_history = bundle.market_snapshots[bundle.market_snapshots["market_id"] == args.market_id]
    as_of = parse_utc_timestamp(args.timestamp)
    feature_store = FeatureStore(config.data.feature_cache_path, config.runtime.feature_schema_version)
    feature_builder = FeatureBuilder(config.runtime.feature_schema_version)
    feature_row = feature_store.get_or_build(
        args.market_id,
        as_of,
        lambda: feature_builder.build_features(
            market_history=market_history,
            crypto_history=bundle.crypto_snapshots,
            as_of=as_of,
            resolution_time=metadata["resolution_time"],
            label=None,
            market_id=args.market_id,
        ),
    )
    predictor = LogisticRegressionPredictor.load(
        model_path=config.data.model_artifact_path,
        scaler_path=config.data.scaler_artifact_path,
        calibrator_path=config.data.calibrator_artifact_path,
        metadata_path=config.data.training_metadata_path,
    )
    raw_probability = predictor.predict_raw(feature_row)
    calibrated_probability = predictor.predict(feature_row)
    print(
        "market_id="
        f"{args.market_id} timestamp={as_of.isoformat()} "
        f"p_model_raw={raw_probability:.6f} "
        f"p_model_calibrated={calibrated_probability:.6f}"
    )


if __name__ == "__main__":
    main()
