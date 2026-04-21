"""Walk-forward training entrypoint."""

from __future__ import annotations

import argparse

from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataStore
from models.trainer import WalkForwardTrainer
from project.configuration import load_config
from utils.logging import configure_logging


def main() -> None:
    """Train the latest walk-forward fold model and persist artifacts."""

    parser = argparse.ArgumentParser(
        description="Train the walk-forward baseline model."
    )
    parser.add_argument(
        "--config", default="config/default.yaml", help="Path to YAML or JSON config."
    )
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.runtime.log_level)
    bundle = DataStore(config).load()
    feature_store = FeatureStore(
        config.data.feature_cache_path, config.runtime.feature_schema_version
    )
    trainer = WalkForwardTrainer(
        config,
        bundle,
        FeatureBuilder(config.runtime.feature_schema_version),
        feature_store,
    )
    trained_folds = trainer.train_strict_holdout()
    if not trained_folds:
        trained_folds = trainer.train_folds()
    if not trained_folds:
        raise RuntimeError("No valid folds were trainable.")

    latest = trained_folds[-1]
    latest.predictor.save(
        model_path=config.data.model_artifact_path,
        scaler_path=config.data.scaler_artifact_path,
        calibrator_path=config.data.calibrator_artifact_path,
        metadata_path=config.data.training_metadata_path,
        metadata={
            "fold_id": latest.fold.fold_id,
            "train_market_ids": latest.fold.train_market_ids,
            "calibration_market_ids": latest.fold.calibration_market_ids,
            "test_market_ids": latest.fold.test_market_ids,
            "model_train_end_time": latest.fold.model_train_end_time.isoformat(),
            "calibration_end_time": latest.fold.calibration_end_time.isoformat(),
            "test_start_time": latest.fold.test_start_time.isoformat(),
            "test_end_time": latest.fold.test_end_time.isoformat(),
        },
    )
    print(
        f"Saved model artifacts for fold {latest.fold.fold_id} to {config.data.model_artifact_path.parent}"
    )


if __name__ == "__main__":
    main()
