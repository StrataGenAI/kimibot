"""Walk-forward training for the logistic regression baseline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd

from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataBundle
from models.calibration import IdentityCalibrator, SigmoidCalibrator
from models.predictor import FEATURE_COLUMNS, LogisticRegressionPredictor
from models.simple_ml import LogisticRegressionModel, StandardScalerModel
from project.configuration import AppConfig
from project.types import WalkForwardFold


@dataclass(frozen=True)
class TrainedFold:
    """A trained model and its active walk-forward test segment."""

    fold: WalkForwardFold
    predictor: LogisticRegressionPredictor


class WalkForwardTrainer:
    """Create walk-forward folds and train one model per fold."""

    def __init__(
        self,
        config: AppConfig,
        bundle: DataBundle,
        feature_builder: FeatureBuilder,
        feature_store: FeatureStore,
    ) -> None:
        """Bind the trainer to configuration, data, and feature components."""

        self.config = config
        self.bundle = bundle
        self.feature_builder = feature_builder
        self.feature_store = feature_store

    def build_folds(self) -> list[WalkForwardFold]:
        """Generate rolling walk-forward folds from resolved markets."""

        metadata = self.bundle.market_metadata.sort_values(
            "resolution_time"
        ).reset_index(drop=True)
        folds: list[WalkForwardFold] = []
        fold_id = 1
        min_train = self.config.walk_forward.min_resolved_markets
        test_size = self.config.walk_forward.test_markets_per_fold

        for start in range(min_train, len(metadata), test_size):
            available_meta = metadata.iloc[:start]
            test_meta = metadata.iloc[start : start + test_size]
            if test_meta.empty:
                break
            if len(available_meta) <= self.config.calibration.min_calibration_markets:
                continue
            calibration_meta = available_meta.iloc[
                -self.config.calibration.min_calibration_markets :
            ]
            train_meta = available_meta.iloc[
                : -self.config.calibration.min_calibration_markets
            ]
            if train_meta.empty:
                continue
            model_train_end_time = train_meta["resolution_time"].max()
            calibration_end_time = calibration_meta["resolution_time"].max()
            test_market_ids = test_meta["market_id"].tolist()
            test_snapshots = self.bundle.market_snapshots[
                self.bundle.market_snapshots["market_id"].isin(
                    test_meta["market_id"].tolist()
                )
            ]
            if test_snapshots.empty:
                continue
            folds.append(
                WalkForwardFold(
                    fold_id=fold_id,
                    train_market_ids=train_meta["market_id"].tolist(),
                    calibration_market_ids=calibration_meta["market_id"].tolist(),
                    test_market_ids=test_market_ids,
                    model_train_end_time=model_train_end_time,
                    calibration_end_time=calibration_end_time,
                    test_start_time=test_snapshots["timestamp"].min(),
                    test_end_time=test_snapshots["timestamp"].max(),
                )
            )
            fold_id += 1
        return folds

    def train_folds(self) -> list[TrainedFold]:
        """Train a predictor for each walk-forward fold."""

        return self.train_folds_with_labels()

    def train_folds_with_labels(
        self, label_map: Mapping[str, int] | None = None
    ) -> list[TrainedFold]:
        """Train walk-forward folds using an optional label override."""

        trained_folds: list[TrainedFold] = []
        for fold in self.build_folds():
            training_rows = self._build_rows(
                market_ids=fold.train_market_ids,
                label_end_time=fold.model_train_end_time,
                label_map=label_map,
            )
            calibration_rows = self._build_rows(
                market_ids=fold.calibration_market_ids,
                label_end_time=fold.calibration_end_time,
                label_map=label_map,
            )
            if len(training_rows) < self.config.walk_forward.min_training_rows:
                continue
            if len(calibration_rows) < self.config.calibration.min_calibration_rows:
                continue
            features_frame = FeatureBuilder.to_frame(training_rows)
            if features_frame["label"].nunique() < 2:
                continue
            train_matrix = features_frame[FEATURE_COLUMNS].to_numpy(dtype=float)
            labels = features_frame["label"].to_numpy(dtype=float)
            scaler = StandardScalerModel().fit(train_matrix)
            scaled = scaler.transform(train_matrix)
            model = LogisticRegressionModel()
            model.fit(scaled, labels)
            calibrator = self._fit_calibrator(model, scaler, calibration_rows)
            predictor = LogisticRegressionPredictor(
                model=model,
                scaler=scaler,
                calibrator=calibrator,
                feature_columns=FEATURE_COLUMNS,
            )
            trained_folds.append(TrainedFold(fold=fold, predictor=predictor))
        return trained_folds

    def train_strict_holdout(
        self, label_map: Mapping[str, int] | None = None
    ) -> list[TrainedFold]:
        """Train a single strict time-split holdout model with no rolling retrain."""

        metadata = self.bundle.market_metadata.sort_values(
            "resolution_time"
        ).reset_index(drop=True)
        holdout_size = min(
            self.config.validation.holdout_test_markets, max(len(metadata) - 1, 1)
        )
        available_meta = metadata.iloc[:-holdout_size]
        test_meta = metadata.iloc[-holdout_size:]
        if available_meta.empty or test_meta.empty:
            return []
        if len(available_meta) <= self.config.calibration.min_calibration_markets:
            return []

        calibration_meta = available_meta.iloc[
            -self.config.calibration.min_calibration_markets :
        ]
        train_meta = available_meta.iloc[
            : -self.config.calibration.min_calibration_markets
        ]
        if train_meta.empty:
            return []

        model_train_end_time = train_meta["resolution_time"].max()
        calibration_end_time = calibration_meta["resolution_time"].max()
        test_snapshots = self.bundle.market_snapshots[
            self.bundle.market_snapshots["market_id"].isin(
                test_meta["market_id"].tolist()
            )
            & (self.bundle.market_snapshots["timestamp"] > calibration_end_time)
        ]
        if test_snapshots.empty:
            return []

        fold = WalkForwardFold(
            fold_id=1,
            train_market_ids=train_meta["market_id"].tolist(),
            calibration_market_ids=calibration_meta["market_id"].tolist(),
            test_market_ids=test_meta["market_id"].tolist(),
            model_train_end_time=model_train_end_time,
            calibration_end_time=calibration_end_time,
            test_start_time=test_snapshots["timestamp"].min(),
            test_end_time=test_snapshots["timestamp"].max(),
        )
        training_rows = self._build_rows(
            market_ids=fold.train_market_ids,
            label_end_time=fold.model_train_end_time,
            label_map=label_map,
        )
        calibration_rows = self._build_rows(
            market_ids=fold.calibration_market_ids,
            label_end_time=fold.calibration_end_time,
            label_map=label_map,
        )
        if len(training_rows) < self.config.walk_forward.min_training_rows:
            return []
        if len(calibration_rows) < self.config.calibration.min_calibration_rows:
            return []
        features_frame = FeatureBuilder.to_frame(training_rows)
        if features_frame["label"].nunique() < 2:
            return []
        train_matrix = features_frame[FEATURE_COLUMNS].to_numpy(dtype=float)
        labels = features_frame["label"].to_numpy(dtype=float)
        scaler = StandardScalerModel().fit(train_matrix)
        scaled = scaler.transform(train_matrix)
        model = LogisticRegressionModel()
        model.fit(scaled, labels)
        calibrator = self._fit_calibrator(model, scaler, calibration_rows)
        predictor = LogisticRegressionPredictor(
            model=model,
            scaler=scaler,
            calibrator=calibrator,
            feature_columns=FEATURE_COLUMNS,
        )
        return [TrainedFold(fold=fold, predictor=predictor)]

    def _build_rows(
        self,
        *,
        market_ids: list[str],
        label_end_time,
        label_map: Mapping[str, int] | None = None,
    ) -> list:
        """Build labeled rows for a fixed set of markets using only past-resolved history."""

        metadata = self.bundle.market_metadata.set_index("market_id")
        rows = []
        market_frame = self.bundle.market_snapshots
        crypto_frame = self.bundle.crypto_snapshots

        for market_id in market_ids:
            meta = metadata.loc[market_id]
            market_history = market_frame[market_frame["market_id"] == market_id]
            eligible = market_history[market_history["timestamp"] <= label_end_time]
            for _, market_row in eligible.iterrows():
                as_of = market_row["timestamp"]
                rows.append(
                    self.feature_store.get_or_build(
                        market_id,
                        as_of,
                        lambda market_history=market_history, crypto_frame=crypto_frame, as_of=as_of, meta=meta, market_id=market_id: (
                            self.feature_builder.build_features(
                                market_history=market_history,
                                crypto_history=crypto_frame,
                                as_of=as_of,
                                resolution_time=meta["resolution_time"],
                                label=int(
                                    label_map.get(market_id, int(meta["outcome_yes"]))
                                )
                                if label_map is not None
                                else int(meta["outcome_yes"]),
                                market_id=market_id,
                            )
                        ),
                    )
                )
        return rows

    def _fit_calibrator(
        self, model, scaler, calibration_rows: list
    ) -> IdentityCalibrator | SigmoidCalibrator:
        """Fit a time-aware calibrator on the held-out calibration slice only."""

        calibration_frame = FeatureBuilder.to_frame(calibration_rows)
        if calibration_frame.empty or calibration_frame["label"].nunique() < 2:
            return IdentityCalibrator()
        calibration_matrix = calibration_frame[FEATURE_COLUMNS].to_numpy(dtype=float)
        calibration_scaled = scaler.transform(calibration_matrix)
        raw_scores = model.predict_proba(calibration_scaled)[:, 1]
        labels = calibration_frame["label"].to_numpy(dtype=float)
        if self.config.calibration.method == "sigmoid":
            return SigmoidCalibrator().fit(raw_scores, labels)
        return IdentityCalibrator().fit(raw_scores, labels)
