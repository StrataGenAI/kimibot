"""Model artifact persistence and inference."""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from models.calibration import BaseCalibrator, IdentityCalibrator
from project.types import FeatureRow


FEATURE_COLUMNS = [
    "p_market",
    "momentum_1m",
    "momentum_5m",
    "volatility",
    "volume_spike",
    "btc_return_1m",
    "btc_return_5m",
    "btc_volatility",
    "funding_rate",
    "time_to_resolution",
]


@dataclass
class LogisticRegressionPredictor:
    """Inference wrapper for a persisted scaler and logistic regression model."""

    model: object
    scaler: object
    calibrator: BaseCalibrator
    feature_columns: list[str]

    def predict_raw(self, feature_row: FeatureRow) -> float:
        """Predict the raw YES probability for a single feature row."""

        frame = pd.DataFrame(
            [{column: feature_row.values[column] for column in self.feature_columns}]
        )
        scaled = self.scaler.transform(frame.to_numpy(dtype=float))
        probability = self.model.predict_proba(scaled)[0, 1]

        # CLIP: prevent overconfident predictions
        # Use 0.20-0.80 to avoid extreme edges after clipping
        probability = float(np.clip(probability, 0.20, 0.80))

        return probability

    def predict(self, feature_row: FeatureRow) -> float:
        """Predict the calibrated YES probability for a single feature row."""

        raw_probability = self.predict_raw(feature_row)
        calibrated = self.calibrator.predict(np.array([raw_probability], dtype=float))[
            0
        ]

        # Additional clip after calibration (redundant but safe)
        calibrated = float(np.clip(calibrated, 0.20, 0.80))

        return calibrated

    def save(
        self,
        model_path: Path,
        scaler_path: Path,
        calibrator_path: Path,
        metadata_path: Path,
        metadata: dict[str, object],
    ) -> None:
        """Persist model artifacts and metadata to disk."""

        model_path.parent.mkdir(parents=True, exist_ok=True)
        with model_path.open("wb") as handle:
            pickle.dump(self.model, handle)
        with scaler_path.open("wb") as handle:
            pickle.dump(self.scaler, handle)
        with calibrator_path.open("wb") as handle:
            pickle.dump(self.calibrator, handle)
        payload = {"feature_columns": self.feature_columns, **metadata}
        with metadata_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)

    @classmethod
    def load(
        cls,
        model_path: Path,
        scaler_path: Path,
        calibrator_path: Path | None,
        metadata_path: Path,
    ) -> "LogisticRegressionPredictor":
        """Load model artifacts from disk."""

        with model_path.open("rb") as handle:
            model = pickle.load(handle)
        with scaler_path.open("rb") as handle:
            scaler = pickle.load(handle)
        calibrator: BaseCalibrator
        if calibrator_path is not None and calibrator_path.exists():
            with calibrator_path.open("rb") as handle:
                calibrator = pickle.load(handle)
        else:
            calibrator = IdentityCalibrator()
        with metadata_path.open("r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        return cls(
            model=model,
            scaler=scaler,
            calibrator=calibrator,
            feature_columns=list(metadata["feature_columns"]),
        )
