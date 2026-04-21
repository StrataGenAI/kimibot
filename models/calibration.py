"""Probability calibration models."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


class BaseCalibrator:
    """Common interface for probability calibrators."""

    def fit(self, raw_scores: np.ndarray, labels: np.ndarray) -> "BaseCalibrator":
        """Fit the calibrator on past validation data."""

        raise NotImplementedError

    def predict(self, raw_scores: np.ndarray) -> np.ndarray:
        """Transform raw model scores into calibrated probabilities."""

        raise NotImplementedError


@dataclass
class IdentityCalibrator(BaseCalibrator):
    """Fallback calibrator that returns raw scores unchanged."""

    def fit(self, raw_scores: np.ndarray, labels: np.ndarray) -> "IdentityCalibrator":
        """No-op fit for interface compatibility."""

        return self

    def predict(self, raw_scores: np.ndarray) -> np.ndarray:
        """Return clipped raw probabilities."""

        return np.clip(raw_scores.astype(float), 1e-6, 1.0 - 1e-6)


@dataclass
class SigmoidCalibrator(BaseCalibrator):
    """Platt-style sigmoid calibration over raw probabilities."""

    learning_rate: float = 0.05
    max_iter: int = 3000
    weight_: float = 1.0
    bias_: float = 0.0

    def fit(self, raw_scores: np.ndarray, labels: np.ndarray) -> "SigmoidCalibrator":
        """Fit a one-dimensional logistic map on past validation scores only."""

        probs = np.clip(raw_scores.astype(float), 1e-6, 1.0 - 1e-6)
        labels = labels.astype(float)
        if probs.size == 0 or np.unique(labels).size < 2:
            self.weight_ = 1.0
            self.bias_ = 0.0
            return self

        logits = np.log(probs / (1.0 - probs))
        self.weight_ = 1.0
        self.bias_ = 0.0
        count = float(len(logits))
        for _ in range(self.max_iter):
            calibrated = self._sigmoid(self.weight_ * logits + self.bias_)
            error = calibrated - labels
            grad_w = float(np.dot(error, logits) / count)
            grad_b = float(error.mean())
            self.weight_ -= self.learning_rate * grad_w
            self.bias_ -= self.learning_rate * grad_b
        return self

    def predict(self, raw_scores: np.ndarray) -> np.ndarray:
        """Apply the fitted sigmoid map to raw model probabilities."""

        probs = np.clip(raw_scores.astype(float), 1e-6, 1.0 - 1e-6)
        logits = np.log(probs / (1.0 - probs))
        return np.clip(self._sigmoid(self.weight_ * logits + self.bias_), 1e-6, 1.0 - 1e-6)

    @staticmethod
    def _sigmoid(values: np.ndarray) -> np.ndarray:
        """Compute numerically stable sigmoid values."""

        clipped = np.clip(values, -35.0, 35.0)
        return 1.0 / (1.0 + np.exp(-clipped))
