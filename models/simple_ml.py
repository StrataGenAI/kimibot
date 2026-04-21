"""Small deterministic ML primitives used by the baseline model."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class StandardScalerModel:
    """A minimal feature standardization model."""

    mean_: np.ndarray | None = None
    scale_: np.ndarray | None = None

    def fit(self, values: np.ndarray) -> "StandardScalerModel":
        """Fit feature means and standard deviations."""

        self.mean_ = values.mean(axis=0)
        scale = values.std(axis=0)
        scale[scale == 0.0] = 1.0
        self.scale_ = scale
        return self

    def transform(self, values: np.ndarray) -> np.ndarray:
        """Standardize features using fitted statistics."""

        if self.mean_ is None or self.scale_ is None:
            raise RuntimeError("Scaler must be fit before transform.")
        return (values - self.mean_) / self.scale_


@dataclass
class LogisticRegressionModel:
    """A deterministic binary logistic regression model trained with gradient descent."""

    learning_rate: float = 0.1
    max_iter: int = 4000
    l2_penalty: float = 1e-4
    weights_: np.ndarray | None = None
    bias_: float = 0.0

    def fit(self, features: np.ndarray, labels: np.ndarray) -> "LogisticRegressionModel":
        """Fit weights with class-balanced batch gradient descent."""

        sample_count, feature_count = features.shape
        self.weights_ = np.zeros(feature_count, dtype=float)
        self.bias_ = 0.0

        # Per-sample class weights inversely proportional to class frequency.
        pos = max(float((labels == 1).sum()), 1.0)
        neg = max(float((labels == 0).sum()), 1.0)
        w_pos = sample_count / (2.0 * pos)
        w_neg = sample_count / (2.0 * neg)
        sample_weights = np.where(labels == 1, w_pos, w_neg)

        for _ in range(self.max_iter):
            linear = features @ self.weights_ + self.bias_
            probs = self._sigmoid(linear)
            weighted_error = (probs - labels) * sample_weights
            grad_w = (features.T @ weighted_error) / sample_count + self.l2_penalty * self.weights_
            grad_b = float(weighted_error.mean())
            self.weights_ -= self.learning_rate * grad_w
            self.bias_ -= self.learning_rate * grad_b
        return self

    def predict_proba(self, features: np.ndarray) -> np.ndarray:
        """Return `[P(NO), P(YES)]` for each row."""

        if self.weights_ is None:
            raise RuntimeError("Model must be fit before prediction.")
        probs_yes = self._sigmoid(features @ self.weights_ + self.bias_)
        probs_no = 1.0 - probs_yes
        return np.column_stack([probs_no, probs_yes])

    @staticmethod
    def _sigmoid(values: np.ndarray) -> np.ndarray:
        """Compute the sigmoid function with numerical clipping."""

        clipped = np.clip(values, -35.0, 35.0)
        return 1.0 / (1.0 + np.exp(-clipped))
