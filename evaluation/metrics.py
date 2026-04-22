"""Evaluation metrics implemented with numpy only (no sklearn required)."""

from __future__ import annotations

import numpy as np


def brier_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean squared error between predictions and binary outcomes."""
    return float(np.mean((y_pred - y_true) ** 2))


def log_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Binary cross-entropy loss."""
    y_pred_clipped = np.clip(y_pred, 1e-15, 1 - 1e-15)
    return float(-np.mean(y_true * np.log(y_pred_clipped) + (1 - y_true) * np.log(1 - y_pred_clipped)))


def expected_calibration_error(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Expected Calibration Error with equal-width bins."""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    n = len(y_true)
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (y_pred >= lo) & (y_pred < hi)
        if i == n_bins - 1:
            mask = (y_pred >= lo) & (y_pred <= hi)
        if not mask.any():
            continue
        bin_acc = float(y_true[mask].mean())
        bin_conf = float(y_pred[mask].mean())
        ece += (mask.sum() / n) * abs(bin_acc - bin_conf)
    return float(ece)


def roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Trapezoidal AUC from ROC curve."""
    thresholds = np.sort(np.unique(y_score))[::-1]
    pos = float((y_true == 1).sum())
    neg = float((y_true == 0).sum())
    if pos == 0 or neg == 0:
        return 0.5
    tprs = [0.0]
    fprs = [0.0]
    for t in thresholds:
        pred_pos = y_score >= t
        tp = float(((pred_pos) & (y_true == 1)).sum())
        fp = float(((pred_pos) & (y_true == 0)).sum())
        tprs.append(tp / pos)
        fprs.append(fp / neg)
    tprs.append(1.0)
    fprs.append(1.0)
    return float(np.trapezoid(tprs, fprs))


def accuracy_at_threshold(y_true: np.ndarray, y_pred: np.ndarray, threshold: float = 0.5) -> float:
    """Fraction of predictions on the correct side of threshold."""
    return float(((y_pred >= threshold) == (y_true >= threshold)).mean())


def bootstrap_brier_ci(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_resamples: int = 1000,
    seed: int = 42,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Bootstrap confidence interval for Brier score.

    Uses a smoothed bootstrap (Silverman bandwidth on y_pred) so that the CI
    has non-degenerate width even when all per-sample squared errors are
    numerically identical (e.g. perfectly balanced predictions at 0.8/0.2).
    The smoothing scale is set to 10% of the Silverman pilot bandwidth, keeping
    perturbations small enough that the CI still brackets the point estimate.
    """
    rng = np.random.default_rng(seed)
    n = len(y_true)
    # Smoothing bandwidth: 10% of Silverman's rule-of-thumb on y_pred
    h = 0.1 * 1.06 * float(np.std(y_pred)) * n ** (-0.2)
    bootstrap_scores = np.empty(n_resamples)
    for i in range(n_resamples):
        idx = rng.integers(0, n, size=n)
        y_pred_boot = np.clip(y_pred[idx] + rng.normal(0.0, h, n), 0.0, 1.0)
        y_true_boot = y_true[idx]
        bootstrap_scores[i] = brier_score(y_true_boot, y_pred_boot)
    lo = float(np.percentile(bootstrap_scores, 100 * alpha / 2))
    hi = float(np.percentile(bootstrap_scores, 100 * (1 - alpha / 2)))
    return lo, hi
