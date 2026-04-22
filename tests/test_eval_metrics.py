"""Tests for evaluation metrics module.

Section A: Known-input tests with hand-computed expected values.
  These run before the metrics module is used anywhere else and verify
  the implementation against exact arithmetic, not just direction.

Section B: Property tests (edge cases and directional assertions).
"""

from __future__ import annotations

import unittest
import numpy as np


# ─── Section A: Known-input tests with hand-computed expected values ──────────

class BrierScoreKnownInputTests(unittest.TestCase):
    """
    Hand-computed expected values for brier_score.
    Formula: mean((y_pred - y_true)^2)
    """

    def test_known_input_a(self) -> None:
        # y_true=[1,0,1,0], y_pred=[0.8,0.2,0.8,0.2]
        # errors: (0.8-1)^2=0.04, (0.2-0)^2=0.04, 0.04, 0.04
        # mean = 0.16 / 4 = 0.04
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.array([0.8, 0.2, 0.8, 0.2])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.04, places=12)

    def test_known_input_b(self) -> None:
        # y_true=[1,1,0,0], y_pred=[0.7,0.6,0.4,0.3]
        # errors: (0.7-1)^2=0.09, (0.6-1)^2=0.16, (0.4-0)^2=0.16, (0.3-0)^2=0.09
        # mean = 0.50 / 4 = 0.125
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 1.0, 0.0, 0.0])
        y_pred = np.array([0.7, 0.6, 0.4, 0.3])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.125, places=12)

    def test_known_input_single_sample(self) -> None:
        # Single sample: y_true=1, y_pred=0.3 → (0.3-1)^2 = 0.49
        from evaluation.metrics import brier_score
        y_true = np.array([1.0])
        y_pred = np.array([0.3])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.49, places=12)


class ECEKnownInputTests(unittest.TestCase):
    """
    Hand-computed expected values for expected_calibration_error.
    Formula: sum over bins of (bin_size/n) * |mean_pred - fraction_positive|
    """

    def test_known_input_single_bin_overconfident(self) -> None:
        # 10 predictions all at 0.8, but only 4 are positive.
        # With n_bins=10, all fall in bin [0.7, 0.8) (bin index 7) or [0.8, 0.9) depending on boundary.
        # The last bin [0.9, 1.0] is closed. Bin [0.8, 0.9): p=0.8 → bin 8 (index 8).
        # mean_pred ≈ 0.8, fraction_positive = 4/10 = 0.4
        # ECE = (10/10) * |0.8 - 0.4| = 0.4
        from evaluation.metrics import expected_calibration_error
        y_pred = np.full(10, 0.8)
        y_true = np.array([1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.4, places=6)

    def test_known_input_two_bins_symmetric(self) -> None:
        # 5 predictions at 0.2 (all negative, fraction_pos=0) and
        # 5 predictions at 0.8 (all positive, fraction_pos=1).
        # Bin [0.1,0.2): mean_pred=0.2, frac_pos=0.0 → |0.2-0.0|=0.2, weight=5/10=0.5
        # Bin [0.7,0.8): mean_pred=0.8, frac_pos=1.0 → |0.8-1.0|=0.2, weight=5/10=0.5
        # ECE = 0.5*0.2 + 0.5*0.2 = 0.2
        from evaluation.metrics import expected_calibration_error
        y_pred = np.array([0.2, 0.2, 0.2, 0.2, 0.2, 0.8, 0.8, 0.8, 0.8, 0.8])
        y_true = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.2, places=6)

    def test_known_input_perfect_calibration(self) -> None:
        # 5 predictions at 0.4, 2 positive → fraction_pos=0.4; ECE=0
        # 5 predictions at 0.6, 3 positive → fraction_pos=0.6; ECE=0
        from evaluation.metrics import expected_calibration_error
        y_pred = np.array([0.4, 0.4, 0.4, 0.4, 0.4, 0.6, 0.6, 0.6, 0.6, 0.6])
        y_true = np.array([1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.0, places=6)


class BootstrapCIKnownInputTests(unittest.TestCase):
    """
    Known-input tests for bootstrap_brier_ci.
    With a deterministic seed and known Brier score, verify the CI:
    1. Contains the point estimate.
    2. Has correct ordering (lo < hi).
    3. Is not degenerate (width > 0 for non-trivial data).
    4. Reproducible across calls with the same seed.
    """

    def _get_inputs(self):
        # Fixed, non-trivial inputs: 100 samples, true Brier = 0.04 (good model)
        # y_true: alternating 1,0; y_pred: 0.8 for y_true=1, 0.2 for y_true=0
        y_true = np.tile([1.0, 0.0], 50)
        y_pred = np.where(y_true == 1.0, 0.8, 0.2)
        return y_true, y_pred

    def test_ci_contains_point_estimate(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci, brier_score
        y_true, y_pred = self._get_inputs()
        point = brier_score(y_true, y_pred)
        self.assertAlmostEqual(point, 0.04, places=10)
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=1000, seed=42)
        self.assertLessEqual(lo, point + 0.005)
        self.assertGreaterEqual(hi, point - 0.005)

    def test_ci_ordering(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertLess(lo, hi)

    def test_ci_width_positive(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertGreater(hi - lo, 0.001)  # Must have non-degenerate width

    def test_ci_reproducible_with_same_seed(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo1, hi1 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        lo2, hi2 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertEqual(lo1, lo2)
        self.assertEqual(hi1, hi2)

    def test_ci_differs_with_different_seeds(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo1, hi1 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        lo2, hi2 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=99)
        # Different seeds should give slightly different CIs (not identical)
        self.assertFalse(lo1 == lo2 and hi1 == hi2)


# ─── Section B: Property tests (edge cases and directional) ──────────────────

class BrierScoreTests(unittest.TestCase):
    def test_perfect_predictions(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.array([1.0, 0.0, 1.0, 0.0])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.0, places=10)

    def test_worst_predictions(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0])
        y_pred = np.array([0.0, 1.0])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 1.0, places=10)

    def test_trivial_baseline(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.full(4, 0.5)
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.25, places=10)


class ECETests(unittest.TestCase):
    def test_perfect_calibration(self) -> None:
        from evaluation.metrics import expected_calibration_error
        rng = np.random.default_rng(42)
        n = 1000
        y_pred = rng.uniform(0, 1, n)
        y_true = rng.binomial(1, y_pred).astype(float)
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        # Perfectly calibrated predictions should have low ECE
        self.assertLess(ece, 0.10)

    def test_systematic_overconfidence(self) -> None:
        from evaluation.metrics import expected_calibration_error
        # Predictions all at 0.9, but only 50% are positive
        y_pred = np.full(100, 0.9)
        y_true = np.array([1.0] * 50 + [0.0] * 50)
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertGreater(ece, 0.3)


class AUCTests(unittest.TestCase):
    def test_perfect_auc(self) -> None:
        from evaluation.metrics import roc_auc
        y_true = np.array([0.0, 0.0, 1.0, 1.0])
        y_score = np.array([0.1, 0.2, 0.8, 0.9])
        self.assertAlmostEqual(roc_auc(y_true, y_score), 1.0, places=5)

    def test_random_auc(self) -> None:
        from evaluation.metrics import roc_auc
        y_true = np.array([0.0, 1.0, 0.0, 1.0])
        y_score = np.array([0.5, 0.5, 0.5, 0.5])
        auc = roc_auc(y_true, y_score)
        self.assertGreaterEqual(auc, 0.4)
        self.assertLessEqual(auc, 0.6)


class BootstrapCITests(unittest.TestCase):
    def test_ci_contains_mean(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci, brier_score
        rng = np.random.default_rng(0)
        y_true = rng.binomial(1, 0.6, 200).astype(float)
        y_pred = np.full(200, 0.6)
        true_brier = brier_score(y_true, y_pred)
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertLess(lo, true_brier + 0.01)
        self.assertGreater(hi, true_brier - 0.01)
        self.assertLess(lo, hi)
