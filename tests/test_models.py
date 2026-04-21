"""Unit tests for ML models, calibration, and predictor."""

from __future__ import annotations

import pickle
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import numpy as np

from models.calibration import IdentityCalibrator, SigmoidCalibrator
from models.predictor import FEATURE_COLUMNS, LogisticRegressionPredictor
from models.simple_ml import LogisticRegressionModel, StandardScalerModel
from project.types import FeatureRow


def _make_feature_row(values: dict | None = None) -> FeatureRow:
    defaults = {col: 0.5 for col in FEATURE_COLUMNS}
    if values:
        defaults.update(values)
    return FeatureRow(
        market_id="m1",
        timestamp=datetime(2026, 1, 1),
        resolution_time=datetime(2026, 1, 2),
        label=1,
        values=defaults,
        market_source_max_ts=datetime(2026, 1, 1),
        crypto_source_max_ts=datetime(2026, 1, 1),
        schema_version="v2",
    )


class StandardScalerTests(unittest.TestCase):

    def test_transform_produces_zero_mean_unit_variance(self):
        data = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        scaler = StandardScalerModel().fit(data)
        scaled = scaler.transform(data)
        self.assertAlmostEqual(scaled[:, 0].mean(), 0.0, places=10)
        self.assertAlmostEqual(scaled[:, 1].mean(), 0.0, places=10)

    def test_constant_feature_does_not_divide_by_zero(self):
        data = np.array([[5.0, 1.0], [5.0, 2.0], [5.0, 3.0]])
        scaler = StandardScalerModel().fit(data)
        scaled = scaler.transform(data)
        self.assertTrue(np.all(np.isfinite(scaled)))

    def test_transform_before_fit_raises(self):
        scaler = StandardScalerModel()
        with self.assertRaises(RuntimeError):
            scaler.transform(np.array([[1.0, 2.0]]))


class LogisticRegressionModelTests(unittest.TestCase):

    def test_converges_on_linearly_separable_data(self):
        np.random.seed(42)
        n = 200
        X = np.random.randn(n, 2)
        y = (X[:, 0] > 0).astype(float)
        model = LogisticRegressionModel(max_iter=2000, learning_rate=0.1)
        model.fit(X, y)
        preds = model.predict_proba(X)[:, 1]
        pred_labels = (preds > 0.5).astype(float)
        accuracy = (pred_labels == y).mean()
        self.assertGreater(accuracy, 0.90)

    def test_predict_proba_sums_to_one(self):
        X = np.random.randn(10, 3)
        y = np.array([0, 1] * 5, dtype=float)
        model = LogisticRegressionModel(max_iter=100)
        model.fit(X, y)
        probs = model.predict_proba(X)
        np.testing.assert_allclose(probs.sum(axis=1), 1.0, atol=1e-10)

    def test_predict_before_fit_raises(self):
        model = LogisticRegressionModel()
        with self.assertRaises(RuntimeError):
            model.predict_proba(np.array([[1.0, 2.0]]))


class CalibrationTests(unittest.TestCase):

    def test_identity_calibrator_clips_extremes(self):
        cal = IdentityCalibrator()
        scores = np.array([0.0, 0.5, 1.0])
        result = cal.predict(scores)
        self.assertTrue(np.all(result > 0))
        self.assertTrue(np.all(result < 1))

    def test_sigmoid_calibrator_fit_and_predict_stay_in_bounds(self):
        scores = np.array([0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
        labels = np.array([0.0, 0.0, 1.0, 1.0, 1.0, 1.0])
        cal = SigmoidCalibrator().fit(scores, labels)
        result = cal.predict(scores)
        self.assertTrue(np.all(result >= 0.0))
        self.assertTrue(np.all(result <= 1.0))
        self.assertTrue(np.all(np.isfinite(result)))

    def test_sigmoid_calibrator_handles_single_class(self):
        scores = np.array([0.4, 0.5, 0.6])
        labels = np.array([1.0, 1.0, 1.0])
        cal = SigmoidCalibrator().fit(scores, labels)
        result = cal.predict(scores)
        self.assertTrue(np.all(np.isfinite(result)))

    def test_sigmoid_calibrator_handles_empty_data(self):
        cal = SigmoidCalibrator().fit(np.array([]), np.array([]))
        result = cal.predict(np.array([0.5]))
        self.assertTrue(np.all(np.isfinite(result)))


class PredictorTests(unittest.TestCase):

    def _make_predictor(self) -> LogisticRegressionPredictor:
        n = 100
        X = np.random.randn(n, len(FEATURE_COLUMNS))
        y = np.random.randint(0, 2, n).astype(float)
        scaler = StandardScalerModel().fit(X)
        scaled = scaler.transform(X)
        model = LogisticRegressionModel(max_iter=200)
        model.fit(scaled, y)
        calibrator = IdentityCalibrator()
        return LogisticRegressionPredictor(
            model=model, scaler=scaler, calibrator=calibrator, feature_columns=FEATURE_COLUMNS
        )

    def test_predict_raw_clips_to_bounds(self):
        predictor = self._make_predictor()
        row = _make_feature_row()
        raw = predictor.predict_raw(row)
        self.assertGreaterEqual(raw, 0.05)
        self.assertLessEqual(raw, 0.95)

    def test_predict_calibrated_clips_to_bounds(self):
        predictor = self._make_predictor()
        row = _make_feature_row()
        calibrated = predictor.predict(row)
        self.assertGreaterEqual(calibrated, 0.05)
        self.assertLessEqual(calibrated, 0.95)

    def test_save_and_load_round_trip(self):
        predictor = self._make_predictor()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            model_path = tmp_path / "model.pkl"
            scaler_path = tmp_path / "scaler.pkl"
            cal_path = tmp_path / "calibrator.pkl"
            meta_path = tmp_path / "metadata.json"
            predictor.save(
                model_path=model_path,
                scaler_path=scaler_path,
                calibrator_path=cal_path,
                metadata_path=meta_path,
                metadata={"fold_id": 1},
            )
            loaded = LogisticRegressionPredictor.load(model_path, scaler_path, cal_path, meta_path)
        row = _make_feature_row()
        original = predictor.predict(row)
        restored = loaded.predict(row)
        self.assertAlmostEqual(original, restored, places=6)

    def test_load_without_calibrator_uses_identity(self):
        predictor = self._make_predictor()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            model_path = tmp_path / "model.pkl"
            scaler_path = tmp_path / "scaler.pkl"
            meta_path = tmp_path / "metadata.json"
            predictor.save(
                model_path=model_path,
                scaler_path=scaler_path,
                calibrator_path=tmp_path / "calibrator.pkl",
                metadata_path=meta_path,
                metadata={"fold_id": 1},
            )
            loaded = LogisticRegressionPredictor.load(model_path, scaler_path, None, meta_path)
        self.assertIsInstance(loaded.calibrator, IdentityCalibrator)


class BalancedCalibrationTests(unittest.TestCase):
    """Verify that class-balanced training + calibration yields a reasonable Brier score."""

    def test_brier_score_on_balanced_synthetic(self) -> None:
        rng = np.random.default_rng(42)
        n = 400
        X = rng.standard_normal((n, 3))
        labels = (X[:, 0] + 0.5 * X[:, 1] > 0).astype(float)

        scaler = StandardScalerModel().fit(X[:300])
        X_scaled = scaler.transform(X)

        model = LogisticRegressionModel()
        model.fit(X_scaled[:300], labels[:300])

        raw_scores = model.predict_proba(X_scaled[300:])[:, 1]

        calibrator = SigmoidCalibrator()
        calibrator.fit(raw_scores[:50], labels[300:350])
        probs = calibrator.predict(raw_scores[50:])
        probs = np.clip(probs, 0.05, 0.95)

        brier = float(np.mean((probs - labels[350:]) ** 2))
        self.assertLess(brier, 0.25, f"Brier score {brier:.4f} too high — calibration is broken")


if __name__ == "__main__":
    unittest.main()
