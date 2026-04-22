"""Walk-forward evaluation pipeline for Limitless historical markets."""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from evaluation.metrics import (
    accuracy_at_threshold,
    bootstrap_brier_ci,
    brier_score,
    expected_calibration_error,
    log_loss,
    roc_auc,
)
from features.builder import FeatureBuilder
from ingestion.binance_historical import build_crypto_history
from models.calibration import SigmoidCalibrator, IdentityCalibrator
from models.predictor import FEATURE_COLUMNS, LogisticRegressionPredictor
from models.simple_ml import LogisticRegressionModel, StandardScalerModel
from project.types import FeatureRow

LOGGER = logging.getLogger(__name__)

ARTIFACT_DIR = Path("models/walk_forward_runs")
RESULTS_PATH = Path("data/walk_forward_results.json")
SCHEMA_VERSION = "v2"
SNAPSHOT_INTERVAL_MINUTES = 15
MAX_SNAPSHOTS_PER_MARKET = 50
RANDOM_SEED = 42


def split_markets(
    markets: list[dict[str, Any]],
    train_frac: float = 0.60,
    calib_frac: float = 0.20,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Sort by resolution_time and split into train/calibrate/test sets."""
    sorted_markets = sorted(markets, key=lambda m: m["resolution_time_utc"])
    n = len(sorted_markets)
    train_end = int(n * train_frac)
    calib_end = int(n * (train_frac + calib_frac))
    return (
        sorted_markets[:train_end],
        sorted_markets[train_end:calib_end],
        sorted_markets[calib_end:],
    )


def _market_to_dataframes(market: dict[str, Any]) -> tuple[pd.DataFrame, datetime, datetime]:
    """
    Convert a cached market dict to a market_history DataFrame.

    Returns (market_history_df, market_open_ts, resolution_ts).
    market_history_df has columns: timestamp, p_market, volume, market_id
    """
    trades = market["trades"]
    if not trades:
        raise ValueError(f"Market {market['condition_id']} has no trades")

    rows = []
    for t in trades:
        ts = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc)
        rows.append({
            "timestamp": ts,
            "p_market": float(t["price"]),
            "volume": float(t["amount_usd"]),
            "market_id": market["condition_id"],
        })
    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    # Forward-fill to 1-minute bars so FeatureBuilder has a consistent time series
    open_ts = df["timestamp"].min().replace(second=0, microsecond=0)
    resolution_ts = datetime.fromisoformat(market["resolution_time_utc"])

    # Create 1-minute grid and forward-fill prices
    minutes = pd.date_range(start=open_ts, end=resolution_ts, freq="1min", tz=timezone.utc)
    grid = pd.DataFrame({"timestamp": minutes})
    merged = pd.merge_asof(grid, df.sort_values("timestamp"), on="timestamp", direction="backward")
    merged["market_id"] = market["condition_id"]
    merged = merged.dropna(subset=["p_market"]).reset_index(drop=True)

    return merged, open_ts, resolution_ts


def _build_snapshots(
    market: dict[str, Any],
    crypto_df: pd.DataFrame,
    builder: FeatureBuilder,
) -> list[FeatureRow]:
    """Build feature rows for all valid snapshot times in a market."""
    try:
        market_df, open_ts, resolution_ts = _market_to_dataframes(market)
    except ValueError as exc:
        LOGGER.warning("Skipping market %s: %s", market["condition_id"], exc)
        return []

    if market_df.empty:
        return []

    # Generate snapshot times: every 15 minutes from open to (resolution - 15 min)
    snapshot_end = resolution_ts - timedelta(minutes=15)
    if snapshot_end <= open_ts:
        return []

    snapshot_times = []
    t = open_ts + timedelta(minutes=SNAPSHOT_INTERVAL_MINUTES)
    while t <= snapshot_end:
        snapshot_times.append(t)
        t += timedelta(minutes=SNAPSHOT_INTERVAL_MINUTES)

    # Cap at MAX_SNAPSHOTS_PER_MARKET (take evenly spaced subset)
    if len(snapshot_times) > MAX_SNAPSHOTS_PER_MARKET:
        indices = np.linspace(0, len(snapshot_times) - 1, MAX_SNAPSHOTS_PER_MARKET, dtype=int)
        snapshot_times = [snapshot_times[i] for i in indices]

    rows: list[FeatureRow] = []
    label = market["label"]

    for as_of in snapshot_times:
        crypto_slice = crypto_df[
            (crypto_df["timestamp"] >= as_of - timedelta(hours=1)) &
            (crypto_df["timestamp"] <= as_of)
        ]
        if crypto_slice.empty:
            LOGGER.debug("No crypto data at %s for market %s, skipping snapshot", as_of, market["condition_id"])
            continue
        try:
            row = builder.build_features(
                market_history=market_df,
                crypto_history=crypto_df,
                as_of=as_of,
                resolution_time=resolution_ts,
                label=label,
                market_id=market["condition_id"],
            )
            rows.append(row)
        except ValueError as exc:
            LOGGER.debug("Snapshot failed for %s at %s: %s", market["condition_id"], as_of, exc)

    return rows


def _rows_to_arrays(rows: list[FeatureRow]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Extract feature matrix, labels, and market prices from FeatureRows."""
    X = np.array([[row.values[c] for c in FEATURE_COLUMNS] for row in rows], dtype=float)
    y = np.array([row.label for row in rows], dtype=float)
    p_market = np.array([row.values["p_market"] for row in rows], dtype=float)
    return X, y, p_market


def sorted_resolution(markets: list[dict[str, Any]]) -> list[str]:
    return sorted(m["resolution_time_utc"] for m in markets)


def run_evaluation(
    markets: list[dict[str, Any]],
    crypto_df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Run the full walk-forward evaluation pipeline.

    Returns a results dict suitable for JSON serialization and EVALUATION_REPORT.md.
    """
    if len(markets) < 10:
        raise ValueError(f"Need at least 10 markets, got {len(markets)}")

    train_markets, calib_markets, test_markets = split_markets(markets)
    LOGGER.info(
        "Walk-forward split: %d train, %d calib, %d test markets",
        len(train_markets), len(calib_markets), len(test_markets),
    )

    builder = FeatureBuilder(schema_version=SCHEMA_VERSION)

    # Build feature rows for each split
    LOGGER.info("Building train feature rows...")
    train_rows: list[FeatureRow] = []
    for m in train_markets:
        train_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info("Building calibration feature rows...")
    calib_rows: list[FeatureRow] = []
    for m in calib_markets:
        calib_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info("Building test feature rows...")
    test_rows: list[FeatureRow] = []
    for m in test_markets:
        test_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info(
        "Snapshot counts: train=%d, calib=%d, test=%d",
        len(train_rows), len(calib_rows), len(test_rows),
    )

    if len(train_rows) < 20:
        raise ValueError(f"Insufficient train rows: {len(train_rows)}")
    if len(test_rows) < 10:
        raise ValueError(f"Insufficient test rows: {len(test_rows)}")

    # Train
    X_train, y_train, _ = _rows_to_arrays(train_rows)
    scaler = StandardScalerModel().fit(X_train)
    X_train_scaled = scaler.transform(X_train)
    model = LogisticRegressionModel()
    model.fit(X_train_scaled, y_train)

    # Calibrate
    X_calib, y_calib, _ = _rows_to_arrays(calib_rows)
    X_calib_scaled = scaler.transform(X_calib)
    raw_calib = model.predict_proba(X_calib_scaled)[:, 1]
    if len(np.unique(y_calib)) >= 2:
        calibrator = SigmoidCalibrator().fit(raw_calib, y_calib)
    else:
        LOGGER.warning("Calibration set has only one class, using identity calibrator")
        calibrator = IdentityCalibrator()

    predictor = LogisticRegressionPredictor(
        model=model,
        scaler=scaler,
        calibrator=calibrator,
        feature_columns=FEATURE_COLUMNS,
    )

    # Predict on test
    X_test, y_test, p_market_test = _rows_to_arrays(test_rows)
    X_test_scaled = scaler.transform(X_test)
    raw_test = model.predict_proba(X_test_scaled)[:, 1]
    cal_test = np.array([predictor.predict(row) for row in test_rows])

    # Baselines
    trivial = np.full(len(y_test), 0.5)

    # Metrics
    model_brier = brier_score(y_test, cal_test)
    market_brier = brier_score(y_test, p_market_test)
    trivial_brier = brier_score(y_test, trivial)

    model_ece = expected_calibration_error(y_test, cal_test)
    model_log_loss = log_loss(y_test, cal_test)
    model_auc = roc_auc(y_test, cal_test)
    model_acc = accuracy_at_threshold(y_test, cal_test)

    market_ece = expected_calibration_error(y_test, p_market_test)
    market_log_loss = log_loss(y_test, p_market_test)
    market_auc = roc_auc(y_test, p_market_test)

    brier_ci_lo, brier_ci_hi = bootstrap_brier_ci(y_test, cal_test, n_resamples=1000, seed=RANDOM_SEED)
    market_ci_lo, market_ci_hi = bootstrap_brier_ci(y_test, p_market_test, n_resamples=1000, seed=RANDOM_SEED)

    delta_vs_market = market_brier - model_brier  # positive = model better

    # Brier by predicted probability decile
    decile_briers = []
    decile_edges = np.percentile(cal_test, np.linspace(0, 100, 11))
    for i in range(10):
        lo_edge = decile_edges[i]
        hi_edge = decile_edges[i + 1]
        mask = (cal_test >= lo_edge) & (cal_test <= hi_edge)
        if mask.sum() == 0:
            continue
        decile_briers.append({
            "decile": i + 1,
            "p_low": round(float(lo_edge), 4),
            "p_high": round(float(hi_edge), 4),
            "count": int(mask.sum()),
            "brier": round(brier_score(y_test[mask], cal_test[mask]), 6),
        })

    # Top 10 markets where model most disagreed with market price
    test_market_ids = [r.market_id for r in test_rows]
    disagreement = np.abs(cal_test - p_market_test)
    top_disagree_idx = np.argsort(disagreement)[::-1][:50]
    seen_mids: set[str] = set()
    top_disagreements = []
    for idx in top_disagree_idx:
        mid = test_market_ids[idx]
        if mid in seen_mids:
            continue
        seen_mids.add(mid)
        top_disagreements.append({
            "market_id": mid,
            "p_model": round(float(cal_test[idx]), 4),
            "p_market": round(float(p_market_test[idx]), 4),
            "disagreement": round(float(disagreement[idx]), 4),
            "label": int(y_test[idx]),
            "model_correct": int(y_test[idx]) == int(cal_test[idx] >= 0.5),
            "market_correct": int(y_test[idx]) == int(p_market_test[idx] >= 0.5),
        })
        if len(top_disagreements) >= 10:
            break

    # Top 10 markets where model was most confidently wrong
    model_error = (cal_test - y_test) ** 2
    confidence = np.abs(cal_test - 0.5) * 2
    confident_wrong = model_error * confidence
    top_wrong_idx = np.argsort(confident_wrong)[::-1][:50]
    seen_wrong: set[str] = set()
    top_confident_wrong = []
    for idx in top_wrong_idx:
        mid = test_market_ids[idx]
        if mid in seen_wrong:
            continue
        seen_wrong.add(mid)
        top_confident_wrong.append({
            "market_id": mid,
            "p_model": round(float(cal_test[idx]), 4),
            "p_market": round(float(p_market_test[idx]), 4),
            "label": int(y_test[idx]),
            "error": round(float(model_error[idx]), 4),
            "confidence": round(float(confidence[idx]), 4),
        })
        if len(top_confident_wrong) >= 10:
            break

    # Save model artifacts
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_base = ARTIFACT_DIR / run_ts
    artifact_base.mkdir(parents=True, exist_ok=True)
    with (artifact_base / "model.pkl").open("wb") as f:
        pickle.dump(model, f)
    with (artifact_base / "scaler.pkl").open("wb") as f:
        pickle.dump(scaler, f)
    with (artifact_base / "calibrator.pkl").open("wb") as f:
        pickle.dump(calibrator, f)

    # Reliability diagram data
    reliability_data = []
    bins = np.linspace(0, 1, 11)
    for i in range(10):
        lo_b, hi_b = bins[i], bins[i + 1]
        mask = (cal_test >= lo_b) & (cal_test < hi_b)
        if i == 9:
            mask = (cal_test >= lo_b) & (cal_test <= hi_b)
        if not mask.any():
            continue
        reliability_data.append({
            "bin_center": round((lo_b + hi_b) / 2, 2),
            "mean_pred": round(float(cal_test[mask].mean()), 4),
            "fraction_positive": round(float(y_test[mask].mean()), 4),
            "count": int(mask.sum()),
        })

    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_ts,
        "dataset": {
            "total_markets": len(markets),
            "train_markets": len(train_markets),
            "calib_markets": len(calib_markets),
            "test_markets": len(test_markets),
            "train_snapshots": len(train_rows),
            "calib_snapshots": len(calib_rows),
            "test_snapshots": len(test_rows),
            "train_date_range": [
                sorted_resolution(train_markets)[0],
                sorted_resolution(train_markets)[-1],
            ],
            "test_date_range": [
                sorted_resolution(test_markets)[0],
                sorted_resolution(test_markets)[-1],
            ],
        },
        "model": {
            "brier_score": round(model_brier, 6),
            "brier_ci_95": [round(brier_ci_lo, 6), round(brier_ci_hi, 6)],
            "ece": round(model_ece, 6),
            "log_loss": round(model_log_loss, 6),
            "auc": round(model_auc, 6),
            "accuracy_at_0_5": round(model_acc, 6),
            "brier_by_decile": decile_briers,
        },
        "market_baseline": {
            "brier_score": round(market_brier, 6),
            "brier_ci_95": [round(market_ci_lo, 6), round(market_ci_hi, 6)],
            "ece": round(market_ece, 6),
            "log_loss": round(market_log_loss, 6),
            "auc": round(market_auc, 6),
        },
        "trivial_baseline": {
            "brier_score": round(trivial_brier, 6),
        },
        "headline": {
            "delta_brier_vs_market": round(delta_vs_market, 6),
            "model_beats_market": delta_vs_market > 0,
            "model_beats_trivial": model_brier < trivial_brier,
        },
        "diagnostics": {
            "top_10_disagreements": top_disagreements,
            "top_10_confident_wrong": top_confident_wrong,
            "reliability_diagram_data": reliability_data,
        },
    }

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(results, indent=2))
    LOGGER.info("Results written to %s", RESULTS_PATH)

    return results
