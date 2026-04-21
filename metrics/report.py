"""Performance metrics and report serialization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _empty_calibration(prefix: str) -> dict[str, Any]:
    """Build an empty calibration payload for a metric prefix."""

    return {
        f"brier_{prefix}": 0.0,
        f"log_loss_{prefix}": 0.0,
        f"ece_{prefix}": 0.0,
        f"calibration_table_{prefix}": [],
    }


def _compute_calibration_for_column(
    frame: pd.DataFrame, bins: int, column: str, prefix: str
) -> dict[str, Any]:
    """Compute calibration metrics for a single probability column."""

    if frame.empty or "label" not in frame.columns or column not in frame.columns:
        return _empty_calibration(prefix)

    frame = frame.dropna(subset=["label", column]).copy()
    if frame.empty:
        return _empty_calibration(prefix)

    probs = frame[column].astype(float).clip(1e-6, 1.0 - 1e-6)
    labels = frame["label"].astype(float)
    brier = float(np.mean((probs - labels) ** 2))
    log_loss = float(
        -np.mean(labels * np.log(probs) + (1.0 - labels) * np.log(1.0 - probs))
    )

    edges = np.linspace(0.0, 1.0, bins + 1)
    frame["calibration_bin"] = pd.cut(
        probs, bins=edges, include_lowest=True, duplicates="drop"
    )
    calibration_table: list[dict[str, Any]] = []
    ece = 0.0
    total_count = len(frame)
    for interval, group in frame.groupby("calibration_bin", observed=False):
        if group.empty:
            continue
        predicted = float(group[column].mean())
        observed = float(group["label"].mean())
        count = int(len(group))
        ece += abs(predicted - observed) * (count / total_count)
        calibration_table.append(
            {
                "bin": str(interval),
                "count": count,
                "mean_predicted": predicted,
                "mean_observed": observed,
            }
        )
    return {
        f"brier_{prefix}": brier,
        f"log_loss_{prefix}": log_loss,
        f"ece_{prefix}": float(ece),
        f"calibration_table_{prefix}": calibration_table,
    }


def _compute_trade_diagnostics(trade_log: list[dict[str, object]]) -> dict[str, float]:
    """Compute trade concentration and distribution metrics from settlements."""

    settlements = pd.DataFrame(
        [trade for trade in trade_log if trade.get("event") == "settlement"]
    )
    if settlements.empty:
        return {
            "trade_count": 0.0,
            "median_trade_pnl": 0.0,
            "top_trade_pnl_share": 0.0,
            "top_3_trade_pnl_share": 0.0,
            "top_5_trade_pnl_share": 0.0,
            "profitable_trade_fraction": 0.0,
        }

    pnls = settlements["realized_pnl"].astype(float)
    positive_total = float(pnls[pnls > 0.0].sum())
    top_trade_share = 0.0
    top_three_share = 0.0
    if positive_total > 0.0:
        top_trade_share = float(pnls.max() / positive_total)
        top_three_share = float(
            pnls.sort_values(ascending=False).head(3).sum() / positive_total
        )
        top_five_share = float(
            pnls.sort_values(ascending=False).head(5).sum() / positive_total
        )
    else:
        top_five_share = 0.0
    return {
        "trade_count": float(len(settlements)),
        "median_trade_pnl": float(pnls.median()),
        "top_trade_pnl_share": top_trade_share,
        "top_3_trade_pnl_share": top_three_share,
        "top_5_trade_pnl_share": top_five_share,
        "profitable_trade_fraction": float((pnls > 0.0).mean()),
    }


def _compute_edge_bucket_report(
    trade_log: list[dict[str, object]],
) -> list[dict[str, Any]]:
    """Group settled positions by entry edge and summarize outcomes."""

    settlements = pd.DataFrame(
        [trade for trade in trade_log if trade.get("event") == "settlement"]
    )
    if settlements.empty or "edge_entry" not in settlements.columns:
        return []
    edges = settlements["edge_entry"].astype(float)
    if edges.nunique() < 2:
        return []
    try:
        settlements["edge_bucket"] = pd.qcut(
            edges, q=min(4, edges.nunique()), duplicates="drop"
        )
    except ValueError:
        return []
    report: list[dict[str, Any]] = []
    for bucket, group in settlements.groupby("edge_bucket", observed=False):
        if group.empty:
            continue
        report.append(
            {
                "bucket": str(bucket),
                "count": int(len(group)),
                "mean_edge": float(group["edge_entry"].astype(float).mean()),
                "mean_realized_pnl": float(group["realized_pnl"].astype(float).mean()),
                "win_rate": float((group["realized_pnl"].astype(float) > 0.0).mean()),
                "mean_holding_duration_seconds": float(
                    group["holding_duration_seconds"].astype(float).mean()
                ),
            }
        )
    return report


def _compute_period_metrics(
    prediction_frame: pd.DataFrame, equity_curve: list[dict[str, object]], prefix: str
) -> dict[str, Any]:
    """Compute early/late prediction-period metrics."""

    if prediction_frame.empty:
        return {
            f"{prefix}_prediction_count": 0,
            f"{prefix}_ece_calibrated": 0.0,
            f"{prefix}_brier_calibrated": 0.0,
        }
    calibration = _compute_calibration_for_column(
        prediction_frame, 5, "p_model_calibrated", f"{prefix}_calibrated"
    )
    return {
        f"{prefix}_prediction_count": int(len(prediction_frame)),
        f"{prefix}_ece_calibrated": calibration[f"ece_{prefix}_calibrated"],
        f"{prefix}_brier_calibrated": calibration[f"brier_{prefix}_calibrated"],
    }


def _compute_time_stability(prediction_log: list[dict[str, object]]) -> dict[str, Any]:
    """Split out-of-sample predictions into early and late periods."""

    frame = pd.DataFrame(prediction_log)
    if frame.empty:
        return {
            "early_prediction_count": 0,
            "late_prediction_count": 0,
            "early_ece_calibrated": 0.0,
            "late_ece_calibrated": 0.0,
            "early_brier_calibrated": 0.0,
            "late_brier_calibrated": 0.0,
        }
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    midpoint = max(len(frame) // 2, 1)
    early = frame.iloc[:midpoint]
    late = frame.iloc[midpoint:]
    results: dict[str, Any] = {}
    results.update(_compute_period_metrics(early, [], "early"))
    results.update(_compute_period_metrics(late, [], "late"))
    return results


def compute_metrics(
    equity_curve: list[dict[str, object]],
    trade_log: list[dict[str, object]],
    prediction_log: list[dict[str, object]],
    calibration_bins: int,
) -> dict[str, Any]:
    """Compute portfolio performance metrics from an equity curve and trade log."""

    equity_frame = pd.DataFrame(equity_curve)
    equity_frame = equity_frame.sort_values("timestamp").reset_index(drop=True)
    returns = (
        equity_frame["equity"].pct_change().fillna(0.0)
        if not equity_frame.empty
        else pd.Series(dtype=float)
    )
    sharpe = 0.0
    if not returns.empty and returns.std(ddof=0) > 0.0:
        sharpe = float((returns.mean() / returns.std(ddof=0)) * np.sqrt(len(returns)))

    running_max = (
        equity_frame["equity"].cummax()
        if not equity_frame.empty
        else pd.Series(dtype=float)
    )
    drawdown = (
        ((equity_frame["equity"] / running_max) - 1.0).min()
        if not equity_frame.empty
        else 0.0
    )
    winning_trades = [
        trade for trade in trade_log if float(trade.get("realized_pnl", 0.0)) > 0.0
    ]
    closed_trades = [trade for trade in trade_log if trade.get("event") == "settlement"]

    total_return = 0.0
    if not equity_frame.empty and float(equity_frame.iloc[0]["equity"]) != 0.0:
        total_return = float(
            equity_frame.iloc[-1]["equity"] / equity_frame.iloc[0]["equity"] - 1.0
        )

    metrics: dict[str, Any] = {
        "total_return": total_return,
        "sharpe_ratio": sharpe,
        "max_drawdown": float(drawdown) if not pd.isna(drawdown) else 0.0,
        "win_rate": float(len(winning_trades) / len(closed_trades))
        if closed_trades
        else 0.0,
    }
    metrics.update(_compute_trade_diagnostics(trade_log))
    prediction_frame = pd.DataFrame(prediction_log)
    metrics.update(
        _compute_calibration_for_column(
            prediction_frame, calibration_bins, "p_model_raw", "raw"
        )
    )
    metrics.update(
        _compute_calibration_for_column(
            prediction_frame, calibration_bins, "p_model_calibrated", "calibrated"
        )
    )
    metrics["brier_score"] = metrics["brier_calibrated"]
    metrics["log_loss"] = metrics["log_loss_calibrated"]
    metrics["expected_calibration_error"] = metrics["ece_calibrated"]
    metrics["calibration_table"] = metrics["calibration_table_calibrated"]
    metrics["edge_bucket_report"] = _compute_edge_bucket_report(trade_log)
    metrics.update(_compute_time_stability(prediction_log))

    # ========== VALIDATION WARNINGS ==========
    # Add warnings for unreliable metrics
    metrics["validation_warnings"] = []

    trade_count = len(closed_trades)

    # Warn if Sharpe > threshold with low trade count
    if trade_count < 50 and sharpe > 3.0:
        metrics["validation_warnings"].append(
            f"UNRELIABLE: Sharpe={sharpe:.2f} with only {trade_count} trades (min 50)"
        )

    # Warn if win rate suspiciously high
    win_rate = metrics.get("win_rate", 0.0)
    if win_rate > 0.80:
        metrics["validation_warnings"].append(
            f"SUSPICIOUS: win_rate={win_rate:.1%} > 80%"
        )

    # Warn if top trade concentration
    top_share = metrics.get("top_trade_pnl_share", 0.0)
    if top_share > 0.30:
        metrics["validation_warnings"].append(
            f"CONCENTRATION RISK: top_trade_pnl_share={top_share:.1%} > 30%"
        )

    # Flag if insufficient trades
    if trade_count < 30:
        metrics["validation_warnings"].append(
            f"INVALID: Only {trade_count} trades (minimum 30 required)"
        )
        metrics["results_valid"] = False
    else:
        metrics["results_valid"] = True

    return metrics


def write_reports(
    metrics: dict[str, Any],
    trade_log: list[dict[str, object]],
    prediction_log: list[dict[str, object]],
    metrics_path: Path,
    trade_log_path: Path,
    prediction_path: Path,
) -> None:
    """Write metrics and trade log artifacts to disk."""

    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, default=str)
    pd.DataFrame(trade_log).to_csv(trade_log_path, index=False)
    pd.DataFrame(prediction_log).to_csv(prediction_path, index=False)
