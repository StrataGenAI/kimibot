"""Generate EVALUATION_REPORT.md, reliability.png, and prob_histogram.png."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np

LOGGER = logging.getLogger(__name__)
REPORTS_DIR = Path("reports")


def _try_import_matplotlib():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        return plt
    except ImportError:
        LOGGER.warning("matplotlib not available; charts will be skipped")
        return None


def generate_charts(results: dict[str, Any]) -> None:
    """Generate reliability diagram and probability histogram PNGs."""
    plt = _try_import_matplotlib()
    if plt is None:
        return

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rel_data = results["diagnostics"]["reliability_diagram_data"]

    if rel_data:
        mean_preds = [d["mean_pred"] for d in rel_data]
        frac_pos = [d["fraction_positive"] for d in rel_data]
        counts = [d["count"] for d in rel_data]

        fig, ax = plt.subplots(figsize=(6, 6))
        ax.plot([0, 1], [0, 1], "k--", lw=1.5, label="Perfect calibration")
        scatter = ax.scatter(
            mean_preds, frac_pos,
            s=[max(c * 2, 20) for c in counts],
            alpha=0.75, color="#2563eb", zorder=5, label="Model",
        )
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xlabel("Mean Predicted Probability", fontsize=12)
        ax.set_ylabel("Fraction of Positives", fontsize=12)
        ax.set_title("Reliability Diagram (Walk-Forward Test Set)", fontsize=13)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        rel_path = REPORTS_DIR / "reliability.png"
        fig.savefig(rel_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        LOGGER.info("Reliability diagram saved to %s", rel_path)

    LOGGER.info("Charts generation complete")


def generate_report(results: dict[str, Any], output_path: Path = Path("EVALUATION_REPORT.md")) -> None:
    """Write EVALUATION_REPORT.md from evaluation results dict."""

    d = results["dataset"]
    m = results["model"]
    mb = results["market_baseline"]
    tb = results["trivial_baseline"]
    h = results["headline"]
    diag = results["diagnostics"]

    beats_market = h["model_beats_market"]
    beats_trivial = h["model_beats_trivial"]
    delta = h["delta_brier_vs_market"]
    brier_lo, brier_hi = m["brier_ci_95"]
    market_lo, market_hi = mb["brier_ci_95"]

    summary_verdict = (
        "**The model BEATS the market baseline.**"
        if beats_market
        else "**The model DOES NOT beat the market baseline.**"
    )
    trivial_verdict = (
        "The model beats the trivial (0.5) baseline."
        if beats_trivial
        else "The model does NOT beat the trivial baseline — something is fundamentally broken."
    )

    ci_overlap = brier_lo <= mb["brier_score"] <= brier_hi or market_lo <= m["brier_score"] <= market_hi
    significance_note = (
        "The confidence intervals overlap — the difference may not be statistically meaningful."
        if ci_overlap
        else "The confidence intervals do NOT overlap — the difference is statistically robust."
    )

    lines = [
        "# Limitless Walk-Forward Evaluation Report",
        "",
        f"Generated: {results['generated_at']}  ",
        f"Run ID: {results['run_id']}",
        "",
        "---",
        "",
        "## 1. Summary",
        "",
        summary_verdict,
        "",
        f"- Model Brier: **{m['brier_score']:.4f}** (95% CI: [{brier_lo:.4f}, {brier_hi:.4f}])",
        f"- Market Baseline Brier: **{mb['brier_score']:.4f}** (95% CI: [{market_lo:.4f}, {market_hi:.4f}])",
        f"- Delta (market - model): **{delta:+.4f}** (positive = model better)",
        f"- Trivial Baseline Brier: {tb['brier_score']:.4f}",
        "",
        trivial_verdict,
        "",
        significance_note,
        "",
        "---",
        "",
        "## 2. Dataset",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Total resolved markets ingested | {d['total_markets']} |",
        f"| Train markets (60%) | {d['train_markets']} |",
        f"| Calibration markets (20%) | {d['calib_markets']} |",
        f"| Test markets (20%) | {d['test_markets']} |",
        f"| Train snapshots | {d['train_snapshots']} |",
        f"| Calibration snapshots | {d['calib_snapshots']} |",
        f"| Test snapshots | {d['test_snapshots']} |",
        f"| Train date range | {d['train_date_range'][0][:10]} to {d['train_date_range'][1][:10]} |",
        f"| Test date range | {d['test_date_range'][0][:10]} to {d['test_date_range'][1][:10]} |",
        "",
        "---",
        "",
        "## 3. Full Metrics",
        "",
        "| Metric | Model | Market Baseline | Trivial (0.5) |",
        "|--------|-------|----------------|---------------|",
        f"| Brier Score | {m['brier_score']:.6f} | {mb['brier_score']:.6f} | {tb['brier_score']:.6f} |",
        f"| Brier 95% CI | [{brier_lo:.4f}, {brier_hi:.4f}] | [{market_lo:.4f}, {market_hi:.4f}] | - |",
        f"| ECE | {m['ece']:.6f} | {mb['ece']:.6f} | - |",
        f"| Log Loss | {m['log_loss']:.6f} | {mb['log_loss']:.6f} | - |",
        f"| AUC | {m['auc']:.6f} | {mb['auc']:.6f} | - |",
        f"| Accuracy @0.5 | {m['accuracy_at_0_5']:.6f} | - | - |",
        "",
        "---",
        "",
        "## 4. Brier Score by Predicted Probability Decile",
        "",
        "| Decile | P Range | Count | Brier |",
        "|--------|---------|-------|-------|",
    ]
    for row in m.get("brier_by_decile", []):
        lines.append(f"| {row['decile']} | [{row['p_low']:.2f}, {row['p_high']:.2f}] | {row['count']} | {row['brier']:.6f} |")

    lines += [
        "",
        "---",
        "",
        "## 5. Top 10 Markets - Largest Disagreement with Market Price",
        "",
        "| Market ID | P(Model) | P(Market) | Disagreement | Label | Model Correct | Market Correct |",
        "|-----------|----------|-----------|-------------|-------|--------------|----------------|",
    ]
    for row in diag.get("top_10_disagreements", []):
        mid = row['market_id']
        mid_display = mid[:20] + "..." if len(mid) > 20 else mid
        lines.append(
            f"| {mid_display} | {row['p_model']:.3f} | {row['p_market']:.3f} | "
            f"{row['disagreement']:.3f} | {row['label']} | {row['model_correct']} | {row['market_correct']} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 6. Top 10 Markets - Most Confidently Wrong",
        "",
        "| Market ID | P(Model) | P(Market) | Label | Error | Confidence |",
        "|-----------|----------|-----------|-------|-------|-----------|",
    ]
    for row in diag.get("top_10_confident_wrong", []):
        mid = row['market_id']
        mid_display = mid[:20] + "..." if len(mid) > 20 else mid
        lines.append(
            f"| {mid_display} | {row['p_model']:.3f} | {row['p_market']:.3f} | "
            f"{row['label']} | {row['error']:.4f} | {row['confidence']:.3f} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 7. Charts",
        "",
        "- Reliability diagram: `reports/reliability.png`",
        "- Probability histogram: `reports/prob_histogram.png`",
        "",
        "---",
        "",
        "## 8. Next Investigation Steps",
        "",
    ]

    if not beats_market:
        lines += [
            "The model does not beat the market baseline. Recommended next steps:",
            "",
            "1. **Feature engineering:** The current features may not encode useful information beyond the market price itself. Consider adding order book imbalance, resolution oracle type, or market age.",
            "2. **Market selection:** Restrict to markets where BTC price is the resolution criterion — these markets may have predictable correlation with BTC momentum.",
            "3. **Temporal effects:** Check if the model's edge (or lack thereof) varies by time-to-resolution. Models may have edge only very close to resolution.",
            "4. **Sample size:** With < 100 test markets, the confidence intervals will be wide. Accumulate more data.",
        ]
    else:
        lines += [
            "The model beats the market baseline. Recommended next steps:",
            "",
            "1. **Live paper trading:** Apply the model to live Limitless markets in read-only mode. Track predicted edge vs realized outcomes.",
            "2. **Feature importance:** Identify which features contribute most to the edge. Drop the others to reduce overfitting risk.",
            "3. **Rolling re-evaluation:** Re-run this evaluation monthly to detect decay.",
            "4. **Slippage accounting:** Before live trading, factor in Limitless AMM fees and market impact.",
        ]

    output_path.write_text("\n".join(lines) + "\n")
    LOGGER.info("Evaluation report written to %s", output_path)
