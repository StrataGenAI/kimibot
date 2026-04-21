"""Backtest runner page."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import streamlit as st

from web.utils import fmt_float, fmt_pct, load_all_validation_results, project_root


def _run_cmd(label: str, args: list[str], root: Path):
    with st.spinner(f"{label}…"):
        result = subprocess.run(
            args, cwd=str(root), capture_output=True, text=True, timeout=300
        )
    if result.returncode == 0:
        st.success(f"{label} completed successfully.")
    else:
        st.error(f"{label} failed (exit {result.returncode}).")
    with st.expander("Output log", expanded=result.returncode != 0):
        out = (result.stdout + result.stderr).strip()
        st.code(out or "(no output)", language="")
    return result.returncode == 0


def render() -> None:
    root = project_root()

    st.markdown('<div class="page-header">Backtest</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Run experiments and compare results</div>', unsafe_allow_html=True)

    # ── Run controls ──────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Run</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("▶  Run Baseline Backtest", use_container_width=True, type="primary"):
            _run_cmd("Baseline backtest", [sys.executable, "main.py", "backtest"], root)
            st.rerun()
    with c2:
        if st.button("🔁  Retrain Model", use_container_width=True):
            _run_cmd("Model training", [sys.executable, "train.py"], root)
    with c3:
        if st.button("✅  Run All Validation", use_container_width=True):
            _run_cmd("Validation suite", [sys.executable, "main.py", "validate", "--mode", "all"], root)
            st.rerun()

    st.markdown('<div class="section-title">Validation Runs</div>', unsafe_allow_html=True)
    cv1, cv2, cv3 = st.columns(3)
    with cv1:
        if st.button("Stress Test", use_container_width=True):
            _run_cmd("Stress test", [sys.executable, "main.py", "validate", "--mode", "stress"], root)
            st.rerun()
    with cv2:
        if st.button("Shuffle Test", use_container_width=True):
            _run_cmd("Shuffle test", [sys.executable, "main.py", "validate", "--mode", "shuffle"], root)
            st.rerun()
    with cv3:
        if st.button("Holdout Test", use_container_width=True):
            _run_cmd("Holdout test", [sys.executable, "main.py", "validate", "--mode", "holdout"], root)
            st.rerun()

    # ── Results table ─────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Results Comparison</div>', unsafe_allow_html=True)
    all_results = load_all_validation_results()
    if not all_results:
        st.info("No results yet. Click **Run Baseline Backtest** above.")
        return

    KEY_METRICS = [
        ("total_return",  "Return",       fmt_pct),
        ("sharpe_ratio",  "Sharpe",       fmt_float),
        ("win_rate",      "Win Rate",     fmt_pct),
        ("trade_count",   "Trades",       lambda v: str(int(float(v))) if v is not None else "—"),
        ("max_drawdown",  "Drawdown",     fmt_pct),
        ("brier_score",   "Brier",        fmt_float),
    ]
    rows = []
    for metric_key, label, fmt in KEY_METRICS:
        row = {"Metric": label}
        for run_name, m in all_results.items():
            v = m.get(metric_key)
            row[run_name] = fmt(v) if v is not None else "—"
        rows.append(row)

    df = pd.DataFrame(rows).set_index("Metric")
    st.dataframe(df, use_container_width=True)
