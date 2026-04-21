"""Validation results comparison."""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from web.utils import fmt_float, fmt_pct, load_all_validation_results


def render() -> None:
    st.markdown('<div class="page-header">Validation</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Compare baseline, stress, shuffle, and holdout runs</div>', unsafe_allow_html=True)

    all_results = load_all_validation_results()
    if not all_results:
        st.warning("No validation results found. Run experiments from the **Backtest** page.")
        return

    KEY_METRICS = [
        ("total_return",  "Total Return", fmt_pct),
        ("sharpe_ratio",  "Sharpe Ratio", fmt_float),
        ("win_rate",      "Win Rate",     fmt_pct),
        ("trade_count",   "Trades",       lambda v: str(int(float(v))) if v else "—"),
        ("max_drawdown",  "Max Drawdown", fmt_pct),
        ("brier_score",   "Brier Score",  fmt_float),
        ("ece",           "ECE",          fmt_float),
        ("avg_edge",      "Avg Edge",     fmt_float),
    ]

    import pandas as pd
    rows = []
    for metric_key, label, fmt in KEY_METRICS:
        row = {"Metric": label}
        for run_name, m in all_results.items():
            v = m.get(metric_key)
            row[run_name] = fmt(v) if v is not None else "—"
        rows.append(row)

    st.markdown('<div class="section-title">Metrics Table</div>', unsafe_allow_html=True)
    df = pd.DataFrame(rows).set_index("Metric")
    st.dataframe(df, use_container_width=True)

    # ── Charts ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Visual Comparison</div>', unsafe_allow_html=True)
    cl, cr = st.columns(2)

    COLORS = ["#00d4aa", "#818cf8", "#f87171", "#fbbf24", "#38bdf8"]

    with cl:
        sharpe_data = {n: m.get("sharpe_ratio") for n, m in all_results.items() if m.get("sharpe_ratio") is not None}
        if sharpe_data:
            names = list(sharpe_data.keys())
            vals = list(sharpe_data.values())
            fig = go.Figure(go.Bar(
                x=names, y=vals, text=[f"{v:.2f}" for v in vals],
                textposition="outside",
                marker_color=COLORS[:len(names)],
            ))
            fig.update_layout(
                title=dict(text="Sharpe Ratio", font=dict(size=13, color="#94a3b8")),
                paper_bgcolor="#111827", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="#1e2a3a", showline=False),
                margin=dict(l=30, r=20, t=40, b=30),
                height=270, showlegend=False,
            )
            st.plotly_chart(fig, width="stretch")

    with cr:
        ret_data = {n: m.get("total_return") for n, m in all_results.items() if m.get("total_return") is not None}
        if ret_data:
            names = list(ret_data.keys())
            vals = [float(v) * 100 for v in ret_data.values()]
            colors = ["#00d4aa" if v >= 0 else "#f87171" for v in vals]
            fig2 = go.Figure(go.Bar(
                x=names, y=vals, text=[f"{v:.1f}%" for v in vals],
                textposition="outside",
                marker_color=colors,
            ))
            fig2.update_layout(
                title=dict(text="Total Return (%)", font=dict(size=13, color="#94a3b8")),
                paper_bgcolor="#111827", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="#1e2a3a", showline=False, ticksuffix="%"),
                margin=dict(l=30, r=20, t=40, b=30),
                height=270, showlegend=False,
            )
            st.plotly_chart(fig2, width="stretch")

    st.markdown('<div class="section-title">Raw JSON</div>', unsafe_allow_html=True)
    selected = st.selectbox("Run", list(all_results.keys()))
    if selected:
        st.json(all_results[selected])
