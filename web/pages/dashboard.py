"""Dashboard: overview metrics and equity curve."""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from web.utils import load_metrics, load_trade_log, load_training_metadata, project_root


def _kpi(label: str, value: str, cls: str = "") -> str:
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value {cls}">{value}</div>
    </div>"""


def _sign_class(v) -> str:
    try:
        return "pos" if float(v) >= 0 else "neg"
    except Exception:
        return ""


def render() -> None:
    root = project_root()
    metrics = load_metrics(root / "data" / "baseline" / "metrics.json")
    trades = load_trade_log(root / "data" / "baseline" / "trade_log.csv")
    model_meta = load_training_metadata()

    st.markdown('<div class="page-header">Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Latest baseline backtest performance</div>', unsafe_allow_html=True)

    if not metrics:
        st.warning("No baseline results yet. Go to **Backtest** and run one.")
        return

    # ── KPI Row ───────────────────────────────────────────────────────────
    def pct(k):
        v = metrics.get(k)
        return f"{float(v)*100:.1f}%" if v is not None else "—"
    def flt(k, d=3):
        v = metrics.get(k)
        return f"{float(v):.{d}f}" if v is not None else "—"

    ret = metrics.get("total_return")
    dd  = metrics.get("max_drawdown")

    cols = st.columns(5)
    cards = [
        ("Total Return",  pct("total_return"),  _sign_class(ret)),
        ("Sharpe Ratio",  flt("sharpe_ratio"),  _sign_class(metrics.get("sharpe_ratio"))),
        ("Win Rate",      pct("win_rate"),       ""),
        ("Total Trades",  str(metrics.get("total_trades", metrics.get("trade_count", "—"))), ""),
        ("Max Drawdown",  pct("max_drawdown"),   _sign_class(-1 if dd is None else -float(dd))),
    ]
    for col, (label, value, cls) in zip(cols, cards):
        col.markdown(_kpi(label, value, cls), unsafe_allow_html=True)

    st.markdown("")

    # ── Equity Curve ──────────────────────────────────────────────────────
    if not trades.empty and "realized_pnl" in trades.columns and "timestamp" in trades.columns:
        settled = trades[trades["realized_pnl"].notna()].sort_values("timestamp").copy()
        if not settled.empty:
            settled["cum_pnl"] = settled["realized_pnl"].cumsum()
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=settled["timestamp"], y=settled["cum_pnl"],
                mode="lines", fill="tozeroy",
                line=dict(color="#00d4aa", width=2),
                fillcolor="rgba(0,212,170,0.08)",
                name="Cumulative PnL",
                hovertemplate="<b>%{x|%b %d %H:%M}</b><br>PnL: $%{y:.2f}<extra></extra>",
            ))
            fig.update_layout(
                title=dict(text="Equity Curve", font=dict(size=14, color="#94a3b8")),
                paper_bgcolor="#111827", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"),
                xaxis=dict(gridcolor="#1e2a3a", showline=False, zeroline=False),
                yaxis=dict(gridcolor="#1e2a3a", showline=False, zeroline=True, zerolinecolor="#334155", tickprefix="$"),
                margin=dict(l=50, r=20, t=40, b=40),
                height=300,
                showlegend=False,
            )
            st.plotly_chart(fig, width="stretch")

    # ── Win/Loss + Action split ───────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        if not trades.empty and "realized_pnl" in trades.columns:
            s = trades[trades["realized_pnl"].notna()]
            wins = int((s["realized_pnl"] > 0).sum())
            losses = int((s["realized_pnl"] <= 0).sum())
            fig2 = go.Figure(go.Bar(
                x=["Wins", "Losses"], y=[wins, losses],
                marker_color=["#00d4aa", "#f87171"],
                text=[str(wins), str(losses)],
                textposition="outside",
            ))
            fig2.update_layout(
                title=dict(text="Trade Outcomes", font=dict(size=13, color="#94a3b8")),
                paper_bgcolor="#111827", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="#1e2a3a", showline=False),
                margin=dict(l=30, r=20, t=40, b=30),
                height=260, showlegend=False,
            )
            st.plotly_chart(fig2, width="stretch")

    with c2:
        if not trades.empty and "action" in trades.columns:
            vc = trades["action"].value_counts()
            fig3 = go.Figure(go.Pie(
                labels=vc.index.tolist(), values=vc.values.tolist(),
                hole=0.55,
                marker=dict(colors=["#00d4aa", "#818cf8", "#f87171", "#fbbf24"]),
                textinfo="label+percent",
            ))
            fig3.update_layout(
                title=dict(text="Actions", font=dict(size=13, color="#94a3b8")),
                paper_bgcolor="#111827",
                font=dict(color="#94a3b8"),
                margin=dict(l=20, r=20, t=40, b=20),
                height=260, showlegend=False,
            )
            st.plotly_chart(fig3, width="stretch")

    # ── Model info + raw metrics ──────────────────────────────────────────
    if model_meta:
        st.markdown('<div class="section-title">Model</div>', unsafe_allow_html=True)
        mc = st.columns(4)
        mc[0].metric("Fold", str(model_meta.get("fold_id", "—")))
        mc[1].metric("Train markets", str(len(model_meta.get("train_market_ids", []))))
        mc[2].metric("Calib markets", str(len(model_meta.get("calibration_market_ids", []))))
        mc[3].metric("Test markets", str(len(model_meta.get("test_market_ids", []))))
        if "model_train_end_time" in model_meta:
            st.caption(f"Trained through {model_meta['model_train_end_time']}")

    with st.expander("All metrics"):
        scalar = {k: v for k, v in metrics.items() if not isinstance(v, (list, dict))}
        st.json(scalar)
