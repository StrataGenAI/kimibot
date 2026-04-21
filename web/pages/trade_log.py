"""Trade log browser."""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st

from web.utils import load_trade_log, project_root


def render() -> None:
    root = project_root()

    st.markdown('<div class="page-header">Trade Log</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Browse and filter all executed trades</div>', unsafe_allow_html=True)

    trades = load_trade_log(root / "data" / "baseline" / "trade_log.csv")
    if trades.empty:
        st.warning("No trade log found. Run a backtest first.")
        return

    # ── Summary bar ───────────────────────────────────────────────────────
    settled = trades[trades["realized_pnl"].notna()] if "realized_pnl" in trades.columns else trades.iloc[:0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total rows", f"{len(trades):,}")
    if not settled.empty:
        total_pnl = settled["realized_pnl"].sum()
        wins = int((settled["realized_pnl"] > 0).sum())
        losses = int((settled["realized_pnl"] <= 0).sum())
        c2.metric("Realized PnL", f"${total_pnl:,.2f}", delta=None)
        c3.metric("Wins", str(wins))
        c4.metric("Losses", str(losses))

    st.markdown('<div class="section-title">Filters</div>', unsafe_allow_html=True)
    fc1, fc2, fc3, fc4 = st.columns(4)

    market_opts = ["All"] + sorted(trades["market_id"].dropna().unique().tolist()) if "market_id" in trades.columns else ["All"]
    action_opts = ["All"] + sorted(trades["action"].dropna().unique().tolist()) if "action" in trades.columns else ["All"]
    event_opts  = ["All"] + sorted(trades["event"].dropna().unique().tolist()) if "event" in trades.columns else ["All"]

    mkt  = fc1.selectbox("Market", market_opts, key="tl_market")
    act  = fc2.selectbox("Action", action_opts, key="tl_action")
    evt  = fc3.selectbox("Event", event_opts, key="tl_event")
    pnl_f = fc4.selectbox("PnL", ["All", "Wins", "Losses"], key="tl_pnl")

    df = trades.copy()
    if mkt  != "All" and "market_id" in df.columns: df = df[df["market_id"] == mkt]
    if act  != "All" and "action"    in df.columns: df = df[df["action"]    == act]
    if evt  != "All" and "event"     in df.columns: df = df[df["event"]     == evt]
    if pnl_f == "Wins"   and "realized_pnl" in df.columns: df = df[df["realized_pnl"] >  0]
    if pnl_f == "Losses" and "realized_pnl" in df.columns: df = df[df["realized_pnl"] <= 0]

    st.caption(f"{len(df):,} of {len(trades):,} rows")

    want = ["timestamp", "market_id", "event", "action", "side",
            "filled_notional", "fill_price", "fees_paid", "realized_pnl",
            "reason", "p_model_calibrated_entry", "edge_entry"]
    show_cols = [c for c in want if c in df.columns]
    if not show_cols:
        show_cols = df.columns.tolist()

    disp = df[show_cols]
    if "timestamp" in disp.columns:
        disp = disp.sort_values("timestamp", ascending=False)

    st.dataframe(disp, use_container_width=True, height=520)
