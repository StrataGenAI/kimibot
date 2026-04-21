"""Kimibot dashboard entry point."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

st.set_page_config(
    page_title="Kimibot",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

from web.utils import css
from web.pages import dashboard, backtest, trade_log, validation, inference, audit

st.markdown(css(), unsafe_allow_html=True)

PAGES = {
    "📊  Dashboard":   dashboard,
    "⚡  Backtest":    backtest,
    "📋  Trade Log":   trade_log,
    "🔬  Validation":  validation,
    "🎯  Inference":   inference,
    "🔍  Data Audit":  audit,
}

with st.sidebar:
    st.markdown('<div class="sidebar-logo">📈 Kimibot</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-sub">Limitless Prediction Market Trader</div>', unsafe_allow_html=True)
    st.markdown("---")
    page_key = st.radio("", list(PAGES.keys()), label_visibility="collapsed")
    st.markdown("---")
    st.caption("v0.1.0 · Walk-forward validated")

PAGES[page_key].render()
