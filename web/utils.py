"""Shared data-loading utilities for the Streamlit dashboard."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_metrics(path: Path | str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with p.open() as f:
            return json.load(f)
    except Exception:
        return {}


def load_trade_log(path: Path | str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
        for col in ("timestamp", "entry_timestamp", "exit_timestamp"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        return df
    except Exception:
        return pd.DataFrame()


def load_all_validation_results() -> dict[str, dict]:
    root = project_root() / "data"
    results: dict[str, dict] = {}
    for name in ("baseline", "stress", "strict_holdout"):
        m = load_metrics(root / name / "metrics.json")
        if m:
            results[name] = m
    for d in sorted(root.glob("shuffle_*")):
        m = load_metrics(d / "metrics.json")
        if m:
            results[d.name] = m
    return results


def load_training_metadata() -> dict:
    return load_metrics(project_root() / "models" / "training_metadata.json")


def fmt_pct(value, decimals: int = 1) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(value)


def fmt_float(value, decimals: int = 3) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def css() -> str:
    return """
<style>
/* ── Global ──────────────────────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0a0e1a !important;
}
[data-testid="stSidebar"] {
    background-color: #0d1117 !important;
    border-right: 1px solid #1e2a3a;
}
/* ── Metric cards ────────────────────────────────────────────── */
.kpi-card {
    background: #111827;
    border: 1px solid #1e2a3a;
    border-radius: 10px;
    padding: 16px 20px;
    text-align: center;
}
.kpi-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #64748b;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 26px;
    font-weight: 700;
    color: #e2e8f0;
    line-height: 1;
}
.kpi-value.pos { color: #00d4aa; }
.kpi-value.neg { color: #f87171; }
/* ── Section headers ─────────────────────────────────────────── */
.section-title {
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #64748b;
    border-bottom: 1px solid #1e2a3a;
    padding-bottom: 6px;
    margin: 20px 0 12px 0;
}
/* ── Page header ─────────────────────────────────────────────── */
.page-header {
    font-size: 22px;
    font-weight: 700;
    color: #e2e8f0;
    margin-bottom: 4px;
}
.page-sub {
    font-size: 13px;
    color: #64748b;
    margin-bottom: 20px;
}
/* ── Sidebar nav ─────────────────────────────────────────────── */
.sidebar-logo {
    font-size: 20px;
    font-weight: 800;
    color: #00d4aa;
    letter-spacing: -0.02em;
    padding: 8px 0 4px 0;
}
.sidebar-sub {
    font-size: 11px;
    color: #475569;
    margin-bottom: 16px;
}
</style>
"""
