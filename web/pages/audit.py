"""Data audit viewer."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st

from web.utils import load_metrics, project_root


def render() -> None:
    root = project_root()
    audit_path = root / "data" / "audit_report.json"

    st.markdown('<div class="page-header">Data Audit</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Read-only health check of ingested market and crypto data</div>', unsafe_allow_html=True)

    if st.button("▶  Run Audit Now", type="primary"):
        with st.spinner("Auditing data…"):
            result = subprocess.run(
                [sys.executable, "main.py", "audit-data"],
                cwd=str(root), capture_output=True, text=True, timeout=120,
            )
        if result.returncode == 0:
            st.success("Audit complete.")
        else:
            st.error("Audit failed.")
        with st.expander("Output"):
            st.code((result.stdout + result.stderr).strip() or "(no output)")
        st.rerun()

    report = load_metrics(audit_path)
    if not report:
        st.info("No audit report yet. Click **Run Audit Now** above.")
        return

    # ── Summary ───────────────────────────────────────────────────────────
    summary = report.get("summary", {})
    if summary:
        st.markdown('<div class="section-title">Summary</div>', unsafe_allow_html=True)
        sc = st.columns(4)
        sc[0].metric("Markets",   str(summary.get("total_markets", "—")))
        sc[1].metric("Snapshots", str(summary.get("total_snapshots", "—")))
        sc[2].metric("Gaps",      str(summary.get("total_gaps", "—")))
        sc[3].metric("Anomalies", str(summary.get("total_anomalies", "—")))

    # ── Per-market table ──────────────────────────────────────────────────
    per_market = report.get("per_market", {})
    if per_market:
        import pandas as pd
        st.markdown('<div class="section-title">Per-Market Health</div>', unsafe_allow_html=True)
        rows = []
        for mid, stats in per_market.items():
            row = {"market_id": mid}
            row.update({k: v for k, v in stats.items() if not isinstance(v, (dict, list))})
            rows.append(row)
        if rows:
            st.dataframe(pd.DataFrame(rows).set_index("market_id"), use_container_width=True, height=360)

    # ── Anomalies ─────────────────────────────────────────────────────────
    anomalies = report.get("anomalies", {})
    if isinstance(anomalies, dict):
        for category, details in anomalies.items():
            if isinstance(details, dict) and details.get("examples"):
                st.markdown(f'<div class="section-title">Anomalies — {category}</div>', unsafe_allow_html=True)
                for ex in details["examples"][:10]:
                    st.warning(str(ex))
    elif isinstance(anomalies, list) and anomalies:
        st.markdown('<div class="section-title">Anomalies</div>', unsafe_allow_html=True)
        for a in anomalies[:20]:
            st.warning(str(a))

    with st.expander("Full audit JSON"):
        st.json(report)
