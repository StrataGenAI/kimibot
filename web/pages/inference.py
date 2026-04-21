"""Single-market inference page."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import streamlit as st

from web.utils import project_root


def render() -> None:
    root = project_root()

    st.markdown('<div class="page-header">Inference</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Run the trained model on any market at any point in time</div>', unsafe_allow_html=True)

    model_path = root / "models" / "logistic_regression.pkl"
    if not model_path.exists():
        st.error("No trained model found at `models/logistic_regression.pkl`. Go to **Backtest → Retrain Model** first.")
        return

    # ── Load market IDs ───────────────────────────────────────────────────
    meta_path = root / "data" / "market_metadata.csv"
    market_ids: list[str] = []
    if meta_path.exists():
        try:
            meta_df = pd.read_csv(meta_path)
            if "market_id" in meta_df.columns:
                market_ids = sorted(meta_df["market_id"].dropna().unique().tolist())
        except Exception:
            pass

    # ── Inputs ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Parameters</div>', unsafe_allow_html=True)
    ci1, ci2, ci3 = st.columns([2, 1, 1])
    with ci1:
        market_id = st.selectbox("Market ID", market_ids) if market_ids else st.text_input("Market ID", placeholder="m5")
    with ci2:
        ts_date = st.date_input("Date (UTC)", value=datetime(2026, 1, 2).date())
    with ci3:
        ts_time = st.time_input("Time (UTC)", value=datetime(2026, 1, 2, 6, 0).time())

    if st.button("Run Prediction", type="primary"):
        _predict(root, str(market_id), datetime.combine(ts_date, ts_time))


def _predict(root: Path, market_id: str, as_of: datetime) -> None:
    from features.builder import FeatureBuilder
    from features.store import FeatureStore
    from ingestion.data_store import DataStore
    from models.predictor import LogisticRegressionPredictor
    from project.configuration import load_config

    try:
        with st.spinner("Loading data and running inference…"):
            config = load_config(root / "config" / "default.yaml")
            bundle = DataStore(config).load()

            meta_row = bundle.market_metadata[bundle.market_metadata["market_id"] == market_id]
            if meta_row.empty:
                st.error(f"Market **{market_id}** not found in dataset.")
                return

            meta = meta_row.iloc[0]
            market_history = bundle.market_snapshots[bundle.market_snapshots["market_id"] == market_id]
            if market_history.empty:
                st.error(f"No snapshots found for market **{market_id}**.")
                return

            as_of_ts = pd.Timestamp(as_of, tz="UTC")
            feature_store = FeatureStore(config.data.feature_cache_path, config.runtime.feature_schema_version)
            builder = FeatureBuilder(config.runtime.feature_schema_version)

            feature_row = feature_store.get_or_build(
                market_id, as_of_ts,
                lambda: builder.build_features(
                    market_history=market_history,
                    crypto_history=bundle.crypto_snapshots,
                    as_of=as_of_ts,
                    resolution_time=meta["resolution_time"],
                    label=None,
                    market_id=market_id,
                ),
            )

            predictor = LogisticRegressionPredictor.load(
                model_path=config.data.model_artifact_path,
                scaler_path=config.data.scaler_artifact_path,
                calibrator_path=config.data.calibrator_artifact_path,
                metadata_path=config.data.training_metadata_path,
            )
            raw_prob  = predictor.predict_raw(feature_row)
            cal_prob  = predictor.predict(feature_row)
            snap = market_history[market_history["timestamp"] <= as_of_ts]
            p_market = float(snap.iloc[-1]["p_market"]) if not snap.empty else None

        # ── Results ───────────────────────────────────────────────────────
        st.markdown('<div class="section-title">Result</div>', unsafe_allow_html=True)
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Raw Probability",        f"{raw_prob:.3f}")
        r2.metric("Calibrated Probability", f"{cal_prob:.3f}")
        if p_market is not None:
            r3.metric("Market Probability", f"{p_market:.3f}")
            edge_yes = cal_prob - p_market
            edge_no  = (1 - cal_prob) - (1 - p_market)
            best_edge = max(edge_yes, edge_no)
            best_side = "YES" if edge_yes > edge_no else "NO"
            cfg = config.trading
            if best_edge > cfg.edge_threshold and best_edge <= cfg.max_edge:
                r4.metric("Signal", f"BUY {best_side}", delta=f"edge {best_edge:+.3f}")
            else:
                r4.metric("Signal", "HOLD", delta=f"edge {best_edge:+.3f}")

        # ── Feature values ────────────────────────────────────────────────
        with st.expander("Feature values"):
            fv = {k: round(v, 5) for k, v in feature_row.values.items()}
            st.dataframe(
                pd.DataFrame(fv.items(), columns=["Feature", "Value"]).set_index("Feature"),
                use_container_width=True,
            )

    except Exception as exc:
        st.error(f"Inference error: {exc}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
