"""Sequential replay backtest with walk-forward and holdout validation support."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

from decision.engine import DecisionEngine
from execution.simulator import SimulatedExecutor
from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataBundle
from ingestion.replay import HistoricalReplaySource
from metrics.report import compute_metrics, write_reports
from models.trainer import WalkForwardTrainer
from portfolio.book import PortfolioManager
from project.configuration import AppConfig
from project.types import (
    BacktestResult,
    MarketSnapshot,
    Prediction,
    Position,
    TradeIntent,
)


@dataclass
class BacktestEngine:
    """Run a sequential, timestamp-safe backtest or validation replay."""

    config: AppConfig
    bundle: DataBundle
    feature_builder: FeatureBuilder
    feature_store: FeatureStore

    def run(
        self,
        persist_artifacts: bool = True,
        *,
        split_mode: str = "walk_forward",
        experiment_id: str = "baseline",
        label_map: Mapping[str, int] | None = None,
        metrics_path: Path | None = None,
        trade_log_path: Path | None = None,
        prediction_path: Path | None = None,
    ) -> BacktestResult:
        """Execute the configured backtest with a chosen split policy and label mapping."""

        trainer = WalkForwardTrainer(
            self.config, self.bundle, self.feature_builder, self.feature_store
        )
        if split_mode == "strict_holdout":
            trained_folds = trainer.train_strict_holdout(label_map=label_map)
        else:
            trained_folds = trainer.train_folds_with_labels(label_map=label_map)
        decision_engine = DecisionEngine(self.config.trading)
        executor = SimulatedExecutor(self.config.trading)
        portfolio = PortfolioManager(self.config.trading)
        state = portfolio.create_initial_state()

        trade_log: list[dict[str, object]] = []
        prediction_log: list[dict[str, object]] = []
        equity_curve: list[dict[str, object]] = []
        metadata_map = self.bundle.market_metadata.set_index("market_id")
        replay = HistoricalReplaySource(self.bundle)
        fold_by_market = {
            market_id: trained_fold
            for trained_fold in trained_folds
            for market_id in trained_fold.fold.test_market_ids
        }
        print(
            f"Folds: {len(trained_folds)}, fold_by_market: {list(fold_by_market.keys())}"
        )
        settled_markets: set[str] = set()
        entry_prediction_by_position: dict[str, dict[str, object]] = {}

        for event in replay.iter_events():
            current_prices: dict[tuple[str, str], float] = {}
            for _, row in event.market_rows.iterrows():
                current_prices[(row["market_id"], "YES")] = float(row["p_market"])
                current_prices[(row["market_id"], "NO")] = 1.0 - float(row["p_market"])
            portfolio.mark_to_market(state, current_prices)

            for _, row in event.market_rows.iterrows():
                market_id = row["market_id"]
                if market_id not in fold_by_market:
                    continue
                trained_fold = fold_by_market[market_id]
                meta = metadata_map.loc[market_id]
                if (
                    event.timestamp <= trained_fold.fold.calibration_end_time
                    or event.timestamp > trained_fold.fold.test_end_time
                ):
                    continue
                if event.timestamp >= meta["resolution_time"]:
                    continue

                market_history = self.bundle.market_snapshots[
                    self.bundle.market_snapshots["market_id"] == market_id
                ]
                feature_row = self.feature_builder.build_features(
                    market_history=market_history,
                    crypto_history=self.bundle.crypto_snapshots,
                    as_of=event.timestamp,
                    resolution_time=meta["resolution_time"],
                    label=None,
                    market_id=market_id,
                )

                p_model_raw = trained_fold.predictor.predict_raw(feature_row)
                p_model_calibrated = trained_fold.predictor.predict(feature_row)
                prediction = Prediction(
                    market_id=market_id,
                    timestamp=event.timestamp,
                    p_model_raw=p_model_raw,
                    p_model_calibrated=p_model_calibrated,
                    p_market=float(row["p_market"]),
                )

                prediction_log.append({
                    "market_id": market_id,
                    "timestamp": prediction.timestamp.isoformat(),
                    "p_model_raw": prediction.p_model_raw,
                    "p_model_calibrated": prediction.p_model_calibrated,
                    "p_market": prediction.p_market,
                    "fold_id": trained_fold.fold.fold_id,
                    "label": None,
                })

                intent = decision_engine.evaluate(
                    prediction, float(row["liquidity"]), state, event.timestamp
                )
                approved, reason = portfolio.can_accept(
                    state, market_id, intent.requested_notional, event.timestamp
                )
                if not approved:
                    intent = TradeIntent(
                        market_id=intent.market_id,
                        timestamp=intent.timestamp,
                        action="HOLD",
                        side=None,
                        requested_notional=0.0,
                        expected_value=intent.expected_value,
                        edge=intent.edge,
                        reason=reason,
                    )

                snapshot = MarketSnapshot(
                    timestamp=event.timestamp,
                    market_id=market_id,
                    p_market=float(row["p_market"]),
                    volume=float(row["volume"]),
                    liquidity=float(row["liquidity"]),
                )
                execution = executor.execute(
                    intent,
                    snapshot,
                    market_history[market_history["timestamp"] <= event.timestamp],
                )
                position = portfolio.apply_execution(
                    state, execution, intent, prediction
                )
                if position is not None:
                    entry_prediction_by_position[position.position_id] = {
                        "p_model_raw": p_model_raw,
                        "p_model_calibrated": p_model_calibrated,
                        "p_market": float(row["p_market"]),
                        "edge": intent.edge,
                        "expected_value": intent.expected_value,
                        "resolution_time": meta["resolution_time"].isoformat(),
                    }
                trade_log.append(
                    self._execution_record(
                        experiment_id=experiment_id,
                        split_mode=split_mode,
                        fold_id=trained_fold.fold.fold_id,
                        market_id=market_id,
                        row=row,
                        prediction=prediction,
                        intent=intent,
                        execution=execution,
                        position=position,
                        resolution_time=meta["resolution_time"].isoformat(),
                    )
                )

            for _, meta in self.bundle.market_metadata.iterrows():
                market_id = meta["market_id"]
                if market_id in settled_markets:
                    continue
                if event.timestamp >= meta["resolution_time"]:
                    settlements = portfolio.settle_market(
                        state, market_id, int(meta["outcome_yes"]), event.timestamp
                    )
                    for settlement in settlements:
                        entry_prediction = entry_prediction_by_position.get(
                            settlement.position_id, {}
                        )
                        trade_log.append(
                            {
                                "experiment_id": experiment_id,
                                "split_type": split_mode,
                                "fold_id": fold_by_market.get(market_id).fold.fold_id
                                if market_id in fold_by_market
                                else None,
                                "position_id": settlement.position_id,
                                "timestamp": event.timestamp.isoformat(),
                                "entry_timestamp": settlement.entry_timestamp.isoformat(),
                                "exit_timestamp": settlement.exit_timestamp.isoformat(),
                                "market_id": market_id,
                                "event": "settlement",
                                "action": "SETTLE",
                                "side": settlement.side,
                                "p_model_raw_entry": float(
                                    entry_prediction.get("p_model_raw", 0.0)
                                ),
                                "p_model_calibrated_entry": float(
                                    entry_prediction.get("p_model_calibrated", 0.0)
                                ),
                                "p_market_entry": float(
                                    entry_prediction.get("p_market", 0.0)
                                ),
                                "edge_entry": float(entry_prediction.get("edge", 0.0)),
                                "ev_entry": float(
                                    entry_prediction.get("expected_value", 0.0)
                                ),
                                "filled_notional": settlement.cost_basis,
                                "fill_price": settlement.average_price,
                                "fees_paid": 0.0,
                                "reason": "resolution",
                                "holding_duration_seconds": settlement.holding_duration_seconds,
                                "resolved_outcome": settlement.resolved_outcome,
                                "realized_pnl": settlement.realized_pnl,
                                "return_on_notional": settlement.realized_pnl
                                / settlement.cost_basis
                                if settlement.cost_basis > 0.0
                                else 0.0,
                                "prediction_error_raw": abs(
                                    float(entry_prediction.get("p_model_raw", 0.0))
                                    - float(settlement.resolved_outcome)
                                ),
                                "prediction_error_calibrated": abs(
                                    float(
                                        entry_prediction.get("p_model_calibrated", 0.0)
                                    )
                                    - float(settlement.resolved_outcome)
                                ),
                                "outcome_vs_prediction_raw": float(
                                    entry_prediction.get("p_model_raw", 0.0)
                                )
                                - float(settlement.resolved_outcome),
                                "outcome_vs_prediction_calibrated": float(
                                    entry_prediction.get("p_model_calibrated", 0.0)
                                )
                                - float(settlement.resolved_outcome),
                            }
                        )
                    settled_markets.add(market_id)
                    for entry in prediction_log:
                        if entry["market_id"] == market_id and entry["label"] is None:
                            entry["label"] = int(meta["outcome_yes"])

            equity_curve.append(
                {
                    "timestamp": event.timestamp,
                    "equity": state.cash + state.gross_exposure,
                }
            )

        # Force-settle any open positions for markets whose outcome is known but
        # resolution_time fell outside the replay window (e.g. test window ends
        # at 23:45 but market resolves at 00:00 the next day).
        final_timestamp = equity_curve[-1]["timestamp"] if equity_curve else None
        if final_timestamp is not None:
            for _, meta in self.bundle.market_metadata.iterrows():
                market_id = meta["market_id"]
                if market_id in settled_markets:
                    continue
                outcome_yes = meta.get("outcome_yes")
                try:
                    outcome_int = int(float(outcome_yes))
                except (TypeError, ValueError):
                    continue
                settlements = portfolio.settle_market(state, market_id, outcome_int, final_timestamp)
                for settlement in settlements:
                    entry_prediction = entry_prediction_by_position.get(settlement.position_id, {})
                    trade_log.append(
                        {
                            "experiment_id": experiment_id,
                            "split_type": split_mode,
                            "fold_id": fold_by_market.get(market_id).fold.fold_id
                            if market_id in fold_by_market
                            else None,
                            "position_id": settlement.position_id,
                            "timestamp": final_timestamp.isoformat(),
                            "entry_timestamp": settlement.entry_timestamp.isoformat(),
                            "exit_timestamp": settlement.exit_timestamp.isoformat(),
                            "market_id": market_id,
                            "event": "settlement",
                            "action": "SETTLE",
                            "side": settlement.side,
                            "p_model_raw_entry": float(entry_prediction.get("p_model_raw", 0.0)),
                            "p_model_calibrated_entry": float(entry_prediction.get("p_model_calibrated", 0.0)),
                            "p_market_entry": float(entry_prediction.get("p_market", 0.0)),
                            "edge_entry": float(entry_prediction.get("edge", 0.0)),
                            "ev_entry": float(entry_prediction.get("expected_value", 0.0)),
                            "filled_notional": settlement.cost_basis,
                            "fill_price": settlement.average_price,
                            "fees_paid": 0.0,
                            "reason": "force_settle_end_of_window",
                            "holding_duration_seconds": settlement.holding_duration_seconds,
                            "resolved_outcome": settlement.resolved_outcome,
                            "realized_pnl": settlement.realized_pnl,
                            "return_on_notional": settlement.realized_pnl / settlement.cost_basis
                            if settlement.cost_basis > 0.0
                            else 0.0,
                            "prediction_error_raw": abs(
                                float(entry_prediction.get("p_model_raw", 0.0)) - float(settlement.resolved_outcome)
                            ),
                            "prediction_error_calibrated": abs(
                                float(entry_prediction.get("p_model_calibrated", 0.0)) - float(settlement.resolved_outcome)
                            ),
                            "outcome_vs_prediction_raw": float(entry_prediction.get("p_model_raw", 0.0))
                            - float(settlement.resolved_outcome),
                            "outcome_vs_prediction_calibrated": float(entry_prediction.get("p_model_calibrated", 0.0))
                            - float(settlement.resolved_outcome),
                        }
                    )
                if settlements:
                    settled_markets.add(market_id)
                    for entry in prediction_log:
                        if entry["market_id"] == market_id and entry["label"] is None:
                            entry["label"] = outcome_int

        metrics = compute_metrics(
            equity_curve,
            trade_log,
            prediction_log,
            self.config.validation.calibration_bins,
        )
        if persist_artifacts:
            write_reports(
                metrics=metrics,
                trade_log=trade_log,
                prediction_log=prediction_log,
                metrics_path=metrics_path or self.config.data.metrics_report_path,
                trade_log_path=trade_log_path or self.config.data.trade_log_path,
                prediction_path=prediction_path
                or self.config.data.prediction_report_path,
            )
        return BacktestResult(
            metrics=metrics, trade_log=trade_log, prediction_log=prediction_log
        )

    @staticmethod
    def _execution_record(
        *,
        experiment_id: str,
        split_mode: str,
        fold_id: int,
        market_id: str,
        row,
        prediction: Prediction,
        intent: TradeIntent,
        execution,
        position: Position | None,
        resolution_time: str,
    ) -> dict[str, object]:
        """Build a normalized execution-log record."""

        return {
            "experiment_id": experiment_id,
            "split_type": split_mode,
            "fold_id": fold_id,
            "position_id": position.position_id if position is not None else None,
            "timestamp": prediction.timestamp.isoformat(),
            "entry_timestamp": prediction.timestamp.isoformat(),
            "market_id": market_id,
            "event": "execution",
            "action": execution.action,
            "side": execution.side,
            "p_market_entry": prediction.p_market,
            "p_model_raw_entry": prediction.p_model_raw,
            "p_model_calibrated_entry": prediction.p_model_calibrated,
            "edge_entry": intent.edge,
            "ev_entry": intent.expected_value,
            "requested_notional": execution.requested_notional,
            "filled_notional": execution.filled_notional,
            "fill_price": execution.fill_price,
            "fees_paid": execution.fees_paid,
            "slippage_paid": execution.slippage_paid,
            "liquidity_at_entry": float(row["liquidity"]),
            "resolution_time": resolution_time,
            "reason": execution.reason,
        }
