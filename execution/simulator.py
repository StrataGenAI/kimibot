"""Execution simulation with slippage, latency, partial fills, and fees."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

from project.configuration import TradingConfig
from project.types import ExecutionResult, MarketSnapshot, TradeIntent


@dataclass
class SimulatedExecutor:
    """Simulate realistic fills using only information available at execution time."""

    config: TradingConfig

    def execute(self, trade_intent: TradeIntent, market_snapshot: MarketSnapshot, market_history: pd.DataFrame) -> ExecutionResult:
        """Execute a trade intent with slippage, latency penalty, and partial fills."""

        if trade_intent.action == "HOLD" or trade_intent.side is None or trade_intent.requested_notional <= 0.0:
            return ExecutionResult(
                market_id=trade_intent.market_id,
                timestamp=trade_intent.timestamp,
                action=trade_intent.action,
                side=trade_intent.side,
                requested_notional=trade_intent.requested_notional,
                filled_notional=0.0,
                fill_price=None,
                fees_paid=0.0,
                slippage_paid=0.0,
                status="rejected",
                reason=trade_intent.reason,
            )

        base_price = market_snapshot.p_market if trade_intent.side == "YES" else (1.0 - market_snapshot.p_market)
        slippage = base_price * (self.config.slippage_bps / 10000.0)
        velocity = self._price_velocity(market_history, trade_intent.timestamp)
        latency_penalty = self.config.latency_seconds * velocity
        signed_penalty = slippage + max(latency_penalty, 0.0)
        fill_price = min(max(base_price + signed_penalty, 0.01), 0.99)

        available_notional = max(market_snapshot.liquidity * 0.15, 0.0)
        filled_notional = min(trade_intent.requested_notional, available_notional)
        fees_paid = filled_notional * self.config.fee_rate
        slippage_paid = filled_notional * max(fill_price - base_price, 0.0)
        status = "filled" if filled_notional > 0.0 else "rejected"
        reason = "partial_fill" if 0.0 < filled_notional < trade_intent.requested_notional else trade_intent.reason
        return ExecutionResult(
            market_id=trade_intent.market_id,
            timestamp=trade_intent.timestamp,
            action=trade_intent.action,
            side=trade_intent.side,
            requested_notional=trade_intent.requested_notional,
            filled_notional=filled_notional,
            fill_price=fill_price if filled_notional > 0.0 else None,
            fees_paid=fees_paid,
            slippage_paid=slippage_paid,
            status=status,
            reason=reason,
        )

    def _price_velocity(self, market_history: pd.DataFrame, as_of) -> float:
        """Estimate market price velocity from recent history only."""

        cutoff = as_of - timedelta(minutes=self.config.price_velocity_lookback_minutes)
        recent = market_history[(market_history["timestamp"] >= cutoff) & (market_history["timestamp"] <= as_of)].sort_values("timestamp")
        if len(recent) < 2:
            return 0.0
        first = recent.iloc[0]
        last = recent.iloc[-1]
        seconds = (last["timestamp"] - first["timestamp"]).total_seconds()
        if seconds <= 0.0:
            return 0.0
        return float(last["p_market"] - first["p_market"]) / seconds
