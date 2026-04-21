"""Portfolio state tracking and risk enforcement."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from project.configuration import TradingConfig
from project.types import ExecutionResult, PortfolioState, Position, Prediction, SettlementResult, TradeIntent
from utils.time import date_key


@dataclass
class PortfolioManager:
    """Manage open positions, PnL, and portfolio risk limits."""

    config: TradingConfig

    def _position_id(self, market_id: str, side: str, timestamp: datetime) -> str:
        """Build a stable position identifier for a market-side lifecycle."""

        return f"{market_id}:{side}:{timestamp.isoformat()}"

    def create_initial_state(self) -> PortfolioState:
        """Create an empty portfolio funded with initial capital."""

        return PortfolioState(cash=self.config.initial_capital)

    def can_accept(self, state: PortfolioState, market_id: str, requested_notional: float, timestamp) -> tuple[bool, str]:
        """Validate portfolio-level limits for a new trade."""

        if requested_notional <= 0.0:
            return False, "zero_notional"
        current_market_exposure = sum(
            position.quantity * position.average_price
            for (position_market_id, _), position in state.open_positions.items()
            if position_market_id == market_id
        )
        if current_market_exposure + requested_notional > self.config.max_position_per_market:
            return False, "max_position_per_market"
        if state.gross_exposure + requested_notional > self.config.max_total_exposure:
            return False, "max_total_exposure"
        daily_pnl = state.daily_realized_pnl.get(date_key(timestamp), 0.0)
        if daily_pnl <= -self.config.daily_loss_limit:
            return False, "daily_loss_limit"
        if state.cash < requested_notional:
            return False, "insufficient_cash"
        return True, "accepted"

    def apply_execution(
        self,
        state: PortfolioState,
        execution: ExecutionResult,
        trade_intent: TradeIntent,
        prediction: Prediction,
    ) -> Position | None:
        """Update portfolio state for an execution result."""

        if execution.status != "filled" or execution.side is None or execution.fill_price is None:
            return None
        quantity = execution.filled_notional / execution.fill_price
        key = (execution.market_id, execution.side)
        position = state.open_positions.get(key)
        if position is None:
            position = Position(
                position_id=self._position_id(execution.market_id, execution.side, execution.timestamp),
                market_id=execution.market_id,
                side=execution.side,
                quantity=0.0,
                average_price=0.0,
                cost_basis=0.0,
                entry_timestamp=execution.timestamp,
                last_update_timestamp=execution.timestamp,
                entry_p_model_raw=prediction.p_model_raw,
                entry_p_model_calibrated=prediction.p_model_calibrated,
                entry_p_market=prediction.p_market,
                entry_edge=trade_intent.edge,
                entry_expected_value=trade_intent.expected_value,
            )
            state.open_positions[key] = position

        total_cost = position.cost_basis + execution.filled_notional + execution.fees_paid
        total_quantity = position.quantity + quantity
        position.quantity = total_quantity
        position.cost_basis = total_cost
        position.average_price = total_cost / total_quantity
        position.last_update_timestamp = execution.timestamp

        state.cash -= execution.filled_notional + execution.fees_paid
        state.gross_exposure += execution.filled_notional
        return position

    def mark_to_market(self, state: PortfolioState, snapshot_map: dict[tuple[str, str], float]) -> None:
        """Revalue open positions using current prices."""

        unrealized = 0.0
        exposure = 0.0
        for key, position in state.open_positions.items():
            price = snapshot_map.get(key)
            if price is None:
                continue
            market_value = position.quantity * price
            unrealized += market_value - position.cost_basis
            exposure += market_value
        state.unrealized_pnl = unrealized
        state.gross_exposure = exposure

    def settle_market(self, state: PortfolioState, market_id: str, outcome_yes: int, timestamp) -> list[SettlementResult]:
        """Settle all open positions in a resolved market."""

        settlements: list[SettlementResult] = []
        for side in ("YES", "NO"):
            key = (market_id, side)
            position = state.open_positions.pop(key, None)
            if position is None:
                continue
            payout_price = 1.0 if ((side == "YES" and outcome_yes == 1) or (side == "NO" and outcome_yes == 0)) else 0.0
            payout = position.quantity * payout_price
            realized = payout - position.cost_basis
            state.cash += payout
            state.realized_pnl += realized
            state.daily_realized_pnl[date_key(timestamp)] = state.daily_realized_pnl.get(date_key(timestamp), 0.0) + realized
            settlements.append(
                SettlementResult(
                    position_id=position.position_id,
                    market_id=market_id,
                    side=side,
                    entry_timestamp=position.entry_timestamp,
                    exit_timestamp=timestamp,
                    holding_duration_seconds=(timestamp - position.entry_timestamp).total_seconds(),
                    quantity=position.quantity,
                    average_price=position.average_price,
                    cost_basis=position.cost_basis,
                    payout_price=payout_price,
                    payout=payout,
                    resolved_outcome=outcome_yes,
                    realized_pnl=realized,
                )
            )
        return settlements
