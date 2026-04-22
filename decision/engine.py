"""Expected-value decision engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from project.configuration import TradingConfig
from project.types import PortfolioState, Prediction, TradeIntent

LOGGER = logging.getLogger(__name__)


@dataclass
class DecisionEngine:
    """Convert model probabilities into trade intents with edge constraints."""

    config: TradingConfig

    # Track last trade time per market for cooldown
    _last_trade_time: dict[str, datetime] = None

    # Signal logging
    _signal_log: list[dict] = None

    def __post_init__(self):
        if self._last_trade_time is None:
            object.__setattr__(self, "_last_trade_time", {})
        if self._signal_log is None:
            object.__setattr__(self, "_signal_log", [])
        if getattr(self.config, "paper_mode_unsafe_liquidity", False):
            LOGGER.warning(
                "DecisionEngine: paper_mode_unsafe_liquidity=True — "
                "liquidity gate is DISABLED. Do not interpret paper PnL as "
                "realistic execution."
            )

    def evaluate(
        self,
        prediction: Prediction,
        liquidity: float,
        portfolio_state: PortfolioState,
        current_time: datetime | None = None,
    ) -> TradeIntent:
        """Produce a trade intent using edge, EV, liquidity, and risk-aware sizing."""

        # Liquidity check — bypassed in Phase A paper mode (see config flag).
        if not getattr(self.config, "paper_mode_unsafe_liquidity", False):
            if liquidity < self.config.min_liquidity:
                return TradeIntent(
                    market_id=prediction.market_id,
                    timestamp=prediction.timestamp,
                    action="HOLD",
                    side=None,
                    requested_notional=0.0,
                    expected_value=0.0,
                    edge=0.0,
                    reason="insufficient_liquidity",
                )

        # Calculate edges for YES and NO
        edge_yes = prediction.p_model_calibrated - prediction.p_market
        edge_no = (1.0 - prediction.p_model_calibrated) - (1.0 - prediction.p_market)

        # ========== SIGNAL LOGGING ==========
        # Log every prediction for analysis
        self._signal_log.append(
            {
                "market_id": prediction.market_id,
                "timestamp": prediction.timestamp,
                "p_model": prediction.p_model_calibrated,
                "p_market": prediction.p_market,
                "edge_yes": edge_yes,
                "edge_no": edge_no,
            }
        )

        # Cost estimation
        estimated_cost = self.config.fee_rate + (self.config.slippage_bps / 10000.0)
        ev_yes = prediction.p_model_calibrated - prediction.p_market - estimated_cost
        ev_no = (
            (1.0 - prediction.p_model_calibrated)
            - (1.0 - prediction.p_market)
            - estimated_cost
        )

        # ========== EDGE CONSTRAINT FIX ==========
        # Reject if edge > max_edge (likely model overconfidence)
        # Reject if edge < min_edge (no signal)

        max_edge = getattr(self.config, "max_edge", 0.20)  # Default 0.20 if not set
        min_edge = getattr(self.config, "min_edge", 0.02)  # Default 0.02 if not set

        # Check cooldown - no re-trade same market within cooldown period
        cooldown_secs = getattr(self.config, "trade_cooldown_seconds", 60)
        if cooldown_secs > 0:
            last_ts = self._last_trade_time.get(prediction.market_id)
            if last_ts is not None:
                if current_time is not None:
                    time_since = (current_time - last_ts).total_seconds()
                    if time_since < cooldown_secs:
                        return TradeIntent(
                            market_id=prediction.market_id,
                            timestamp=prediction.timestamp,
                            action="HOLD",
                            side=None,
                            requested_notional=0.0,
                            expected_value=0.0,
                            edge=0.0,
                            reason="cooldown_active",
                        )

        action = "HOLD"
        side = None
        edge = 0.0
        ev = 0.0

        # BUY_YES branch with edge constraints
        if (
            edge_yes > self.config.edge_threshold
            and ev_yes > self.config.min_expected_value
        ):
            # Check edge is within realistic bounds (0.02 - 0.15)
            if min_edge <= edge_yes <= max_edge:
                action, side, edge, ev = "BUY_YES", "YES", edge_yes, ev_yes
            else:
                return TradeIntent(
                    market_id=prediction.market_id,
                    timestamp=prediction.timestamp,
                    action="HOLD",
                    side=None,
                    requested_notional=0.0,
                    expected_value=ev_yes,
                    edge=edge_yes,
                    reason=f"edge_out_of_bounds_{edge_yes:.3f}",
                )
        # BUY_NO branch with edge constraints
        elif (
            edge_no > self.config.edge_threshold
            and ev_no > self.config.min_expected_value
        ):
            # Check edge is within realistic bounds
            if min_edge <= edge_no <= max_edge:
                action, side, edge, ev = "BUY_NO", "NO", edge_no, ev_no
            else:
                return TradeIntent(
                    market_id=prediction.market_id,
                    timestamp=prediction.timestamp,
                    action="HOLD",
                    side=None,
                    requested_notional=0.0,
                    expected_value=ev_no,
                    edge=edge_no,
                    reason=f"edge_out_of_bounds_{edge_no:.3f}",
                )
        else:
            return TradeIntent(
                market_id=prediction.market_id,
                timestamp=prediction.timestamp,
                action="HOLD",
                side=None,
                requested_notional=0.0,
                expected_value=max(ev_yes, ev_no),
                edge=max(edge_yes, edge_no),
                reason="edge_or_ev_below_threshold",
            )

        # Position sizing - use fixed percentage if action is BUY
        base_size = portfolio_state.cash * self.config.position_size_multiplier
        requested_notional = max(
            min(base_size, self.config.max_position_per_market), 0.0
        )

        # Update last trade time
        if action != "HOLD" and current_time is not None:
            self._last_trade_time[prediction.market_id] = current_time

        return TradeIntent(
            market_id=prediction.market_id,
            timestamp=prediction.timestamp,
            action=action,
            side=side,
            requested_notional=requested_notional,
            expected_value=ev,
            edge=edge,
            reason="trade_candidate",
        )
