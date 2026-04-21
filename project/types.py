"""Shared typed records used across the trading system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class MarketMetadata:
    """Static metadata for a prediction market."""

    market_id: str
    slug: str
    asset: str
    resolution_time: datetime
    outcome_yes: int


@dataclass(frozen=True)
class MarketSnapshot:
    """A timestamped market observation."""

    timestamp: datetime
    market_id: str
    p_market: float
    volume: float
    liquidity: float


@dataclass(frozen=True)
class CryptoSnapshot:
    """A timestamped crypto market observation."""

    timestamp: datetime
    btc_price: float
    eth_price: float
    funding_rate: float


@dataclass(frozen=True)
class FeatureRow:
    """Deterministic model features for a market and timestamp."""

    market_id: str
    timestamp: datetime
    resolution_time: datetime
    label: int | None
    values: dict[str, float]
    market_source_max_ts: datetime
    crypto_source_max_ts: datetime
    schema_version: str


@dataclass(frozen=True)
class Prediction:
    """Model prediction for the probability of YES."""

    market_id: str
    timestamp: datetime
    p_model_raw: float
    p_model_calibrated: float
    p_market: float


@dataclass(frozen=True)
class TradeIntent:
    """Decision engine output before execution."""

    market_id: str
    timestamp: datetime
    action: str
    side: str | None
    requested_notional: float
    expected_value: float
    edge: float
    reason: str


@dataclass(frozen=True)
class ExecutionResult:
    """Simulated or real execution result."""

    market_id: str
    timestamp: datetime
    action: str
    side: str | None
    requested_notional: float
    filled_notional: float
    fill_price: float | None
    fees_paid: float
    slippage_paid: float
    status: str
    reason: str


@dataclass(frozen=True)
class SettlementResult:
    """Closed-position settlement details."""

    position_id: str
    market_id: str
    side: str
    entry_timestamp: datetime
    exit_timestamp: datetime
    holding_duration_seconds: float
    quantity: float
    average_price: float
    cost_basis: float
    payout_price: float
    payout: float
    resolved_outcome: int
    realized_pnl: float


@dataclass
class Position:
    """An open market position."""

    position_id: str
    market_id: str
    side: str
    quantity: float
    average_price: float
    cost_basis: float
    entry_timestamp: datetime
    last_update_timestamp: datetime
    entry_p_model_raw: float
    entry_p_model_calibrated: float
    entry_p_market: float
    entry_edge: float
    entry_expected_value: float
    realized_pnl: float = 0.0


@dataclass
class PortfolioState:
    """Full portfolio state and constraints."""

    cash: float
    open_positions: dict[tuple[str, str], Position] = field(default_factory=dict)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    gross_exposure: float = 0.0
    daily_realized_pnl: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class WalkForwardFold:
    """A single walk-forward training and testing segment."""

    fold_id: int
    train_market_ids: list[str]
    calibration_market_ids: list[str]
    test_market_ids: list[str]
    model_train_end_time: datetime
    calibration_end_time: datetime
    test_start_time: datetime
    test_end_time: datetime


@dataclass(frozen=True)
class BacktestResult:
    """Backtest artifacts returned at the end of a run."""

    metrics: dict[str, object]
    trade_log: list[dict[str, object]]
    prediction_log: list[dict[str, object]]
