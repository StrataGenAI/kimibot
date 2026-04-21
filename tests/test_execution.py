"""Unit tests for the execution simulator."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import pandas as pd

from execution.simulator import SimulatedExecutor
from project.configuration import TradingConfig
from project.types import MarketSnapshot, TradeIntent


def _make_config(**overrides) -> TradingConfig:
    defaults = dict(
        initial_capital=10000.0,
        edge_threshold=0.03,
        min_edge=0.02,
        max_edge=0.15,
        min_expected_value=0.01,
        min_liquidity=500.0,
        max_position_per_market=800.0,
        max_total_exposure=3500.0,
        daily_loss_limit=800.0,
        fee_rate=0.01,
        slippage_bps=40.0,
        latency_seconds=2.0,
        price_velocity_lookback_minutes=2,
        position_size_multiplier=0.10,
        trade_cooldown_seconds=60,
    )
    defaults.update(overrides)
    return TradingConfig(**defaults)


NOW = datetime(2026, 1, 1, 12, 0, 0)


def _make_snapshot(*, p_market=0.50, liquidity=2000.0) -> MarketSnapshot:
    return MarketSnapshot(
        timestamp=NOW,
        market_id="m1",
        p_market=p_market,
        volume=500.0,
        liquidity=liquidity,
    )


def _make_intent(*, action="BUY_YES", side="YES", notional=100.0, reason="trade_candidate") -> TradeIntent:
    return TradeIntent(
        market_id="m1",
        timestamp=NOW,
        action=action,
        side=side,
        requested_notional=notional,
        expected_value=0.05,
        edge=0.10,
        reason=reason,
    )


def _empty_history() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "market_id", "p_market", "volume", "liquidity"])


class SimulatedExecutorTests(unittest.TestCase):

    def test_hold_results_in_no_fill(self):
        executor = SimulatedExecutor(config=_make_config())
        result = executor.execute(_make_intent(action="HOLD", side=None, notional=0.0), _make_snapshot(), _empty_history())
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.filled_notional, 0.0)
        self.assertIsNone(result.fill_price)

    def test_fill_price_includes_slippage(self):
        # slippage_bps=40 → slippage = 0.50 * 0.004 = 0.002
        executor = SimulatedExecutor(config=_make_config(slippage_bps=40.0, latency_seconds=0.0))
        result = executor.execute(_make_intent(notional=100.0), _make_snapshot(p_market=0.50), _empty_history())
        self.assertEqual(result.status, "filled")
        self.assertGreater(result.fill_price, 0.50)

    def test_partial_fill_when_liquidity_limited(self):
        # liquidity=200 → available = 200 * 0.15 = 30, requested=100 → partial
        executor = SimulatedExecutor(config=_make_config(latency_seconds=0.0))
        result = executor.execute(_make_intent(notional=100.0), _make_snapshot(liquidity=200.0), _empty_history())
        self.assertEqual(result.status, "filled")
        self.assertLess(result.filled_notional, 100.0)
        self.assertAlmostEqual(result.filled_notional, 30.0)
        self.assertEqual(result.reason, "partial_fill")

    def test_zero_liquidity_results_in_rejection(self):
        executor = SimulatedExecutor(config=_make_config(latency_seconds=0.0))
        result = executor.execute(_make_intent(notional=100.0), _make_snapshot(liquidity=0.0), _empty_history())
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.filled_notional, 0.0)

    def test_fees_paid_proportional_to_fill(self):
        # fee_rate=0.01, liquidity=2000 → available=300, fill=100 → fees=1.0
        executor = SimulatedExecutor(config=_make_config(fee_rate=0.01, latency_seconds=0.0))
        result = executor.execute(_make_intent(notional=100.0), _make_snapshot(liquidity=2000.0), _empty_history())
        self.assertAlmostEqual(result.fees_paid, result.filled_notional * 0.01, places=5)

    def test_buy_no_uses_complement_price(self):
        # p_market=0.60 → base_price for NO = 1 - 0.60 = 0.40
        executor = SimulatedExecutor(config=_make_config(slippage_bps=0.0, latency_seconds=0.0))
        result = executor.execute(
            _make_intent(action="BUY_NO", side="NO", notional=100.0),
            _make_snapshot(p_market=0.60, liquidity=2000.0),
            _empty_history(),
        )
        self.assertEqual(result.status, "filled")
        self.assertAlmostEqual(result.fill_price, 0.40, places=3)

    def test_velocity_adjusts_fill_price(self):
        # Rising market → positive velocity → higher fill price for YES
        executor = SimulatedExecutor(config=_make_config(slippage_bps=0.0, latency_seconds=1.0, price_velocity_lookback_minutes=5))
        ts = NOW
        history = pd.DataFrame([
            {"timestamp": ts - timedelta(minutes=2), "market_id": "m1", "p_market": 0.48, "volume": 100.0, "liquidity": 2000.0},
            {"timestamp": ts, "market_id": "m1", "p_market": 0.50, "volume": 100.0, "liquidity": 2000.0},
        ])
        result = executor.execute(_make_intent(notional=100.0), _make_snapshot(p_market=0.50, liquidity=2000.0), history)
        # velocity = (0.50 - 0.48) / 120 = 0.000167/s, latency_penalty = 1.0 * 0.000167
        self.assertGreater(result.fill_price, 0.50)


if __name__ == "__main__":
    unittest.main()
