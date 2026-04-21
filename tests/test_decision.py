"""Unit tests for the decision engine."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from decision.engine import DecisionEngine
from project.configuration import TradingConfig
from project.types import PortfolioState, Prediction


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


def _make_prediction(*, market_id="m1", p_model=0.60, p_market=0.50) -> Prediction:
    return Prediction(
        market_id=market_id,
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        p_model_raw=p_model,
        p_model_calibrated=p_model,
        p_market=p_market,
    )


def _make_portfolio(*, cash=10000.0) -> PortfolioState:
    return PortfolioState(cash=cash)


NOW = datetime(2026, 1, 1, 12, 0, 0)


class DecisionEngineTests(unittest.TestCase):

    def test_hold_when_liquidity_too_low(self):
        engine = DecisionEngine(config=_make_config(min_liquidity=1000.0))
        intent = engine.evaluate(_make_prediction(), liquidity=499.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "HOLD")
        self.assertEqual(intent.reason, "liquidity_below_threshold")

    def test_hold_when_edge_below_threshold(self):
        # p_model = 0.52, p_market = 0.50 → edge_yes = 0.02, below edge_threshold=0.03
        engine = DecisionEngine(config=_make_config(edge_threshold=0.03))
        intent = engine.evaluate(_make_prediction(p_model=0.52, p_market=0.50), liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "HOLD")
        self.assertEqual(intent.reason, "edge_or_ev_below_threshold")

    def test_hold_when_ev_negative(self):
        # edge_yes = 0.05, but fee+slippage=0.014 so ev>0 is fine
        # To get negative EV: edge < cost → use very high fees
        engine = DecisionEngine(config=_make_config(fee_rate=0.10, slippage_bps=400.0, edge_threshold=0.03, min_expected_value=0.01))
        # edge_yes=0.05, cost=0.10+0.04=0.14, ev_yes=0.05-0.50-0.14 → negative
        intent = engine.evaluate(_make_prediction(p_model=0.55, p_market=0.50), liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "HOLD")

    def test_buy_yes_when_model_above_market(self):
        # edge_yes = 0.70 - 0.50 = 0.20 — too high, hits max_edge=0.15
        # Use edge within bounds: p_model=0.60, p_market=0.50 → edge=0.10
        engine = DecisionEngine(config=_make_config())
        intent = engine.evaluate(_make_prediction(p_model=0.60, p_market=0.50), liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "BUY_YES")
        self.assertEqual(intent.side, "YES")
        self.assertGreater(intent.edge, 0)

    def test_buy_no_when_model_below_market(self):
        # p_model=0.40, p_market=0.50 → edge_no = (1-0.40)-(1-0.50) = 0.10, within (0.02, 0.15)
        engine = DecisionEngine(config=_make_config())
        intent = engine.evaluate(_make_prediction(p_model=0.40, p_market=0.50), liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "BUY_NO")
        self.assertEqual(intent.side, "NO")
        self.assertGreater(intent.edge, 0)

    def test_cooldown_blocks_repeat_trade(self):
        engine = DecisionEngine(config=_make_config(trade_cooldown_seconds=120))
        pred = _make_prediction(p_model=0.60, p_market=0.50)
        first = engine.evaluate(pred, liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(first.action, "BUY_YES")
        # Same market 30s later — within 120s cooldown
        second = engine.evaluate(pred, liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW + timedelta(seconds=30))
        self.assertEqual(second.action, "HOLD")
        self.assertEqual(second.reason, "cooldown_active")

    def test_cooldown_allows_trade_after_expiry(self):
        engine = DecisionEngine(config=_make_config(trade_cooldown_seconds=60))
        pred = _make_prediction(p_model=0.60, p_market=0.50)
        engine.evaluate(pred, liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        # 90s later — cooldown expired
        second = engine.evaluate(pred, liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW + timedelta(seconds=90))
        self.assertEqual(second.action, "BUY_YES")

    def test_max_edge_rejects_overconfident(self):
        # edge_yes = 0.80 - 0.50 = 0.30 → exceeds max_edge=0.15
        engine = DecisionEngine(config=_make_config(edge_threshold=0.03, max_edge=0.15))
        intent = engine.evaluate(_make_prediction(p_model=0.80, p_market=0.50), liquidity=1000.0, portfolio_state=_make_portfolio(), current_time=NOW)
        self.assertEqual(intent.action, "HOLD")
        self.assertIn("edge_out_of_bounds", intent.reason)

    def test_position_sizing_uses_cash_multiplier(self):
        engine = DecisionEngine(config=_make_config(position_size_multiplier=0.10, max_position_per_market=800.0))
        portfolio = _make_portfolio(cash=5000.0)
        intent = engine.evaluate(_make_prediction(p_model=0.60, p_market=0.50), liquidity=1000.0, portfolio_state=portfolio, current_time=NOW)
        self.assertEqual(intent.action, "BUY_YES")
        # 5000 * 0.10 = 500, capped at max_position_per_market=800
        self.assertAlmostEqual(intent.requested_notional, 500.0)


if __name__ == "__main__":
    unittest.main()
