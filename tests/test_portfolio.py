"""Unit tests for the portfolio manager."""

from __future__ import annotations

import unittest
from datetime import datetime

from portfolio.book import PortfolioManager
from project.configuration import TradingConfig
from project.types import ExecutionResult, PortfolioState, Prediction, TradeIntent


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


def _make_execution(*, filled=100.0, fill_price=0.55, side="YES", status="filled") -> ExecutionResult:
    return ExecutionResult(
        market_id="m1",
        timestamp=NOW,
        action="BUY_YES",
        side=side,
        requested_notional=100.0,
        filled_notional=filled,
        fill_price=fill_price,
        fees_paid=filled * 0.01,
        slippage_paid=0.50,
        status=status,
        reason="trade_candidate",
    )


def _make_intent(*, edge=0.10, ev=0.05) -> TradeIntent:
    return TradeIntent(
        market_id="m1",
        timestamp=NOW,
        action="BUY_YES",
        side="YES",
        requested_notional=100.0,
        expected_value=ev,
        edge=edge,
        reason="trade_candidate",
    )


def _make_prediction() -> Prediction:
    return Prediction(
        market_id="m1",
        timestamp=NOW,
        p_model_raw=0.60,
        p_model_calibrated=0.60,
        p_market=0.50,
    )


class PortfolioManagerTests(unittest.TestCase):

    def test_create_initial_state_has_full_cash(self):
        mgr = PortfolioManager(config=_make_config(initial_capital=5000.0))
        state = mgr.create_initial_state()
        self.assertEqual(state.cash, 5000.0)
        self.assertEqual(state.gross_exposure, 0.0)

    def test_can_accept_rejects_zero_notional(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        ok, reason = mgr.can_accept(state, "m1", 0.0, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "zero_notional")

    def test_can_accept_rejects_over_position_limit(self):
        mgr = PortfolioManager(config=_make_config(max_position_per_market=200.0))
        state = mgr.create_initial_state()
        # First fill to push close to limit
        mgr.apply_execution(state, _make_execution(filled=180.0, fill_price=0.55), _make_intent(), _make_prediction())
        ok, reason = mgr.can_accept(state, "m1", 100.0, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "max_position_per_market")

    def test_can_accept_rejects_over_total_exposure(self):
        mgr = PortfolioManager(config=_make_config(max_total_exposure=150.0))
        state = mgr.create_initial_state()
        state.gross_exposure = 120.0
        ok, reason = mgr.can_accept(state, "m2", 60.0, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "max_total_exposure")

    def test_can_accept_rejects_on_daily_loss_limit(self):
        mgr = PortfolioManager(config=_make_config(daily_loss_limit=100.0))
        state = mgr.create_initial_state()
        state.daily_realized_pnl["2026-01-01"] = -100.0
        ok, reason = mgr.can_accept(state, "m1", 50.0, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "daily_loss_limit")

    def test_can_accept_rejects_insufficient_cash(self):
        mgr = PortfolioManager(config=_make_config(initial_capital=50.0))
        state = mgr.create_initial_state()
        ok, reason = mgr.can_accept(state, "m1", 100.0, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "insufficient_cash")

    def test_apply_execution_updates_cash_and_exposure(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        execution = _make_execution(filled=100.0, fill_price=0.55)
        mgr.apply_execution(state, execution, _make_intent(), _make_prediction())
        # cash reduced by filled + fees
        self.assertAlmostEqual(state.cash, 10000.0 - 100.0 - 1.0)
        self.assertAlmostEqual(state.gross_exposure, 100.0)
        self.assertIn(("m1", "YES"), state.open_positions)

    def test_apply_execution_ignored_for_unfilled(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        mgr.apply_execution(state, _make_execution(filled=0.0, status="rejected", fill_price=None), _make_intent(), _make_prediction())
        self.assertEqual(state.cash, 10000.0)
        self.assertEqual(len(state.open_positions), 0)

    def test_settle_market_yes_win_produces_profit(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        execution = _make_execution(filled=100.0, fill_price=0.55)
        mgr.apply_execution(state, execution, _make_intent(), _make_prediction())
        # YES wins (outcome_yes=1) → payout_price=1.0
        settlements = mgr.settle_market(state, "m1", outcome_yes=1, timestamp=NOW)
        self.assertEqual(len(settlements), 1)
        s = settlements[0]
        self.assertEqual(s.side, "YES")
        self.assertEqual(s.payout_price, 1.0)
        self.assertGreater(s.payout, 0)

    def test_settle_market_yes_loss_produces_zero_payout(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        execution = _make_execution(filled=100.0, fill_price=0.55)
        mgr.apply_execution(state, execution, _make_intent(), _make_prediction())
        settlements = mgr.settle_market(state, "m1", outcome_yes=0, timestamp=NOW)
        self.assertEqual(len(settlements), 1)
        self.assertEqual(settlements[0].payout_price, 0.0)
        self.assertLess(settlements[0].realized_pnl, 0)

    def test_settle_market_no_win(self):
        mgr = PortfolioManager(config=_make_config())
        state = mgr.create_initial_state()
        # Buy NO side
        execution = _make_execution(filled=100.0, fill_price=0.45, side="NO")
        intent = TradeIntent(market_id="m1", timestamp=NOW, action="BUY_NO", side="NO",
                             requested_notional=100.0, expected_value=0.05, edge=0.10, reason="trade_candidate")
        mgr.apply_execution(state, execution, intent, _make_prediction())
        # NO wins when outcome_yes=0
        settlements = mgr.settle_market(state, "m1", outcome_yes=0, timestamp=NOW)
        no_settlement = next((s for s in settlements if s.side == "NO"), None)
        self.assertIsNotNone(no_settlement)
        self.assertEqual(no_settlement.payout_price, 1.0)


if __name__ == "__main__":
    unittest.main()
