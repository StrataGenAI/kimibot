export type SignalType = "BUY_YES" | "BUY_NO" | "HOLD";

export interface Signal {
  market_id: string;
  question: string;
  p_model: number;
  p_market: number;
  edge: number;
  ev: number;
  signal: SignalType;
  timestamp: string;
  resolution_time: string | null;
  volume: number;
  liquidity: number;
}

export interface MarketHistory {
  timestamp: string;
  p_market: number;
  p_model: number | null;
  volume: number;
  liquidity: number;
}

export interface Position {
  position_id: string;
  market_id: string;
  side: string;
  quantity: number;
  average_price: number;
  cost_basis: number;
  entry_timestamp: string;
  entry_edge: number;
  entry_ev: number;
  realized_pnl: number;
  current_price: number;
  unrealized_pnl: number;
  resolution_time: string;
}

export interface PortfolioData {
  cash: number;
  initial_capital: number;
  realized_pnl: number;
  unrealized_pnl: number;
  gross_exposure: number;
  total_equity: number;
  positions: Position[];
}

export interface TradeRow {
  market_id: string;
  timestamp: string;
  event: string;
  action: string;
  side: string;
  fill_price: number | null;
  filled_notional: number;
  realized_pnl: number | null;
  edge_entry: number;
  ev_entry: number;
  reason: string;
}

export interface EquityPoint {
  timestamp: string;
  equity: number;
  pnl: number;
}

export interface AnalyticsData {
  metrics: {
    total_return: number;
    sharpe_ratio: number;
    max_drawdown: number;
    win_rate: number;
    trade_count: number;
    median_trade_pnl: number;
    top_trade_pnl_share: number;
    profitable_trade_fraction: number;
    brier_score: number;
    expected_calibration_error: number;
    results_valid: boolean;
    validation_warnings: string[];
    edge_bucket_report: EdgeBucket[];
  };
  equity_curve: EquityPoint[];
  drawdown_curve: { timestamp: string; drawdown: number }[];
  trades: TradeRow[];
}

export interface EdgeBucket {
  bucket: string;
  count: number;
  mean_edge: number;
  mean_realized_pnl: number;
  win_rate: number;
}

export interface HealthData {
  model_loaded: boolean;
  last_prediction_time: string | null;
  last_snapshot_time: string | null;
  last_crypto_time: string | null;
  data_freshness_seconds: number | null;
  market_count: number;
  prediction_count: number;
  trade_count: number;
  scaler_loaded: boolean;
  calibrator_loaded: boolean;
  training_metadata: Record<string, unknown> | null;
}
