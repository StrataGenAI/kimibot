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
  ingestion_status: Record<string, unknown> | null;
  brier_score: number | null;
  ece: number | null;
}

export interface WalkForwardMarketSet {
  train_markets: number;
  calib_markets: number;
  test_markets: number;
  train_snapshots: number;
  test_snapshots: number;
  train_date_range: [string, string];
  test_date_range: [string, string];
  total_markets: number;
}

export interface WalkForwardModel {
  brier_score: number;
  brier_ci_95: [number, number];
  ece: number;
  log_loss: number;
  auc: number;
  accuracy_at_0_5: number;
}

export interface WalkForwardBaseline {
  brier_score: number;
  brier_ci_95: [number, number];
  ece: number;
  log_loss: number;
  auc: number;
}

export interface WalkForwardHeadline {
  delta_brier_vs_market: number;
  model_beats_market: boolean;
  model_beats_trivial: boolean;
}

export interface ReliabilityPoint {
  bin_center: number;
  mean_pred: number;
  fraction_positive: number;
  count: number;
}

export interface WalkForwardData {
  generated_at: string;
  run_id: string;
  dataset: WalkForwardMarketSet;
  model: WalkForwardModel;
  market_baseline: WalkForwardBaseline;
  trivial_baseline: { brier_score: number };
  headline: WalkForwardHeadline;
  diagnostics: {
    reliability_diagram_data: ReliabilityPoint[];
    top_10_disagreements: unknown[];
    top_10_confident_wrong: unknown[];
  };
}
