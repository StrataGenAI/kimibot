"use client";

import { useState, useEffect } from "react";
import { useAnalyticsPolling } from "@/lib/hooks";
import { useStore } from "@/lib/store";
import { MetricCard } from "@/components/cards/MetricCard";
import { MetricCardSkeleton } from "@/components/Skeleton";
import { EquityCurve } from "@/components/charts/EquityCurve";
import { DrawdownChart } from "@/components/charts/DrawdownChart";
import { EdgeHistogram } from "@/components/charts/EdgeHistogram";
import { TradeList } from "@/components/tables/TradeList";
import { fmtPct, fmt } from "@/lib/utils";
import { AlertTriangle, CheckCircle } from "lucide-react";
import type { WalkForwardData, ReliabilityPoint } from "@/lib/types";

function ReliabilityDiagram({ data }: { data: ReliabilityPoint[] }) {
  if (!data || data.length === 0) return <p className="text-text-secondary text-sm">No reliability data.</p>;
  const maxCount = Math.max(...data.map(d => d.count));
  return (
    <div className="relative h-48 w-full">
      <svg viewBox="0 0 200 200" className="w-full h-full">
        <line x1="20" y1="180" x2="180" y2="20" stroke="#4b5563" strokeWidth="1" strokeDasharray="4,4" />
        <line x1="20" y1="180" x2="180" y2="180" stroke="#374151" strokeWidth="1" />
        <line x1="20" y1="20" x2="20" y2="180" stroke="#374151" strokeWidth="1" />
        {data.map((d, i) => {
          const cx = 20 + d.mean_pred * 160;
          const cy = 180 - d.fraction_positive * 160;
          const r = Math.max(3, (d.count / maxCount) * 10);
          return (
            <circle key={i} cx={cx} cy={cy} r={r} fill="#2563eb" opacity={0.75}>
              <title>{`Pred: ${d.mean_pred.toFixed(2)}, Actual: ${d.fraction_positive.toFixed(2)}, n=${d.count}`}</title>
            </circle>
          );
        })}
        <text x="100" y="198" textAnchor="middle" fontSize="8" fill="#9ca3af">Mean Predicted Prob</text>
        <text x="10" y="100" textAnchor="middle" fontSize="8" fill="#9ca3af" transform="rotate(-90,10,100)">Fraction Positive</text>
      </svg>
    </div>
  );
}

function WalkForwardPanel() {
  const [data, setData] = useState<WalkForwardData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/walk-forward")
      .then(r => r.json())
      .then(d => {
        if (d.error) setError(d.error);
        else setData(d);
      })
      .catch(() => setError("Failed to load walk-forward results"))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="skeleton h-32 rounded-xl" />;
  if (error) return (
    <div className="bg-bg-surface border border-border rounded-xl p-4 text-text-secondary text-sm">
      Walk-Forward Evaluation not yet run. Execute: <code className="font-mono text-xs">python main.py evaluate-limitless</code>
    </div>
  );
  if (!data) return null;

  const { headline, model, market_baseline, dataset } = data;
  const beatsBadge = headline.model_beats_market
    ? <span className="text-green text-xs font-semibold">Beats Market</span>
    : <span className="text-red text-xs font-semibold">Loses to Market</span>;

  return (
    <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <div>
          <h3 className="text-md font-semibold text-text-primary">Walk-Forward Evaluation (Real Limitless Data)</h3>
          <p className="text-2xs text-text-secondary mt-0.5">
            {dataset.test_markets} test markets · {dataset.test_snapshots.toLocaleString()} snapshots · {data.dataset.test_date_range[0].slice(0,10)} – {data.dataset.test_date_range[1].slice(0,10)}
          </p>
        </div>
        {beatsBadge}
      </div>
      <div className="p-4 grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Model Brier</div>
          <div className="font-mono font-semibold text-text-primary">{model.brier_score.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">CI [{model.brier_ci_95[0].toFixed(4)}, {model.brier_ci_95[1].toFixed(4)}]</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Market Brier</div>
          <div className="font-mono font-semibold text-text-primary">{market_baseline.brier_score.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">CI [{market_baseline.brier_ci_95[0].toFixed(4)}, {market_baseline.brier_ci_95[1].toFixed(4)}]</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Delta vs Market</div>
          <div className={`font-mono font-semibold ${headline.delta_brier_vs_market > 0 ? "text-green" : "text-red"}`}>
            {headline.delta_brier_vs_market > 0 ? "+" : ""}{headline.delta_brier_vs_market.toFixed(4)}
          </div>
          <div className="text-2xs text-text-secondary">Brier improvement</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Model AUC</div>
          <div className="font-mono font-semibold text-text-primary">{model.auc.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">ECE: {model.ece.toFixed(4)}</div>
        </div>
      </div>
      <div className="px-4 pb-4">
        <div className="text-2xs text-text-secondary mb-2">Reliability Diagram (bubble size = sample count)</div>
        <ReliabilityDiagram data={data.diagnostics.reliability_diagram_data} />
      </div>
      <div className="px-4 pb-4 text-2xs text-text-secondary">
        Train: {dataset.train_markets} markets · Calibrate: {dataset.calib_markets} markets · Test: {dataset.test_markets} markets
        · Run ID: {data.run_id}
      </div>
    </div>
  );
}

export default function AnalyticsPage() {
  useAnalyticsPolling();
  const { analytics, analyticsLoading, analyticsError } = useStore();

  const m = analytics?.metrics;

  return (
    <div className="flex flex-col gap-5">
      <div>
        <h1 className="text-2xl font-semibold text-text-primary">Analytics</h1>
        <p className="text-sm text-text-secondary mt-0.5">Backtest performance and model evaluation</p>
      </div>

      {analyticsError && (
        <div className="flex items-center gap-2 p-3 text-red text-sm bg-red/5 border border-red/20 rounded-lg">
          <AlertTriangle size={14} />
          <span>{analyticsError}</span>
        </div>
      )}

      {/* Validation warnings */}
      {m?.validation_warnings && m.validation_warnings.length > 0 && (
        <div className="space-y-1.5">
          {(m.validation_warnings as string[]).map((w, i) => (
            <div key={i} className="flex items-start gap-2 p-2.5 text-yellow text-xs bg-yellow/5 border border-yellow/20 rounded-lg">
              <AlertTriangle size={12} className="shrink-0 mt-0.5" />
              <span>{w}</span>
            </div>
          ))}
        </div>
      )}

      {m?.results_valid && (
        <div className="flex items-center gap-2 p-2.5 text-green text-xs bg-green/5 border border-green/20 rounded-lg">
          <CheckCircle size={12} />
          <span>Backtest results are valid ({m.trade_count} trades meet minimum requirements)</span>
        </div>
      )}

      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-3">
        {analyticsLoading && !analytics
          ? Array(5).fill(null).map((_, i) => <MetricCardSkeleton key={i} />)
          : m && (
            <>
              <MetricCard
                label="Total Return"
                value={fmtPct(m.total_return)}
                trend={m.total_return >= 0 ? "up" : "down"}
                tooltip="Total return over the backtest period"
              />
              <MetricCard
                label="Sharpe Ratio"
                value={fmt(m.sharpe_ratio, 2)}
                trend={m.sharpe_ratio >= 1 ? "up" : m.sharpe_ratio >= 0 ? "neutral" : "down"}
                tooltip="Risk-adjusted return. >1 is good, >2 is excellent"
              />
              <MetricCard
                label="Win Rate"
                value={fmtPct(m.win_rate)}
                trend={m.win_rate >= 0.55 ? "up" : "down"}
                tooltip="Fraction of closed trades with positive PnL"
              />
              <MetricCard
                label="Max Drawdown"
                value={fmtPct(m.max_drawdown)}
                trend="down"
                tooltip="Largest peak-to-trough decline"
              />
              <MetricCard
                label="Trade Count"
                value={fmt(m.trade_count, 0)}
                tooltip="Total settled trades in backtest"
              />
            </>
          )}
      </div>

      {/* Secondary metrics */}
      {m && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <MetricCard
            label="Median PnL/Trade"
            value={`$${fmt(m.median_trade_pnl, 2)}`}
            trend={m.median_trade_pnl >= 0 ? "up" : "down"}
          />
          <MetricCard
            label="Brier Score"
            value={fmt(m.brier_score, 4)}
            tooltip="Calibration score (lower = better, 0 = perfect)"
          />
          <MetricCard
            label="ECE"
            value={fmt(m.expected_calibration_error, 4)}
            tooltip="Expected Calibration Error. <0.05 is well-calibrated"
          />
          <MetricCard
            label="Top Trade Concentration"
            value={fmtPct(m.top_trade_pnl_share)}
            trend={m.top_trade_pnl_share > 0.3 ? "down" : "neutral"}
            tooltip="Fraction of total profit from the single best trade"
          />
        </div>
      )}

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-bg-surface border border-border rounded-xl p-4">
          <h3 className="text-sm font-semibold text-text-primary mb-3">Equity Curve</h3>
          {analyticsLoading && !analytics
            ? <div className="skeleton h-52 rounded-lg" />
            : <EquityCurve data={analytics?.equity_curve ?? []} height={210} />}
        </div>
        <div className="bg-bg-surface border border-border rounded-xl p-4">
          <h3 className="text-sm font-semibold text-text-primary mb-3">Drawdown</h3>
          {analyticsLoading && !analytics
            ? <div className="skeleton h-52 rounded-lg" />
            : <DrawdownChart data={analytics?.drawdown_curve ?? []} height={210} />}
        </div>
      </div>

      {/* Edge histogram */}
      {m?.edge_bucket_report && (m.edge_bucket_report as unknown[]).length > 0 && (
        <div className="bg-bg-surface border border-border rounded-xl p-4">
          <h3 className="text-sm font-semibold text-text-primary mb-1">Edge Distribution</h3>
          <p className="text-2xs text-text-secondary mb-3">Trade count by edge quartile</p>
          <EdgeHistogram data={m.edge_bucket_report as Parameters<typeof EdgeHistogram>[0]["data"]} height={180} />
        </div>
      )}

      {/* Trade list */}
      <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
        <div className="px-4 py-3 border-b border-border">
          <h3 className="text-md font-semibold text-text-primary">Trade Log</h3>
          <p className="text-2xs text-text-secondary mt-0.5">Last 50 executions and settlements</p>
        </div>
        <TradeList trades={analytics?.trades ?? []} maxRows={50} />
      </div>

      {/* Walk-Forward Evaluation */}
      <WalkForwardPanel />
    </div>
  );
}
