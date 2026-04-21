"use client";

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
    </div>
  );
}
