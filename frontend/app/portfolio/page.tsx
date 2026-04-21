"use client";

import { usePortfolioPolling } from "@/lib/hooks";
import { useStore } from "@/lib/store";
import { MetricCard } from "@/components/cards/MetricCard";
import { MetricCardSkeleton } from "@/components/Skeleton";
import { PositionsTable } from "@/components/tables/PositionsTable";
import { fmtDollar, fmtPct, pnlColor, cn } from "@/lib/utils";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";
import { AlertTriangle } from "lucide-react";

const EXPOSURE_COLORS = ["#00d4aa", "#3b82f6", "#f0b429", "#f05e6e", "#818cf8", "#34d399"];

export default function PortfolioPage() {
  usePortfolioPolling();
  const { portfolio, portfolioLoading, portfolioError } = useStore();

  if (portfolioError) {
    return (
      <div className="flex items-center gap-2 p-4 text-red text-sm bg-red/5 border border-red/20 rounded-lg">
        <AlertTriangle size={14} />
        <span>{portfolioError}</span>
      </div>
    );
  }

  const pnlTrend =
    portfolio && (portfolio.realized_pnl + portfolio.unrealized_pnl) >= 0 ? "up" : "down";

  const pieData = portfolio?.positions.map((p) => ({
    name: p.market_id,
    value: p.cost_basis,
  })) ?? [];

  return (
    <div className="flex flex-col gap-5">
      <div>
        <h1 className="text-2xl font-semibold text-text-primary">Portfolio</h1>
        <p className="text-sm text-text-secondary mt-0.5">Capital allocation and open positions</p>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {portfolioLoading && !portfolio
          ? Array(4).fill(null).map((_, i) => <MetricCardSkeleton key={i} />)
          : portfolio && (
            <>
              <MetricCard
                label="Total Equity"
                value={fmtDollar(portfolio.total_equity)}
                sub={`Initial $${portfolio.initial_capital.toLocaleString()}`}
                trend={portfolio.total_equity >= portfolio.initial_capital ? "up" : "down"}
              />
              <MetricCard
                label="Realized PnL"
                value={fmtDollar(portfolio.realized_pnl)}
                trend={portfolio.realized_pnl >= 0 ? "up" : "down"}
              />
              <MetricCard
                label="Unrealized PnL"
                value={fmtDollar(portfolio.unrealized_pnl)}
                trend={portfolio.unrealized_pnl >= 0 ? "up" : "down"}
              />
              <MetricCard
                label="Gross Exposure"
                value={fmtDollar(portfolio.gross_exposure)}
                sub={`${fmtPct(portfolio.gross_exposure / portfolio.total_equity)} of equity`}
              />
            </>
          )}
      </div>

      {/* Layout: positions + exposure chart */}
      <div className="flex gap-4">
        {/* Positions table */}
        <div className="flex-1 min-w-0 bg-bg-surface border border-border rounded-xl overflow-hidden">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-md font-semibold text-text-primary">Open Positions</h2>
          </div>
          <PositionsTable positions={portfolio?.positions ?? []} />
        </div>

        {/* Exposure breakdown */}
        {pieData.length > 0 && (
          <div className="w-64 shrink-0 bg-bg-surface border border-border rounded-xl p-4">
            <h3 className="text-sm font-semibold text-text-primary mb-3">Exposure by Market</h3>
            <ResponsiveContainer width="100%" height={160}>
              <PieChart>
                <Pie data={pieData} cx="50%" cy="50%" innerRadius={40} outerRadius={65} dataKey="value" paddingAngle={2}>
                  {pieData.map((_, i) => (
                    <Cell key={i} fill={EXPOSURE_COLORS[i % EXPOSURE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(v: number) => fmtDollar(v)}
                  contentStyle={{ background: "#16202e", border: "1px solid #1a2640", borderRadius: 6, fontSize: 11 }}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="space-y-1 mt-2">
              {pieData.slice(0, 6).map((d, i) => (
                <div key={d.name} className="flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <div className="w-2 h-2 rounded-full" style={{ background: EXPOSURE_COLORS[i % EXPOSURE_COLORS.length] }} />
                    <span className="text-2xs font-mono text-text-secondary">{d.name}</span>
                  </div>
                  <span className="text-2xs font-mono tabular-num text-text-primary">{fmtDollar(d.value)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
