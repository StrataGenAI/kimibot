"use client";

import { SignalTable } from "@/components/tables/SignalTable";
import { MarketDetail } from "@/components/MarketDetail";
import { QuickTradePanel } from "@/components/QuickTradePanel";
import { useStore } from "@/lib/store";
import { useSignalPolling, usePortfolioPolling } from "@/lib/hooks";
import { fmtDollar, pnlColor, cn } from "@/lib/utils";

function SectionHeader({ title, sub }: { title: string; sub?: string }) {
  return (
    <div className="flex items-baseline gap-2 mb-3">
      <h2 className="text-md font-semibold text-text-primary">{title}</h2>
      {sub && <span className="text-xs text-text-secondary">{sub}</span>}
    </div>
  );
}

function Panel({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={cn("bg-bg-surface border border-border rounded-xl overflow-hidden", className)}>
      {children}
    </div>
  );
}

export default function DashboardPage() {
  useSignalPolling();
  usePortfolioPolling();

  const { signals, portfolio } = useStore();

  const buySignals = signals.filter((s) => s.signal !== "HOLD").length;
  const totalPnl = (portfolio?.realized_pnl ?? 0) + (portfolio?.unrealized_pnl ?? 0);

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* Summary strip */}
      <div className="flex items-center gap-6 px-1">
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider">Signals</div>
          <div className="font-mono font-semibold text-lg text-text-primary">{signals.length}</div>
        </div>
        <div className="w-px h-8 bg-border" />
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider">Actionable</div>
          <div className={cn("font-mono font-semibold text-lg", buySignals > 0 ? "text-green" : "text-text-secondary")}>{buySignals}</div>
        </div>
        <div className="w-px h-8 bg-border" />
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider">Total PnL</div>
          <div className={cn("font-mono font-semibold text-lg tabular-num", pnlColor(totalPnl))}>
            {fmtDollar(totalPnl)}
          </div>
        </div>
        {portfolio && (
          <>
            <div className="w-px h-8 bg-border" />
            <div>
              <div className="text-2xs text-text-secondary uppercase tracking-wider">Exposure</div>
              <div className="font-mono font-semibold text-lg tabular-num text-text-primary">
                {fmtDollar(portfolio.gross_exposure)}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Main grid: signal table (left) + detail + trade (right) */}
      <div className="flex gap-4 flex-1 min-h-0">
        {/* Signal Panel */}
        <Panel className="flex-1 min-w-0 flex flex-col">
          <div className="px-4 py-3 border-b border-border flex items-center justify-between">
            <SectionHeader title="Signals" sub={`${signals.length} markets`} />
            <span className="text-2xs font-mono text-text-muted">polling every 3s</span>
          </div>
          <div className="flex-1 overflow-auto">
            <SignalTable />
          </div>
        </Panel>

        {/* Right column: detail + trade */}
        <div className="flex flex-col gap-4 w-80 xl:w-96 shrink-0">
          {/* Market Detail */}
          <Panel className="flex-1 min-h-0 p-4 flex flex-col">
            <MarketDetail />
          </Panel>

          {/* Quick Trade */}
          <Panel className="p-4">
            <div className="text-md font-semibold text-text-primary mb-3">Quick Trade</div>
            <QuickTradePanel />
          </Panel>
        </div>
      </div>
    </div>
  );
}
