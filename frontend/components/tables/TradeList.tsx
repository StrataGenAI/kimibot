"use client";

import type { TradeRow } from "@/lib/types";
import { cn, fmt, fmtDollar, pnlColor } from "@/lib/utils";

interface Props { trades: TradeRow[]; maxRows?: number }

export function TradeList({ trades, maxRows = 50 }: Props) {
  const displayed = trades.slice(-maxRows).reverse();

  if (displayed.length === 0) {
    return <div className="p-4 text-text-secondary text-sm text-center">No trades recorded</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-border">
            {["Time", "Market", "Event", "Action", "Side", "Fill", "Notional", "PnL", "Edge"].map((h) => (
              <th key={h} className="text-left px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayed.map((t, i) => (
            <tr key={i} className="border-b border-border-subtle hover:bg-bg-elevated transition-colors">
              <td className="px-3 py-2 tabular-num text-2xs text-text-secondary whitespace-nowrap">
                {new Date(t.timestamp).toLocaleString("en", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}
              </td>
              <td className="px-3 py-2 font-mono text-xs text-text-primary">{t.market_id}</td>
              <td className="px-3 py-2 text-xs text-text-secondary">{t.event}</td>
              <td className="px-3 py-2">
                <span className={cn(
                  "text-2xs font-mono px-1.5 py-0.5 rounded uppercase font-semibold",
                  t.action === "OPEN" ? "bg-blue/10 text-blue-400" :
                  t.action === "CLOSE" ? "bg-text-muted/20 text-text-secondary" :
                  t.action === "HOLD" ? "bg-text-muted/10 text-text-muted" :
                  "bg-text-muted/10 text-text-secondary"
                )}>
                  {t.action}
                </span>
              </td>
              <td className="px-3 py-2">
                {t.side && (
                  <span className={cn(
                    "text-2xs font-mono px-1.5 py-0.5 rounded uppercase font-semibold",
                    t.side === "YES" ? "text-green" : "text-red"
                  )}>
                    {t.side}
                  </span>
                )}
              </td>
              <td className="px-3 py-2 tabular-num text-xs">{t.fill_price != null ? fmt(t.fill_price, 3) : "—"}</td>
              <td className="px-3 py-2 tabular-num text-xs">{t.filled_notional > 0 ? fmtDollar(t.filled_notional) : "—"}</td>
              <td className={cn("px-3 py-2 tabular-num text-xs font-semibold", t.realized_pnl != null ? pnlColor(t.realized_pnl) : "text-text-secondary")}>
                {t.realized_pnl != null ? fmtDollar(t.realized_pnl) : "—"}
              </td>
              <td className={cn("px-3 py-2 tabular-num text-xs", t.edge_entry >= 0.005 ? "text-green" : t.edge_entry < -0.005 ? "text-red" : "text-text-secondary")}>
                {fmt(t.edge_entry * 100, 2)}pp
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
