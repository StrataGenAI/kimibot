"use client";

import type { Position } from "@/lib/types";
import { cn, fmtDollar, fmt, pnlColor } from "@/lib/utils";

interface Props { positions: Position[] }

export function PositionsTable({ positions }: Props) {
  if (positions.length === 0) {
    return <div className="p-4 text-text-secondary text-sm text-center">No open positions</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-border">
            {["Market", "Side", "Size", "Entry", "Current", "Unreal. PnL", "Edge@Entry"].map((h) => (
              <th key={h} className="text-left px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium first:pl-3 last:pr-3">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => (
            <tr key={p.position_id} className="border-b border-border-subtle hover:bg-bg-elevated transition-colors">
              <td className="px-3 py-2.5 font-mono text-xs text-text-primary">{p.market_id}</td>
              <td className="px-3 py-2.5">
                <span className={cn(
                  "text-2xs font-mono font-semibold px-1.5 py-0.5 rounded uppercase",
                  p.side === "YES" ? "bg-green/10 text-green" : "bg-red/10 text-red"
                )}>
                  {p.side}
                </span>
              </td>
              <td className="px-3 py-2.5 tabular-num text-xs">{fmtDollar(p.cost_basis)}</td>
              <td className="px-3 py-2.5 tabular-num text-xs">{fmt(p.average_price, 3)}</td>
              <td className="px-3 py-2.5 tabular-num text-xs">{fmt(p.current_price, 3)}</td>
              <td className={cn("px-3 py-2.5 tabular-num text-xs font-semibold", pnlColor(p.unrealized_pnl))}>
                {fmtDollar(p.unrealized_pnl)}
              </td>
              <td className={cn("px-3 py-2.5 tabular-num text-xs", p.entry_edge >= 0 ? "text-green" : "text-red")}>
                {fmt(p.entry_edge * 100, 2)}pp
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
