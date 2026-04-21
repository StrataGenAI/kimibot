"use client";

import { useStore } from "@/lib/store";
import { useSignalPolling } from "@/lib/hooks";
import { cn, fmt, fmtEdge, signalBg, edgeColor } from "@/lib/utils";
import { TableSkeleton } from "@/components/Skeleton";
import type { Signal } from "@/lib/types";
import { AlertTriangle } from "lucide-react";

function SignalBadge({ signal }: { signal: Signal["signal"] }) {
  return (
    <span className={cn("inline-flex items-center px-1.5 py-0.5 rounded text-2xs font-mono font-semibold uppercase tracking-wide", signalBg(signal))}>
      {signal === "BUY_YES" ? "BUY YES" : signal === "BUY_NO" ? "BUY NO" : "HOLD"}
    </span>
  );
}

export function SignalTable() {
  useSignalPolling();

  const { signals, signalsLoading, signalsError, selectedMarketId, setSelectedMarket, changedFields } = useStore();

  if (signalsError) {
    return (
      <div className="flex items-center gap-2 p-4 text-red text-sm bg-red/5 border border-red/20 rounded-lg">
        <AlertTriangle size={14} />
        <span>Signal feed error: {signalsError}</span>
      </div>
    );
  }

  if (signalsLoading && signals.length === 0) {
    return <TableSkeleton rows={8} />;
  }

  if (signals.length === 0) {
    return (
      <div className="p-4 text-text-secondary text-sm text-center">
        No signals available. Run inference to generate predictions.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium">Market</th>
            <th className="text-right px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium"
                data-tooltip="Model's calibrated probability of YES resolution">
              p_model
            </th>
            <th className="text-right px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium"
                data-tooltip="Current market-implied probability from orderbook">
              p_market
            </th>
            <th className="text-right px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium"
                data-tooltip="Edge = p_model − p_market. Positive = model thinks YES is underpriced">
              Edge
            </th>
            <th className="text-right px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium"
                data-tooltip="Expected Value per unit traded">
              EV
            </th>
            <th className="text-right px-3 py-2 text-2xs text-text-secondary uppercase tracking-wider font-medium">Signal</th>
          </tr>
        </thead>
        <tbody>
          {signals.map((sig) => {
            const isSelected = sig.market_id === selectedMarketId;
            const pMarketFlash = changedFields.has(`${sig.market_id}-p_market`);
            const edgeFlash = changedFields.has(`${sig.market_id}-edge`);
            const signalFlash = changedFields.has(`${sig.market_id}-signal`);
            const edgePosKey = sig.edge >= 0 ? "flash-green" : "flash-red";

            return (
              <tr
                key={sig.market_id}
                onClick={() => setSelectedMarket(sig.market_id)}
                className={cn(
                  "border-b border-border-subtle cursor-pointer transition-colors",
                  isSelected
                    ? "bg-green/5 border-l-2 border-l-green"
                    : "hover:bg-bg-elevated"
                )}
              >
                <td className="px-3 py-2.5">
                  <div className="font-mono text-xs text-text-primary font-medium">{sig.market_id}</div>
                  <div className="text-2xs text-text-secondary truncate max-w-[180px]">{sig.question}</div>
                </td>
                <td className="px-3 py-2.5 text-right">
                  <span className="tabular-num text-xs text-text-primary">{fmt(sig.p_model, 3)}</span>
                </td>
                <td className={cn("px-3 py-2.5 text-right", pMarketFlash && edgePosKey)}>
                  <span className="tabular-num text-xs text-text-primary">{fmt(sig.p_market, 3)}</span>
                </td>
                <td className={cn("px-3 py-2.5 text-right", edgeFlash && edgePosKey)}>
                  <span className={cn("tabular-num text-xs font-semibold", edgeColor(sig.edge))}>
                    {fmtEdge(sig.edge)}
                  </span>
                </td>
                <td className="px-3 py-2.5 text-right">
                  <span className={cn("tabular-num text-xs", sig.ev > 0.005 ? "text-green" : "text-text-secondary")}>
                    {fmt(sig.ev, 3)}
                  </span>
                </td>
                <td className={cn("px-3 py-2.5 text-right", signalFlash && "flash-green")}>
                  <SignalBadge signal={sig.signal} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
