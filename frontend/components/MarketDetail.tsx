"use client";

import { useStore } from "@/lib/store";
import { useMarketHistory } from "@/lib/hooks";
import { PriceChart } from "./charts/PriceChart";
import { VolumeChart } from "./charts/VolumeChart";
import { fmt, fmtEdge, edgeColor, signalBg, cn } from "@/lib/utils";
import { Skeleton } from "./Skeleton";

export function MarketDetail() {
  const { selectedMarketId, signals, marketHistory, marketHistoryLoading } = useStore();
  useMarketHistory(selectedMarketId);

  if (!selectedMarketId) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-text-secondary text-sm gap-2">
        <div className="w-8 h-8 rounded-lg bg-bg-elevated flex items-center justify-center text-text-muted text-lg">↑</div>
        <span>Click a market to view details</span>
      </div>
    );
  }

  const sig = signals.find((s) => s.market_id === selectedMarketId);

  return (
    <div className="flex flex-col gap-3 h-full animate-fade-in">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="font-mono text-md font-semibold text-text-primary">{selectedMarketId}</div>
          {sig?.resolution_time && (
            <div className="text-2xs text-text-secondary font-mono mt-0.5">
              Resolves {new Date(sig.resolution_time).toLocaleDateString()}
            </div>
          )}
        </div>
        {sig && (
          <span className={cn("text-2xs font-mono font-semibold px-2 py-1 rounded uppercase", signalBg(sig.signal))}>
            {sig.signal.replace("_", " ")}
          </span>
        )}
      </div>

      {/* Probability row */}
      {sig && (
        <div className="grid grid-cols-3 gap-2">
          <div className="bg-bg-card border border-border rounded p-2">
            <div className="text-2xs text-text-secondary">p_model</div>
            <div className="tabular-num text-md font-mono font-semibold text-blue-400">{fmt(sig.p_model, 3)}</div>
          </div>
          <div className="bg-bg-card border border-border rounded p-2">
            <div className="text-2xs text-text-secondary">p_market</div>
            <div className="tabular-num text-md font-mono font-semibold text-text-primary">{fmt(sig.p_market, 3)}</div>
          </div>
          <div className="bg-bg-card border border-border rounded p-2">
            <div className="text-2xs text-text-secondary">edge</div>
            <div className={cn("tabular-num text-md font-mono font-semibold", edgeColor(sig.edge))}>
              {fmtEdge(sig.edge)}
            </div>
          </div>
        </div>
      )}

      {/* Price chart */}
      <div className="flex-1 min-h-0">
        <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1.5">Price History</div>
        {marketHistoryLoading ? (
          <Skeleton className="w-full h-40" />
        ) : (
          <PriceChart data={marketHistory} height={180} />
        )}
      </div>

      {/* Volume chart */}
      {marketHistory.length > 0 && (
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1.5">Volume</div>
          <VolumeChart data={marketHistory} height={80} />
        </div>
      )}
    </div>
  );
}
