"use client";

import { useState } from "react";
import { useStore } from "@/lib/store";
import { cn, fmt, fmtDollar, signalBg, edgeColor } from "@/lib/utils";
import { AlertTriangle, CheckCircle, TrendingUp } from "lucide-react";

const INITIAL_CAPITAL = 10_000;
const POSITION_SIZE_MULT = 0.10;
const EDGE_THRESHOLD = 0.005;
const FEE_RATE = 0.01;
const SLIPPAGE_BPS = 40 / 10_000;

export function QuickTradePanel() {
  const { selectedMarketId, signals, portfolio } = useStore();
  const [executed, setExecuted] = useState(false);

  const sig = signals.find((s) => s.market_id === selectedMarketId);
  const cash = portfolio?.cash ?? INITIAL_CAPITAL;

  if (!sig || sig.signal === "HOLD") {
    return (
      <div className="flex flex-col items-center justify-center gap-2 py-6 text-center">
        <div className="w-8 h-8 rounded-lg bg-bg-elevated flex items-center justify-center text-text-muted">
          <TrendingUp size={16} />
        </div>
        <div className="text-text-secondary text-sm">
          {sig ? "No actionable signal" : "Select a market"}
        </div>
        {sig && (
          <div className="text-2xs text-text-muted">
            Edge {fmt(sig.edge * 100, 2)}pp · below threshold
          </div>
        )}
      </div>
    );
  }

  const rawSize = cash * POSITION_SIZE_MULT;
  const kellyFraction = Math.abs(sig.edge) / (1 - Math.min(sig.p_market, 0.99));
  const suggestedSize = Math.min(rawSize, cash * 0.15 * Math.min(kellyFraction * 0.5, 1));
  const netCost = suggestedSize * (1 + FEE_RATE + SLIPPAGE_BPS);
  const expectedPayout = suggestedSize * (1 + sig.ev);
  const expectedProfit = expectedPayout - netCost;
  const isRisky = Math.abs(sig.edge) < 0.02 || suggestedSize > cash * 0.12;

  return (
    <div className="flex flex-col gap-3 animate-fade-in">
      {/* Signal summary */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-text-secondary">Suggested trade</span>
        <span className={cn("text-2xs font-mono font-semibold px-2 py-0.5 rounded uppercase", signalBg(sig.signal))}>
          {sig.signal.replace("_", " ")}
        </span>
      </div>

      {/* Trade details */}
      <div className="bg-bg-elevated border border-border rounded-lg divide-y divide-border-subtle">
        {[
          { label: "Market", value: sig.market_id, mono: true },
          { label: "Position size", value: fmtDollar(suggestedSize), color: "text-text-primary" },
          { label: "Net cost (fees + slippage)", value: fmtDollar(netCost), color: "text-text-secondary" },
          { label: "Expected profit", value: fmtDollar(expectedProfit), color: expectedProfit > 0 ? "text-green" : "text-red" },
          { label: "Edge", value: `${fmt(sig.edge * 100, 2)}pp`, color: edgeColor(sig.edge) },
          { label: "EV", value: fmt(sig.ev, 4), color: sig.ev > 0 ? "text-green" : "text-text-secondary" },
        ].map(({ label, value, mono, color }) => (
          <div key={label} className="flex justify-between items-center px-3 py-2">
            <span className="text-2xs text-text-secondary">{label}</span>
            <span className={cn("text-xs tabular-num", mono ? "font-mono text-text-primary" : color ?? "text-text-primary")}>{value}</span>
          </div>
        ))}
      </div>

      {/* Risk warning */}
      {isRisky && (
        <div className="flex items-start gap-2 bg-yellow/5 border border-yellow/20 rounded p-2.5">
          <AlertTriangle size={12} className="text-yellow mt-0.5 shrink-0" />
          <span className="text-2xs text-yellow/90">
            {Math.abs(sig.edge) < 0.02
              ? "Thin edge — model confidence close to market price"
              : "Large position relative to available cash"}
          </span>
        </div>
      )}

      {/* Execute button */}
      {executed ? (
        <div className="flex items-center justify-center gap-2 py-2.5 rounded-lg bg-green/10 border border-green/30 text-green text-sm">
          <CheckCircle size={14} />
          <span className="font-medium">Order submitted (sim)</span>
        </div>
      ) : (
        <button
          onClick={() => setExecuted(true)}
          className={cn(
            "w-full py-2.5 rounded-lg text-sm font-semibold transition-all",
            sig.signal === "BUY_YES"
              ? "bg-green text-bg hover:bg-green-500 active:scale-[0.99]"
              : "bg-red text-white hover:bg-red-500 active:scale-[0.99]"
          )}
        >
          {sig.signal === "BUY_YES" ? "Buy YES" : "Buy NO"} · {fmtDollar(suggestedSize)}
        </button>
      )}

      <p className="text-2xs text-text-muted text-center">
        Simulated execution · not connected to live exchange
      </p>
    </div>
  );
}
