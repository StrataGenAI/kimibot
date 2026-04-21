"use client";

import { useStore } from "@/lib/store";
import { relativeTime } from "@/lib/utils";
import { RefreshCw, Wifi, WifiOff } from "lucide-react";

export function TopBar() {
  const { lastSignalUpdate, signalsError, signalsLoading } = useStore();

  const isLive = !signalsError;
  const updatedAt = lastSignalUpdate ? relativeTime(new Date(lastSignalUpdate).toISOString()) : null;

  return (
    <header className="flex items-center justify-between px-5 py-2.5 border-b border-border bg-bg-surface shrink-0">
      {/* Mobile logo */}
      <div className="lg:hidden flex items-center gap-2">
        <div className="w-6 h-6 rounded bg-green/20 flex items-center justify-center">
          <span className="text-green font-mono font-bold text-xs">K</span>
        </div>
        <span className="font-semibold text-text-primary">KimiBot</span>
      </div>

      <div className="hidden lg:block" />

      {/* Status */}
      <div className="flex items-center gap-4">
        {signalsLoading && !lastSignalUpdate && (
          <div className="flex items-center gap-1.5 text-text-secondary text-xs">
            <RefreshCw size={11} className="animate-spin" />
            <span>Loading…</span>
          </div>
        )}
        {updatedAt && !signalsError && (
          <span className="text-text-secondary text-xs font-mono">Updated {updatedAt}</span>
        )}
        <div className="flex items-center gap-1.5">
          {isLive ? (
            <>
              <span className="w-1.5 h-1.5 rounded-full bg-green animate-pulse" />
              <span className="text-xs text-green font-medium">LIVE</span>
            </>
          ) : (
            <>
              <span className="w-1.5 h-1.5 rounded-full bg-red" />
              <span className="text-xs text-red">OFFLINE</span>
            </>
          )}
        </div>
      </div>
    </header>
  );
}
