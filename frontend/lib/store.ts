"use client";

import { create } from "zustand";
import type { Signal, PortfolioData, AnalyticsData, HealthData, MarketHistory } from "./types";

interface KimiBotStore {
  // Signals
  signals: Signal[];
  signalsLoading: boolean;
  signalsError: string | null;
  lastSignalUpdate: number | null;
  changedFields: Set<string>;
  setSignals: (signals: Signal[], prev: Signal[]) => void;
  setSignalsLoading: (v: boolean) => void;
  setSignalsError: (e: string | null) => void;

  // Selected market
  selectedMarketId: string | null;
  setSelectedMarket: (id: string) => void;
  marketHistory: MarketHistory[];
  marketHistoryLoading: boolean;
  setMarketHistory: (h: MarketHistory[]) => void;
  setMarketHistoryLoading: (v: boolean) => void;

  // Portfolio
  portfolio: PortfolioData | null;
  portfolioLoading: boolean;
  portfolioError: string | null;
  setPortfolio: (p: PortfolioData) => void;
  setPortfolioLoading: (v: boolean) => void;
  setPortfolioError: (e: string | null) => void;

  // Analytics
  analytics: AnalyticsData | null;
  analyticsLoading: boolean;
  analyticsError: string | null;
  setAnalytics: (a: AnalyticsData) => void;
  setAnalyticsLoading: (v: boolean) => void;
  setAnalyticsError: (e: string | null) => void;

  // Health
  health: HealthData | null;
  healthLoading: boolean;
  setHealth: (h: HealthData) => void;
  setHealthLoading: (v: boolean) => void;
}

export const useStore = create<KimiBotStore>((set) => ({
  signals: [],
  signalsLoading: true,
  signalsError: null,
  lastSignalUpdate: null,
  changedFields: new Set(),

  setSignals: (signals, prev) => {
    const changed = new Set<string>();
    signals.forEach((sig) => {
      const old = prev.find((p) => p.market_id === sig.market_id);
      if (!old) return;
      if (Math.abs(old.p_market - sig.p_market) > 0.001) changed.add(`${sig.market_id}-p_market`);
      if (Math.abs(old.edge - sig.edge) > 0.001) changed.add(`${sig.market_id}-edge`);
      if (old.signal !== sig.signal) changed.add(`${sig.market_id}-signal`);
    });
    set({ signals, changedFields: changed, lastSignalUpdate: Date.now(), signalsError: null });
    if (changed.size > 0) {
      setTimeout(() => set({ changedFields: new Set() }), 900);
    }
  },
  setSignalsLoading: (v) => set({ signalsLoading: v }),
  setSignalsError: (e) => set({ signalsError: e }),

  selectedMarketId: null,
  setSelectedMarket: (id) => set({ selectedMarketId: id }),
  marketHistory: [],
  marketHistoryLoading: false,
  setMarketHistory: (h) => set({ marketHistory: h, marketHistoryLoading: false }),
  setMarketHistoryLoading: (v) => set({ marketHistoryLoading: v }),

  portfolio: null,
  portfolioLoading: true,
  portfolioError: null,
  setPortfolio: (p) => set({ portfolio: p, portfolioLoading: false, portfolioError: null }),
  setPortfolioLoading: (v) => set({ portfolioLoading: v }),
  setPortfolioError: (e) => set({ portfolioError: e }),

  analytics: null,
  analyticsLoading: true,
  analyticsError: null,
  setAnalytics: (a) => set({ analytics: a, analyticsLoading: false, analyticsError: null }),
  setAnalyticsLoading: (v) => set({ analyticsLoading: v }),
  setAnalyticsError: (e) => set({ analyticsError: e }),

  health: null,
  healthLoading: true,
  setHealth: (h) => set({ health: h, healthLoading: false }),
  setHealthLoading: (v) => set({ healthLoading: v }),
}));
