"use client";

import { useEffect, useRef } from "react";
import { useStore } from "./store";

const POLL_MS = parseInt(process.env.NEXT_PUBLIC_POLL_INTERVAL_MS ?? "3000", 10);

async function fetchJSON<T>(url: string): Promise<T> {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export function useSignalPolling() {
  const { signals, setSignals, setSignalsLoading, setSignalsError } = useStore();
  const prevRef = useRef(signals);

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const data = await fetchJSON<{ signals: typeof signals }>("/api/signals");
        if (!cancelled) {
          setSignals(data.signals, prevRef.current);
          prevRef.current = data.signals;
          setSignalsLoading(false);
        }
      } catch (e) {
        if (!cancelled) setSignalsError((e as Error).message);
      }
    };

    poll();
    const id = setInterval(poll, POLL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
}

export function useMarketHistory(marketId: string | null) {
  const { setMarketHistory, setMarketHistoryLoading } = useStore();

  useEffect(() => {
    if (!marketId) return;
    let cancelled = false;
    setMarketHistoryLoading(true);

    const load = async () => {
      try {
        const data = await fetchJSON<{ history: ReturnType<typeof useStore.getState>["marketHistory"] }>(
          `/api/market/${encodeURIComponent(marketId)}`
        );
        if (!cancelled) setMarketHistory(data.history);
      } catch {
        if (!cancelled) setMarketHistoryLoading(false);
      }
    };

    load();
    const id = setInterval(load, POLL_MS * 2);
    return () => { cancelled = true; clearInterval(id); };
  }, [marketId]); // eslint-disable-line react-hooks/exhaustive-deps
}

export function usePortfolioPolling() {
  const { setPortfolio, setPortfolioLoading, setPortfolioError } = useStore();

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const data = await fetchJSON<ReturnType<typeof useStore.getState>["portfolio"]>("/api/portfolio");
        if (!cancelled && data) setPortfolio(data);
      } catch (e) {
        if (!cancelled) { setPortfolioError((e as Error).message); setPortfolioLoading(false); }
      }
    };

    poll();
    const id = setInterval(poll, POLL_MS * 2);
    return () => { cancelled = true; clearInterval(id); };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
}

export function useAnalyticsPolling() {
  const { setAnalytics, setAnalyticsLoading, setAnalyticsError } = useStore();

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const data = await fetchJSON<ReturnType<typeof useStore.getState>["analytics"]>("/api/analytics");
        if (!cancelled && data) setAnalytics(data);
      } catch (e) {
        if (!cancelled) { setAnalyticsError((e as Error).message); setAnalyticsLoading(false); }
      }
    };

    load();
    return () => { cancelled = true; };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
}

export function useHealthPolling() {
  const { setHealth, setHealthLoading } = useStore();

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const data = await fetchJSON<ReturnType<typeof useStore.getState>["health"]>("/api/health");
        if (!cancelled && data) setHealth(data);
      } catch {
        if (!cancelled) setHealthLoading(false);
      }
    };

    poll();
    const id = setInterval(poll, POLL_MS * 3);
    return () => { cancelled = true; clearInterval(id); };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
}
