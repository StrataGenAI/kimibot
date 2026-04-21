import { type ClassValue, clsx } from "clsx";
import type { SignalType } from "./types";

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

export function fmt(n: number | null | undefined, decimals = 2): string {
  if (n == null || isNaN(n)) return "—";
  return n.toFixed(decimals);
}

export function fmtPct(n: number | null | undefined, decimals = 1): string {
  if (n == null || isNaN(n)) return "—";
  return `${(n * 100).toFixed(decimals)}%`;
}

export function fmtDollar(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "—";
  const abs = Math.abs(n);
  const prefix = n < 0 ? "-$" : "$";
  if (abs >= 1000) return `${prefix}${(abs / 1000).toFixed(2)}k`;
  return `${prefix}${abs.toFixed(2)}`;
}

export function fmtEdge(edge: number): string {
  return `${edge >= 0 ? "+" : ""}${(edge * 100).toFixed(2)}pp`;
}

export function signalColor(signal: SignalType): string {
  if (signal === "BUY_YES") return "text-green";
  if (signal === "BUY_NO") return "text-red";
  return "text-text-secondary";
}

export function edgeColor(edge: number, threshold = 0.005): string {
  if (edge > threshold) return "text-green";
  if (edge < -threshold) return "text-red";
  return "text-text-secondary";
}

export function pnlColor(val: number): string {
  if (val > 0) return "text-green";
  if (val < 0) return "text-red";
  return "text-text-secondary";
}

export function signalBg(signal: SignalType): string {
  if (signal === "BUY_YES") return "bg-green/10 text-green border border-green/20";
  if (signal === "BUY_NO") return "bg-red/10 text-red border border-red/20";
  return "bg-text-muted/20 text-text-secondary border border-border";
}

export function relativeTime(ts: string | null): string {
  if (!ts) return "never";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return "never";
  const secs = Math.floor((Date.now() - d.getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

export function shortMarketId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) + "…" : id;
}

/** Parse a CSV string into typed records */
export function parseCSV(text: string): Record<string, string>[] {
  const lines = text.trim().split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const headers = splitCSVLine(lines[0]);
  return lines.slice(1).map((line) => {
    const vals = splitCSVLine(line);
    const row: Record<string, string> = {};
    headers.forEach((h, i) => {
      row[h.trim()] = (vals[i] ?? "").trim();
    });
    return row;
  });
}

function splitCSVLine(line: string): string[] {
  const result: string[] = [];
  let cur = "";
  let inQ = false;
  for (const c of line) {
    if (c === '"') { inQ = !inQ; continue; }
    if (c === "," && !inQ) { result.push(cur); cur = ""; continue; }
    cur += c;
  }
  result.push(cur);
  return result;
}
