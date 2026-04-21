import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { AnalyticsData, TradeRow, EquityPoint } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");
const INITIAL_CAPITAL = 10000;

function parseCSV(text: string): Record<string, string>[] {
  const lines = text.trim().split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const headers = splitLine(lines[0]);
  return lines.slice(1).map((line) => {
    const vals = splitLine(line);
    const row: Record<string, string> = {};
    headers.forEach((h, i) => { row[h.trim()] = (vals[i] ?? "").trim(); });
    return row;
  });
}

function splitLine(line: string): string[] {
  const r: string[] = [];
  let cur = "";
  let inQ = false;
  for (const c of line) {
    if (c === '"') { inQ = !inQ; continue; }
    if (c === "," && !inQ) { r.push(cur); cur = ""; continue; }
    cur += c;
  }
  r.push(cur);
  return r;
}

function readJSON(relPath: string): Record<string, unknown> | null {
  try {
    const full = path.join(DATA_DIR, relPath);
    if (!fs.existsSync(full)) return null;
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function readCSV(relPath: string): Record<string, string>[] {
  try {
    const full = path.join(DATA_DIR, relPath);
    if (!fs.existsSync(full)) return [];
    return parseCSV(fs.readFileSync(full, "utf8"));
  } catch {
    return [];
  }
}

export async function GET() {
  // Load metrics
  const rawMetrics =
    readJSON("baseline/metrics.json") ??
    readJSON("metrics_report.json") ??
    {};

  // Load trade log
  let tradeRows = readCSV("baseline/trade_log.csv");
  if (tradeRows.length === 0) tradeRows = readCSV("trade_log.csv");

  // Build equity curve from settlements
  const settlements = tradeRows
    .filter((r) => r.event === "settlement")
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp));

  let equity = INITIAL_CAPITAL;
  const equityCurve: EquityPoint[] = [];
  for (const s of settlements) {
    const pnl = parseFloat(s.realized_pnl ?? "0");
    equity += pnl;
    equityCurve.push({ timestamp: s.timestamp, equity, pnl });
  }

  // Build drawdown curve
  let peak = INITIAL_CAPITAL;
  const drawdownCurve = equityCurve.map((p) => {
    if (p.equity > peak) peak = p.equity;
    return { timestamp: p.timestamp, drawdown: (p.equity / peak) - 1 };
  });

  // Format trades for table (last 200 only)
  const trades: TradeRow[] = tradeRows
    .filter((r) => r.event === "settlement" || (r.event === "execution" && r.action !== "HOLD"))
    .slice(-200)
    .map((r) => ({
      market_id: r.market_id,
      timestamp: r.timestamp,
      event: r.event,
      action: r.action,
      side: r.side ?? "",
      fill_price: r.fill_price ? parseFloat(r.fill_price) : null,
      filled_notional: parseFloat(r.filled_notional ?? "0"),
      realized_pnl: r.realized_pnl ? parseFloat(r.realized_pnl) : null,
      edge_entry: parseFloat(r.edge_entry ?? "0"),
      ev_entry: parseFloat(r.ev_entry ?? "0"),
      reason: r.reason ?? "",
    }));

  const metrics = rawMetrics as AnalyticsData["metrics"];
  const data: AnalyticsData = {
    metrics,
    equity_curve: equityCurve,
    drawdown_curve: drawdownCurve,
    trades,
  };

  return NextResponse.json(data);
}
