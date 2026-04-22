import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { PortfolioData, Position } from "@/lib/types";

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
  let trades = readCSV("trade_log.csv");
  if (trades.length === 0) trades = readCSV("baseline/trade_log.csv");

  // Compute realized PnL from settlements
  const settlements = trades.filter((r) => r.event === "settlement");
  const executions = trades.filter((r) => r.event === "execution" && r.action !== "HOLD");

  const realizedPnl = settlements.reduce((sum, r) => sum + parseFloat(r.realized_pnl ?? "0"), 0);

  // Build open positions (executions without matching settlement)
  const settledIds = new Set(settlements.map((r) => r.position_id).filter(Boolean));
  const openExecs = executions.filter(
    (r) => r.position_id && !settledIds.has(r.position_id)
  );

  // Get latest snapshot prices for current value
  const snaps = readCSV("market_snapshots.csv");
  const latestPrice = new Map<string, number>();
  for (const s of snaps) {
    latestPrice.set(s.market_id, parseFloat(s.p_market ?? "0.5"));
  }

  const positions: Position[] = openExecs.map((r, i) => {
    const qty = parseFloat(r.filled_notional ?? "0");
    const fillPrice = parseFloat(r.fill_price ?? "0.5");
    const costBasis = qty;
    const currentPrice = latestPrice.get(r.market_id) ?? fillPrice;
    const side = r.side ?? "YES";
    const currentValue =
      side === "YES" ? qty * (currentPrice / fillPrice) : qty * ((1 - currentPrice) / (1 - fillPrice));
    const unrealizedPnl = currentValue - costBasis;

    return {
      position_id: r.position_id || `pos_${i}`,
      market_id: r.market_id,
      side,
      quantity: qty,
      average_price: fillPrice,
      cost_basis: costBasis,
      entry_timestamp: r.entry_timestamp ?? r.timestamp,
      entry_edge: parseFloat(r.edge_entry ?? "0"),
      entry_ev: parseFloat(r.ev_entry ?? "0"),
      realized_pnl: 0,
      current_price: currentPrice,
      unrealized_pnl: isNaN(unrealizedPnl) ? 0 : unrealizedPnl,
      resolution_time: r.resolution_time ?? "",
    };
  });

  const grossExposure = positions.reduce((s, p) => s + p.cost_basis, 0);
  const unrealizedPnl = positions.reduce((s, p) => s + p.unrealized_pnl, 0);
  const cash = INITIAL_CAPITAL - grossExposure + realizedPnl;
  const totalEquity = cash + grossExposure + unrealizedPnl;

  const data: PortfolioData = {
    cash,
    initial_capital: INITIAL_CAPITAL,
    realized_pnl: realizedPnl,
    unrealized_pnl: unrealizedPnl,
    gross_exposure: grossExposure,
    total_equity: totalEquity,
    positions,
  };

  return NextResponse.json(data);
}
