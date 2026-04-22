import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { Signal } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");
const EDGE_THRESHOLD = parseFloat(process.env.KIMIBOT_EDGE_THRESHOLD ?? "0.005");

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
  // 1. Read latest prediction per market. Prefer root (live infer_only
  //    writes here); fall back to baseline/ which holds historical
  //    validation artifacts.
  let preds = readCSV("predictions.csv");
  if (preds.length === 0) preds = readCSV("baseline/predictions.csv");

  // 2. Get latest prediction per market
  const latestPred = new Map<string, Record<string, string>>();
  for (const row of preds) {
    const existing = latestPred.get(row.market_id);
    if (!existing || row.timestamp > existing.timestamp) {
      latestPred.set(row.market_id, row);
    }
  }

  // 3. Read market snapshots for volume/liquidity
  const snapshots = readCSV("market_snapshots.csv");
  const latestSnap = new Map<string, Record<string, string>>();
  for (const row of snapshots) {
    const existing = latestSnap.get(row.market_id);
    if (!existing || row.timestamp > existing.timestamp) {
      latestSnap.set(row.market_id, row);
    }
  }

  // 4. Read metadata for resolution_time
  const metadata = readCSV("market_metadata.csv");
  const metaMap = new Map(metadata.map((r) => [r.market_id, r]));

  // 5. Build signals
  const signals: Signal[] = [];
  for (const [marketId, pred] of latestPred.entries()) {
    const pModel = parseFloat(pred.p_model_calibrated ?? pred.p_model_raw ?? "0");
    const pMarket = parseFloat(pred.p_market ?? "0.5");
    if (isNaN(pModel) || isNaN(pMarket)) continue;

    const edge = pModel - pMarket;
    const ev = Math.abs(edge);
    let signal: Signal["signal"] = "HOLD";
    if (edge > EDGE_THRESHOLD) signal = "BUY_YES";
    else if (edge < -EDGE_THRESHOLD) signal = "BUY_NO";

    const snap = latestSnap.get(marketId);
    const meta = metaMap.get(marketId);

    signals.push({
      market_id: marketId,
      question: `Market ${marketId}`,
      p_model: pModel,
      p_market: pMarket,
      edge,
      ev,
      signal,
      timestamp: pred.timestamp ?? new Date().toISOString(),
      resolution_time: meta?.resolution_time ?? pred.resolution_time ?? null,
      volume: parseFloat(snap?.volume ?? "0"),
      liquidity: parseFloat(snap?.liquidity ?? "0"),
    });
  }

  // Sort: strongest absolute edge first
  signals.sort((a, b) => Math.abs(b.edge) - Math.abs(a.edge));

  return NextResponse.json({ signals, updated_at: new Date().toISOString() });
}
