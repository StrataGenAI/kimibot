import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { MarketHistory } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");

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

export async function GET(
  _req: Request,
  context: { params: Promise<{ id: string }> }
) {
  const { id: marketId } = await context.params;

  const snapshots = readCSV("market_snapshots.csv");
  const marketSnaps = snapshots
    .filter((r) => r.market_id === marketId)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp));

  // Get model predictions for this market
  let preds = readCSV("baseline/predictions.csv");
  if (preds.length === 0) preds = readCSV("predictions.csv");
  const marketPreds = preds.filter((r) => r.market_id === marketId);
  const predByTs = new Map(marketPreds.map((r) => [r.timestamp, r]));

  const history: MarketHistory[] = marketSnaps.map((snap) => {
    const ts = snap.timestamp;
    // Convert unix timestamp to ISO if numeric
    let isoTs = ts;
    if (/^\d{10,}$/.test(ts)) {
      isoTs = new Date(parseInt(ts) * 1000).toISOString();
    }
    const pred = predByTs.get(isoTs) ?? predByTs.get(ts);
    return {
      timestamp: isoTs,
      p_market: parseFloat(snap.p_market ?? "0"),
      p_model: pred ? parseFloat(pred.p_model_calibrated ?? pred.p_model_raw ?? "") : null,
      volume: parseFloat(snap.volume ?? "0"),
      liquidity: parseFloat(snap.liquidity ?? "0"),
    };
  });

  // Limit to last 200 points for performance
  const trimmed = history.slice(-200);

  return NextResponse.json({ market_id: marketId, history: trimmed });
}
