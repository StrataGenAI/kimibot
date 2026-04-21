import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { HealthData } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");
const MODELS_DIR = process.env.KIMIBOT_MODELS_DIR ?? path.join(process.cwd(), "..", "models");

function parseCSV(text: string): Record<string, string>[] {
  const lines = text.trim().split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const vals = line.split(",").map((v) => v.trim());
    const row: Record<string, string> = {};
    headers.forEach((h, i) => { row[h] = vals[i] ?? ""; });
    return row;
  });
}

function exists(p: string) {
  try { return fs.existsSync(p); } catch { return false; }
}

function mtime(p: string): Date | null {
  try { return fs.statSync(p).mtime; } catch { return null; }
}

function latestCSVTimestamp(relPath: string): string | null {
  try {
    const full = path.join(DATA_DIR, relPath);
    if (!exists(full)) return null;
    const rows = parseCSV(fs.readFileSync(full, "utf8"));
    if (rows.length === 0) return null;
    const tss = rows.map((r) => r.timestamp).filter(Boolean).sort();
    return tss[tss.length - 1] ?? null;
  } catch {
    return null;
  }
}

function countCSV(relPath: string): number {
  try {
    const full = path.join(DATA_DIR, relPath);
    if (!exists(full)) return 0;
    const rows = parseCSV(fs.readFileSync(full, "utf8"));
    return rows.length;
  } catch {
    return 0;
  }
}

export async function GET() {
  const modelPath = path.join(MODELS_DIR, "logistic_regression.pkl");
  const scalerPath = path.join(MODELS_DIR, "standard_scaler.pkl");
  const calibPath = path.join(MODELS_DIR, "probability_calibrator.pkl");
  const metaPath = path.join(MODELS_DIR, "training_metadata.json");

  const modelLoaded = exists(modelPath);
  const scalerLoaded = exists(scalerPath);
  const calibratorLoaded = exists(calibPath);

  let trainingMetadata: Record<string, unknown> | null = null;
  try {
    if (exists(metaPath)) trainingMetadata = JSON.parse(fs.readFileSync(metaPath, "utf8"));
  } catch { /* */ }

  const lastSnapshotTime = latestCSVTimestamp("market_snapshots.csv");
  const lastCryptoTime = latestCSVTimestamp("crypto_snapshots.csv");

  let lastPredictionTime = latestCSVTimestamp("baseline/predictions.csv");
  if (!lastPredictionTime) lastPredictionTime = latestCSVTimestamp("predictions.csv");

  const snapshotMtime = mtime(path.join(DATA_DIR, "market_snapshots.csv"));
  const dataFreshnessSecs = snapshotMtime
    ? Math.floor((Date.now() - snapshotMtime.getTime()) / 1000)
    : null;

  // Count unique markets in snapshots
  let marketCount = 0;
  try {
    const full = path.join(DATA_DIR, "market_snapshots.csv");
    if (exists(full)) {
      const rows = parseCSV(fs.readFileSync(full, "utf8"));
      marketCount = new Set(rows.map((r) => r.market_id)).size;
    }
  } catch { /* */ }

  const predCount = countCSV("baseline/predictions.csv") || countCSV("predictions.csv");
  const tradeCount = countCSV("baseline/trade_log.csv") || countCSV("trade_log.csv");

  const data: HealthData = {
    model_loaded: modelLoaded,
    last_prediction_time: lastPredictionTime,
    last_snapshot_time: lastSnapshotTime,
    last_crypto_time: lastCryptoTime,
    data_freshness_seconds: dataFreshnessSecs,
    market_count: marketCount,
    prediction_count: predCount,
    trade_count: tradeCount,
    scaler_loaded: scalerLoaded,
    calibrator_loaded: calibratorLoaded,
    training_metadata: trainingMetadata,
  };

  return NextResponse.json(data);
}
