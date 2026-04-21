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

  // Prefer ingestion_status.json freshness; fall back to snapshot mtime
  let dataFreshnessSecs: number | null = null;
  let ingestionStatus: Record<string, unknown> | null = null;
  try {
    const statusPath = path.join(DATA_DIR, "ingestion_status.json");
    if (exists(statusPath)) {
      ingestionStatus = JSON.parse(fs.readFileSync(statusPath, "utf8"));
      const limitlessTs = (ingestionStatus as Record<string, Record<string, string>>)?.limitless?.last_fetch_utc;
      if (limitlessTs) {
        dataFreshnessSecs = Math.floor((Date.now() - new Date(limitlessTs).getTime()) / 1000);
      }
    }
  } catch { /* */ }
  if (dataFreshnessSecs === null) {
    const snapshotMtime = mtime(path.join(DATA_DIR, "market_snapshots.csv"));
    dataFreshnessSecs = snapshotMtime
      ? Math.floor((Date.now() - snapshotMtime.getTime()) / 1000)
      : null;
  }

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

  // Compute Brier and ECE from predictions where label is known (ground truth available).
  let brierScore: number | null = null;
  let ece: number | null = null;
  try {
    const predPath = exists(path.join(DATA_DIR, "baseline/predictions.csv"))
      ? path.join(DATA_DIR, "baseline/predictions.csv")
      : exists(path.join(DATA_DIR, "predictions.csv"))
      ? path.join(DATA_DIR, "predictions.csv")
      : null;

    if (predPath) {
      const rows = parseCSV(fs.readFileSync(predPath, "utf8")).filter(
        (r) => r.label !== "" && r.label !== undefined && r.p_model_calibrated !== ""
      );
      if (rows.length > 0) {
        const pairs = rows.map((r) => ({
          p: parseFloat(r.p_model_calibrated),
          y: parseFloat(r.label),
        })).filter((p) => !isNaN(p.p) && !isNaN(p.y));

        if (pairs.length > 0) {
          brierScore = pairs.reduce((s, { p, y }) => s + (p - y) ** 2, 0) / pairs.length;

          // ECE: 10 equal-width bins [0,0.1), [0.1,0.2), …
          const N_BINS = 10;
          let ecSum = 0;
          for (let b = 0; b < N_BINS; b++) {
            const lo = b / N_BINS;
            const hi = (b + 1) / N_BINS;
            const bin = pairs.filter(({ p }) => p >= lo && p < hi);
            if (bin.length === 0) continue;
            const avgP = bin.reduce((s, { p }) => s + p, 0) / bin.length;
            const avgY = bin.reduce((s, { y }) => s + y, 0) / bin.length;
            ecSum += (bin.length / pairs.length) * Math.abs(avgP - avgY);
          }
          ece = ecSum;
        }
      }
    }
  } catch { /* */ }

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
    ingestion_status: ingestionStatus,
    brier_score: brierScore !== null ? Math.round(brierScore * 10000) / 10000 : null,
    ece: ece !== null ? Math.round(ece * 10000) / 10000 : null,
  };

  return NextResponse.json(data);
}
