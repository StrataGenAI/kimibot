import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { WalkForwardData } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");

export async function GET() {
  const resultsPath = path.join(DATA_DIR, "walk_forward_results.json");

  if (!fs.existsSync(resultsPath)) {
    return NextResponse.json(
      { error: "Walk-forward results not found. Run: python main.py evaluate-limitless" },
      { status: 404 }
    );
  }

  try {
    const raw = fs.readFileSync(resultsPath, "utf8");
    const data: WalkForwardData = JSON.parse(raw);
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Failed to parse walk_forward_results.json" }, { status: 500 });
  }
}
