import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");

export async function GET() {
  const reportPath = path.join(DATA_DIR, "sanity_report.json");

  try {
    if (!fs.existsSync(reportPath)) {
      return NextResponse.json(
        {
          error: "No sanity report found. Run: python main.py sanity",
          passed: null,
          cases: [],
        },
        { status: 404 }
      );
    }

    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const status = report.passed === false ? 500 : 200;
    return NextResponse.json(report, { status });
  } catch (err) {
    return NextResponse.json(
      { error: String(err), passed: null, cases: [] },
      { status: 500 }
    );
  }
}
