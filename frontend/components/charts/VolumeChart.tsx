"use client";

import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import type { MarketHistory } from "@/lib/types";

interface Props { data: MarketHistory[]; height?: number }

const fmtTs = (ts: string) => {
  try {
    const d = new Date(ts);
    return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}`;
  } catch { return ts.slice(11, 16); }
};

export function VolumeChart({ data, height = 100 }: Props) {
  if (data.length === 0) return null;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1a2640" vertical={false} />
        <XAxis
          dataKey="timestamp"
          tickFormatter={fmtTs}
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={{ stroke: "#1a2640" }}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={false}
          width={36}
          tickFormatter={(v) => v >= 1000 ? `${(v/1000).toFixed(0)}k` : v.toFixed(0)}
        />
        <Tooltip
          formatter={(v: number) => [`$${v.toFixed(0)}`, "Volume"]}
          labelFormatter={(l) => new Date(l).toLocaleString()}
          contentStyle={{
            background: "#16202e", border: "1px solid #1a2640",
            borderRadius: 6, fontFamily: "IBM Plex Mono", fontSize: 11,
          }}
        />
        <Bar dataKey="volume" fill="rgba(59,130,246,0.5)" maxBarSize={8} />
      </BarChart>
    </ResponsiveContainer>
  );
}
