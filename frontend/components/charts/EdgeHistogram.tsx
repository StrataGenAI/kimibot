"use client";

import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Cell, ResponsiveContainer } from "recharts";
import type { EdgeBucket } from "@/lib/types";

interface Props { data: EdgeBucket[]; height?: number }

export function EdgeHistogram({ data, height = 180 }: Props) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center text-text-secondary text-sm" style={{ height }}>
        No edge data
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 8, bottom: 4, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1a2640" vertical={false} />
        <XAxis
          dataKey="bucket"
          tick={{ fontSize: 9, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={{ stroke: "#1a2640" }}
          interval={0}
        />
        <YAxis
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={false}
          width={24}
        />
        <Tooltip
          formatter={(v: number, name: string) => [
            name === "win_rate" ? `${(v * 100).toFixed(1)}%` : v.toFixed(3),
            name === "win_rate" ? "Win Rate" : "Mean PnL",
          ]}
          contentStyle={{
            background: "#16202e", border: "1px solid #1a2640",
            borderRadius: 6, fontFamily: "IBM Plex Mono", fontSize: 11,
          }}
        />
        <Bar dataKey="count" name="Trades" maxBarSize={40}>
          {data.map((d, i) => (
            <Cell key={i} fill={d.mean_realized_pnl >= 0 ? "#00d4aa" : "#f05e6e"} fillOpacity={0.7} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
