"use client";

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import type { MarketHistory } from "@/lib/types";

interface Props { data: MarketHistory[]; height?: number }

const fmtTs = (ts: string) => {
  try {
    const d = new Date(ts);
    return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}`;
  } catch { return ts.slice(5, 16); }
};

export function PriceChart({ data, height = 220 }: Props) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center text-text-secondary text-sm" style={{ height }}>
        Select a market to view price history
      </div>
    );
  }

  const hasModel = data.some((d) => d.p_model !== null);

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
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
          domain={[0, 1]}
          tickFormatter={(v) => `${(v * 100).toFixed(0)}%`}
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={false}
          width={36}
        />
        <Tooltip
          formatter={(v: number, name: string) => [
            `${(v * 100).toFixed(2)}%`,
            name === "p_market" ? "Market" : "Model",
          ]}
          labelFormatter={(l) => new Date(l).toLocaleString()}
          contentStyle={{
            background: "#16202e", border: "1px solid #1a2640",
            borderRadius: 6, fontFamily: "IBM Plex Mono", fontSize: 11,
          }}
        />
        <ReferenceLine y={0.5} stroke="#2d3f55" strokeDasharray="4 4" />
        {hasModel && (
          <Line
            type="monotone"
            dataKey="p_model"
            stroke="#3b82f6"
            strokeWidth={1.5}
            dot={false}
            activeDot={{ r: 3, strokeWidth: 0 }}
            name="p_model"
            connectNulls
          />
        )}
        <Line
          type="monotone"
          dataKey="p_market"
          stroke="#00d4aa"
          strokeWidth={1.5}
          dot={false}
          activeDot={{ r: 3, strokeWidth: 0 }}
          name="p_market"
        />
        {hasModel && (
          <Legend
            verticalAlign="top"
            align="right"
            formatter={(v) => (
              <span style={{ fontSize: 10, fontFamily: "IBM Plex Mono", color: "#64748b" }}>
                {v === "p_market" ? "Market" : "Model"}
              </span>
            )}
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  );
}
