"use client";

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface Props {
  data: { timestamp: string; drawdown: number }[];
  height?: number;
}

const fmtTs = (ts: string) => {
  try { return new Date(ts).toLocaleDateString("en", { month: "short", day: "numeric" }); }
  catch { return ts.slice(5, 10); }
};

export function DrawdownChart({ data, height = 160 }: Props) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center text-text-secondary text-sm" style={{ height }}>
        No drawdown data
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="dd-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#f05e6e" stopOpacity={0.2} />
            <stop offset="95%" stopColor="#f05e6e" stopOpacity={0.01} />
          </linearGradient>
        </defs>
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
          tickFormatter={(v) => `${(v * 100).toFixed(1)}%`}
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={false}
          width={44}
          domain={["auto", 0]}
        />
        <Tooltip
          formatter={(v: number) => [`${(v * 100).toFixed(2)}%`, "Drawdown"]}
          labelFormatter={(l) => new Date(l).toLocaleString()}
          contentStyle={{
            background: "#16202e", border: "1px solid #1a2640",
            borderRadius: 6, fontFamily: "IBM Plex Mono", fontSize: 11,
          }}
        />
        <Area
          type="monotone"
          dataKey="drawdown"
          stroke="#f05e6e"
          strokeWidth={1.5}
          fill="url(#dd-fill)"
          dot={false}
          activeDot={{ r: 3, strokeWidth: 0, fill: "#f05e6e" }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
