"use client";

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import type { EquityPoint } from "@/lib/types";

const fmt = (v: number) =>
  v >= 1000 ? `$${(v / 1000).toFixed(1)}k` : `$${v.toFixed(0)}`;

const fmtTs = (ts: string) => {
  try {
    const d = new Date(ts);
    return `${d.getMonth() + 1}/${d.getDate()}`;
  } catch {
    return ts.slice(0, 10);
  }
};

interface Props { data: EquityPoint[]; height?: number }

export function EquityCurve({ data, height = 220 }: Props) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center text-text-secondary text-sm" style={{ height }}>
        No equity data yet
      </div>
    );
  }

  const initial = data[0]?.equity ?? 10000;
  const final = data[data.length - 1]?.equity ?? initial;
  const isUp = final >= initial;
  const color = isUp ? "#00d4aa" : "#f05e6e";
  const fillColor = isUp ? "rgba(0,212,170,0.08)" : "rgba(240,94,110,0.08)";

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="eq-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={color} stopOpacity={0.15} />
            <stop offset="95%" stopColor={color} stopOpacity={0.01} />
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
          tickFormatter={fmt}
          tick={{ fontSize: 10, fill: "#64748b", fontFamily: "IBM Plex Mono" }}
          tickLine={false}
          axisLine={false}
          width={48}
        />
        <Tooltip
          formatter={(v: number) => [`$${v.toFixed(2)}`, "Equity"]}
          labelFormatter={(l) => new Date(l).toLocaleString()}
          contentStyle={{
            background: "#16202e",
            border: "1px solid #1a2640",
            borderRadius: 6,
            fontFamily: "IBM Plex Mono",
            fontSize: 11,
          }}
        />
        <Area
          type="monotone"
          dataKey="equity"
          stroke={color}
          strokeWidth={1.5}
          fill="url(#eq-fill)"
          dot={false}
          activeDot={{ r: 3, strokeWidth: 0, fill: color }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
