import { cn } from "@/lib/utils";

interface MetricCardProps {
  label: string;
  value: string;
  sub?: string;
  trend?: "up" | "down" | "neutral";
  tooltip?: string;
  size?: "sm" | "md";
}

export function MetricCard({ label, value, sub, trend, tooltip, size = "md" }: MetricCardProps) {
  const trendColor =
    trend === "up" ? "text-green" : trend === "down" ? "text-red" : "text-text-primary";

  return (
    <div
      className="bg-bg-card border border-border rounded-lg px-4 py-3 flex flex-col gap-1"
      data-tooltip={tooltip}
    >
      <div className="text-2xs text-text-secondary uppercase tracking-wider">{label}</div>
      <div className={cn("font-mono font-semibold tabular-num leading-none", trendColor, size === "md" ? "text-xl" : "text-lg")}>
        {value}
      </div>
      {sub && <div className="text-2xs text-text-secondary font-mono">{sub}</div>}
    </div>
  );
}
