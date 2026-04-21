import { cn } from "@/lib/utils";

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("skeleton rounded", className)} />;
}

export function MetricCardSkeleton() {
  return (
    <div className="bg-bg-card border border-border rounded-lg px-4 py-3 flex flex-col gap-2">
      <Skeleton className="h-3 w-20" />
      <Skeleton className="h-6 w-24" />
    </div>
  );
}

export function TableSkeleton({ rows = 6 }: { rows?: number }) {
  return (
    <div className="space-y-px">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="flex gap-4 px-3 py-2.5 bg-bg-card border-b border-border-subtle">
          <Skeleton className="h-3 w-24" />
          <Skeleton className="h-3 w-16 ml-auto" />
          <Skeleton className="h-3 w-12" />
          <Skeleton className="h-3 w-12" />
          <Skeleton className="h-3 w-16" />
        </div>
      ))}
    </div>
  );
}

export function ChartSkeleton({ height = 200 }: { height?: number }) {
  return (
    <div className="bg-bg-card border border-border rounded-lg flex items-center justify-center" style={{ height }}>
      <div className="text-text-muted text-sm">Loading chart…</div>
    </div>
  );
}
