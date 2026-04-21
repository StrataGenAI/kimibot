"use client";

import { useHealthPolling } from "@/lib/hooks";
import { useStore } from "@/lib/store";
import { relativeTime, cn } from "@/lib/utils";
import { CheckCircle, XCircle, Clock, Database, Cpu, Activity } from "lucide-react";

function StatusRow({
  label,
  ok,
  value,
  icon: Icon,
}: {
  label: string;
  ok: boolean | null;
  value?: string;
  icon?: React.ElementType;
}) {
  return (
    <div className="flex items-center justify-between py-3 border-b border-border-subtle last:border-0">
      <div className="flex items-center gap-2.5">
        {Icon && <Icon size={14} className="text-text-secondary shrink-0" />}
        <span className="text-sm text-text-primary">{label}</span>
      </div>
      <div className="flex items-center gap-2">
        {value && <span className="text-xs font-mono text-text-secondary tabular-num">{value}</span>}
        {ok === true && <CheckCircle size={14} className="text-green" />}
        {ok === false && <XCircle size={14} className="text-red" />}
        {ok === null && <div className="w-3.5 h-3.5 rounded-full border-2 border-text-muted border-t-text-secondary animate-spin" />}
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
      <div className="px-4 py-3 border-b border-border">
        <h3 className="text-md font-semibold text-text-primary">{title}</h3>
      </div>
      <div className="px-4">{children}</div>
    </div>
  );
}

export default function HealthPage() {
  useHealthPolling();
  const { health, healthLoading } = useStore();

  const freshness = health?.data_freshness_seconds;
  const freshnessOk = freshness != null ? freshness < 60 : null;
  const freshnessStr = freshness != null ? `${freshness}s ago` : "—";

  return (
    <div className="flex flex-col gap-5 max-w-2xl">
      <div>
        <h1 className="text-2xl font-semibold text-text-primary">System Health</h1>
        <p className="text-sm text-text-secondary mt-0.5">Model status, data freshness, and runtime diagnostics</p>
      </div>

      {healthLoading && !health && (
        <div className="space-y-3">
          {[1,2,3].map(i => <div key={i} className="skeleton h-32 rounded-xl" />)}
        </div>
      )}

      {health && (
        <>
          {/* Model artifacts */}
          <Section title="Model Artifacts">
            <StatusRow label="Logistic Regression model" ok={health.model_loaded} icon={Cpu} />
            <StatusRow label="Standard Scaler" ok={health.scaler_loaded} icon={Cpu} />
            <StatusRow label="Probability Calibrator" ok={health.calibrator_loaded} icon={Cpu} />
          </Section>

          {/* Data freshness */}
          <Section title="Data Freshness">
            <StatusRow
              label="Market snapshot freshness"
              ok={freshnessOk}
              value={freshnessStr}
              icon={Clock}
            />
            <StatusRow
              label="Last market snapshot"
              ok={health.last_snapshot_time != null}
              value={relativeTime(health.last_snapshot_time)}
              icon={Database}
            />
            <StatusRow
              label="Last crypto snapshot"
              ok={health.last_crypto_time != null}
              value={relativeTime(health.last_crypto_time)}
              icon={Database}
            />
            <StatusRow
              label="Last prediction"
              ok={health.last_prediction_time != null}
              value={relativeTime(health.last_prediction_time)}
              icon={Activity}
            />
          </Section>

          {/* Counters */}
          <Section title="Data Counts">
            <StatusRow label="Active markets" ok={health.market_count > 0} value={health.market_count.toString()} />
            <StatusRow label="Predictions stored" ok={health.prediction_count > 0} value={health.prediction_count.toString()} />
            <StatusRow label="Trade records" ok={health.trade_count > 0} value={health.trade_count.toString()} />
          </Section>

          {/* Training metadata */}
          {health.training_metadata && (
            <Section title="Training Metadata">
              {Object.entries(health.training_metadata).map(([k, v]) => {
                if (typeof v === "object") return null;
                return (
                  <div key={k} className="flex items-center justify-between py-2.5 border-b border-border-subtle last:border-0">
                    <span className="text-xs text-text-secondary font-mono">{k}</span>
                    <span className="text-xs font-mono text-text-primary tabular-num">{String(v)}</span>
                  </div>
                );
              })}
            </Section>
          )}

          {/* Overall status */}
          <div className={cn(
            "flex items-center gap-3 p-4 rounded-xl border",
            health.model_loaded && health.prediction_count > 0
              ? "bg-green/5 border-green/20 text-green"
              : "bg-yellow/5 border-yellow/20 text-yellow"
          )}>
            <Activity size={16} />
            <div>
              <div className="text-sm font-semibold">
                {health.model_loaded && health.prediction_count > 0 ? "System operational" : "System degraded"}
              </div>
              <div className="text-xs opacity-70 mt-0.5">
                {health.model_loaded
                  ? `Model loaded · ${health.prediction_count} predictions`
                  : "Model not loaded — run training first"}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
