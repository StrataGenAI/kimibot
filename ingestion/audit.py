"""Read-only data quality audit for raw ingestion datasets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ingestion.recorder import RawReplayStore
from project.configuration import AppConfig


LOGGER = logging.getLogger(__name__)


@dataclass
class DataQualityAuditor:
    """Analyze raw Parquet datasets and emit a JSON audit report."""

    config: AppConfig

    def run(self) -> dict[str, Any]:
        """Run the full read-only data quality audit."""

        replay_store = RawReplayStore(self.config.data.raw_storage_root)
        market = replay_store.read_all_market_data()
        crypto = replay_store.read_all_crypto_data()
        trade_log = self._load_trade_log(self.config.data.trade_log_path)
        market_metadata = self._load_market_metadata(self.config.data.market_metadata_path)

        report = {
            "per_market": self._per_market_report(market, market_metadata),
            "global_intervals": self._global_interval_report(market, crypto),
            "anomalies": {
                "market": self._anomaly_report(market, key_column="market_id", price_column="yes_price"),
                "crypto": self._anomaly_report(crypto, key_column="symbol", price_column="price"),
            },
            "alignment": self._alignment_report(market, crypto),
            "replay_integrity": {
                "market": replay_store.replay_integrity_check(
                    dataset="market",
                    sample_count=self.config.audit.replay_integrity_samples,
                ),
                "crypto": replay_store.replay_integrity_check(
                    dataset="crypto",
                    sample_count=self.config.audit.replay_integrity_samples,
                ),
            },
            "liquidity_validation": self._liquidity_validation(trade_log),
            "health": self._health_report(market, crypto),
            "drift": self._drift_report(market, trade_log),
        }
        self._write_report(report)
        self._log_summary(report)
        return report

    def _per_market_report(self, market: pd.DataFrame, market_metadata: pd.DataFrame) -> dict[str, Any]:
        """Compute per-market duration, interval, missingness, and lifecycle completeness."""

        if market.empty:
            return {}
        resolution_map = {}
        if not market_metadata.empty:
            metadata = market_metadata.copy()
            metadata["resolution_time"] = pd.to_datetime(metadata["resolution_time"], utc=True)
            resolution_map = metadata.set_index("market_id")["resolution_time"].to_dict()

        report: dict[str, Any] = {}
        for market_id, group in market.sort_values("event_time").groupby("market_id", observed=False):
            intervals = group["event_time"].diff().dropna().dt.total_seconds()
            first_ts = group["event_time"].min()
            last_ts = group["event_time"].max()
            duration = max((last_ts - first_ts).total_seconds(), 0.0)
            expected = self.config.audit.expected_market_interval_seconds
            expected_ticks = max(int(duration / expected) + 1, 1)
            missing_pct = max(0.0, 1.0 - (len(group) / expected_ticks))
            resolution_time = resolution_map.get(market_id)
            lifecycle = "unknown"
            coverage_ratio = None
            if resolution_time is not None and resolution_time > first_ts:
                total_lifecycle = max((resolution_time - first_ts).total_seconds(), 1.0)
                observed_ratio = min(max(duration / total_lifecycle, 0.0), 1.0)
                coverage_ratio = observed_ratio
                lifecycle = "late_only" if observed_ratio < self.config.audit.lifecycle_late_only_ratio else "covered"
            report[str(market_id)] = {
                "total_duration_seconds": duration,
                "average_interval_seconds": float(intervals.mean()) if not intervals.empty else 0.0,
                "missing_interval_percentage": missing_pct,
                "first_event_time": first_ts.isoformat(),
                "last_event_time": last_ts.isoformat(),
                "observed_ticks": int(len(group)),
                "coverage_ratio": coverage_ratio,
                "lifecycle_status": lifecycle,
            }
        return report

    def _global_interval_report(self, market: pd.DataFrame, crypto: pd.DataFrame) -> dict[str, Any]:
        """Compute interval distributions and gap counts."""

        return {
            "market": self._interval_distribution(market, "market_id", self.config.audit.gap_threshold_seconds),
            "crypto": self._interval_distribution(crypto, "symbol", self.config.audit.gap_threshold_seconds),
        }

    def _interval_distribution(self, frame: pd.DataFrame, key_column: str, gap_threshold: int) -> dict[str, Any]:
        """Compute interval summary statistics for a stream family."""

        if frame.empty:
            return {"interval_distribution_seconds": {}, "gaps_above_threshold": 0}
        intervals = (
            frame.sort_values([key_column, "event_time"])
            .groupby(key_column, observed=False)["event_time"]
            .diff()
            .dropna()
            .dt.total_seconds()
        )
        if intervals.empty:
            return {"interval_distribution_seconds": {}, "gaps_above_threshold": 0}
        return {
            "interval_distribution_seconds": {
                "p50": float(intervals.quantile(0.50)),
                "p90": float(intervals.quantile(0.90)),
                "p99": float(intervals.quantile(0.99)),
                "max": float(intervals.max()),
            },
            "gaps_above_threshold": int((intervals > gap_threshold).sum()),
        }

    def _anomaly_report(self, frame: pd.DataFrame, *, key_column: str, price_column: str) -> dict[str, Any]:
        """Detect duplicates, out-of-order records, and extreme price jumps."""

        if frame.empty:
            return {
                "duplicate_timestamps": 0,
                "out_of_order_events": 0,
                "large_price_jumps": 0,
                "examples": [],
            }
        ordered = frame.sort_values([key_column, "ingestion_time", "event_time"]).copy()
        duplicates = ordered.duplicated(subset=[key_column, "event_time"], keep=False)
        out_of_order = ordered.groupby(key_column, observed=False)["event_time"].diff().dt.total_seconds().fillna(1.0) < 0.0
        sorted_by_event = ordered.sort_values([key_column, "event_time"]).copy()
        jump_sizes = sorted_by_event.groupby(key_column, observed=False)[price_column].diff().abs()
        if float(pd.to_numeric(sorted_by_event[price_column], errors="coerce").max()) <= 1.0:
            jumps = jump_sizes > self.config.audit.price_jump_threshold
        else:
            previous = sorted_by_event.groupby(key_column, observed=False)[price_column].shift(1).replace(0.0, np.nan)
            jumps = (jump_sizes / previous).fillna(0.0) > 0.05
        examples = ordered[duplicates | out_of_order | jumps].head(5)
        return {
            "duplicate_timestamps": int(duplicates.sum()),
            "out_of_order_events": int(out_of_order.sum()),
            "large_price_jumps": int(jumps.sum()),
            "examples": examples[[key_column, "event_time", price_column]].to_dict(orient="records"),
        }

    def _alignment_report(self, market: pd.DataFrame, crypto: pd.DataFrame) -> dict[str, Any]:
        """Compare market and crypto timestamps across event and ingestion clocks."""

        if market.empty or crypto.empty:
            return {"pairs": 0, "event_time_delta_seconds": {}, "ingestion_time_delta_seconds": {}, "above_threshold": 0}
        market_sorted = market.sort_values("event_time")[["market_id", "event_time", "ingestion_time"]].copy()
        crypto_sorted = crypto.sort_values("event_time")[["symbol", "event_time", "ingestion_time"]].copy()
        market_sorted = market_sorted.rename(columns={"event_time": "market_event_time", "ingestion_time": "market_ingestion_time"})
        crypto_sorted = crypto_sorted.rename(columns={"event_time": "crypto_event_time", "ingestion_time": "crypto_ingestion_time"})
        market_sorted = market_sorted.dropna(subset=["market_event_time"])
        crypto_sorted = crypto_sorted.dropna(subset=["crypto_event_time"])
        if market_sorted.empty or crypto_sorted.empty:
            return {"pairs": 0, "event_time_delta_seconds": {}, "ingestion_time_delta_seconds": {}, "above_threshold": 0}
        aligned = pd.merge_asof(
            market_sorted,
            crypto_sorted,
            left_on="market_event_time",
            right_on="crypto_event_time",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=max(self.config.audit.gap_threshold_seconds, self.config.audit.alignment_threshold_seconds * 4)),
        ).dropna(subset=["symbol"])
        if aligned.empty:
            return {"pairs": 0, "event_time_delta_seconds": {}, "ingestion_time_delta_seconds": {}, "above_threshold": 0}
        event_deltas = (aligned["market_event_time"] - aligned["crypto_event_time"]).abs().dt.total_seconds()
        ingestion_deltas = (aligned["market_ingestion_time"] - aligned["crypto_ingestion_time"]).abs().dt.total_seconds()
        return {
            "pairs": int(len(aligned)),
            "event_time_delta_seconds": self._distribution(event_deltas),
            "ingestion_time_delta_seconds": self._distribution(ingestion_deltas),
            "above_threshold": int((ingestion_deltas > self.config.audit.alignment_threshold_seconds).sum()),
        }

    def _liquidity_validation(self, trade_log: pd.DataFrame) -> dict[str, Any]:
        """Estimate realistic fill-rate behavior from requested size vs liquidity."""

        if trade_log.empty:
            return {
                "trade_count": 0,
                "average_fill_rate": 0.0,
                "partial_fill_fraction": 0.0,
                "realistic_fill_fraction_breaches": 0,
            }
        executions = trade_log[trade_log["event"] == "execution"].copy()
        if executions.empty:
            return {
                "trade_count": 0,
                "average_fill_rate": 0.0,
                "partial_fill_fraction": 0.0,
                "realistic_fill_fraction_breaches": 0,
            }
        executions["requested_notional"] = pd.to_numeric(executions["requested_notional"], errors="coerce").fillna(0.0)
        executions["filled_notional"] = pd.to_numeric(executions["filled_notional"], errors="coerce").fillna(0.0)
        executions["liquidity_at_entry"] = pd.to_numeric(executions["liquidity_at_entry"], errors="coerce").fillna(0.0)
        executions = executions[executions["requested_notional"] > 0.0]
        if executions.empty:
            return {
                "trade_count": 0,
                "average_fill_rate": 0.0,
                "partial_fill_fraction": 0.0,
                "realistic_fill_fraction_breaches": 0,
            }
        fill_rate = executions["filled_notional"] / executions["requested_notional"]
        liquidity_fraction = executions["filled_notional"] / executions["liquidity_at_entry"].replace(0.0, np.nan)
        breaches = liquidity_fraction > self.config.audit.realistic_fill_fraction
        return {
            "trade_count": int(len(executions)),
            "average_fill_rate": float(fill_rate.mean()),
            "partial_fill_fraction": float(((fill_rate > 0.0) & (fill_rate < 1.0)).mean()),
            "full_fill_fraction": float((fill_rate >= 0.999).mean()),
            "realistic_fill_fraction_breaches": int(breaches.fillna(False).sum()),
        }

    def _health_report(self, market: pd.DataFrame, crypto: pd.DataFrame) -> dict[str, Any]:
        """Compute simple ingestion health metrics from stored data."""

        return {
            "market_events_per_minute": self._events_per_minute(market, "event_time"),
            "crypto_events_per_minute": self._events_per_minute(crypto, "event_time"),
            "market_latency_seconds": self._latency_distribution(market),
            "crypto_latency_seconds": self._latency_distribution(crypto),
            "market_missing_intervals": self._missing_interval_count(market, "market_id", self.config.audit.gap_threshold_seconds),
            "crypto_missing_intervals": self._missing_interval_count(crypto, "symbol", self.config.audit.gap_threshold_seconds),
        }

    def _drift_report(self, market: pd.DataFrame, trade_log: pd.DataFrame) -> dict[str, Any]:
        """Track simple distribution drift over early vs late periods."""

        if market.empty:
            return {}
        ordered = market.sort_values("event_time").copy()
        ordered["price_delta"] = ordered.groupby("market_id", observed=False)["yes_price"].diff()
        ordered["rolling_volatility"] = (
            ordered.groupby("market_id", observed=False)["yes_price"].diff().rolling(window=3, min_periods=1).std().reset_index(level=0, drop=True)
        )
        midpoint = max(len(ordered) // 2, 1)
        early = ordered.iloc[:midpoint]
        late = ordered.iloc[midpoint:]
        report = {
            "market_price_delta": {
                "early_mean": float(early["price_delta"].dropna().mean()) if not early["price_delta"].dropna().empty else 0.0,
                "late_mean": float(late["price_delta"].dropna().mean()) if not late["price_delta"].dropna().empty else 0.0,
            },
            "market_volatility": {
                "early_mean": float(early["rolling_volatility"].dropna().mean()) if not early["rolling_volatility"].dropna().empty else 0.0,
                "late_mean": float(late["rolling_volatility"].dropna().mean()) if not late["rolling_volatility"].dropna().empty else 0.0,
            },
            "liquidity": {
                "early_mean": float(early["liquidity"].mean()) if "liquidity" in early else 0.0,
                "late_mean": float(late["liquidity"].mean()) if "liquidity" in late else 0.0,
            },
        }
        if not trade_log.empty and "edge_entry" in trade_log.columns:
            executions = trade_log[trade_log["event"] == "execution"].copy()
            if not executions.empty:
                exec_midpoint = max(len(executions) // 2, 1)
                report["edge"] = {
                    "early_mean": float(pd.to_numeric(executions.iloc[:exec_midpoint]["edge_entry"], errors="coerce").mean()),
                    "late_mean": float(pd.to_numeric(executions.iloc[exec_midpoint:]["edge_entry"], errors="coerce").mean()),
                }
        return report

    @staticmethod
    def _load_trade_log(path: Path) -> pd.DataFrame:
        """Load the trade log if present."""

        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    @staticmethod
    def _load_market_metadata(path: Path) -> pd.DataFrame:
        """Load market metadata if present."""

        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    @staticmethod
    def _distribution(values: pd.Series) -> dict[str, float]:
        """Return a compact numeric distribution summary."""

        clean = pd.to_numeric(values, errors="coerce").dropna()
        if clean.empty:
            return {}
        return {
            "p50": float(clean.quantile(0.50)),
            "p90": float(clean.quantile(0.90)),
            "p99": float(clean.quantile(0.99)),
            "max": float(clean.max()),
            "mean": float(clean.mean()),
        }

    @staticmethod
    def _events_per_minute(frame: pd.DataFrame, time_column: str) -> dict[str, float]:
        """Compute events per minute distribution for a dataset."""

        if frame.empty:
            return {}
        grouped = frame.set_index(time_column).resample("1min").size()
        return {
            "mean": float(grouped.mean()),
            "p90": float(grouped.quantile(0.90)),
            "max": float(grouped.max()),
        }

    @staticmethod
    def _latency_distribution(frame: pd.DataFrame) -> dict[str, float]:
        """Compute ingestion latency distribution in seconds."""

        if frame.empty:
            return {}
        latency = (frame["ingestion_time"] - frame["event_time"]).dt.total_seconds()
        return DataQualityAuditor._distribution(latency)

    @staticmethod
    def _missing_interval_count(frame: pd.DataFrame, key_column: str, gap_threshold: int) -> int:
        """Count intervals that exceed the configured gap threshold."""

        if frame.empty:
            return 0
        intervals = (
            frame.sort_values([key_column, "event_time"])
            .groupby(key_column, observed=False)["event_time"]
            .diff()
            .dropna()
            .dt.total_seconds()
        )
        return int((intervals > gap_threshold).sum())

    def _write_report(self, report: dict[str, Any]) -> None:
        """Write the audit JSON report to disk."""

        path = self.config.data.audit_report_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)

    @staticmethod
    def _log_summary(report: dict[str, Any]) -> None:
        """Emit high-signal audit summary logs."""

        LOGGER.info(
            "Audit summary: market_gaps=%s crypto_gaps=%s replay_market_pass=%s replay_crypto_pass=%s",
            report.get("global_intervals", {}).get("market", {}).get("gaps_above_threshold", 0),
            report.get("global_intervals", {}).get("crypto", {}).get("gaps_above_threshold", 0),
            report.get("replay_integrity", {}).get("market", {}).get("passed", False),
            report.get("replay_integrity", {}).get("crypto", {}).get("passed", False),
        )
