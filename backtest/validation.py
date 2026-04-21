"""Validation and falsification experiment runner."""

from __future__ import annotations

import json
from dataclasses import replace

import numpy as np

from backtest.engine import BacktestEngine
from features.builder import FeatureBuilder
from features.store import FeatureStore
from ingestion.data_store import DataBundle, DataStore
from project.configuration import AppConfig, DataConfig, RuntimeConfig, TradingConfig, clone_config


class ValidationRunner:
    """Run baseline and falsification experiments using the shared backtest pipeline."""

    def __init__(self, config: AppConfig) -> None:
        """Bind the runner to a base config."""

        self.config = config
        self.bundle = DataStore(config).load()

    def run(self, mode: str) -> dict[str, object]:
        """Run one or more validation modes and write a summary report."""

        if mode == "all":
            modes = ["stress", "shuffle", "holdout", "diagnostics"]
        else:
            modes = [mode]

        results: dict[str, object] = {}
        baseline_result = self._run_experiment("baseline", split_mode="walk_forward", persist_artifacts=True)
        results["baseline"] = baseline_result.metrics

        if "stress" in modes:
            stress_result = self._run_stress()
            results["stress"] = {
                "metrics": stress_result.metrics,
                "degradation_return": stress_result.metrics["total_return"] - baseline_result.metrics["total_return"],
                "degradation_sharpe": stress_result.metrics["sharpe_ratio"] - baseline_result.metrics["sharpe_ratio"],
            }

        if "shuffle" in modes:
            results["shuffle"] = self._run_shuffle()

        if "holdout" in modes:
            holdout_result = self._run_experiment("strict_holdout", split_mode="strict_holdout", persist_artifacts=True)
            results["holdout"] = holdout_result.metrics

        if "diagnostics" in modes:
            results["diagnostics"] = {
                "metrics": baseline_result.metrics,
                "sample_trade_count": len(baseline_result.trade_log),
                "sample_prediction_count": len(baseline_result.prediction_log),
            }

        self._write_summary(results)
        return results

    def _run_stress(self):
        """Run a stressed-frictions experiment."""

        stressed_trading = replace(
            self.config.trading,
            slippage_bps=self.config.trading.slippage_bps * self.config.validation.stress_slippage_multiplier,
            fee_rate=self.config.trading.fee_rate * self.config.validation.stress_fee_multiplier,
        )
        stressed_config = clone_config(
            self.config,
            trading=stressed_trading,
        )
        stressed_bundle = DataBundle(
            market_metadata=self.bundle.market_metadata.copy(),
            market_snapshots=self.bundle.market_snapshots.assign(
                liquidity=self.bundle.market_snapshots["liquidity"] * self.config.validation.stress_liquidity_haircut
            ),
            crypto_snapshots=self.bundle.crypto_snapshots.copy(),
        )
        return self._run_experiment(
            "stress",
            config=stressed_config,
            bundle=stressed_bundle,
            split_mode="walk_forward",
            persist_artifacts=True,
        )

    def _run_shuffle(self) -> dict[str, object]:
        """Run repeated market-label shuffles to falsify leakage."""

        market_ids = self.bundle.market_metadata["market_id"].tolist()
        labels = self.bundle.market_metadata["outcome_yes"].astype(int).to_numpy()
        rng = np.random.default_rng(self.config.validation.shuffle_seed)
        returns: list[float] = []
        sharpes: list[float] = []
        win_rates: list[float] = []
        for repeat in range(self.config.validation.shuffle_repeats):
            shuffled = rng.permutation(labels)
            label_map = dict(zip(market_ids, shuffled.tolist(), strict=False))
            result = self._run_experiment(
                f"shuffle_{repeat + 1}",
                split_mode="walk_forward",
                label_map=label_map,
                persist_artifacts=False,
            )
            returns.append(float(result.metrics["total_return"]))
            sharpes.append(float(result.metrics["sharpe_ratio"]))
            win_rates.append(float(result.metrics["win_rate"]))
        mean_return = float(np.mean(returns)) if returns else 0.0
        return {
            "mean_total_return": mean_return,
            "mean_sharpe_ratio": float(np.mean(sharpes)) if sharpes else 0.0,
            "mean_win_rate": float(np.mean(win_rates)) if win_rates else 0.0,
            "returns": returns,
            "falsification_failed": mean_return > 0.02,
        }

    def _run_experiment(
        self,
        experiment_id: str,
        *,
        config: AppConfig | None = None,
        bundle: DataBundle | None = None,
        split_mode: str,
        label_map: dict[str, int] | None = None,
        persist_artifacts: bool,
    ):
        """Run a single experiment with isolated cache/report paths."""

        active_config = self._experiment_config(config or self.config, experiment_id)
        feature_store = FeatureStore(active_config.data.feature_cache_path, active_config.runtime.feature_schema_version)
        feature_store.clear()
        engine = BacktestEngine(
            config=active_config,
            bundle=bundle or self.bundle,
            feature_builder=FeatureBuilder(active_config.runtime.feature_schema_version),
            feature_store=feature_store,
        )
        return engine.run(
            persist_artifacts=persist_artifacts,
            split_mode=split_mode,
            experiment_id=experiment_id,
            label_map=label_map,
            metrics_path=active_config.data.metrics_report_path,
            trade_log_path=active_config.data.trade_log_path,
            prediction_path=active_config.data.prediction_report_path,
        )

    def _experiment_config(self, config: AppConfig, experiment_id: str) -> AppConfig:
        """Create an experiment-scoped config with isolated cache and report paths."""

        report_root = self.config.data.validation_report_path.parent / experiment_id
        data = DataConfig(
            market_metadata_path=config.data.market_metadata_path,
            market_snapshots_path=config.data.market_snapshots_path,
            crypto_snapshots_path=config.data.crypto_snapshots_path,
            raw_storage_root=config.data.raw_storage_root,
            audit_report_path=config.data.audit_report_path,
            feature_cache_path=report_root / "feature_cache.csv",
            trade_log_path=report_root / "trade_log.csv",
            metrics_report_path=report_root / "metrics.json",
            prediction_report_path=report_root / "predictions.csv",
            validation_report_path=config.data.validation_report_path,
            model_artifact_path=config.data.model_artifact_path,
            scaler_artifact_path=config.data.scaler_artifact_path,
            calibrator_artifact_path=config.data.calibrator_artifact_path,
            training_metadata_path=config.data.training_metadata_path,
        )
        runtime = RuntimeConfig(
            live_sim_sleep_seconds=config.runtime.live_sim_sleep_seconds,
            log_level=config.runtime.log_level,
            feature_schema_version=f"{config.runtime.feature_schema_version}:{experiment_id}",
        )
        return clone_config(config, data=data, runtime=runtime)

    def _write_summary(self, results: dict[str, object]) -> None:
        """Write the validation summary report to disk."""

        output_path = self.config.data.validation_report_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2, default=str)
