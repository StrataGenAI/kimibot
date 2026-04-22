"""Configuration loading and validation."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml


def _substitute_env_vars(value: Any) -> Any:
    """Substitute {{VAR}} patterns with environment variables."""
    if isinstance(value, str):
        pattern = r"\{\{(\w+)\}\}"
        matches = re.findall(pattern, value)
        for var_name in matches:
            env_value = os.environ.get(var_name, "")
            value = value.replace("{{" + var_name + "}}", env_value)
        return value
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(v) for v in value]
    return value


@dataclass(frozen=True)
class DataConfig:
    market_metadata_path: Path
    market_snapshots_path: Path
    crypto_snapshots_path: Path
    raw_storage_root: Path
    audit_report_path: Path
    feature_cache_path: Path
    trade_log_path: Path
    metrics_report_path: Path
    prediction_report_path: Path
    validation_report_path: Path
    model_artifact_path: Path
    scaler_artifact_path: Path
    calibrator_artifact_path: Path
    training_metadata_path: Path
    source_mode: str = "synthetic"


@dataclass(frozen=True)
class TradingConfig:
    initial_capital: float = 10000.0
    edge_threshold: float = 0.03
    min_edge: float = 0.02
    max_edge: float = 0.15
    min_expected_value: float = 0.01
    min_liquidity: float = 500.0
    max_position_per_market: float = 800.0
    max_total_exposure: float = 3500.0
    daily_loss_limit: float = 800.0
    fee_rate: float = 0.01
    slippage_bps: float = 40.0
    latency_seconds: float = 2.0
    price_velocity_lookback_minutes: int = 2
    position_size_multiplier: float = 0.10
    trade_cooldown_seconds: int = 60
    # Phase A escape hatch: skip the min_liquidity gate so signals flow while
    # the Limitless public API exposes no liquidity field. Defaults False so
    # existing configs retain the safe behavior. MUST be False before live
    # trading — see ingestion/limitless_client.py::fetch_orderbook_depth.
    paper_mode_unsafe_liquidity: bool = False


@dataclass(frozen=True)
class WalkForwardConfig:
    min_resolved_markets: int = 2
    test_markets_per_fold: int = 2
    min_training_rows: int = 8


@dataclass(frozen=True)
class RuntimeConfig:
    live_sim_sleep_seconds: float = 0.0
    log_level: str = "INFO"
    feature_schema_version: str = "v2"
    ingestion_enabled: bool = True
    live_sim_mode: str = "walk_forward"


@dataclass(frozen=True)
class ValidationConfig:
    stress_slippage_multiplier: float = 2.0
    stress_fee_multiplier: float = 2.0
    stress_liquidity_haircut: float = 0.5
    shuffle_seed: int = 17
    shuffle_repeats: int = 5
    holdout_test_markets: int = 2
    calibration_bins: int = 5
    calibration_tolerance: float = 0.15
    min_trade_count: int = 50
    warn_sharpe_threshold: float = 3.0
    warn_win_rate_threshold: float = 0.80
    warn_concentration_threshold: float = 0.30


@dataclass(frozen=True)
class CalibrationConfig:
    method: str = "sigmoid"
    min_calibration_markets: int = 1
    min_calibration_rows: int = 4
    target_ece: float = 0.05


@dataclass(frozen=True)
class IngestionConfig:
    limitless_rest_base_url: str = ""
    limitless_ws_url: str = ""
    limitless_api_key: str = ""
    limitless_private_key: str = ""
    graph_api_key: str = ""
    limitless_discovery_interval_seconds: int = 60
    limitless_poll_interval_seconds: int = 2
    crypto_rest_base_url: str = "https://api.binance.com"
    binance_api_key: str = ""
    binance_api_secret: str = ""
    crypto_poll_interval_seconds: int = 1
    flush_interval_seconds: int = 5
    retry_base_delay_seconds: float = 1.0
    retry_max_delay_seconds: float = 16.0
    market_allowlist: list[str] = None
    market_denylist: list[str] = None
    # Pagination for /markets/active. Limitless enforces server-side page
    # size (currently 25); page_size is the expected size used to detect
    # the last page.
    pagination_page_size: int = 25
    pagination_delay_seconds: float = 0.2
    pagination_max_pages: int = 50
    # Crypto filter. "auto" and "slug_regex" both use the ticker allowlist;
    # "off" disables filtering (debug only).
    crypto_filter_mode: str = "auto"
    crypto_ticker_allowlist: list[str] = None
    # Safety cap on snapshot fetches per cycle (protects against a large
    # universe when crypto_filter_mode=off).
    max_snapshots_per_cycle: int = 300

    def __post_init__(self):
        if self.market_allowlist is None:
            object.__setattr__(self, "market_allowlist", [])
        if self.market_denylist is None:
            object.__setattr__(self, "market_denylist", [])
        if not self.crypto_ticker_allowlist:
            object.__setattr__(
                self,
                "crypto_ticker_allowlist",
                [
                    "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
                    "xrp", "ripple", "doge", "dogecoin", "ada", "cardano",
                    "avax", "avalanche", "bnb", "hype", "sui", "tao",
                    "ton", "trx", "tron", "link", "chainlink",
                    "dot", "polkadot", "matic", "polygon",
                    "arb", "arbitrum", "op", "optimism",
                    "atom", "cosmos", "near", "ltc", "litecoin", "bch",
                    "xlm", "stellar", "uni", "uniswap", "pepe", "shib",
                    "wif", "bonk", "fet", "inj", "apt", "sei", "tia",
                    "stx", "mnt", "mantle", "ena", "ondo", "jup", "wld",
                    "worldcoin", "fil", "filecoin", "icp", "hbar", "vet",
                    "algo", "xmr", "monero", "etc",
                ],
            )


@dataclass(frozen=True)
class AuditConfig:
    resample_frequency: str = "10s"
    expected_market_interval_seconds: int = 10
    expected_crypto_interval_seconds: int = 5
    gap_threshold_seconds: int = 30
    alignment_threshold_seconds: int = 3
    replay_integrity_samples: int = 10
    price_jump_threshold: float = 0.3
    lifecycle_late_only_ratio: float = 0.5
    realistic_fill_fraction: float = 0.15


@dataclass(frozen=True)
class AppConfig:
    data: DataConfig
    trading: TradingConfig
    walk_forward: WalkForwardConfig
    runtime: RuntimeConfig
    validation: ValidationConfig
    calibration: CalibrationConfig
    ingestion: IngestionConfig
    audit: AuditConfig


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (base_dir / path).resolve()


def _read_raw_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        if path.suffix.lower() == ".json":
            return json.load(handle)
        raw = yaml.safe_load(handle)
    return _substitute_env_vars(raw)


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).resolve()
    raw = _read_raw_config(config_path)
    base_dir = (
        config_path.parent.parent
        if config_path.parent.name == "config"
        else config_path.parent
    )

    data = raw["data"]
    trading = raw["trading"]
    walk_forward = raw["walk_forward"]
    runtime = raw["runtime"]
    validation = raw["validation"]
    calibration = raw["calibration"]
    ingestion = raw["ingestion"]
    audit = raw["audit"]

    return AppConfig(
        data=DataConfig(
            market_metadata_path=_resolve_path(base_dir, data["market_metadata_path"]),
            market_snapshots_path=_resolve_path(
                base_dir, data["market_snapshots_path"]
            ),
            crypto_snapshots_path=_resolve_path(
                base_dir, data["crypto_snapshots_path"]
            ),
            raw_storage_root=_resolve_path(base_dir, data["raw_storage_root"]),
            audit_report_path=_resolve_path(base_dir, data["audit_report_path"]),
            feature_cache_path=_resolve_path(base_dir, data["feature_cache_path"]),
            trade_log_path=_resolve_path(base_dir, data["trade_log_path"]),
            metrics_report_path=_resolve_path(base_dir, data["metrics_report_path"]),
            prediction_report_path=_resolve_path(
                base_dir, data["prediction_report_path"]
            ),
            validation_report_path=_resolve_path(
                base_dir, data["validation_report_path"]
            ),
            model_artifact_path=_resolve_path(base_dir, data["model_artifact_path"]),
            scaler_artifact_path=_resolve_path(base_dir, data["scaler_artifact_path"]),
            calibrator_artifact_path=_resolve_path(
                base_dir, data["calibrator_artifact_path"]
            ),
            training_metadata_path=_resolve_path(
                base_dir, data["training_metadata_path"]
            ),
            source_mode=data.get("source_mode", "synthetic"),
        ),
        trading=TradingConfig(**trading),
        walk_forward=WalkForwardConfig(**walk_forward),
        runtime=RuntimeConfig(**runtime),
        validation=ValidationConfig(**validation),
        calibration=CalibrationConfig(**calibration),
        ingestion=IngestionConfig(**ingestion),
        audit=AuditConfig(**audit),
    )


def clone_config(
    config: AppConfig,
    *,
    data: DataConfig | None = None,
    trading: TradingConfig | None = None,
    walk_forward: WalkForwardConfig | None = None,
    runtime: RuntimeConfig | None = None,
    validation: ValidationConfig | None = None,
    calibration: CalibrationConfig | None = None,
    ingestion: IngestionConfig | None = None,
    audit: AuditConfig | None = None,
) -> AppConfig:
    return replace(
        config,
        data=data or config.data,
        trading=trading or config.trading,
        walk_forward=walk_forward or config.walk_forward,
        runtime=runtime or config.runtime,
        validation=validation or config.validation,
        calibration=calibration or config.calibration,
        ingestion=ingestion or config.ingestion,
        audit=audit or config.audit,
    )
