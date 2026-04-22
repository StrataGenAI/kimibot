"""Resolution scanner — polls Limitless for resolved-market metadata.

Reads the live-ingest sidecar to discover slugs of crypto markets we've
seen. For each whose ``resolution_time`` has passed but which we have
not yet captured, calls ``GET /markets/{slug}`` and, if the market is
``RESOLVED``, appends one row to ``data/resolved_markets.parquet``.

Writes are atomic: the scanner writes to a temp file and ``os.replace``s
onto the final path. Running twice does not duplicate rows — already
captured ``market_id``s are skipped.

See ``docs/resolved_markets_schema.md`` for the column contract.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ingestion.filters import is_crypto_market
from ingestion.limitless_client import LimitlessClient
from project.configuration import AppConfig, IngestionConfig

LOGGER = logging.getLogger(__name__)

CAPTURE_METHOD = "scanner_v1"

SCHEMA_COLUMNS: list[str] = [
    "market_id",
    "slug",
    "condition_id",
    "category_tags",
    "expiration_timestamp",
    "resolved_at",
    "winning_outcome_index",
    "final_yes_price",
    "final_no_price",
    "volume_total",
    "liquidity_at_resolution",
    "first_seen",
    "capture_method",
]


@dataclass
class ScanReport:
    """Summary of one scanner run."""

    candidates: int = 0
    already_captured: int = 0
    fetched: int = 0
    resolved: int = 0
    still_active: int = 0
    errors: int = 0
    duration_seconds: float = 0.0
    skipped_non_crypto: int = 0
    written_rows: int = 0
    error_slugs: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates": self.candidates,
            "already_captured": self.already_captured,
            "fetched": self.fetched,
            "resolved": self.resolved,
            "still_active": self.still_active,
            "errors": self.errors,
            "duration_seconds": round(self.duration_seconds, 2),
            "skipped_non_crypto": self.skipped_non_crypto,
            "written_rows": self.written_rows,
            "error_slugs": self.error_slugs[:10],
        }


def _sidecar_path(config: AppConfig) -> Path:
    return Path(config.data.raw_storage_root) / "limitless" / "market_metadata.parquet"


def _resolved_path(config: AppConfig) -> Path:
    # resolved_markets.parquet lives next to other top-level data files,
    # not inside raw_storage_root/limitless. Derive from the parent of
    # raw_storage_root so this tracks moves of the data/ directory.
    return Path(config.data.raw_storage_root).parent / "resolved_markets.parquet"


def _now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_volume(payload: dict[str, Any]) -> float:
    v = payload.get("volumeFormatted")
    if v is None:
        v = payload.get("volume")
    return _coerce_float(v, default=0.0)


def _extract_liquidity(payload: dict[str, Any]) -> float:
    for key in ("liquidity", "liquidity24h", "open_interest", "openInterest"):
        v = payload.get(key)
        if v is not None:
            return _coerce_float(v, default=0.0)
    return 0.0


def _extract_prices(payload: dict[str, Any]) -> tuple[float, float]:
    prices = payload.get("prices")
    if isinstance(prices, list) and len(prices) >= 2:
        # Limitless occasionally scales [0, 100] instead of [0, 1]
        yes = _coerce_float(prices[0])
        no = _coerce_float(prices[1])
        if yes > 1.0 or no > 1.0:
            yes, no = yes / 100.0, no / 100.0
        return yes, no
    if isinstance(prices, dict):
        return _coerce_float(prices.get("yes")), _coerce_float(prices.get("no"))
    return 0.0, 0.0


def _extract_expiration_timestamp(payload: dict[str, Any]) -> pd.Timestamp | None:
    raw = payload.get("expirationTimestamp")
    if raw is None:
        return None
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return None
    # API returns unix-ms; detect and normalise
    unit = "ms" if val > 1e12 else "s"
    try:
        return pd.to_datetime(val, unit=unit, utc=True)
    except (ValueError, OverflowError):
        return None


def _merge_category_tags(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for key in ("categories", "tags"):
        items = payload.get(key) or []
        if not isinstance(items, list):
            continue
        for item in items:
            s = str(item)
            if s and s not in seen:
                seen.add(s)
                out.append(s)
    return out


def _decide_winning_outcome(payload: dict[str, Any]) -> int:
    """Return 0 (YES), 1 (NO), or -1 (invalid/refunded)."""
    raw = payload.get("winningOutcomeIndex")
    idx = _coerce_int(raw)
    if idx in (0, 1):
        return idx
    # Fall back to prices — resolved markets canonically have [1, 0] or [0, 1]
    yes, no = _extract_prices(payload)
    if yes >= 0.999 and no <= 0.001:
        return 0
    if no >= 0.999 and yes <= 0.001:
        return 1
    return -1


def _load_candidates(
    sidecar_path: Path,
    ingestion_cfg: IngestionConfig,
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Load markets from the live sidecar filtered to crypto + past-expiration.

    Returns a DataFrame with columns ``market_id, slug, resolution_time,
    first_seen``. Empty if the sidecar is missing.
    """
    if not sidecar_path.exists():
        return pd.DataFrame(
            columns=["market_id", "slug", "resolution_time", "first_seen"]
        )
    df = pd.read_parquet(sidecar_path)
    required = {"market_id", "slug", "resolution_time", "first_seen"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Sidecar {sidecar_path} missing columns: {sorted(missing)}"
        )
    # Crypto filter
    df["_is_crypto"] = df["slug"].apply(
        lambda s: is_crypto_market(
            s,
            ingestion_cfg.crypto_ticker_allowlist,
            ingestion_cfg.crypto_filter_mode,
        )
    )
    crypto = df[df["_is_crypto"]].drop(columns=["_is_crypto"]).copy()
    # Past-expiration
    now = now if now is not None else _now_utc()
    if not pd.api.types.is_datetime64_any_dtype(crypto["resolution_time"]):
        crypto["resolution_time"] = pd.to_datetime(
            crypto["resolution_time"], utc=True, errors="coerce"
        )
    past = crypto[
        crypto["resolution_time"].notna() & (crypto["resolution_time"] < now)
    ].copy()
    return past[["market_id", "slug", "resolution_time", "first_seen"]]


def _load_existing_resolved(path: Path) -> tuple[pd.DataFrame, set[str]]:
    """Return (existing df, set of already-captured market_ids). Empty if absent."""
    if not path.exists():
        return pd.DataFrame(columns=SCHEMA_COLUMNS), set()
    df = pd.read_parquet(path)
    captured = set(df["market_id"].astype(str).tolist()) if "market_id" in df else set()
    return df, captured


def _build_row(
    sidecar_row: dict[str, Any],
    payload: dict[str, Any],
    resolved_at: pd.Timestamp,
) -> dict[str, Any]:
    yes, no = _extract_prices(payload)
    exp_ts = _extract_expiration_timestamp(payload)
    if exp_ts is None:
        # fall back to the sidecar's resolution_time
        exp_ts = pd.to_datetime(sidecar_row["resolution_time"], utc=True)
    return {
        "market_id": str(payload.get("id") or sidecar_row["market_id"]),
        "slug": str(payload.get("slug") or sidecar_row["slug"]),
        "condition_id": str(payload.get("conditionId") or ""),
        "category_tags": _merge_category_tags(payload),
        "expiration_timestamp": exp_ts,
        "resolved_at": resolved_at,
        "winning_outcome_index": _decide_winning_outcome(payload),
        "final_yes_price": yes,
        "final_no_price": no,
        "volume_total": _extract_volume(payload),
        "liquidity_at_resolution": _extract_liquidity(payload),
        "first_seen": pd.to_datetime(sidecar_row["first_seen"], utc=True),
        "capture_method": CAPTURE_METHOD,
    }


def _normalise_existing_for_concat(existing: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return pd.DataFrame(columns=SCHEMA_COLUMNS)
    # Keep only schema columns in the correct order, add missing ones as NA.
    out = existing.copy()
    for col in SCHEMA_COLUMNS:
        if col not in out.columns:
            out[col] = None
    return out[SCHEMA_COLUMNS]


def _atomic_write(df: pd.DataFrame, path: Path) -> None:
    """Write ``df`` to ``path`` atomically via ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f"{path.name}.tmp_{os.getpid()}_{time.time_ns()}"
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def scan_resolutions(
    config: AppConfig,
    dry_run: bool = False,
    *,
    client_factory: Callable[[IngestionConfig], LimitlessClient] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: pd.Timestamp | None = None,
) -> ScanReport:
    """Single-pass scan. Returns a ``ScanReport``.

    ``client_factory`` and ``sleep`` are overridable for testing.
    """
    started = time.monotonic()
    report = ScanReport()

    sidecar_path = _sidecar_path(config)
    resolved_path = _resolved_path(config)
    rate_per_second = config.ingestion.resolution_scanner_rate_per_second

    candidates = _load_candidates(
        sidecar_path, config.ingestion, now=now
    )
    report.candidates = int(len(candidates))

    existing_df, already_captured = _load_existing_resolved(resolved_path)
    report.already_captured = int(len(already_captured))

    todo = candidates[~candidates["market_id"].astype(str).isin(already_captured)]
    LOGGER.info(
        "scanner: candidates=%d already_captured=%d to_fetch=%d",
        report.candidates, report.already_captured, int(len(todo)),
    )

    if client_factory is None:
        client_factory = lambda cfg: LimitlessClient(config=cfg)
    client = client_factory(config.ingestion)

    min_interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
    new_rows: list[dict[str, Any]] = []
    last_call = 0.0

    for row in todo.to_dict(orient="records"):
        slug = row.get("slug") or ""
        if not slug:
            report.errors += 1
            continue

        # rate limit
        elapsed = time.monotonic() - last_call
        if elapsed < min_interval:
            sleep(min_interval - elapsed)

        try:
            payload = client.fetch_market_by_slug(slug)
            last_call = time.monotonic()
            report.fetched += 1
        except Exception as exc:
            LOGGER.warning("scanner fetch failed slug=%s: %s", slug, exc)
            report.errors += 1
            report.error_slugs.append(slug)
            last_call = time.monotonic()
            continue

        status = str(payload.get("status") or "").upper()
        if status != "RESOLVED":
            LOGGER.info(
                "scanner: %s still status=%s despite expired timestamp; will retry",
                slug, status or "<unknown>",
            )
            report.still_active += 1
            continue

        try:
            new_rows.append(_build_row(row, payload, _now_utc()))
            report.resolved += 1
        except Exception as exc:
            LOGGER.warning("scanner row-build failed slug=%s: %s", slug, exc)
            report.errors += 1
            report.error_slugs.append(slug)

    if new_rows and not dry_run:
        new_df = pd.DataFrame(new_rows, columns=SCHEMA_COLUMNS)
        combined = pd.concat(
            [_normalise_existing_for_concat(existing_df), new_df],
            ignore_index=True,
        )
        combined = combined.sort_values("expiration_timestamp").reset_index(drop=True)
        _atomic_write(combined, resolved_path)
        report.written_rows = int(len(new_df))

    report.duration_seconds = time.monotonic() - started
    return report
