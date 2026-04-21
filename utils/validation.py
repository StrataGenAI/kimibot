"""Raw ingestion row validation helpers."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from utils.time_utils import parse_utc_timestamp


def _split_valid_invalid(frame: pd.DataFrame, valid_mask: pd.Series, reason: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a frame into valid and rejected rows with a rejection reason."""

    valid = frame[valid_mask].copy()
    rejected = frame[~valid_mask].copy()
    if not rejected.empty:
        rejected["validation_error"] = reason
    return valid, rejected


def validate_limitless_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate Limitless raw rows against required schema and ranges."""

    if frame.empty:
        return frame.copy(), frame.copy()
    normalized = frame.copy()
    normalized["timestamp"] = normalized["timestamp"].map(parse_utc_timestamp)
    critical = ["market_id", "timestamp", "yes_price", "volume", "liquidity"]
    valid_mask = normalized[critical].notnull().all(axis=1)
    valid, rejected = _split_valid_invalid(normalized, valid_mask, "null_critical_field")
    if valid.empty:
        return valid, pd.concat([rejected], ignore_index=True)

    price_mask = valid["yes_price"].astype(float).between(0.0, 1.0, inclusive="both")
    valid_price, rejected_price = _split_valid_invalid(valid, price_mask, "invalid_yes_price")
    valid_price["volume"] = valid_price["volume"].astype(float)
    valid_price["liquidity"] = valid_price["liquidity"].astype(float)
    non_negative_mask = (valid_price["volume"] >= 0.0) & (valid_price["liquidity"] >= 0.0)
    valid_final, rejected_non_negative = _split_valid_invalid(valid_price, non_negative_mask, "negative_volume_or_liquidity")
    monotonic_mask = valid_final.sort_values(["market_id", "timestamp"]).groupby("market_id")["timestamp"].diff().fillna(pd.Timedelta(seconds=1)) >= pd.Timedelta(0)
    valid_monotonic, rejected_monotonic = _split_valid_invalid(valid_final.sort_values(["market_id", "timestamp"]), monotonic_mask, "non_monotonic_timestamp")
    rejected_all = pd.concat([rejected, rejected_price, rejected_non_negative, rejected_monotonic], ignore_index=True)
    return valid_monotonic.reset_index(drop=True), rejected_all.reset_index(drop=True)


def validate_crypto_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate BTC/ETH raw rows against required schema and ranges."""

    if frame.empty:
        return frame.copy(), frame.copy()
    normalized = frame.copy()
    normalized["timestamp"] = normalized["timestamp"].map(parse_utc_timestamp)
    critical = ["symbol", "timestamp", "price"]
    valid_mask = normalized[critical].notnull().all(axis=1)
    valid, rejected = _split_valid_invalid(normalized, valid_mask, "null_critical_field")
    if valid.empty:
        return valid, pd.concat([rejected], ignore_index=True)

    price_mask = valid["price"].astype(float) > 0.0
    valid_price, rejected_price = _split_valid_invalid(valid, price_mask, "invalid_price")
    if "volume" in valid_price.columns:
        valid_price["volume"] = pd.to_numeric(valid_price["volume"], errors="coerce").fillna(0.0)
        non_negative_mask = valid_price["volume"] >= 0.0
        valid_final, rejected_volume = _split_valid_invalid(valid_price, non_negative_mask, "negative_volume")
    else:
        valid_final = valid_price
        rejected_volume = pd.DataFrame(columns=list(valid.columns) + ["validation_error"])
    monotonic_mask = valid_final.sort_values(["symbol", "timestamp"]).groupby("symbol")["timestamp"].diff().fillna(pd.Timedelta(seconds=1)) >= pd.Timedelta(0)
    valid_monotonic, rejected_monotonic = _split_valid_invalid(valid_final.sort_values(["symbol", "timestamp"]), monotonic_mask, "non_monotonic_timestamp")
    rejected_all = pd.concat([rejected, rejected_price, rejected_volume, rejected_monotonic], ignore_index=True)
    return valid_monotonic.reset_index(drop=True), rejected_all.reset_index(drop=True)
