"""Time parsing and normalization helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def ensure_utc(ts: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""

    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def parse_utc_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into UTC."""

    return ensure_utc(pd.Timestamp(value).to_pydatetime())


def date_key(ts: datetime) -> str:
    """Return a YYYY-MM-DD date key for daily risk tracking."""

    return ensure_utc(ts).strftime("%Y-%m-%d")
