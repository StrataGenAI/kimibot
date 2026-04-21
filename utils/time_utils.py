"""UTC parsing, alignment, and partition helpers for ingestion."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def ensure_utc(ts: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""

    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def parse_utc_timestamp(value: str | int | float | datetime | pd.Timestamp) -> datetime:
    """Parse arbitrary timestamp values into UTC datetimes.

    Integers and floats are treated as Unix epoch seconds.
    Callers must not pass NaN; use na_action='ignore' in pandas .map() calls.
    """

    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, (int, float)):
        if value != value:  # NaN check without importing math
            raise ValueError("Cannot parse NaN as a UTC timestamp")
        return ensure_utc(datetime.fromtimestamp(float(value), timezone.utc))
    return ensure_utc(pd.Timestamp(value).to_pydatetime())


def utc_now() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc)


def date_key(ts: datetime) -> str:
    """Return a YYYY-MM-DD partition key for a UTC timestamp."""

    return ensure_utc(ts).strftime("%Y-%m-%d")


def align_timestamp(ts: datetime, frequency: str = "1s") -> datetime:
    """Align a timestamp down to a pandas-supported frequency boundary."""

    return ensure_utc(pd.Timestamp(ensure_utc(ts)).floor(frequency).to_pydatetime())
