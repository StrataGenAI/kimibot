"""Shared filters reused by both live and historical ingestion paths."""

from __future__ import annotations

from typing import Iterable


def is_crypto_market(
    slug: str | None,
    ticker_allowlist: Iterable[str],
    mode: str = "auto",
) -> bool:
    """Return True iff ``slug`` begins with a known crypto ticker.

    Matches ``ticker`` exactly or ``ticker-<rest>`` at the slug start.
    ``mode="off"`` disables filtering (returns True for any slug).
    """
    if mode == "off":
        return True
    s = str(slug or "").lower()
    if not s:
        return False
    for ticker in ticker_allowlist:
        t = str(ticker).lower()
        if not t:
            continue
        if s == t or s.startswith(f"{t}-"):
            return True
    return False
