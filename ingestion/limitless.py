"""Backward-compatible export for the newer Limitless client."""

from ingestion.limitless_client import LimitlessClient as LimitlessAPIClient

__all__ = ["LimitlessAPIClient"]
