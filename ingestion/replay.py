"""Sequential replay sources for backtest and simulated live trading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from ingestion.data_store import DataBundle


@dataclass(frozen=True)
class ReplayEvent:
    """A single replay event containing all market rows at a timestamp."""

    timestamp: datetime
    market_rows: pd.DataFrame


class HistoricalReplaySource:
    """Yield market snapshots grouped by timestamp in chronological order."""

    def __init__(self, bundle: DataBundle) -> None:
        """Bind the replay source to a loaded data bundle."""

        self.bundle = bundle

    def iter_events(self, market_ids: list[str] | None = None) -> list[ReplayEvent]:
        """Return a deterministic list of replay events."""

        market = self.bundle.market_snapshots
        if market_ids is not None:
            market = market[market["market_id"].isin(market_ids)]
        events: list[ReplayEvent] = []
        for timestamp, rows in market.groupby("timestamp", sort=True):
            events.append(ReplayEvent(timestamp=timestamp, market_rows=rows.copy()))
        return events
