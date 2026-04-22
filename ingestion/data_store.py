"""Historical data loading and normalized storage access.

Two source modes are supported (see ``config.data.source_mode``):

* ``synthetic`` — reads the bundled CSV fixtures under ``data/``.
* ``live``      — reads append-only Parquet partitions under ``data/raw/``
                  written by the ingestion loop, plus a metadata sidecar
                  at ``data/raw/limitless/market_metadata.parquet``.

Column names returned to consumers are identical across modes. The live
path renames ``yes_price`` → ``p_market`` and pivots the long-format crypto
parquet (rows keyed on ``symbol``) into the wide
``{btc_price, eth_price}`` shape consumers expect.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from project.configuration import AppConfig
from utils.time_utils import parse_utc_timestamp


_LIVE_MARKET_COLUMNS = ["market_id", "timestamp", "p_market", "volume", "liquidity"]
_LIVE_CRYPTO_COLUMNS = ["timestamp", "btc_price", "eth_price"]
_METADATA_COLUMNS = [
    "market_id",
    "slug",
    "status",
    "resolution_time",
    "outcome_yes",
    "resolved",
    "first_seen",
    "last_seen",
]


@dataclass
class DataBundle:
    """Normalized in-memory tables used by the pipeline."""

    market_metadata: pd.DataFrame
    market_snapshots: pd.DataFrame
    crypto_snapshots: pd.DataFrame


class DataStore:
    """Load normalized market and crypto data from disk."""

    def __init__(self, config: AppConfig) -> None:
        """Create a data store bound to application config."""

        self.config = config

    @property
    def source_mode(self) -> str:
        return getattr(self.config.data, "source_mode", "synthetic")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> DataBundle:
        """Load and normalize all configured datasets."""

        return DataBundle(
            market_metadata=self.load_market_metadata(),
            market_snapshots=self.load_market_snapshots(),
            crypto_snapshots=self.load_crypto_snapshots(),
        )

    def load_market_snapshots(
        self, lookback_hours: int | None = None
    ) -> pd.DataFrame:
        """Return market snapshots in the normalized shape consumers expect."""

        if self.source_mode == "live":
            return self._load_market_snapshots_live(lookback_hours)
        return self._load_market_snapshots_synthetic()

    def load_crypto_snapshots(
        self, lookback_hours: int | None = None
    ) -> pd.DataFrame:
        """Return crypto snapshots as a wide frame with btc_price/eth_price."""

        if self.source_mode == "live":
            return self._load_crypto_snapshots_live(lookback_hours)
        return self._load_crypto_snapshots_synthetic()

    def load_market_metadata(self) -> pd.DataFrame:
        """Return market metadata with at least market_id and resolution_time."""

        if self.source_mode == "live":
            return self._load_market_metadata_live()
        return self._load_market_metadata_synthetic()

    # ------------------------------------------------------------------
    # Synthetic path (CSV fixtures — unchanged behavior)
    # ------------------------------------------------------------------

    def _load_market_metadata_synthetic(self) -> pd.DataFrame:
        meta = pd.read_csv(self.config.data.market_metadata_path)
        meta["resolution_time"] = meta["resolution_time"].map(parse_utc_timestamp)
        return meta.sort_values("resolution_time").reset_index(drop=True)

    def _load_market_snapshots_synthetic(self) -> pd.DataFrame:
        market = pd.read_csv(self.config.data.market_snapshots_path)
        market["timestamp"] = market["timestamp"].map(parse_utc_timestamp)
        return market.sort_values(["timestamp", "market_id"]).reset_index(drop=True)

    def _load_crypto_snapshots_synthetic(self) -> pd.DataFrame:
        crypto = pd.read_csv(self.config.data.crypto_snapshots_path)
        crypto["timestamp"] = crypto["timestamp"].map(parse_utc_timestamp)
        return crypto.sort_values("timestamp").reset_index(drop=True)

    # ------------------------------------------------------------------
    # Live path (append-only Parquet under data/raw/)
    # ------------------------------------------------------------------

    def _live_root(self) -> Path:
        return Path(self.config.data.raw_storage_root)

    def _load_market_snapshots_live(
        self, lookback_hours: int | None
    ) -> pd.DataFrame:
        root = self._live_root() / "limitless"
        if not root.exists():
            raise RuntimeError(
                f"Live mode: no Limitless raw directory at {root}. "
                "Run `python main.py ingest` to populate it."
            )
        files = self._partition_files(
            root, "market_id=*", "date=*/*.parquet", lookback_hours
        )
        if not files:
            raise RuntimeError(
                f"Live mode: no Limitless parquet files under {root}. "
                "Run `python main.py ingest` to populate it."
            )
        frames = [pd.read_parquet(f) for f in files]
        frame = pd.concat(frames, ignore_index=True)
        frame["timestamp"] = pd.to_datetime(
            frame["event_time"] if "event_time" in frame.columns else frame["timestamp"],
            utc=True,
        )
        if "yes_price" in frame.columns and "p_market" not in frame.columns:
            frame = frame.rename(columns={"yes_price": "p_market"})
        for col in _LIVE_MARKET_COLUMNS:
            if col not in frame.columns:
                raise RuntimeError(
                    f"Live mode: Limitless parquet missing required column {col!r} "
                    f"(have {sorted(frame.columns)})."
                )
        frame = frame[_LIVE_MARKET_COLUMNS]
        if lookback_hours is not None:
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=lookback_hours)
            frame = frame[frame["timestamp"] >= cutoff]
        frame = (
            frame.drop_duplicates(subset=["market_id", "timestamp"], keep="last")
            .sort_values(["timestamp", "market_id"])
            .reset_index(drop=True)
        )
        return frame

    def _load_crypto_snapshots_live(
        self, lookback_hours: int | None
    ) -> pd.DataFrame:
        root = self._live_root() / "crypto"
        if not root.exists():
            raise RuntimeError(
                f"Live mode: no crypto raw directory at {root}. "
                "Run `python main.py ingest` to populate it."
            )
        files = self._partition_files(
            root, "symbol=*", "date=*/*.parquet", lookback_hours
        )
        if not files:
            raise RuntimeError(
                f"Live mode: no crypto parquet files under {root}. "
                "Run `python main.py ingest` to populate it."
            )
        frames = [pd.read_parquet(f) for f in files]
        frame = pd.concat(frames, ignore_index=True)
        frame["timestamp"] = pd.to_datetime(
            frame["event_time"] if "event_time" in frame.columns else frame["timestamp"],
            utc=True,
        )
        if lookback_hours is not None:
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=lookback_hours)
            frame = frame[frame["timestamp"] >= cutoff]
        frame = frame.drop_duplicates(
            subset=["symbol", "timestamp"], keep="last"
        )
        pivot = frame.pivot_table(
            index="timestamp", columns="symbol", values="price", aggfunc="last"
        )
        wide = pd.DataFrame({"timestamp": pivot.index})
        wide["btc_price"] = (
            pivot["BTCUSDT"].to_numpy()
            if "BTCUSDT" in pivot.columns
            else np.nan
        )
        wide["eth_price"] = (
            pivot["ETHUSDT"].to_numpy()
            if "ETHUSDT" in pivot.columns
            else np.nan
        )
        wide["funding_rate"] = 0.0
        wide = (
            wide.dropna(subset=["btc_price"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        return wide[_LIVE_CRYPTO_COLUMNS + ["funding_rate"]]

    @staticmethod
    def _partition_files(
        root: Path,
        key_glob: str,
        file_glob: str,
        lookback_hours: int | None,
    ) -> list[Path]:
        """List parquet files under ``root/<key_glob>/<file_glob>``, pruned by date.

        When ``lookback_hours`` is set, ``date=YYYY-MM-DD`` partitions older
        than the cutoff are skipped at the filesystem level. This is the
        difference between opening 124K one-row parquet files and opening
        ~300 — per-file Parquet read overhead otherwise dominates the load.
        """

        min_date: str | None = None
        min_hms: str | None = None
        if lookback_hours is not None:
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=lookback_hours)
            min_date = cutoff.strftime("%Y-%m-%d")
            # Recorder filenames are "HHMMSS_<uuid>.parquet" (see
            # ParquetRecorder._write_rows). For the boundary day we can prune
            # by filename prefix — crucial when the recorder writes one
            # parquet per poll (crypto does, ~86K files/day).
            min_hms = cutoff.strftime("%H%M%S")

        files: list[Path] = []
        for key_dir in sorted(root.glob(key_glob)):
            if not key_dir.is_dir():
                continue
            for date_dir in sorted(key_dir.iterdir()):
                name = date_dir.name
                if not name.startswith("date="):
                    continue
                date_value = name[len("date="):]
                if min_date is not None and date_value < min_date:
                    continue
                parquet_files = sorted(date_dir.glob("*.parquet"))
                if (
                    min_hms is not None
                    and min_date is not None
                    and date_value == min_date
                ):
                    parquet_files = [
                        f for f in parquet_files if f.name[:6] >= min_hms
                    ]
                files.extend(parquet_files)
        return files

    def _load_market_metadata_live(self) -> pd.DataFrame:
        sidecar = self._live_root() / "limitless" / "market_metadata.parquet"
        if not sidecar.exists():
            raise RuntimeError(
                f"Live mode: missing metadata sidecar at {sidecar}. "
                "The sidecar is written by LimitlessClient during discovery; "
                "run `python main.py ingest` for at least one cycle."
            )
        meta = pd.read_parquet(sidecar)
        # Backfill columns older sidecar snapshots may lack.
        if "outcome_yes" not in meta.columns:
            meta["outcome_yes"] = pd.NA
        if "resolved" not in meta.columns:
            meta["resolved"] = False
        meta["resolution_time"] = pd.to_datetime(
            meta["resolution_time"], utc=True, errors="coerce"
        )
        return meta.sort_values("resolution_time").reset_index(drop=True)
