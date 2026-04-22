"""Resolution scanner tests (mocked REST client, no network)."""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pandas as pd

from ingestion.resolution_scanner import (
    SCHEMA_COLUMNS,
    ScanReport,
    _atomic_write,
    scan_resolutions,
)
from project.configuration import DataConfig, clone_config, load_config


def _sidecar_row(
    *,
    market_id: str,
    slug: str,
    resolution_time: pd.Timestamp,
    first_seen: pd.Timestamp | None = None,
    status: str = "FUNDED",
) -> dict:
    return {
        "market_id": market_id,
        "slug": slug,
        "status": status,
        "resolution_time": resolution_time,
        "outcome_yes": None,
        "resolved": False,
        "first_seen": first_seen
        or (resolution_time - pd.Timedelta(hours=1)),
        "last_seen": resolution_time,
    }


def _payload(
    *,
    market_id: str = "100001",
    slug: str,
    status: str = "RESOLVED",
    winning: int = 0,
    categories: list[str] | None = None,
    tags: list[str] | None = None,
    exp_ms: int = 1776866400000,
    volume: float = 1234.5,
    liquidity: float = 56.7,
    condition_id: str = "0x" + "ab" * 32,
) -> dict:
    yes, no = (1.0, 0.0) if winning == 0 else ((0.0, 1.0) if winning == 1 else (0.5, 0.5))
    try:
        payload_id: int | str = int(market_id)
    except (TypeError, ValueError):
        payload_id = market_id
    return {
        "id": payload_id,
        "slug": slug,
        "status": status,
        "winningOutcomeIndex": winning,
        "categories": categories or ["Crypto", "BTC"],
        "tags": tags or ["Lumy", "Recurring"],
        "expirationTimestamp": exp_ms,
        "prices": [yes, no],
        "volumeFormatted": volume,
        "liquidity": liquidity,
        "conditionId": condition_id,
    }


class FakeLimitlessClient:
    """Deterministic fake — returns canned payloads keyed by slug."""

    def __init__(self, payloads: dict[str, object]) -> None:
        self._payloads = payloads
        self.calls: list[str] = []

    def fetch_market_by_slug(self, slug: str) -> dict:
        self.calls.append(slug)
        val = self._payloads.get(slug)
        if isinstance(val, Exception):
            raise val
        if val is None:
            raise RuntimeError(f"no fake payload for {slug}")
        return val


class ResolutionScannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)

        # Point DataConfig at our tmpdir so paths resolve under it.
        base = load_config("config/default.yaml")
        data = replace(
            base.data,
            raw_storage_root=self.root / "raw",
            market_metadata_path=self.root / "market_metadata.csv",
            market_snapshots_path=self.root / "market_snapshots.csv",
            crypto_snapshots_path=self.root / "crypto_snapshots.csv",
            audit_report_path=self.root / "audit.json",
            feature_cache_path=self.root / "features.csv",
            trade_log_path=self.root / "trades.csv",
            metrics_report_path=self.root / "metrics.json",
            prediction_report_path=self.root / "predictions.csv",
            validation_report_path=self.root / "validation.json",
            model_artifact_path=self.root / "model.pkl",
            scaler_artifact_path=self.root / "scaler.pkl",
            calibrator_artifact_path=self.root / "calibrator.pkl",
            training_metadata_path=self.root / "meta.json",
        )
        self.config = clone_config(base, data=data)
        self.sidecar_path = (
            Path(self.config.data.raw_storage_root) / "limitless" / "market_metadata.parquet"
        )
        self.resolved_path = (
            Path(self.config.data.raw_storage_root).parent / "resolved_markets.parquet"
        )
        self.sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        self.now = pd.Timestamp("2026-04-22 22:00:00", tz="UTC")

    def _write_sidecar(self, rows: list[dict]) -> None:
        pd.DataFrame(rows).to_parquet(self.sidecar_path, index=False)

    # ---------- tests ----------

    def test_dry_run_does_not_write_file(self) -> None:
        """--dry-run performs fetches but never writes the parquet."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="100001",
                    slug="btc-above-dollar1-on-apr-22-1400-utc-1",
                    resolution_time=self.now - pd.Timedelta(minutes=30),
                )
            ]
        )
        client = FakeLimitlessClient(
            {"btc-above-dollar1-on-apr-22-1400-utc-1": _payload(slug="btc-above-dollar1-on-apr-22-1400-utc-1")}
        )
        report = scan_resolutions(
            self.config,
            dry_run=True,
            client_factory=lambda cfg: client,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(report.resolved, 1)
        self.assertEqual(report.written_rows, 0)
        self.assertFalse(self.resolved_path.exists())
        self.assertEqual(client.calls, ["btc-above-dollar1-on-apr-22-1400-utc-1"])

    def test_skips_already_captured_markets(self) -> None:
        """Markets already in resolved_markets.parquet are not re-fetched."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="A",
                    slug="btc-already-1",
                    resolution_time=self.now - pd.Timedelta(hours=2),
                ),
                _sidecar_row(
                    market_id="B",
                    slug="btc-new-2",
                    resolution_time=self.now - pd.Timedelta(hours=1),
                ),
            ]
        )
        # Pre-seed the resolved table with A.
        seed = pd.DataFrame(
            [{col: None for col in SCHEMA_COLUMNS}],
        )
        seed["market_id"] = "A"
        seed["slug"] = "btc-already-1"
        seed["expiration_timestamp"] = pd.Timestamp("2026-04-22 20:00", tz="UTC")
        seed["resolved_at"] = pd.Timestamp("2026-04-22 20:05", tz="UTC")
        seed["first_seen"] = pd.Timestamp("2026-04-22 19:00", tz="UTC")
        seed["winning_outcome_index"] = 0
        seed["final_yes_price"] = 1.0
        seed["final_no_price"] = 0.0
        seed["volume_total"] = 100.0
        seed["liquidity_at_resolution"] = 0.0
        seed["condition_id"] = "0x" + "cd" * 32
        seed["category_tags"] = [["Crypto", "BTC"]]
        seed["capture_method"] = "scanner_v1"
        seed.to_parquet(self.resolved_path, index=False)

        client = FakeLimitlessClient(
            {"btc-new-2": _payload(market_id="B", slug="btc-new-2")}
        )
        report = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(report.already_captured, 1)
        self.assertEqual(client.calls, ["btc-new-2"])
        self.assertEqual(report.resolved, 1)

        df = pd.read_parquet(self.resolved_path)
        self.assertEqual(set(df["market_id"].tolist()), {"A", "B"})
        # A is untouched
        self.assertTrue(df[df["market_id"] == "A"]["slug"].iloc[0] == "btc-already-1")

    def test_handles_active_markets_gracefully(self) -> None:
        """A market that's past expiration but still ACTIVE is not written."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="LATE",
                    slug="btc-late-1",
                    resolution_time=self.now - pd.Timedelta(minutes=5),
                )
            ]
        )
        client = FakeLimitlessClient(
            {"btc-late-1": _payload(slug="btc-late-1", status="FUNDED")}
        )
        report = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(report.still_active, 1)
        self.assertEqual(report.resolved, 0)
        self.assertEqual(report.written_rows, 0)
        self.assertFalse(self.resolved_path.exists())

    def test_handles_invalid_markets(self) -> None:
        """Ambiguous or refunded markets get winning_outcome_index = -1."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="INV",
                    slug="btc-invalid-1",
                    resolution_time=self.now - pd.Timedelta(minutes=10),
                )
            ]
        )
        # winningOutcomeIndex is None AND prices are ambiguous.
        payload = _payload(slug="btc-invalid-1", winning=-1)
        payload["winningOutcomeIndex"] = None
        payload["prices"] = [0.5, 0.5]
        client = FakeLimitlessClient({"btc-invalid-1": payload})
        report = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(report.resolved, 1)
        df = pd.read_parquet(self.resolved_path)
        self.assertEqual(int(df["winning_outcome_index"].iloc[0]), -1)

    def test_atomic_write_does_not_clobber_on_error(self) -> None:
        """If the pandas write fails mid-call, existing file is untouched."""
        # Seed an existing file.
        existing = pd.DataFrame(
            [
                {col: None for col in SCHEMA_COLUMNS}
                | {
                    "market_id": "EXISTING",
                    "slug": "btc-pre-1",
                    "condition_id": "0x" + "ee" * 32,
                    "category_tags": ["Crypto"],
                    "expiration_timestamp": pd.Timestamp("2026-04-22 18:00", tz="UTC"),
                    "resolved_at": pd.Timestamp("2026-04-22 18:05", tz="UTC"),
                    "winning_outcome_index": 0,
                    "final_yes_price": 1.0,
                    "final_no_price": 0.0,
                    "volume_total": 999.9,
                    "liquidity_at_resolution": 0.0,
                    "first_seen": pd.Timestamp("2026-04-22 17:00", tz="UTC"),
                    "capture_method": "scanner_v1",
                }
            ]
        )
        self.resolved_path.parent.mkdir(parents=True, exist_ok=True)
        existing.to_parquet(self.resolved_path, index=False)

        # Force the atomic write to blow up.
        class Broken(pd.DataFrame):
            def to_parquet(self, *_a, **_k):
                raise RuntimeError("disk full simulation")

        broken = Broken(existing.copy())
        with self.assertRaises(RuntimeError):
            _atomic_write(broken, self.resolved_path)

        # Existing file still readable, unchanged.
        after = pd.read_parquet(self.resolved_path)
        self.assertEqual(list(after["market_id"]), ["EXISTING"])

    def test_resumes_after_partial_capture(self) -> None:
        """A second run picks up markets that failed in the first run."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="OK",
                    slug="btc-ok-1",
                    resolution_time=self.now - pd.Timedelta(minutes=30),
                ),
                _sidecar_row(
                    market_id="FAIL",
                    slug="btc-fail-1",
                    resolution_time=self.now - pd.Timedelta(minutes=20),
                ),
            ]
        )
        client_run1 = FakeLimitlessClient(
            {
                "btc-ok-1": _payload(market_id="OK", slug="btc-ok-1"),
                "btc-fail-1": ConnectionError("network down"),
            }
        )
        r1 = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client_run1,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(r1.resolved, 1)
        self.assertEqual(r1.errors, 1)
        df1 = pd.read_parquet(self.resolved_path)
        self.assertEqual(set(df1["market_id"]), {"OK"})

        # Second run — network is back, FAIL resolves.
        client_run2 = FakeLimitlessClient(
            {"btc-fail-1": _payload(market_id="FAIL", slug="btc-fail-1")}
        )
        r2 = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client_run2,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(r2.already_captured, 1)  # OK
        self.assertEqual(client_run2.calls, ["btc-fail-1"])
        self.assertEqual(r2.resolved, 1)

        df2 = pd.read_parquet(self.resolved_path)
        self.assertEqual(set(df2["market_id"]), {"OK", "FAIL"})

    def test_filters_to_crypto_only(self) -> None:
        """Non-crypto slugs in the sidecar are dropped before fetching."""
        self._write_sidecar(
            [
                _sidecar_row(
                    market_id="CRYPT",
                    slug="btc-above-1-on-apr-22-1400-utc-1",
                    resolution_time=self.now - pd.Timedelta(minutes=30),
                ),
                _sidecar_row(
                    market_id="SPORT",
                    slug="cagliari-vs-atalanta-4-total-cards-1",
                    resolution_time=self.now - pd.Timedelta(minutes=30),
                ),
                _sidecar_row(
                    market_id="POL",
                    slug="will-trump-do-anything-specific-2",
                    resolution_time=self.now - pd.Timedelta(minutes=30),
                ),
            ]
        )
        client = FakeLimitlessClient(
            {
                "btc-above-1-on-apr-22-1400-utc-1": _payload(
                    market_id="CRYPT", slug="btc-above-1-on-apr-22-1400-utc-1"
                )
            }
        )
        report = scan_resolutions(
            self.config,
            client_factory=lambda cfg: client,
            sleep=lambda _s: None,
            now=self.now,
        )
        self.assertEqual(report.candidates, 1)  # only crypto after filter
        self.assertEqual(client.calls, ["btc-above-1-on-apr-22-1400-utc-1"])
        df = pd.read_parquet(self.resolved_path)
        self.assertEqual(set(df["market_id"].tolist()), {"CRYPT"})


if __name__ == "__main__":
    unittest.main()
