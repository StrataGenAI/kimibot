# Limitless Trader

Single-node prediction market trading system for crypto-related Limitless markets.

## Requirements

- Python 3.10+
- `pip install -e .`

## Commands

- Backtest:
  `python main.py backtest`
- Simulated live replay:
  `python main.py live-sim`
- Continuous raw ingestion:
  `python main.py ingest`
- Raw data quality audit:
  `python main.py audit-data`
- Validation / falsification suite:
  `python main.py validate --validation-mode all`
- Train the latest walk-forward fold artifact:
  `python train.py`
- Score a single market/timestamp:
  `python infer.py --market-id m5 --timestamp 2026-01-01T00:16:00Z`

## Data

Bundled sample data lives in `data/`:

- `market_metadata.csv`
- `market_snapshots.csv`
- `crypto_snapshots.csv`

Raw append-only Parquet samples live under `data/raw/`:

- `data/raw/limitless/market_id=<id>/date=<YYYY-MM-DD>/*.parquet`
- `data/raw/crypto/symbol=<symbol>/date=<YYYY-MM-DD>/*.parquet`

Generated artifacts:

- `data/feature_cache.csv`
- `data/trade_log.csv`
- `data/metrics_report.json`
- `data/predictions.csv`
- `data/validation_report.json`
- `data/audit_report.json`
- `models/logistic_regression.pkl`
- `models/standard_scaler.pkl`
- `models/probability_calibrator.pkl`
- `models/training_metadata.json`

Validation experiment outputs are written under `data/` sibling directories such as:

- `baseline/metrics.json`
- `baseline/trade_log.csv`
- `baseline/predictions.csv`
- `stress/metrics.json`
- `strict_holdout/metrics.json`

Prediction reports now include both raw and calibrated probabilities:

- `p_model_raw`
- `p_model_calibrated`

## Verify Raw Data

- Run ingestion:
  `python main.py ingest`
- Inspect raw files:
  `find data/raw -type f | sort`
- Verify replay behavior with a short Python snippet:
  `from ingestion.recorder import RawReplayStore`
  `RawReplayStore(Path("data/raw")).get_market_data_until("2026-01-01T00:05:00Z")`
- Run the data quality audit:
  `python main.py audit-data`
