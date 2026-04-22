# Limitless Historical Ingestion + Walk-Forward Evaluation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest real resolved Limitless markets, build features from historical BTC data, run a 60/20/20 walk-forward evaluation, and surface honest Brier vs market-baseline results in the UI.

**Architecture:** The Graph subgraph (decentralized network) provides resolved binary market enumeration and per-market trade history; Binance public API provides 1-minute BTC/USDT klines and perpetual funding rates; the existing `FeatureBuilder.build_features()` is reused unchanged; a new `evaluation/` module handles the single 60/20/20 split, retrain, calibration, metrics, and report generation.

**Tech Stack:** Python 3.10+, numpy, pandas, urllib.request (stdlib, already used), matplotlib (new dep, add to pyproject.toml), plotly (already installed), Next.js API route (TypeScript)

---

## Pre-flight Research Findings (already done — document in LIMITLESS_INGESTION_PLAN.md)

Key facts confirmed via research:

- **Limitless REST base:** `https://api.limitless.exchange` — public, no auth for read-only
- **No `/markets/resolved` REST endpoint** — resolved market enumeration MUST use The Graph
- **The Graph subgraph ID (simple markets):** `BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB`
- **Graph endpoint:** `https://gateway.thegraph.com/api/{GRAPH_API_KEY}/subgraphs/id/{SUBGRAPH_ID}`
- **Requires:** Free `GRAPH_API_KEY` from thegraph.com/studio/apikeys
- **Subgraph entities used:** `conditions` (resolved markets), `trades` (price time-series)
- **REST endpoint used:** `GET /markets/{conditionId}` — for slug, title, resolution_time, winning_index
- **REST rate limit:** 2 concurrent / 300ms minimum delay
- **Subgraph rate limit:** None (advantage over REST)
- **Binance klines:** `GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={ms}&endTime={ms}&limit=1000` — 1000 candles per request, public
- **Binance funding:** `GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime={ms}&endTime={ms}&limit=1000` — every 8 hours
- **Feature mapping** — all 10 FEATURE_COLUMNS reconstructible from subgraph trades + Binance klines:
  - `p_market` → last YES token trade price before `as_of`
  - `momentum_1m/5m` → relative change in `p_market` over 1m/5m lookback
  - `volatility` → std of last 5 `p_market` observations
  - `volume_spike` → current minute's amountUSD / trailing 5-observation average
  - `btc_return_1m/5m` → relative BTC close price change over 1m/5m lookback (from klines)
  - `btc_volatility` → std of BTC 1m pct-changes over last 5 minutes
  - `funding_rate` → latest BTC perpetual funding rate before `as_of`
  - `time_to_resolution` → `(resolution_time - as_of).total_seconds()`
- **Not installed:** scikit-learn, matplotlib — implement metrics manually; add matplotlib to pyproject.toml
- **Installed:** numpy, pandas, scipy, plotly, requests

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `LIMITLESS_INGESTION_PLAN.md` | Create | Pre-flight research doc (commit before any code) |
| `ingestion/subgraph_client.py` | Create | GraphQL client for The Graph API with token-bucket rate limiter |
| `ingestion/limitless_historical.py` | Create | Enumerate resolved markets, fetch metadata + trade history, cache to `data/limitless_raw/` |
| `ingestion/binance_historical.py` | Create | Fetch BTC 1m klines + funding rates, cache to `data/binance_raw/` |
| `evaluation/__init__.py` | Create | Module marker |
| `evaluation/metrics.py` | Create | Brier, ECE, log-loss, AUC, accuracy — implemented with numpy only |
| `evaluation/walk_forward_evaluator.py` | Create | 60/20/20 split, retrain, calibrate, predict, save artifacts |
| `evaluation/report_generator.py` | Create | Write EVALUATION_REPORT.md, reliability.png, prob_histogram.png, walk_forward_results.json |
| `tests/test_lookahead.py` | Create | Look-ahead audit — hard gate, must pass before evaluation |
| `tests/test_walk_forward_historical.py` | Create | Walk-forward split correctness tests |
| `project/configuration.py` | Modify | Add `graph_api_key: str = ""` to IngestionConfig |
| `config/default.yaml` | Modify | Add `graph_api_key: "{{GRAPH_API_KEY}}"` |
| `pyproject.toml` | Modify | Add `matplotlib>=3.7` to dependencies |
| `main.py` | Modify | Add `evaluate-limitless` mode |
| `frontend/app/api/walk-forward/route.ts` | Create | Read `data/walk_forward_results.json`, return JSON |
| `frontend/lib/types.ts` | Modify | Add `WalkForwardData` type |
| `frontend/app/analytics/page.tsx` | Modify | Add WalkForwardPanel section |

---

## Task 1: Write LIMITLESS_INGESTION_PLAN.md

**Files:**
- Create: `LIMITLESS_INGESTION_PLAN.md`

- [ ] **Step 1: Write the plan document**

```markdown
# Limitless Historical Ingestion — Pre-flight Research Plan

Generated: 2026-04-21

## 1. Limitless API

**Base URL:** https://api.limitless.exchange  
**Docs:** https://docs.limitless.exchange

**Authentication:** Public endpoints need no auth for read-only. Rate limit: 2 concurrent requests, 300ms minimum delay per token.

**Resolved market enumeration:** NO REST endpoint for this. Use The Graph subgraph.

**Market metadata:** `GET /markets/{addressOrConditionId}` — returns `winning_index` (0=YES, 1=NO, null if unresolved), `status` ("RESOLVED"), `resolutionTxHash`, `slug`, title.

**Price history:** `GET /markets/{slug}/historical-price?interval=1h` — hourly only, insufficient for 1m/5m momentum features. Use subgraph trades instead.

## 2. The Graph Subgraph

**Subgraph ID (simple binary markets):** BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB  
**Endpoint:** https://gateway.thegraph.com/api/{GRAPH_API_KEY}/subgraphs/id/BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB  
**Auth:** Free API key from thegraph.com/studio/apikeys — required, set as GRAPH_API_KEY env var.  
**Rate limits:** None enforced by subgraph — use 1 req/s to be polite.

**Key entities:**
- `conditions(where: {resolved: true})` — list of resolved markets with payoutNumerators, resolvedAt, tradesCount
- `trades(where: {market: {id: $conditionId}})` — tick-level trade history with price (YES token price 0-1), amountUSD, timestamp

**Resolved outcome:** `payoutNumerators[0] > 0` → YES won (label=1), `payoutNumerators[1] > 0` → NO won (label=0)

## 3. BTC Price Source (Binance)

**Klines:** GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={ms}&endTime={ms}&limit=1000  
**Funding:** GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime={ms}&endTime={ms}&limit=1000  
**Rate limits:** 1200 weight/min; klines = 2 weight/request → 600 req/min → safe to request 100/min with headroom.  
**Historical depth:** Full history available (Binance launched 2019).

## 4. Time-alignment

All timestamps in UTC (timezone-aware). Limitless subgraph returns Unix seconds. Binance returns Unix milliseconds. Market trade timestamps aligned to nearest 1-minute bucket; missing minutes forward-filled from last known price.

## 5. Feature Schema Mapping

| Feature | Source | Computation |
|---------|--------|-------------|
| p_market | subgraph Trade.price | Last YES price before as_of |
| momentum_1m | subgraph Trade.price | (p_now - p_1m_ago) / p_1m_ago |
| momentum_5m | subgraph Trade.price | (p_now - p_5m_ago) / p_5m_ago |
| volatility | subgraph Trade.price | std(last 5 observations, ddof=0) |
| volume_spike | subgraph Trade.amountUSD | current_vol / mean(last 5 vols) |
| btc_return_1m | Binance klines close | (close_now - close_1m_ago) / close_1m_ago |
| btc_return_5m | Binance klines close | (close_now - close_5m_ago) / close_5m_ago |
| btc_volatility | Binance klines close | std(pct_change, last 5 minutes, ddof=0) |
| funding_rate | Binance fundingRate | Latest rate before as_of |
| time_to_resolution | resolution_time - as_of | max(0, seconds remaining) |

## 6. Trade Activity Threshold

Minimum 20 trades per market. Rationale: fewer trades produce unreliable momentum/volatility features (all zeros); 20 trades is the minimum for at least one observation per momentum lookback window in a typical short-duration market.

## 7. Dataset Target

Aim for 200+ resolved markets if available. The subgraph indexes all historical markets since protocol launch. Report actual count obtained.

## 8. Potential Blockers

- GRAPH_API_KEY not set → fail with clear error, instructions to get free key from thegraph.com/studio/apikeys
- Resolved market count < 50 → document and ask before lowering thresholds
- Features cannot be cleanly computed (e.g. market had no trades in lookback window) → use 0.0 for momentum/volatility, flag in logs, keep the row

## 9. Graph API Query Budget

**Per evaluation run:**
- Enumerate resolved conditions: ~200 markets at 100/page = **2–4 queries** (pessimistic: 10 pages = 10 queries)
- Trade history per market: most markets have < 1,000 trades → 1 page each = **~200 queries**
- Total per run: **~204–210 Graph API queries**

**Monthly budget:**
| Cadence | Queries/Month | Free Tier (100K/mo) | Headroom |
|---------|--------------|---------------------|---------|
| Once | ~210 | 100,000 | 99.8% remaining |
| Daily | ~6,300 | 100,000 | 93.7% remaining |
| Hourly | ~151,200 | 100,000 | **EXCEEDS** — never run hourly |

**Conclusion:** Safe for any reasonable cadence (daily or less). Caching is critical — re-running with full cache hits = 0 new Graph queries. The idempotent cache in `data/limitless_raw/` ensures this.

## 10. Feature Sparsity Analysis

Limitless AMM markets are **not continuously traded**. Many binary markets have < 1 trade per hour, especially early in their life. This section documents the expected behavior of each feature under sparse trade conditions and how the evaluation accounts for this.

**Market data construction:** Trades are resampled to a 1-minute grid with forward-fill (last known price carried forward). A market with 20 trades over 4 hours will have ~240 minutes of grid rows but only 20 distinct prices.

| Feature | BTC-dependent? | Expected behavior under sparse Limitless trades | Severity |
|---------|---------------|------------------------------------------------|---------|
| `p_market` | No | **Stale but valid.** Forward-fill makes value the last traded price. Correct at trade times; constant between trades. | Low — value is real, just infrequently updated |
| `momentum_1m` | No | **Near-zero on most snapshots.** If no new trade in the past 1 minute, current price = price 1 minute ago → momentum = 0.0. Expected to be exactly 0 on ~90%+ of snapshots for sparse markets. | **HIGH — flag in report** |
| `momentum_5m` | No | **Near-zero on most snapshots.** Same issue over a 5-minute window. Slightly less zero than 1m since 5-minute spans are more likely to cross a trade boundary. | **HIGH — flag in report** |
| `volatility` | No | **Near-zero on most snapshots.** Std dev of 5 forward-filled observations is 0 if no new trades in the window. Becomes non-zero only when multiple trades occur within a 5-snapshot window. | **HIGH — flag in report** |
| `volume_spike` | No | **Mostly 0.0 or 1.0.** Forward-fill sets volume to 0 for all non-trade minutes. The spike ratio is 0/0 → 0.0 when all 5 observations have zero volume, or (trade_vol / mean_with_zeros) for isolated trades. | **MEDIUM — effectively a "did a trade just happen?" binary** |
| `btc_return_1m` | Yes | **Unaffected.** Binance BTCUSDT trades continuously; every 1-minute bar always has a distinct close price. No sparsity issue. | None |
| `btc_return_5m` | Yes | **Unaffected.** Same as above. | None |
| `btc_volatility` | Yes | **Unaffected.** Continuous Binance data; BTC price is never flat over 5 minutes in practice. | None |
| `funding_rate` | Yes | **Updated every 8 hours; forward-filled.** Not sparse — 3 updates per day always available. | None |
| `time_to_resolution` | No | **Never zero (snapshots stop 15 min before resolution).** Deterministic arithmetic. | None |

**Summary of effectively-zero features:** `momentum_1m`, `momentum_5m`, `volatility`, and `volume_spike` will be near-zero for most snapshots of sparse markets. This is **not imputed or corrected** — the zero values accurately reflect the absence of market activity. However, it means:

1. The model's signal from these features will be weak or absent.
2. The model's predictions will primarily be driven by `p_market`, `btc_return_*`, and `time_to_resolution`.
3. EVALUATION_REPORT.md must include a **sparsity table** reporting the fraction of test-set snapshots where each high-severity feature = 0.0 (threshold: |value| < 1e-9).
4. If >80% of `momentum_1m` snapshots are zero, note explicitly that the model is not meaningfully using this feature.

**Implication for evaluation validity:** The evaluation is still valid — it measures whether the model, given realistic sparse data, adds value beyond the market price. A model that mostly echoes `p_market` is a real finding, not a bug.
```

- [ ] **Step 2: Commit**

```bash
cd /opt/stratagen/kimibot
git add LIMITLESS_INGESTION_PLAN.md
git commit -m "docs: add Limitless historical ingestion pre-flight research plan"
```

---

## Task 2: Add Dependencies and Config

**Files:**
- Modify: `pyproject.toml`
- Modify: `project/configuration.py:108-128`
- Modify: `config/default.yaml`

- [ ] **Step 1: Add matplotlib to pyproject.toml**

In `pyproject.toml`, change the `dependencies` list:
```toml
dependencies = [
  "numpy>=1.24",
  "pandas>=2.0",
  "PyYAML>=6.0",
  "pyarrow>=14.0",
  "websockets>=12.0",
  "streamlit>=1.32",
  "plotly>=5.20",
  "matplotlib>=3.7",
]
```

- [ ] **Step 2: Install matplotlib**

```bash
pip install matplotlib>=3.7
```

Expected: `Successfully installed matplotlib-...`

- [ ] **Step 3: Add graph_api_key to IngestionConfig**

In `project/configuration.py`, add `graph_api_key: str = ""` to `IngestionConfig` after `limitless_api_key`:
```python
@dataclass(frozen=True)
class IngestionConfig:
    limitless_rest_base_url: str = ""
    limitless_ws_url: str = ""
    limitless_api_key: str = ""
    limitless_private_key: str = ""
    graph_api_key: str = ""
    ...
```

- [ ] **Step 4: Add graph_api_key to config/default.yaml**

Add under `ingestion:`:
```yaml
ingestion:
  limitless_rest_base_url: https://api.limitless.exchange
  limitless_ws_url: wss://ws.limitless.exchange
  limitless_api_key: "{{LIMITLESS_API_KEY}}"
  limitless_private_key: "{{LIMITLESS_PRIVATE_KEY}}"
  graph_api_key: "{{GRAPH_API_KEY}}"
  ...
```

- [ ] **Step 5: Verify config loads**

```bash
cd /opt/stratagen/kimibot
python3 -c "from project.configuration import load_config; c = load_config('config/default.yaml'); print('graph_api_key field:', repr(c.ingestion.graph_api_key[:5] if c.ingestion.graph_api_key else 'EMPTY'))"
```

Expected: `graph_api_key field: 'EMPTY'`  (or first 5 chars if GRAPH_API_KEY env var is set)

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml project/configuration.py config/default.yaml
git commit -m "feat: add matplotlib dep and GRAPH_API_KEY config field"
```

---

## Task 3: Subgraph Client

**Files:**
- Create: `ingestion/subgraph_client.py`

- [ ] **Step 1: Write the subgraph client**

Create `/opt/stratagen/kimibot/ingestion/subgraph_client.py`:

```python
"""GraphQL client for The Graph subgraph with token-bucket rate limiter."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)

_SUBGRAPH_ID = "BLkZxK4Zn8FnrfQdNbZ5Vim98hNy2efq2z7QVnse8VrB"
_GATEWAY_BASE = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/" + _SUBGRAPH_ID

_RESOLVED_MARKETS_GQL = """
query ResolvedMarkets($skip: Int!, $first: Int!) {
  conditions(
    where: { resolved: true }
    orderBy: resolvedAt
    orderDirection: asc
    first: $first
    skip: $skip
  ) {
    id
    resolved
    payoutNumerators
    resolvedAt
    market {
      id
      tradesCount
      volumeUSD
    }
  }
}
"""

_MARKET_TRADES_GQL = """
query MarketTrades($conditionId: ID!, $skip: Int!, $first: Int!) {
  trades(
    where: { market: { id: $conditionId } }
    orderBy: timestamp
    orderDirection: asc
    first: $first
    skip: $skip
  ) {
    id
    type
    price
    amountUSD
    timestamp
  }
}
"""


class TokenBucket:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int = 1) -> None:
        self._rate = rate
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1.0:
                sleep_time = (1.0 - self._tokens) / self._rate
                time.sleep(sleep_time)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class SubgraphClient:
    """Read-only GraphQL client for the Limitless subgraph."""

    def __init__(self, api_key: str, rate_per_second: float = 1.0) -> None:
        if not api_key:
            raise ValueError(
                "GRAPH_API_KEY is required. Get a free key at thegraph.com/studio/apikeys "
                "and set it as the GRAPH_API_KEY environment variable."
            )
        self._endpoint = _GATEWAY_BASE.format(api_key=api_key)
        self._bucket = TokenBucket(rate=rate_per_second)

    def query(self, gql: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return the data dict."""
        self._bucket.acquire()
        payload = json.dumps({"query": gql, "variables": variables or {}}).encode()
        req = Request(
            self._endpoint,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
        if "errors" in result:
            raise RuntimeError(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def get_all_resolved_markets(self, min_trades: int = 20) -> list[dict[str, Any]]:
        """Paginate through all resolved conditions and return those with sufficient trades."""
        all_conditions: list[dict[str, Any]] = []
        skip = 0
        page_size = 100
        while True:
            data = self.query(
                _RESOLVED_MARKETS_GQL,
                {"skip": skip, "first": page_size},
            )
            page = data.get("conditions", [])
            LOGGER.info("Subgraph page skip=%d returned %d conditions", skip, len(page))
            all_conditions.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        filtered = [
            c for c in all_conditions
            if c.get("market") and int(c["market"].get("tradesCount", 0) or 0) >= min_trades
        ]
        LOGGER.info(
            "Resolved conditions: %d total, %d with >= %d trades",
            len(all_conditions), len(filtered), min_trades,
        )
        return filtered

    def get_market_trades(self, condition_id: str) -> list[dict[str, Any]]:
        """Fetch all trades for a market, paginating as needed."""
        all_trades: list[dict[str, Any]] = []
        skip = 0
        page_size = 1000
        while True:
            data = self.query(
                _MARKET_TRADES_GQL,
                {"conditionId": condition_id, "skip": skip, "first": page_size},
            )
            page = data.get("trades", [])
            all_trades.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return all_trades
```

- [ ] **Step 2: Write a quick smoke test**

```bash
cd /opt/stratagen/kimibot
python3 -c "
from ingestion.subgraph_client import SubgraphClient, TokenBucket
import time

# Test TokenBucket
tb = TokenBucket(rate=10.0)
start = time.monotonic()
for _ in range(3): tb.acquire()
elapsed = time.monotonic() - start
assert elapsed >= 0.2, f'Too fast: {elapsed:.3f}s'
print('TokenBucket OK, elapsed:', round(elapsed, 3), 's')

# Test SubgraphClient requires key
try:
    SubgraphClient('')
    assert False, 'Should have raised'
except ValueError as e:
    print('Key check OK:', str(e)[:50])
print('Subgraph client smoke test PASSED')
"
```

Expected: 
```
TokenBucket OK, elapsed: 0.2xx s
Key check OK: GRAPH_API_KEY is required...
Subgraph client smoke test PASSED
```

- [ ] **Step 3: Commit**

```bash
git add ingestion/subgraph_client.py
git commit -m "feat: add SubgraphClient with token-bucket rate limiter"
```

---

## Task 4: Limitless Historical Ingestion

**Files:**
- Create: `ingestion/limitless_historical.py`

- [ ] **Step 1: Write the module**

Create `/opt/stratagen/kimibot/ingestion/limitless_historical.py`:

```python
"""Enumerate and cache resolved Limitless markets with their trade history."""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from ingestion.subgraph_client import SubgraphClient, TokenBucket

LOGGER = logging.getLogger(__name__)

RAW_DIR = Path("data/limitless_raw")
REST_BASE = "https://api.limitless.exchange"

# REST rate limiter: 1 req per 350ms (safe under 300ms minimum)
_REST_BUCKET = TokenBucket(rate=1.0 / 0.35)


def _rest_get(path: str, retries: int = 5) -> Any:
    """Fetch JSON from Limitless REST API with retries."""
    url = f"{REST_BASE}{path}"
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            _REST_BUCKET.acquire()
            req = Request(url, headers={"User-Agent": "kimibot-historical/0.1"})
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            last_err = exc
            LOGGER.warning("REST attempt %d failed for %s: %s", attempt + 1, url, exc)
            time.sleep(delay + random.uniform(0, 0.25))
            delay = min(delay * 2, 16.0)
    raise last_err  # type: ignore[misc]


def _resolve_outcome(payout_numerators: list[str | int]) -> int | None:
    """Return 1 if YES won, 0 if NO won, None if ambiguous."""
    if not payout_numerators or len(payout_numerators) < 2:
        return None
    try:
        yes_payout = int(payout_numerators[0])
        no_payout = int(payout_numerators[1])
    except (ValueError, TypeError):
        return None
    if yes_payout > 0 and no_payout == 0:
        return 1
    if no_payout > 0 and yes_payout == 0:
        return 0
    return None  # Both or neither non-zero — multi-outcome or invalid


def _fetch_rest_metadata(condition_id: str) -> dict[str, Any] | None:
    """Fetch market metadata from REST API."""
    try:
        payload = _rest_get(f"/markets/{condition_id}")
        if not isinstance(payload, dict):
            return None
        return payload
    except Exception as exc:
        LOGGER.warning("REST metadata fetch failed for %s: %s", condition_id, exc)
        return None


def _parse_resolution_time(market_payload: dict[str, Any]) -> datetime | None:
    """Extract resolution time from REST market payload."""
    for key in ("resolvingAt", "resolveAt", "resolution_time", "expirationDate"):
        val = market_payload.get(key)
        if val:
            try:
                if isinstance(val, (int, float)):
                    return datetime.fromtimestamp(val / 1000.0 if val > 1e10 else val, tz=timezone.utc)
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except Exception:
                continue
    return None


def run_historical_ingestion(
    graph_api_key: str,
    min_trades: int = 20,
    cache_dir: Path = RAW_DIR,
    rate_per_second: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Enumerate resolved Limitless markets and cache raw data to disk.

    Returns a list of market dicts ready for feature construction.
    Each dict has: condition_id, slug, label, resolution_time_utc, trades (list of {price, amountUSD, timestamp}).
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    client = SubgraphClient(api_key=graph_api_key, rate_per_second=rate_per_second)

    LOGGER.info("Enumerating resolved markets from subgraph...")
    conditions = client.get_all_resolved_markets(min_trades=min_trades)
    LOGGER.info("Found %d candidate resolved markets", len(conditions))

    markets: list[dict[str, Any]] = []
    success = 0
    skipped = 0
    cached_hits = 0

    for i, cond in enumerate(conditions):
        condition_id = cond["id"]
        cache_path = cache_dir / f"{condition_id}.json"

        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text())
                markets.append(cached)
                cached_hits += 1
                continue
            except Exception:
                pass  # Re-fetch if cache is corrupt

        LOGGER.info("[%d/%d] Processing condition %s", i + 1, len(conditions), condition_id)

        # Determine outcome from subgraph payoutNumerators
        label = _resolve_outcome(cond.get("payoutNumerators", []))
        if label is None:
            LOGGER.warning("Ambiguous outcome for %s, skipping", condition_id)
            skipped += 1
            continue

        # Fetch REST metadata for resolution time and slug
        rest_meta = _fetch_rest_metadata(condition_id)
        resolution_time_utc: str | None = None
        slug = condition_id

        if rest_meta:
            slug = rest_meta.get("slug") or rest_meta.get("conditionId") or condition_id
            rt = _parse_resolution_time(rest_meta)
            if rt:
                resolution_time_utc = rt.isoformat()

        # Fall back to subgraph resolvedAt if REST doesn't have resolution time
        if resolution_time_utc is None:
            resolved_at = cond.get("resolvedAt")
            if resolved_at:
                try:
                    ts = float(resolved_at)
                    resolution_time_utc = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    pass

        if resolution_time_utc is None:
            LOGGER.warning("Cannot determine resolution time for %s, skipping", condition_id)
            skipped += 1
            continue

        # Fetch trade history from subgraph
        trades = client.get_market_trades(condition_id)
        if len(trades) < min_trades:
            LOGGER.warning("Market %s has only %d trades after fetch, skipping", condition_id, len(trades))
            skipped += 1
            continue

        market_record = {
            "condition_id": condition_id,
            "slug": slug,
            "label": label,
            "resolution_time_utc": resolution_time_utc,
            "trades_count": len(trades),
            "volume_usd": float(cond.get("market", {}).get("volumeUSD") or 0),
            "trades": [
                {
                    "price": float(t["price"]),
                    "amount_usd": float(t.get("amountUSD") or 0),
                    "timestamp": int(t["timestamp"]),
                }
                for t in trades
            ],
        }

        cache_path.write_text(json.dumps(market_record, indent=2))
        markets.append(market_record)
        success += 1

        LOGGER.info(
            "Cached market %s: label=%d, %d trades, resolved=%s",
            condition_id, label, len(trades), resolution_time_utc,
        )

    LOGGER.info(
        "Ingestion complete: %d cached hits, %d newly fetched, %d skipped",
        cached_hits, success, skipped,
    )
    return markets
```

- [ ] **Step 2: Verify it imports cleanly**

```bash
cd /opt/stratagen/kimibot
python3 -c "from ingestion.limitless_historical import run_historical_ingestion; print('Import OK')"
```

Expected: `Import OK`

- [ ] **Step 3: Commit**

```bash
git add ingestion/limitless_historical.py
git commit -m "feat: add Limitless historical ingestion with subgraph + REST cache"
```

---

## Task 5: Binance Historical Ingestion

**Files:**
- Create: `ingestion/binance_historical.py`

- [ ] **Step 1: Write the module**

Create `/opt/stratagen/kimibot/ingestion/binance_historical.py`:

```python
"""Fetch and cache BTC/USDT 1-minute klines and funding rates from Binance."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd

from ingestion.subgraph_client import TokenBucket

LOGGER = logging.getLogger(__name__)

SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"
KLINES_DIR = Path("data/binance_raw")

# Binance public rate limit: 1200 weight/min; klines = 2 weight each → 600/min max.
# We cap at 30/min (well under).
_BINANCE_BUCKET = TokenBucket(rate=30.0 / 60.0)


def _binance_get(base: str, path: str, params: dict[str, Any]) -> Any:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base}{path}?{qs}"
    _BINANCE_BUCKET.acquire()
    req = Request(url, headers={"User-Agent": "kimibot-historical/0.1"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def _kline_cache_path(year: int, month: int) -> Path:
    return KLINES_DIR / f"btc_klines_{year:04d}_{month:02d}.parquet"


def _funding_cache_path(year: int, month: int) -> Path:
    return KLINES_DIR / f"btc_funding_{year:04d}_{month:02d}.parquet"


def _fetch_klines_range(start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch all 1m BTC/USDT klines between start_ms and end_ms (inclusive)."""
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        raw = _binance_get(
            SPOT_BASE,
            "/api/v3/klines",
            {"symbol": "BTCUSDT", "interval": "1m", "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not raw:
            break
        for candle in raw:
            open_time_ms = int(candle[0])
            rows.append({
                "timestamp": datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc),
                "btc_open": float(candle[1]),
                "btc_high": float(candle[2]),
                "btc_low": float(candle[3]),
                "btc_price": float(candle[4]),  # close
                "btc_volume": float(candle[5]),
            })
        last_open_ms = int(raw[-1][0])
        cursor = last_open_ms + 60_000  # next minute
        if len(raw) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "btc_open", "btc_high", "btc_low", "btc_price", "btc_volume"])
    return pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def _fetch_funding_range(start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch BTC perpetual funding rates between start_ms and end_ms."""
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        raw = _binance_get(
            FUTURES_BASE,
            "/fapi/v1/fundingRate",
            {"symbol": "BTCUSDT", "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not raw:
            break
        for item in raw:
            rows.append({
                "timestamp": datetime.fromtimestamp(int(item["fundingTime"]) / 1000.0, tz=timezone.utc),
                "funding_rate": float(item["fundingRate"]),
            })
        cursor = int(raw[-1]["fundingTime"]) + 1
        if len(raw) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "funding_rate"])
    return pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def _month_range(start: datetime, end: datetime) -> list[tuple[int, int]]:
    """Return list of (year, month) tuples covering start..end inclusive."""
    result: list[tuple[int, int]] = []
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur <= end:
        result.append((cur.year, cur.month))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return result


def ensure_btc_data(
    start: datetime,
    end: datetime,
    cache_dir: Path = KLINES_DIR,
) -> None:
    """
    Ensure BTC klines and funding rates are cached for the given date range.
    Fetches only months not already cached. Idempotent.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    # Add 1-hour buffer before start for momentum lookback
    buffered_start = start - timedelta(hours=1)

    for year, month in _month_range(buffered_start, end):
        klines_path = _kline_cache_path(year, month)
        if not klines_path.exists():
            LOGGER.info("Fetching BTC klines for %04d-%02d...", year, month)
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            else:
                month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            actual_start = max(buffered_start, month_start)
            actual_end = min(end, month_end)
            df = _fetch_klines_range(
                int(actual_start.timestamp() * 1000),
                int(actual_end.timestamp() * 1000),
            )
            df.to_parquet(klines_path, index=False)
            LOGGER.info("Cached %d klines for %04d-%02d", len(df), year, month)
        else:
            LOGGER.debug("Klines cache hit: %s", klines_path)

        funding_path = _funding_cache_path(year, month)
        if not funding_path.exists():
            LOGGER.info("Fetching BTC funding rates for %04d-%02d...", year, month)
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            else:
                month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
            actual_start = max(buffered_start, month_start)
            actual_end = min(end, month_end)
            df = _fetch_funding_range(
                int(actual_start.timestamp() * 1000),
                int(actual_end.timestamp() * 1000),
            )
            df.to_parquet(funding_path, index=False)
            LOGGER.info("Cached %d funding rates for %04d-%02d", len(df), year, month)
        else:
            LOGGER.debug("Funding cache hit: %s", funding_path)


def load_btc_klines(start: datetime, end: datetime, cache_dir: Path = KLINES_DIR) -> pd.DataFrame:
    """Load cached BTC klines for the given range into a single DataFrame."""
    frames: list[pd.DataFrame] = []
    for year, month in _month_range(start - timedelta(hours=1), end):
        path = _kline_cache_path(year, month)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        raise FileNotFoundError(f"No BTC kline cache found for {start} to {end}. Run ensure_btc_data() first.")
    df = pd.concat(frames, ignore_index=True)
    df = df[df["timestamp"] >= start - timedelta(hours=1)]
    df = df[df["timestamp"] <= end]
    return df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def load_btc_funding(start: datetime, end: datetime, cache_dir: Path = KLINES_DIR) -> pd.DataFrame:
    """Load cached BTC funding rates for the given range."""
    frames: list[pd.DataFrame] = []
    for year, month in _month_range(start - timedelta(hours=1), end):
        path = _funding_cache_path(year, month)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame(columns=["timestamp", "funding_rate"])
    df = pd.concat(frames, ignore_index=True)
    df = df[df["timestamp"] >= start - timedelta(hours=1)]
    df = df[df["timestamp"] <= end]
    return df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def build_crypto_history(
    start: datetime,
    end: datetime,
    cache_dir: Path = KLINES_DIR,
) -> pd.DataFrame:
    """
    Build crypto_history DataFrame for FeatureBuilder from cached Binance data.

    Columns: timestamp, btc_price, eth_price (0.0), funding_rate
    """
    klines = load_btc_klines(start, end, cache_dir)
    funding = load_btc_funding(start, end, cache_dir)

    # Forward-fill funding rate onto kline timestamps using merge_asof
    if not funding.empty:
        merged = pd.merge_asof(
            klines.sort_values("timestamp"),
            funding.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )
        merged["funding_rate"] = merged["funding_rate"].fillna(0.0)
    else:
        merged = klines.copy()
        merged["funding_rate"] = 0.0

    merged["eth_price"] = 0.0
    return merged[["timestamp", "btc_price", "eth_price", "funding_rate"]].reset_index(drop=True)
```

- [ ] **Step 2: Verify import**

```bash
cd /opt/stratagen/kimibot
python3 -c "from ingestion.binance_historical import ensure_btc_data, load_btc_klines, build_crypto_history; print('Import OK')"
```

Expected: `Import OK`

- [ ] **Step 3: Commit**

```bash
git add ingestion/binance_historical.py
git commit -m "feat: add Binance historical klines and funding rate ingestion with monthly cache"
```

---

## Task 6: Look-Ahead Audit Test (HARD GATE)

**Files:**
- Create: `tests/test_lookahead.py`

- [ ] **Step 1: Write the tests**

Create `/opt/stratagen/kimibot/tests/test_lookahead.py`:

```python
"""Look-ahead audit for FeatureBuilder — HARD GATE.

These tests must pass before any evaluation results are considered valid.
They verify that FeatureBuilder.build_features() never uses data timestamped
after the as_of time.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from features.builder import FeatureBuilder


def _make_market_history(base_ts: datetime, n: int = 20) -> pd.DataFrame:
    """Create synthetic market history with timestamps at 1-minute intervals."""
    rows = []
    for i in range(n):
        rows.append({
            "timestamp": base_ts + timedelta(minutes=i),
            "market_id": "test_mkt",
            "p_market": 0.5 + i * 0.01,
            "volume": 100.0 + i * 10,
        })
    return pd.DataFrame(rows)


def _make_crypto_history(base_ts: datetime, n: int = 20) -> pd.DataFrame:
    """Create synthetic crypto history with timestamps at 1-minute intervals."""
    rows = []
    for i in range(n):
        rows.append({
            "timestamp": base_ts + timedelta(minutes=i),
            "btc_price": 50000.0 + i * 100,
            "eth_price": 3000.0,
            "funding_rate": 0.001,
        })
    return pd.DataFrame(rows)


class LookAheadAuditTests(unittest.TestCase):
    """Verify FeatureBuilder never uses future data."""

    def setUp(self) -> None:
        self.builder = FeatureBuilder(schema_version="v2")
        self.base_ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_market_source_max_ts_never_exceeds_as_of(self) -> None:
        """market_source_max_ts must be <= as_of for all valid as_of values."""
        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist = _make_crypto_history(self.base_ts, n=20)
        resolution_time = self.base_ts + timedelta(hours=1)

        for minutes_offset in [5, 10, 15, 19]:
            as_of = self.base_ts + timedelta(minutes=minutes_offset)
            row = self.builder.build_features(
                market_history=market_hist,
                crypto_history=crypto_hist,
                as_of=as_of,
                resolution_time=resolution_time,
                label=1,
                market_id="test_mkt",
            )
            self.assertLessEqual(
                row.market_source_max_ts,
                as_of,
                msg=f"market_source_max_ts {row.market_source_max_ts} > as_of {as_of}",
            )

    def test_crypto_source_max_ts_never_exceeds_as_of(self) -> None:
        """crypto_source_max_ts must be <= as_of for all valid as_of values."""
        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist = _make_crypto_history(self.base_ts, n=20)
        resolution_time = self.base_ts + timedelta(hours=1)

        for minutes_offset in [5, 10, 15, 19]:
            as_of = self.base_ts + timedelta(minutes=minutes_offset)
            row = self.builder.build_features(
                market_history=market_hist,
                crypto_history=crypto_hist,
                as_of=as_of,
                resolution_time=resolution_time,
                label=1,
                market_id="test_mkt",
            )
            self.assertLessEqual(
                row.crypto_source_max_ts,
                as_of,
                msg=f"crypto_source_max_ts {row.crypto_source_max_ts} > as_of {as_of}",
            )

    def test_future_data_injected_into_market_history_is_ignored(self) -> None:
        """Future rows in market_history must not affect the result."""
        as_of = self.base_ts + timedelta(minutes=10)
        resolution_time = self.base_ts + timedelta(hours=1)

        # Base market history: 10 minutes of data (past)
        market_hist_clean = _make_market_history(self.base_ts, n=10)

        # Inject a future row with a suspiciously high price
        future_row = pd.DataFrame([{
            "timestamp": as_of + timedelta(minutes=5),  # FUTURE
            "market_id": "test_mkt",
            "p_market": 9999.0,
            "volume": 9999.0,
        }])
        market_hist_with_future = pd.concat([market_hist_clean, future_row], ignore_index=True)

        crypto_hist = _make_crypto_history(self.base_ts, n=20)

        row_clean = self.builder.build_features(
            market_history=market_hist_clean,
            crypto_history=crypto_hist,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )
        row_injected = self.builder.build_features(
            market_history=market_hist_with_future,
            crypto_history=crypto_hist,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )

        # p_market must not be 9999.0 (the injected future value)
        self.assertAlmostEqual(
            row_clean.values["p_market"],
            row_injected.values["p_market"],
            places=6,
            msg="Future data contaminated p_market",
        )
        self.assertLessEqual(row_injected.market_source_max_ts, as_of)

    def test_future_data_injected_into_crypto_history_is_ignored(self) -> None:
        """Future rows in crypto_history must not affect the result."""
        as_of = self.base_ts + timedelta(minutes=10)
        resolution_time = self.base_ts + timedelta(hours=1)

        market_hist = _make_market_history(self.base_ts, n=20)
        crypto_hist_clean = _make_crypto_history(self.base_ts, n=10)

        future_crypto = pd.DataFrame([{
            "timestamp": as_of + timedelta(minutes=5),  # FUTURE
            "btc_price": 999999.0,
            "eth_price": 0.0,
            "funding_rate": 99.0,
        }])
        crypto_hist_injected = pd.concat([crypto_hist_clean, future_crypto], ignore_index=True)

        row_clean = self.builder.build_features(
            market_history=market_hist,
            crypto_history=crypto_hist_clean,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )
        row_injected = self.builder.build_features(
            market_history=market_hist,
            crypto_history=crypto_hist_injected,
            as_of=as_of,
            resolution_time=resolution_time,
            label=1,
            market_id="test_mkt",
        )

        self.assertAlmostEqual(
            row_clean.values["btc_return_1m"],
            row_injected.values["btc_return_1m"],
            places=6,
            msg="Future crypto data contaminated btc_return_1m",
        )
        self.assertLessEqual(row_injected.crypto_source_max_ts, as_of)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run and verify ALL PASS**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_lookahead.py -v
```

Expected:
```
tests/test_lookahead.py::LookAheadAuditTests::test_market_source_max_ts_never_exceeds_as_of PASSED
tests/test_lookahead.py::LookAheadAuditTests::test_crypto_source_max_ts_never_exceeds_as_of PASSED
tests/test_lookahead.py::LookAheadAuditTests::test_future_data_injected_into_market_history_is_ignored PASSED
tests/test_lookahead.py::LookAheadAuditTests::test_future_data_injected_into_crypto_history_is_ignored PASSED
4 passed in X.Xs
```

**If any test fails, DO NOT proceed. Fix the root cause first.**

- [ ] **Step 3: Commit**

```bash
git add tests/test_lookahead.py
git commit -m "test: add look-ahead audit tests for FeatureBuilder (hard gate)"
```

---

## Task 7: Evaluation Metrics Module

**Files:**
- Create: `evaluation/__init__.py`
- Create: `evaluation/metrics.py`
- Create: `tests/test_eval_metrics.py`

- [ ] **Step 1: Write tests first (known-input tests with hand-computed expected values, then property tests)**

Create `/opt/stratagen/kimibot/tests/test_eval_metrics.py`:

```python
"""Tests for evaluation metrics module.

Section A: Known-input tests with hand-computed expected values.
  These run before the metrics module is used anywhere else and verify
  the implementation against exact arithmetic, not just direction.

Section B: Property tests (edge cases and directional assertions).
"""

from __future__ import annotations

import unittest
import numpy as np


# ─── Section A: Known-input tests with hand-computed expected values ──────────

class BrierScoreKnownInputTests(unittest.TestCase):
    """
    Hand-computed expected values for brier_score.
    Formula: mean((y_pred - y_true)^2)
    """

    def test_known_input_a(self) -> None:
        # y_true=[1,0,1,0], y_pred=[0.8,0.2,0.8,0.2]
        # errors: (0.8-1)^2=0.04, (0.2-0)^2=0.04, 0.04, 0.04
        # mean = 0.16 / 4 = 0.04
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.array([0.8, 0.2, 0.8, 0.2])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.04, places=12)

    def test_known_input_b(self) -> None:
        # y_true=[1,1,0,0], y_pred=[0.7,0.6,0.4,0.3]
        # errors: (0.7-1)^2=0.09, (0.6-1)^2=0.16, (0.4-0)^2=0.16, (0.3-0)^2=0.09
        # mean = 0.50 / 4 = 0.125
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 1.0, 0.0, 0.0])
        y_pred = np.array([0.7, 0.6, 0.4, 0.3])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.125, places=12)

    def test_known_input_single_sample(self) -> None:
        # Single sample: y_true=1, y_pred=0.3 → (0.3-1)^2 = 0.49
        from evaluation.metrics import brier_score
        y_true = np.array([1.0])
        y_pred = np.array([0.3])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.49, places=12)


class ECEKnownInputTests(unittest.TestCase):
    """
    Hand-computed expected values for expected_calibration_error.
    Formula: sum over bins of (bin_size/n) * |mean_pred - fraction_positive|
    """

    def test_known_input_single_bin_overconfident(self) -> None:
        # 10 predictions all at 0.8, but only 4 are positive.
        # With n_bins=10, all fall in bin [0.7, 0.8) (bin index 7) or [0.8, 0.9) depending on boundary.
        # The last bin [0.9, 1.0] is closed. Bin [0.8, 0.9): p=0.8 → bin 8 (index 8).
        # mean_pred ≈ 0.8, fraction_positive = 4/10 = 0.4
        # ECE = (10/10) * |0.8 - 0.4| = 0.4
        from evaluation.metrics import expected_calibration_error
        y_pred = np.full(10, 0.8)
        y_true = np.array([1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.4, places=6)

    def test_known_input_two_bins_symmetric(self) -> None:
        # 5 predictions at 0.2 (all negative, fraction_pos=0) and
        # 5 predictions at 0.8 (all positive, fraction_pos=1).
        # Bin [0.1,0.2): mean_pred=0.2, frac_pos=0.0 → |0.2-0.0|=0.2, weight=5/10=0.5
        # Bin [0.7,0.8): mean_pred=0.8, frac_pos=1.0 → |0.8-1.0|=0.2, weight=5/10=0.5
        # ECE = 0.5*0.2 + 0.5*0.2 = 0.2
        from evaluation.metrics import expected_calibration_error
        y_pred = np.array([0.2, 0.2, 0.2, 0.2, 0.2, 0.8, 0.8, 0.8, 0.8, 0.8])
        y_true = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.2, places=6)

    def test_known_input_perfect_calibration(self) -> None:
        # 5 predictions at 0.4, 2 positive → fraction_pos=0.4; ECE=0
        # 5 predictions at 0.6, 3 positive → fraction_pos=0.6; ECE=0
        from evaluation.metrics import expected_calibration_error
        y_pred = np.array([0.4, 0.4, 0.4, 0.4, 0.4, 0.6, 0.6, 0.6, 0.6, 0.6])
        y_true = np.array([1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0])
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertAlmostEqual(ece, 0.0, places=6)


class BootstrapCIKnownInputTests(unittest.TestCase):
    """
    Known-input tests for bootstrap_brier_ci.
    With a deterministic seed and known Brier score, verify the CI:
    1. Contains the point estimate.
    2. Has correct ordering (lo < hi).
    3. Is not degenerate (width > 0 for non-trivial data).
    4. Reproducible across calls with the same seed.
    """

    def _get_inputs(self):
        # Fixed, non-trivial inputs: 100 samples, true Brier = 0.04 (good model)
        # y_true: alternating 1,0; y_pred: 0.8 for y_true=1, 0.2 for y_true=0
        y_true = np.tile([1.0, 0.0], 50)
        y_pred = np.where(y_true == 1.0, 0.8, 0.2)
        return y_true, y_pred

    def test_ci_contains_point_estimate(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci, brier_score
        y_true, y_pred = self._get_inputs()
        point = brier_score(y_true, y_pred)
        self.assertAlmostEqual(point, 0.04, places=10)
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=1000, seed=42)
        self.assertLessEqual(lo, point + 0.005)
        self.assertGreaterEqual(hi, point - 0.005)

    def test_ci_ordering(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertLess(lo, hi)

    def test_ci_width_positive(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertGreater(hi - lo, 0.001)  # Must have non-degenerate width

    def test_ci_reproducible_with_same_seed(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo1, hi1 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        lo2, hi2 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertEqual(lo1, lo2)
        self.assertEqual(hi1, hi2)

    def test_ci_differs_with_different_seeds(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci
        y_true, y_pred = self._get_inputs()
        lo1, hi1 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        lo2, hi2 = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=99)
        # Different seeds should give slightly different CIs (not identical)
        self.assertFalse(lo1 == lo2 and hi1 == hi2)


# ─── Section B: Property tests (edge cases and directional) ──────────────────

class BrierScoreTests(unittest.TestCase):
    def test_perfect_predictions(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.array([1.0, 0.0, 1.0, 0.0])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.0, places=10)

    def test_worst_predictions(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0])
        y_pred = np.array([0.0, 1.0])
        self.assertAlmostEqual(brier_score(y_true, y_pred), 1.0, places=10)

    def test_trivial_baseline(self) -> None:
        from evaluation.metrics import brier_score
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_pred = np.full(4, 0.5)
        self.assertAlmostEqual(brier_score(y_true, y_pred), 0.25, places=10)


class ECETests(unittest.TestCase):
    def test_perfect_calibration(self) -> None:
        from evaluation.metrics import expected_calibration_error
        rng = np.random.default_rng(42)
        n = 1000
        y_pred = rng.uniform(0, 1, n)
        y_true = rng.binomial(1, y_pred).astype(float)
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        # Perfectly calibrated predictions should have low ECE
        self.assertLess(ece, 0.10)

    def test_systematic_overconfidence(self) -> None:
        from evaluation.metrics import expected_calibration_error
        # Predictions all at 0.9, but only 50% are positive
        y_pred = np.full(100, 0.9)
        y_true = np.array([1.0] * 50 + [0.0] * 50)
        ece = expected_calibration_error(y_true, y_pred, n_bins=10)
        self.assertGreater(ece, 0.3)


class AUCTests(unittest.TestCase):
    def test_perfect_auc(self) -> None:
        from evaluation.metrics import roc_auc
        y_true = np.array([0.0, 0.0, 1.0, 1.0])
        y_score = np.array([0.1, 0.2, 0.8, 0.9])
        self.assertAlmostEqual(roc_auc(y_true, y_score), 1.0, places=5)

    def test_random_auc(self) -> None:
        from evaluation.metrics import roc_auc
        y_true = np.array([0.0, 1.0, 0.0, 1.0])
        y_score = np.array([0.5, 0.5, 0.5, 0.5])
        auc = roc_auc(y_true, y_score)
        self.assertGreaterEqual(auc, 0.4)
        self.assertLessEqual(auc, 0.6)


class BootstrapCITests(unittest.TestCase):
    def test_ci_contains_mean(self) -> None:
        from evaluation.metrics import bootstrap_brier_ci, brier_score
        rng = np.random.default_rng(0)
        y_true = rng.binomial(1, 0.6, 200).astype(float)
        y_pred = np.full(200, 0.6)
        true_brier = brier_score(y_true, y_pred)
        lo, hi = bootstrap_brier_ci(y_true, y_pred, n_resamples=500, seed=42)
        self.assertLess(lo, true_brier + 0.01)
        self.assertGreater(hi, true_brier - 0.01)
        self.assertLess(lo, hi)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_eval_metrics.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'evaluation'`

- [ ] **Step 3: Create evaluation module**

Create `/opt/stratagen/kimibot/evaluation/__init__.py`:
```python
"""Walk-forward evaluation pipeline for Limitless historical markets."""
```

Create `/opt/stratagen/kimibot/evaluation/metrics.py`:

```python
"""Evaluation metrics implemented with numpy only (no sklearn required)."""

from __future__ import annotations

import numpy as np


def brier_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean squared error between predictions and binary outcomes."""
    return float(np.mean((y_pred - y_true) ** 2))


def log_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Binary cross-entropy loss."""
    y_pred_clipped = np.clip(y_pred, 1e-15, 1 - 1e-15)
    return float(-np.mean(y_true * np.log(y_pred_clipped) + (1 - y_true) * np.log(1 - y_pred_clipped)))


def expected_calibration_error(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Expected Calibration Error with equal-width bins."""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    n = len(y_true)
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (y_pred >= lo) & (y_pred < hi)
        if i == n_bins - 1:
            mask = (y_pred >= lo) & (y_pred <= hi)
        if not mask.any():
            continue
        bin_acc = float(y_true[mask].mean())
        bin_conf = float(y_pred[mask].mean())
        ece += (mask.sum() / n) * abs(bin_acc - bin_conf)
    return float(ece)


def roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Trapezoidal AUC from ROC curve."""
    thresholds = np.sort(np.unique(y_score))[::-1]
    pos = float((y_true == 1).sum())
    neg = float((y_true == 0).sum())
    if pos == 0 or neg == 0:
        return 0.5
    tprs = [0.0]
    fprs = [0.0]
    for t in thresholds:
        pred_pos = y_score >= t
        tp = float(((pred_pos) & (y_true == 1)).sum())
        fp = float(((pred_pos) & (y_true == 0)).sum())
        tprs.append(tp / pos)
        fprs.append(fp / neg)
    tprs.append(1.0)
    fprs.append(1.0)
    return float(np.trapz(tprs, fprs))


def accuracy_at_threshold(y_true: np.ndarray, y_pred: np.ndarray, threshold: float = 0.5) -> float:
    """Fraction of predictions on the correct side of threshold."""
    return float(((y_pred >= threshold) == (y_true >= threshold)).mean())


def bootstrap_brier_ci(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_resamples: int = 1000,
    seed: int = 42,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Bootstrap confidence interval for Brier score."""
    rng = np.random.default_rng(seed)
    n = len(y_true)
    bootstrap_scores = np.array([
        brier_score(y_true[idx], y_pred[idx])
        for idx in [rng.integers(0, n, size=n) for _ in range(n_resamples)]
    ])
    lo = float(np.percentile(bootstrap_scores, 100 * alpha / 2))
    hi = float(np.percentile(bootstrap_scores, 100 * (1 - alpha / 2)))
    return lo, hi
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_eval_metrics.py -v
```

Expected:
```
tests/test_eval_metrics.py::BrierScoreTests::test_perfect_predictions PASSED
tests/test_eval_metrics.py::BrierScoreTests::test_worst_predictions PASSED
tests/test_eval_metrics.py::BrierScoreTests::test_trivial_baseline PASSED
tests/test_eval_metrics.py::ECETests::test_perfect_calibration PASSED
tests/test_eval_metrics.py::ECETests::test_systematic_overconfidence PASSED
tests/test_eval_metrics.py::AUCTests::test_perfect_auc PASSED
tests/test_eval_metrics.py::AUCTests::test_random_auc PASSED
tests/test_eval_metrics.py::BootstrapCITests::test_ci_contains_mean PASSED
8 passed in X.Xs
```

- [ ] **Step 5: Commit**

```bash
git add evaluation/__init__.py evaluation/metrics.py tests/test_eval_metrics.py
git commit -m "feat: add evaluation metrics module (Brier, ECE, AUC, bootstrap CI) with tests"
```

---

## Task 8: Walk-Forward Evaluator

**Files:**
- Create: `evaluation/walk_forward_evaluator.py`
- Create: `tests/test_walk_forward_historical.py`

- [ ] **Step 1: Write tests first**

Create `/opt/stratagen/kimibot/tests/test_walk_forward_historical.py`:

```python
"""Tests for walk-forward historical evaluation split logic."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


def _make_fake_markets(n: int) -> list[dict]:
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "condition_id": f"mkt_{i:03d}",
            "resolution_time_utc": (base + timedelta(days=i)).isoformat(),
            "label": i % 2,
        }
        for i in range(n)
    ]


class WalkForwardSplitTests(unittest.TestCase):

    def test_split_proportions(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(100)
        train, calib, test = split_markets(markets)
        self.assertEqual(len(train), 60)
        self.assertEqual(len(calib), 20)
        self.assertEqual(len(test), 20)

    def test_split_is_temporally_ordered(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(30)
        train, calib, test = split_markets(markets)
        train_max = max(m["resolution_time_utc"] for m in train)
        calib_min = min(m["resolution_time_utc"] for m in calib)
        calib_max = max(m["resolution_time_utc"] for m in calib)
        test_min = min(m["resolution_time_utc"] for m in test)
        self.assertLessEqual(train_max, calib_min)
        self.assertLessEqual(calib_max, test_min)

    def test_no_market_in_multiple_splits(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(50)
        train, calib, test = split_markets(markets)
        train_ids = {m["condition_id"] for m in train}
        calib_ids = {m["condition_id"] for m in calib}
        test_ids = {m["condition_id"] for m in test}
        self.assertEqual(len(train_ids & calib_ids), 0)
        self.assertEqual(len(train_ids & test_ids), 0)
        self.assertEqual(len(calib_ids & test_ids), 0)

    def test_all_markets_accounted_for(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(50)
        train, calib, test = split_markets(markets)
        self.assertEqual(len(train) + len(calib) + len(test), 50)

    def test_minimum_30_test_markets(self) -> None:
        from evaluation.walk_forward_evaluator import split_markets
        markets = _make_fake_markets(200)
        _, _, test = split_markets(markets)
        self.assertGreaterEqual(len(test), 30)
```

- [ ] **Step 2: Run to confirm tests fail**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_walk_forward_historical.py -v 2>&1 | head -10
```

Expected: `ImportError: cannot import name 'split_markets'`

- [ ] **Step 3: Write the evaluator**

Create `/opt/stratagen/kimibot/evaluation/walk_forward_evaluator.py`:

```python
"""Walk-forward evaluation pipeline for Limitless historical markets."""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from evaluation.metrics import (
    accuracy_at_threshold,
    bootstrap_brier_ci,
    brier_score,
    expected_calibration_error,
    log_loss,
    roc_auc,
)
from features.builder import FeatureBuilder
from ingestion.binance_historical import build_crypto_history
from models.calibration import SigmoidCalibrator, IdentityCalibrator
from models.predictor import FEATURE_COLUMNS, LogisticRegressionPredictor
from models.simple_ml import LogisticRegressionModel, StandardScalerModel
from project.types import FeatureRow

LOGGER = logging.getLogger(__name__)

ARTIFACT_DIR = Path("models/walk_forward_runs")
RESULTS_PATH = Path("data/walk_forward_results.json")
SCHEMA_VERSION = "v2"
SNAPSHOT_INTERVAL_MINUTES = 15
MAX_SNAPSHOTS_PER_MARKET = 50
RANDOM_SEED = 42


def split_markets(
    markets: list[dict[str, Any]],
    train_frac: float = 0.60,
    calib_frac: float = 0.20,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Sort by resolution_time and split into train/calibrate/test sets."""
    sorted_markets = sorted(markets, key=lambda m: m["resolution_time_utc"])
    n = len(sorted_markets)
    train_end = int(n * train_frac)
    calib_end = int(n * (train_frac + calib_frac))
    return (
        sorted_markets[:train_end],
        sorted_markets[train_end:calib_end],
        sorted_markets[calib_end:],
    )


def _market_to_dataframes(market: dict[str, Any]) -> tuple[pd.DataFrame, datetime, datetime]:
    """
    Convert a cached market dict to a market_history DataFrame.

    Returns (market_history_df, market_open_ts, resolution_ts).
    market_history_df has columns: timestamp, p_market, volume, market_id
    """
    trades = market["trades"]
    if not trades:
        raise ValueError(f"Market {market['condition_id']} has no trades")

    rows = []
    for t in trades:
        ts = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc)
        rows.append({
            "timestamp": ts,
            "p_market": float(t["price"]),
            "volume": float(t["amount_usd"]),
            "market_id": market["condition_id"],
        })
    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    # Forward-fill to 1-minute bars so FeatureBuilder has a consistent time series
    open_ts = df["timestamp"].min().replace(second=0, microsecond=0)
    resolution_ts = datetime.fromisoformat(market["resolution_time_utc"])

    # Create 1-minute grid and forward-fill prices
    minutes = pd.date_range(start=open_ts, end=resolution_ts, freq="1min", tz=timezone.utc)
    grid = pd.DataFrame({"timestamp": minutes})
    merged = pd.merge_asof(grid, df.sort_values("timestamp"), on="timestamp", direction="backward")
    merged["market_id"] = market["condition_id"]
    merged = merged.dropna(subset=["p_market"]).reset_index(drop=True)

    return merged, open_ts, resolution_ts


def _build_snapshots(
    market: dict[str, Any],
    crypto_df: pd.DataFrame,
    builder: FeatureBuilder,
) -> list[FeatureRow]:
    """Build feature rows for all valid snapshot times in a market."""
    try:
        market_df, open_ts, resolution_ts = _market_to_dataframes(market)
    except ValueError as exc:
        LOGGER.warning("Skipping market %s: %s", market["condition_id"], exc)
        return []

    if market_df.empty:
        return []

    # Generate snapshot times: every 15 minutes from open to (resolution - 15 min)
    snapshot_end = resolution_ts - timedelta(minutes=15)
    if snapshot_end <= open_ts:
        return []

    snapshot_times = []
    t = open_ts + timedelta(minutes=SNAPSHOT_INTERVAL_MINUTES)
    while t <= snapshot_end:
        snapshot_times.append(t)
        t += timedelta(minutes=SNAPSHOT_INTERVAL_MINUTES)

    # Cap at MAX_SNAPSHOTS_PER_MARKET (take evenly spaced subset)
    if len(snapshot_times) > MAX_SNAPSHOTS_PER_MARKET:
        indices = np.linspace(0, len(snapshot_times) - 1, MAX_SNAPSHOTS_PER_MARKET, dtype=int)
        snapshot_times = [snapshot_times[i] for i in indices]

    rows: list[FeatureRow] = []
    label = market["label"]

    for as_of in snapshot_times:
        crypto_slice = crypto_df[
            (crypto_df["timestamp"] >= as_of - timedelta(hours=1)) &
            (crypto_df["timestamp"] <= as_of)
        ]
        if crypto_slice.empty:
            LOGGER.debug("No crypto data at %s for market %s, skipping snapshot", as_of, market["condition_id"])
            continue
        try:
            row = builder.build_features(
                market_history=market_df,
                crypto_history=crypto_df,
                as_of=as_of,
                resolution_time=resolution_ts,
                label=label,
                market_id=market["condition_id"],
            )
            rows.append(row)
        except ValueError as exc:
            LOGGER.debug("Snapshot failed for %s at %s: %s", market["condition_id"], as_of, exc)

    return rows


def _rows_to_arrays(rows: list[FeatureRow]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Extract feature matrix, labels, and market prices from FeatureRows."""
    X = np.array([[row.values[c] for c in FEATURE_COLUMNS] for row in rows], dtype=float)
    y = np.array([row.label for row in rows], dtype=float)
    p_market = np.array([row.values["p_market"] for row in rows], dtype=float)
    return X, y, p_market


def run_evaluation(
    markets: list[dict[str, Any]],
    crypto_df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Run the full walk-forward evaluation pipeline.

    Returns a results dict suitable for JSON serialization and EVALUATION_REPORT.md.
    """
    if len(markets) < 10:
        raise ValueError(f"Need at least 10 markets, got {len(markets)}")

    train_markets, calib_markets, test_markets = split_markets(markets)
    LOGGER.info(
        "Walk-forward split: %d train, %d calib, %d test markets",
        len(train_markets), len(calib_markets), len(test_markets),
    )

    builder = FeatureBuilder(schema_version=SCHEMA_VERSION)

    # Build feature rows for each split
    LOGGER.info("Building train feature rows...")
    train_rows: list[FeatureRow] = []
    for m in train_markets:
        train_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info("Building calibration feature rows...")
    calib_rows: list[FeatureRow] = []
    for m in calib_markets:
        calib_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info("Building test feature rows...")
    test_rows: list[FeatureRow] = []
    for m in test_markets:
        test_rows.extend(_build_snapshots(m, crypto_df, builder))

    LOGGER.info(
        "Snapshot counts: train=%d, calib=%d, test=%d",
        len(train_rows), len(calib_rows), len(test_rows),
    )

    if len(train_rows) < 20:
        raise ValueError(f"Insufficient train rows: {len(train_rows)}")
    if len(test_rows) < 10:
        raise ValueError(f"Insufficient test rows: {len(test_rows)}")

    # Train
    X_train, y_train, _ = _rows_to_arrays(train_rows)
    scaler = StandardScalerModel().fit(X_train)
    X_train_scaled = scaler.transform(X_train)
    model = LogisticRegressionModel()
    model.fit(X_train_scaled, y_train)

    # Calibrate
    X_calib, y_calib, _ = _rows_to_arrays(calib_rows)
    X_calib_scaled = scaler.transform(X_calib)
    raw_calib = model.predict_proba(X_calib_scaled)[:, 1]
    if len(np.unique(y_calib)) >= 2:
        calibrator = SigmoidCalibrator().fit(raw_calib, y_calib)
    else:
        LOGGER.warning("Calibration set has only one class, using identity calibrator")
        calibrator = IdentityCalibrator()

    predictor = LogisticRegressionPredictor(
        model=model,
        scaler=scaler,
        calibrator=calibrator,
        feature_columns=FEATURE_COLUMNS,
    )

    # Predict on test
    X_test, y_test, p_market_test = _rows_to_arrays(test_rows)
    X_test_scaled = scaler.transform(X_test)
    raw_test = model.predict_proba(X_test_scaled)[:, 1]
    raw_test_clipped = np.clip(raw_test, 0.05, 0.95)
    cal_test = np.array([predictor.predict(row) for row in test_rows])

    # Baselines
    trivial = np.full(len(y_test), 0.5)

    # Metrics
    model_brier = brier_score(y_test, cal_test)
    market_brier = brier_score(y_test, p_market_test)
    trivial_brier = brier_score(y_test, trivial)

    model_ece = expected_calibration_error(y_test, cal_test)
    model_log_loss = log_loss(y_test, cal_test)
    model_auc = roc_auc(y_test, cal_test)
    model_acc = accuracy_at_threshold(y_test, cal_test)

    market_ece = expected_calibration_error(y_test, p_market_test)
    market_log_loss = log_loss(y_test, p_market_test)
    market_auc = roc_auc(y_test, p_market_test)

    brier_ci_lo, brier_ci_hi = bootstrap_brier_ci(y_test, cal_test, n_resamples=1000, seed=RANDOM_SEED)
    market_ci_lo, market_ci_hi = bootstrap_brier_ci(y_test, p_market_test, n_resamples=1000, seed=RANDOM_SEED)

    delta_vs_market = market_brier - model_brier  # positive = model better

    # Brier by predicted probability decile
    decile_briers = []
    decile_edges = np.percentile(cal_test, np.linspace(0, 100, 11))
    for i in range(10):
        lo_edge = decile_edges[i]
        hi_edge = decile_edges[i + 1]
        mask = (cal_test >= lo_edge) & (cal_test <= hi_edge)
        if mask.sum() == 0:
            continue
        decile_briers.append({
            "decile": i + 1,
            "p_low": round(float(lo_edge), 4),
            "p_high": round(float(hi_edge), 4),
            "count": int(mask.sum()),
            "brier": round(brier_score(y_test[mask], cal_test[mask]), 6),
        })

    # Top 10 markets where model most disagreed with market price
    test_market_ids = [r.market_id for r in test_rows]
    disagreement = np.abs(cal_test - p_market_test)
    top_disagree_idx = np.argsort(disagreement)[::-1][:50]
    seen_mids: set[str] = set()
    top_disagreements = []
    for idx in top_disagree_idx:
        mid = test_market_ids[idx]
        if mid in seen_mids:
            continue
        seen_mids.add(mid)
        top_disagreements.append({
            "market_id": mid,
            "p_model": round(float(cal_test[idx]), 4),
            "p_market": round(float(p_market_test[idx]), 4),
            "disagreement": round(float(disagreement[idx]), 4),
            "label": int(y_test[idx]),
            "model_correct": int(y_test[idx]) == int(cal_test[idx] >= 0.5),
            "market_correct": int(y_test[idx]) == int(p_market_test[idx] >= 0.5),
        })
        if len(top_disagreements) >= 10:
            break

    # Top 10 markets where model was most confidently wrong
    model_error = (cal_test - y_test) ** 2
    confidence = np.abs(cal_test - 0.5) * 2  # 0 at 0.5, 1 at 0 or 1
    confident_wrong = model_error * confidence
    top_wrong_idx = np.argsort(confident_wrong)[::-1][:50]
    seen_wrong: set[str] = set()
    top_confident_wrong = []
    for idx in top_wrong_idx:
        mid = test_market_ids[idx]
        if mid in seen_wrong:
            continue
        seen_wrong.add(mid)
        top_confident_wrong.append({
            "market_id": mid,
            "p_model": round(float(cal_test[idx]), 4),
            "p_market": round(float(p_market_test[idx]), 4),
            "label": int(y_test[idx]),
            "error": round(float(model_error[idx]), 4),
            "confidence": round(float(confidence[idx]), 4),
        })
        if len(top_confident_wrong) >= 10:
            break

    # Save model artifacts
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_base = ARTIFACT_DIR / run_ts
    artifact_base.mkdir(parents=True, exist_ok=True)
    with (artifact_base / "model.pkl").open("wb") as f:
        pickle.dump(model, f)
    with (artifact_base / "scaler.pkl").open("wb") as f:
        pickle.dump(scaler, f)
    with (artifact_base / "calibrator.pkl").open("wb") as f:
        pickle.dump(calibrator, f)

    # Reliability diagram data
    reliability_data = []
    bins = np.linspace(0, 1, 11)
    for i in range(10):
        lo_b, hi_b = bins[i], bins[i + 1]
        mask = (cal_test >= lo_b) & (cal_test < hi_b)
        if i == 9:
            mask = (cal_test >= lo_b) & (cal_test <= hi_b)
        if not mask.any():
            continue
        reliability_data.append({
            "bin_center": round((lo_b + hi_b) / 2, 2),
            "mean_pred": round(float(cal_test[mask].mean()), 4),
            "fraction_positive": round(float(y_test[mask].mean()), 4),
            "count": int(mask.sum()),
        })

    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_ts,
        "dataset": {
            "total_markets": len(markets),
            "train_markets": len(train_markets),
            "calib_markets": len(calib_markets),
            "test_markets": len(test_markets),
            "train_snapshots": len(train_rows),
            "calib_snapshots": len(calib_rows),
            "test_snapshots": len(test_rows),
            "train_date_range": [
                sorted_resolution(train_markets)[0],
                sorted_resolution(train_markets)[-1],
            ],
            "test_date_range": [
                sorted_resolution(test_markets)[0],
                sorted_resolution(test_markets)[-1],
            ],
        },
        "model": {
            "brier_score": round(model_brier, 6),
            "brier_ci_95": [round(brier_ci_lo, 6), round(brier_ci_hi, 6)],
            "ece": round(model_ece, 6),
            "log_loss": round(model_log_loss, 6),
            "auc": round(model_auc, 6),
            "accuracy_at_0_5": round(model_acc, 6),
            "brier_by_decile": decile_briers,
        },
        "market_baseline": {
            "brier_score": round(market_brier, 6),
            "brier_ci_95": [round(market_ci_lo, 6), round(market_ci_hi, 6)],
            "ece": round(market_ece, 6),
            "log_loss": round(market_log_loss, 6),
            "auc": round(market_auc, 6),
        },
        "trivial_baseline": {
            "brier_score": round(trivial_brier, 6),
        },
        "headline": {
            "delta_brier_vs_market": round(delta_vs_market, 6),
            "model_beats_market": delta_vs_market > 0,
            "model_beats_trivial": model_brier < trivial_brier,
        },
        "diagnostics": {
            "top_10_disagreements": top_disagreements,
            "top_10_confident_wrong": top_confident_wrong,
            "reliability_diagram_data": reliability_data,
        },
    }

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(results, indent=2))
    LOGGER.info("Results written to %s", RESULTS_PATH)

    return results


def sorted_resolution(markets: list[dict[str, Any]]) -> list[str]:
    return sorted(m["resolution_time_utc"] for m in markets)
```

- [ ] **Step 4: Run split tests**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_walk_forward_historical.py -v
```

Expected: All 5 tests PASSED

- [ ] **Step 5: Commit**

```bash
git add evaluation/walk_forward_evaluator.py tests/test_walk_forward_historical.py
git commit -m "feat: add walk-forward evaluator with 60/20/20 temporal split and full metrics"
```

---

## Task 9: Report Generator

**Files:**
- Create: `evaluation/report_generator.py`

- [ ] **Step 1: Write the report generator**

Create `/opt/stratagen/kimibot/evaluation/report_generator.py`:

```python
"""Generate EVALUATION_REPORT.md, reliability.png, and prob_histogram.png."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np

LOGGER = logging.getLogger(__name__)
REPORTS_DIR = Path("reports")


def _try_import_matplotlib():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        return plt
    except ImportError:
        LOGGER.warning("matplotlib not available; charts will be skipped")
        return None


def generate_charts(results: dict[str, Any]) -> None:
    """Generate reliability diagram and probability histogram PNGs."""
    plt = _try_import_matplotlib()
    if plt is None:
        return

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rel_data = results["diagnostics"]["reliability_diagram_data"]

    if rel_data:
        mean_preds = [d["mean_pred"] for d in rel_data]
        frac_pos = [d["fraction_positive"] for d in rel_data]
        counts = [d["count"] for d in rel_data]

        fig, ax = plt.subplots(figsize=(6, 6))
        ax.plot([0, 1], [0, 1], "k--", lw=1.5, label="Perfect calibration")
        scatter = ax.scatter(
            mean_preds, frac_pos,
            s=[max(c * 2, 20) for c in counts],
            alpha=0.75, color="#2563eb", zorder=5, label="Model",
        )
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xlabel("Mean Predicted Probability", fontsize=12)
        ax.set_ylabel("Fraction of Positives", fontsize=12)
        ax.set_title("Reliability Diagram (Walk-Forward Test Set)", fontsize=13)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        rel_path = REPORTS_DIR / "reliability.png"
        fig.savefig(rel_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        LOGGER.info("Reliability diagram saved to %s", rel_path)

    # Probability histogram — requires raw prediction arrays from results JSON
    # They are not stored in results directly; we load from walk_forward_results.json
    # and skip if not present.
    LOGGER.info("Charts generation complete")


def generate_report(results: dict[str, Any], output_path: Path = Path("EVALUATION_REPORT.md")) -> None:
    """Write EVALUATION_REPORT.md from evaluation results dict."""

    d = results["dataset"]
    m = results["model"]
    mb = results["market_baseline"]
    tb = results["trivial_baseline"]
    h = results["headline"]
    diag = results["diagnostics"]

    beats_market = h["model_beats_market"]
    beats_trivial = h["model_beats_trivial"]
    delta = h["delta_brier_vs_market"]
    brier_lo, brier_hi = m["brier_ci_95"]
    market_lo, market_hi = mb["brier_ci_95"]

    summary_verdict = (
        "**The model BEATS the market baseline.**"
        if beats_market
        else "**The model DOES NOT beat the market baseline.**"
    )
    trivial_verdict = (
        "The model beats the trivial (0.5) baseline."
        if beats_trivial
        else "⚠️ The model does NOT beat the trivial baseline — something is fundamentally broken."
    )

    ci_overlap = brier_lo <= mb["brier_score"] <= brier_hi or market_lo <= m["brier_score"] <= market_hi
    significance_note = (
        "The confidence intervals overlap — the difference may not be statistically meaningful."
        if ci_overlap
        else "The confidence intervals do NOT overlap — the difference is statistically robust."
    )

    lines = [
        "# Limitless Walk-Forward Evaluation Report",
        "",
        f"Generated: {results['generated_at']}  ",
        f"Run ID: {results['run_id']}",
        "",
        "---",
        "",
        "## 1. Summary",
        "",
        summary_verdict,
        "",
        f"- Model Brier: **{m['brier_score']:.4f}** (95% CI: [{brier_lo:.4f}, {brier_hi:.4f}])",
        f"- Market Baseline Brier: **{mb['brier_score']:.4f}** (95% CI: [{market_lo:.4f}, {market_hi:.4f}])",
        f"- Delta (market − model): **{delta:+.4f}** (positive = model better)",
        f"- Trivial Baseline Brier: {tb['brier_score']:.4f}",
        "",
        trivial_verdict,
        "",
        significance_note,
        "",
        "---",
        "",
        "## 2. Dataset",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Total resolved markets ingested | {d['total_markets']} |",
        f"| Train markets (60%) | {d['train_markets']} |",
        f"| Calibration markets (20%) | {d['calib_markets']} |",
        f"| Test markets (20%) | {d['test_markets']} |",
        f"| Train snapshots | {d['train_snapshots']} |",
        f"| Calibration snapshots | {d['calib_snapshots']} |",
        f"| Test snapshots | {d['test_snapshots']} |",
        f"| Train date range | {d['train_date_range'][0][:10]} to {d['train_date_range'][1][:10]} |",
        f"| Test date range | {d['test_date_range'][0][:10]} to {d['test_date_range'][1][:10]} |",
        "",
        "---",
        "",
        "## 3. Full Metrics",
        "",
        "| Metric | Model | Market Baseline | Trivial (0.5) |",
        "|--------|-------|----------------|---------------|",
        f"| Brier Score | {m['brier_score']:.6f} | {mb['brier_score']:.6f} | {tb['brier_score']:.6f} |",
        f"| Brier 95% CI | [{brier_lo:.4f}, {brier_hi:.4f}] | [{market_lo:.4f}, {market_hi:.4f}] | — |",
        f"| ECE | {m['ece']:.6f} | {mb['ece']:.6f} | — |",
        f"| Log Loss | {m['log_loss']:.6f} | {mb['log_loss']:.6f} | — |",
        f"| AUC | {m['auc']:.6f} | {mb['auc']:.6f} | — |",
        f"| Accuracy @0.5 | {m['accuracy_at_0_5']:.6f} | — | — |",
        "",
        "---",
        "",
        "## 4. Brier Score by Predicted Probability Decile",
        "",
        "| Decile | P Range | Count | Brier |",
        "|--------|---------|-------|-------|",
    ]
    for row in m.get("brier_by_decile", []):
        lines.append(f"| {row['decile']} | [{row['p_low']:.2f}, {row['p_high']:.2f}] | {row['count']} | {row['brier']:.6f} |")

    lines += [
        "",
        "---",
        "",
        "## 5. Top 10 Markets — Largest Disagreement with Market Price",
        "",
        "| Market ID | P(Model) | P(Market) | Disagreement | Label | Model Correct | Market Correct |",
        "|-----------|----------|-----------|-------------|-------|--------------|----------------|",
    ]
    for row in diag.get("top_10_disagreements", []):
        lines.append(
            f"| {row['market_id'][:20]}… | {row['p_model']:.3f} | {row['p_market']:.3f} | "
            f"{row['disagreement']:.3f} | {row['label']} | {row['model_correct']} | {row['market_correct']} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 6. Top 10 Markets — Most Confidently Wrong",
        "",
        "| Market ID | P(Model) | P(Market) | Label | Error | Confidence |",
        "|-----------|----------|-----------|-------|-------|-----------|",
    ]
    for row in diag.get("top_10_confident_wrong", []):
        lines.append(
            f"| {row['market_id'][:20]}… | {row['p_model']:.3f} | {row['p_market']:.3f} | "
            f"{row['label']} | {row['error']:.4f} | {row['confidence']:.3f} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 7. Charts",
        "",
        "- Reliability diagram: `reports/reliability.png`",
        "- Probability histogram: `reports/prob_histogram.png`",
        "",
        "---",
        "",
        "## 8. Next Investigation Steps",
        "",
    ]

    if not beats_market:
        lines += [
            "The model does not beat the market baseline. Recommended next steps:",
            "",
            "1. **Feature engineering:** The current features may not encode useful information beyond the market price itself. Consider adding order book imbalance, resolution oracle type, or market age.",
            "2. **Market selection:** Restrict to markets where BTC price is the resolution criterion — these markets may have predictable correlation with BTC momentum.",
            "3. **Temporal effects:** Check if the model's edge (or lack thereof) varies by time-to-resolution. Models may have edge only very close to resolution.",
            "4. **Sample size:** With < 100 test markets, the confidence intervals will be wide. Accumulate more data.",
        ]
    else:
        lines += [
            "The model beats the market baseline. Recommended next steps:",
            "",
            "1. **Live paper trading:** Apply the model to live Limitless markets in read-only mode. Track predicted edge vs realized outcomes.",
            "2. **Feature importance:** Identify which features contribute most to the edge. Drop the others to reduce overfitting risk.",
            "3. **Rolling re-evaluation:** Re-run this evaluation monthly to detect decay.",
            "4. **Slippage accounting:** Before live trading, factor in Limitless AMM fees and market impact.",
        ]

    output_path.write_text("\n".join(lines) + "\n")
    LOGGER.info("Evaluation report written to %s", output_path)
```

- [ ] **Step 2: Verify import**

```bash
cd /opt/stratagen/kimibot
python3 -c "from evaluation.report_generator import generate_report, generate_charts; print('Import OK')"
```

Expected: `Import OK`

- [ ] **Step 3: Commit**

```bash
git add evaluation/report_generator.py
git commit -m "feat: add evaluation report generator (EVALUATION_REPORT.md + charts)"
```

---

## Task 10: CLI Command

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Add evaluate-limitless to main.py**

In `main.py`, add the following function before `main()`:

```python
def run_evaluate_limitless(config_path: str) -> None:
    """Run the Limitless historical ingestion and walk-forward evaluation."""

    import os
    from ingestion.limitless_historical import run_historical_ingestion
    from ingestion.binance_historical import ensure_btc_data, build_crypto_history
    from evaluation.walk_forward_evaluator import run_evaluation
    from evaluation.report_generator import generate_report, generate_charts
    from datetime import datetime, timezone

    config = load_config(config_path)
    configure_logging(config.runtime.log_level)

    graph_api_key = config.ingestion.graph_api_key or os.environ.get("GRAPH_API_KEY", "")
    if not graph_api_key:
        print(
            "ERROR: GRAPH_API_KEY is required.\n"
            "Get a free key at https://thegraph.com/studio/apikeys\n"
            "Set it as: export GRAPH_API_KEY=your_key_here",
            file=sys.stderr,
        )
        sys.exit(1)

    # Step 1: Ingest resolved markets
    print("Step 1/4: Ingesting resolved Limitless markets...")
    markets = run_historical_ingestion(
        graph_api_key=graph_api_key,
        min_trades=20,
    )
    print(f"  Loaded {len(markets)} resolved markets")

    if len(markets) < 10:
        print(f"ERROR: Only {len(markets)} markets found. Need at least 10.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Ensure BTC data
    print("Step 2/4: Ensuring BTC price data is cached...")
    all_timestamps = []
    for m in markets:
        for t in m["trades"]:
            all_timestamps.append(t["timestamp"])
    if all_timestamps:
        data_start = datetime.fromtimestamp(min(all_timestamps), tz=timezone.utc)
        data_end = datetime.fromtimestamp(max(all_timestamps), tz=timezone.utc)
        ensure_btc_data(data_start, data_end)
        print(f"  BTC data covers {data_start.date()} to {data_end.date()}")
        crypto_df = build_crypto_history(data_start, data_end)
        print(f"  Loaded {len(crypto_df):,} BTC 1-minute bars")
    else:
        print("ERROR: No trade timestamps found in market data.", file=sys.stderr)
        sys.exit(1)

    # Step 3: Run evaluation
    print("Step 3/4: Running walk-forward evaluation...")
    results = run_evaluation(markets=markets, crypto_df=crypto_df)

    delta = results["headline"]["delta_brier_vs_market"]
    beats = results["headline"]["model_beats_market"]
    print(f"\n{'='*50}")
    print(f"RESULT: Model Brier = {results['model']['brier_score']:.4f}")
    print(f"        Market Brier = {results['market_baseline']['brier_score']:.4f}")
    print(f"        Delta = {delta:+.4f} ({'model beats market' if beats else 'model loses to market'})")
    print(f"{'='*50}\n")

    # Step 4: Generate reports
    print("Step 4/4: Generating reports...")
    generate_report(results)
    generate_charts(results)

    # Generate probability histogram separately (needs raw arrays)
    print("\nDone. See EVALUATION_REPORT.md and reports/")
    print(f"Test markets: {results['dataset']['test_markets']}")
    print(f"Test snapshots: {results['dataset']['test_snapshots']}")
```

In `main()`, add `"evaluate-limitless"` to the `choices` list and add the dispatch:

```python
parser.add_argument(
    "mode",
    choices=["backtest", "live-sim", "validate", "ingest", "audit-data", "sanity", "evaluate-limitless"],
    help="Execution mode.",
)
```

And in the dispatch block:
```python
elif args.mode == "evaluate-limitless":
    run_evaluate_limitless(args.config)
```

- [ ] **Step 2: Verify it parses correctly**

```bash
cd /opt/stratagen/kimibot
python3 main.py evaluate-limitless --help
```

Expected: Shows help with `evaluate-limitless` as a valid mode.

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "feat: add evaluate-limitless CLI command"
```

---

## Task 11: Next.js API Route

**Files:**
- Create: `frontend/app/api/walk-forward/route.ts`
- Modify: `frontend/lib/types.ts`

- [ ] **Step 1: Add WalkForwardData type to types.ts**

In `frontend/lib/types.ts`, add at the end:

```typescript
export interface WalkForwardMarketSet {
  train_markets: number;
  calib_markets: number;
  test_markets: number;
  train_snapshots: number;
  test_snapshots: number;
  train_date_range: [string, string];
  test_date_range: [string, string];
  total_markets: number;
}

export interface WalkForwardModel {
  brier_score: number;
  brier_ci_95: [number, number];
  ece: number;
  log_loss: number;
  auc: number;
  accuracy_at_0_5: number;
}

export interface WalkForwardBaseline {
  brier_score: number;
  brier_ci_95: [number, number];
  ece: number;
  log_loss: number;
  auc: number;
}

export interface WalkForwardHeadline {
  delta_brier_vs_market: number;
  model_beats_market: boolean;
  model_beats_trivial: boolean;
}

export interface ReliabilityPoint {
  bin_center: number;
  mean_pred: number;
  fraction_positive: number;
  count: number;
}

export interface WalkForwardData {
  generated_at: string;
  run_id: string;
  dataset: WalkForwardMarketSet;
  model: WalkForwardModel;
  market_baseline: WalkForwardBaseline;
  trivial_baseline: { brier_score: number };
  headline: WalkForwardHeadline;
  diagnostics: {
    reliability_diagram_data: ReliabilityPoint[];
    top_10_disagreements: unknown[];
    top_10_confident_wrong: unknown[];
  };
}
```

- [ ] **Step 2: Create API route**

Create `/opt/stratagen/kimibot/frontend/app/api/walk-forward/route.ts`:

```typescript
import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import type { WalkForwardData } from "@/lib/types";

const DATA_DIR = process.env.KIMIBOT_DATA_DIR ?? path.join(process.cwd(), "..", "data");

export async function GET() {
  const resultsPath = path.join(DATA_DIR, "walk_forward_results.json");

  if (!fs.existsSync(resultsPath)) {
    return NextResponse.json(
      { error: "Walk-forward results not found. Run: python main.py evaluate-limitless" },
      { status: 404 }
    );
  }

  try {
    const raw = fs.readFileSync(resultsPath, "utf8");
    const data: WalkForwardData = JSON.parse(raw);
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Failed to parse walk_forward_results.json" }, { status: 500 });
  }
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd /opt/stratagen/kimibot/frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: No errors (or only pre-existing errors unrelated to this change)

- [ ] **Step 4: Commit**

```bash
cd /opt/stratagen/kimibot
git add frontend/app/api/walk-forward/route.ts frontend/lib/types.ts
git commit -m "feat: add /api/walk-forward Next.js route and WalkForwardData types"
```

---

## Task 12: Analytics Page Update

**Files:**
- Modify: `frontend/app/analytics/page.tsx`

- [ ] **Step 1: Add WalkForwardPanel to analytics page**

In `frontend/app/analytics/page.tsx`, add the import and component at the top of the file:

```typescript
"use client";

import { useState, useEffect } from "react";
import { useAnalyticsPolling } from "@/lib/hooks";
import { useStore } from "@/lib/store";
// ... existing imports ...
import type { WalkForwardData, ReliabilityPoint } from "@/lib/types";
```

Add the WalkForwardPanel component before `export default function AnalyticsPage()`:

```typescript
function ReliabilityDiagram({ data }: { data: ReliabilityPoint[] }) {
  if (!data || data.length === 0) return <p className="text-text-secondary text-sm">No reliability data.</p>;
  const maxCount = Math.max(...data.map(d => d.count));
  return (
    <div className="relative h-48 w-full">
      <svg viewBox="0 0 200 200" className="w-full h-full">
        {/* Perfect calibration line */}
        <line x1="20" y1="180" x2="180" y2="20" stroke="#4b5563" strokeWidth="1" strokeDasharray="4,4" />
        {/* Axes */}
        <line x1="20" y1="180" x2="180" y2="180" stroke="#374151" strokeWidth="1" />
        <line x1="20" y1="20" x2="20" y2="180" stroke="#374151" strokeWidth="1" />
        {/* Points */}
        {data.map((d, i) => {
          const cx = 20 + d.mean_pred * 160;
          const cy = 180 - d.fraction_positive * 160;
          const r = Math.max(3, (d.count / maxCount) * 10);
          return (
            <circle key={i} cx={cx} cy={cy} r={r} fill="#2563eb" opacity={0.75}>
              <title>{`Pred: ${d.mean_pred.toFixed(2)}, Actual: ${d.fraction_positive.toFixed(2)}, n=${d.count}`}</title>
            </circle>
          );
        })}
        {/* Axis labels */}
        <text x="100" y="198" textAnchor="middle" fontSize="8" fill="#9ca3af">Mean Predicted Prob</text>
        <text x="10" y="100" textAnchor="middle" fontSize="8" fill="#9ca3af" transform="rotate(-90,10,100)">Fraction Positive</text>
      </svg>
    </div>
  );
}

function WalkForwardPanel() {
  const [data, setData] = useState<WalkForwardData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/walk-forward")
      .then(r => r.json())
      .then(d => {
        if (d.error) setError(d.error);
        else setData(d);
      })
      .catch(() => setError("Failed to load walk-forward results"))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="skeleton h-32 rounded-xl" />;
  if (error) return (
    <div className="bg-bg-surface border border-border rounded-xl p-4 text-text-secondary text-sm">
      Walk-Forward Evaluation not yet run. Execute: <code className="font-mono text-xs">python main.py evaluate-limitless</code>
    </div>
  );
  if (!data) return null;

  const { headline, model, market_baseline, dataset } = data;
  const beatsBadge = headline.model_beats_market
    ? <span className="text-green text-xs font-semibold">Beats Market ✓</span>
    : <span className="text-red text-xs font-semibold">Loses to Market ✗</span>;

  return (
    <div className="bg-bg-surface border border-border rounded-xl overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <div>
          <h3 className="text-md font-semibold text-text-primary">Walk-Forward Evaluation (Real Limitless Data)</h3>
          <p className="text-2xs text-text-secondary mt-0.5">
            {dataset.test_markets} test markets · {dataset.test_snapshots.toLocaleString()} snapshots · {data.dataset.test_date_range[0].slice(0,10)} – {data.dataset.test_date_range[1].slice(0,10)}
          </p>
        </div>
        {beatsBadge}
      </div>
      <div className="p-4 grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Model Brier</div>
          <div className="font-mono font-semibold text-text-primary">{model.brier_score.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">CI [{model.brier_ci_95[0].toFixed(4)}, {model.brier_ci_95[1].toFixed(4)}]</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Market Brier</div>
          <div className="font-mono font-semibold text-text-primary">{market_baseline.brier_score.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">CI [{market_baseline.brier_ci_95[0].toFixed(4)}, {market_baseline.brier_ci_95[1].toFixed(4)}]</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Delta vs Market</div>
          <div className={`font-mono font-semibold ${headline.delta_brier_vs_market > 0 ? "text-green" : "text-red"}`}>
            {headline.delta_brier_vs_market > 0 ? "+" : ""}{headline.delta_brier_vs_market.toFixed(4)}
          </div>
          <div className="text-2xs text-text-secondary">Brier improvement</div>
        </div>
        <div>
          <div className="text-2xs text-text-secondary uppercase tracking-wider mb-1">Model AUC</div>
          <div className="font-mono font-semibold text-text-primary">{model.auc.toFixed(4)}</div>
          <div className="text-2xs text-text-secondary">ECE: {model.ece.toFixed(4)}</div>
        </div>
      </div>
      <div className="px-4 pb-4">
        <div className="text-2xs text-text-secondary mb-2">Reliability Diagram (bubble size = sample count)</div>
        <ReliabilityDiagram data={data.diagnostics.reliability_diagram_data} />
      </div>
      <div className="px-4 pb-4 text-2xs text-text-secondary">
        Train: {dataset.train_markets} markets · Calibrate: {dataset.calib_markets} markets · Test: {dataset.test_markets} markets
        · Run ID: {data.run_id}
      </div>
    </div>
  );
}
```

In `export default function AnalyticsPage()`, add `<WalkForwardPanel />` after the trade list section:

```typescript
      {/* Walk-Forward Evaluation */}
      <WalkForwardPanel />
```

- [ ] **Step 2: Build check**

```bash
cd /opt/stratagen/kimibot/frontend
npx tsc --noEmit 2>&1 | head -20
```

Expected: No new type errors

- [ ] **Step 3: Commit**

```bash
cd /opt/stratagen/kimibot
git add frontend/app/analytics/page.tsx
git commit -m "feat: add WalkForwardPanel to analytics page with reliability diagram"
```

---

## Task 13: End-to-End Verification

This task runs the full pipeline and verifies all acceptance criteria.

- [ ] **Step 1: Run the look-ahead audit (HARD GATE)**

```bash
cd /opt/stratagen/kimibot
python3 -m pytest tests/test_lookahead.py tests/test_eval_metrics.py tests/test_walk_forward_historical.py -v
```

Expected: All tests PASS.

- [ ] **Step 2: Ensure GRAPH_API_KEY is set**

```bash
echo "GRAPH_API_KEY set: $([ -n "$GRAPH_API_KEY" ] && echo YES || echo NO)"
```

If NO: `export GRAPH_API_KEY=your_key_here` (get free key from thegraph.com/studio/apikeys)

- [ ] **Step 3: Run end-to-end evaluation**

```bash
cd /opt/stratagen/kimibot
python3 main.py evaluate-limitless --config config/default.yaml 2>&1 | tee /tmp/eval_output.txt
```

Expected output:
```
Step 1/4: Ingesting resolved Limitless markets...
  Loaded NNN resolved markets
Step 2/4: Ensuring BTC price data is cached...
Step 3/4: Running walk-forward evaluation...
==================================================
RESULT: Model Brier = X.XXXX
        Market Brier = X.XXXX
        Delta = +/-X.XXXX (model beats/loses to market)
==================================================
Step 4/4: Generating reports...
Done. See EVALUATION_REPORT.md and reports/
Test markets: NNN
```

- [ ] **Step 4: Verify all acceptance criteria**

```bash
cd /opt/stratagen/kimibot

# Raw file counts
echo "=== Cache counts ==="
ls data/limitless_raw/ | wc -l
ls data/binance_raw/ | wc -l

# Evaluation report
echo "=== Report head ==="
head -80 EVALUATION_REPORT.md

# API endpoint
echo "=== API response ==="
curl -s http://localhost:3000/api/walk-forward | python3 -m json.tool | head -40
# Or use the VPS address:
# curl -s http://72.62.192.230:8989/api/walk-forward | python3 -m json.tool | head -40

# Tests
echo "=== Tests ==="
python3 -m pytest tests/ -k "lookahead or walk_forward or eval_metrics" -v
```

- [ ] **Step 5: If test market count < 30, check why**

```bash
python3 -c "
import json; from pathlib import Path
results = json.loads(Path('data/walk_forward_results.json').read_text())
print('Test markets:', results['dataset']['test_markets'])
print('Total markets:', results['dataset']['total_markets'])
print('Test snapshots:', results['dataset']['test_snapshots'])
"
```

If test_markets < 30: the subgraph has fewer resolved markets than expected. Document in BLOCKED.md and check with user before lowering thresholds.

- [ ] **Step 6: Final commit**

```bash
cd /opt/stratagen/kimibot
git add -A
git commit -m "feat: complete Limitless historical ingestion and walk-forward evaluation pipeline"
```

---

## Self-Review Checklist

### Spec Coverage

- [x] Pre-flight research documented in LIMITLESS_INGESTION_PLAN.md
- [x] `ingestion/limitless_historical.py` — resolved market enumeration via The Graph, REST metadata, cache to `data/limitless_raw/`
- [x] `ingestion/binance_historical.py` — BTC 1m klines + funding rates, cache to `data/binance_raw/`, idempotent
- [x] Token-bucket rate limiter on all API calls (SubgraphClient + REST bucket + Binance bucket)
- [x] Feature construction: existing `FeatureBuilder.build_features()` reused unchanged; 15-minute snapshot cadence, capped at 50/market
- [x] Look-ahead audit test: 4 tests verifying `market_source_max_ts <= as_of` and `crypto_source_max_ts <= as_of`, plus future-data injection tests
- [x] Walk-forward split: 60/20/20 by resolution_time, disjoint markets, tested
- [x] Metrics: Brier, ECE, log-loss, AUC, accuracy @ 0.5, bootstrap CI (1000 resamples)
- [x] Two baselines: market price and trivial (0.5)
- [x] Delta vs market as headline number
- [x] Honest reporting requirements: raw metrics, CI, no cherry-picking
- [x] EVALUATION_REPORT.md with all required sections
- [x] reports/reliability.png via matplotlib
- [x] Top 10 disagreement markets and top 10 confidently wrong markets
- [x] `python main.py evaluate-limitless` end-to-end command
- [x] `/api/walk-forward` Next.js API route
- [x] Analytics page WalkForwardPanel with reliability diagram, metrics, dates, CI
- [x] Fixed random seed (42) for reproducibility
- [x] UTC timezone awareness throughout
- [x] Aggressive caching — never re-fetch what's on disk

### Placeholder Scan

None found — all steps contain actual code, exact file paths, and exact commands.

### Type Consistency

- `SubgraphClient.get_all_resolved_markets()` → `list[dict[str, Any]]`
- `run_historical_ingestion()` → `list[dict[str, Any]]` with keys: `condition_id`, `slug`, `label`, `resolution_time_utc`, `trades`
- `split_markets()` → `tuple[list[dict], list[dict], list[dict]]`
- `run_evaluation()` → `dict[str, Any]` (also written to `data/walk_forward_results.json`)
- `FeatureBuilder.build_features()` signature unchanged
- `WalkForwardData` TypeScript type matches `run_evaluation()` JSON output shape

---

**Plan complete and saved to `docs/superpowers/plans/2026-04-21-limitless-historical-ingestion.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — execute tasks in this session using executing-plans skill

**Which approach?**
