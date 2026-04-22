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
