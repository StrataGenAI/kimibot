# `data/resolved_markets.parquet` — schema contract

This is the append-only store of crypto markets the resolution scanner
has observed in their post-resolution state. The live ingest service
captures slugs while markets are active; the scanner follows up once
`expiration_timestamp` has passed and writes one row per resolved market
here. Rows are never overwritten in place.

## Columns

| Column                    | Type                | Nullable | Source (in `/markets/{slug}` payload) |
|---------------------------|---------------------|----------|---------------------------------------|
| `market_id`               | `str`               | no       | `id` (Limitless numeric, stringified) |
| `slug`                    | `str`               | no       | `slug`                                |
| `condition_id`            | `str`               | no       | `conditionId`                         |
| `category_tags`           | `list[str]`         | no       | `categories` ∪ `tags` (deduplicated, preserves first-seen order) |
| `expiration_timestamp`    | `datetime64[UTC]`   | no       | `expirationTimestamp` (ms) → UTC      |
| `resolved_at`             | `datetime64[UTC]`   | no       | wall clock when the scanner captured the row |
| `winning_outcome_index`   | `int64`             | no       | `winningOutcomeIndex` (0 = YES, 1 = NO, `-1` = invalid/refunded) |
| `final_yes_price`         | `float64`           | no       | `prices[0]`                           |
| `final_no_price`          | `float64`           | no       | `prices[1]`                           |
| `volume_total`            | `float64`           | no       | `volumeFormatted` if present else `volume`; `0.0` fallback |
| `liquidity_at_resolution` | `float64`           | no       | `liquidity` / `liquidity24h` / `open_interest`; `0.0` if absent (REST payload for resolved markets rarely carries a live liquidity snapshot — see note below) |
| `first_seen`              | `datetime64[UTC]`   | no       | joined from the live-ingest sidecar (`first_seen` column) |
| `capture_method`          | `str`               | no       | hard-coded tag identifying the writer (e.g. `scanner_v1`) |

## Rationale

- **`market_id`** — primary join key for the live ingest sidecar and
  downstream feature builders that partition by `market_id`.
- **`slug`** — human-readable, stable for the life of the market. The
  only value the REST API accepts for re-lookup.
- **`condition_id`** — lets us cross-reference the Graph subgraph if
  we ever need on-chain trade data; stored even though we don't use it
  at write time.
- **`category_tags`** — replaces the slug-regex category heuristic with
  the platform's native `categories`/`tags`. Merged deduplicated
  because Limitless splits information across both fields (e.g.
  `categories = ["Crypto","15 min","BTC"]`, `tags = ["Lumy","Recurring","Minutely"]`).
- **`expiration_timestamp`** — the moment the market was decided. Used
  as the temporal anchor for walk-forward splits.
- **`resolved_at`** — when we captured the row. Gap between
  `expiration_timestamp` and `resolved_at` reveals platform-side
  settlement lag; we record it rather than suppressing it.
- **`winning_outcome_index`** — integer encoding keeps downstream
  label-building trivial. `-1` reserves a slot for invalid/refunded
  markets so consumers can filter without null-checks.
- **`final_yes_price` / `final_no_price`** — resolved markets report
  `prices: [1, 0]` or `[0, 1]` canonically. Stored as-is so we can
  validate the outcome encoding later.
- **`volume_total`** — cumulative volume over the market's lifetime.
  `volumeFormatted` is the human-scaled float; we prefer it when
  present.
- **`liquidity_at_resolution`** — nullable-as-zero with explicit doc
  note because Limitless's current REST payload for resolved markets
  does not consistently expose a live liquidity snapshot. Kept as a
  column so the schema doesn't have to change when the upstream API
  starts returning it. A value of `0.0` means "not present in API
  response", not "actually zero".
- **`first_seen`** — joined from the live-ingest sidecar. Tells us how
  long we had visibility on the market, useful for filtering out
  markets we only caught near resolution.
- **`capture_method`** — version tag for the writer. Lets us audit the
  table if we ever change extraction logic.

## Invariants

1. `expiration_timestamp <= resolved_at` always.
2. `winning_outcome_index ∈ {-1, 0, 1}`.
3. `(market_id)` is unique across the table — the scanner filters out
   already-captured market_ids before writing.
4. `capture_method` is never empty.
5. Rows are sorted by `expiration_timestamp` ascending on disk.

## File-level guarantees

- Writes are atomic: the scanner writes to
  `data/resolved_markets.parquet.tmp_<pid>_<unix_ns>` and then
  `os.replace`s onto the final path. A crash or reboot mid-write leaves
  the existing file untouched.
- The file is append-only from the outside: each run reads the current
  file, concatenates new rows, sorts, and rewrites. Existing rows are
  never mutated — only new rows are added.
