# NQ-Data Data Fetcher

Local data fetching service for NQ-Signal.

The minute `src.main` loop keeps the existing realtime/meta collection. The new
ETF historical-source path is independent: it collects complete source tables,
then sends them only through HTTP to `api_server` internal raw ingest. It does
not use database/Redis connections or credentials, compute premiums/ranks,
clean prices, discard missing rows, or partition rows into business dates.

## ETF history initialization and recovery

Deploy the compatible Server source-table ingest first, then run:

```bash
uv run python -m src.etf_history
```

This explicit command requests all source-available history for verified ETF
entries in the existing `etf` fund-configs array. It does not fetch quotes or
other assets. Each price/NAV dataset is fetched and posted independently;
failed/empty sources produce no clearing update. It exits nonzero on partial
source or HTTP failure so an operator can retry. Repeated runs are idempotent
at Server ingest. `src.backfill` also routes the verified ETFs to this complete
source-table collector while preserving its existing other-asset behavior.

The minute service also starts a separate background ETF history worker.

```bash
uv run python -m src.main
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ETF_HISTORY_INTERVAL_SECONDS` | `21600` | Six-hour source check; minimum one hour |
| `ETF_HISTORY_OVERLAP_DAYS` | `14` | Inclusive recent request overlap for late NAV/revisions |
| `ETF_HISTORY_RECONCILE_DAYS` | `7` | Full retained source history reconciliation interval |
| `ETF_HISTORY_SOURCE_START_DATE` | `19700101` | Earliest requested source range, YYYYMMDD |
| `ETF_HISTORY_SOURCE_TIMEOUT_SECONDS` | `180` | Per-source collection deadline |
| `ETF_HISTORY_INGEST_TIMEOUT_SECONDS` | `180` | Complete-table HTTP ingest timeout, finite positive seconds; legacy stays 20 |
| `ETF_HISTORY_WORKERS` | `3` | Concurrent independent source datasets, 1–5 |

Startup always requests the full retained source range. Recovery therefore does
not depend on a local last-success file or remembering which collection day was
missed. A successful full source fetch is acknowledged only after successful
HTTP ingest; failed datasets keep full recovery pending and retry hourly.
Ordinary successful checks use the overlap window; full reconciliation catches
older official revisions. A process restart resets acknowledgements and safely
replays full source history. The minute quote cycle never triggers these full
history requests and is not blocked by the historical-source worker.

The default start predates the configured products' listings. It requests the
available source range, not a guaranteed archive back to 1970. Changing this
bound intentionally changes retention/recovery scope and must be documented by
the operator. It is unrelated to a 52-week display/percentile window.

## Source layers and retention limits

* Price: `akshare.fund_etf_hist_em(..., period="daily", adjust="")` complete
  unadjusted SDK output, `eastmoney-fund-history-v1`. If that price source is
  unavailable, complete `akshare.fund_etf_hist_sina(symbol=...)` price SDK output
  uses `sina-fund-history-v1`. Sina accepts no date-range parameters, so even a
  low-frequency overlap run must request its available full response and keep
  every returned row/column; Fetcher never trims it to the report window.
* Unit NAV: Eastmoney `https://api.fund.eastmoney.com/f10/lsjz`, complete paginated
  JSON records with `FSRQ`/`DWJZ`/`LJJZ` and all other/future fields. It uses
  `eastmoney-fund-history-v1`, `dataset=unit-nav-history`, and accurate
  `sourceLayer=upstream-json-records`. This avoids AKShare 1.17.95's observed
  positional 13-column failure after the source added a 14th field. Requests,
  per-page envelope metadata and source row positions remain in raw.

Both use `rawPayload.histories` with `columns/index/data`, `requestParams`,
`fetchedAt`, provider, dataset, symbol, interval, currency, share unit and source
time zone. SDK tables are explicitly labelled `sourceLayer=sdk-table`: fields
already discarded by the SDK are not claimed to be raw upstream HTTP. HTTP NAV
means parsed JSON records, not a byte-exact HTTP archive. Only JSON-required
conversion is applied; NaN/Infinity become null while rows and columns remain.

The history envelope's `date`/`dataDate` is the actual Shanghai collection day.
Server ingest owns valuation-date grouping and unresolved-date preservation.
`fetchedAt` is collection metadata; unknown upstream update time remains null.
There is no price-to-NAV fallback. Legacy non-report fund/LOF consumers retain
their old price format; the new verified ETF path never calls that simplified
history adapter. All existing fund-list entries remain available to comparison.

Current public sources can revise, limit, remove, fail or stop exposing old
history. The collector can reconstruct missing collection days only while the
upstream history still exists. Back up and restore the Server's MySQL raw table
with the existing database operations procedure; Fetcher state is not a raw
backup. After database restore or recreation, run the explicit repair command.
Never fill unavailable records with zero or promise unconditional recovery.

## Verified existing configuration

`funds.json` retains the existing ETF/fund lists and adds verified ETF metadata
through `premiumEligible`, `premiumBenchmark`, `listingDate`, and official
`premiumMetadataSource` references. Server DB config still overrides local
non-null fields under the existing migration fallback. Classification is never
inferred from a display name. `161128` remains in the old comparison list and
is ineligible; `159509` belongs to the separate `nasdaq-technology` benchmark;
the other 10 verified entries track `nasdaq100`.

`premiumValuationCalendar=CN` is an explicit **conservative pairing-completeness
requirement**, independently supported by each product's official Shanghai or
Shenzhen listing and prospectus workday definition. Every confirmed domestic
exchange trading day must have its same-date close and official unit NAV before
Server can call the 52-week window complete. A U.S. holiday never removes a
domestic trading day. This does not claim that each product's statutory valuation
days equal CN or that its subscription/open days equal CN; some prospectuses use
joint-market open days or related-market valuation definitions. Any additional
CN day required by this stricter rule can only create a missing-data state, not
false completeness. Non-CN trading days cannot supply this instrument's domestic
exchange close. Missing/unverified calendar configuration remains unknown.

Read-only probes on 2026-10-23 obtained all 11 price histories (Sina SDK,
719–1445 rows, through 2026-10-22) and all 11 NAV histories (Eastmoney JSON,
724–1452 rows, through valuation day 2026-10-21). Observed first price dates
match the official listing references. Successful source retrieval alone does
not establish complete 52-week coverage: Server checks independent calendars,
pair validity, dates, corporate actions and share units before publishing a
percentile. No probe writes to production ingest.

## Nasdaq-100 constituent snapshot

The regular poll fetches the Nasdaq constituent table at most once every five
minutes. The complete decoded JSON response is sent as rawPayload.nasdaqResponse
under ^NDX:CONSTITUENTS. The server validates the snapshot before replacing the
last valid one and derives the display values.

## Offline checks

```bash
uv run python -m unittest discover -s tests -v
```

Tests cover complete field/row/offset serialization, unknown source columns,
HTTP pagination completeness, independent source failures, no NAV-as-price
fallback, low-frequency scheduling, startup recovery after missed collection
days, HTTP acknowledgement, partial retry and chunked HTTP transport. They do
not require production API credentials or external network access.
