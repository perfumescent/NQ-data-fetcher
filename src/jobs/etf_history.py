"""Independent low-frequency ETF history recovery; only source tables cross HTTP ingest."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
import threading
from typing import Callable
from zoneinfo import ZoneInfo

from src.providers.api_server_client import APIServerClient
from src.providers.etf_history import DATASETS, HistorySourceResult, fetch_source_table


@dataclass(frozen=True)
class ETFHistoryPolicy:
    """Source retention/reconciliation policy, independent from report windows and UI point counts."""

    interval_seconds: int = 21600
    overlap_days: int = 14
    reconcile_days: int = 7
    source_start_date: str = "19700101"
    source_timeout_seconds: int = 180
    workers: int = 3

    @classmethod
    def from_environment(cls) -> "ETFHistoryPolicy":
        """Load and validate the explicit low-frequency source acquisition policy.

        Args:
            cls: ETFHistoryPolicy type; values come from ETF_HISTORY_* environment variables.
        Returns:
            Validated immutable policy; invalid values raise ValueError before any network call.

        Created: 2026-10
        易错点: A minimum hourly interval keeps history collection independent of minute quotes.
        """
        policy = cls(
            interval_seconds=int(os.getenv("ETF_HISTORY_INTERVAL_SECONDS", "21600")),
            overlap_days=int(os.getenv("ETF_HISTORY_OVERLAP_DAYS", "14")),
            reconcile_days=int(os.getenv("ETF_HISTORY_RECONCILE_DAYS", "7")),
            source_start_date=os.getenv("ETF_HISTORY_SOURCE_START_DATE", "19700101"),
            source_timeout_seconds=int(os.getenv("ETF_HISTORY_SOURCE_TIMEOUT_SECONDS", "180")),
            workers=int(os.getenv("ETF_HISTORY_WORKERS", "3")),
        )
        policy.validate()
        return policy

    def validate(self) -> None:
        """Reject invalid source bounds or an unsafe collection frequency.

        Args:
            self: Immutable policy with source start and scheduling limits.
        Returns:
            None; raises ValueError for invalid dates, subhour intervals, or nonpositive limits.

        Created: 2026-10
        易错点: These are acquisition limits, not a 52-week/UI trim. Startup and periodic
            reconciliation request the full configured source retention range.
        """
        datetime.strptime(self.source_start_date, "%Y%m%d")
        if self.interval_seconds < 3600 or self.overlap_days < 2 or self.reconcile_days < 1:
            raise ValueError("ETF history needs hourly-or-slower jobs, >=2 day overlap and reconciliation")
        if self.source_timeout_seconds <= 0 or not 1 <= self.workers <= 5:
            raise ValueError("ETF source timeout must be positive and workers must be 1...5")


def eligible_etfs(config: dict) -> list[dict]:
    """Select verified existing ETF configuration entries without creating a second fund universe.

    Args:
        config: Existing fund-configs response; etf may be absent or malformed.
    Returns:
        Original ETF configuration dictionaries with strict premiumEligible=True and unique codes.

    Created: 2026-10
    易错点: Names never classify benchmarks. LOFs/unverified products remain in the old config
        and comparison UI but are excluded from this new source acquisition path.
    """
    selected = {}
    for item in config.get("etf", []) if isinstance(config, dict) else []:
        if not isinstance(item, dict) or item.get("premiumEligible") is not True:
            continue
        code = str(item.get("code", ""))
        if len(code) == 6 and code.isdigit():
            selected[code] = item
    return list(selected.values())


def source_history_item(table: dict, collection_date: str) -> dict:
    """Wrap one complete source table in the existing history HTTP envelope.

    Args:
        table: Successful eastmoney-fund-history-v1 SDK source table; collection_date: Shanghai
            collection day YYYY-MM-DD, not a claimed price/NAV valuation or publication date.
    Returns:
        History item for internal/raw/history; Server ingest owns source-date row partitioning.

    Created: 2026-10
    易错点: Never bucket/filter rows in Fetcher. Invalid dates and successful null-valued rows
        must reach Server intact. Unknown upstream update time stays None.
    """
    return {
        "symbol": table["symbol"], "assetType": "etf", "date": collection_date,
        "dataDate": collection_date, "updatedAt": None,
        "rawPayload": {"histories": [table], "_sources": {
            "histories": {"provider": table["provider"], "sourceLayer": table["sourceLayer"]}
        }},
    }


def fetch_backfill_items(item: dict, policy: ETFHistoryPolicy | None = None) -> list[dict]:
    """Fetch complete available price and unit-NAV history for an explicitly eligible ETF.

    Args:
        item: Existing ETF config with strict premiumEligible=True; policy: source retention and
            deadline policy, or defaults. No server credentials/database handles are accepted.
    Returns:
        Zero to two complete source-table HTTP items; each dataset failure is logged independently.

    Created: 2026-10
    易错点: Failed prices never use NAV as fallback, and a successful dataset survives the other's failure.
    """
    selected = eligible_etfs({"etf": [item]})
    if not selected:
        return []
    active_policy = policy or ETFHistoryPolicy.from_environment()
    active_policy.validate()
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    items = []
    for dataset in DATASETS:
        result = fetch_source_table(str(item["code"]), dataset, active_policy.source_start_date,
                                    now.strftime("%Y%m%d"),
                                    timeout_seconds=active_policy.source_timeout_seconds)
        if result.table is not None:
            items.append(source_history_item(result.table, now.date().isoformat()))
        else:
            print(f"[ETFHistory] {result.symbol} {dataset} unavailable: {result.error}")
    return items


class ETFHistoryJob:
    """Memory tracks only scheduling; restarting always reconstructs full source history."""

    def __init__(
        self,
        policy: ETFHistoryPolicy,
        *,
        load_config: Callable = APIServerClient.get_fund_configs,
        fetch_source: Callable = fetch_source_table,
        push_history: Callable = APIServerClient.push_raw_history,
    ) -> None:
        """Construct an isolated recovery job with injectable source and HTTP boundaries.

        Args:
            policy: Valid acquisition policy; load_config: existing HTTP config loader;
            fetch_source: complete SDK table adapter; push_history: existing HTTP ingest client.
        Returns:
            None; no network calls occur until run_due/start is invoked.

        Created: 2026-10
        易错点: No persisted watermark is required for recovery. In-memory success markers are
            acknowledged only after HTTP success and disappear safely on restart.
        """
        policy.validate()
        self.policy = policy
        self.load_config = load_config
        self.fetch_source = fetch_source
        self.push_history = push_history
        self._full_success: dict[tuple[str, str], datetime] = {}
        self._next_due: datetime | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def run_due(self, now: datetime | None = None, *, force: bool = False) -> dict:
        """Recover startup gaps or refresh overlapping source history when due.

        Args:
            now: Offset-aware scheduling clock, or UTC now; force: explicit one-shot backfill
                bypassing the scheduled due time. A naive clock raises ValueError.
        Returns:
            Counts {attempted, ingested, failed}; all zero means no eligible work, not due, or already running.

        Created: 2026-10
        易错点: Every unseen/failed full source dataset stays in recovery mode. A short overlap
            never acknowledges missing older history; restart always requests full history again.
        """
        current = now or datetime.now(timezone.utc)
        if current.utcoffset() is None:
            raise ValueError("History scheduling clock must include an offset")
        counts = {"attempted": 0, "ingested": 0, "failed": 0}
        if not force and self._next_due is not None and current < self._next_due:
            return counts
        if not self._lock.acquire(blocking=False):
            return counts
        try:
            today = current.astimezone(ZoneInfo("Asia/Shanghai")).date()
            requests = []
            for item in eligible_etfs(self.load_config()):
                for dataset in DATASETS:
                    key = (str(item["code"]), dataset)
                    full_at = self._full_success.get(key)
                    full = force or full_at is None or current - full_at >= timedelta(days=self.policy.reconcile_days)
                    start = self.policy.source_start_date if full else (
                        today - timedelta(days=self.policy.overlap_days)
                    ).strftime("%Y%m%d")
                    requests.append((key, start, full))
            with ThreadPoolExecutor(max_workers=self.policy.workers) as executor:
                futures = {
                    executor.submit(self.fetch_source, key[0], key[1], start,
                                    today.strftime("%Y%m%d"),
                                    timeout_seconds=self.policy.source_timeout_seconds): (key, full)
                    for key, start, full in requests
                }
                for future in as_completed(futures):
                    key, full = futures[future]
                    counts["attempted"] += 1
                    try:
                        result: HistorySourceResult = future.result()
                        if result.table is None:
                            raise RuntimeError(result.error or "source_unavailable")
                        pushed = self.push_history({"items": [source_history_item(result.table, today.isoformat())]})
                        if not pushed:
                            raise RuntimeError("http_ingest_failed")
                        counts["ingested"] += 1
                        if full:
                            self._full_success[key] = current
                    except Exception as exc:
                        counts["failed"] += 1
                        print(f"[ETFHistory] {key[0]} {key[1]} incomplete: {type(exc).__name__}")
            delay = min(self.policy.interval_seconds, 3600) if counts["failed"] else self.policy.interval_seconds
            self._next_due = current + timedelta(seconds=delay)
            return counts
        finally:
            self._lock.release()

    def start(self) -> threading.Thread:
        """Start the background history worker alongside the minute quote loop.

        Args:
            self: History job instance; no external arguments.
        Returns:
            Daemon Thread that begins with full recovery.

        Created: 2026-10
        易错点: The background job must never call fetch_incremental_raw or reuse its cycle
            cache. Source subprocess deadlines prevent permanent historical-source stalls.
        """
        thread = threading.Thread(target=self._run, name="etf-source-history", daemon=True)
        thread.start()
        return thread

    def _run(self) -> None:
        """Drive the independent due checks and contain source/config errors.

        Args:
            self: Started history job; stop event is owned by this process.
        Returns:
            None when stopped. Failed cycles retry after a minute due-check without source floods.

        Created: 2026-10
        易错点: Logs contain only operation/code/error categories, never raw responses or credentials.
        """
        while not self._stop.is_set():
            try:
                counts = self.run_due()
                if counts["attempted"]:
                    print(f"[ETFHistory] cycle {counts}")
            except Exception as exc:
                print(f"[ETFHistory] cycle unavailable: {type(exc).__name__}")
            self._stop.wait(60)
