"""Explicit ETF history initialization/repair entry point; never invokes quote ingestion."""

from src.jobs.etf_history import ETFHistoryJob, ETFHistoryPolicy


def main() -> None:
    """Run one complete source-history recovery using the existing HTTP config and ingest clients.

    Args:
        None; ETF_HISTORY_* variables configure source range, concurrency and deadlines.
    Returns:
        None on success; exits nonzero if any selected source or HTTP ingest fails.

    Created: 2026-10
    易错点: This explicit repair works when the background flag is off. It needs only HTTP
        backend access, not DB/Redis credentials, and does not claim unavailable history exists.
    """
    counts = ETFHistoryJob(ETFHistoryPolicy.from_environment()).run_due(force=True)
    print(f"[ETFHistory] complete history repair: {counts}")
    if counts["failed"]:
        raise SystemExit(1)
    if counts["attempted"] == 0:
        print("[ETFHistory] no verified eligible ETF config; no raw data was sent")


if __name__ == "__main__":
    main()
