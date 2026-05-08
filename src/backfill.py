# Disable tqdm progress bars BEFORE any imports
import os
os.environ["TQDM_DISABLE"] = "1"

import sys
import time
import warnings

# Suppress akshare's date format warnings
warnings.filterwarnings("ignore", message="Could not infer format")

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.jobs.raw_push import push_raw_payload
from src.jobs.schedulers import RawJobScheduler


def main() -> None:
    """
    主动执行 Pragmatic ELT 全量历史回填。

    Args:
        无。
    Returns:
        None；执行一次后退出。

    Created: 2026-05
    易错点: 本入口会全量拉取产品 daily history，适合初始化/删表重建/历史修复，不应作为常驻任务运行。
    """
    start_time = time.time()
    print("Starting Pragmatic ELT Backfill...")
    payload = RawJobScheduler.fetch_backfill_raw()

    realtime_count = len(payload.get("realtime", []))
    meta_count = len(payload.get("meta", []))
    history_count = len(payload.get("history", []))
    print(f"[RawJob] Prepared realtime={realtime_count}, meta={meta_count}, history={history_count}")

    push_raw_payload(payload)

    elapsed = time.time() - start_time
    print(f"[Done] Backfill took {elapsed:.2f}s.")


if __name__ == "__main__":
    main()
