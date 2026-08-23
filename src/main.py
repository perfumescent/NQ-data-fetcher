# Disable tqdm progress bars BEFORE any imports
import os
os.environ['TQDM_DISABLE'] = '1'

import time
import sys
import warnings

# Suppress akshare's date format warnings
warnings.filterwarnings('ignore', message='Could not infer format')

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.jobs.schedulers import RawJobScheduler
from src.jobs.raw_push import push_raw_payload

INTERVAL_SECONDS = 60

def main():
    """
    启动 Pragmatic ELT 增量 data_fetcher 常驻循环。

    Args:
        无。
    Returns:
        None；进程常驻，每轮只采 realtime/meta 增量数据。

    Created: 2026-05
    易错点: 本入口永远常驻运行，严禁加参数分支；初始化或修复历史请运行 src.backfill。
    """
    print("Starting Pragmatic ELT Incremental Pusher...")

    while True:
        try:
            start_time = time.time()

            print("\n[RawJob] Starting incremental fetch cycle...")
            payload = RawJobScheduler.fetch_incremental_raw()

            realtime_count = len(payload.get("realtime", []))
            meta_count = len(payload.get("meta", []))
            history_count = len(payload.get("history", []))
            print(f"[RawJob] Prepared realtime={realtime_count}, meta={meta_count}, history={history_count}")

            push_raw_payload(payload)

            elapsed = time.time() - start_time
            print(f"[Done] Cycle took {elapsed:.2f}s.")

        except Exception as e:
            print(f"[Critial Error] {e}")
            import traceback
            traceback.print_exc()

        print(f"Sleeping for {INTERVAL_SECONDS}s...")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
