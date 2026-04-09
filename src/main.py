# Disable tqdm progress bars BEFORE any imports
import os
os.environ['TQDM_DISABLE'] = '1'

import time
import requests
import json
import sys
import warnings

# Suppress akshare's date format warnings
warnings.filterwarnings('ignore', message='Could not infer format')

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.jobs.schedulers import JobScheduler
from src.providers.api_server_client import APIServerClient
from src.core.cache import fund_cache, fund_history_cache

# Configuration
REMOTE_API_URL = os.getenv("REMOTE_API_URL", "http://localhost:8000/v1/internal/ingest")
INTERVAL_SECONDS = 60 # 1 minute refresh

def main():
    print(f"Starting Data Pusher...")
    print(f"Target: {REMOTE_API_URL}")

    if "<" in REMOTE_API_URL or ">" in REMOTE_API_URL:
        print(f"[Error] Invalid URL configuration: {REMOTE_API_URL}")
        print("Please set REMOTE_API_URL to a valid address (e.g. http://localhost:8000/v1/internal/ingest)")
        return

    while True:
        try:
            start_time = time.time()

            # 清空场外基金相关缓存，确保每轮都从源头重新拉取
            fund_cache.clear()
            fund_history_cache.clear()

            # 1. Fetch All Data
            print("\n[Job] Starting Validated Fetch Cycle...")
            data = JobScheduler.fetch_all_jobs()

            # 2. Serialize
            payload = data.model_dump(mode='json')

            # 3. Push realtime data (snapshot/intraday/indicators → memory cache)
            print(f"[Push] Sending {len(str(payload))} bytes to cloud...")

            import certifi
            # Temporary workaround for SSL chain issues
            import urllib3
            urllib3.disable_warnings()
            resp = requests.post(REMOTE_API_URL, json=payload, timeout=10, verify=False)

            if resp.status_code == 200:
                print(f"[Success] Data Ingested. Server says: {resp.text}")
            else:
                print(f"[Error] Server returned {resp.status_code}: {resp.text}")

            # 4. Fund list data: check if today's data exists, push if not
            _try_push_fund_list_data(data)

            elapsed = time.time() - start_time
            print(f"[Done] Cycle took {elapsed:.2f}s.")

        except requests.exceptions.ConnectionError as e:
            print(f"[Error] Could not connect to {REMOTE_API_URL}. Is the server running?")
            print(f"[Details] {e}")
        except Exception as e:
            print(f"[Critial Error] {e}")
            import traceback
            traceback.print_exc()

        print(f"Sleeping for {INTERVAL_SECONDS}s...")
        time.sleep(INTERVAL_SECONDS)


def _try_push_fund_list_data(data):
    """
    每个主循环周期都将基金列表数据 UPSERT 到 DB。
    api_server 侧使用 ON DUPLICATE KEY UPDATE，多次推送幂等安全。
    失败不影响主循环。
    """
    try:
        from datetime import datetime, timezone, timedelta
        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

        payload = JobScheduler.extract_fund_list_data(data, today)

        etf_count = len(payload.get("etf", []))
        fund_count = len(payload.get("fund", []))
        print(f"[FundData] Pushing {etf_count} ETFs + {fund_count} Funds for {today}")
        for item in payload.get("fund", []):
            print(f"[FundData]   {item['code']} {item.get('name', '')} quota={item.get('quota')!r}")

        ok = APIServerClient.push_fund_data(payload)
        if not ok:
            print(f"[FundData] Push failed, will retry next cycle")
    except Exception as e:
        print(f"[FundData] Error in fund list push (non-fatal): {e}")


if __name__ == "__main__":
    main()
