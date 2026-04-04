# Disable tqdm progress bars BEFORE any imports
import os
os.environ['TQDM_DISABLE'] = '1'

import time
import requests
import json
import sys
import warnings
from datetime import date

# Suppress akshare's date format warnings
warnings.filterwarnings('ignore', message='Could not infer format')

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.jobs.schedulers import JobScheduler
from src.providers.api_server_client import APIServerClient

# Configuration
REMOTE_API_URL = os.getenv("REMOTE_API_URL", "http://localhost:8000/v1/internal/ingest")
INTERVAL_SECONDS = 60 # 1 minute refresh
RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"

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

        if RUN_ONCE:
            print("RUN_ONCE is set, exiting...")
            break

        print(f"Sleeping for {INTERVAL_SECONDS}s...")
        time.sleep(INTERVAL_SECONDS)


def _try_push_fund_list_data(data):
    """
    检查今天是否已推送基金列表数据到 DB。
    如果没有，从已组装好的 data 中提取列表字段并推送。
    失败不影响主循环。
    """
    try:
        today = date.today().isoformat()  # "YYYY-MM-DD"
        latest = APIServerClient.get_fund_data_latest_date()

        if latest == today:
            print(f"[FundData] Today's data ({today}) already exists, skipping.")
            return

        print(f"[FundData] No data for {today} (latest: {latest}), pushing...")
        payload = JobScheduler.extract_fund_list_data(data, today)

        etf_count = len(payload.get("etf", []))
        fund_count = len(payload.get("fund", []))
        print(f"[FundData] Extracted {etf_count} ETFs + {fund_count} Funds")

        ok = APIServerClient.push_fund_data(payload)
        if ok:
            print(f"[FundData] Successfully pushed fund list data for {today}")
        else:
            print(f"[FundData] Push failed, will retry next cycle")
    except Exception as e:
        print(f"[FundData] Error in fund list push (non-fatal): {e}")


if __name__ == "__main__":
    main()
