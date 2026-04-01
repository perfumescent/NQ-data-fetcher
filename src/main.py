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
            
            # 3. Push
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

if __name__ == "__main__":
    main()
