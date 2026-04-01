import concurrent.futures
import json
import os
from src.services.assemblers import IndexAssembler, ETFAssembler, FundAssembler
from src.domain.models import UnifiedMarketData

class JobScheduler:
    @staticmethod
    def _load_config():
        try:
            # Structure: scripts/data_fetcher/funds.json
            path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../funds.json"))
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}

    @staticmethod
    def fetch_all_jobs() -> UnifiedMarketData:
        conf = JobScheduler._load_config()
        
        # 1. Index
        print(f"[Job] Fetching Index ^NDX...")
        index_data = IndexAssembler.assemble("^NDX")
        
        # 2. ETFs & Funds (parallel in single executor)
        etf_list = conf.get("etf", [])
        fund_list = conf.get("fund", [])
        
        def fetch_etf(item):
            code = item["code"]
            print(f"[Job] Fetching ETF {code}...")
            return ("etf", ETFAssembler.assemble(code))
        
        def fetch_fund(item):
            code = item["code"]
            print(f"[Job] Fetching Fund {code}...")
            return ("fund", FundAssembler.assemble(code))
        
        etfs = []
        funds = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all tasks at once
            etf_futures = [executor.submit(fetch_etf, item) for item in etf_list]
            fund_futures = [executor.submit(fetch_fund, item) for item in fund_list]
            
            # Collect results
            for future in concurrent.futures.as_completed(etf_futures + fund_futures):
                tag, data = future.result()
                if tag == "etf":
                    etfs.append(data)
                else:
                    funds.append(data)
        
        return UnifiedMarketData(
            index=index_data,
            etfs=etfs,
            funds=funds,
            updatedAt=index_data.updatedAt
        )
