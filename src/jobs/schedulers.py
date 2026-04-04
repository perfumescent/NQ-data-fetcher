import concurrent.futures
from typing import List
from src.services.assemblers import IndexAssembler, ETFAssembler, FundAssembler
from src.domain.models import UnifiedMarketData, ETFData, FundData
from src.providers.api_server_client import APIServerClient

class JobScheduler:
    @staticmethod
    def _load_config():
        """
        从 api_server 拉取基金配置（GET /v1/internal/fund-configs）。
        api_server 不可达或 DB 未配置时自动 fallback 到本地 funds.json。
        """
        return APIServerClient.get_fund_configs()

    @staticmethod
    def extract_fund_list_data(data: UnifiedMarketData, today: str) -> dict:
        """
        从已组装的 UnifiedMarketData 中提取基金列表字段，构造推送 payload。

        不需要额外调用外部 API — 所有字段已在 fetch_all_jobs() 中组装好。

        Args:
            data: fetch_all_jobs() 的返回值
            today: 日期字符串 "YYYY-MM-DD"

        Returns:
            {"date": "YYYY-MM-DD", "etf": [...], "fund": [...]}
        """
        etf_items = []
        for etf in data.etfs:
            etf_items.append({
                "code": etf.symbol,
                "name": etf.displayName,
                "lastPrice": etf.price,
                "changePct": etf.changePct,
                "premiumRate": etf.premiumRate,
                "yearChange": etf.yearChange,
                "sixMonthChange": etf.sixMonthChange,
                "fee": etf.fee,
                "inceptionDate": etf.inceptionDate,
                "volume": etf.turnoverAmount,    # 原始值（元）
                "fundSize": etf.fundSize,         # 原始值（亿元）
                "trackingError": None,
            })

        fund_items = []
        for fund in data.funds:
            fund_items.append({
                "code": fund.symbol,
                "name": fund.displayName,
                "lastPrice": fund.price,
                "changePct": fund.changePct,
                "yearChange": fund.yearChange,
                "sixMonthChange": fund.sixMonthChange,
                "fee": fund.fee,
                "inceptionDate": fund.inceptionDate,
                "fundSize": fund.fundSize,
                "quota": fund.quota,
                "trackingError": fund.trackingError,
            })

        return {"date": today, "etf": etf_items, "fund": fund_items}

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
