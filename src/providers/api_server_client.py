"""
api_server_client.py
data_fetcher 与 api_server 通信的 HTTP 客户端。

职责：
1. 拉取基金配置（GET /internal/fund-configs），失败时 fallback 到本地 funds.json
2. 查询列表数据状态（GET /internal/fund-data/status）
3. 推送列表数据（POST /internal/fund-data）
"""

import json
import os
from urllib.parse import urlparse

import requests

# 从 REMOTE_API_URL 提取 base（与 main.py 共用同一个环境变量）
_remote = os.getenv("REMOTE_API_URL", "http://localhost:8000/v1/internal/ingest")
_parsed = urlparse(_remote)
_API_SERVER_BASE = f"{_parsed.scheme}://{_parsed.netloc}"
_FUND_CONFIGS_URL = f"{_API_SERVER_BASE}/v1/internal/fund-configs"
_FUND_DATA_STATUS_URL = f"{_API_SERVER_BASE}/v1/internal/fund-data/status"
_FUND_DATA_INGEST_URL = f"{_API_SERVER_BASE}/v1/internal/fund-data"

# 本地 fallback 路径
_LOCAL_FUNDS_JSON = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../funds.json")
)


class APIServerClient:

    # ---------------------------------------------------------------------------
    # 基金配置
    # ---------------------------------------------------------------------------

    @staticmethod
    def get_fund_configs() -> dict:
        """
        返回基金配置字典，格式：{"etf": [...], "fund": [...]}

        优先从 api_server 拉取；以下情况自动 fallback 到本地 funds.json：
        - api_server 不可达
        - 返回非 2xx（含 503，表示 DB 未配置）
        - 返回数据格式不符合预期
        """
        try:
            resp = requests.get(_FUND_CONFIGS_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and ("etf" in data or "fund" in data):
                    print(f"[Config] Loaded fund configs from api_server ({_FUND_CONFIGS_URL})")
                    return data
                print(f"[Config] api_server returned unexpected format, falling back to local JSON")
            else:
                print(f"[Config] api_server returned {resp.status_code}, falling back to local JSON")
        except requests.exceptions.RequestException as e:
            print(f"[Config] Could not reach api_server ({e}), falling back to local JSON")

        return APIServerClient._load_local()

    @staticmethod
    def _load_local() -> dict:
        try:
            with open(_LOCAL_FUNDS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"[Config] Loaded fund configs from local {_LOCAL_FUNDS_JSON}")
            return data
        except Exception as e:
            print(f"[Config] Failed to load local funds.json: {e}")
            return {"etf": [], "fund": []}

    # ---------------------------------------------------------------------------
    # 列表数据状态检查
    # ---------------------------------------------------------------------------

    @staticmethod
    def get_fund_data_latest_date() -> str | None:
        """
        查询 api_server 中基金列表数据的最新日期。
        返回 "YYYY-MM-DD" 字符串，或 None（表示无数据 / 不可达）。
        """
        try:
            resp = requests.get(_FUND_DATA_STATUS_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("latestDate")
            else:
                print(f"[FundData] Status check returned {resp.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"[FundData] Status check failed: {e}")
            return None

    # ---------------------------------------------------------------------------
    # 推送列表数据
    # ---------------------------------------------------------------------------

    @staticmethod
    def push_fund_data(payload: dict) -> bool:
        """
        推送基金列表数据到 api_server。
        payload 格式：{"date": "YYYY-MM-DD", "etf": [...], "fund": [...]}
        返回是否成功。
        """
        try:
            resp = requests.post(_FUND_DATA_INGEST_URL, json=payload, timeout=10)
            if resp.status_code == 200:
                print(f"[FundData] Push succeeded: {resp.json()}")
                return True
            else:
                print(f"[FundData] Push failed with {resp.status_code}: {resp.text}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"[FundData] Push failed: {e}")
            return False
