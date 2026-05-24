"""
api_server_client.py
data_fetcher 与 api_server 通信的 HTTP 客户端。

职责：
1. 拉取基金配置（GET /internal/fund-configs），并用本地 funds.json 补齐迁移期缺失默认值
2. 查询列表数据状态（GET /internal/fund-data/status）
3. 推送列表数据（POST /internal/fund-data）
"""

import json
import os
from time import perf_counter
from urllib.parse import urlparse

import requests

# 从 REMOTE_API_URL 提取 base（与 main.py 共用同一个环境变量）
_remote = os.getenv("REMOTE_API_URL", "http://localhost:8000/v1/internal/ingest")
_parsed = urlparse(_remote)
_API_SERVER_BASE = f"{_parsed.scheme}://{_parsed.netloc}"
_FUND_CONFIGS_URL = f"{_API_SERVER_BASE}/v1/internal/fund-configs"
_FUND_DATA_INGEST_URL = f"{_API_SERVER_BASE}/v1/internal/fund-data"
_RAW_REALTIME_URL = f"{_API_SERVER_BASE}/v1/internal/raw/realtime"
_RAW_META_URL = f"{_API_SERVER_BASE}/v1/internal/raw/meta"
_RAW_HISTORY_URL = f"{_API_SERVER_BASE}/v1/internal/raw/history"
_RAW_HISTORY_CHUNK_SIZE = 500

# 本地迁移期默认配置路径。DB 字段完整后可删除，但删除前必须先完成线上 default_fields 迁移。
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
        返回 data_fetcher ingest 使用的基金配置字典。

        Args:
            无。
        Returns:
            {"etf": [...], "fund": [...]}；api_server 有数据时以 DB 非空字段优先，并用本地 funds.json 补齐缺失默认值；
            api_server 不可达、DB 未配置或响应格式异常时返回本地 funds.json。

        Created: 2026-05
        易错点: 线上 DB default_fields 可能尚未完整迁移；直接删除本地默认值会导致 quota/name 等历史静态字段丢失。
        """
        started_at = perf_counter()
        try:
            resp = requests.get(_FUND_CONFIGS_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and ("etf" in data or "fund" in data):
                    print(f"[Config] Loaded fund configs from api_server ({_FUND_CONFIGS_URL})")
                    return APIServerClient._merge_with_local_defaults(data)
                print("[Config] api_server returned unexpected format, falling back to local JSON")
            else:
                print(f"[Config] api_server returned {resp.status_code}, falling back to local JSON")
        except requests.exceptions.RequestException as e:
            print(f"[Config] Could not reach api_server ({e}), falling back to local JSON")
        finally:
            print(f"[Timing] config GET /internal/fund-configs took {perf_counter() - started_at:.2f}s")

        return APIServerClient._load_local()

    @staticmethod
    def _load_local() -> dict:
        """
        读取本地迁移期默认基金配置。

        Args:
            无。
        Returns:
            funds.json 内容；读取失败时返回空列表结构。

        Created: 2026-05
        易错点: 这是 DB 配置迁移期的安全兜底，不是新的主配置源；删除前必须确认线上 DB default_fields 已补齐。
        """
        try:
            with open(_LOCAL_FUNDS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"[Config] Loaded fund configs from local {_LOCAL_FUNDS_JSON}")
            return data
        except Exception as e:
            print(f"[Config] Failed to load local funds.json: {e}")
            return {"etf": [], "fund": []}

    @staticmethod
    def _merge_with_local_defaults(remote: dict) -> dict:
        """
        将 api_server 返回的基金配置与本地默认配置合并。

        Args:
            remote: api_server /internal/fund-configs 返回值，格式 {"etf": [...], "fund": [...]}。
        Returns:
            合并后的配置；本地 funds.json 提供 name、fee、quota 等迁移期默认值，remote 非空字段优先。

        Created: 2026-05
        易错点: 管理后台 DB 里可能只有 code/type/defaultFields 的极简配置，不能因此丢掉本地 funds.json 的 quota 兜底。
        """
        local = APIServerClient._load_local()
        merged = {}
        for segment in ("etf", "fund"):
            local_items = local.get(segment, []) if isinstance(local, dict) else []
            remote_items = remote.get(segment, []) if isinstance(remote, dict) else []
            local_by_code = {
                str(item.get("code")): item
                for item in local_items
                if isinstance(item, dict) and item.get("code")
            }
            segment_items = []
            seen = set()
            for item in remote_items:
                if not isinstance(item, dict) or not item.get("code"):
                    continue
                code = str(item["code"])
                base = dict(local_by_code.get(code, {}))
                override = {k: v for k, v in item.items() if v is not None}
                segment_items.append({**base, **override})
                seen.add(code)
            segment_items.extend(
                item
                for code, item in local_by_code.items()
                if code not in seen
            )
            merged[segment] = segment_items
        return merged

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

    # ---------------------------------------------------------------------------
    # Raw ELT 推送
    # ---------------------------------------------------------------------------

    @staticmethod
    def push_raw_realtime(payload: dict) -> bool:
        """
        推送实时 raw 数据。

        Args:
            payload: {"items": [...]}；每个 item 包含 symbol、assetType、rawPayload、updatedAt、dataDate；字段来源放在 rawPayload._sources。
        Returns:
            True 表示 api_server 返回 200；False 表示请求失败或服务端拒绝。

        Created: 2026-05
        易错点: raw 接口只在 api_server MySQL 模式可用，503 时应视为可重试失败。
        """
        return APIServerClient._post_raw(_RAW_REALTIME_URL, payload, "realtime")

    @staticmethod
    def push_raw_meta(payload: dict) -> bool:
        """
        推送元数据 raw 数据。

        Args:
            payload: {"items": [...]}；每个 item 包含 symbol、assetType、rawPayload、updatedAt、dataDate；字段来源放在 rawPayload._sources。
        Returns:
            True 表示 api_server 返回 200；False 表示请求失败或服务端拒绝。

        Created: 2026-05
        易错点: 同一 symbol+dataDate 会覆盖当天 meta_payload，是单表方案下的预期行为。
        """
        return APIServerClient._post_raw(_RAW_META_URL, payload, "meta")

    @staticmethod
    def push_raw_history(payload: dict) -> bool:
        """
        推送日级历史 raw 数据。

        Args:
            payload: {"items": [...]}；每个 item 包含 symbol、date、assetType、rawPayload；字段来源放在 rawPayload._sources。
        Returns:
            True 表示 api_server 返回 200；False 表示请求失败或服务端拒绝。

        Created: 2026-05
        易错点: 全量 backfill 的 history payload 会很大，必须分片 POST，避免 nginx 413 Request Entity Too Large。
        """
        items = payload.get("items", [])
        if not isinstance(items, list):
            return APIServerClient._post_raw(_RAW_HISTORY_URL, payload, "history")
        if not items:
            print("[RawData] history skipped: no items")
            return True

        ok = True
        total = len(items)
        for start in range(0, total, _RAW_HISTORY_CHUNK_SIZE):
            chunk = items[start:start + _RAW_HISTORY_CHUNK_SIZE]
            chunk_no = start // _RAW_HISTORY_CHUNK_SIZE + 1
            chunk_total = (total + _RAW_HISTORY_CHUNK_SIZE - 1) // _RAW_HISTORY_CHUNK_SIZE
            label = f"history chunk {chunk_no}/{chunk_total}"
            if not APIServerClient._post_raw(_RAW_HISTORY_URL, {"items": chunk}, label):
                ok = False
        return ok

    @staticmethod
    def _post_raw(url: str, payload: dict, label: str) -> bool:
        """
        执行 raw POST 请求。

        Args:
            url: internal raw ingest URL。
            payload: JSON payload。
            label: 日志标签。
        Returns:
            True 表示成功；False 表示失败。

        Created: 2026-05
        易错点: 这里不 raise，避免 raw 单路失败导致 data_fetcher 整个循环退出。
        """
        started_at = perf_counter()
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.status_code == 200:
                print(f"[RawData] {label} push succeeded: {resp.json()}")
                return True
            print(f"[RawData] {label} push failed with {resp.status_code}: {resp.text}")
            return False
        except requests.exceptions.RequestException as e:
            print(f"[RawData] {label} push failed: {e}")
            return False
        finally:
            print(f"[Timing] raw POST {label} took {perf_counter() - started_at:.2f}s")
