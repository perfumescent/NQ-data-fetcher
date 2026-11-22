"""Nasdaq-100 名单与行情 raw 采集；不计算业务指标。"""

from datetime import datetime, timezone
from time import monotonic

import requests


class NasdaqProvider:
    """批量读取 Nasdaq 公共成分股快照，失败时保留服务端旧快照。"""

    URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
    _next_attempt = 0.0

    @classmethod
    def get_constituents_raw(cls) -> dict | None:
        """
        最多每五分钟采集一次完整成分股快照。

        Args:
            cls: Provider 类型，持有进程内尝试时间；无业务入参。
        Returns:
            可推送到 realtime 槽位的 raw item，包含完整 JSON 对象；未到时、请求失败或响应不是 JSON 对象时返回 None。

        Created: 2026-09
        易错点: 跳过时不能重新打时间戳推送旧行情；名单完整性和行情字段由服务端处理。
        """
        now = monotonic()
        if now < cls._next_attempt:
            return None
        cls._next_attempt = now + 300
        try:
            response = requests.get(
                cls.URL,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=(4, 12),
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict):
                raise ValueError("Expected a Nasdaq JSON object")
            collected = datetime.now(timezone.utc)
            return {
                "symbol": "^NDX:CONSTITUENTS",
                "assetType": "index",
                "source": "nasdaq",
                "dataDate": collected.date().isoformat(),
                "updatedAt": collected.strftime("%Y-%m-%d %H:%M:%S"),
                "rawPayload": {
                    "nasdaqResponse": body,
                    "collectedAt": collected.isoformat(),
                    "_sources": {"nasdaq": cls.URL},
                },
            }
        except (requests.RequestException, ValueError):
            print("[Nasdaq] Constituent snapshot unavailable; retaining previous raw snapshot")
            return None
