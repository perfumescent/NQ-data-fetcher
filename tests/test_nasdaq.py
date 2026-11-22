"""离线验证成分股源的名单完整性、节流与调度，不调用任何外部接口。"""

import unittest
from unittest.mock import Mock, patch

import requests

from src.providers.nasdaq import NasdaqProvider


def source_body(count=102):
    """创建完整源表。Args: count 为证券数。Returns: Nasdaq JSON 字典。Created: 2026-09。易错点: 不能把指数 100 家公司硬编码成 100 个证券。"""
    return {"data": {"date": "Sep 3, 2026", "totalrecords": count, "data": {"rows": [
        {"symbol": f"T{i:03}", "percentageChange": "-1.00%", "lastSalePrice": "$10.00"}
        for i in range(count)
    ]}}}


class NasdaqTests(unittest.TestCase):
    """Provider 原始数据完整性与失败隔离。"""

    def setUp(self):
        """重置节流时钟。Args: 无。Returns: 无。Created: 2026-09。易错点: 每个测试隔离进程内状态。"""
        NasdaqProvider._next_attempt = 0

    def test_complete_batch_preserves_raw_and_skips_repush(self):
        """验证完整批次和节流。Args: 无。Returns: 无。Created: 2026-09。易错点: 命中节流不能用新时间重发旧数据。"""
        response = Mock()
        response.json.return_value = dict(source_body(), status={"rCode": 200}, extraSourceField=["retained"])
        with patch("src.providers.nasdaq.requests.get", return_value=response) as get:
            first = NasdaqProvider.get_constituents_raw()
            second = NasdaqProvider.get_constituents_raw()
        self.assertEqual(get.call_count, 1)
        self.assertIsNone(second)
        self.assertEqual(first["symbol"], "^NDX:CONSTITUENTS")
        self.assertEqual(first["rawPayload"]["nasdaqResponse"], response.json.return_value)
        self.assertEqual(len(first["rawPayload"]["nasdaqResponse"]["data"]["data"]["rows"]), 102)
        self.assertIn("_sources", first["rawPayload"])
        self.assertEqual(get.call_args.kwargs["timeout"], (4, 12))

    def test_non_object_response_is_ignored(self):
        """验证非 JSON 对象不推送。Args: 无。Returns: 无。Created: 2026-11。易错点: 合法 JSON 对象的名单验证由服务端完成。"""
        with patch("src.providers.nasdaq.requests.get") as get:
            get.return_value.json.return_value = []
            self.assertIsNone(NasdaqProvider.get_constituents_raw())

    def test_timeout_is_bounded_and_retried_next_slot(self):
        """验证失败隔离与下轮重试。Args: 无。Returns: 无。Created: 2026-09。易错点: 失败同样节流，避免限流期间请求风暴。"""
        with patch("src.providers.nasdaq.monotonic", side_effect=[100, 200, 401]):
            with patch("src.providers.nasdaq.requests.get", side_effect=requests.Timeout) as get:
                self.assertIsNone(NasdaqProvider.get_constituents_raw())
                self.assertIsNone(NasdaqProvider.get_constituents_raw())
                self.assertIsNone(NasdaqProvider.get_constituents_raw())
                self.assertEqual(get.call_count, 2)

    def test_incremental_scheduler_only_adds_realtime(self):
        """验证常驻调度槽位。Args: 无。Returns: 无。Created: 2026-09。易错点: 成分股不能触发历史采集，也不能覆盖指数 key。"""
        from src.jobs.schedulers import RawJobScheduler
        item = {"symbol": "^NDX:CONSTITUENTS", "rawPayload": {"nasdaqResponse": source_body()}}
        with patch.object(RawJobScheduler, "_load_config", return_value={"etf": [], "fund": []}), \
             patch.object(RawJobScheduler, "_fetch_index_raw", return_value={"symbol": "^NDX"}), \
             patch.object(RawJobScheduler, "_fetch_fx_raw", return_value=None), \
             patch.object(NasdaqProvider, "get_constituents_raw", return_value=item):
            result = RawJobScheduler.fetch_incremental_raw()
        self.assertEqual(result["history"], [])
        self.assertEqual(result["meta"], [])
        self.assertEqual([row["symbol"] for row in result["realtime"]], ["^NDX:CONSTITUENTS", "^NDX"])


if __name__ == "__main__":
    unittest.main()
