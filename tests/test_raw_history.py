"""Fetcher 边界回归：完整搬运源表，只通过现有 HTTP 写入，不做行情清洗。"""

import json
import unittest
from unittest.mock import patch

import pandas as pd

from src.jobs.schedulers import RawJobScheduler, _json_ready, _raw_history_items
from src.providers.yahoo import YahooProvider


class RawHistoryTests(unittest.TestCase):
    """无需网络、数据库或新测试依赖的原始采集验证。"""

    def setUp(self):
        """
        创建含源缺失值、非法价格、附加列与带偏移索引的 SDK 响应。

        Args: 无。
        Returns: None；设置每个测试独立的源表并清空测试进程内缓存。
        Created: 2026-09
        易错点: 这里的无效价格必须被 Fetcher 保留，不能先做成理想化数据。
        """
        YahooProvider.get_raw_history.cache_clear()
        self.frame = pd.DataFrame({
            "Open": [100, float("nan"), 98], "High": [104, 103, 102],
            "Low": [99, 95, 97], "Close": [102, float("nan"), -1],
            "Adj Close": [101, float("nan"), -1], "Volume": [12345, 0, 67890],
            "Dividends": [0, 1.25, 0], "Stock Splits": [0, 0, 2],
            "Future Column": [7, 8, 9],
        }, index=pd.date_range("2026-09-04 08:00", periods=3, freq="5min", tz="America/New_York"))

    def test_source_table_is_preserved_without_cleaning(self):
        """
        验证全部列、缺失行、盘前数据、负价和原时间偏移均保留。

        Args: 无。
        Returns: None；断言严格 JSON 可传输，原表未被改变。
        Created: 2026-09
        易错点: JSON null 是 NaN 的传输表示，不得借此删除原始行。
        """
        before = self.frame.copy(deep=True)
        with patch("src.providers.yahoo.yf.Ticker") as ticker:
            ticker.return_value.history.return_value = self.frame
            raw = YahooProvider.get_raw_history("^NDX", interval="5m", lookback_days=59)
            self.assertEqual(raw["columns"], list(self.frame.columns))
            self.assertEqual(len(raw["data"]), 3)
            self.assertEqual(raw["data"][1][3], None)
            self.assertEqual(raw["data"][2][3], -1)
            self.assertEqual(raw["data"][2][-1], 9)
            self.assertEqual(raw["index"][0], "2026-09-04T08:00:00-04:00")
            self.assertEqual(raw["timezone"], "America/New_York")
            options = ticker.return_value.history.call_args.kwargs
            self.assertTrue(options["keepna"])
            self.assertTrue(options["prepost"])
            self.assertTrue(options["actions"])
            for option in ("auto_adjust", "back_adjust", "repair", "rounding"):
                self.assertFalse(options[option])
            self.assertIn("start", options)
            self.assertNotIn("period", options)
            self.assertEqual(raw, YahooProvider.get_raw_history("^NDX", interval="5m", lookback_days=59))
            self.assertEqual(ticker.call_count, 1)
            json.dumps(_json_ready(raw), allow_nan=False)
        pd.testing.assert_frame_equal(before, self.frame)

    def test_backfill_preserves_same_format_and_all_rows(self):
        """
        验证按日拆包不改变源格式或过滤价格缺失记录。

        Args: 无。
        Returns: None；断言完整字段进入现有 daily 槽位信封。
        Created: 2026-09
        易错点: 拆分仅用于 DB 日期键和 HTTP 分片，不是行情转换。
        """
        self.frame.index = pd.date_range("2026-09-01", periods=3, freq="D", tz="America/New_York")
        with patch("src.providers.yahoo.yf.Ticker") as ticker:
            ticker.return_value.history.return_value = self.frame
            raw = YahooProvider.get_raw_history("^NDX")
        rows = _raw_history_items("^NDX", "index", raw)
        self.assertEqual([r["date"] for r in rows], ["2026-09-01", "2026-09-02", "2026-09-03"])
        for i, row in enumerate(rows):
            table = row["rawPayload"]["histories"][0]
            self.assertEqual(table["columns"], raw["columns"])
            self.assertEqual(table["data"], [raw["data"][i]])
            self.assertEqual(table["index"], [raw["index"][i]])
            self.assertNotIn("p", row["rawPayload"])
        self.assertEqual(_raw_history_items("^NDX", "index", None), [])

    def test_scheduler_stores_one_history_family_and_no_derived_fields(self):
        """
        验证指数 quote 只保存统一 histories，不再生成页面专用历史副本或 52W 指标。

        Args: 无。
        Returns: None；采集计划按源粒度而非四个 UI 按钮定义。
        Created: 2026-09
        易错点: 业务接口依赖的旧字段由 server 兼容，不得让 Fetcher 双写新旧副本。
        """
        with patch("src.jobs.schedulers.YahooProvider.get_price", return_value={"price": 100}), \
             patch("src.jobs.schedulers.ValuationProvider.get_qqq_available_valuation_yahoo", return_value=None), \
             patch("src.jobs.schedulers.YahooProvider.get_raw_history", return_value={"format": "yahoo-history-v1"}) as history, \
             patch("src.jobs.schedulers.YahooProvider.get_52w_high") as high:
            payload = RawJobScheduler._fetch_index_raw("^NDX")["rawPayload"]
        self.assertEqual(len(payload["histories"]), 6)
        self.assertEqual(
            [(call.args, call.kwargs) for call in history.call_args_list[:-1]],
            [((symbol,), {"interval": "1d", "period": "1y"}) for symbol in ("^NDX", "NQ=F", "^VXN", "^VIX", "^MOVE")],
        )
        high.assert_not_called()
        for field in ("dailyHistory", "chartHistory", "intradayHistory", "high52w", "ranges", "rhythmBuckets"):
            self.assertNotIn(field, payload)
        self.assertEqual(history.call_args_list[-1].kwargs, {"interval": "5m", "lookback_days": 59})

    def test_failed_source_does_not_invent_rows(self):
        """
        验证网络或源 SDK 失败只返回缺失，不制造数据。

        Args: 无。
        Returns: None；无外部请求。
        Created: 2026-09
        易错点: 不能把请求失败变成价格为零的行情条。
        """
        with patch("src.providers.yahoo.yf.Ticker", side_effect=RuntimeError("fixture failure")):
            self.assertIsNone(YahooProvider.get_raw_history("^FAILED"))
