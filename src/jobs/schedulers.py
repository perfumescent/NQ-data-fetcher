import concurrent.futures
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from src.providers.api_server_client import APIServerClient
from src.providers.yahoo import YahooProvider
from src.providers.akshare_api import AkShareProvider
from src.providers.valuation import ValuationProvider


@dataclass(frozen=True)
class RawFetchPlan:
    """一次 raw 采集周期的调度计划。"""

    name: str
    include_realtime: bool
    include_meta: bool
    include_history: bool
    history_limit: int | None


class RawJobScheduler:
    @staticmethod
    def incremental_plan() -> RawFetchPlan:
        """
        构造常驻增量采集计划。

        Args:
            无。
        Returns:
            RawFetchPlan；只采 realtime/meta，不采产品 daily history。

        Created: 2026-05
        易错点: main.py 的常驻循环只能使用本计划，避免每分钟重复拉写历史。
        """
        return RawFetchPlan(
            name="incremental",
            include_realtime=True,
            include_meta=True,
            include_history=False,
            history_limit=None,
        )

    @staticmethod
    def backfill_plan() -> RawFetchPlan:
        """
        构造主动全量回填计划。

        Args:
            无。
        Returns:
            RawFetchPlan；采 realtime/meta，并全量采产品 daily history。

        Created: 2026-05
        易错点: 该计划只允许 backfill.py 主动调用，不应放入常驻循环。
        """
        return RawFetchPlan(
            name="backfill",
            include_realtime=True,
            include_meta=True,
            include_history=True,
            history_limit=None,
        )

    @staticmethod
    def _load_config():
        """
        从 api_server 读取基金清单。

        Args:
            无。
        Returns:
            {"etf": [...], "fund": [...]}，缺失时返回空列表结构。

        Created: 2026-05
        易错点: 基金配置只来自 api_server/DB；api_server 无 DB 时返回空列表，避免旧静态配置继续生效。
        """
        conf = APIServerClient.get_fund_configs()
        return {
            "etf": conf.get("etf", []) if isinstance(conf, dict) else [],
            "fund": conf.get("fund", []) if isinstance(conf, dict) else [],
        }

    @staticmethod
    def fetch_incremental_raw() -> dict:
        """
        拉取常驻增量 raw 数据。

        Args:
            无。
        Returns:
            {"realtime": [...], "meta": [...], "history": []}。

        Created: 2026-05
        易错点: 该方法不拉产品日级历史；历史初始化请运行 backfill.py。
        """
        return RawJobScheduler.fetch_raw(RawJobScheduler.incremental_plan())

    @staticmethod
    def fetch_backfill_raw() -> dict:
        """
        拉取全量回填 raw 数据。

        Args:
            无。
        Returns:
            {"realtime": [...], "meta": [...], "history": [...]}，history 为全量 provider 历史点。

        Created: 2026-05
        易错点: 本方法可能产生大量 history item，只能由主动回填入口调用；指数历史必须用 Yahoo max，不要写死 10y。
        """
        return RawJobScheduler.fetch_raw(RawJobScheduler.backfill_plan())

    @staticmethod
    def fetch_raw(plan: RawFetchPlan) -> dict:
        """
        按采集计划拉取 raw-ish 数据，不做业务指标计算。

        Args:
            plan: RawFetchPlan；由 incremental_plan/backfill_plan 显式创建。
        Returns:
            {"realtime": [...], "meta": [...], "history": [...]}，可直接推送到 raw ingest 接口。

        Created: 2026-05
        易错点: provider 层已经做了最小标准化；调度层只决定拉哪些槽位，不拼 subtitle、不算 RSI/MA。
        """
        fetch_started_at = perf_counter()
        conf = _timed_call("fetch config", RawJobScheduler._load_config)
        AkShareProvider.reset_cycle_cache()
        realtime_items = []
        meta_items = []
        history_items = []

        if plan.include_realtime:
            realtime_items.extend([
                RawJobScheduler._fetch_index_raw("^NDX"),
                RawJobScheduler._fetch_fx_raw("CNY=X"),
            ])
        if plan.include_history:
            history_items.extend(
                _history_items(
                    "^NDX",
                    "index",
                    "yahoo",
                    YahooProvider.get_history("^NDX", period="max", interval="1d"),
                    plan.history_limit,
                )
            )
            for related_symbol in ["^VXN", "^VIX", "^MOVE", "^TNX"]:
                history_items.extend(
                    _history_items(
                        related_symbol,
                        "index",
                        "yahoo",
                        YahooProvider.get_history(related_symbol, period="max", interval="1d"),
                        plan.history_limit,
                    )
                )

        etf_items = conf.get("etf", [])
        fund_items = conf.get("fund", [])

        def fetch_etf(item):
            """
            拉取单只 ETF raw 数据。

            Args:
                item: api_server/DB 配置项，必须包含 code。
            Returns:
                (realtime_item, meta_item, history_items)。

            Created: 2026-05
            易错点: 单只 ETF 异常时返回空结构，由调用方跳过，不影响其他产品。
            """
            try:
                return RawJobScheduler._fetch_etf_raw(item, plan)
            except Exception as e:
                print(f"[RawJob] ETF {item.get('code')} failed: {e}")
                return None, None, []

        def fetch_fund(item):
            """
            拉取单只场外基金 raw 数据。

            Args:
                item: api_server/DB 配置项，必须包含 code。
            Returns:
                (realtime_item, meta_item, history_items)。

            Created: 2026-05
            易错点: 单只基金异常时返回空结构，由调用方跳过，不影响其他产品。
            """
            try:
                return RawJobScheduler._fetch_fund_raw(item, plan)
            except Exception as e:
                print(f"[RawJob] Fund {item.get('code')} failed: {e}")
                return None, None, []

        products_started_at = perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_etf, item) for item in etf_items]
            futures.extend(executor.submit(fetch_fund, item) for item in fund_items)
            for future in concurrent.futures.as_completed(futures):
                realtime, meta, history = future.result()
                if realtime:
                    realtime_items.append(realtime)
                if meta:
                    meta_items.append(meta)
                history_items.extend(history)
        print(
            f"[Timing] products fetch total took {perf_counter() - products_started_at:.2f}s "
            f"(etf={len(etf_items)}, fund={len(fund_items)}, workers=5)"
        )

        json_started_at = perf_counter()
        result = {
            "realtime": [_json_ready(item) for item in realtime_items if item],
            "meta": [_json_ready(item) for item in meta_items if item],
            "history": [_json_ready(item) for item in history_items if item],
        }
        print(f"[Timing] json ready took {perf_counter() - json_started_at:.2f}s")
        print(f"[Timing] fetch_raw {plan.name} total took {perf_counter() - fetch_started_at:.2f}s")
        return result

    @staticmethod
    def _fetch_index_raw(symbol: str) -> dict | None:
        """
        拉取指数相关 raw 数据。

        Args:
            symbol: 指数代码，当前为 ^NDX。
        Returns:
            单表快照中的 quote 槽位 item；核心 priceData 缺失时返回 None。

        Created: 2026-05
        易错点: dailyHistory 用于 api_server 计算 RSI/MA；priceData 缺失时不能推送，否则会覆盖 DB 中当天有效 quote。
        """
        started_at = perf_counter()
        print(f"[RawJob] Fetching Index {symbol}...")
        try:
            price_data = _timed_call(f"index {symbol} price", YahooProvider.get_price, symbol)
            if not _has_price_data(price_data):
                print(f"[RawJob] Index {symbol} skipped: missing core priceData")
                return None
            related = {
                "tnx": _timed_call(f"index {symbol} related ^TNX", YahooProvider.get_price, "^TNX"),
                "vxn": _timed_call(f"index {symbol} related ^VXN", YahooProvider.get_price, "^VXN"),
                "vix": _timed_call(f"index {symbol} related ^VIX", YahooProvider.get_price, "^VIX"),
                "move": _timed_call(f"index {symbol} related ^MOVE", YahooProvider.get_price, "^MOVE"),
                "fut": _timed_call(f"index {symbol} related NQ=F", YahooProvider.get_price, "NQ=F"),
            }
            valuation = (
                _timed_call(
                    f"index {symbol} valuation QQQ",
                    ValuationProvider.get_qqq_available_valuation_yahoo,
                )
                if symbol == "^NDX"
                else None
            )
            daily_history = _timed_call(
                f"index {symbol} dailyHistory",
                YahooProvider.get_history,
                symbol,
                period="1y",
                interval="1d",
            )
            chart_history = _timed_call(
                "index NQ=F chartHistory",
                YahooProvider.get_history,
                "NQ=F",
                period="2mo",
                interval="1d",
            )[-30:]
            high_52w = _timed_call(f"index {symbol} high52w", YahooProvider.get_52w_high, symbol)
            payload = {
                "priceData": price_data,
                "dailyHistory": daily_history,
                "chartHistory": chart_history,
                "relatedPrices": related,
                "valuation": {"qqq": valuation} if valuation else {},
                "high52w": high_52w,
                "_sources": {
                    "priceData": "yahoo",
                    "dailyHistory": "yahoo",
                    "chartHistory": "yahoo:NQ=F",
                    "relatedPrices": {
                        "tnx": "yahoo:^TNX",
                        "vxn": "yahoo:^VXN",
                        "vix": "yahoo:^VIX",
                        "move": "yahoo:^MOVE",
                        "fut": "yahoo:NQ=F",
                    },
                    "valuation": "yahoo:QQQ",
                    "high52w": "yahoo",
                },
            }
            return _raw_item(symbol, "index", payload, _updated_at(payload.get("priceData")))
        finally:
            print(f"[Timing] index {symbol} total took {perf_counter() - started_at:.2f}s")

    @staticmethod
    def _fetch_fx_raw(symbol: str) -> dict | None:
        """
        拉取汇率 raw 数据。

        Args:
            symbol: Yahoo Finance 汇率代码，如 CNY=X。
        Returns:
            单表快照中的 quote 槽位 item；核心 priceData 缺失时返回 None。

        Created: 2026-05
        易错点: 场外基金指标只需要 price，但空 quote 不能覆盖当天已有汇率快照。
        """
        started_at = perf_counter()
        print(f"[RawJob] Fetching FX {symbol}...")
        try:
            price_data = _timed_call(f"fx {symbol} price", YahooProvider.get_price, symbol)
            if not _has_price_data(price_data):
                print(f"[RawJob] FX {symbol} skipped: missing core priceData")
                return None
            payload = {
                "priceData": price_data,
                "_sources": {"priceData": "yahoo"},
            }
            return _raw_item(symbol, "fx", payload, _updated_at(payload.get("priceData")))
        finally:
            print(f"[Timing] fx {symbol} total took {perf_counter() - started_at:.2f}s")

    @staticmethod
    def _fetch_etf_raw(item: dict, plan: RawFetchPlan) -> tuple[dict | None, dict | None, list[dict]]:
        """
        拉取 ETF raw 数据。

        Args:
            item: 配置项，包含 code/name/fee 等静态字段。
            plan: 采集计划，决定是否拉 quote/meta/history。
        Returns:
            (quote item 或 None, meta item 或 None, daily items)。

        Created: 2026-05
        易错点: 增量计划不要触碰 history；ETF 历史优先新浪，只有新浪为空才 fallback 东财。
        """
        code = item["code"]
        started_at = perf_counter()
        print(f"[RawJob] Fetching ETF {code}...")
        try:
            realtime = (
                _timed_call(f"ETF {code} realtime", AkShareProvider.get_etf_realtime, code)
                if plan.include_realtime
                else None
            )
            realtime_item = None
            if realtime:
                realtime_item = _raw_item(
                    code,
                    "etf",
                    {
                        "realtime": realtime,
                        "_sources": {
                            "realtime": _etf_realtime_source(realtime),
                        },
                    },
                    _updated_at(realtime),
                )

            meta_item = None
            if plan.include_meta:
                metadata = _timed_call(f"ETF {code} metadata", AkShareProvider.get_fund_metadata, code)
                inception_date = item.get("inceptionDate") or _timed_call(
                    f"ETF {code} inceptionDate",
                    AkShareProvider.get_fund_inception_date,
                    code,
                    is_etf=True,
                )
                meta_item = _raw_item(
                    code,
                    "etf",
                    {
                        "config": item,
                        "metadata": metadata,
                        "inceptionDate": inception_date,
                        "_sources": {
                            "config": _config_sources(item),
                            "metadata": _metadata_sources(metadata, default_source="eastmoney"),
                            "inceptionDate": "fund_config" if item.get("inceptionDate") else "eastmoney",
                        },
                    },
                    _now_db_time(),
                )

            history_items = []
            if plan.include_history:
                history = _timed_call(f"ETF {code} history sina", AkShareProvider.get_etf_history_sina, code)
                history_source = "sina"
                if not history:
                    history = _timed_call(f"ETF {code} history eastmoney", AkShareProvider.get_nav_history_em, code)
                    history_source = "eastmoney"
                history_items = _history_items(code, "etf", history_source, history, plan.history_limit)
            return realtime_item, meta_item, history_items
        finally:
            print(f"[Timing] ETF {code} total took {perf_counter() - started_at:.2f}s")

    @staticmethod
    def _fetch_fund_raw(item: dict, plan: RawFetchPlan) -> tuple[dict | None, dict | None, list[dict]]:
        """
        拉取场外基金 raw 数据。

        Args:
            item: 配置项，包含 code/name/fee/quota/trackingError 等静态字段。
            plan: 采集计划，决定是否拉 quote/meta/history。
        Returns:
            (quote item 或 None, meta item 或 None, daily items)。

        Created: 2026-05
        易错点: 增量计划不要触碰 history；全量回填才拉 NAV 历史。
        """
        code = item["code"]
        started_at = perf_counter()
        print(f"[RawJob] Fetching Fund {code}...")
        try:
            estimate = (
                _timed_call(f"Fund {code} estimate", AkShareProvider.get_fund_estimate_direct, code)
                if plan.include_realtime
                else None
            )
            realtime_item = None
            if estimate:
                realtime_item = _raw_item(
                    code,
                    "fund",
                    {
                        "estimate": estimate,
                        "_sources": {
                            "estimate": "eastmoney",
                        },
                    },
                    _updated_at(estimate),
                )

            meta_item = None
            if plan.include_meta:
                metadata = _timed_call(f"Fund {code} metadata", AkShareProvider.get_fund_metadata, code)
                inception_date = item.get("inceptionDate") or _timed_call(
                    f"Fund {code} inceptionDate",
                    AkShareProvider.get_fund_inception_date,
                    code,
                    is_etf=False,
                )
                meta_item = _raw_item(
                    code,
                    "fund",
                    {
                        "config": item,
                        "metadata": metadata,
                        "inceptionDate": inception_date,
                        "_sources": {
                            "config": _config_sources(item),
                            "metadata": _metadata_sources(metadata, default_source="eastmoney"),
                            "inceptionDate": "fund_config" if item.get("inceptionDate") else "eastmoney",
                        },
                    },
                    _now_db_time(),
                )

            history_items = []
            if plan.include_history:
                history = _timed_call(f"Fund {code} history", AkShareProvider.get_fund_history_em, code)
                history_items = _history_items(code, "fund", "eastmoney", history, plan.history_limit)
            return realtime_item, meta_item, history_items
        finally:
            print(f"[Timing] Fund {code} total took {perf_counter() - started_at:.2f}s")


def _timed_call(label: str, func, *args, **kwargs):
    """
    执行一个采集子步骤并打印耗时。

    Args:
        label: 日志标签，需包含资产类型、代码和 provider 子步骤，方便定位慢源。
        func: 可调用对象；异常不吞掉，由原调用链继续处理。
        *args: 传给 func 的位置参数；允许为空。
        **kwargs: 传给 func 的关键字参数；允许为空。
    Returns:
        func 的原始返回值；失败时沿用 func 抛出的异常。

    Created: 2026-05
    易错点: 该 helper 只观测耗时，不改变异常语义；并发产品日志可能交错，排查时以 label 为准。
    """
    started_at = perf_counter()
    try:
        return func(*args, **kwargs)
    finally:
        print(f"[Timing] {label} took {perf_counter() - started_at:.2f}s")


def _raw_item(symbol: str, asset_type: str, payload: dict, updated_at: str | None = None) -> dict:
    """
    构造 raw ingest item。

    Args:
        symbol: 产品代码。
        asset_type: index/etf/fund/fx。
        payload: rawPayload dict。
        updated_at: 上游更新时间，MySQL DATETIME 字符串或 None。
    Returns:
        raw ingest item。

    Created: 2026-05
    易错点: HTTP payload 用 camelCase 字段名，和 api_server 约定一致；不要再写顶层 source，真实字段来源看 payload._sources。
    """
    return {
        "symbol": symbol,
        "assetType": asset_type,
        "rawPayload": payload,
        "updatedAt": updated_at,
        "dataDate": _today_utc_date(),
    }


def _has_price_data(payload: dict | None) -> bool:
    """
    判断 quote payload 是否包含可写入的核心价格。

    Args:
        payload: YahooProvider.get_price 返回值，None 表示上游彻底失败。
    Returns:
        True 表示 price 字段存在且可转为 float；False 表示应跳过本次 realtime upsert。

    Created: 2026-05
    易错点: raw_daily_snapshots 当天 quote 会被覆盖，不能让空 priceData 污染本地首页数据。
    """
    if not isinstance(payload, dict):
        return False
    try:
        return float(payload.get("price")) > 0
    except (TypeError, ValueError):
        return False


def _history_items(symbol: str, asset_type: str, source: str, points: list[dict], limit: int | None) -> list[dict]:
    """
    构造单表 daily 槽位 ingest items。

    Args:
        symbol: 产品代码。
        asset_type: index/etf/fund。
        source: 数据来源标签，会被同时写入 rawPayload._sources 供字段级追踪。
        points: [{"t": YYYY-MM-DD, "p": float}, ...]。
        limit: 最近 N 条；None 表示全量。
    Returns:
        raw history item 列表。

    Created: 2026-05
    易错点: backfill 计划传 None 做全量；增量计划不会调用本函数。
    """
    items = []
    for point in _select_history_points(points, limit):
        date = str(point.get("t", ""))[:10]
        if not date:
            continue
        items.append({
            "symbol": symbol,
            "assetType": asset_type,
            "date": date,
            "rawPayload": {
                **point,
                "_sources": {
                    "t": source,
                    "p": source,
                },
            },
        })
    return items


def _select_history_points(points: list[dict] | None, limit: int | None) -> list[dict]:
    """
    按计划选择日级历史点。

    Args:
        points: provider 返回的日级点位列表；None 或空列表返回空列表。
        limit: 最近 N 条；None 表示全量。
    Returns:
        选中的点位，顺序保持不变。

    Created: 2026-05
    易错点: 只有 backfill 才允许全量；daily 应传小窗口，避免定时任务误写全量。
    """
    if not points:
        return []
    if limit is None:
        return points
    return points[-limit:]


def _config_sources(config: dict) -> dict:
    """
    标注静态配置字段来源。

    Args:
        config: fund-configs 返回的配置项。
    Returns:
        {field: source} 形式的来源映射。

    Created: 2026-05
    易错点: None 字段也保留来源，避免后续误判为“漏采集”而非“配置明确为空”。
    """
    if not isinstance(config, dict):
        return {}
    return {str(key): "fund_config" for key in config.keys()}


def _metadata_sources(metadata: dict, default_source: str) -> dict:
    """
    标注动态元数据字段来源。

    Args:
        metadata: provider 返回的 metadata dict。
        default_source: 默认来源标签，如 eastmoney。
    Returns:
        {field: source} 形式的来源映射。

    Created: 2026-05
    易错点: 这里按字段统一来源标注；若未来 metadata 内部再混源，再细分到子字段即可。
    """
    if not isinstance(metadata, dict):
        return {}
    return {str(key): default_source for key in metadata.keys()}


def _etf_realtime_source(realtime: dict) -> dict:
    """
    标注 ETF 实时字段来源。

    Args:
        realtime: ETF 实时行情 dict。
    Returns:
        {field: source} 形式的来源映射。

    Created: 2026-05
    易错点: 新浪降级路径和东方财富批量路径返回同构数据，但来源不同；需按字段拆开而不是用一个总 source。
    """
    if not isinstance(realtime, dict):
        return {}
    provider = str(realtime.get("_dataSource") or "eastmoney_or_sina")
    source_by_field = {}
    for key in realtime.keys():
        if key == "_dataSource":
            continue
        if key == "timestamp":
            source_by_field[str(key)] = "provider_runtime"
        else:
            source_by_field[str(key)] = provider
    return source_by_field


def _updated_at(payload: dict | None) -> str | None:
    """
    从 provider payload 提取 DB 更新时间。

    Args:
        payload: provider 返回 dict，可能包含 timestamp 或 updatedAt。
    Returns:
        YYYY-MM-DD HH:MM:SS 或 None。

    Created: 2026-05
    易错点: raw_payload 内可以保留 ISO 字符串，但 raw 表 updated_at 用 MySQL DATETIME 更稳。
    """
    if not payload:
        return None
    value = payload.get("timestamp") or payload.get("updatedAt")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str) and value:
        text = value.replace("T", " ").replace("Z", "")
        return text[:19]
    return _now_db_time()


def _now_db_time() -> str:
    """
    返回当前 UTC MySQL DATETIME 字符串。

    Args:
        无。
    Returns:
        YYYY-MM-DD HH:MM:SS。

    Created: 2026-05
    易错点: 使用 UTC，api_server 对外再转 ISO8601。
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _today_utc_date() -> str:
    """
    返回当前 UTC 业务日期。

    Args:
        无。
    Returns:
        YYYY-MM-DD。

    Created: 2026-05
    易错点: 单表快照用该日期作为 dataDate；如需按中国自然日统计，应统一改为 Asia/Shanghai。
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _json_ready(value):
    """
    将 provider 返回值递归转为 requests 可 JSON 序列化对象。

    Args:
        value: 任意对象，可能包含 datetime、pandas Timestamp、numpy 标量等。
    Returns:
        JSON 可序列化对象。

    Created: 2026-05
    易错点: requests.post(json=...) 不支持 default=str，必须在发送前清洗。
    """
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        pass
    return value
