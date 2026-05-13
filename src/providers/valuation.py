from __future__ import annotations

import html as html_lib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from cachetools import cached
import requests
import yfinance as yf

from ..core.cache import indicator_cache


class ValuationProvider:
    """估值数据源实验 provider。"""

    WORLDPERATIO_NASDAQ100_URL = "https://worldperatio.com/index/nasdaq-100/"
    REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    @staticmethod
    @cached(indicator_cache)
    def get_nasdaq100_pe_worldperatio(timeout_seconds: int = 20) -> dict[str, Any] | None:
        """
        从 WorldPERatio 抓取 Nasdaq 100 的 PE 当前值和月度历史。

        Args:
            timeout_seconds: HTTP 超时时间，单位秒；必须为正数，过小时可能导致页面未返回即失败。
        Returns:
            dict，包含 value/asOfDate/percentile/history 等字段；抓取或解析失败时返回 None。

        Created: 2026-05
        易错点: WorldPERatio 没有公开 JSON API，这里解析 HTML/JS；该源明确说明用 QQQ ETF 估算 Nasdaq 100 PE。
        """
        try:
            response = requests.get(
                ValuationProvider.WORLDPERATIO_NASDAQ100_URL,
                headers=ValuationProvider.REQUEST_HEADERS,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return _parse_worldperatio_nasdaq100_page(response.text)
        except Exception as exc:
            print(f"[Warn] WorldPERatio Nasdaq 100 PE failed: {exc}")
            return None

    @staticmethod
    @cached(indicator_cache)
    def get_qqq_pe_yahoo() -> dict[str, Any] | None:
        """
        从 Yahoo Finance 读取 QQQ 当前 trailing PE，用于交叉校验。

        Args:
            无。
        Returns:
            dict，包含 value/sourceSymbol/sourceLabel/fetchedAt；Yahoo 无 PE 或失败时返回 None。

        Created: 2026-05
        易错点: Yahoo 的 ^NDX 不返回 PE，只有 QQQ ETF 可返回 trailingPE；该值没有历史序列。
        """
        try:
            info = yf.Ticker("QQQ").info
            value = _float(info.get("trailingPE"))
            if value is None:
                return None
            return {
                "source": "yahoo",
                "sourceSymbol": "QQQ",
                "sourceLabel": "Invesco QQQ Trust trailingPE",
                "value": value,
                "history": [],
                "historyFrequency": None,
                "fetchedAt": _utc_now_iso(),
            }
        except Exception as exc:
            print(f"[Warn] Yahoo QQQ PE failed: {exc}")
            return None

    @staticmethod
    @cached(indicator_cache)
    def get_qqq_available_valuation_yahoo(max_holdings: int = 10) -> dict[str, Any] | None:
        """
        从 Yahoo 可得字段拼出 QQQ 当前估值快照。

        Args:
            max_holdings: 最多拉取的 QQQ 持仓数量；当前 Yahoo fund data 只稳定返回 top holdings，建议为 10。
        Returns:
            dict，包含 QQQ 当前基金估值、Top holdings 权重、成分股 PE/EPS 和覆盖范围内的加权估值；失败时返回 None。

        Created: 2026-05
        易错点: 该结果只覆盖 Yahoo 返回的 top holdings，不是完整 QQQ 100 只持仓；不能用于最终完整持仓估值。
        """
        try:
            ticker = yf.Ticker("QQQ")
            funds_data = ticker.funds_data
            top_holdings = _qqq_top_holdings_from_yahoo_funds_data(funds_data, max_holdings=max_holdings)
            fund_pe = _qqq_fund_pe_from_yahoo_funds_data(funds_data)
            fundamentals = _fetch_component_fundamentals([holding["symbol"] for holding in top_holdings])
            component_rows = _merge_holdings_and_fundamentals(top_holdings, fundamentals)
            weighted = _compute_weighted_component_valuation(component_rows)
            return {
                "source": "yahoo",
                "sourceSymbol": "QQQ",
                "scope": "qqq_top_holdings",
                "fundLevel": fund_pe,
                "holdingsCount": len(top_holdings),
                "holdingsWeight": round(sum(holding["weight"] for holding in top_holdings), 6),
                "components": component_rows,
                "weightedTopHoldings": weighted,
                "limitations": [
                    "Yahoo funds_data currently exposes top holdings, not full QQQ holdings.",
                    "Component fundamentals are pulled from Yahoo Finance and may have missing fields.",
                    "This is QQQ holdings valuation, not official NDX index valuation.",
                ],
                "fetchedAt": _utc_now_iso(),
            }
        except Exception as exc:
            print(f"[Warn] Yahoo QQQ available valuation failed: {exc}")
            return None

    @staticmethod
    def probe_nasdaq100_pe_sources() -> dict[str, Any]:
        """
        同时跑当前可用的 Nasdaq 100 PE 候选数据源。

        Args:
            无。
        Returns:
            dict；worldperatio/yahooQqq 分别为单源结果或 None，recommendation 给出当前接入建议。

        Created: 2026-05
        易错点: probe 是实验入口，不应被增量任务调用；正式接入前还需要确认源使用条款和失败兜底。
        """
        worldperatio = ValuationProvider.get_nasdaq100_pe_worldperatio()
        yahoo = ValuationProvider.get_qqq_pe_yahoo()
        qqq_available = ValuationProvider.get_qqq_available_valuation_yahoo()
        recommendation = "worldperatio_primary_yahoo_cross_check" if worldperatio else "no_stable_source"
        return {
            "worldperatio": worldperatio,
            "yahooQqq": yahoo,
            "qqqAvailableValuation": qqq_available,
            "valuationDataset": None,
            "blockedReason": "accurate earnings decomposition requires official index/fundamental earnings data; inferred earnings is rejected",
            "recommendation": recommendation,
            "fetchedAt": _utc_now_iso(),
        }


def _parse_worldperatio_nasdaq100_page(page_html: str) -> dict[str, Any]:
    """
    解析 WorldPERatio Nasdaq 100 页面中的 PE 当前值和历史序列。

    Args:
        page_html: WorldPERatio 返回的完整 HTML 文本，不能为空。
    Returns:
        dict，包含 value/asOfDate/history/percentile/methodology；解析不到核心字段时抛 ValueError。

    Created: 2026-05
    易错点: JS Date.UTC 的月份从 0 开始，写入 YYYY-MM-DD 时必须加 1。
    """
    text = _visible_text(page_html)
    current_match = re.search(
        r"Nasdaq 100 Index P/E Ratio\s+"
        r"(?P<value>\d+(?:\.\d+)?)\s+"
        r"(?P<date>\d{2}\s+[A-Za-z]+\s+\d{4})",
        text,
    )
    if not current_match:
        raise ValueError("current PE block not found")

    value = _float(current_match.group("value"))
    if value is None:
        raise ValueError("current PE value is invalid")

    history = _parse_worldperatio_history(page_html)
    history_values = [point["pe"] for point in history]
    percentile = _percentile_rank(value, history_values)
    methodology = _extract_methodology(text)

    return {
        "source": "worldperatio",
        "sourceUrl": ValuationProvider.WORLDPERATIO_NASDAQ100_URL,
        "sourceSymbol": "QQQ",
        "sourceLabel": "Nasdaq 100 PE estimated from QQQ ETF",
        "value": value,
        "asOfDate": _date_iso(current_match.group("date")),
        "percentile": percentile,
        "historyFrequency": "monthly",
        "history": history,
        "methodology": methodology,
        "fetchedAt": _utc_now_iso(),
    }


def _parse_worldperatio_history(page_html: str) -> list[dict[str, Any]]:
    """
    从 WorldPERatio 页面脚本中解析 detailPE_data 月度序列。

    Args:
        page_html: WorldPERatio 返回的完整 HTML 文本。
    Returns:
        [{"date": YYYY-MM-DD, "pe": float}, ...]，按页面顺序返回；为空时抛 ValueError。

    Created: 2026-05
    易错点: 页面还包含均值和标准差序列，只能解析 detailPE_data，避免误把 rolling avg 当成 PE 历史。
    """
    block_match = re.search(r"detailPE_data\s*=\s*\[(?P<body>.*?)\];", page_html, flags=re.S)
    if not block_match:
        raise ValueError("detailPE_data block not found")

    points: list[dict[str, Any]] = []
    for year, month, day, pe in re.findall(
        r"Date\.UTC\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2})\),\s*([-+]?\d+(?:\.\d+)?)",
        block_match.group("body"),
    ):
        pe_value = _float(pe)
        if pe_value is None:
            continue
        points.append({
            "date": f"{int(year):04d}-{int(month) + 1:02d}-{int(day):02d}",
            "pe": pe_value,
        })

    if not points:
        raise ValueError("detailPE_data has no valid points")
    return points




def _extract_methodology(text: str) -> str:
    """
    从可见文本中截取 WorldPERatio 对估算口径的说明。

    Args:
        text: 已清洗的页面可见文本。
    Returns:
        方法说明字符串；找不到时返回保守兜底文案。

    Created: 2026-05
    易错点: 该说明是数据口径，不是产品解释文案；前端展示时应翻译成更短的人话。
    """
    match = re.search(
        r"P/E Ratio is calculated on the QQQ Etf, whose benchmark is the Nasdaq 100 Index\.",
        text,
    )
    if match:
        return html_lib.unescape(match.group(0))
    return "P/E Ratio is estimated from QQQ ETF, whose benchmark is the Nasdaq 100 Index."


def _qqq_top_holdings_from_yahoo_funds_data(funds_data: Any, max_holdings: int) -> list[dict[str, Any]]:
    """
    从 yfinance FundsData 中提取 QQQ Top holdings。

    Args:
        funds_data: yf.Ticker("QQQ").funds_data 对象。
        max_holdings: 最多返回数量；小于等于 0 时返回空列表。
    Returns:
        [{"symbol": str, "name": str, "weight": float}, ...]，weight 为 0-1 小数。

    Created: 2026-05
    易错点: Yahoo 返回的是 top_holdings DataFrame，不保证是完整持仓；weight 已经是小数，不要再除以 100。
    """
    if max_holdings <= 0:
        return []
    table = getattr(funds_data, "top_holdings", None)
    if table is None or getattr(table, "empty", True):
        return []

    holdings: list[dict[str, Any]] = []
    for symbol, row in table.head(max_holdings).iterrows():
        weight = _float(row.get("Holding Percent"))
        if weight is None or weight <= 0:
            continue
        holdings.append({
            "symbol": str(symbol),
            "name": str(row.get("Name") or symbol),
            "weight": weight,
        })
    return holdings


def _qqq_fund_pe_from_yahoo_funds_data(funds_data: Any) -> dict[str, Any] | None:
    """
    从 yfinance FundsData 中提取 QQQ 当前基金层面 PE。

    Args:
        funds_data: yf.Ticker("QQQ").funds_data 对象。
    Returns:
        dict，包含 rawPriceEarnings、interpretedTrailingPe、methodology；缺失时返回 None。

    Created: 2026-05
    易错点: Yahoo 的 equity_holdings["Price/Earnings"] 当前表现为 earnings yield 小数，需用 1/value 转成 PE。
    """
    table = getattr(funds_data, "equity_holdings", None)
    if table is None or getattr(table, "empty", True):
        return None
    try:
        raw_value = _float(table.loc["Price/Earnings", "QQQ"])
    except Exception:
        raw_value = None
    if raw_value is None:
        return None
    interpreted_pe = _fund_price_earnings_to_pe(raw_value)
    return {
        "rawPriceEarnings": raw_value,
        "interpretedTrailingPe": interpreted_pe,
        "methodology": "Yahoo FundsData equity_holdings Price/Earnings; values below 1 are interpreted as earnings yield and inverted.",
    }


def _fetch_component_fundamentals(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """
    并发拉取成分股 Yahoo fundamentals。

    Args:
        symbols: 股票代码列表；空列表返回空 dict。
    Returns:
        {symbol: fundamentals dict}；单个 symbol 失败时该 symbol 值为包含 error 的 dict。

    Created: 2026-05
    易错点: yfinance info 较慢且字段可能缺失，必须逐股容错，不能让一只股票失败拖垮整批。
    """
    fundamentals: dict[str, dict[str, Any]] = {}
    if not symbols:
        return fundamentals
    max_workers = min(5, len(symbols))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one_component_fundamental, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                fundamentals[symbol] = future.result()
            except Exception as exc:
                fundamentals[symbol] = {"symbol": symbol, "error": str(exc)}
    return fundamentals


def _fetch_one_component_fundamental(symbol: str) -> dict[str, Any]:
    """
    拉取单只股票的 Yahoo PE/EPS 字段。

    Args:
        symbol: 美股代码，如 AAPL、NVDA、GOOGL。
    Returns:
        fundamentals dict，字段缺失时值为 None。

    Created: 2026-05
    易错点: forwardEps/forwardPE 对部分股票可能为空；计算时只能使用正数 PE。
    """
    info = yf.Ticker(symbol).info
    return {
        "symbol": symbol,
        "shortName": info.get("shortName") or info.get("longName"),
        "trailingPe": _float(info.get("trailingPE")),
        "forwardPe": _float(info.get("forwardPE")),
        "trailingEps": _float(info.get("trailingEps")),
        "forwardEps": _float(info.get("forwardEps")),
        "marketCap": _float(info.get("marketCap")),
    }


def _merge_holdings_and_fundamentals(
    holdings: list[dict[str, Any]],
    fundamentals: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    合并持仓权重和逐股 fundamentals。

    Args:
        holdings: _qqq_top_holdings_from_yahoo_funds_data 返回的持仓列表。
        fundamentals: _fetch_component_fundamentals 返回的逐股数据。
    Returns:
        component rows；每行包含 symbol/name/weight/trailingPe/forwardPe/trailingEps/forwardEps。

    Created: 2026-05
    易错点: 成分股名称以 holdings 为准，Yahoo info shortName 只作为补充，避免展示口径漂移。
    """
    rows: list[dict[str, Any]] = []
    for holding in holdings:
        symbol = holding["symbol"]
        fundamental = fundamentals.get(symbol, {})
        row = {
            "symbol": symbol,
            "name": holding.get("name") or fundamental.get("shortName") or symbol,
            "weight": holding.get("weight"),
            "trailingPe": fundamental.get("trailingPe"),
            "forwardPe": fundamental.get("forwardPe"),
            "trailingEps": fundamental.get("trailingEps"),
            "forwardEps": fundamental.get("forwardEps"),
            "marketCap": fundamental.get("marketCap"),
        }
        if fundamental.get("error"):
            row["error"] = fundamental["error"]
        rows.append(row)
    return rows


def _compute_weighted_component_valuation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    计算覆盖范围内的简单加权 PE 和调和 PE。

    Args:
        rows: _merge_holdings_and_fundamentals 返回的 rows；weight 为 0-1 小数。
    Returns:
        dict，包含 trailing/forward 的 simpleWeightedPe、harmonicPe、coverageWeight、validCount。

    Created: 2026-05
    易错点: 只对 PE 为正数的股票计算；coverageWeight 表示有效 PE 覆盖权重，不是完整 QQQ 权重。
    """
    return {
        "trailing": _weighted_pe_for_key(rows, "trailingPe"),
        "forward": _weighted_pe_for_key(rows, "forwardPe"),
    }


def _weighted_pe_for_key(rows: list[dict[str, Any]], pe_key: str) -> dict[str, Any]:
    """
    对指定 PE 字段计算简单加权和调和加权。

    Args:
        rows: component rows。
        pe_key: "trailingPe" 或 "forwardPe"。
    Returns:
        dict；coverageWeight=0 表示没有可计算股票。

    Created: 2026-05
    易错点: 调和 PE 公式是 sum(weight) / sum(weight / pe)，更接近基金常用口径；简单加权仅供对照。
    """
    valid = []
    for row in rows:
        weight = _float(row.get("weight"))
        pe = _float(row.get(pe_key))
        if weight is None or pe is None or weight <= 0 or pe <= 0:
            continue
        valid.append((weight, pe))
    coverage_weight = sum(weight for weight, _ in valid)
    if coverage_weight <= 0:
        return {
            "coverageWeight": 0.0,
            "validCount": 0,
            "simpleWeightedPe": None,
            "harmonicPe": None,
        }
    simple = sum(weight * pe for weight, pe in valid) / coverage_weight
    harmonic_denominator = sum(weight / pe for weight, pe in valid)
    harmonic = coverage_weight / harmonic_denominator if harmonic_denominator > 0 else None
    return {
        "coverageWeight": round(coverage_weight, 6),
        "validCount": len(valid),
        "simpleWeightedPe": round(simple, 4),
        "harmonicPe": round(harmonic, 4) if harmonic is not None else None,
    }


def _fund_price_earnings_to_pe(raw_value: float) -> float:
    """
    将 Yahoo fund Price/Earnings 字段解释为 PE。

    Args:
        raw_value: Yahoo equity_holdings 中的 Price/Earnings 原始值。
    Returns:
        PE 值；raw_value 小于 1 时按 earnings yield 取倒数，否则按 PE 原值返回。

    Created: 2026-05
    易错点: QQQ 当前 raw_value 约 0.0307，直接当 PE 会明显错误；取倒数后约 32.57。
    """
    if 0 < raw_value < 1:
        return round(1 / raw_value, 4)
    return round(raw_value, 4)


def _visible_text(page_html: str) -> str:
    """
    把 HTML 转为粗略可见文本，便于解析当前值和日期。

    Args:
        page_html: 完整 HTML 字符串。
    Returns:
        压缩空白后的可见文本。

    Created: 2026-05
    易错点: 不能在这里删除 script 前解析历史序列；历史序列只存在于脚本 detailPE_data 中。
    """
    text = re.sub(r"<script.*?</script>", " ", page_html, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _date_iso(date_text: str) -> str:
    """
    把英文日期文本转换成 YYYY-MM-DD。

    Args:
        date_text: 形如 "08 May 2026" 的日期字符串。
    Returns:
        YYYY-MM-DD 字符串；格式非法时抛 ValueError。

    Created: 2026-05
    易错点: 页面语言虽标记为意大利语，但日期目前是英文月份，解析格式不能写成本地化月份。
    """
    return datetime.strptime(date_text, "%d %B %Y").date().isoformat()


def _percentile_rank(value: float, history_values: list[float]) -> float | None:
    """
    计算当前值在历史样本中的百分位。

    Args:
        value: 当前 PE 值。
        history_values: 历史 PE 样本；空列表返回 None。
    Returns:
        0-100 的百分位；None 表示历史样本为空。

    Created: 2026-05
    易错点: 这里用 <= 当前值的经验分位，不代表估值高低判断阈值，前端不要把它文案化成买卖信号。
    """
    if not history_values:
        return None
    below_or_equal = sum(1 for item in history_values if item <= value)
    return round(below_or_equal / len(history_values) * 100, 1)


def _float(value: Any) -> float | None:
    """
    安全转换浮点数。

    Args:
        value: 任意输入；None、空字符串、不可解析文本视为无效。
    Returns:
        float 或 None。

    Created: 2026-05
    易错点: Yahoo 可能返回 NaN/None，不能直接写入 raw payload 让后端 Swift 解码链路承压。
    """
    try:
        if value is None or value == "":
            return None
        result = float(value)
        return result if result == result else None
    except (TypeError, ValueError):
        return None


def _utc_now_iso() -> str:
    """
    返回当前 UTC ISO 时间戳。

    Args:
        无。
    Returns:
        ISO 8601 字符串，精确到秒，以 Z 结尾。

    Created: 2026-05
    易错点: fetcher raw payload 中统一使用 UTC，避免 api_server 再猜本地时区。
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
