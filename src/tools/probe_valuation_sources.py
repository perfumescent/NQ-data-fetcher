from __future__ import annotations

import json
import sys
from typing import Any

from src.providers.valuation import ValuationProvider


def summarize_probe_result(result: dict[str, Any]) -> dict[str, Any]:
    """
    压缩 PE 数据源实验结果，便于人工快速判断源是否可用。

    Args:
        result: ValuationProvider.probe_nasdaq100_pe_sources 返回的完整结果。
    Returns:
        dict；保留当前值、历史范围、样本数和首尾样本，不输出完整历史。

    Created: 2026-05
    易错点: 摘要只用于命令行检查，正式接入时仍应使用 provider 的完整 history 字段。
    """
    worldperatio = result.get("worldperatio") if isinstance(result.get("worldperatio"), dict) else None
    yahoo = result.get("yahooQqq") if isinstance(result.get("yahooQqq"), dict) else None
    qqq_available = result.get("qqqAvailableValuation") if isinstance(result.get("qqqAvailableValuation"), dict) else None

    worldperatio_summary = None
    if worldperatio:
        history = worldperatio.get("history") if isinstance(worldperatio.get("history"), list) else []
        worldperatio_summary = {
            "source": worldperatio.get("source"),
            "value": worldperatio.get("value"),
            "asOfDate": worldperatio.get("asOfDate"),
            "percentile": worldperatio.get("percentile"),
            "historyFrequency": worldperatio.get("historyFrequency"),
            "historyCount": len(history),
            "historyStart": history[0] if history else None,
            "historyEnd": history[-1] if history else None,
            "methodology": worldperatio.get("methodology"),
        }

    yahoo_summary = None
    if yahoo:
        yahoo_summary = {
            "source": yahoo.get("source"),
            "sourceSymbol": yahoo.get("sourceSymbol"),
            "value": yahoo.get("value"),
            "historyFrequency": yahoo.get("historyFrequency"),
        }

    qqq_available_summary = None
    if qqq_available:
        components = qqq_available.get("components") if isinstance(qqq_available.get("components"), list) else []
        qqq_available_summary = {
            "source": qqq_available.get("source"),
            "sourceSymbol": qqq_available.get("sourceSymbol"),
            "scope": qqq_available.get("scope"),
            "fundLevel": qqq_available.get("fundLevel"),
            "holdingsCount": qqq_available.get("holdingsCount"),
            "holdingsWeight": qqq_available.get("holdingsWeight"),
            "weightedTopHoldings": qqq_available.get("weightedTopHoldings"),
            "componentsSample": components[:5],
            "limitations": qqq_available.get("limitations"),
        }

    return {
        "worldperatio": worldperatio_summary,
        "yahooQqq": yahoo_summary,
        "qqqAvailableValuation": qqq_available_summary,
        "valuationDataset": result.get("valuationDataset"),
        "blockedReason": result.get("blockedReason"),
        "recommendation": result.get("recommendation"),
        "fetchedAt": result.get("fetchedAt"),
    }


def main() -> None:
    """
    运行 Nasdaq 100 PE 候选数据源实验并输出 JSON。

    Args:
        无。命令行传入 --full 时输出完整历史，否则输出摘要。
    Returns:
        None；结果直接打印到 stdout，供人工检查或重定向保存。

    Created: 2026-05
    易错点: 本脚本只做源稳定性验证，不写数据库，也不调用 api_server ingest。
    """
    result = ValuationProvider.probe_nasdaq100_pe_sources()
    payload = result if "--full" in sys.argv else summarize_probe_result(result)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
