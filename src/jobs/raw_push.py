from src.providers.api_server_client import APIServerClient


def push_raw_payload(payload: dict) -> None:
    """
    按非空槽位推送 raw payload。

    Args:
        payload: RawJobScheduler 返回值，包含 realtime/meta/history 三个列表。
    Returns:
        None。

    Created: 2026-05
    易错点: 增量入口 history 为空时不要发空 POST，避免日志误导为“本轮处理了 history”。
    """
    realtime = payload.get("realtime", [])
    meta = payload.get("meta", [])
    history = payload.get("history", [])
    if realtime:
        APIServerClient.push_raw_realtime({"items": realtime})
    else:
        print("[RawData] realtime skipped: no items")
    if meta:
        APIServerClient.push_raw_meta({"items": meta})
    else:
        print("[RawData] meta skipped: no items")
    if history:
        APIServerClient.push_raw_history({"items": history})
    else:
        print("[RawData] history skipped: no items")
