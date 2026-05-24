from time import perf_counter

from src.providers.api_server_client import APIServerClient


def push_raw_payload(payload: dict) -> None:
    """
    按非空槽位推送 raw payload。

    Args:
        payload: RawJobScheduler 返回值，包含 realtime/meta/history 三个列表。
    Returns:
        None。

    Created: 2026-05
    易错点: 增量入口 history 为空时不要发空 POST；耗时日志按槽位打印，避免把抓取慢误判成写入慢。
    """
    started_at = perf_counter()
    realtime = payload.get("realtime", [])
    meta = payload.get("meta", [])
    history = payload.get("history", [])
    try:
        if realtime:
            slot_started_at = perf_counter()
            APIServerClient.push_raw_realtime({"items": realtime})
            print(f"[Timing] raw push realtime slot took {perf_counter() - slot_started_at:.2f}s")
        else:
            print("[RawData] realtime skipped: no items")
        if meta:
            slot_started_at = perf_counter()
            APIServerClient.push_raw_meta({"items": meta})
            print(f"[Timing] raw push meta slot took {perf_counter() - slot_started_at:.2f}s")
        else:
            print("[RawData] meta skipped: no items")
        if history:
            slot_started_at = perf_counter()
            APIServerClient.push_raw_history({"items": history})
            print(f"[Timing] raw push history slot took {perf_counter() - slot_started_at:.2f}s")
        else:
            print("[RawData] history skipped: no items")
    finally:
        print(f"[Timing] raw push total took {perf_counter() - started_at:.2f}s")
