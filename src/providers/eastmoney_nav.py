"""Lossless Eastmoney JSON NAV pagination, independent from AKShare's positional columns."""

from datetime import datetime, timezone
from time import monotonic
from typing import Callable

import requests

from src.providers.etf_history import json_value


NAV_URL = "https://api.fund.eastmoney.com/f10/lsjz"


def fetch_nav_table(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    timeout_seconds: float = 180,
    get: Callable = requests.get,
) -> dict | None:
    """Fetch and preserve every NAV record field across all source pages.

    Args:
        symbol: Verified six-digit ETF code; start_date/end_date: inclusive YYYYMMDD bounds;
        timeout_seconds: Positive overall collection budget; get: injectable HTTP GET adapter.
    Returns:
        Complete eastmoney-fund-history-v1 unit-nav-history source table, or None for a truly
        empty source. Incomplete pages, source errors or changed totals raise an exception.

    Created: 2026-10
    易错点: AKShare 1.17.95 fails when this source adds columns. Keep native FSRQ/DWJZ/LJJZ
        and all unknown fields; never rename by position or convert NAV values/dates here.
        sourceLayer describes parsed JSON records, not the original HTTP byte stream.
    """
    lower = datetime.strptime(start_date, "%Y%m%d").date()
    upper = datetime.strptime(end_date, "%Y%m%d").date()
    if lower > upper or timeout_seconds <= 0:
        raise ValueError("Invalid NAV date range or deadline")
    params = {"fundCode": symbol, "pageIndex": 1, "pageSize": 200,
              "startDate": lower.isoformat(), "endDate": upper.isoformat()}
    headers = {"Referer": f"https://fundf10.eastmoney.com/jjjz_{symbol}.html",
               "User-Agent": "Mozilla/5.0"}
    deadline = monotonic() + timeout_seconds
    pages = []
    requests_sent = []
    records = []
    indexes = []
    expected_total = None
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("NAV source pagination exceeded its deadline")
        request_params = dict(params)
        response = get(NAV_URL, params=request_params, headers=headers, timeout=min(20, remaining))
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict) or body.get("ErrCode") not in (None, 0):
            raise ValueError("NAV source returned an error envelope")
        data = body.get("Data")
        rows = data.get("LSJZList") if isinstance(data, dict) else None
        total = body.get("TotalCount")
        if not isinstance(rows, list) or not isinstance(total, int) or total < 0:
            raise ValueError("NAV source returned an invalid page")
        if expected_total is None:
            expected_total = total
        if total != expected_total:
            raise ValueError("NAV record count changed during pagination; retry a complete collection")
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("NAV source returned a malformed record")
        requests_sent.append(request_params)
        pages.append({**body, "Data": {key: value for key, value in data.items() if key != "LSJZList"}})
        records.extend(rows)
        indexes.extend([[params["pageIndex"], position] for position in range(len(rows))])
        if len(records) == expected_total:
            break
        if not rows or len(records) > expected_total:
            raise ValueError("NAV pagination is incomplete or exceeds the advertised total")
        params["pageIndex"] += 1
    if not records:
        return None
    columns = list(dict.fromkeys(key for row in records for key in row))
    return {
        "schema": "eastmoney-fund-history-v1", "provider": "eastmoney",
        "dataset": "unit-nav-history", "symbol": symbol, "interval": "1d",
        "timezone": "Asia/Shanghai", "adjustment": "none", "currency": "CNY",
        "shareUnit": "fund-share", "sourceLayer": "upstream-json-records",
        "sourceUrl": NAV_URL, "requestParams": {"pages": requests_sent},
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "columns": columns, "index": indexes,
        "data": [[json_value(row.get(column)) for column in columns] for row in records],
        # All row fields are in columns/data; preserve source envelope metadata as well.
        "sourcePageMetadata": json_value(pages),
    }
