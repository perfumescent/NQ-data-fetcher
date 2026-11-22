"""Complete ETF source tables; acquisition only, with no price/date/row business cleaning."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
from importlib.metadata import version
import math
import multiprocessing
import re
from typing import Callable

import numpy as np
import pandas as pd


DATASETS = ("price-history", "unit-nav-history")


class SourceSDKError(RuntimeError):
    """An isolated SDK failure category, with no upstream response/credential logging."""


@dataclass(frozen=True)
class HistorySourceResult:
    """One independently retryable source result; failure never becomes an empty raw update."""

    symbol: str
    dataset: str
    table: dict | None
    error: str | None = None


def json_value(value):
    """Serialize source values only as required by strict JSON.

    Args:
        value: SDK cell/index/column/request value, including nested objects and missing scalars.
    Returns:
        A JSON value; non-finite/missing scalars become None, unsupported types raise TypeError.

    Created: 2026-10
    易错点: Dates keep their original offset or date-only representation; serialization must not
        drop missing rows, normalize market time zones, select columns or repair source values.
    """
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        return json_value(value.item())
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"Unsupported SDK value: {type(value).__name__}")


def serialize_sdk_table(
    frame: pd.DataFrame,
    *,
    symbol: str,
    dataset: str,
    request_params: dict,
    fetched_at: datetime | None = None,
    provider: str = "eastmoney",
) -> dict:
    """Represent every SDK table column, row and original index in the shared histories protocol.

    Args:
        frame: The complete nonempty DataFrame returned by the chosen SDK call, without mutation.
        symbol: Six-digit ETF code; dataset: price-history or unit-nav-history.
        request_params: Exact arguments passed to the SDK; fetched_at: aware collection time,
            or None to capture UTC now. This is not an upstream quote/NAV publication time.
        provider: eastmoney, or sina for its independent complete unadjusted price SDK table.
    Returns:
        Provider-versioned source table with columns/index/data and accurate SDK source metadata.

    Created: 2026-10
    易错点: This preserves the SDK output, not upstream HTTP fields already discarded by AKShare.
        Duplicate columns, duplicate indexes, unknown columns and null-valued rows stay intact.
    """
    if frame.empty:
        raise ValueError("An empty response must not be ingested as a successful table")
    if (dataset not in DATASETS or not re.fullmatch(r"\d{6}", symbol)
            or provider not in ("eastmoney", "sina") or (provider == "sina" and dataset != "price-history")):
        raise ValueError("Expected a six-digit ETF symbol and a supported source dataset")
    collected = fetched_at or datetime.now(timezone.utc)
    if collected.utcoffset() is None:
        raise ValueError("fetched_at must include an offset")
    function = "fund_etf_hist_sina" if provider == "sina" else (
        "fund_etf_hist_em" if dataset == "price-history" else "fund_etf_fund_info_em"
    )
    return {
        "schema": f"{provider}-fund-history-v1",
        "provider": provider,
        "dataset": dataset,
        "symbol": symbol,
        "interval": "1d",
        "timezone": "Asia/Shanghai",
        "adjustment": "none",
        "currency": "CNY",
        "shareUnit": "fund-share",
        "sourceLayer": "sdk-table",
        "sdk": {"name": "akshare", "version": version("akshare"), "function": function},
        "requestParams": json_value(request_params),
        "fetchedAt": collected.isoformat(),
        "columns": json_value(frame.columns.tolist()),
        "index": json_value(frame.index.tolist()),
        "data": json_value(list(frame.itertuples(index=False, name=None))),
    }


def _sdk_worker(connection, function_name: str, params: dict) -> None:
    """Run one unmodified SDK call in an isolated process with a parent-owned time limit.

    Args:
        connection: Child end of a one-way pipe; function_name: supported AKShare ETF function;
        params: Exact SDK arguments. No credentials or server access are passed to this worker.
    Returns:
        None; sends (True, DataFrame) or (False, exception type) and closes its pipe.

    Created: 2026-10
    易错点: AKShare uses requests calls without timeouts; global monkey patches would affect
        concurrent legacy jobs. A separate process bounds this source without changing them.
    """
    try:
        import akshare as ak
        connection.send((True, getattr(ak, function_name)(**params)))
    except Exception as exc:
        connection.send((False, type(exc).__name__))
    finally:
        connection.close()


def call_sdk(function_name: str, params: dict, timeout_seconds: float) -> pd.DataFrame:
    """Fetch a complete SDK table with a finite wall-clock deadline.

    Args:
        function_name: One of the two supported ETF SDK function names; params: SDK arguments;
        timeout_seconds: Positive total request/page deadline, including worker initialization.
    Returns:
        The unmodified DataFrame; raises TimeoutError/RuntimeError for unavailable sources.

    Created: 2026-10
    易错点: NAV pagination must finish completely; a timed-out partial table is never ingested.
        Pipe data is received before join so a large complete table cannot deadlock the worker.
    """
    if function_name not in ("fund_etf_hist_em", "fund_etf_fund_info_em", "fund_etf_hist_sina") or timeout_seconds <= 0:
        raise ValueError("Unsupported SDK call or deadline")
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_sdk_worker, args=(sender, function_name, params), daemon=True)
    process.start()
    sender.close()
    try:
        if not receiver.poll(timeout_seconds):
            raise TimeoutError("ETF SDK source exceeded its collection deadline")
        try:
            successful, result = receiver.recv()
        except EOFError as exc:
            raise RuntimeError("ETF SDK worker exited without a complete response") from exc
        if not successful:
            raise SourceSDKError(str(result))
        if not isinstance(result, pd.DataFrame):
            raise TypeError("ETF SDK returned an unexpected result type")
        return result
    finally:
        receiver.close()
        process.join(timeout=1)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)


def fetch_source_table(
    symbol: str,
    dataset: str,
    start_date: str,
    end_date: str,
    *,
    timeout_seconds: float = 180,
    sdk_call: Callable = call_sdk,
    nav_fetch: Callable | None = None,
) -> HistorySourceResult:
    """Request a complete price table or lossless HTTP unit-NAV table without cross-dataset fallback.

    Args:
        symbol: Six-digit ETF code; dataset: price-history or unit-nav-history; start_date/end_date:
            Inclusive YYYYMMDD source retention/recovery bounds, not a UI history window.
        timeout_seconds: Positive source deadline; sdk_call: injectable SDK adapter; nav_fetch:
            injectable lossless Eastmoney HTTP paginator, or None for the default implementation.
    Returns:
        Complete source table, or table=None plus an explicit empty/error category. Prices use
        Eastmoney first, then an independent Sina price SDK source if Eastmoney is unavailable.

    Created: 2026-10
    易错点: Sina's API accepts no date bounds; its full available table is preserved even during
        overlap collection. NAV is never a price fallback. The source layer/schema/provider tell
        Server which native columns to validate. Cross-provider conflicts must remain explicit;
        a source priority must not silently resolve them.
    """
    if not re.fullmatch(r"\d{6}", symbol) or dataset not in DATASETS:
        raise ValueError("Unsupported symbol or dataset")
    lower = datetime.strptime(start_date, "%Y%m%d").date()
    upper = datetime.strptime(end_date, "%Y%m%d").date()
    if lower > upper or timeout_seconds <= 0:
        raise ValueError("Invalid history range or deadline")
    if dataset == "unit-nav-history":
        if nav_fetch is None:
            from src.providers.eastmoney_nav import fetch_nav_table
            nav_fetch = fetch_nav_table
        try:
            table = nav_fetch(symbol, start_date, end_date, timeout_seconds=timeout_seconds)
            return HistorySourceResult(symbol, dataset, table, None if table else "empty_response")
        except Exception as exc:
            return HistorySourceResult(symbol, dataset, None, type(exc).__name__)
    failures = []
    for provider in ("eastmoney", "sina"):
        if provider == "eastmoney":
            function = "fund_etf_hist_em"
            params = {"symbol": symbol, "period": "daily", "start_date": start_date,
                      "end_date": end_date, "adjust": ""}
        else:
            function = "fund_etf_hist_sina"
            params = {"symbol": ("sh" if symbol.startswith(("5", "6")) else "sz") + symbol}
        try:
            frame = sdk_call(function, params, timeout_seconds)
            if frame.empty:
                failures.append(f"{provider}:empty_response")
                continue
            table = serialize_sdk_table(frame, symbol=symbol, dataset=dataset,
                                        request_params=params, provider=provider)
            return HistorySourceResult(symbol, dataset, table)
        except Exception as exc:
            category = str(exc) if isinstance(exc, SourceSDKError) else type(exc).__name__
            failures.append(f"{provider}:{category}")
    return HistorySourceResult(symbol, dataset, None, ";".join(failures))
