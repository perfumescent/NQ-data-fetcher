import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from ..core.cache import price_cache, chart_cache, indicator_cache, high52w_cache
from cachetools import cached


def _fast_info_value(fast, *names):
    """
    Read a yfinance fast_info value without assuming attribute access is safe.

    Args:
        fast: yfinance FastInfo-like object.
        *names: Candidate snake_case/camelCase keys or attributes.
    Returns:
        First non-empty value; None means the value is unavailable.

    Created: 2026-05
    易错点: Some yfinance properties lazily call quote metadata and can raise
    currentTradingPeriod KeyError; callers must be able to continue fallback.
    """
    for name in names:
        try:
            if hasattr(fast, "get"):
                value = fast.get(name)
                if not _is_missing(value):
                    return value
        except Exception:
            pass
        try:
            value = getattr(fast, name)
            if not _is_missing(value):
                return value
        except Exception:
            pass
    return None


def _is_missing(value) -> bool:
    """
    Decide whether a Yahoo numeric field is absent or unusable.

    Args:
        value: Any provider value, commonly None, NaN, or a numeric scalar.
    Returns:
        True when the value should be treated as missing.

    Created: 2026-05
    易错点: pandas.isna raises for some container-like objects, so keep it behind
    a broad guard and only use this helper for scalar quote fields.
    """
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _market_timestamp(info: dict | None, history_timestamp) -> datetime:
    """
    返回 Yahoo quote 对应的市场时间。

    Args:
        info: yfinance Ticker.info 字典；缺失 regularMarketTime 时允许为 None。
        history_timestamp: yfinance history 最后一根 K 线索引；未使用 history fallback 时为 None。
    Returns:
        timezone-naive UTC datetime；无法确认市场时间时返回当前 UTC。

    Created: 2026-05
    易错点: 不能直接用抓取时间作为行情时间；周末抓取会把周五收盘价伪造成周末 K 线，导致 RSI/MA 偏高。
    """
    if isinstance(info, dict):
        raw = info.get("regularMarketTime") or info.get("postMarketTime") or info.get("preMarketTime")
        if raw:
            try:
                return datetime.fromtimestamp(float(raw), tz=timezone.utc).replace(tzinfo=None)
            except Exception:
                pass
    if history_timestamp is not None:
        try:
            if hasattr(history_timestamp, "to_pydatetime"):
                value = history_timestamp.to_pydatetime()
            else:
                value = history_timestamp
            if getattr(value, "tzinfo", None) is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return value
        except Exception:
            pass
    return datetime.utcnow()


class YahooProvider:

    @staticmethod
    @cached(price_cache)
    def get_price(symbol: str) -> dict | None:
        """
        Fetch the latest Yahoo quote with layered fallbacks.

        Args:
            symbol: Yahoo Finance symbol such as ^NDX, ^TNX, NQ=F, or CNY=X.
        Returns:
            {"price": float, "changePct": float, "timestamp": datetime} when a valid
            price can be found; None means every quote/history source failed.

        Created: 2026-05
        易错点: yfinance fast_info may raise KeyError("currentTradingPeriod") for
        indices/volatility symbols; that must not prevent the history fallback.
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # 1. Try `info` first for Futures accuracy (NQ=F needs regularMarketPreviousClose)
            # fast_info often returns stale previous_close for futures (e.g. 25509 vs 25798)
            try:
                info = ticker.info
                price = info.get("regularMarketPrice") or info.get("currentPrice") or info.get("price")
                prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
            except:
                info = None
                price = None
                prev_close = None

            # 2. Fallback to fast_info if info failed or keys missing
            if price is None or prev_close is None:
                try:
                    fast = ticker.fast_info
                    if price is None:
                        price = _fast_info_value(fast, "last_price", "lastPrice")
                    if prev_close is None:
                        prev_close = _fast_info_value(fast, "previous_close", "previousClose")
                except Exception as e:
                    print(f"[Warn] Yahoo fast_info failed for {symbol}: {e}")

            # 3. Fallback to History
            history_timestamp = None
            if _is_missing(price) or _is_missing(prev_close):
                hist = ticker.history(period="5d")
                if not hist.empty:
                    history_timestamp = hist.index[-1]
                    if _is_missing(price):
                        price = float(hist.iloc[-1]["Close"])
                    if _is_missing(prev_close):
                        prev_close = float(hist.iloc[-2]["Close"]) if len(hist) > 1 else price
            
            if _is_missing(price):
                print(f"[Warn] Yahoo get_price {symbol}: no valid price")
                return None
            
            # Ensure floats
            price = float(price)
            prev_close = float(prev_close) if not _is_missing(prev_close) else price
            
            change_pct = (price - prev_close) / prev_close if prev_close else 0.0
            
            return {
                "price": price,
                "changePct": change_pct,
                "timestamp": _market_timestamp(info, history_timestamp)
            }
        except Exception as e:
            print(f"[Error] Yahoo get_price {symbol}: {e}")
            return None

    @staticmethod
    @cached(chart_cache)
    def get_intraday(symbol: str) -> list[dict]:
        try:
            ticker = yf.Ticker(symbol)
            # 1m interval, 1 day range
            hist = ticker.history(period="1d", interval="1m")
            
            if hist.empty:
                return []
                
            points = []
            # yfinance index is tz-aware datetime
            for t, row in hist.iterrows():
                points.append({
                    "t": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "p": float(row["Close"])
                })
            return points
        except Exception as e:
            print(f"[Error] Yahoo get_intraday {symbol}: {e}")
            return []

    @staticmethod
    @cached(chart_cache)
    def get_history(symbol: str, period: str = "1mo", interval: str = "1d") -> list[dict]:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                return []
                
            points = []
            for t, row in hist.iterrows():
                # For daily history, we usually want T00:00:00Z to verify clearly
                points.append({
                    "t": t.strftime("%Y-%m-%dT00:00:00Z"), 
                    "p": float(row["Close"])
                })
            return points
        except Exception as e:
            print(f"[Error] Yahoo get_history {symbol}: {e}")
            return []

    @staticmethod
    @cached(indicator_cache)
    def get_indicators(symbol: str) -> dict:
        """
        Get complex indicators (RSI, MA) using history.
        """
        # ... (implementation covered by TechnicalAnalysis in market_data service, 
        # but here we could provide raw history for it)
        # For now, MarketDataService handles the calculation using history from valid source.
        return {}

    @staticmethod
    @cached(price_cache)
    def get_year_change(symbol: str) -> float | None:
        """
        Get 52-week change percentage.
        Returns float (e.g. 0.25 for 25%), or None if failed.
        """
        try:
            ticker = yf.Ticker(symbol)
            # Try info first
            try:
                info = ticker.info
                if info and "52WeekChange" in info:
                    val = info["52WeekChange"]
                    if val is not None:
                        return float(val) 
            except:
                pass
                
            # Fallback to history calculation
            hist = ticker.history(period="1y")
            if not hist.empty and len(hist) > 200:
                start = float(hist.iloc[0]["Close"])
                end = float(hist.iloc[-1]["Close"])
                if start > 0:
                    return (end - start) / start
        except Exception as e:
            print(f"[Warn] Yahoo 52w change failed for {symbol}: {e}")
        return None
                
    @staticmethod
    @cached(price_cache)
    def get_month_change(symbol: str) -> float | None:
        """
        Get 1-month change percentage.
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1mo")
            if not hist.empty and len(hist) > 5:
                start = float(hist.iloc[0]["Close"])
                end = float(hist.iloc[-1]["Close"])
                if start > 0:
                    return (end - start) / start
        except Exception as e:
            print(f"[Warn] Yahoo 1m change failed for {symbol}: {e}")
        return None

    @staticmethod
    @cached(high52w_cache)
    def get_52w_high(symbol: str) -> float | None:
        """
        Get 52-week high directly from info to calculate drawdown.
        """
        try:
            ticker = yf.Ticker(symbol)
            try:
                info = ticker.info
                if info and "fiftyTwoWeekHigh" in info:
                    val = info["fiftyTwoWeekHigh"]
                    if val is not None:
                        return float(val) 
            except:
                pass
            
            # Fallback to history
            hist = ticker.history(period="1y")
            if not hist.empty:
                return float(hist["High"].max())
        except Exception as e:
            print(f"[Warn] Yahoo 52w high failed for {symbol}: {e}")
        return None

    @staticmethod
    @cached(indicator_cache)
    def get_daily_indicators(symbol: str) -> dict:
        """
        Calculates RSI, MA120 from daily history
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1y") # Need enough data for MA120 and MA200 (200 trading days > 9 months)
            
            if hist.empty:
                return {}
                
            close = hist["Close"]
            current_price = close.iloc[-1]
            
            # RSI (14)
            # Using pandas_ta for reliability
            import pandas_ta as ta
            rsi_series = ta.rsi(close, length=6)
            rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50.0
            
            # MA120
            ma120_series = ta.sma(close, length=120)
            ma120 = ma120_series.iloc[-1] if not ma120_series.empty else current_price
            ma_dev = (current_price - ma120) / ma120
            
            # MA200
            ma200_series = ta.sma(close, length=200)
            ma200 = ma200_series.iloc[-1] if not (ma200_series is None or ma200_series.empty) else current_price
            ma200_dev = (current_price - ma200) / ma200
            
            return {
                "rsi": float(rsi),
                "ma120": float(ma120),
                "ma120Deviation": float(ma_dev),
                "ma200": float(ma200),
                "ma200Deviation": float(ma200_dev)
            }
        except Exception as e:
            print(f"[Error] Yahoo get_daily_indicators {symbol}: {e}")
            return {}
