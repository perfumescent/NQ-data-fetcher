import yfinance as yf
import pandas as pd
from datetime import datetime
from ..core.cache import price_cache, chart_cache, indicator_cache, high52w_cache
from cachetools import cached

class YahooProvider:

    @staticmethod
    @cached(price_cache)
    def get_price(symbol: str) -> dict | None:
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
                fast = ticker.fast_info
                price = fast.last_price
                prev_close = fast.previous_close

            # 3. Fallback to History
            if price is None or pd.isna(price):
                hist = ticker.history(period="2d")
                if not hist.empty:
                     price = float(hist.iloc[-1]["Close"])
                     if prev_close is None or pd.isna(prev_close):
                         prev_close = float(hist.iloc[-2]["Close"]) if len(hist) > 1 else price
            
            # Ensure floats
            price = float(price) if price else 0.0
            prev_close = float(prev_close) if prev_close else 0.0
            
            change_pct = (price - prev_close) / prev_close if prev_close else 0.0
            
            return {
                "price": price,
                "changePct": change_pct,
                "timestamp": datetime.utcnow()
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
