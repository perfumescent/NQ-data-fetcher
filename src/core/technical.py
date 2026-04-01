import pandas as pd
import pandas_ta as ta

class TechnicalAnalysis:
    @staticmethod
    def calculate_rsi(prices: list[float], period: int = 14) -> float:
        """
        Calculate RSI-14. Returns 50.0 if not enough data.
        """
        if len(prices) < period + 1:
            return 50.0
            
        series = pd.Series(prices)
        rsi_series = ta.rsi(series, length=period)
        
        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
            return 50.0
            
        return float(rsi_series.iloc[-1])

    @staticmethod
    def calculate_ma_deviation(prices: list[float], period: int = 120) -> tuple[float, float]:
        """
        Calculate MA120 and Deviation % ((Price - MA) / MA).
        Returns (ma_value, deviation_pct). 
        Returns (0.0, 0.0) if not enough data.
        """
        if len(prices) < period:
            return 0.0, 0.0
            
        series = pd.Series(prices)
        ma_series = ta.sma(series, length=period)
        
        if ma_series.empty or pd.isna(ma_series.iloc[-1]):
            return 0.0, 0.0
            
        ma_val = float(ma_series.iloc[-1])
        current_price = float(prices[-1])
        
        if ma_val == 0:
            return 0.0, 0.0
            
        dev_pct = (current_price - ma_val) / ma_val
        return ma_val, dev_pct
