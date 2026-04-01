from pydantic import BaseModel, Field
from typing import List, Optional, Union

# --- Shared Models ---

class IndicatorValue(BaseModel):
    value: Optional[float] = None
    valuePct: Optional[float] = None
    subtitle: str
    percentile: Optional[float] = None
    change: Optional[float] = None
    ma120: Optional[float] = None
    mae: Optional[float] = None

class TechnicalIndicators(BaseModel):
    updatedAt: str
    dataQuality: str
    rsi: Optional[IndicatorValue] = None
    ma120Deviation: Optional[IndicatorValue] = None
    ma200Deviation: Optional[IndicatorValue] = None
    
class IntradayPoint(BaseModel):
    t: str # ISO8601
    p: float

# --- Index Models ---

class IndexIndicators(TechnicalIndicators):
    yield10y: Optional[IndicatorValue] = None
    vxn: Optional[IndicatorValue] = None
    futQqq: Optional[dict] = None
    pe: Optional[IndicatorValue] = None
    drawdown52w: Optional[dict] = None

class IndexData(BaseModel):
    # Snapshot
    symbol: str
    displayName: str
    shortName: str
    price: float
    changePct: float
    updatedAt: str
    dataQuality: str
    
    # Chart
    intraday: List[IntradayPoint] = []
    
    # Indicators
    indicators: IndexIndicators

# --- ETF Models ---

class ETFIndicators(TechnicalIndicators):
    premiumRate: Optional[IndicatorValue] = None
    turnoverRate: Optional[IndicatorValue] = None

class ETFData(BaseModel):
    # Snapshot
    symbol: str
    displayName: str
    shortName: str
    price: float
    changePct: float
    yearChange: Optional[float] = None
    sixMonthChange: Optional[float] = None
    monthChange: Optional[float] = None
    fee: Optional[float] = None
    inceptionDate: Optional[str] = None
    updatedAt: str
    dataQuality: str
    
    # Chart
    intraday: List[IntradayPoint] = []
    
    # Indicators
    indicators: ETFIndicators
    
    # Snapshot extras
    premiumRate: Optional[float] = None
    turnoverRate: Optional[float] = None
    fundSize: Optional[float] = None # In 100M (亿元)
    volume: Optional[float] = None   # Share count
    turnoverAmount: Optional[float] = None # Currency value

# --- Fund Models ---

class FundIndicators(TechnicalIndicators):
    trackingDeviation1m: Optional[IndicatorValue] = None
    quota: Optional[dict] = None
    usdCny: Optional[IndicatorValue] = None
    maxDrawdown: Optional[IndicatorValue] = None

class FundData(BaseModel):
    # Snapshot
    symbol: str
    displayName: str
    shortName: str
    price: float
    changePct: float
    yearChange: Optional[float] = None
    sixMonthChange: Optional[float] = None
    monthChange: Optional[float] = None
    fundSize: Optional[float] = None # In 100M
    fee: Optional[float] = None
    quota: Optional[str] = None
    trackingError: Optional[float] = None
    inceptionDate: Optional[str] = None
    updatedAt: str
    dataQuality: str
    
    # Chart
    intraday: List[IntradayPoint] = []
    
    # Indicators
    indicators: FundIndicators

# --- Unified Wrapper ---

class UnifiedMarketData(BaseModel):
    index: IndexData
    etfs: List[ETFData]
    funds: List[FundData]
    updatedAt: str
