from datetime import datetime
import json
import os
import concurrent.futures
from typing import List

from src.domain.models import (
    IndexData, IndexIndicators, ETFData, ETFIndicators, FundData, FundIndicators,
    IntradayPoint, IndicatorValue
)
from src.providers.yahoo import YahooProvider
from src.providers.akshare_api import AkShareProvider
from src.core.technical import TechnicalAnalysis

class BaseAssembler:
    @staticmethod
    def _now_iso():
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
    @staticmethod
    def _load_config(segment: str):
        # Helper to load funds.json
        try:
            # Structure: scripts/data_fetcher/funds.json
            # This file: scripts/data_fetcher/src/services/assemblers.py
            path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../funds.json"))
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(segment, [])
        except Exception as e:
            print(f"[Error] Loading config for {segment}: {e}")
            return []
            
    @staticmethod
    def _find_conf(code: str, list_conf: list):
        for item in list_conf:
            if item["code"] == code:
                return item
        return {}
        
    @staticmethod
    def calculate_period_return(history: List[dict], days_back: int) -> float | None:
        """
        Calculate return rate based on NAV history and calendar days.
        Formula: (NAV_now - NAV_old) / NAV_old
        Matches date strictly by calendar (same date N days ago) or finds nearest preceding trading day.
        
        Args:
            history: List of {"t": "YYYY-MM-DD", "p": float} (sorted by date asc)
            days_back: Number of calendar days to look back (e.g. 365 for 1Y)
            
        Returns:
            Return rate as float (e.g. 0.15 for 15%), or None if not enough data.
        """
        if not history or len(history) < 2:
            return None
            
        from datetime import datetime, timedelta
        
        try:
            # Latest data point
            latest = history[-1]
            t_now = datetime.strptime(latest["t"], "%Y-%m-%d")
            p_now = latest["p"]
            
            target_date = t_now - timedelta(days=days_back)
            
            # Find closest date (<= target_date)
            # Since history is sorted asc, we iterate backwards
            matching_entry = None
            
            # Optimization: binary search or just linear scan from end
            # Linear scan from end is fine for small N (< 1000)
            for i in range(len(history)-2, -1, -1):
                entry = history[i]
                t_entry = datetime.strptime(entry["t"], "%Y-%m-%d")
                
                # Precise math: we want the trading day that represents the state "days_back" ago.
                # If target_date is a trading day, use it.
                # If not, use the most recent trading day BEFORE it.
                if t_entry <= target_date:
                     matching_entry = entry
                     break
            
            # If we went too far back (e.g. data starts after target date), we can't calculate
            if matching_entry:
                p_old = matching_entry["p"]
                if p_old > 0:
                    return (p_now - p_old) / p_old
                    
            return None
            
        except Exception:
            return None

    @staticmethod
    def get_smart_returns(code: str, nav_history: List[dict]):
        """
        Get returns (1Y, 6M, 1M) using Hybrid Approach:
        1. Priority: Official Metadata from EastMoney (consistent with brokers).
        2. Fallback: Self-calculated from NAV history (robustness).
        
        Returns: (yearChange, sixMonthChange, monthChange) as floats (e.g 0.15), or None.
        """
        # 1. Try Official Metadata
        meta = AkShareProvider.get_fund_metadata(code)
        
        yc = meta.get("yearChange")
        smc = meta.get("sixMonthChange")
        mc = meta.get("oneMonthChange")
        
        # Helper to convert percent string/float to ratio
        # meta returns are typically in percent e.g. 15.5
        def parse_meta(val):
            try:
                if val is not None:
                    return float(val) / 100.0
            except:
                pass
            return None

        features = [parse_meta(yc), parse_meta(smc), parse_meta(mc)]
        
        # 2. Fallback Calculation
        # Map indices: 0->1Y(365), 1->6M(183), 2->1M(30)
        days_map = [365, 183, 30]
        
        final_results = []
        for i, val in enumerate(features):
            if val is not None:
                final_results.append(val)
            else:
                # Fallback
                calc = BaseAssembler.calculate_period_return(nav_history, days_map[i])
                final_results.append(calc)
                
        return final_results[0], final_results[1], final_results[2]

class IndexAssembler(BaseAssembler):
    @staticmethod
    def assemble(symbol: str = "^NDX") -> IndexData:
        # 1. Snapshot
        price_data = YahooProvider.get_price(symbol)
        if not price_data:
             # Fallback
             return IndexData(
                 symbol=symbol, displayName="NDX", shortName="纳指", 
                 price=0.0, changePct=0.0, updatedAt=BaseAssembler._now_iso(), 
                 dataQuality="nodata", indicators=IndexIndicators(updatedAt=BaseAssembler._now_iso(), dataQuality="nodata")
             )
        
        # 2. Intraday (Futures for T-30)
        target_hist = "NQ=F"
        full_hist = YahooProvider.get_history(target_hist, period="2mo", interval="1d")
        cutoff = -30 if len(full_hist) >= 30 else 0
        points = []
        for p in full_hist[cutoff:]:
            points.append(IntradayPoint(t=p["t"], p=p["p"]))
            
        # 3. Indicators
        # We reuse the logic from MarketDataService.get_index_indicators but return typed object
        # Parallel fetch for specialized fields
        def fetch_ticker(args):
            sym, key = args
            return key, YahooProvider.get_price(sym)

        targets = [("^TNX", "tnx"), ("^VXN", "vxn"), ("^VIX", "vix"), ("NQ=F", "fut")]
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for key, data in executor.map(fetch_ticker, targets):
                results[key] = data
                
        # Base Tech (RSI, MA)
        index_tech = YahooProvider.get_daily_indicators(symbol)
        
        rsi_val = index_tech.get("rsi", 50.0)
        ma_val = index_tech.get("ma120Deviation", 0.0)
        
        rsi_sub = "趋势中性"
        if rsi_val > 70: rsi_sub = "超买风险"
        elif rsi_val < 30: rsi_sub = "超卖反弹"
        elif rsi_val > 50: rsi_sub = "多头主导"
        else: rsi_sub = "空头主导"

        ma_sub = "靠近半年线"
        if ma_val > 0.15: ma_sub = "上行超买"
        elif ma_val > 0.03: ma_sub = "多头运行"
        elif ma_val < -0.10: ma_sub = "深度跌破"
        elif ma_val < -0.03: ma_sub = "失守均线"
        
        ma200_val = index_tech.get("ma200Deviation", 0.0)
        ma200_sub = "考验牛熊"
        if ma200_val > 0.15: ma200_sub = "极度飙升"
        elif ma200_val > 0.03: ma200_sub = "长线多头"
        elif ma200_val < -0.10: ma200_sub = "陷入熊市"
        elif ma200_val < -0.03: ma200_sub = "跌破牛熊分界线"
        
        # TNX
        tnx = results.get("tnx")
        tnx_val = tnx["price"] if tnx else 4.5
        tnx_chg = tnx["changePct"] if tnx else 0.0
        tnx_sub = "利率企稳"
        if tnx_chg > 0.01: tnx_sub = "利空科技"
        elif tnx_chg < -0.01: tnx_sub = "利好估值"
        
        # VXN & VIX
        vxn_data = results.get("vxn")
        vix_data = results.get("vix")
        
        vxn_val = vxn_data["price"] if vxn_data else 18.5
        vix_val = vix_data["price"] if vix_data else None
        
        vxn_sub_label = "情绪中性"
        if vxn_val >= 40:
            vxn_sub_label = "究极恐慌"
        elif vxn_val >= 30:
            vxn_sub_label = "极度恐慌"
        elif vxn_val >= 25:
            vxn_sub_label = "避险情绪"
        elif vxn_val >= 20:
            vxn_sub_label = "情绪中性"
        else:
            vxn_sub_label = "贪婪蔓延"
            
        vix_str = f" | VIX: {vix_val:.1f}" if vix_val else " | VIX: --"
        vxn_sub = vxn_sub_label + vix_str
        
        # Calculate bounded percentile for progress bar (min=12 representing extreme greed, max=40 representing extreme panic)
        vxn_pct = min(100, max(0, int((vxn_val - 12) / (40 - 12) * 100)))
        
        # FUT
        fut_data = results.get("fut")
        fut_chg = fut_data["changePct"] if fut_data else 0.0
        fut_sub = "盘前震荡"
        if fut_chg > 0.003: fut_sub = "盘前上涨"
        elif fut_chg < -0.003: fut_sub = "盘前下跌"

        # Drawdown calculation
        high52w = YahooProvider.get_52w_high(symbol)
        drawdown_val = 0.0
        drawdown_sub = "距52周最高"
        nav_price = price_data["price"]
        
        if high52w and high52w > 0:
            if nav_price >= high52w:
                drawdown_val = 0.0
                drawdown_sub = "创新高"
            else:
                drawdown_val = (nav_price - high52w) / high52w
        
        drawdown_dict = {
            "valuePct": drawdown_val,
            "high52w": high52w or nav_price,
            "subtitle": drawdown_sub,
            "isNewHigh": (nav_price >= high52w) if high52w else False
        }

        inds = IndexIndicators(
            updatedAt=BaseAssembler._now_iso(),
            dataQuality="good",
            rsi=IndicatorValue(value=rsi_val, percentile=50, subtitle=rsi_sub),
            ma120Deviation=IndicatorValue(valuePct=ma_val, ma120=index_tech.get("ma120", 0.0), subtitle=ma_sub),
            ma200Deviation=IndicatorValue(valuePct=ma200_val, ma120=index_tech.get("ma200", 0.0), subtitle=ma200_sub),
            yield10y=IndicatorValue(value=tnx_val, change=tnx_chg, subtitle=tnx_sub),
            vxn=IndicatorValue(value=vxn_val, percentile=vxn_pct, subtitle=vxn_sub),
            futQqq={"retPct": fut_chg, "subtitle": fut_sub},
            pe=IndicatorValue(value=32.5, percentile=85, subtitle="估值偏高"),
            drawdown52w=drawdown_dict
        )
        
        return IndexData(
            symbol=symbol,
            displayName="NDX" if symbol == '^NDX' else symbol,
            shortName="纳指",
            price=price_data["price"],
            changePct=price_data["changePct"],
            updatedAt=price_data["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            dataQuality="good",
            intraday=points,
            indicators=inds
        )

class ETFAssembler(BaseAssembler):
    @staticmethod
    def assemble(code: str) -> ETFData:
        conf = BaseAssembler._find_conf(code, BaseAssembler._load_config("etf"))
        name = conf.get("name", code)
        
        # Dynamic Inception Date
        inception_date = conf.get("inceptionDate")
        if not inception_date:
            inception_date = AkShareProvider.get_fund_inception_date(code, is_etf=True)
        
        # 1. Snapshot & Realtime
        etf_data = AkShareProvider.get_etf_realtime(code)
        
        
        price = etf_data.get("price", 0.0) if etf_data else 0.0
        change_pct = etf_data.get("changePct", 0.0) if etf_data else 0.0
        
        # New Fields: Volume, Amount
        dt_volume = etf_data.get("volume") if etf_data else None
        dt_amount = etf_data.get("turnoverAmount") if etf_data else None
        
        # New Field: Fund Size (fetch explicitly, though smart_returns calls it too, it's cached)
        meta_data = AkShareProvider.get_fund_metadata(code)
        fund_size = meta_data.get("fundSize")
        
        # 2. History (Sina) for Chart & Indicators
        history = AkShareProvider.get_etf_history_sina(code)
        points = []
        prices = []
        if history:
            cutoff = -30 if len(history) >= 30 else 0
            for pt in history[cutoff:]:
                points.append(IntradayPoint(t=pt["t"] + "T00:00:00Z", p=pt["p"]))
            prices = [pt["p"] for pt in history]
        
        # 2.1 Calculate Returns using Smart Hybrid Approach
        # Priority: Official Meta > NAV Calc
        nav_history = AkShareProvider.get_nav_history_em(code)
        year_change, six_month_change, month_change = BaseAssembler.get_smart_returns(code, nav_history)

        # Fallback to Sina history if NAV/Meta failed entirely
        if year_change is None and history and len(history) >= 240:
             # Logic from before
             p_now = history[-1]["p"]
             p_old = history[0]["p"]
             if p_old > 0: year_change = (p_now - p_old) / p_old


        # 3. Indicators
        rsi_val = TechnicalAnalysis.calculate_rsi(prices, period=6)
        rsi_sub = "趋势中性"
        if rsi_val > 70: rsi_sub = "超买风险"
        elif rsi_val < 30: rsi_sub = "超卖反弹"
        elif rsi_val > 50: rsi_sub = "多头主导"
        
        ma_val, ma_dev_raw = TechnicalAnalysis.calculate_ma_deviation(prices, period=120)
        ma_sub = "靠近半年线"
        if ma_dev_raw > 0.15: ma_sub = "上行超买"
        elif ma_dev_raw > 0.03: ma_sub = "多头运行"
        elif ma_dev_raw < -0.10: ma_sub = "深度跌破"
        elif ma_dev_raw < -0.03: ma_sub = "失守均线"
        
        ma200_val, ma200_dev_raw = TechnicalAnalysis.calculate_ma_deviation(prices, period=200)
        ma200_sub = "考验牛熊"
        if ma200_dev_raw > 0.15: ma200_sub = "极度飙升"
        elif ma200_dev_raw > 0.03: ma200_sub = "长线多头"
        elif ma200_dev_raw < -0.10: ma200_sub = "深陷熊市"
        elif ma200_dev_raw < -0.03: ma200_sub = "跌破年线"
        
        prem = etf_data.get("premiumRate", 0.0) if etf_data else 0.0
        turn = etf_data.get("turnoverRate", 0.0) if etf_data else 0.0
        
        prem_sub = "价格合理"
        if prem > 0.01: prem_sub = "溢价过高"
        elif prem < -0.01: prem_sub = "折价机会"
        
        turn_sub = "交易清淡"
        if turn > 10: turn_sub = "交易拥挤"
        elif turn > 5: turn_sub = "交投活跃"
        
        inds = ETFIndicators(
            updatedAt=BaseAssembler._now_iso(),
            dataQuality="good",
            rsi=IndicatorValue(value=rsi_val, percentile=50, subtitle=rsi_sub),
            ma120Deviation=IndicatorValue(valuePct=ma_dev_raw, ma120=ma_val, subtitle=ma_sub),
            ma200Deviation=IndicatorValue(valuePct=ma200_dev_raw, ma120=ma200_val, subtitle=ma200_sub),
            premiumRate=IndicatorValue(value=prem, subtitle=prem_sub),
            turnoverRate=IndicatorValue(value=turn, subtitle=turn_sub)
        )
        
        return ETFData(
            symbol=code,
            fundSize=fund_size,
            volume=dt_volume,
            turnoverAmount=dt_amount,
            displayName=name,
            shortName=code,
            price=price,
            changePct=change_pct,
            yearChange=year_change,
            sixMonthChange=six_month_change,
            fee=conf.get("fee"),
            inceptionDate=inception_date,
            updatedAt=BaseAssembler._now_iso(),
            dataQuality="good" if etf_data else "stale",
            premiumRate=prem,
            turnoverRate=turn,
            intraday=points,
            indicators=inds
        )

class FundAssembler(BaseAssembler):
    @staticmethod
    def assemble(code: str) -> FundData:
        conf = BaseAssembler._find_conf(code, BaseAssembler._load_config("fund"))
        name = conf.get("name", code)
        
        # Dynamic Inception Date
        inception_date = conf.get("inceptionDate")
        if not inception_date:
            inception_date = AkShareProvider.get_fund_inception_date(code, is_etf=False)
        
        # 1. Estimate
        est_data = AkShareProvider.get_fund_estimate_direct(code)
        
        # 2. History
        history = AkShareProvider.get_fund_history_em(code)
        points = []
        if history:
            cutoff = -30 if len(history) >= 30 else 0
            for pt in history[cutoff:]:
                points.append(IntradayPoint(t=pt["t"] + "T00:00:00Z", p=pt["p"]))
                
        # 3. Metadata
        meta = AkShareProvider.get_fund_metadata(code)
        
        # 4. Indicators
        # quota：优先取接口动态值，回退到 funds.json 静态配置，最终兜底 "--"
        quota_val = meta.get("quota") or conf.get("quota") or "--"
        
        dev = float(conf.get("trackingError", 0.0))
        dev_sub = "误差可控"
        if dev < -0.01: dev_sub = "大幅跑输"
        elif dev > 0.01: dev_sub = "小幅跑赢"
        
        usd = YahooProvider.get_price("CNY=X")
        u_val = usd["price"] if usd else 7.25
        u_sub = "汇率波动"
        
        mdd_val = 0.0
        if history:
            import numpy as np
            navs = [item["p"] for item in history[-250:]]
            if navs:
                navs_arr = np.array(navs)
                acc_max = np.maximum.accumulate(navs_arr)
                with np.errstate(divide='ignore', invalid='ignore'):
                    dds = (navs_arr - acc_max) / acc_max
                    mdd_val = float(abs(np.min(dds)))
                    
        mdd_sub = "中等回撤"
        if mdd_val > 0.10: mdd_sub = "回撤较大"
        elif mdd_val < 0.01: mdd_sub = "控制惊人"

        inds = FundIndicators(
             updatedAt=BaseAssembler._now_iso(),
             dataQuality="good",
             trackingDeviation1m=IndicatorValue(valuePct=dev, subtitle=dev_sub),
             quota={"valueStr": quota_val, "subtitle": "单日限购"},
             usdCny=IndicatorValue(value=u_val, subtitle=u_sub),
             maxDrawdown=IndicatorValue(value=mdd_val, subtitle=mdd_sub)
        )
        
        price = est_data.get("estNav", 0.0) if est_data else 0.0
        # If no estimate, use last close
        if price == 0 and history:
            price = history[-1]["p"]
            
        change_pct = est_data.get("changePct", 0.0) if est_data else 0.0
        
        
        
        # Calculate Returns using Smart Hybrid Approach
        # We reuse 'history' variable which already has NAV data from get_fund_history_em
        yc, smc, mc = BaseAssembler.get_smart_returns(code, history)
        
        # Fund Size from Metadata
        fund_size = meta.get("fundSize")

        return FundData(
            symbol=code,
            fundSize=fund_size,
            displayName=name,
            shortName="场外基金",
            price=price,
            changePct=change_pct,
            yearChange=yc,
            sixMonthChange=smc,
            monthChange=mc,
            fee=conf.get("fee"),
            quota=meta.get("quota") or conf.get("quota"),
            trackingError=conf.get("trackingError"),
            inceptionDate=inception_date,
            updatedAt=est_data.get("updatedAt", BaseAssembler._now_iso()) if est_data else BaseAssembler._now_iso(),
            dataQuality="good" if est_data else "stale",
            intraday=points,
            indicators=inds
        )
