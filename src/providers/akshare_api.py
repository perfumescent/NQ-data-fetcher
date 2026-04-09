import akshare as ak
import pandas as pd
from datetime import datetime
from ..core.cache import price_cache, chart_cache, fund_cache, fund_history_cache
from cachetools import cached
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import json

# Setup a robust session with Retries
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Common Headers for scraping to avoid blocks (especially on Cloud IPs)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://finance.sina.com.cn", 
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

class AkShareProvider:
    @staticmethod
    @cached(price_cache)
    def get_etf_realtime(code: str) -> dict | None:
        """
        Get ETF realtime price (Spot).
        Primary Source: Yahoo Finance (Stable)
        Secondary: AkShare (Metadata)
        """
    @staticmethod
    @cached(price_cache)
    def get_etf_realtime(code: str) -> dict | None:
        """
        Get ETF realtime price (Spot) & IOPV & Turnover.
        Source: Sina Finance (Price + NAV).
        Turnover: EastMoney (f168).
        """
        # Sina code format for Price
        market_prefix = "sh" if code.startswith("5") else "sz"
        sina_stock = f"{market_prefix}{code}"    # e.g. sh513100
        # Sina code format for Fund Info (NAV/IOPV)
        sina_fund = f"f_{code}"                 # e.g. f_513100
        
        url = f"http://hq.sinajs.cn/list={sina_stock},{sina_fund}"
        
        price = None
        change_pct = 0.0
        iopv = None
        
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                lines = r.text.split('\n')
                
                # 1. Parse Price Line
                # var hq_str_sh513100="Name,Open,PrevClose,Price,..."
                stock_line = next((l for l in lines if sina_stock in l), None)
                volume = None # Shares
                amount = None # Currency
                
                if stock_line and '="' in stock_line:
                    content = stock_line.split('="')[1].strip('";')
                    parts = content.split(',')
                    if len(parts) > 30:
                        # Index 3: Price, Index 2: PrevClose
                        price_val = float(parts[3])
                        prev_close = float(parts[2])
                        
                        # Handle Suspended/Auction (Price=0)
                        if price_val == 0 and prev_close > 0:
                            price_val = prev_close
                            
                        price = price_val
                        if prev_close > 0:
                            change_pct = (price - prev_close) / prev_close
                        
                        # Index 8: Volume (Shares), Index 9: Amount (CNY)
                        try:
                            volume = float(parts[8])
                            amount = float(parts[9])
                        except: pass
                
                # 2. Parse Fund Info Line (for IOPV)
                # var hq_str_f_513100="Name,NAV,AccumNAV,IOPV,Date,..."
                # Verified by probe: Index 3 is IOPV (Realtime Estimate)
                fund_line = next((l for l in lines if sina_fund in l), None)
                if fund_line and '="' in fund_line:
                    content = fund_line.split('="')[1].strip('";')
                    parts = content.split(',')
                    if len(parts) > 3:
                        try:
                            val = float(parts[3])
                            if val > 0:
                                iopv = val
                        except:
                            pass
                                
        except Exception as e:
            print(f"[WARN] Sina fetch failed: {e}")
            volume = None
            amount = None

        # If Sina failed, return None to signal "No Data" rather than sending default/fake data
        if price is None:
            return None

        # 3. Calculate Premium Rate
        premium = 0.0
        if iopv and iopv > 0:
            premium = (price - iopv) / iopv

        # 4. Fetch Turnover (EastMoney)
        turnover = 0.0
        try:
            secid = f"1.{code}" if code.startswith("5") else f"0.{code}"
            em_url = "https://push2.eastmoney.com/api/qt/stock/get"
            em_params = {
                "secid": secid,
                "fields": "f168", # Turnover Rate
                "fltt": "2",
                "invt": "2"
            }
            # Use HEADERS but override Referer if needed (EastMoney doesn't strictly need it, but User-Agent is critical)
            r = session.get(em_url, params=em_params, headers=HEADERS, timeout=10)
            data = r.json()
            # f168 is percentage value e.g. 3.95 means 3.95%
            if data and data.get("data") and "f168" in data["data"]:
                val = data["data"]["f168"]
                if val != "-":
                    turnover = float(val)
        except Exception as e:
            # Silence warning if it's a common timeout in GH Action, or log as debug
            print(f"[DEBUG] EB Turnover fetch failed for {code}: {e}")
             
        return {
            "price": price,
            "changePct": change_pct,
            "premiumRate": premium,
            "turnoverRate": turnover,
            "volume": volume,
            "turnoverAmount": amount,
            "timestamp": datetime.utcnow()
        }

    @staticmethod
    @cached(chart_cache)
    def get_etf_intraday(code: str) -> list[dict]:
        """
        Get ETF intraday chart.
        Primary: Yahoo Finance
        """
        import yfinance as yf
        suffix = ".SS" if code.startswith("5") else ".SZ"
        symbol = f"{code}{suffix}"
        
        try:
            ticker = yf.Ticker(symbol)
            # Yahoo Chart 1d 1m
            hist = ticker.history(period="1d", interval="1m")
            if not hist.empty:
                points = []
                for t, row in hist.iterrows():
                    points.append({
                        "t": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "p": float(row["Close"])
                    })
                return points
        except Exception as e:
             # Only warn if it looked like an ETF code (5xxxxx or 15xxxx)
             if code.startswith("5") or code.startswith("1"):
                 print(f"[WARN] Yahoo CN Chart fetch failed for {symbol}: {e}")

        # Special handling for Funds (00xxxx) which don't have real-time charts usually
        # Loop back to AkShare (Mock for now as AkShare Intraday usually for ETFs)
        if code.startswith("0"):
             # It's an open-ended fund, return mock chart or estimated curve
             # AkShare fund_etf_hist_min_em likely fails for 000834
             print(f"[INFO] Fund {code} detected, returning mock intraday curve")
             return AkShareProvider._get_mock_chart()

        # Fallback to AkShare logic (if we fix the method name) or Mock
        try:
            print(f"[DEBUG] Fetching AkShare ETF Intraday for {code}...")
            df = ak.fund_etf_hist_min_em(symbol=code)
            points = []
            for _, row in df.iterrows():
                t_str = str(row["timestamp"])
                try:
                    dt = pd.to_datetime(t_str)
                    t_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    t_iso = t_str
                points.append({
                    "t": t_iso,
                    "p": float(row["close"])
                })
            return points
        except Exception as e:
            print(f"[Error] AkShare get_etf_intraday {code}: {e}")
            print(f"[INFO] Using Fallback Mock Chart for {code}")
            # Mock Chart
            import math
            import pandas as pd
            mock_points = []
            base = 1.240
            now = datetime.now()
            for i in range(240): # 4 hours
                t = (now - pd.Timedelta(minutes=240-i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                p = base + math.sin(i/20) * 0.01 + (i*0.0001)
                mock_points.append({"t": t, "p": p})
            return mock_points

    @staticmethod
    @cached(fund_cache)
    def get_fund_nav_history(code: str) -> pd.DataFrame:
        """
        Get off-exchange fund NAV history for tracking error calc.
        """
        try:
            df = ak.fund_open_fund_info_em(fund=code, indicator="单位净值走势")
            # Columns: 净值日期, 单位净值, 日增长率
            return df
        except Exception as e:
            print(f"[Error] AkShare get_fund_nav_history {code}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _get_mock_chart() -> list[dict]:
        import math
        mock_points = []
        base = 1.240 # Arbitrary base
        now = datetime.now()
        for i in range(240): # 4 hours
            t = (now - pd.Timedelta(minutes=240-i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            p = base + math.sin(i/20) * 0.01 + (i*0.0001)
            mock_points.append({"t": t, "p": p})
        return mock_points

    @staticmethod
    @cached(price_cache)
    def get_fund_estimate_direct(code: str) -> dict | None:
        """
        Get Fund Estimate directly from EastMoney JS API.
        Returns: { "estNav": float, "changePct": float, "nav": float, "valDate": str, "name": str }
        """
        
        url = f"http://fundgz.1234567.com.cn/js/{code}.js"
        
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            # ... (rest of implementation unchanged)
            if r.status_code == 200:
                text = r.text.strip()
                # Parse JSONP: jsonpgz({...});
                if text.startswith("jsonpgz(") and text.endswith(");"):
                    json_str = text[8:-2]
                    data = json.loads(json_str)
                    
                    est_nav = float(data.get("gsz", 0))
                    change_pct = float(data.get("gszzl", 0)) / 100.0
                    nav = float(data.get("dwjz", 0))
                    name = data.get("name", "")
                    
                    return {
                        "estNav": est_nav,
                        "changePct": change_pct,
                        "nav": nav,
                        "name": name,
                        "updatedAt": AkShareProvider._parse_iso(data.get("gztime")),
                        "navDate": data.get("jzrq")
                    }
        except Exception as e:
            print(f"[WARN] EastMoney Fund Estimate fetch failed for {code}: {e}")
            
        return None

    @staticmethod
    def _parse_iso(date_str: str) -> str:
        """
        Convert 'YYYY-MM-DD HH:MM' to 'YYYY-MM-DDTHH:MM:SSZ'
        """
        if not date_str:
            return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            # Try full format
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            # Fallback or already ISO
            return date_str

    @staticmethod
    def _parse_quota(sgzt: str, maxsg, code: str = "") -> str | None:
        """
        将 FundMNBasicInformation 的 SGZT + MAXSG 转为可展示字符串。
        返回 None 表示无法解析，调用方应回退到静态配置。

        Args:
            sgzt: 申购状态字符串，如 "限大额" / "暂停申购" / "正常"
            maxsg: 单日最大申购额（元），如 10000.0
            code: 基金代码，仅用于日志

        Returns:
            格式化后的限购字符串（"1万" / "暂停" / "正常" 等），或 None（无法识别）

        Created: 2025-11
        易错点: MAXSG 单位是元，不是万元；场内 ETF 的 SGZT 通常为"场内交易"，此处返回 None 属正常。
        """
        sgzt_str = str(sgzt).strip() if sgzt and str(sgzt) != "--" else ""

        # 暂停申购：优先判断，忽略 MAXSG
        if "暂停" in sgzt_str:
            print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → 暂停")
            return "暂停"

        # 限大额：解析 MAXSG 数值
        if maxsg is not None and str(maxsg).strip() not in ("--", "", "None"):
            try:
                val = float(maxsg)
                if val > 0:
                    if val >= 100_000_000:
                        result = f"{val / 100_000_000:.0f}亿"
                    elif val >= 10_000:
                        result = f"{val / 10_000:.0f}万"
                    else:
                        result = f"{int(val)}元"
                    print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → {result}")
                    return result
            except (ValueError, TypeError):
                pass

        # 正常申购
        if "正常" in sgzt_str:
            print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → 正常")
            return "正常"

        # 场内交易 / 其他无法识别的状态 → 返回 None
        print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → None (unrecognized)")
        return None

    @staticmethod
    @cached(fund_cache)
    def _fetch_fund_info_fallback(code: str) -> dict:
        """
        Fallback API: EastMoney Mobile API (FundMNBasicInformation)
        Returns dict with keys: "fundSize" (亿元), "inceptionDate" (YYYY-MM-DD), "quota" (str)
        """
        url = f"https://fundmobapi.eastmoney.com/FundMNewApi/FundMNBasicInformation?FCODE={code}&deviceid=1&plat=Iphone&product=EFund&version=6.6.6"
        res = {}
        try:
             r = session.get(url, headers=HEADERS, timeout=15)
             data = r.json()
             if data and "Datas" in data:
                 info = data["Datas"]
                 
                 # Size
                 end_nav = info.get("ENDNAV") # Raw val in RMB
                 if end_nav:
                     try:
                         res["fundSize"] = float(end_nav) / 100000000.0
                     except: pass
                     
                 # Inception Date
                 estab_date = info.get("ESTABDATE")
                 if estab_date and str(estab_date) != "NaT":
                     res["inceptionDate"] = str(estab_date).strip()

                 # Quota（仅场外基金有意义；场内 ETF 的 SGZT 为"场内交易"，_parse_quota 会返回 None）
                 quota_str = AkShareProvider._parse_quota(info.get("SGZT"), info.get("MAXSG"), code)
                 if quota_str is not None:
                     res["quota"] = quota_str

        except Exception as e:
            print(f"[WARN] Fallback Info fetch failed for {code}: {e}")
        return res

    @staticmethod
    def get_fund_inception_date(code: str, is_etf: bool = False) -> str | None:
        """
        Get inception date dynamically.
        Priority 1: EastMoney History / Xueqiu
        Priority 2: EastMoney Mobile API (Fallback)
        """
        date_str = None
        try:
            if is_etf:
                # Priority 1: ETF History
                df = ak.fund_etf_fund_info_em(fund=code)
                if not df.empty:
                    if "净值日期" in df.columns:
                        first_date = df["净值日期"].iloc[0]
                        if not (pd.isna(first_date) or str(first_date) == "NaT"):
                            if isinstance(first_date, pd.Timestamp):
                                date_str = first_date.strftime("%Y-%m-%d")
                            else:
                                date_str = str(first_date).split(" ")[0]
            else:
                # Priority 1: Fund Xueqiu
                df = ak.fund_individual_basic_info_xq(symbol=code)
                if not df.empty:
                    row = df[df['item'] == '成立时间']
                    if not row.empty:
                        date_str = str(row.iloc[0]['value'])
        except Exception as e:
             pass # Fallback
             
        # Priority 2: Fallback
        if not date_str:
            fallback = AkShareProvider._fetch_fund_info_fallback(code)
            date_str = fallback.get("inceptionDate")

        return date_str

    @staticmethod
    @cached(fund_cache)
    def get_fund_metadata(code: str) -> dict:
        """
        Get extra fund metadata (Returns, Size, etc) from EastMoney.
        Returns: { "yearChange": float, "sixMonthChange": float, "fundSize": float (亿元) }
        """
        url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        res = {}
        
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            # ... (rest of pingzhong parsing) ...
            if r.status_code == 200:
                text = r.text
                
                # Extract syl_1n
                m1 = re.search(r'var syl_1n\s*=\s*"([^"]+)"', text)
                if m1: res["yearChange"] = float(m1.group(1))
                    
                # Extract syl_6y
                m2 = re.search(r'var syl_6y\s*=\s*"([^"]+)"', text)
                if m2: res["sixMonthChange"] = float(m2.group(1))
            
                # Extract syl_1y
                m3 = re.search(r'var syl_1y\s*=\s*"([^"]+)"', text)
                if m3: res["oneMonthChange"] = float(m3.group(1))
                    
                # Extract Fund Size
                m4 = re.search(r'var Data_fluctuationScale\s*=\s*(.+?);', text, re.DOTALL)
                if m4:
                    try:
                        json_str = m4.group(1)
                        data = json.loads(json_str) 
                        if data and "series" in data:
                            series = data["series"]
                            if series and isinstance(series, list) and len(series) > 0:
                                last_item = series[-1]
                                if isinstance(last_item, dict):
                                    res["fundSize"] = float(last_item.get("y", 0))
                    except: pass

        except Exception as e:
            print(f"[WARN] Fund metadata fetch failed for {code}: {e}")
            
        # Fallback API（有缓存，不产生额外网络请求）：补全 fundSize 与 quota
        fallback = AkShareProvider._fetch_fund_info_fallback(code)
        if not res.get("fundSize") and fallback.get("fundSize"):
            res["fundSize"] = fallback["fundSize"]
        if fallback.get("quota"):
            res["quota"] = fallback["quota"]

        print(f"[Metadata] {code} final quota={res.get('quota')!r}")
        return res

    @staticmethod
    @cached(fund_history_cache)  # Use separate cache to avoid key collision with get_fund_metadata
    def get_fund_history_em(code: str) -> list[dict]:
        """
        Get Fund Historical NAV from EastMoney Pingzhong Data.
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
        Using fund_cache (1 hour) as NAV only updates daily/weekly.
        """
        url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
            if r.status_code == 200:
                text = r.text
                match = re.search(r"var Data_netWorthTrend = (\[.*?\]);", text, re.DOTALL)
                if match:
                    json_str = match.group(1)
                    try:
                        data = json.loads(json_str) 
                    except:
                        json_str = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', json_str)
                        data = json.loads(json_str)

                    result = []
                    for item in data:
                        ts_ms = item.get("x")
                        nav = item.get("y")
                        if ts_ms and nav is not None:
                            try:
                                dt = datetime.fromtimestamp(ts_ms / 1000.0)
                                t_str = dt.strftime("%Y-%m-%d")
                                result.append({"t": t_str, "p": float(nav)})
                            except:
                                pass
                            
                    return result

        except Exception as e:
            print(f"[WARN] Pingzhong fetch failed: {e}")
            
        return []

    @staticmethod
    @cached(fund_history_cache)
    def get_nav_history_em(code: str) -> list[dict]:
        """
        Get NAV History for both ETF and Fund from EastMoney.
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
        This effectively reuses the logic of get_fund_history_em but is named for clarity
        and potential future divergence if needed.
        """
        return AkShareProvider.get_fund_history_em(code)

    @staticmethod
    @cached(chart_cache)
    def get_etf_history_sina(code: str) -> list[dict]:
        """
        Get ETF Historical Data from Sina (via AkShare).
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
        """
        import akshare as ak
        import pandas as pd
        
        try:
            # Need market prefix for Sina. 513100 -> sh513100
            symbol = f"sh{code}" if code.startswith("5") or code.startswith("6") else f"sz{code}"
            
            df = ak.fund_etf_hist_sina(symbol=symbol)
            if not df.empty:
                # df columns: date, open, high, low, close, volume
                # Sort by date just in case
                # df['date'] is object or datetime? AkShare returns object usually
                
                # Take last 250 days (approx 1 year) to ensure enough for MA120
                df = df.tail(250)
                
                result = []
                for _, row in df.iterrows():
                    # row['date'] format 2025-12-08
                    t_str = str(row['date']) 
                    # Ensure format YYYY-MM-DD. Sometimes it is datetime
                    if isinstance(row['date'], pd.Timestamp):
                        t_str = row['date'].strftime("%Y-%m-%d")
                    
                    result.append({
                        "t": t_str,
                        "p": float(row['close'])
                    })
                return result
        except Exception as e:
            print(f"[WARN] Sina ETF Hist fetch failed for {code}: {e}")
            
        return []
