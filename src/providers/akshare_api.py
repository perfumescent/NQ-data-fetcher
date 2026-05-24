import akshare as ak
import pandas as pd
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import json
import threading

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


def _safe_float(val, default: float | None = None) -> float | None:
    """
    安全地将接口返回值转为 float。

    Args:
        val: 任意值，可能是 float、int、str、None、NaN、"-" 等。
        default: 转换失败时的兜底值。None 表示"无数据"，调用方自行处理。

    Returns:
        转换后的 float，或 default。

    Created: 2026-04
    易错点: pandas 返回的 NaN 能通过 float() 但不等于有效数据，需用 pd.isna 检测。
    """
    if val is None:
        return default
    try:
        f = float(val)
        if pd.isna(f):
            return default
        return f
    except (ValueError, TypeError):
        return default


class AkShareProvider:
    _etf_spot_batch_cache: pd.DataFrame | None = None
    _etf_spot_batch_lock = threading.Lock()

    @staticmethod
    def reset_cycle_cache() -> None:
        """
        清空单轮采集缓存。

        Args:
            无。
        Returns:
            None。

        Created: 2026-05
        易错点: data_fetcher 是常驻进程；不在每轮开始清空会导致 ETF 实时行情跨轮复用旧数据。
        """
        with AkShareProvider._etf_spot_batch_lock:
            AkShareProvider._etf_spot_batch_cache = None

    # ──────────────────────────────────────────────────────────────
    # ETF 实时行情（批量 + 按 code 查询）
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _fetch_etf_spot_batch() -> pd.DataFrame:
        """
        批量拉取全市场 ETF 实时行情快照（东方财富，经 AkShare 封装）。

        Args:
            无。
        Returns:
            包含所有 ETF 当日行情的 DataFrame，列名为中文（东方财富原始命名）。
            核心列：代码、最新价、IOPV实时估值、基金折价率、涨跌幅、换手率、成交量、成交额。
            失败时返回空 DataFrame。

        Created: 2026-04
        易错点:
            - fund_etf_spot_em() 内部分页请求东方财富 push2 批量接口（clist），
              在部分网络环境（如本地开发机）会被拦截返回 RemoteDisconnected，
              此时调用方应降级到 _fetch_etf_realtime_sina()。
            - 非交易时段（盘前/盘后/节假日）返回的是上一交易日收盘数据，
              此时"基金折价率"可能为 0，调用方需兜底。
            - 同一采集轮会并发拉多只 ETF，必须用锁保护进程内缓存，否则批量接口会被重复请求。
        """
        with AkShareProvider._etf_spot_batch_lock:
            if AkShareProvider._etf_spot_batch_cache is not None:
                return AkShareProvider._etf_spot_batch_cache
            df = pd.DataFrame()
            try:
                df = ak.fund_etf_spot_em()
                print(f"[ETF Batch] ✓ 东方财富批量接口成功，共 {len(df)} 只 ETF")
            except Exception as e:
                print(f"[ETF Batch] ✗ 东方财富批量接口失败，将对每只 ETF 降级至新浪单次请求。原因: {e}")
            AkShareProvider._etf_spot_batch_cache = df
            return df

    @staticmethod
    def get_etf_realtime(code: str) -> dict | None:
        """
        获取单只 ETF 的实时行情快照（价格、涨跌幅、溢价率、换手率、成交量/额）。

        策略：优先从 _fetch_etf_spot_batch()（东方财富批量接口）获取，
        若批量接口失败或找不到该 code，自动降级到 _fetch_etf_realtime_sina()（新浪逐只请求）。
        两条路径最终返回相同格式的 dict，Assembler 层无感知。

        Args:
            code: 6 位 ETF 代码，如 "159660"、"513300"。

        Returns:
            标准化 dict，键说明：
                price         (float)  : 最新成交价（元）
                changePct     (float)  : 涨跌幅，比率制（0.01 = 1%）
                premiumRate   (float)  : 溢价率，比率制（正=溢价，负=折价；0.01 = 1%）
                turnoverRate  (float)  : 换手率，百分比制（1.97 = 1.97%）
                volume        (float)  : 成交量（股）
                turnoverAmount(float)  : 成交额（元）
                timestamp     (datetime): 数据时间戳
            找不到该 code 或数据异常时返回 None。

        Created: 2026-04
        易错点:
            - 东方财富"基金折价率"符号约定：负数=溢价，正数=折价（即 (IOPV-Price)/IOPV）。
              本方法取反后转为比率制，使正数=溢价，与业务层约定一致。
            - 停牌/集合竞价时"最新价"可能为 0 或 NaN，此时返回 None 让上层标记 stale。
        """
        # ── 主路径：东方财富批量接口 ──
        result = AkShareProvider._extract_from_batch(code)
        if result is not None:
            return result

        # ── 降级路径：新浪逐只请求 ──
        print(f"[ETF Sina] {code} → 新浪单次请求中...")
        result = AkShareProvider._fetch_etf_realtime_sina(code)
        if result is not None:
            print(
                f"[ETF Sina] {code} ✓ 成功 | "
                f"price={result['price']:.4f} "
                f"changePct={result['changePct']*100:+.2f}% "
                f"premiumRate={result['premiumRate']*100:+.2f}% "
                f"turnoverAmount={result['turnoverAmount']}"
            )
        else:
            print(f"[ETF Sina] {code} ✗ 失败（price 为空，可能停牌或接口异常）")
        return result

    @staticmethod
    def _extract_from_batch(code: str) -> dict | None:
        """
        从批量拉取的 DataFrame 中提取单只 ETF 数据并转换为标准 dict。

        Args:
            code: 6 位 ETF 代码。

        Returns:
            标准化 dict（格式同 get_etf_realtime），或 None（批量数据不可用 / 未找到该 code）。

        Created: 2026-04
        易错点: 仅做字段映射，不发起网络请求；返回 None 不代表 ETF 不存在，可能只是批量接口暂时不可用。
        """
        df = AkShareProvider._fetch_etf_spot_batch()
        if df.empty:
            return None

        row = df[df["代码"] == code]
        if row.empty:
            return None

        row = row.iloc[0]

        # 最新价：停牌/异常时可能为 NaN 或 0
        price = _safe_float(row.get("最新价"))
        if price is None or price <= 0:
            return None

        # 涨跌幅：东方财富返回百分比数值（如 0.41 代表 0.41%），转为比率制
        change_pct = _safe_float(row.get("涨跌幅"), default=0.0) / 100.0

        # 溢价率：东方财富"基金折价率"为 (IOPV-Price)/IOPV*100，负数=溢价
        #         取反并除以 100 转为比率制，使 正数=溢价、负数=折价
        discount_rate = _safe_float(row.get("基金折价率"), default=0.0)
        premium_rate = -discount_rate / 100.0

        # 换手率：东方财富返回百分比数值（如 1.97 代表 1.97%），保持百分比制
        turnover_rate = _safe_float(row.get("换手率"), default=0.0)

        # 成交量（股）& 成交额（元）
        volume = _safe_float(row.get("成交量"))
        amount = _safe_float(row.get("成交额"))

        return {
            "price": price,
            "changePct": change_pct,
            "premiumRate": premium_rate,
            "turnoverRate": turnover_rate,
            "volume": volume,
            "turnoverAmount": amount,
            "timestamp": datetime.utcnow(),
            "_dataSource": "eastmoney",
        }

    @staticmethod
    def _fetch_etf_realtime_sina(code: str) -> dict | None:
        """
        从新浪财经逐只获取 ETF 实时行情（降级路径）。

        请求两个标的：股票行情（价格/涨跌幅/成交量/额）+ 基金净值（IOPV），
        手动计算溢价率 = (price - iopv) / iopv。

        Args:
            code: 6 位 ETF 代码，如 "159660"。

        Returns:
            标准化 dict（格式同 get_etf_realtime），或 None（请求失败 / 停牌）。

        Created: 2026-04
        易错点:
            - 新浪股票行情字段顺序：[3]=最新价, [2]=昨收, [8]=成交量(股), [9]=成交额(元)。
            - 新浪基金净值行（f_前缀）字段顺序：[1]=当日参考净值（盘中实时，近似 IOPV）。
              不能用 [3]（上一日单位净值），否则溢价率会系统性偏高约 1-2%。
            - 停牌时 price=0，需用昨收兜底；IOPV 可能为空，此时溢价率回退为 0。
            - 不含换手率（新浪不提供），回退为 0.0。
        """
        market_prefix = "sh" if code.startswith("5") else "sz"
        sina_stock = f"{market_prefix}{code}"
        sina_fund = f"f_{code}"

        url = f"http://hq.sinajs.cn/list={sina_stock},{sina_fund}"

        price = None
        change_pct = 0.0
        iopv = None
        volume = None
        amount = None

        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                lines = r.text.split("\n")

                # ── 解析股票行情行 ──
                stock_line = next((l for l in lines if sina_stock in l), None)
                if stock_line and '="' in stock_line:
                    content = stock_line.split('="')[1].strip('";')
                    parts = content.split(",")
                    if len(parts) > 30:
                        price_val = float(parts[3])
                        prev_close = float(parts[2])
                        # 停牌/集合竞价时 price=0，用昨收兜底
                        if price_val == 0 and prev_close > 0:
                            price_val = prev_close
                        price = price_val
                        if prev_close > 0:
                            change_pct = (price - prev_close) / prev_close
                        try:
                            volume = float(parts[8])
                            amount = float(parts[9])
                        except (ValueError, IndexError):
                            pass

                # ── 解析基金净值行（IOPV） ──
                # var hq_str_f_159660="名称,当日参考净值,累计净值,上日单位净值,净值日期,份额"
                # [1]=当日参考净值（盘中实时更新，最接近 IOPV），
                # [3]=上一日单位净值（T-1 收盘值，用来手算溢价率会偏高）。
                # 必须用 [1] 而非 [3]。
                fund_line = next((l for l in lines if sina_fund in l), None)
                if fund_line and '="' in fund_line:
                    content = fund_line.split('="')[1].strip('";')
                    parts = content.split(",")
                    if len(parts) > 1:
                        try:
                            val = float(parts[1])
                            if val > 0:
                                iopv = val
                        except (ValueError, IndexError):
                            pass

        except Exception as e:
            print(f"[WARN] Sina fetch failed for {code}: {e}")

        if price is None:
            return None

        # 手动计算溢价率
        premium = 0.0
        if iopv and iopv > 0:
            premium = (price - iopv) / iopv

        return {
            "price": price,
            "changePct": change_pct,
            "premiumRate": premium,
            "turnoverRate": 0.0,  # 新浪不提供换手率
            "volume": volume,
            "turnoverAmount": amount,
            "timestamp": datetime.utcnow(),
            "_dataSource": "sina",
        }

    @staticmethod
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
            if code.startswith("5") or code.startswith("1"):
                print(f"[WARN] Yahoo CN Chart fetch failed for {symbol}: {e}")

        if code.startswith("0"):
            print(f"[INFO] Fund {code} detected, returning mock intraday curve")
            return AkShareProvider._get_mock_chart()

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
            import math
            mock_points = []
            base = 1.240
            now = datetime.now()
            for i in range(240):
                t = (now - pd.Timedelta(minutes=240-i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                p = base + math.sin(i/20) * 0.01 + (i*0.0001)
                mock_points.append({"t": t, "p": p})
            return mock_points

    @staticmethod
    def get_fund_nav_history(code: str) -> pd.DataFrame:
        """
        Get off-exchange fund NAV history for tracking error calc.
        """
        try:
            df = ak.fund_open_fund_info_em(fund=code, indicator="单位净值走势")
            return df
        except Exception as e:
            print(f"[Error] AkShare get_fund_nav_history {code}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _get_mock_chart() -> list[dict]:
        import math
        mock_points = []
        base = 1.240
        now = datetime.now()
        for i in range(240):
            t = (now - pd.Timedelta(minutes=240-i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            p = base + math.sin(i/20) * 0.01 + (i*0.0001)
            mock_points.append({"t": t, "p": p})
        return mock_points

    @staticmethod
    def get_fund_estimate_direct(code: str) -> dict | None:
        """
        Get Fund Estimate directly from EastMoney JS API.
        Returns: { "estNav": float, "changePct": float, "nav": float, "valDate": str, "name": str }
        """
        url = f"http://fundgz.1234567.com.cn/js/{code}.js"

        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                text = r.text.strip()
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
                        "navDate": data.get("jzrq"),
                        "_dataSource": "eastmoney",
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
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
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
        易错点: 东财可能同时返回 "暂停申购" 和正数 MAXSG；这种 QDII 场景不能直接覆盖静态限额为暂停。
        """
        sgzt_str = str(sgzt).strip() if sgzt and str(sgzt) != "--" else ""
        parsed_maxsg = None
        if maxsg is not None and str(maxsg).strip() not in ("--", "", "None"):
            try:
                parsed_maxsg = float(maxsg)
            except (ValueError, TypeError):
                parsed_maxsg = None

        if "暂停" in sgzt_str:
            if parsed_maxsg is not None and parsed_maxsg > 0:
                print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → None (use configured quota)")
                return None
            print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → 暂停")
            return "暂停"

        if parsed_maxsg is not None and parsed_maxsg > 0:
            val = parsed_maxsg
            if val >= 100_000_000:
                result = f"{val / 100_000_000:.0f}亿"
            elif val >= 10_000:
                result = f"{val / 10_000:.0f}万"
            else:
                result = f"{int(val)}元"
            print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → {result}")
            return result

        if "正常" in sgzt_str:
            print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → 正常")
            return "正常"

        print(f"[Quota] {code} SGZT={sgzt_str!r} MAXSG={maxsg!r} → None (unrecognized)")
        return None

    @staticmethod
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

                end_nav = info.get("ENDNAV")
                if end_nav:
                    try:
                        res["fundSize"] = float(end_nav) / 100000000.0
                    except:
                        pass

                estab_date = info.get("ESTABDATE")
                if estab_date and str(estab_date) != "NaT":
                    res["inceptionDate"] = str(estab_date).strip()

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
                df = ak.fund_individual_basic_info_xq(symbol=code)
                if not df.empty:
                    row = df[df['item'] == '成立时间']
                    if not row.empty:
                        date_str = str(row.iloc[0]['value'])
        except Exception as e:
            pass

        if not date_str:
            fallback = AkShareProvider._fetch_fund_info_fallback(code)
            date_str = fallback.get("inceptionDate")

        return date_str

    @staticmethod
    def get_fund_metadata(code: str) -> dict:
        """
        Get extra fund metadata (Returns, Size, etc) from EastMoney.
        Returns: { "yearChange": float, "sixMonthChange": float, "fundSize": float (亿元) }
        """
        url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        res = {}

        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                text = r.text

                m1 = re.search(r'var syl_1n\s*=\s*"([^"]+)"', text)
                if m1: res["yearChange"] = float(m1.group(1))

                m2 = re.search(r'var syl_6y\s*=\s*"([^"]+)"', text)
                if m2: res["sixMonthChange"] = float(m2.group(1))

                m3 = re.search(r'var syl_1y\s*=\s*"([^"]+)"', text)
                if m3: res["oneMonthChange"] = float(m3.group(1))

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
                    except:
                        pass

        except Exception as e:
            print(f"[WARN] Fund metadata fetch failed for {code}: {e}")

        fallback = AkShareProvider._fetch_fund_info_fallback(code)
        if not res.get("fundSize") and fallback.get("fundSize"):
            res["fundSize"] = fallback["fundSize"]
        if fallback.get("quota"):
            res["quota"] = fallback["quota"]

        print(f"[Metadata] {code} final quota={res.get('quota')!r}")
        return res

    @staticmethod
    def get_fund_history_em(code: str) -> list[dict]:
        """
        Get Fund Historical NAV from EastMoney Pingzhong Data.
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
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
    def get_nav_history_em(code: str) -> list[dict]:
        """
        Get NAV History for both ETF and Fund from EastMoney.
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
        This effectively reuses the logic of get_fund_history_em but is named for clarity
        and potential future divergence if needed.
        """
        return AkShareProvider.get_fund_history_em(code)

    @staticmethod
    def get_etf_history_sina(code: str) -> list[dict]:
        """
        Get ETF Historical Data from Sina (via AkShare).
        Returns list of dicts: [{"t": "YYYY-MM-DD", "p": float}, ...]
        """
        try:
            symbol = f"sh{code}" if code.startswith("5") or code.startswith("6") else f"sz{code}"

            df = ak.fund_etf_hist_sina(symbol=symbol)
            if not df.empty:
                df = df.tail(250)

                result = []
                for _, row in df.iterrows():
                    t_str = str(row['date'])
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
