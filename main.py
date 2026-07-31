from fastapi import FastAPI, Query
import requests
import re
import json
import asyncio
import aiohttp
from datetime import datetime
from typing import Optional, Dict, List, Any
import httpx

app = FastAPI(title="A股基本面数据服务")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.eastmoney.com/"
}

# ========== 配置 ==========
CACHE_ENABLED = False
CACHE_TTL = 3600

if CACHE_ENABLED:
    try:
        import redis
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
    except:
        CACHE_ENABLED = False


# ========== 字段映射表 ==========
class FieldMapper:
    EASTMONEY_MAPPING = {
        # 利润表
        "TOTAL_OPERATE_INCOME": "revenue",
        "OPERATE_COST": "operating_cost",
        "SALES_EXPENSE": "sales_expense",
        "MANAGE_EXPENSE": "management_expense",
        "FINANCE_EXPENSE": "finance_expense",
        "OPERATE_PROFIT": "operating_profit",
        "TOTAL_PROFIT": "total_profit",
        "PARENT_NETPROFIT": "net_profit",
        "KCFJCXSYJLR": "deducted_net_profit",
        "DEDUCT_BASIC_EPS": "basic_eps",
        "DILUTE_EPS": "diluted_eps",
        # 资产负债表
        "TOTAL_ASSET": "total_assets",
        "TOTAL_LIAB": "total_liabilities",
        "SHAREHOLDER_EQUITY": "shareholder_equity",
        "CASH_BALANCE": "cash_balance",
        "ACCOUNTS_RECEIVABLE": "accounts_receivable",
        "INVENTORIES": "inventories",
        "FIXED_ASSET": "fixed_assets",
        "INTANGIBLE_ASSET": "intangible_assets",
        "GOODWILL": "goodwill",
        "TOTAL_CAPITAL_STOCK": "total_capital_stock",
        # 现金流量表
        "OPERATE_CASH_FLOW": "operating_cash_flow",
        "INVEST_CASH_FLOW": "investing_cash_flow",
        "FINANCE_CASH_FLOW": "financing_cash_flow",
        "FREE_CASH_FLOW": "free_cash_flow",
        # 财务指标
        "WEIGHTAVG_ROE": "roe",
        "ROA": "roa",
        "GROSS_PROFIT_RATIO": "gross_margin",
        "SALES_NETP_RATIO": "net_margin",
        "DEBT_ASSET_RATIO": "debt_ratio",
        "OPERATE_CASHFLOW_PER_SHARE": "operating_cash_per_share",
        "CURRENT_RATIO": "current_ratio",
        "QUICK_RATIO": "quick_ratio"
    }

    @classmethod
    def to_standard(cls, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for raw_key, raw_value in raw_data.items():
            if raw_key in cls.EASTMONEY_MAPPING:
                standard_key = cls.EASTMONEY_MAPPING[raw_key]
                if raw_value not in [None, "", "N/A", "-", "--", "NaN", "null"]:
                    result[standard_key] = safe_float(raw_value)
        return result


# ========== 工具函数 ==========
def safe_float(value, default=0.0):
    try:
        if value in [None, "", "N/A", "-", "--", "NaN", "null"]:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        cleaned = str(value).replace(",", "").replace("%", "").strip()
        return float(cleaned) if cleaned else default
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    try:
        if value in [None, "", "N/A", "-", "--"]:
            return default
        return int(float(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return default


def clean_symbol(symbol: str):
    """提取6位数字代码，并返回带前缀的代码"""
    match = re.search(r'\d{6}', str(symbol))
    if not match:
        digits = re.sub(r'\D', '', str(symbol))
        code = digits.zfill(6)
    else:
        code = match.group(0)
    prefix = "sh" if code.startswith("6") or code.startswith("9") else "sz"
    return code, f"{prefix}{code}"


def get_cache_key(prefix: str, symbol: str) -> str:
    return f"{prefix}:{symbol}"


def get_from_cache(key: str) -> Optional[Dict]:
    if not CACHE_ENABLED:
        return None
    try:
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except:
        pass
    return None


def set_to_cache(key: str, data: Dict, ttl: int = CACHE_TTL):
    if not CACHE_ENABLED:
        return
    try:
        redis_client.setex(key, ttl, json.dumps(data))
    except:
        pass


# ========== 异步请求 ==========
async def fetch_url(session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        print(f"请求失败 {url}: {str(e)}")
    return None


async def fetch_multiple_urls(urls: List[str]) -> List[Optional[Dict]]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # 过滤掉异常，只保留正常结果
        return [r for r in results if not isinstance(r, Exception)]


# ========== 数据源1：东方财富（主数据源）==========
async def fetch_eastmoney_financial(code: str) -> Optional[List[Dict]]:
    """从东方财富获取财务数据"""
    cache_key = get_cache_key("financial", code)
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    
    try:
        code = str(code).zfill(6)
        
        # 使用通用业绩报表接口
        base_params = f"reportName=RPT_LICO_FN_CPD&filter=(SECURITY_CODE%3D%22{code}%22)&pageNumber=1&pageSize=8&sortTypes=-1&sortColumns=REPORT_DATE"
        
        income_fields = "SECURITY_CODE,REPORT_DATE,TOTAL_OPERATE_INCOME,OPERATE_COST,SALES_EXPENSE,MANAGE_EXPENSE,FINANCE_EXPENSE,OPERATE_PROFIT,TOTAL_PROFIT,PARENT_NETPROFIT,KCFJCXSYJLR,DEDUCT_BASIC_EPS,DILUTE_EPS"
        balance_fields = "SECURITY_CODE,REPORT_DATE,TOTAL_ASSET,TOTAL_LIAB,SHAREHOLDER_EQUITY,CASH_BALANCE,ACCOUNTS_RECEIVABLE,INVENTORIES,FIXED_ASSET,INTANGIBLE_ASSET,GOODWILL,TOTAL_CAPITAL_STOCK"
        cashflow_fields = "SECURITY_CODE,REPORT_DATE,OPERATE_CASH_FLOW,INVEST_CASH_FLOW,FINANCE_CASH_FLOW,FREE_CASH_FLOW"
        indicator_fields = "SECURITY_CODE,REPORT_DATE,WEIGHTAVG_ROE,ROA,GROSS_PROFIT_RATIO,SALES_NETP_RATIO,DEBT_ASSET_RATIO,OPERATE_CASHFLOW_PER_SHARE,CURRENT_RATIO,QUICK_RATIO"
        
        urls = [
            f"https://datacenter-web.eastmoney.com/api/data/v1/get?{base_params}&columns={income_fields}",
            f"https://datacenter-web.eastmoney.com/api/data/v1/get?{base_params}&columns={balance_fields}",
            f"https://datacenter-web.eastmoney.com/api/data/v1/get?{base_params}&columns={cashflow_fields}",
            f"https://datacenter-web.eastmoney.com/api/data/v1/get?{base_params}&columns={indicator_fields}"
        ]
        
        responses = await fetch_multiple_urls(urls)
        
        reports_dict = {}
        for resp in responses:
            if not resp:
                continue
            try:
                if "result" in resp and resp.get("result") and "data" in resp["result"]:
                    for item in resp["result"]["data"]:
                        report_date = item.get("REPORT_DATE", "")
                        if not report_date:
                            continue
                        date_key = str(report_date)[:10]
                        if date_key not in reports_dict:
                            reports_dict[date_key] = {}
                        
                        standard_data = FieldMapper.to_standard(item)
                        for field_key, field_value in standard_data.items():
                            if field_value is not None and field_value != 0.0:
                                reports_dict[date_key][field_key] = field_value
            except Exception as e:
                print(f"解析数据失败: {str(e)}")
                continue
        
        if not reports_dict:
            return None
        
        sorted_dates = sorted(reports_dict.keys(), reverse=True)
        reports = []
        for date in sorted_dates[:5]:
            if reports_dict[date]:
                reports.append({
                    "report_date": date,
                    "data": reports_dict[date]
                })
        
        if reports:
            set_to_cache(cache_key, reports)
        
        return reports
        
    except Exception as e:
        print(f"东方财富数据获取失败: {str(e)}")
        return None


# ========== 数据源2：新浪财经（备用）==========
async def fetch_sina_financial(code: str) -> Optional[List[Dict]]:
    """从新浪财经获取财务数据（备用，覆盖中小板）"""
    try:
        code = str(code).zfill(6)
        
        # 新浪财经的财务数据接口
        # 使用新浪的API接口（更稳定）
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getStockFinace?symbol={code}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS, timeout=10) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # 新浪返回的是JSONP格式，需要处理
                    if text.startswith("/*"):
                        # 移除JSONP包装
                        json_str = re.search(r'\{.*\}', text, re.DOTALL)
                        if json_str:
                            data = json.loads(json_str.group())
                            
                            # 解析财务数据
                            reports = []
                            # 根据实际数据结构解析
                            # 这里简化处理，实际需要根据返回结构调整
                            return reports
        return None
    except Exception as e:
        print(f"新浪数据获取失败: {str(e)}")
        return None


# ========== 数据源3：腾讯财经（行情+财务）==========
def fetch_tencent_spot(symbol: str) -> Optional[Dict]:
    """从腾讯获取实时行情"""
    try:
        _, full_symbol = clean_symbol(symbol)
        url = f"http://qt.gtimg.cn/q={full_symbol}"

        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = 'gbk'
        text = res.text

        if text and "~" in text:
            parts = text.split("~")
            if len(parts) > 49:
                def get_part(idx, default=""):
                    return parts[idx] if idx < len(parts) else default

                return {
                    "name": get_part(1),
                    "code": get_part(2),
                    "price": safe_float(get_part(3)),
                    "change": safe_float(get_part(31)),
                    "pct_change": safe_float(get_part(32)),
                    "open": safe_float(get_part(5)),
                    "prev_close": safe_float(get_part(4)),
                    "high": safe_float(get_part(33)),
                    "low": safe_float(get_part(34)),
                    "volume": safe_int(get_part(6)),
                    "amount": safe_float(get_part(37)),
                    "turnover_rate": safe_float(get_part(38)),
                    "amplitude": safe_float(get_part(43)),
                    "volume_ratio": safe_float(get_part(46)) if len(parts) > 46 else 0.0,
                    "market_cap": safe_float(get_part(45)),
                    "circulating_cap": safe_float(get_part(44)),
                    "pe_ttm": safe_float(get_part(39)),
                    "pe_static": safe_float(get_part(40)) if len(parts) > 40 else 0.0,
                    "pb": safe_float(get_part(46)) if len(parts) > 46 else 0.0,
                    "high_52w": safe_float(get_part(48)) if len(parts) > 48 else 0.0,
                    "low_52w": safe_float(get_part(47)) if len(parts) > 47 else 0.0,
                    "dividend_yield": safe_float(get_part(50)) if len(parts) > 50 else 0.0
                }
    except Exception as e:
        print(f"腾讯行情获取失败: {str(e)}")
    return None


# ========== 数据源4：AKShare（最全面，需安装）==========
def fetch_akshare_financial(code: str) -> Optional[List[Dict]]:
    """使用AKShare获取财务数据（覆盖面最广）"""
    try:
        import akshare as ak
        
        # 获取利润表
        income_df = ak.stock_profit_sheet_by_report_em(symbol=code)
        if income_df.empty:
            return None
        
        # 获取资产负债表
        balance_df = ak.stock_balance_sheet_by_report_em(symbol=code)
        
        # 获取现金流量表
        cashflow_df = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
        
        # 合并数据...
        # 这里简化处理
        return None
    except ImportError:
        print("AKShare未安装，跳过")
        return None
    except Exception as e:
        print(f"AKShare获取失败: {str(e)}")
        return None


# ========== 多数据源聚合 ==========
async def fetch_financial_multi_source(code: str) -> Optional[List[Dict]]:
    """
    多数据源轮询获取财务数据
    优先级：东方财富 → 新浪 → AKShare
    """
    # 1. 尝试东方财富
    reports = await fetch_eastmoney_financial(code)
    if reports:
        print(f"✅ 东方财富获取成功: {code}")
        return reports
    
    # 2. 尝试新浪财经
    print(f"⚠️ 东方财富无数据，尝试新浪: {code}")
    reports = await fetch_sina_financial(code)
    if reports:
        print(f"✅ 新浪获取成功: {code}")
        return reports
    
    # 3. 尝试AKShare
    print(f"⚠️ 新浪无数据，尝试AKShare: {code}")
    reports = fetch_akshare_financial(code)
    if reports:
        print(f"✅ AKShare获取成功: {code}")
        return reports
    
    return None


# ========== API 路由 ==========
@app.get("/stock_spot")
def get_stock_spot(symbol: str = Query(..., description="6位股票代码")):
    """获取实时行情数据"""
    try:
        cache_key = get_cache_key("spot", symbol)
        cached = get_from_cache(cache_key)
        if cached:
            return cached

        data = fetch_tencent_spot(symbol)
        if not data:
            return {"status": "error", "message": f"未查询到股票 {symbol} 的数据"}

        result = {
            "status": "success",
            "symbol": symbol,
            "data": {
                "info": {
                    "name": data["name"], 
                    "code": data["code"],
                    "market": "SH" if data["code"].startswith("6") else "SZ"
                },
                "market": {
                    "price": data["price"], 
                    "change": data["change"], 
                    "pct_change": data["pct_change"],
                    "open": data["open"], 
                    "prev_close": data["prev_close"], 
                    "high": data["high"], 
                    "low": data["low"],
                    "volume": data["volume"], 
                    "amount": data["amount"], 
                    "turnover_rate": data["turnover_rate"],
                    "amplitude": data["amplitude"], 
                    "volume_ratio": data["volume_ratio"]
                },
                "valuation": {
                    "market_cap": data["market_cap"], 
                    "circulating_cap": data["circulating_cap"],
                    "pe_ttm": data["pe_ttm"], 
                    "pe_static": data["pe_static"], 
                    "pb": data["pb"],
                    "dividend_yield": data["dividend_yield"], 
                    "high_52w": data["high_52w"], 
                    "low_52w": data["low_52w"]
                }
            }
        }
        set_to_cache(cache_key, result, ttl=60)
        return result
    except Exception as e:
        return {"status": "error", "message": f"行情获取失败: {str(e)}"}


@app.get("/stock_fundamental")
async def get_stock_fundamental(
    symbol: str = Query(..., description="6位股票代码"),
    periods: int = Query(5, description="返回期数(1-5)", ge=1, le=5)
):
    """获取上市公司财务数据（自动切换数据源）"""
    try:
        code, _ = clean_symbol(symbol)
        
        # 检查缓存
        cache_key = get_cache_key("fundamental", code)
        cached = get_from_cache(cache_key)
        if cached:
            reports = cached
        else:
            # 多数据源获取
            reports = await fetch_financial_multi_source(code)
            
            if reports:
                set_to_cache(cache_key, reports)

        if not reports:
            return {
                "status": "error", 
                "message": f"无法获取股票 {symbol} 的财务数据。\n"
                          f"已尝试：东方财富、新浪财经、AKShare。\n"
                          f"建议：\n"
                          f"1. 使用 /stock_spot 查看实时行情\n"
                          f"2. 尝试分析同行业公司\n"
                          f"3. 稍后重试"
            }

        reports = reports[:periods]
        latest = reports[0] if reports else {}
        history = reports[1:] if len(reports) > 1 else []

        return {
            "status": "success",
            "symbol": code,
            "data": {
                "currency": "CNY",
                "latest": latest,
                "history": history,
                "reports": reports,
                "summary": {
                    "total_periods": len(reports),
                    "latest_date": latest.get("report_date") if latest else None,
                    "available_fields": list(latest.get("data", {}).keys()) if latest else []
                }
            }
        }
    except Exception as e:
        return {"status": "error", "message": f"财务数据获取失败: {str(e)}"}


@app.get("/stock_comprehensive")
async def get_stock_comprehensive(symbol: str = Query(..., description="6位股票代码")):
    """综合接口：同时获取行情和财务数据"""
    spot_result = get_stock_spot(symbol)
    fundamental_result = await get_stock_fundamental(symbol)
    
    return {
        "status": "success" if spot_result.get("status") == "success" or fundamental_result.get("status") == "success" else "error",
        "symbol": symbol,
        "data": {
            "spot": spot_result.get("data") if spot_result.get("status") == "success" else None,
            "fundamental": fundamental_result.get("data") if fundamental_result.get("status") == "success" else None,
            "spot_error": spot_result.get("message") if spot_result.get("status") == "error" else None,
            "fundamental_error": fundamental_result.get("message") if fundamental_result.get("status") == "error" else None
        }
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy", 
        "cache_enabled": CACHE_ENABLED, 
        "timestamp": datetime.now().isoformat()
    }


# ========== 启动命令 ==========
# uvicorn main:app --reload --host 0.0.0.0 --port 8000
