from fastapi import FastAPI, Query
import requests
import re
import json
import asyncio
import aiohttp
from datetime import datetime
from typing import Optional, Dict, List, Any
import redis

app = FastAPI(title="A股基本面数据服务")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.eastmoney.com/"
}

# ========== 配置 ==========
CACHE_ENABLED = False  # 是否启用Redis缓存
CACHE_TTL = 3600  # 缓存1小时

if CACHE_ENABLED:
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
    except:
        CACHE_ENABLED = False


# ========== 字段映射表 ==========
class FieldMapper:
    STANDARD_FIELDS = {
        "revenue": "营业收入",
        "operating_cost": "营业成本",
        "sales_expense": "销售费用",
        "management_expense": "管理费用",
        "finance_expense": "财务费用",
        "operating_profit": "营业利润",
        "total_profit": "利润总额",
        "net_profit": "归母净利润",
        "deducted_net_profit": "扣非净利润",
        "basic_eps": "基本每股收益",
        "diluted_eps": "稀释每股收益",
        "total_assets": "总资产",
        "total_liabilities": "总负债",
        "shareholder_equity": "股东权益",
        "cash_balance": "货币资金",
        "accounts_receivable": "应收账款",
        "inventories": "存货",
        "fixed_assets": "固定资产",
        "intangible_assets": "无形资产",
        "goodwill": "商誉",
        "total_capital_stock": "股本",
        "operating_cash_flow": "经营活动现金流",
        "investing_cash_flow": "投资活动现金流",
        "financing_cash_flow": "筹资活动现金流",
        "free_cash_flow": "自由现金流",
        "roe": "净资产收益率(%)",
        "roa": "总资产收益率(%)",
        "gross_margin": "销售毛利率(%)",
        "net_margin": "销售净利率(%)",
        "debt_ratio": "资产负债率(%)",
        "operating_cash_per_share": "每股经营现金流(元)",
        "current_ratio": "流动比率",
        "quick_ratio": "速动比率"
    }

    EASTMONEY_MAPPING = {
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
        "OPERATE_CASH_FLOW": "operating_cash_flow",
        "INVEST_CASH_FLOW": "investing_cash_flow",
        "FINANCE_CASH_FLOW": "financing_cash_flow",
        "FREE_CASH_FLOW": "free_cash_flow",
        "WEIGHTAVG_ROE": "roe",
        "ROA": "roa",
        "GROSS_PROFIT_RATIO": "gross_margin",
        "SALES_NETP_RATIO": "net_margin",
        "DEBT_ASSET_RATIO": "debt_ratio",
        "OPERATE_CASHFLOW_PER_SHARE": "operating_cash_per_share",
        "BASIC_EPS": "basic_eps",
        "CURRENT_RATIO": "current_ratio",
        "QUICK_RATIO": "quick_ratio"
    }

    @classmethod
    def to_standard(cls, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for raw_key, raw_value in raw_data.items():
            if raw_key in cls.EASTMONEY_MAPPING:
                standard_key = cls.EASTMONEY_MAPPING[raw_key]
                if raw_value not in [None, "", "N/A", "-", "--", "NaN"]:
                    result[standard_key] = safe_float(raw_value)
        return result


# ========== 工具函数 ==========
def safe_float(value, default=0.0):
    try:
        if value in [None, "", "N/A", "-", "--", "NaN"]:
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
    match = re.search(r'\d{6}', symbol)
    if not match:
        return symbol, symbol
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
        return await asyncio.gather(*tasks, return_exceptions=True)


# ========== 财务核心（纯异步机制）==========
async def fetch_eastmoney_financial(code: str) -> Optional[List[Dict]]:
    cache_key = get_cache_key("financial", code)
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    try:
        base_params = f"reportName=RPT_LICO_FN_CPD&filter=(SECURITY_CODE%3D%22{code}%22)&pageNumber=1&pageSize=5&sortTypes=-1&sortColumns=REPORT_DATE"

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
            if not resp or isinstance(resp, Exception):
                continue
            try:
                if "result" in resp and "data" in resp.get("result", {}):
                    for item in resp["result"]["data"]:
                        report_date = item.get("REPORT_DATE", "")
                        if not report_date:
                            continue
                        date_key = report_date[:10]
                        if date_key not in reports_dict:
                            reports_dict[date_key] = {}

                        standard_data = FieldMapper.to_standard(item)
                        for field_key, field_value in standard_data.items():
                            if field_value != 0.0:
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


def fetch_tencent_spot(symbol: str) -> Optional[Dict]:
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


# ========== API 路由 ==========
@app.get("/stock_spot")
def get_stock_spot(symbol: str = Query(..., description="6位股票代码")):
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
                "info": {"name": data["name"], "code": data["code"],
                         "market": "SH" if data["code"].startswith("6") else "SZ"},
                "market": {
                    "price": data["price"], "change": data["change"], "pct_change": data["pct_change"],
                    "open": data["open"], "prev_close": data["prev_close"], "high": data["high"], "low": data["low"],
                    "volume": data["volume"], "amount": data["amount"], "turnover_rate": data["turnover_rate"],
                    "amplitude": data["amplitude"], "volume_ratio": data["volume_ratio"]
                },
                "valuation": {
                    "market_cap": data["market_cap"], "circulating_cap": data["circulating_cap"],
                    "pe_ttm": data["pe_ttm"], "pe_static": data["pe_static"], "pb": data["pb"],
                    "dividend_yield": data["dividend_yield"], "high_52w": data["high_52w"], "low_52w": data["low_52w"]
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
    try:
        code, _ = clean_symbol(symbol)
        cache_key = get_cache_key("fundamental", code)
        cached = get_from_cache(cache_key)

        # 加上 await，安全解决死锁
        reports = cached if cached else await fetch_eastmoney_financial(code)

        if not reports:
            return {"status": "error", "message": f"无法获取股票 {symbol} 的财务数据"}

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


@app.get("/health")
def health_check():
    return {"status": "healthy", "cache_enabled": CACHE_ENABLED, "timestamp": datetime.now().isoformat()}