from typing import Any, Dict, List
import akshare as ak
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import requests

# ==================== 1. 全局请求头伪装（防止 Render 海外 IP 被挂断） ====================
old_get = requests.get


def new_get(*args, **kwargs):
    headers = kwargs.get("headers", {}) or {}
    # 注入真实浏览器 User-Agent
    headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    kwargs["headers"] = headers
    return old_get(*args, **kwargs)


# 替换 requests.get
requests.get = new_get
# ==================================================================================

app = FastAPI(
    title="A-Share Financial Data Agent Tool",
    description="专为 Agent 设计的 A 股财务数据查询工具 API",
    version="1.1.0",
)


@app.get("/")
def health_check():
    """健康检查接口"""
    return {"status": "ok", "service": "A-Share Financial Agent Tool"}


@app.get("/health")
def health():
    """兼顾常用的 /health 路径"""
    return {"status": "ok"}


@app.get(
    "/stock_fundamental",
    summary="获取主要财务基本面数据",
    response_model=List[Dict[str, Any]],
)
def get_stock_fundamental(
    symbol: str = Query(
        ..., description="6位股票代码，例如 '600519'", example="600519"
    ),
    periods: int = Query(5, description="返回最新几期数据", ge=1, le=20),
):
    """**Agent 工具**：获取核心财务指标（ROE、营业收入增长率、净利润增长率、每股收益等）。"""
    try:
        clean_symbol = symbol.strip()

        # 使用主要财务指标接口（源站稳定性更高，抗海外 IP 封锁能力强）
        df = ak.stock_financial_analysis_indicator(symbol=clean_symbol)

        if df is None or df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"未找到股票 {clean_symbol} 的财务数据，请确认代码是否正确",
            )

        # 清理 NaN 异常值，防止 JSON 报错
        df = df.where(df.notnull(), None)

        # 截取前 N 期并转成字典数组返回
        result = df.head(periods).to_dict(orient="records")
        return result

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"获取财务数据失败: {str(e)}"
        )


if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
