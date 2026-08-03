import os
import akshare as ak
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, List

app = FastAPI(
    title="A-Share Financial Data Agent Tool",
    description="专为 Agent 设计的 A 股财务数据查询工具 API",
    version="1.0.0",
)


class ErrorResponse(BaseModel):
    error: str


@app.get("/")
def health_check():
    """健康检查接口（Render 用于探针检测）"""
    return {"status": "ok", "service": "A-Share Financial Agent Tool"}


@app.get(
    "/api/financial/indicators",
    summary="获取主要财务指标",
    response_model=List[Dict[str, Any]],
)
def get_financial_indicators(
    symbol: str = Query(
        ...,
        description="6位股票代码，例如 '600519' 或 '000001'",
        example="600519",
    ),
    limit: int = Query(5, description="返回最新几期的数据", ge=1, le=50),
):
    """**Agent 工具方法**：获取上市公司的 ROE、营业收入增长率、净利润增长率、每股收益等核心财务指标。"""
    try:
        clean_symbol = symbol.strip()
        df = ak.stock_financial_analysis_indicator(symbol=clean_symbol)

        if df is None or df.empty:
            raise HTTPException(
                status_code=404, detail=f"未找到股票 {clean_symbol} 的财务指标数据"
            )

        # 替换 NaN 为 None 以保证 JSON 序列化合法
        df = df.where(df.notnull(), None)

        # 截取最新 limit 条，转为 JSON 兼容字典格式
        result = df.head(limit).to_dict(orient="records")
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"获取财务指标失败: {str(e)}"
        )


@app.get(
    "/api/financial/report",
    summary="获取三大财务报表",
    response_model=List[Dict[str, Any]],
)
def get_financial_report(
    symbol: str = Query(
        ...,
        description="6位股票代码，例如 '600519' 或 '000001'",
        example="600519",
    ),
    report_type: str = Query(
        ...,
        description="报表类型，可选: '资产负债表', '利润表', '现金流量表'",
        example="利润表",
    ),
    limit: int = Query(5, description="返回最新几期的数据", ge=1, le=20),
):
    """**Agent 工具方法**：获取上市公司的原始三大财务报表数据。"""
    valid_types = ["资产负债表", "利润表", "现金流量表"]
    if report_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"无效的报表类型，必须是以下之一: {valid_types}",
        )

    try:
        clean_symbol = symbol.strip()
        # 处理市场前缀 (新浪接口必须指定 sh/sz)
        market_symbol = (
            f"sh{clean_symbol}"
            if clean_symbol.startswith("6")
            else f"sz{clean_symbol}"
        )

        df = ak.stock_financial_report_sina(
            stock=market_symbol, symbol=report_type
        )

        if df is None or df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"未找到股票 {clean_symbol} 的 {report_type} 数据",
            )

        df = df.where(df.notnull(), None)
        result = df.head(limit).to_dict(orient="records")
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"获取 {report_type} 失败: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    # Render 会自动注入 PORT 环境变量
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
