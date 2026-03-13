# -*- coding: utf-8 -*-
"""
数据库管理
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
from fontTools.misc.plistlib import end_date
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Date,
    DateTime,
    Integer,
    Index,
    UniqueConstraint,
    select,
    and_,
    desc,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError

from config import get_config

logger = logging.getLogger(__name__)

# SQLAlchemy ORM 基类
Base = declarative_base()


# === 数据模型定义 ===

class StockDaily(Base):
    """
    股票日线数据模型 - ORM映射类

    数据库表: stock_daily
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_daily'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockDaily(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

class StockWeekly(Base):
    """
    股票周线数据模型 - ORM映射类

    数据库表: stock_weekly
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, trade_date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_weekly'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)
    # 计算截止日期
    end_date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)
    # 涨跌额
    change = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date', 'end_date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockDaily(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.trade_date,
            'end_date': self.end_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'change': self.change,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

class StockMonth(Base):
    """
    股票月线数据模型 - ORM映射类

    数据库表: stock_month
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, trade_date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_month'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)
    # 计算截止日期
    end_date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)
    # 涨跌额
    change = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date', 'end_date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockDaily(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.trade_date,
            'end_date': self.end_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'change': self.change,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

# === 新增表1：股票基本信息表 ===
class StockBasic(Base):
    """
    股票基本信息模型 - ORM映射类

    数据库表: stock_basic
    功能：存储股票基础属性（非行情类静态/低频更新数据）

    设计原则：
    1. 完整性：包含股票分析所需的核心基本信息
    2. 唯一性：股票代码唯一标识一条记录
    3. 可追溯：记录更新时间，便于数据审计
    4. 高性能：code字段索引优化查询

    字段说明：
    • 核心标识：code（股票代码，唯一）
    • 基础信息：name（股票名称）、industry（所属行业）、area（所属地域）
    • 上市信息：list_date（上市日期）、market（市场类型：沪A/深A/创业板等）
    • 财务简讯：total_share（总股本）、circulating_share（流通股本）
    • 元数据：updated_at（最后更新时间）
    """
    __tablename__ = 'stock_basic'

    # 字段定义
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True, index=True)  # 股票代码（唯一）
    name = Column(String(50), nullable=False)  # 股票名称（如：贵州茅台）
    industry = Column(String(50))  # 所属行业（如：白酒、半导体）
    list_date = Column(Date)  # 上市日期（YYYY-MM-DD）
    market = Column(String(10))  # 市场类型（沪A/深A/创业板/科创板）
    total_share = Column(Float)  # 总股本（亿股）
    circulating_share = Column(Float)  # 流通股本（亿股）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  # 最后更新时间

    def __repr__(self):
        return f"<StockBasic(code={self.code}, name={self.name}, industry={self.industry})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'name': self.name,
            'industry': self.industry,
            'list_date': self.list_date,
            'market': self.market,
            'total_share': self.total_share,
            'circulating_share': self.circulating_share,
            'updated_at': self.updated_at
        }


# === 当天预测的数据
class DailyForecast(Base):
    __tablename__ = 'daily_forecast'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True, index=True)
    forecast_date = Column(Date, nullable=False, unique=True, index=True)
    forecast_rue = Column(String, nullable=False)
    practice_rue = Column(String, nullable=False)
    forecast_model = Column(String, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # 复合约束与索引（核心：股票+日期唯一）
    __table_args__ = (
        UniqueConstraint('code', 'forecast_date', name='uix_daily_forecast_code_date'),
        Index('ix_daily_forecast_code_date', 'code', 'forecast_date'),
    )

    def __repr__(self):
        return f"<StockMoneyFlow(code={self.code}, date={self.date}, main_inflow={self.main_inflow})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'forecast_date': self.forecast_date,
            'forecast_rue': self.forecast_rue,
            'practice_rue': self.practice_rue,
            'forecast_model': self.forecast_model,
        }

# === 新增表2：股票资金流向表 ===
class StockMoneyFlow(Base):
    """
    股票资金流向模型 - ORM映射类

    数据库表: stock_money_flow
    功能：存储每日资金流向数据（主力/散户/北向资金等）

    设计原则：
    1. 完整性：包含资金分析核心维度
    2. 唯一性：(code, date)复合唯一约束
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：(code, date)复合索引优化查询

    字段说明：
    • 标识字段：code（股票代码）、date（交易日期）
    • 资金数据：main_inflow（主力净流入）、retail_inflow（散户净流入）、north_inflow（北向资金净流入）
    • 占比数据：main_ratio（主力资金占比）、retail_ratio（散户资金占比）
    • 元数据：data_source（数据来源）、updated_at（更新时间）
    """
    __tablename__ = 'stock_money_flow'

    # 字段定义
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)  # 股票代码
    date = Column(Date, nullable=False, index=True)  # 交易日期
    main_inflow = Column(Float)  # 主力资金净流入（万元）
    retail_inflow = Column(Float)  # 散户资金净流入（万元）
    north_inflow = Column(Float)  # 北向资金净流入（万元）
    main_ratio = Column(Float)  # 主力资金占比（%）
    retail_ratio = Column(Float)  # 散户资金占比（%）
    data_source = Column(String(50))  # 数据来源（如：EastMoneyFetcher）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合约束与索引（核心：股票+日期唯一）
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_money_flow_code_date'),
        Index('ix_money_flow_code_date', 'code', 'date'),
    )

    def __repr__(self):
        return f"<StockMoneyFlow(code={self.code}, date={self.date}, main_inflow={self.main_inflow})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'main_inflow': self.main_inflow,
            'retail_inflow': self.retail_inflow,
            'north_inflow': self.north_inflow,
            'main_ratio': self.main_ratio,
            'retail_ratio': self.retail_ratio,
            'data_source': self.data_source
        }

class DatabaseManager:
    """
    数据库管理器
    """
    # 单例模式类变量：存储唯一的实例
    _instance: Optional['DatabaseManager'] = None

    def __new__(cls, *args, **kwargs):
        """
        单例模式实现 - 重写 __new__ 方法

        设计原理：
        1. __new__ 方法在 __init__ 之前调用，负责创建对象实例
        2. 检查类变量 _instance 是否已存在
        3. 如果不存在，调用父类的 __new__ 创建新实例
        4. 标记实例为未初始化状态（通过 _initialized 标志）
        5. 返回单例实例

        这样确保整个应用生命周期内只有一个 DatabaseManager 实例

        Returns:
            DatabaseManager: 单例实例
        """
        if cls._instance is None:
            # 创建新实例
            cls._instance = super().__new__(cls)
            # 标记为未初始化，防止 __init__ 重复初始化
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器

        注意：由于单例模式，__init__ 方法可能被多次调用
        使用 _initialized 标志确保只初始化一次

        初始化流程：
        1. 获取数据库连接URL（从参数或配置文件）
        2. 创建SQLAlchemy引擎（配置连接池）
        3. 创建会话工厂（配置会话行为）
        4. 创建数据库表（如果不存在）

        Args:
            db_url: 数据库连接URL
                格式：sqlite:///path/to/database.db
                示例：sqlite:///./data/stock_analysis.db
                如果为None，则从配置文件中读取
        """
        # 单例初始化保护：如果已经初始化，直接返回
        if self._initialized:
            return

        # 步骤1：获取数据库连接URL
        if db_url is None:
            config = get_config()
            db_url = config.get_db_url()  # 从配置文件读取

        # 步骤2：创建SQLAlchemy引擎（连接池管理器）
        # 参数说明：
        # - echo=False: 生产环境关闭SQL语句日志（调试时可设为True）
        # - pool_pre_ping=True: 连接健康检查，避免使用失效连接
        # - 其他参数使用SQLAlchemy默认值，适合大多数场景
        self._engine = create_engine(
            db_url,
            echo=False,  # 设为 True 可查看 SQL 语句（调试用）
            pool_pre_ping=True,  # 连接健康检查（推荐开启）
        )

        # 步骤3：创建会话工厂
        # sessionmaker 是一个工厂函数，用于创建新的Session对象
        # 配置说明：
        # - bind=self._engine: 绑定到上面创建的引擎
        # - autocommit=False: 手动控制事务提交（推荐）
        # - autoflush=False: 手动控制数据刷新（提高性能）
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,  # 手动提交事务，确保数据一致性
            autoflush=False,  # 手动刷新数据，提高性能
        )

        # 步骤4：创建所有表（如果不存在）
        # Base.metadata.create_all 会检查表是否存在，不存在则创建
        # 这是SQLAlchemy的便利功能，避免手动编写CREATE TABLE语句
        Base.metadata.create_all(self._engine)

        # 标记为已初始化，防止重复初始化
        self._initialized = True
        logger.info(f"数据库初始化完成: {db_url}")

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """
        获取数据库管理器单例实例（推荐使用此方法）

        这是访问 DatabaseManager 的标准方式，优于直接实例化。

        设计优势：
        1. 延迟初始化：首次调用时才创建实例，节省资源
        2. 线程安全：确保多线程环境下只有一个实例
        3. 简化调用：隐藏单例实现的复杂性
        4. 类型安全：返回类型明确的DatabaseManager实例

        使用场景：
        from storage import get_db  # 推荐使用这个便捷函数
        db = get_db()  # 内部调用此方法

        或者：
        db = DatabaseManager.get_instance()

        Returns:
            DatabaseManager: 数据库管理器单例实例
        """
        if cls._instance is None:
            cls._instance = cls()  # 创建新实例（触发__init__）
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """
        重置单例实例（主要用于测试）

        使用场景：
        1. 单元测试：每个测试用例需要干净的数据库状态
        2. 配置变更：重新加载数据库配置（如切换数据库）
        3. 连接故障：强制重新建立数据库连接
        4. 内存管理：释放数据库连接资源

        工作原理：
        1. 如果存在实例，调用 _engine.dispose() 释放所有连接
        2. 将类变量 _instance 设为 None
        3. 下次调用 get_instance() 时会创建新实例

        注意事项：
        • 生产环境慎用：释放连接可能导致正在进行的操作失败
        • 线程安全：调用此方法时确保没有其他线程在使用数据库
        • 数据一致性：确保所有事务已提交或回滚

        示例：
            # 在测试开始时重置数据库
            DatabaseManager.reset_instance()
            db = DatabaseManager.get_instance()  # 创建新实例
        """
        if cls._instance is not None:
            cls._instance._engine.dispose()  # 释放数据库连接
            cls._instance = None  # 清除单例实例

    def get_session(self) -> Session:
        """
        获取数据库会话（上下文管理器）

        设计模式：工作单元模式 (Unit of Work Pattern)

        核心概念：
        • Session（会话）：一组相关的数据库操作集合
        • 事务：确保一组操作要么全部成功，要么全部失败
        • 上下文管理器：使用 with 语句自动管理资源

        设计优势：
        1. 自动资源管理：确保会话正确关闭，避免连接泄漏
        2. 异常安全：异常时自动回滚事务，保证数据一致性
        3. 代码简洁：无需手动 try-finally，代码更清晰
        4. 事务控制：支持嵌套事务和保存点

        工作流程：
        1. 创建新会话（从连接池获取连接）
        2. 执行数据库操作（查询、插入、更新、删除）
        3. 提交事务（如果所有操作成功）
        4. 回滚事务（如果任何操作失败）
        5. 关闭会话（释放连接回连接池）

        使用示例：
            # 基本用法
            with db.get_session() as session:
                # 查询数据
                stock = session.query(StockDaily).filter_by(code='600519').first()

                # 修改数据
                stock.close = 1850.0

                # 提交事务（重要！）
                session.commit()

            # 事务自动回滚示例
            try:
                with db.get_session() as session:
                    # 操作1：成功
                    session.add(StockDaily(...))

                    # 操作2：失败，触发异常
                    raise ValueError("模拟错误")

                    # 这行不会执行
                    session.commit()
            except Exception:
                # 事务已自动回滚，操作1不会保存到数据库
                print("事务已回滚")

        Returns:
            Session: SQLAlchemy 会话对象，支持上下文管理器协议

        Raises:
            Exception: 创建会话失败时抛出原始异常
        """
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            # 创建会话失败时，确保关闭会话
            session.close()
            raise  # 重新抛出异常

    def is_date_exist(self, code, freq: str, target_date: Optional[date] = None, )-> bool:
        """当前日期的数据是否存在
        args:
            freq: 频率(日：daily，周：week，月：month)
        """
        if target_date is None:
            target_date = date.today()

        t = StockDaily
        if freq == "week":
            t = StockWeekly
        elif freq == "month":
            t = StockMonth
        with self._SessionLocal() as session:
            # 构建查询：查找指定股票和日期的记录
            # select(StockDaily): 选择 StockDaily 表的所有列
            # .where(): 添加查询条件
            # and_(): 逻辑与，两个条件必须同时满足
            # scalar_one_or_none(): 返回单个结果或None
            result = session.execute(
                select(t).where(
                    and_(
                        t.code == code, t.date == target_date
                    )
                )
            ).scalar_one_or_none()
            return result is not None

    def get_latest_daily_data(self, code: str, days: int = 2) -> List[StockDaily]:
        """
        获取N天的日线数据（按日期降序排列）
        """
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()

            # 将SQLAlchemy的Scalar序列转换为Python列表
            return list(results)

    def get_daily_data_range(self, code: str, start_date: date, end_date: date) -> List[StockDaily]:
        """获取一段时间的日线数据(按日期降序排列)"""
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(desc(StockDaily.date))
            ).scalars().all()
            return list(results)

    def get_latest_weekly_data(self, code: str, days: int = 2) -> List[StockWeekly]:
        """获取N天的周线数据（按日期降序排列）"""
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockWeekly)
                .where(StockWeekly.code == code)
                .order_by(desc(StockWeekly.date))
                .limit(days)
            ).scalars().all()
            return list(results)

    def get_weekly_data_range(self, code: str, start_date: date, end_date: date) -> List[StockWeekly]:
        """获取一段时间的周线数据(按日期降序排列)"""
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockWeekly)
                .where(
                    and_(
                        StockWeekly.code == code,
                        StockWeekly.date >= start_date,
                        StockWeekly.date <= end_date
                    )
                )
                .order_by(desc(StockWeekly.date))
            ).scalars().all()
            return list(results)

    def get_latest_month_data(self, code: str, days: int = 2) -> List[StockMonth]:
        """获取N天的月线数据（按照日期降序排列）"""
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockMonth)
                .where(StockMonth.code == code)
                .order_by(desc(StockMonth.date))
                .limit(days)
            ).scalars().all()
            return list(results)

    def get_month_data_range(self, code: str, start_date: date, end_date: date) -> List[StockMonth]:
        """获取N天的月线数据（按照日期降序排列）"""
        with self._SessionLocal() as session:
            results = session.execute(
                select(StockMonth)
                .where(
                    and_(
                        StockMonth.code == code,
                        StockMonth.date >= start_date,
                        StockMonth.date <= end_date
                    )
                )
                .order_by(desc(StockMonth.date))
            ).scalars().all()
            return list(results)

    def save_daily_data(
            self,
            df: pd.DataFrame,
            code: str,
            data_source: str = "Unknown"
    ) -> int:
        """
        保存日线数据到数据库（支持UPSERT操作）

        设计模式：UPSERT (Update or Insert) + 批处理 (Batch Processing)

        核心功能：
        1. 将Pandas DataFrame中的数据保存到数据库
        2. 智能更新：存在则更新，不存在则插入
        3. 事务安全：保证数据一致性
        4. 性能优化：批处理减少数据库交互

        技术实现：手动实现UPSERT逻辑
        1. 遍历DataFrame的每一行
        2. 对每一行检查是否已存在（通过code+date唯一标识）
        3. 如果存在：更新现有记录
        4. 如果不存在：插入新记录
        5. 所有操作在一个事务中提交

        为什么手动实现UPSERT？
        1. SQLite不支持原生UPSERT语法（INSERT ... ON CONFLICT）
        2. 需要更细粒度的控制（更新部分字段而非全部）
        3. 需要记录数据来源和更新时间
        4. 需要统计新增记录数

        数据流：
        Pandas DataFrame → 数据清洗 → 逐行处理 → 数据库

        性能优化策略：
        1. 批量提交：所有操作在一个事务中提交，减少IO
        2. 索引优化：利用(code, date)索引快速检查存在性
        3. 内存优化：逐行处理避免一次性加载所有数据到内存
        4. 连接复用：使用同一个数据库会话

        错误处理：
        1. 空数据检查：如果DataFrame为空，直接返回0
        2. 事务回滚：任何异常都回滚整个事务
        3. 详细日志：记录成功和失败信息
        4. 异常传播：抛出原始异常供调用者处理

        使用场景：
            从API获取数据后保存：
            data = fetch_stock_data('600519', '2026-01-01', '2026-01-15')
            saved_count = db.save_daily_data(data, '600519', 'AkshareFetcher')

        数据格式要求：
        DataFrame必须包含以下列（名称需匹配）：
        • date: 日期（支持str/datetime/pd.Timestamp格式）
        • open, high, low, close: OHLC价格
        • volume, amount: 成交量和成交额
        • pct_chg: 涨跌幅
        • ma5, ma10, ma20, ma50, ma120, ma200: 移动平均线
        • volume_ratio: 量比

        Args:
            df: 包含日线数据的Pandas DataFrame
                不能为None或空，否则直接返回0
                支持多种日期格式：str、datetime、pd.Timestamp
            code: 股票代码，如 '600519'
                用于标识数据所属的股票
            data_source: 数据来源名称，如 'AkshareFetcher'
                用于数据质量追踪和问题排查
                默认值：'Unknown'

        Returns:
            int: 新增的记录数（不包括更新的记录）
                返回0表示：1) 数据为空 2) 所有数据已存在（只更新不新增）

        时间复杂度：O(n × log m)，n为DataFrame行数，m为表中记录数
        空间复杂度：O(1)（除了输入DataFrame）

        Raises:
            Exception: 保存过程中任何错误都会抛出，事务自动回滚
        """
        # 前置检查：确保输入数据有效
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数

        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = row.get('date')

                    # 情况1：字符串格式，如 "2026-01-15"
                    if isinstance(row_date, str):
                        # datetime.strptime: 字符串解析为datetime对象
                        # .date(): 提取日期部分（去除时间）
                        row_date = datetime.strptime(row_date, '%Y-%m-%d').date()

                    # 情况2：datetime对象（直接使用日期部分）
                    elif isinstance(row_date, datetime):
                        row_date = row_date.date()

                    # 情况3：Pandas Timestamp对象（转换为datetime再提取日期）
                    elif isinstance(row_date, pd.Timestamp):
                        row_date = row_date.date()

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,  # 股票代码匹配
                                StockDaily.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockDaily(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_week_data(
            self,
            df: pd.DataFrame,
            code: str,
            data_source: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数

        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('date'))
                    end_date = parse_row_date(row.get('end_date'))


                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockWeekly).where(
                            and_(
                                StockWeekly.code == code,  # 股票代码匹配
                                StockWeekly.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.end_date = end_date
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.change = row.get('change')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockWeekly(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期
                            end_date = end_date,

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            change=row.get('change'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_month_data(
            self,
            df: pd.DataFrame,
            code: str,
            data_source: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数

        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('date'))
                    e_date = parse_row_date(row.get('end_date'))


                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockMonth).where(
                            and_(
                                StockMonth.code == code,  # 股票代码匹配
                                StockMonth.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.end_date = e_date
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.change = row.get('change')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockMonth(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期
                            end_date = e_date,

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            change=row.get('change'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_daily_forecast(
            self,
            df: pd.DataFrame,
            code: str,
            forecast_model: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存
        saved_count = 0  # 计数器：记录新增（非更新）的记录数

        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('forecast_date'))

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(DailyForecast).where(
                            and_(
                                DailyForecast.code == code,  # 股票代码匹配
                                DailyForecast.forecast_date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.practice_rue = row.get('practice_rue')
                        forecast_model = forecast_model
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = DailyForecast(
                            # 标识字段
                            code=code,  # 股票代码
                            foreccst_date=row_date,  # 交易日期
                            forecast_rue = row.get('forecast_rue'),
                            practice_rue=row.get('practice_rue'),
                            forecast_model=row.get('forecast_model'),
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        return saved_count



def parse_row_date(row):
    row_date = row.get('date')
    if isinstance(row_date, str):
        row_date = datetime.datetime.strptime(row_date, '%Y-%m-%d').date()
    elif isinstance(row_date, datetime.datetime):
        row_date = row_date.date()
    elif isinstance(row_date, pd.Timestamp):
        row_date = row_date.date()
    return row_date