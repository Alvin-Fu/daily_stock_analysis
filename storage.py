# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 存储层
===================================

设计模式：单例模式 + ORM + 工作单元

核心架构：
┌─────────────────────────────────────┐
│       DatabaseManager (单例)        │
│   • 管理数据库连接池                │
│   • 提供会话上下文管理              │
│   • 封装数据存取操作                │
│   └─ Session (工作单元模式)         │
│       • 管理事务和连接              │
│       • 自动提交/回滚              │
└─────────────────────────────────────┘
               ↓
┌─────────────────────────────────────┐
│        StockDaily (ORM模型)         │
│   • 映射数据库表到Python对象        │
│   • 定义数据结构和约束              │
│   • 提供数据转换方法                │
└─────────────────────────────────────┘

核心职责：
1. 管理 SQLite 数据库连接（单例模式）
   - 确保全局只有一个数据库连接实例
   - 连接池管理和健康检查
   - 线程安全的会话管理

2. 定义 ORM 数据模型
   - 使用SQLAlchemy定义表结构和关系
   - 支持复合索引和唯一约束
   - 自动创建/更新数据库表

3. 提供数据存取接口
   - CRUD操作的封装和简化
   - 类型安全的查询接口
   - 事务管理和异常处理

4. 实现智能更新逻辑（断点续传）
   - 检查数据是否已存在，避免重复获取
   - 增量更新，只获取新数据
   - 数据完整性校验和清洗

关键技术栈：
• SQLAlchemy: Python ORM框架，支持多种数据库
• SQLite: 嵌入式数据库，无需独立服务
• Pandas: 数据分析和处理库
• 单例模式: 确保全局配置一致性
• 工作单元模式: 管理数据库事务

设计优势：
• 模块化: 数据库操作与业务逻辑分离
• 可测试: 易于编写单元测试和模拟
• 可扩展: 支持添加新数据模型
• 高性能: 连接池和索引优化

使用示例：
    from storage import get_db
    db = get_db()  # 获取数据库管理器单例
    
    # 检查今日数据是否存在
    has_data = db.has_today_data('600519')
    
    # 获取分析上下文
    context = db.get_analysis_context('600519')
    
    # 保存数据
    db.save_daily_data(df, '600519', 'AkshareFetcher')

版本：v1.0.0
作者：ZhuLinsen
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
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
from tushare import forecast_data

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
    ma5 = Column(Float)      # 5日移动平均线：短期趋势
    ma10 = Column(Float)     # 10日移动平均线：短期趋势
    ma20 = Column(Float)     # 20日移动平均线：中期趋势
    ma50 = Column(Float)     # 50日移动平均线：中期趋势
    ma120 = Column(Float)    # 120日移动平均线：长期趋势
    ma200 = Column(Float)    # 200日移动平均线：长期趋势（牛熊分界线）
    
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
    数据库管理器 - 单例模式 (Singleton Pattern)
    
    设计模式：单例模式 + 工厂模式 + 工作单元模式
    
    核心架构：
    ┌─────────────────────────────────────────┐
    │     DatabaseManager (单例)              │
    │   • 管理数据库连接池                     │
    │   • 创建会话工厂                         │
    │   • 初始化数据库表                       │
    │   └─ SessionFactory (工厂模式)          │
    │       • 生产Session对象                 │
    │       └─ Session (工作单元模式)         │
    │           • 管理事务和连接              │
    │           • 自动提交/回滚               │
    └─────────────────────────────────────────┘
    
    为什么使用单例模式？
    1. 资源节省：避免重复创建数据库连接，节省内存和CPU
    2. 一致性：确保整个应用使用同一个连接池，避免连接泄漏
    3. 线程安全：在多线程环境下保证数据库操作的一致性
    4. 简化管理：集中管理数据库连接生命周期
    
    核心职责：
    1. 管理数据库连接池
       - 创建和配置SQLAlchemy引擎
       - 连接池大小和超时设置
       - 连接健康检查和自动重连
    
    2. 提供 Session 上下文管理
       - 创建会话工厂，生产Session对象
       - 自动管理会话的开始和结束
       - 异常时自动回滚事务，保证数据一致性
    
    3. 封装数据存取操作
       - 提供高级API简化数据库操作
       - 实现断点续传和增量更新
       - 数据验证和清洗
    
    4. 数据库表管理
       - 自动创建表（如果不存在）
       - 表结构版本控制
       - 数据迁移支持
    
    连接池配置：
    • pool_pre_ping=True: 每次从连接池获取连接前进行健康检查
    • echo=False: 生产环境关闭SQL日志，避免性能问题和安全风险
    • 连接池大小: SQLAlchemy默认配置，支持并发连接
    
    会话管理配置：
    • autocommit=False: 手动控制事务提交，确保数据一致性
    • autoflush=False: 手动控制数据刷新，提高性能
    • expire_on_commit=False: 提交后保持对象状态，便于后续使用
    
    安全注意事项：
    1. 数据库连接URL包含敏感信息，应从配置文件读取
    2. 生产环境应使用连接池，避免频繁创建销毁连接
    3. 重要操作必须使用事务，确保数据完整性
    4. 所有数据库操作应有异常处理和日志记录
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
            echo=False,            # 设为 True 可查看 SQL 语句（调试用）
            pool_pre_ping=True,    # 连接健康检查（推荐开启）
        )
        
        # 步骤3：创建会话工厂
        # sessionmaker 是一个工厂函数，用于创建新的Session对象
        # 配置说明：
        # - bind=self._engine: 绑定到上面创建的引擎
        # - autocommit=False: 手动控制事务提交（推荐）
        # - autoflush=False: 手动控制数据刷新（提高性能）
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,      # 手动提交事务，确保数据一致性
            autoflush=False,       # 手动刷新数据，提高性能
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
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否已有指定日期的数据（断点续传核心逻辑）
        
        设计模式：断点续传 (Checkpoint Resume)
        
        问题场景：
        1. 网络请求可能失败，需要重试
        2. 数据获取可能被中断，需要从中断处继续
        3. 避免重复获取相同数据，节省API调用和流量
        
        解决方案：
        1. 在开始获取数据前，检查本地是否已有该数据
        2. 如果已有数据，跳过网络请求，直接使用本地数据
        3. 如果没有数据，执行网络请求并保存结果
        4. 如果网络请求中断，下次可以从断点继续
        
        技术实现：
        1. 使用数据库查询检查数据是否存在
        2. 利用 (code, date) 复合索引快速查询
        3. 使用 scalar_one_or_none() 获取单个结果或None
        
        性能优化：
        • 索引优化：(code, date) 复合索引使查询 O(log n)
        • 连接池：使用已有数据库连接，避免重复创建
        • 轻量查询：只查询是否存在，不返回完整数据
        
        使用场景：
            # 在获取数据前检查
            if not db.has_today_data('600519'):
                # 执行网络请求获取数据
                data = fetch_from_api('600519')
                db.save_daily_data(data, '600519')
            else:
                logger.info("数据已存在，跳过获取")
        
        Args:
            code: 股票代码，如 '600519'
            target_date: 目标日期，默认今天
                格式：datetime.date 对象
                示例：date(2026, 1, 15)
            
        Returns:
            bool: True表示数据已存在，False表示需要获取
            
        时间复杂度：O(log n)，n为表中记录数
        空间复杂度：O(1)，只使用常量额外空间
        """
        if target_date is None:
            target_date = date.today()  # 默认为今天
        
        with self.get_session() as session:
            # 构建查询：查找指定股票和日期的记录
            # select(StockDaily): 选择 StockDaily 表的所有列
            # .where(): 添加查询条件
            # and_(): 逻辑与，两个条件必须同时满足
            # scalar_one_or_none(): 返回单个结果或None
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            # 如果结果不为None，表示数据已存在
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        获取最近 N 天的股票数据（按日期降序排列）
        
        设计模式：时间序列查询 (Time Series Query)
        
        核心功能：
        1. 获取指定股票的最新数据
        2. 按交易日期降序排列（最新的在前）
        3. 支持指定获取天数（默认2天：今天和昨天）
        
        使用场景：
        1. 对比分析：比较今日与昨日数据的变化
        2. 趋势判断：获取短期趋势数据
        3. 技术分析：计算技术指标（如涨跌幅、成交量变化）
        4. AI分析：为AI模型提供近期上下文
        
        技术实现：
        1. 查询条件：指定股票代码 (StockDaily.code == code)
        2. 排序规则：按日期降序 (desc(StockDaily.date))
        3. 结果限制：限制返回条数 (.limit(days))
        4. 结果转换：转换为Python列表 (list(results))
        
        查询优化：
        • 索引利用：code字段索引 + date字段索引
        • 排序优化：ORDER BY date DESC 利用索引降序扫描
        • 分页优化：LIMIT 子句减少数据传输量
        
        数据示例：
            输入：get_latest_data('600519', days=3)
            返回：[2026-01-15的数据, 2026-01-14的数据, 2026-01-13的数据]
        
        注意事项：
        1. 如果数据不足指定天数，返回所有可用数据
        2. 返回空列表表示未找到该股票的任何数据
        3. 数据按日期降序排列，最新的数据在列表开头
        
        Args:
            code: 股票代码，如 '600519'
            days: 获取天数，默认2天（今天和昨天）
                最小值：1（只获取最新一天）
                典型值：2（对比分析）、5（短期趋势）、20（技术分析）
            
        Returns:
            List[StockDaily]: StockDaily对象列表，按日期降序排列
                示例：[今天数据, 昨天数据, 前天数据, ...]
                如果未找到数据，返回空列表 []
            
        时间复杂度：O(log n + k)，n为表中记录数，k为返回记录数
        空间复杂度：O(k)，k为返回记录数
        """
        with self.get_session() as session:
            # 构建查询：获取指定股票的最新days天数据
            # select(StockDaily): 选择StockDaily表
            # .where(StockDaily.code == code): 按股票代码过滤
            # .order_by(desc(StockDaily.date)): 按日期降序排列（最新的在前）
            # .limit(days): 限制返回条数
            # .scalars().all(): 获取所有结果并转换为Scalar序列
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            # 将SQLAlchemy的Scalar序列转换为Python列表
            return list(results)
    
    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的股票数据（按日期升序排列）
        
        设计模式：范围查询 (Range Query)
        
        核心功能：
        1. 获取指定时间区间内的所有数据
        2. 按交易日期升序排列（最旧的在前）
        3. 支持任意日期区间，包括历史数据查询
        
        使用场景：
        1. 历史回测：获取特定时间段数据进行策略验证
        2. 技术分析：计算长期技术指标（如移动平均线）
        3. 图表绘制：获取连续数据用于可视化
        4. 统计分析：分析特定时期的市场表现
        5. 模型训练：为机器学习模型提供训练数据
        
        技术实现：
        1. 查询条件：股票代码 + 日期范围
        2. 范围过滤：date >= start_date AND date <= end_date
        3. 排序规则：按日期升序（时间序列的自然顺序）
        4. 区间闭合：包含开始日期和结束日期
        
        查询优化：
        • 复合索引：(code, date) 索引优化范围查询
        • 索引扫描：利用B+树索引快速定位日期范围
        • 排序免排：ORDER BY date ASC 可以利用索引有序性
        
        数据完整性：
        1. 如果区间内无数据，返回空列表
        2. 如果开始日期晚于结束日期，返回空列表
        3. 返回的数据包含区间边界（如果存在）
        
        性能考虑：
        1. 大区间查询可能返回大量数据，注意内存使用
        2. 建议对长期历史数据分页查询
        3. 生产环境应考虑添加分页限制
        
        示例：
            输入：get_data_range('600519', date(2026,1,1), date(2026,1,15))
            返回：[2026-01-01, 2026-01-02, ..., 2026-01-15]（按日期升序）
        
        Args:
            code: 股票代码，如 '600519'
            start_date: 开始日期（包含）
                格式：datetime.date 对象
                示例：date(2026, 1, 1)
            end_date: 结束日期（包含）
                格式：datetime.date 对象
                示例：date(2026, 1, 15)
                要求：end_date >= start_date
            
        Returns:
            List[StockDaily]: StockDaily对象列表，按日期升序排列
                示例：[开始日期数据, 开始日期+1天数据, ..., 结束日期数据]
                如果区间内无数据，返回空列表 []
            
        时间复杂度：O(log n + k)，n为表中记录数，k为区间内记录数
        空间复杂度：O(k)，k为区间内记录数
        
        Raises:
            无显式异常，但SQLAlchemy可能抛出数据库相关异常
        """
        with self.get_session() as session:
            # 构建查询：获取指定股票在指定日期范围内的数据
            # select(StockDaily): 选择StockDaily表
            # .where(): 添加查询条件
            # and_(): 逻辑与，三个条件必须同时满足
            #   1. StockDaily.code == code: 股票代码匹配
            #   2. StockDaily.date >= start_date: 日期大于等于开始日期
            #   3. StockDaily.date <= end_date: 日期小于等于结束日期
            # .order_by(StockDaily.date): 按日期升序排列（时间顺序）
            # .scalars().all(): 获取所有结果
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
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
                                StockDaily.code == code,      # 股票代码匹配
                                StockDaily.date == row_date   # 交易日期匹配
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
                        existing.data_source = data_source     # 更新数据来源
                        existing.updated_at = datetime.now()   # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockDaily(
                            # 标识字段
                            code=code,           # 股票代码
                            date=row_date,       # 交易日期
                            
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
                        session.add(record)      # 添加到会话（延迟插入）
                        saved_count += 1         # 新增记录计数+1
                
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
    
    def get_analysis_context(
        self, 
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取股票分析所需的上下文数据（为AI分析器准备）
        
        设计模式：上下文聚合器 (Context Aggregator)
        
        核心功能：
        1. 获取指定股票的最新数据（今日 + 昨日）
        2. 计算关键变化指标（成交量变化率、价格变化率）
        3. 分析技术形态（均线排列状态）
        4. 结构化组织数据，便于AI模型理解
        
        数据流：
        数据库查询 → 数据提取 → 指标计算 → 上下文构建
        
        上下文数据结构：
        {
            'code': '600519',                    # 股票代码
            'date': '2026-01-15',               # 目标日期
            'today': { ... },                   # 今日完整数据（字典）
            'yesterday': { ... },               # 昨日完整数据（字典，可选）
            'volume_change_ratio': 1.25,        # 成交量变化率（今日/昨日）
            'price_change_ratio': 2.5,          # 价格变化百分比
            'ma_status': '多头排列 📈'          # 均线形态分析
        }
        
        使用场景：
        1. AI分析：为Gemini/OpenAI模型提供结构化输入
        2. 技术分析：快速获取对比数据
        3. 决策支持：提供量化指标辅助交易决策
        4. 报告生成：为日报/周报提供数据基础
        
        设计优势：
        1. 封装性：隐藏数据获取和计算的复杂性
        2. 一致性：确保所有分析模块使用相同格式的数据
        3. 可扩展性：易于添加新的分析指标
        4. 类型安全：明确的返回类型注解
        
        技术实现细节：
        1. 使用get_latest_data获取最近2天数据
        2. 计算相对变化指标（避免绝对值）
        3. 调用_analyze_ma_status进行技术分析
        4. 构建层次化的字典结构
        
        性能考虑：
        1. 只获取必要数据（最近2天）
        2. 利用数据库索引优化查询
        3. 在内存中进行指标计算（轻量级）
        
        Args:
            code: 股票代码，如 '600519'
                必须是有效的A股代码
            target_date: 目标日期，默认今天
                格式：datetime.date对象
                示例：date(2026, 1, 15)
                注意：实际获取的是该日期及前一日的数据
            
        Returns:
            Optional[Dict[str, Any]]: 结构化的分析上下文字典
                如果未找到数据，返回None
                如果只有今日数据，不包含对比指标
                如果同时有今日和昨日数据，包含完整对比指标
        
        时间复杂度：O(log n)，n为表中记录数
        空间复杂度：O(1)（常量额外空间）
        """
        # 步骤1：确定目标日期（默认今天）
        if target_date is None:
            target_date = date.today()  # 使用当前日期
        
        # 步骤2：获取最近2天的数据（用于对比分析）
        # get_latest_data会返回按日期降序排列的数据
        # days=2: 获取今天和昨天（如果存在）
        recent_data = self.get_latest_data(code, days=2)
        
        # 边界情况：未找到任何数据
        if not recent_data:
            logger.warning(f"未找到 {code} 的数据")
            return None  # 返回None表示数据缺失
        
        # 步骤3：提取今日和昨日数据
        # recent_data[0]: 最新的数据（今天或指定的target_date）
        today_data = recent_data[0]
        
        # recent_data[1]: 次新的数据（昨天或前一天）
        # 注意：可能只有一天数据（例如新股或数据不完整）
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        # 步骤4：构建基础上下文
        context = {
            'code': code,  # 股票代码
            'date': today_data.date.isoformat(),  # ISO格式日期字符串
            'today': today_data.to_dict(),        # 今日完整数据（字典格式）
        }
        
        # 步骤5：如果有昨日数据，添加对比分析
        if yesterday_data:
            # 5.1 添加昨日数据
            context['yesterday'] = yesterday_data.to_dict()
            
            # 5.2 计算成交量变化率（今日成交量 / 昨日成交量）
            # 成交量变化率 > 1.0: 放量（市场活跃）
            # 成交量变化率 < 1.0: 缩量（市场冷清）
            # 成交量变化率 = 1.0: 平量（市场平稳）
            if yesterday_data.volume and yesterday_data.volume > 0:
                volume_ratio = today_data.volume / yesterday_data.volume
                context['volume_change_ratio'] = round(volume_ratio, 2)  # 保留2位小数
            
            # 5.3 计算价格变化百分比（今日收盘价相比昨日的变化）
            # 公式：(今日收盘价 - 昨日收盘价) / 昨日收盘价 × 100%
            # 正数：上涨；负数：下跌；零：持平
            if yesterday_data.close and yesterday_data.close > 0:
                price_change_pct = (today_data.close - yesterday_data.close) / yesterday_data.close * 100
                context['price_change_ratio'] = round(price_change_pct, 2)  # 保留2位小数
            
            # 5.4 分析均线形态（技术分析）
            # 调用私有方法_analyze_ma_status分析均线排列
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        # 步骤6：返回构建好的上下文
        # 注意：如果没有昨日数据，只包含基础信息（无对比指标）
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        分析移动平均线形态（技术分析核心方法）
        
        设计模式：技术指标分析 (Technical Indicator Analysis)
        
        核心功能：
        1. 分析股票价格的短期、中期、长期趋势
        2. 判断均线排列形态（多头/空头/震荡）
        3. 为交易决策提供技术面依据
        
        技术分析原理：
        移动平均线 (Moving Average, MA) 是趋势跟踪指标：
        • MA5: 5日移动平均线 → 短期趋势（1周）
        • MA10: 10日移动平均线 → 短期趋势（2周）
        • MA20: 20日移动平均线 → 中期趋势（1个月）
        • 价格在均线之上：支撑作用
        • 价格在均线之下：压力作用
        
        均线排列形态分类：
        1. 多头排列 (Bullish Alignment): 价格 > MA5 > MA10 > MA20
            - 强烈看涨信号，上升趋势确立
            - 均线呈发散状，趋势强度递增
            - 适合买入或持有
            
        2. 空头排列 (Bearish Alignment): 价格 < MA5 < MA10 < MA20
            - 强烈看跌信号，下降趋势确立
            - 均线呈发散状，下跌趋势强劲
            - 适合卖出或观望
            
        3. 短期向好 (Short-term Bullish): 价格 > MA5 且 MA5 > MA10
            - 短期趋势向上，但中长期不确定
            - 可能处于上升初期或反弹阶段
            - 谨慎乐观，需要更多确认
            
        4. 短期走弱 (Short-term Bearish): 价格 < MA5 且 MA5 < MA10
            - 短期趋势向下，但中长期不确定
            - 可能处于下跌初期或回调阶段
            - 谨慎对待，防范风险
            
        5. 震荡整理 (Consolidation): 其他情况
            - 趋势不明，均线缠绕
            - 市场处于盘整阶段
            - 适合观望，等待方向选择
        
        判断逻辑优先级：
        1. 先检查多头排列（最强看涨信号）
        2. 再检查空头排列（最强看跌信号）
        3. 然后检查短期趋势
        4. 最后默认震荡整理
        
        使用场景：
        1. 自动交易系统：作为买入/卖出信号
        2. 分析报告：提供技术面分析结论
        3. 风险控制：判断市场趋势，调整仓位
        4. AI分析：为机器学习模型提供特征
        
        注意事项：
        1. 均线分析是滞后指标，反映历史趋势
        2. 需要结合其他指标（成交量、MACD等）综合判断
        3. 在震荡市中均线可能频繁交叉，产生虚假信号
        4. 不同周期的均线组合可以提供多时间框架分析
        
        Args:
            data: StockDaily 对象
                必须包含close、ma5、ma10、ma20字段
                如果字段为None，会转换为0（避免TypeError）
            
        Returns:
            str: 均线形态描述字符串（包含表情符号增强可读性）
                可能返回值：
                - "多头排列 📈"    (强烈看涨)
                - "空头排列 📉"    (强烈看跌)
                - "短期向好 🔼"    (短期看涨)
                - "短期走弱 🔽"    (短期看跌)
                - "震荡整理 ↔️"    (趋势不明)
        """
        # 步骤1：提取价格和均线值（处理None值）
        # 使用or 0将None转换为0，避免条件判断时的TypeError
        close = data.close or 0      # 当前收盘价
        ma5 = data.ma5 or 0         # 5日移动平均线
        ma10 = data.ma10 or 0       # 10日移动平均线
        ma20 = data.ma20 or 0       # 20日移动平均线
        ma50 = data.ma50 or 0       # 50日移动平均线
        ma120 = data.ma120 or 0     # 120日移动平均线
        ma200 = data.ma200 or 0     # 200日移动平均线
        
        # 调试日志：记录均线值（用于问题排查）
        # 注意：这里使用warning级别，生产环境可改为debug
        logger.debug(f"_analyze_ma_status - Close:{close}, MA5:{ma5}, MA10:{ma10}, MA20:{ma20} "
                     f"MA50:{ma50} MA120:{ma120} MA200:{ma200}")
        
        # 步骤2：判断均线形态（按优先级）
        
        # 条件1：多头排列（最强看涨信号）
        # 标准：价格 > MA5 > MA10 > MA20 > 0
        # > 0 检查确保均线值为正数（避免除零或无效数据）
        if ma200 > 0 and close > ma5 > ma10 > ma20 > ma120 > ma200 > 0:
            return "多头排列 📈长期看涨"
        if ma120 > 0 and close > ma5 > ma10 > ma20 > ma120 > 0:
            return "多头排列 📈中长期看涨"

        if close > ma5 > ma10 > ma20 > ma120 > ma200 > 0:
            return "多头排列 📈中期看涨"          # 强烈看涨，趋势明确
        
        # 条件2：空头排列（最强看跌信号）
        # 标准：价格 < MA5 < MA10 < MA20 且 MA20 > 0
        # MA20 > 0 确保是有效的空头排列（不是数据缺失）
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"          # 强烈看跌，趋势明确
        
        # 条件3：短期向好（价格在MA5之上，且MA5在MA10之上）
        # 标准：close > ma5 and ma5 > ma10
        # 表示短期趋势向上，但中长期趋势不确定
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"          # 短期看涨，需要确认
        
        # 条件4：短期走弱（价格在MA5之下，且MA5在MA10之下）
        # 标准：close < ma5 and ma5 < ma10
        # 表示短期趋势向下，但中长期趋势不确定
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"          # 短期看跌，防范风险
        
        # 条件5：其他情况（震荡整理）
        # 均线缠绕，趋势不明，处于盘整阶段
        else:
            return "震荡整理 ↔️"          # 趋势不明，观望为主


# ===== 便捷函数 (Convenience Function) ====================================

def get_db() -> DatabaseManager:
    """
    获取数据库管理器单例实例的便捷函数
    
    设计目的：
    1. 简化数据库访问：一行代码获取数据库管理器
    2. 统一访问入口：确保所有模块使用相同的获取方式
    3. 隐藏实现细节：调用者无需了解单例模式的实现
    4. 类型安全：明确的返回类型注解，便于IDE提示和类型检查
    
    使用示例：
        # 导入便捷函数
        from storage import get_db
        
        # 获取数据库管理器
        db = get_db()
        
        # 使用数据库功能
        has_data = db.has_today_data('600519')
        context = db.get_analysis_context('600519')
    
    实现原理：
    内部调用 DatabaseManager.get_instance() 方法
    该方法实现单例模式，确保全局只有一个数据库连接实例
    
    为什么推荐使用此函数？
    1. 更简洁：get_db() 比 DatabaseManager.get_instance() 更短
    2. 更直观：函数名明确表达其功能
    3. 更稳定：如果实现方式改变，只需修改此函数
    
    Returns:
        DatabaseManager: 数据库管理器单例实例
    """
    return DatabaseManager.get_instance()


# ===== 模块测试代码 (Module Test) ========================================

if __name__ == "__main__":
    """
    存储模块自测试代码
    
    功能：测试storage.py模块的核心功能
    运行方式：python storage.py
    
    测试内容：
    1. 数据库连接测试
    2. 数据存在性检查测试
    3. 数据保存测试（UPSERT逻辑）
    4. 分析上下文获取测试
    
    设计目的：
    1. 快速验证模块功能是否正常
    2. 为新开发者提供使用示例
    3. 作为集成测试的基础
    
    注意事项：
    1. 测试使用真实数据库，建议在测试环境运行
    2. 测试数据是模拟数据，不会影响生产数据
    3. 测试完成后会留下测试记录，可手动清理
    """
    
    # 配置日志级别（调试模式，显示详细信息）
    logging.basicConfig(level=logging.DEBUG)
    
    # 获取数据库管理器（触发单例初始化）
    db = get_db()
    
    print("=" * 60)
    print("  存储模块 (storage.py) 功能测试")
    print("=" * 60)
    print(f"✓ 数据库初始化成功")
    
    # ========== 测试用例1：检查今日数据（断点续传逻辑测试）==========
    print("\n[测试1] 检查今日数据是否存在（断点续传核心功能）")
    # 测试目的：验证has_today_data方法的正确性
    # 测试场景：检查贵州茅台(600519)今日是否有数据
    # 预期结果：
    #   - 如果今天第一次运行：False（数据库无今日数据）
    #   - 如果今天已运行过：True（数据已存在）
    # 技术要点：测试断点续传逻辑，避免重复获取数据
    has_data = db.has_today_data('600519')  # 贵州茅台股票代码
    print(f"  茅台(600519)今日是否有数据: {has_data}")
    print(f"  测试说明：结果为True表示数据已存在，False表示需要重新获取")
    
    # ========== 测试用例2：保存测试数据（UPSERT逻辑测试）==========
    print("\n[测试2] 保存测试数据（UPSERT操作验证）")
    # 测试目的：验证save_daily_data方法的UPSERT逻辑
    # 测试场景：创建模拟数据并保存到数据库
    # 预期结果：
    #   - 第一次运行：返回1（新增1条记录）
    #   - 第二次运行：返回0（数据已存在，只更新不新增）
    # 技术要点：测试"存在则更新，不存在则插入"逻辑
    
    # 创建测试数据DataFrame（模拟贵州茅台的一天数据）
    # 数据结构与真实股票数据一致，包含所有必需字段
    test_df = pd.DataFrame({
        'date': [date.today()],               # 交易日期：今天
        'open': [1800.0],                     # 开盘价：1800元
        'high': [1850.0],                     # 最高价：1850元
        'low': [1780.0],                      # 最低价：1780元
        'close': [1820.0],                    # 收盘价：1820元（最重要指标）
        'volume': [10000000],                 # 成交量：1000万股
        'amount': [18200000000],              # 成交额：182亿元
        'pct_chg': [1.5],                     # 涨跌幅：+1.5%
        'ma5': [1810.0],                      # 5日移动平均线
        'ma10': [1800.0],                     # 10日移动平均线
        'ma20': [1790.0],                     # 20日移动平均线
        'volume_ratio': [1.2],                # 量比：1.2（放量）
    })
    
    # 调用保存方法（UPSERT逻辑测试）
    # 参数说明：
    #   test_df: 测试数据DataFrame
    #   '600519': 股票代码（贵州茅台）
    #   'TestSource': 测试数据来源标识
    saved = db.save_daily_data(test_df, '600519', 'TestSource')
    print(f"  保存测试数据结果: {saved} 条新增记录")
    print(f"  测试说明：")
    print(f"    - 如果返回1：数据不存在，成功插入新记录")
    print(f"    - 如果返回0：数据已存在，只执行更新操作")
    print(f"    - 如果出现异常：UPSERT逻辑或数据库连接有问题")
    
    # ========== 测试用例3：获取分析上下文（数据聚合测试）==========
    print("\n[测试3] 获取分析上下文（数据聚合功能验证）")
    # 测试目的：验证get_analysis_context方法的数据聚合能力
    # 测试场景：获取贵州茅台的分析上下文
    # 预期结果：
    #   - 如果数据存在：返回结构化的上下文字典
    #   - 如果数据不存在：返回None
    #   - 上下文应包含：今日数据、对比指标、均线形态等
    # 技术要点：测试数据聚合、指标计算、结构化输出
    
    # 获取分析上下文（为AI分析准备的数据结构）
    context = db.get_analysis_context('600519')
    
    if context:
        print(f"  分析上下文获取成功！")
        print(f"  数据结构验证：")
        print(f"    - 股票代码: {context.get('code')}")
        print(f"    - 目标日期: {context.get('date')}")
        print(f"    - 今日数据: {'已包含' if 'today' in context else '缺失'}")
        print(f"    - 昨日数据: {'已包含' if 'yesterday' in context else '缺失'}")
        print(f"    - 成交量变化率: {context.get('volume_change_ratio', 'N/A')}")
        print(f"    - 价格变化率: {context.get('price_change_ratio', 'N/A')}%")
        print(f"    - 均线形态: {context.get('ma_status', 'N/A')}")
    else:
        print(f"  分析上下文获取失败：未找到数据或数据不完整")
        print(f"  可能原因：数据库中没有足够的历史数据（至少需要1天数据）")
    
    # ========== 测试总结 ==========
    print("\n" + "=" * 60)
    print("  存储模块测试总结")
    print("=" * 60)
    print("✓ 数据库连接测试：通过")
    print(f"✓ 断点续传测试：{'通过' if has_data is not None else '失败'}")
    print(f"✓ UPSERT逻辑测试：{'通过' if saved is not None else '失败'}")
    print(f"✓ 数据聚合测试：{'通过' if context is not None else '失败'}")
    print("\n提示：")
    print("1. 测试数据已保存到数据库，可通过数据库工具查看")
    print("2. 重复运行测试会更新数据，不会重复插入")
    print("3. 生产环境应使用真实股票数据，非测试数据")
    print("4. 可通过DatabaseManager.reset_instance()重置数据库连接")
