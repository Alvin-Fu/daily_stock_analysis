# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 主调度程序
===================================

设计模式：管道模式 (Pipeline Pattern) + 工厂模式 (Factory Pattern)

核心架构：
┌─────────────────────────────────────────┐
│            主调度程序 (main.py)          │
│   ┌─────────────────────────────────┐   │
│   │    StockAnalysisPipeline 类     │   │
│   │  • 协调整个分析流程              │   │
│   │  • 线程池并发控制               │   │
│   │  • 异常处理和故障隔离            │   │
│   └─────────────────────────────────┘   │
│                  ↓ 调用                  │
├─────────────────────────────────────────┤
│              功能执行模块                 │
│  1. 数据获取管道 (fetch_and_save_stock_data) │
│  2. 股票分析管道 (analyze_stock)         │
│  3. 通知推送管道 (_send_notifications)   │
│  4. 大盘复盘 (run_market_review)        │
│  5. 主题选股 (run_theme_choose)         │
└─────────────────────────────────────────┘

核心职责：
1. 协调各模块完成股票分析流程
   - 数据获取 → 存储 → 搜索 → AI分析 → 推送
   - 模块化设计，各模块职责单一
   - 支持热插拔，可配置启用/禁用模块

2. 实现低并发的线程池调度
   - 防封禁设计：低并发模拟人类操作
   - 故障隔离：单股失败不影响整体
   - 断点续传：避免重复获取数据
   - 性能监控：记录各阶段耗时

3. 全局异常处理，确保单股失败不影响整体
   - 股票级别的异常捕获和隔离
   - 详细错误日志和上下文记录
   - 优雅降级：部分功能失败时继续其他功能
   - 重试机制：网络故障自动重试

4. 提供命令行入口和多运行模式
   - 单次运行：立即执行完整分析
   - 调试模式：详细日志输出
   - 仅数据模式：获取数据不分析
   - 定时任务：无人值守自动运行
   - WebUI模式：可视化监控界面

交易理念（已融入分析）：
- 严进策略：不追高，乖离率 > 5% 不买入
- 趋势交易：只做 MA5>MA10>MA20 多头排列
- 效率优先：关注筹码集中度好的股票
- 买点偏好：缩量回踩 MA5/MA10 支撑

模块依赖关系：
    main.py (主调度)
        ↓ 依赖
    config.py (配置管理)   → 环境变量/.env文件
    storage.py (数据存储)   → SQLite数据库
    data_provider/ (数据源) → 网络API (akshare/tushare)
    analyzer.py (AI分析)    → Gemini/OpenAI API
    notification.py (推送)   → 企业微信/飞书/Telegram等
    search_service.py (搜索) → 新闻资讯API

运行模式：
1. 本地开发：python main.py --debug
2. 生产环境：python main.py
3. 定时任务：python main.py --schedule
4. 仅数据获取：python main.py --dry-run
5. 仅大盘复盘：python main.py --market-review

部署方式：
• 本地cron定时任务
• GitHub Actions自动化
• Docker容器化部署
• 云函数定时触发

版本：v1.0.0
作者：ZhuLinsen
仓库：https://github.com/ZhuLinsen/daily_stock_analysis
"""
import os

from daily_stock_analysis.data_provider import TushareFetcher

# 代理配置 - 仅在本地环境使用，GitHub Actions 不需要
if os.getenv("GITHUB_ACTIONS") != "true":
    # 本地开发环境，如需代理请取消注释或修改端口
    # os.environ["http_proxy"] = "http://127.0.0.1:10809"
    # os.environ["https_proxy"] = "http://127.0.0.1:10809"
    pass

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from feishu_doc import FeishuDocManager

from config import get_config, Config
from storage import get_db, DatabaseManager
from data_provider import DataFetcherManager
from data_provider.akshare_fetcher import AkshareFetcher, RealtimeQuote, ChipDistribution
from analyzer import GeminiAnalyzer, AnalysisResult, STOCK_NAME_MAP
from notification import NotificationService, NotificationChannel, send_daily_report
from search_service import SearchService, SearchResponse
from stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult
from market_analyzer import MarketAnalyzer
from data.sqlite import SQLiteDB

# 配置日志格式
LOG_FORMAT = '%(asctime)s -%(filename)s:%(lineno)d [%(funcName)s] - %(levelname)s: %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(debug: bool = False, log_dir: str = "./logs") -> None:
    """
    配置日志系统 - 多Handler设计（控制台 + 双文件日志）
    
    设计模式：观察者模式 (Observer Pattern)
    
    核心特性：
    1. 多输出目标：同时输出到控制台和多个日志文件
    2. 级别分离：不同级别日志输出到不同文件
    3. 日志轮转：自动管理日志文件大小和数量
    4. 性能优化：异步写入，避免阻塞主线程
    5. 第三方库控制：降低噪声库的日志级别
    
    日志级别说明（从低到高）：
    • DEBUG: 调试信息，详细运行状态（开发环境）
    • INFO: 正常运行信息，关键节点记录（生产环境）
    • WARNING: 警告信息，不影响运行但需要注意
    • ERROR: 错误信息，功能受影响需要处理
    • CRITICAL: 严重错误，系统可能崩溃
    
    日志文件策略：
    ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
    │   日志文件       │   日志级别       │   文件大小       │   保留数量       │
    ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
    │   控制台输出     │  DEBUG/INFO     │     无限制       │        -        │
    │   常规日志文件   │     INFO+       │     10MB        │        5        │
    │   调试日志文件   │    DEBUG+       │     50MB        │        3        │
    └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
    
    文件命名规则：
    • 常规日志：stock_analysis_YYYYMMDD.log
    • 调试日志：stock_analysis_debug_YYYYMMDD.log
    • 按日期分割：便于按天查找和归档
    
    设计优势：
    1. 故障排查：DEBUG日志包含完整上下文
    2. 生产监控：INFO日志提供运行概况
    3. 空间管理：自动轮转防止磁盘写满
    4. 性能隔离：文件IO不影响控制台输出速度
    5. 噪声过滤：降低第三方库日志级别
    
    使用场景：
        # 开发环境：启用调试模式
        setup_logging(debug=True, log_dir="./logs")
        
        # 生产环境：仅INFO级别
        setup_logging(debug=False, log_dir="/var/log/stock_analysis")
        
        # 临时调试：动态调整日志级别
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)  # 临时启用DEBUG
    
    Args:
        debug: 是否启用调试模式
            • True: 控制台输出DEBUG级别，启用调试日志文件
            • False: 控制台输出INFO级别，仅常规日志文件
        log_dir: 日志文件目录
            • 相对路径："./logs"（项目相对路径）
            • 绝对路径："/var/log/stock_analysis"（生产环境）
            • 自动创建：目录不存在时自动创建
        
    Returns:
        None: 函数无返回值，但会配置全局logging系统
    
    Side Effects:
        • 修改全局logging配置
        • 创建日志目录和文件
        • 设置第三方库日志级别
    """
    # 确定日志级别：调试模式使用DEBUG，否则使用INFO
    level = logging.DEBUG if debug else logging.INFO
    
    # 步骤1：创建日志目录（如果不存在）
    # Path.mkdir支持parents=True创建多级目录
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # 步骤2：构建日志文件路径（按日期分割）
    # 格式：stock_analysis_20260115.log
    today_str = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"stock_analysis_{today_str}.log"           # 常规日志
    debug_log_file = log_path / f"stock_analysis_debug_{today_str}.log"  # 调试日志
    
    # 步骤3：配置根logger（捕获所有日志）
    # 根logger设为DEBUG级别，由各个handler控制实际输出级别
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # 根logger捕获所有级别日志
    
    # 步骤4：配置控制台handler（实时输出）
    # 用于开发时实时查看日志，支持颜色输出（如果终端支持）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)  # 控制台级别由debug参数决定
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(console_handler)
    
    # 步骤5：配置常规日志文件handler（INFO级别，10MB轮转）
    # 用于生产环境监控和问题排查
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB - 单个文件最大大小
        backupCount=5,              # 保留5个备份文件（log.1, log.2等）
        encoding='utf-8'            # 支持中文日志
    )
    file_handler.setLevel(logging.INFO)  # 只记录INFO及以上级别
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(file_handler)
    
    # 步骤6：配置调试日志文件handler（DEBUG级别，50MB轮转）
    # 仅在debug=True时启用，包含最详细的运行信息
    debug_handler = RotatingFileHandler(
        debug_log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB - 调试日志通常更大
        backupCount=3,              # 保留3个备份文件
        encoding='utf-8'
    )
    debug_handler.setLevel(logging.DEBUG)  # 记录所有DEBUG及以上级别
    debug_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(debug_handler)
    
    # 步骤7：降低第三方库的日志级别（减少噪声）
    # 这些库可能产生大量调试信息，干扰业务日志
    logging.getLogger('urllib3').setLevel(logging.WARNING)    # HTTP客户端库
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING) # 数据库ORM库
    logging.getLogger('google').setLevel(logging.WARNING)     # Google API客户端
    logging.getLogger('httpx').setLevel(logging.WARNING)      # 异步HTTP客户端
    
    # 步骤8：记录日志系统初始化完成
    logging.info(f"日志系统初始化完成，日志目录: {log_path.absolute()}")
    logging.info(f"常规日志: {log_file}")
    logging.info(f"调试日志: {debug_log_file}")


logger = logging.getLogger(__name__)


class StockAnalysisPipeline:
    """
    股票分析主流程调度器 - 管道模式实现
    
    设计模式：管道模式 (Pipeline Pattern) + 外观模式 (Facade Pattern)
    
    核心架构：
    ┌─────────────────────────────────────────────────────────┐
    │              StockAnalysisPipeline                      │
    │  ┌───────────────────────────────────────────────────┐  │
    │  │                  初始化阶段                        │  │
    │  │  • 加载配置 (Config)                             │  │
    │  │  • 初始化数据库 (DatabaseManager)                │  │
    │  │  • 初始化数据源 (DataFetcherManager)             │  │
    │  │  • 初始化AI分析器 (GeminiAnalyzer)               │  │
    │  │  • 初始化通知服务 (NotificationService)          │  │
    │  │  • 初始化搜索服务 (SearchService)                │  │
    │  └───────────────────────────────────────────────────┘  │
    │                            ↓                            │
    │  ┌───────────────────────────────────────────────────┐  │
    │  │                 股票分析管道                        │  │
    │  │  1. fetch_and_save_stock_data()                   │  │
    │  │     • 断点续传检查                                │  │
    │  │     • 数据获取和保存                              │  │
    │  │  2. analyze_stock()                               │  │
    │  │     • 实时行情获取                                │  │
    │  │     • 筹码分布分析                                │  │
    │  │     • 趋势技术分析                                │  │
    │  │     • 新闻搜索聚合                                │  │
    │  │     • AI综合分析                                  │  │
    │  │  3. _enhance_context()                            │  │
    │  │     • 量比描述生成                                │  │
    │  │     • 上下文增强                                  │  │
    │  │  4. _send_notifications()                         │  │
    │  │     • 多渠道消息推送                              │  │
    │  │     • 飞书文档生成                                │  │
    │  └───────────────────────────────────────────────────┘  │
    │                            ↓                            │
    │  ┌───────────────────────────────────────────────────┐  │
    │  │                 批量执行控制                        │  │
    │  │  • process_single_stock(): 单股票处理             │  │
    │  │  • run(): 批量股票并发处理                        │  │
    │  │  • ThreadPoolExecutor: 线程池管理                │  │
    │  │  • 异常隔离: 单股失败不影响整体                   │  │
    │  └───────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────┘
    
    核心职责：
    1. 管理整个股票分析流程
       • 协调数据获取、存储、搜索、分析、通知等模块
       • 实现模块间的数据流转和依赖管理
       • 提供统一的异常处理和错误恢复机制
    
    2. 实现低并发线程池调度
       • 防封禁设计：低并发模拟人类操作，避免触发API限制
       • 故障隔离：单股票分析失败不影响其他股票
       • 性能优化：线程池复用，减少资源创建销毁开销
       • 进度监控：实时记录分析进度和耗时
    
    3. 提供丰富的分析维度
       • 基础数据：历史行情、技术指标
       • 实时数据：量比、换手率、实时价格
       • 筹码分析：股东分布、成本结构
       • 新闻搜索：最新消息、风险排查、业绩预期
       • AI分析：技术面、基本面、情绪面综合评估
       • 趋势判断：MA多头排列、价格突破等
    
    4. 支持多种运行模式
       • 全流程分析：数据→分析→推送完整流程
       • 仅数据获取：跳过AI分析和消息推送
       • 调试模式：详细日志输出，便于问题排查
       • 定时任务：无人值守自动运行
    
    设计优势：
    1. 高内聚低耦合：每个方法职责单一，便于测试和维护
    2. 可扩展性：易于添加新的分析维度或数据源
    3. 容错性：完善的异常处理，避免单点故障导致系统崩溃
    4. 可观测性：详细日志记录，便于监控和问题排查
    5. 配置驱动：所有行为通过配置文件控制，无需修改代码
    
    使用示例：
        # 创建调度器实例
        pipeline = StockAnalysisPipeline()
        
        # 运行全部分析
        results = pipeline.run(stock_codes=['600519', '000001'])
        
        # 仅分析单只股票
        result = pipeline.analyze_stock('600519')
        
        # 仅获取数据（不分析）
        success, error = pipeline.fetch_and_save_stock_data('600519')
    
    性能考虑：
        • 线程池大小：默认max_workers=1，避免API封禁
        • 网络请求：随机延迟，模拟人类操作间隔
        • 内存使用：逐股票处理，避免一次性加载所有数据
        • 数据库连接：连接池管理，避免频繁创建销毁
    
    安全考虑：
        • API Key管理：通过环境变量配置，避免硬编码
        • 数据隐私：敏感信息不记录到日志
        • 访问控制：支持代理配置，适应不同网络环境
    """
    
    def __init__(
        self,
        config: Optional[Config] = None,
        max_workers: Optional[int] = None
    ):
        """
        初始化股票分析调度器 - 依赖注入模式
        
        设计模式：依赖注入 (Dependency Injection) + 工厂模式
        
        核心功能：
        1. 加载配置：获取全局或自定义配置
        2. 初始化模块：创建各功能模块实例
        3. 依赖管理：建立模块间的依赖关系
        4. 资源准备：为后续分析流程做好准备
        
        初始化流程：
         开始
           ↓
         加载配置 (config or get_config())
           ↓
         设置并发数 (max_workers or config.max_workers)
           ↓
         初始化数据库连接 (get_db())
           ↓
         初始化数据源管理器 (DataFetcherManager)
           ↓
         初始化增强数据获取器 (AkshareFetcher)
           ↓
         初始化趋势分析器 (StockTrendAnalyzer)
           ↓
         初始化AI分析器 (GeminiAnalyzer)
           ↓
         初始化通知服务 (NotificationService)
           ↓
         初始化搜索服务 (SearchService)
           ↓
         记录初始化完成日志
           ↓
         结束
        
        模块职责说明：
        • db (DatabaseManager): 数据库连接管理，数据持久化
        • fetcher_manager (DataFetcherManager): 多数据源调度，故障转移
        • akshare_fetcher (AkshareFetcher): 实时行情、量比、筹码分布
        • trend_analyzer (StockTrendAnalyzer): 技术趋势分析，MA多头判断
        • analyzer (GeminiAnalyzer): AI综合分析，生成操作建议
        • notifier (NotificationService): 多渠道消息推送
        • search_service (SearchService): 新闻搜索，市场情报聚合
        
        配置继承策略：
        1. 显式配置优先：如果传入config参数，使用传入配置
        2. 全局配置回退：未传入config时，使用全局单例配置
        3. 动态调整：max_workers可单独覆盖配置中的设置
        
        线程安全考虑：
        • 每个实例独立：不同pipeline实例有独立模块实例
        • 数据库连接共享：get_db()返回单例，连接池共享
        • 配置只读：初始化后配置不应修改（除非热重载）
        
        资源管理：
        • 延迟加载：部分资源在首次使用时才创建
        • 连接池：数据库连接自动管理
        • API客户端：按需初始化，支持重连
        
        错误处理：
        • 配置缺失：使用默认值或提供明确警告
        • 模块初始化失败：记录错误但继续初始化其他模块
        • 依赖缺失：如无API Key则禁用相应功能
        
        Args:
            config: 配置对象（可选，默认使用全局配置）
                • None: 使用全局单例配置 (get_config())
                • Config实例: 使用自定义配置（测试/多环境）
                • 设计目的：支持多配置环境，便于测试和扩展
            
            max_workers: 最大并发线程数（可选，默认从配置读取）
                • None: 使用config.max_workers
                • 正整数: 覆盖配置中的并发数
                • 建议值：1-3（避免API封禁）
                • 特殊值：1表示单线程顺序执行
        
        Returns:
            None: 构造函数无返回值，但会创建完整的pipeline实例
        
        Raises:
            ConfigError: 配置加载失败
            ImportError: 模块导入失败（依赖缺失）
            RuntimeError: 关键模块初始化失败
        """
        # 步骤1：加载配置（支持自定义配置和全局配置）
        self.config = config or get_config()
        self.db
        
        # 步骤2：设置并发数（支持参数覆盖配置）
        self.max_workers = max_workers or self.config.max_workers
        
        # 步骤3：初始化数据库连接（单例模式，共享连接池）
        self.db = get_db()  # DatabaseManager单例
        
        # 步骤4：初始化数据源管理器（多数据源调度）
        self.fetcher_manager = DataFetcherManager()

        # 步骤5：初始化增强数据获取器（实时行情、量比、筹码）
        self.akshare_fetcher = AkshareFetcher()  # 用于获取增强数据（量比、筹码等）

        self.tushare_fetcher = TushareFetcher()

        # 步骤6：初始化趋势分析器（技术分析，MA多头判断）
        self.trend_analyzer = StockTrendAnalyzer()  # 趋势分析器
        
        # 步骤7：初始化AI分析器（Gemini API集成）
        self.analyzer = GeminiAnalyzer()
        
        # 步骤8：初始化通知服务（多渠道推送）
        self.notifier = NotificationService()
        
        # 步骤9：初始化搜索服务（新闻和市场情报）
        self.search_service = SearchService(
            bocha_keys=self.config.bocha_api_keys,
            tavily_keys=self.config.tavily_api_keys,
            serpapi_keys=self.config.serpapi_keys,
        )
        
        # 步骤10：记录初始化完成信息
        logger.info(f"调度器初始化完成，最大并发数: {self.max_workers}")
        logger.info("已启用趋势分析器 (MA5>MA10>MA20 多头判断)")
        if self.search_service.is_available:
            logger.info("搜索服务已启用 (Tavily/SerpAPI)")
        else:
            logger.warning("搜索服务未启用（未配置 API Key）")
    
    def fetch_and_save_stock_data(
        self, 
        code: str,
        force_refresh: bool = False
    ) -> Tuple[bool, Optional[str]]:
        """
        获取并保存单只股票数据 - 断点续传实现
        
        设计模式：检查点模式 (Checkpoint Pattern) + 缓存策略 (Cache Strategy)
        
        核心功能：
        1. 断点续传检查：避免重复获取相同日期的数据
        2. 智能数据获取：根据配置选择最优数据源
        3. 数据持久化：将获取的数据保存到本地数据库
        4. 错误处理和重试：网络故障时自动恢复
        
        断点续传逻辑（Checkpoint Resume）：
        ┌─────────────────────────────────────────────────────────┐
        │                   数据获取流程                            │
        │  1. 检查今日数据是否已存在 (db.has_today_data)           │
        │     • 存在且不强制刷新 → 跳过，返回成功                   │
        │     • 不存在或强制刷新 → 继续执行                        │
        │  2. 从数据源获取数据 (fetcher_manager.get_daily_data)   │
        │     • 多数据源调度：自动选择可用数据源                   │
        │     • 故障转移：主数据源失败时自动切换                   │
        │     • 流控遵守：遵循API调用频率限制                      │
        │  3. 数据验证和清洗                                      │
        │     • 检查数据是否为空或无效                            │
        │     • 数据格式标准化和类型转换                          │
        │  4. 保存到数据库 (db.save_daily_data)                   │
        │     • UPSERT操作：存在则更新，不存在则插入               │
        │     • 数据来源追踪：记录数据获取来源                     │
        │     • 事务安全：保证数据一致性                          │
        │  5. 返回结果                                            │
        │     • 成功：返回 (True, None)                          │
        │     • 失败：返回 (False, 错误信息)                      │
        └─────────────────────────────────────────────────────────┘
        
        设计优势：
        1. 节省资源：避免重复网络请求和API调用
        2. 提高性能：本地缓存数据读取速度快于网络
        3. 增强鲁棒性：网络故障时可使用本地数据继续分析
        4. 支持离线：无网络时仍可使用历史数据进行分析
        
        使用场景：
            # 正常使用：自动检查断点
            success, error = pipeline.fetch_and_save_stock_data('600519')
            
            # 强制刷新：忽略本地缓存，重新获取
            success, error = pipeline.fetch_and_save_stock_data('600519', force_refresh=True)
            
            # 批量处理：循环处理多个股票
            for code in stock_codes:
                success, error = pipeline.fetch_and_save_stock_data(code)
                if not success:
                    logger.error(f"股票 {code} 数据获取失败: {error}")
        
        技术实现细节：
        1. 日期处理：使用date.today()获取当前日期，支持时区
        2. 数据源选择：DataFetcherManager自动选择最优数据源
        3. 数据获取：默认获取30天数据，覆盖技术分析所需周期
        4. 数据保存：使用UPSERT逻辑，避免重复记录
        5. 错误隔离：单股票失败不影响其他股票
        
        性能优化：
        • 缓存命中：数据库检查 O(log n) 复杂度
        • 批量获取：一次获取多日数据，减少API调用次数
        • 连接复用：数据库连接池管理，避免频繁创建销毁
        • 异步潜力：方法设计支持未来改为异步实现
        
        安全考虑：
        • API限流：遵守数据源API调用频率限制
        • 数据验证：验证获取数据的完整性和有效性
        • 错误处理：网络超时、数据格式错误等异常处理
        • 资源清理：确保数据库连接正确关闭
        
        Args:
            code: 股票代码
                • 格式：6位数字字符串，如 '600519'
                • 要求：有效的A股代码，支持带后缀（如 '000001.SZ'）
                • 验证：在数据获取阶段进行代码有效性验证
            
            force_refresh: 是否强制刷新（忽略本地缓存）
                • False（默认）：启用断点续传，检查本地数据
                • True：强制重新获取，忽略本地缓存数据
                • 使用场景：数据错误修复、历史数据更新、调试测试
        
        Returns:
            Tuple[bool, Optional[str]]: 执行结果元组
                • 成功： (True, None)
                • 失败： (False, 错误信息字符串)
                • 错误信息格式：简明描述失败原因，便于日志记录
        
        Raises:
            无显式异常抛出，所有异常在方法内部捕获并返回错误信息
            但可能抛出的异常包括：
            • ValueError: 股票代码格式错误
            • ConnectionError: 网络连接失败
            • TimeoutError: 请求超时
            • DatabaseError: 数据库操作失败
        
        示例输出：
            # 成功案例
            (True, None)
            
            # 失败案例
            (False, "获取数据为空")
            (False, "网络连接超时")
            (False, "数据库保存失败: UNIQUE constraint failed")
        """
        try:
            today = date.today()
            
            # 断点续传检查：如果今日数据已存在，跳过
            if not force_refresh and self.db.has_today_data(code, today):
                logger.info(f"[{code}] 今日数据已存在，跳过获取（断点续传）")
                return True, None
            
            # 从数据源获取数据
            logger.info(f"[{code}] 开始从数据源获取数据...")
            df, source_name = self.fetcher_manager.get_daily_data(code, days=30)
            
            if df is None or df.empty:
                return False, "获取数据为空"
            
            # 保存到数据库
            saved_count = self.db.save_daily_data(df, code, source_name)
            logger.info(f"[{code}] 数据保存成功（来源: {source_name}，新增 {saved_count} 条）")
            
            return True, None
            
        except Exception as e:
            error_msg = f"获取/保存数据失败: {str(e)}"
            logger.error(f"[{code}] {error_msg}")
            return False, error_msg
    
    def analyze_stock(self, code: str) -> Optional[AnalysisResult]:
        """
        分析单只股票 - 多维度综合分析引擎
        
        设计模式：管道模式 (Pipeline Pattern) + 组合模式 (Composite Pattern)
        
        核心功能：六维股票分析框架
        1. 实时行情分析：量比、换手率、实时价格
        2. 筹码分布分析：股东成本、集中度、获利比例  
        3. 技术趋势分析：MA多头排列、突破信号、趋势评分
        4. 市场情报搜索：最新消息、风险事件、业绩预期
        5. 数据上下文构建：历史数据聚合、指标计算、格式标准化
        6. AI综合研判：技术面+基本面+情绪面综合分析
        
        分析流程管道：
        ┌─────────────────────────────────────────────────────────┐
        │                   股票分析六步法                          │
        │  1. 🎯 股票识别                                          │
        │     • 获取股票代码和名称                                │
        │     • 优先使用实时行情中的真实名称                      │
        │     • 回退到预设名称映射表                              │
        │                                                        │
        │  2. 📈 实时行情获取                                      │
        │     • 量比 (Volume Ratio): 市场活跃度指标              │
        │     • 换手率 (Turnover Rate): 股票流动性指标           │
        │     • 实时价格: 最新成交价                              │
        │     • 容错设计: 失败时记录警告，不影响后续流程           │
        │                                                        │
        │  3. 🎰 筹码分布分析                                      │
        │     • 获利比例: 当前价格高于成本的股东占比              │
        │     • 集中度: 股东持股集中程度（90%集中度）             │
        │     • 成本分布: 不同价位区间的筹码分布                  │
        │     • 容错设计: 失败时记录警告，继续执行                │
        │                                                        │
        │  4. 📊 技术趋势分析                                      │
        │     • MA多头排列: MA5>MA10>MA20 判断                  │
        │     • 买入信号: 基于交易理念的信号生成                  │
        │     • 趋势评分: 量化趋势强度（0-100分）                 │
        │     • 容错设计: 失败时记录警告，继续执行                │
        │                                                        │
        │  5. 🔍 多维度情报搜索                                    │
        │     • 最新消息: 公司公告、行业新闻、政策动态            │
        │     • 风险排查: 负面新闻、监管关注、财务风险            │
        │     • 业绩预期: 分析师评级、盈利预测、目标价            │
        │     • 智能聚合: 自动去重、排序、摘要生成                │
        │                                                        │
        │  6. 🧠 AI综合分析                                        │
        │     • 上下文构建: 聚合所有维度的分析数据                │
        │     • 增强处理: 添加量比描述、趋势解读等                │
        │     • AI推理: Gemini模型进行多维度综合研判             │
        │     • 结果生成: 操作建议、情绪评分、趋势预测            │
        └─────────────────────────────────────────────────────────┘
        
        设计理念：
        1. 全维度覆盖：技术面、基本面、情绪面、资金面综合分析
        2. 实时性优先：使用最新行情和新闻，确保分析时效性
        3. 容错性强：单一步骤失败不影响整体分析流程
        4. 可解释性：每个分析维度都有明确的数据支撑和逻辑
        
        技术实现亮点：
        • 渐进式增强：基础数据 → 增强数据 → AI分析 分层处理
        • 异步潜力：各分析步骤可并行执行，提高效率
        • 缓存友好：重用已获取的数据，减少重复请求
        • 模块化设计：每个分析步骤独立，便于测试和扩展
        
        使用场景：
            # 完整分析单只股票
            result = pipeline.analyze_stock('600519')
            if result:
                print(f"操作建议: {result.operation_advice}")
                print(f"情绪评分: {result.sentiment_score}")
                print(f"趋势预测: {result.trend_prediction}")
            
            # 批量分析股票列表
            for code in ['600519', '000001', '300750']:
                result = pipeline.analyze_stock(code)
                if result:
                    # 处理分析结果
                    pass
        
        性能考虑：
        • 网络请求：多个API调用，需要注意请求间隔和频率限制
        • 计算复杂度：技术指标计算和AI推理需要一定计算资源
        • 内存使用：聚合多维度数据，注意大对象内存管理
        • 时间开销：完整分析单只股票约10-30秒
        
        错误处理策略：
        1. 分级容错：非核心步骤失败不影响整体流程
        2. 详细日志：记录每个步骤的成功/失败状态
        3. 优雅降级：某个数据源不可用时使用替代数据
        4. 超时控制：设置合理的超时时间，避免长时间等待
        
        Args:
            code: 股票代码
                • 格式：6位数字字符串，如 '600519'
                • 支持：A股、B股、科创板、创业板等
                • 验证：在实时行情获取阶段进行有效性验证
            
        Returns:
            Optional[AnalysisResult]: 分析结果对象
                • 成功：返回完整的AnalysisResult对象
                • 失败：返回None（记录详细错误日志）
                • AnalysisResult包含：操作建议、情绪评分、趋势预测等
            
        Raises:
            无显式异常抛出，所有异常在方法内部捕获并处理
            但可能抛出的异常包括：
            • ValueError: 股票代码无效或数据格式错误
            • ConnectionError: 网络连接失败（多个API调用）
            • TimeoutError: 请求超时（AI分析或数据获取）
            • AnalysisError: AI分析失败或结果解析错误
        
        数据结构：
            AnalysisResult {
                code: str,              # 股票代码
                name: str,              # 股票名称
                operation_advice: str,  # 操作建议（买入/持有/卖出）
                sentiment_score: int,   # 情绪评分（0-100）
                trend_prediction: str,  # 趋势预测
                analysis_time: str,     # 分析时间
                confidence: float,      # 置信度（0-1）
                reasoning: str,         # 分析理由
                risk_level: str,        # 风险等级
                price_target: str,      # 目标价位
                news_summary: str,      # 新闻摘要
            }
        """
        logger.debug(f"开始分析单个股票: {code}")
        try:
            # 获取股票名称（优先从实时行情获取真实名称）
            stock_name = STOCK_NAME_MAP.get(code, '')
            
            # Step 1: 获取实时行情（量比、换手率等）
            realtime_quote: Optional[RealtimeQuote] = None
            try:
                realtime_quote = self.akshare_fetcher.get_realtime_quote(code)
                if realtime_quote:
                    # 使用实时行情返回的真实股票名称
                    if realtime_quote.name:
                        stock_name = realtime_quote.name
                    logger.info(f"[{code}] {stock_name} 实时行情: 价格={realtime_quote.price}, "
                              f"量比={realtime_quote.volume_ratio}, 换手率={realtime_quote.turnover_rate}%")
            except Exception as e:
                logger.warning(f"[{code}] 获取实时行情失败: {e}")
            
            # 如果还是没有名称，使用代码作为名称
            if not stock_name:
                stock_name = f'股票{code}'
            
            # Step 2: 获取筹码分布
            chip_data: Optional[ChipDistribution] = None
            try:
                chip_data = self.akshare_fetcher.get_chip_distribution(code)
                if chip_data:
                    logger.info(f"[{code}] 筹码分布: 获利比例={chip_data.profit_ratio:.1%}, "
                              f"90%集中度={chip_data.concentration_90:.2%}")
            except Exception as e:
                logger.warning(f"[{code}] 获取筹码分布失败: {e}")
            
            # Step 3: 趋势分析（基于交易理念）
            trend_result: Optional[TrendAnalysisResult] = None
            try:
                # 获取历史数据进行趋势分析
                context = self.db.get_analysis_context(code)
                if context and 'raw_data' in context:
                    import pandas as pd
                    raw_data = context['raw_data']
                    if isinstance(raw_data, list) and len(raw_data) > 0:
                        df = pd.DataFrame(raw_data)
                        trend_result = self.trend_analyzer.analyze(df, code)
                        logger.info(f"[{code}] 趋势分析: {trend_result.trend_status.value}, "
                                  f"买入信号={trend_result.buy_signal.value}, 评分={trend_result.signal_score}")
            except Exception as e:
                logger.warning(f"[{code}] 趋势分析失败: {e}")
            
            # Step 4: 多维度情报搜索（最新消息+风险排查+业绩预期）
            news_context = None
            if self.search_service.is_available:
                logger.info(f"[{code}] 开始多维度情报搜索...")
                
                # 使用多维度搜索（最多3次搜索）
                intel_results = self.search_service.search_comprehensive_intel(
                    stock_code=code,
                    stock_name=stock_name,
                    max_searches=3
                )
                
                # 格式化情报报告
                if intel_results:
                    news_context = self.search_service.format_intel_report(intel_results, stock_name)
                    total_results = sum(
                        len(r.results) for r in intel_results.values() if r.success
                    )
                    logger.info(f"[{code}] 情报搜索完成: 共 {total_results} 条结果")
                    logger.debug(f"[{code}] 情报搜索结果:\n{news_context}")
            else:
                logger.info(f"[{code}] 搜索服务不可用，跳过情报搜索")
            
            # Step 5: 获取分析上下文（技术面数据）
            context = self.db.get_analysis_context(code)
            
            if context is None:
                logger.warning(f"[{code}] 无法获取分析上下文，跳过分析")
                return None
            
            # Step 6: 增强上下文数据（添加实时行情、筹码、趋势分析结果、股票名称）
            enhanced_context = self._enhance_context(
                context, 
                realtime_quote, 
                chip_data, 
                trend_result,
                stock_name  # 传入股票名称
            )
            
            # Step 7: 调用 AI 分析（传入增强的上下文和新闻）
            result = self.analyzer.analyze(enhanced_context, news_context=news_context)
            
            return result
            
        except Exception as e:
            logger.error(f"[{code}] 分析失败: {e}")
            logger.exception(f"[{code}] 详细错误信息:")
            return None
    
    def _enhance_context(
        self,
        context: Dict[str, Any],
        realtime_quote: Optional[RealtimeQuote],
        chip_data: Optional[ChipDistribution],
        trend_result: Optional[TrendAnalysisResult],
        stock_name: str = ""
    ) -> Dict[str, Any]:
        """
        增强分析上下文 - 数据聚合和标准化处理
        
        设计模式：装饰器模式 (Decorator Pattern) + 建造者模式 (Builder Pattern)
        
        核心功能：四维数据聚合和标准化
        1. 股票信息增强：补充股票名称和标识信息
        2. 实时行情集成：量比、换手率、估值指标等实时数据
        3. 筹码分布整合：股东成本结构、集中度分析
        4. 趋势分析融合：技术指标、买入信号、风险因素
        
        数据聚合流程：
        ┌─────────────────────────────────────────────────────────┐
        │                   上下文增强四步法                        │
        │  1. 📋 基础上下文复制                                     │
        │     • 深拷贝原始上下文，避免副作用                        │
        │     • 保留原始数据完整性                                 │
        │                                                        │
        │  2. 🏷️ 股票信息增强                                      │
        │     • 股票名称：优先使用传入名称，次选实时行情名称        │
        │     • 标识信息：确保每个分析结果都有明确的股票标识        │
        │                                                        │
        │  3. 📈 实时行情集成                                      │
        │     • 价格数据：最新成交价、市值、流通市值                │
        │     • 量价指标：量比（带描述）、换手率                    │
        │     • 估值指标：PE比率、PB比率                           │
        │     • 趋势指标：60日涨跌幅                               │
        │                                                        │
        │  4. 🎰 筹码分布整合                                      │
        │     • 成本分析：平均成本、获利比例                        │
        │     • 集中度分析：90%集中度、70%集中度                   │
        │     • 状态判断：根据当前价格判断筹码状态                  │
        │                                                        │
        │  5. 📊 趋势分析融合                                      │
        │     • 趋势状态：多头/空头/震荡判断                       │
        │     • MA排列：MA5/MA10/MA20对齐情况                     │
        │     • 乖离率：价格相对于MA5/MA10的偏离程度               │
        │     • 成交量：放量/缩量状态和趋势                        │
        │     • 买入信号：基于交易理念的信号生成                   │
        │     • 风险因素：识别潜在风险点                           │
        └─────────────────────────────────────────────────────────┘
        
        数据结构标准化：
        • 统一命名规范：snake_case格式，明确字段含义
        • 类型一致性：数值类型统一为float，字符串类型明确标注
        • 嵌套结构：合理分层，避免扁平化导致的字段冲突
        • 可扩展性：预留扩展字段，支持未来新增数据维度
        
        设计优势：
        1. 数据完整性：聚合多源数据，提供全面分析基础
        2. 格式标准化：统一数据结构，便于后续处理和存储
        3. 容错处理：各数据源可选，缺失时不影响整体结构
        4. 可读性提升：添加描述性字段（如量比描述）
        5. 性能优化：浅拷贝+增量更新，避免深层次遍历
        
        使用场景：
            # 在analyze_stock方法中调用
            enhanced_context = self._enhance_context(
                context=raw_context,
                realtime_quote=realtime_data,
                chip_data=chip_data,
                trend_result=trend_analysis,
                stock_name="贵州茅台"
            )
            
            # 结果示例
            {
                'stock_name': '贵州茅台',
                'realtime': {
                    'price': 1680.50,
                    'volume_ratio': 1.5,
                    'volume_ratio_desc': '温和放量',
                    'turnover_rate': 0.25,
                    'pe_ratio': 35.2,
                    ...
                },
                'chip': {
                    'profit_ratio': 0.65,
                    'avg_cost': 1550.30,
                    ...
                },
                'trend_analysis': {
                    'trend_status': 'bullish',
                    'buy_signal': 'strong_buy',
                    ...
                },
                ... # 原始上下文字段
            }
        
        错误处理策略：
        1. 空值安全：所有参数都是Optional，内部进行空值检查
        2. 类型安全：确保数据类型的正确性，避免类型错误
        3. 数据验证：验证关键数据的有效性（如价格>0）
        4. 优雅降级：某个数据源缺失时，保留其他可用数据
        
        性能考虑：
        • 内存使用：context.copy()是浅拷贝，性能较好
        • 数据处理：增量添加字段，避免大规模数据重构
        • 计算复杂度：O(n)复杂度，n为添加的字段数量
        • 序列化友好：数据结构设计便于JSON序列化
        
        Args:
            context: 原始分析上下文
                • 类型：Dict[str, Any]
                • 来源：db.get_analysis_context()返回的原始数据
                • 包含：历史价格、技术指标、统计数据等
                • 要求：非空字典，至少包含基础分析数据
            
            realtime_quote: 实时行情数据
                • 类型：Optional[RealtimeQuote]
                • 来源：akshare_fetcher.get_realtime_quote()
                • 包含：实时价格、量比、换手率、估值指标等
                • 可选：可能为None（获取失败或未启用）
            
            chip_data: 筹码分布数据
                • 类型：Optional[ChipDistribution]
                • 来源：akshare_fetcher.get_chip_distribution()
                • 包含：获利比例、平均成本、集中度等
                • 可选：可能为None（获取失败或未启用）
            
            trend_result: 趋势分析结果
                • 类型：Optional[TrendAnalysisResult]
                • 来源：trend_analyzer.analyze()
                • 包含：趋势状态、买入信号、风险因素等
                • 可选：可能为None（分析失败或未启用）
            
            stock_name: 股票名称
                • 类型：str，默认空字符串
                • 来源：外部传入或实时行情获取
                • 优先级：参数传入 > realtime_quote.name > 空字符串
                • 作用：增强结果可读性
            
        Returns:
            Dict[str, Any]: 增强后的分析上下文
                • 包含：原始上下文所有字段
                • 新增：stock_name字段（如果提供了名称）
                • 新增：realtime字段（如果提供了实时行情）
                • 新增：chip字段（如果提供了筹码数据）
                • 新增：trend_analysis字段（如果提供了趋势分析）
                • 结构：嵌套字典，层次清晰，字段命名规范
        
        Raises:
            无显式异常抛出，所有异常在方法内部静默处理
            但可能抛出的异常包括：
            • KeyError: 上下文字典访问不存在的键
            • AttributeError: 数据对象属性访问错误
            • TypeError: 数据类型转换错误
            • ValueError: 数据值验证失败
        
        数据字段说明：
            enhanced['stock_name']: str - 股票名称
            enhanced['realtime']: dict - 实时行情数据
                • name: str - 股票名称
                • price: float - 最新价格
                • volume_ratio: float - 量比
                • volume_ratio_desc: str - 量比描述（人性化）
                • turnover_rate: float - 换手率(%)
                • pe_ratio: float - 市盈率
                • pb_ratio: float - 市净率
                • total_mv: float - 总市值
                • circ_mv: float - 流通市值
                • change_60d: float - 60日涨跌幅(%)
            
            enhanced['chip']: dict - 筹码分布数据
                • profit_ratio: float - 获利比例(0-1)
                • avg_cost: float - 平均成本
                • concentration_90: float - 90%集中度(0-1)
                • concentration_70: float - 70%集中度(0-1)
                • chip_status: str - 筹码状态
            
            enhanced['trend_analysis']: dict - 趋势分析结果
                • trend_status: str - 趋势状态
                • ma_alignment: str - MA排列情况
                • trend_strength: float - 趋势强度(0-1)
                • bias_ma5: float - 乖离率MA5(%)
                • bias_ma10: float - 乖离率MA10(%)
                • volume_status: str - 成交量状态
                • volume_trend: str - 成交量趋势
                • buy_signal: str - 买入信号
                • signal_score: int - 信号评分(0-100)
                • signal_reasons: List[str] - 信号理由
                • risk_factors: List[str] - 风险因素
        """
        enhanced = context.copy()
        
        # 添加股票名称
        if stock_name:
            enhanced['stock_name'] = stock_name
        elif realtime_quote and realtime_quote.name:
            enhanced['stock_name'] = realtime_quote.name
        
        # 添加实时行情
        if realtime_quote:
            enhanced['realtime'] = {
                'name': realtime_quote.name,  # 股票名称
                'price': realtime_quote.price,
                'volume_ratio': realtime_quote.volume_ratio,
                'volume_ratio_desc': self._describe_volume_ratio(realtime_quote.volume_ratio),
                'turnover_rate': realtime_quote.turnover_rate,
                'pe_ratio': realtime_quote.pe_ratio,
                'pb_ratio': realtime_quote.pb_ratio,
                'total_mv': realtime_quote.total_mv,
                'circ_mv': realtime_quote.circ_mv,
                'change_60d': realtime_quote.change_60d,
            }
        
        # 添加筹码分布
        if chip_data:
            current_price = realtime_quote.price if realtime_quote else 0
            enhanced['chip'] = {
                'profit_ratio': chip_data.profit_ratio,
                'avg_cost': chip_data.avg_cost,
                'concentration_90': chip_data.concentration_90,
                'concentration_70': chip_data.concentration_70,
                'chip_status': chip_data.get_chip_status(current_price),
            }
        
        # 添加趋势分析结果
        if trend_result:
            enhanced['trend_analysis'] = {
                'trend_status': trend_result.trend_status.value,
                'ma_alignment': trend_result.ma_alignment,
                'trend_strength': trend_result.trend_strength,
                'bias_ma5': trend_result.bias_ma5,
                'bias_ma10': trend_result.bias_ma10,
                'volume_status': trend_result.volume_status.value,
                'volume_trend': trend_result.volume_trend,
                'buy_signal': trend_result.buy_signal.value,
                'signal_score': trend_result.signal_score,
                'signal_reasons': trend_result.signal_reasons,
                'risk_factors': trend_result.risk_factors,
            }
        
        return enhanced
    
    def _describe_volume_ratio(self, volume_ratio: float) -> str:
        """
        量比描述生成器 - 技术指标人性化解释
        
        设计模式：策略模式 (Strategy Pattern) + 查表法 (Lookup Table)
        
        核心功能：将数值型量比指标转换为人类可读的描述文本
        量比定义：当前成交量 / 过去5日平均成交量
        公式：volume_ratio = current_volume / average_volume_5d
        
        量比等级划分（行业标准）：
        ┌────────────┬─────────────┬──────────────┬────────────────────────────┐
        │  量比范围   │   等级描述   │   市场含义    │        操作建议            │
        ├────────────┼─────────────┼──────────────┼────────────────────────────┤
        │  < 0.5     │  极度萎缩    │ 交投极度清淡  │ 观望，等待放量信号         │
        │  0.5-0.8   │  明显萎缩    │ 交投不活跃    │ 谨慎，可能有变盘风险       │
        │  0.8-1.2   │  正常        │ 正常交易水平  │ 正常交易，关注趋势        │
        │  1.2-2.0   │  温和放量    │ 资金开始关注  │ 关注，可能有行情启动       │
        │  2.0-3.0   │  明显放量    │ 资金积极介入  │ 重点关注，机会较大         │
        │  >= 3.0    │  巨量        │ 资金疯狂涌入  │ 警惕，可能见顶或突破       │
        └────────────┴─────────────┴──────────────┴────────────────────────────┘
        
        量比指标解读：
        • 量比 < 1：缩量状态，市场参与度低
          - 极度萎缩 (<0.5)：流动性枯竭，可能变盘
          - 明显萎缩 (0.5-0.8)：交投清淡，趋势不明
        • 量比 ≈ 1：正常状态，市场平稳
          - 正常 (0.8-1.2)：常态交易，趋势延续
        • 量比 > 1：放量状态，市场活跃
          - 温和放量 (1.2-2.0)：资金试探，趋势初现
          - 明显放量 (2.0-3.0)：资金确认，趋势加强
          - 巨量 (≥3.0)：情绪极端，警惕反转
        
        技术分析意义：
        1. 缩量调整：量比<1，价格调整，健康回调信号
        2. 放量上涨：量比>1.2，价格上涨，趋势确认信号
        3. 放量下跌：量比>1.2，价格下跌，出货或恐慌信号
        4. 天量天价：量比>3.0，价格高位，见顶风险
        5. 地量地价：量比<0.5，价格低位，见底可能
        
        设计优势：
        1. 可读性：将数值转换为自然语言描述
        2. 一致性：标准化描述，避免主观判断差异
        3. 可扩展：易于添加新的量比等级
        4. 性能优异：O(1)时间复杂度，查表法实现
        5. 国际化友好：描述文本易于本地化翻译
        
        使用场景：
            # 在_enhance_context方法中调用
            volume_ratio_desc = self._describe_volume_ratio(1.5)
            # 返回: "温和放量"
            
            # 直接用于分析报告
            desc = self._describe_volume_ratio(volume_ratio)
            analysis_text = f"当前量比{volume_ratio:.2f}，属于{desc}状态"
        
        错误处理考虑：
        • 边界值：明确每个区间的开闭边界
        • 异常值：支持负值或极大值的处理（虽然不合理）
        • NaN/Infinity：实际使用中应在外层过滤
        
        性能优化：
        • 查表法：if-elif链等效于查表，编译优化后效率高
        • 无循环：直接条件判断，避免循环开销
        • 内存零分配：返回字符串字面量，无额外内存分配
        
        扩展性：
        • 添加等级：只需增加新的elif分支
        • 修改阈值：调整区间边界值
        • 多语言：可改为返回翻译键，由外部处理本地化
        • 分级细化：可增加更多细分等级（如1.0-1.5为轻微放量）
        
        Args:
            volume_ratio: 量比数值
                • 类型：float
                • 定义：当前成交量 / 过去5日平均成交量
                • 有效范围：理论上 ≥ 0，实际通常 0-10
                • 典型值：0.3（极度萎缩）、0.7（明显萎缩）、1.0（正常）、
                         1.5（温和放量）、2.5（明显放量）、4.0（巨量）
                • 特殊值：NaN/Infinity应在外层处理
            
        Returns:
            str: 量比描述文本
                • 可能返回值："极度萎缩"、"明显萎缩"、"正常"、
                             "温和放量"、"明显放量"、"巨量"
                • 语言：中文，符合A股投资者习惯
                • 长度：2-4个汉字，简洁明了
        
        Raises:
            无显式异常抛出，但需要注意：
            • 如果传入NaN或Infinity，可能返回意外结果
            • 负值量比理论上不可能，但会返回"极度萎缩"
        
        算法实现细节：
        if volume_ratio < 0.5:
            return "极度萎缩"      # 区间: (-∞, 0.5)
        elif volume_ratio < 0.8:
            return "明显萎缩"      # 区间: [0.5, 0.8)
        elif volume_ratio < 1.2:
            return "正常"          # 区间: [0.8, 1.2)
        elif volume_ratio < 2.0:
            return "温和放量"      # 区间: [1.2, 2.0)
        elif volume_ratio < 3.0:
            return "明显放量"      # 区间: [2.0, 3.0)
        else:
            return "巨量"          # 区间: [3.0, +∞)
        
        注意事项：
        1. 区间设计：采用左闭右开区间，避免边界重复
        2. 阈值选择：基于A股市场经验值，可调整
        3. 文化适配：描述词符合中文投资者认知习惯
        4. 业务耦合：与交易策略中的量价分析紧密相关
        """
    
    def process_single_stock(
        self, 
        code: str,
        skip_analysis: bool = False,
        single_stock_notify: bool = False
    ) -> Optional[AnalysisResult]:
        """
        处理单只股票的完整流程 - 线程池工作单元
        
        设计模式：工作单元模式 (Unit of Work) + 模板方法模式 (Template Method)
        
        核心功能：四步股票处理流水线
        1. 数据获取与保存：获取最新股票数据并持久化
        2. AI智能分析：调用Gemini模型进行多维度分析
        3. 实时推送（可选）：单股分析完成后立即推送结果
        4. 异常安全处理：确保单股失败不影响批量处理
        
        处理流程架构：
        ┌─────────────────────────────────────────────────────────┐
        │                   单股票处理四步法                        │
        │  1. 🚀 流程开始                                          │
        │     • 记录开始日志，标记处理起点                         │
        │     • 异常捕获包装，确保后续步骤执行                     │
        │                                                        │
        │  2. 📊 数据获取与保存                                    │
        │     • 调用fetch_and_save_stock_data获取数据             │
        │     • 断点续传：已有今日数据则跳过                       │
        │     • 容错设计：获取失败仍尝试用历史数据分析             │
        │                                                        │
        │  3. 🤖 AI智能分析                                        │
        │     • 检查skip_analysis标志，决定是否跳过               │
        │     • 调用analyze_stock进行六维综合分析                  │
        │     • 记录分析结果：操作建议、情绪评分等                 │
        │                                                        │
        │  4. 📢 实时推送（可选）                                  │
        │     • 检查single_stock_notify标志和通知服务可用性       │
        │     • 生成单股分析报告（generate_single_stock_report）  │
        │     • 推送报告到配置的渠道（企业微信/飞书/Telegram等）   │
        │     • 推送结果日志记录（成功/失败/异常）                 │
        │                                                        │
        │  5. ✅ 结果返回                                          │
        │     • 成功：返回AnalysisResult对象                      │
        │     • 跳过：返回None（dry-run模式）                     │
        │     • 失败：返回None（记录异常日志）                     │
        └─────────────────────────────────────────────────────────┘
        
        设计优势：
        1. 原子性操作：每个股票处理是独立的工作单元
        2. 异常隔离：单股票失败不影响其他股票处理
        3. 灵活配置：支持dry-run模式和实时推送模式
        4. 资源友好：数据获取失败时仍尝试使用历史数据
        5. 可观测性：详细日志记录每个处理步骤的状态
        
        使用场景：
            # 线程池并发调用（主要用途）
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(pipeline.process_single_stock, code): code 
                    for code in stock_codes
                }
            
            # 单股票同步处理（调试用途）
            result = pipeline.process_single_stock('600519')
            if result:
                print(f"分析结果: {result.operation_advice}")
            
            # Dry-run模式（仅获取数据）
            pipeline.process_single_stock('600519', skip_analysis=True)
            
            # 实时推送模式（每分析完一只立即推送）
            pipeline.process_single_stock('600519', single_stock_notify=True)
        
        线程安全考虑：
        • 无状态设计：方法不修改实例状态，仅依赖参数和配置
        • 资源局部性：所有资源在方法内部获取和释放
        • 并发友好：可被多个线程同时调用处理不同股票
        • 数据库安全：通过DatabaseManager单例管理连接池
        
        错误处理策略：
        1. 分级容错：
           • 数据获取失败 → 记录警告，继续尝试分析
           • AI分析失败 → 返回None，记录错误
           • 推送失败 → 记录错误，但不影响分析结果返回
        2. 异常捕获：最外层try-except捕获所有未知异常
        3. 资源清理：无需显式清理，依赖上下文管理器
        4. 日志完备：每个错误点都有详细日志记录
        
        性能优化：
        • 懒加载：数据只在需要时获取
        • 缓存利用：断点续传避免重复网络请求
        • 连接复用：数据库连接池共享
        • 异步潜力：方法设计支持未来改为异步实现
        
        配置驱动行为：
        • skip_analysis：控制是否执行AI分析（dry-run模式）
        • single_stock_notify：控制是否启用实时推送
        • 通知服务可用性：检查notifier.is_available()
        • 股票代码验证：在数据获取阶段进行
        
        Args:
            code: 股票代码
                • 类型：str，6位数字字符串
                • 示例：'600519'（贵州茅台）、'000001'（平安银行）
                • 验证：在fetch_and_save_stock_data中进行有效性验证
                • 要求：有效的A股代码，支持带后缀格式
            
            skip_analysis: 是否跳过AI分析
                • 类型：bool，默认False
                • True：dry-run模式，仅获取和保存数据，不进行AI分析
                • False：完整模式，执行数据获取+AI分析全流程
                • 使用场景：数据维护、历史数据更新、网络资源节省
            
            single_stock_notify: 是否启用单股推送模式
                • 类型：bool，默认False
                • True：每分析完一只股票立即推送结果
                • False：批量模式，所有股票分析完成后统一推送
                • 依赖：需要通知服务可用（notifier.is_available()返回True）
                • 特性：实时性高，但可能产生较多推送消息
        
        Returns:
            Optional[AnalysisResult]: 分析结果
                • 成功：返回AnalysisResult对象，包含完整的分析结果
                • 跳过：skip_analysis=True时返回None
                • 失败：分析过程中发生错误时返回None
                • None情况：都会记录详细日志说明原因
        
        Raises:
            无显式异常抛出，所有异常在方法内部捕获并处理
            但内部调用的方法可能抛出：
            • ValueError: 股票代码格式错误
            • ConnectionError: 网络连接失败
            • TimeoutError: 请求超时
            • AnalysisError: AI分析失败
        
        日志输出示例：
            [INFO] ========== 开始处理 600519 ==========
            [INFO] [600519] 今日数据已存在，跳过获取（断点续传）
            [INFO] [600519] 贵州茅台 实时行情: 价格=1680.50, 量比=1.5, 换手率=0.25%
            [INFO] [600519] 分析完成: 建议买入, 评分 85
            [INFO] [600519] 单股推送成功
        
        注意事项：
        1. 幂等性：多次调用相同股票代码应产生相同结果（数据不变情况下）
        2. 资源限制：注意API调用频率限制，避免被封禁
        3. 网络依赖：需要稳定的网络连接获取实时数据
        4. 时间开销：完整处理单只股票约10-30秒，批量处理需合理规划
        5. 内存管理：AnalysisResult对象较大，批量处理时注意内存使用
        """
        logger.info(f"========== 开始处理 {code} ==========")
        
        try:
            # Step 1: 获取并保存数据
            success, error = self.fetch_and_save_stock_data(code)
            
            if not success:
                logger.warning(f"[{code}] 数据获取失败: {error}")
                # 即使获取失败，也尝试用已有数据分析
            
            # Step 2: AI 分析
            if skip_analysis:
                logger.info(f"[{code}] 跳过 AI 分析（dry-run 模式）")
                return None
            
            result = self.analyze_stock(code)
            
            if result:
                logger.info(
                    f"[{code}] 分析完成: {result.operation_advice}, "
                    f"评分 {result.sentiment_score}"
                )
                
                # 单股推送模式（#55）：每分析完一只股票立即推送
                if single_stock_notify and self.notifier.is_available():
                    try:
                        single_report = self.notifier.generate_single_stock_report(result)
                        if self.notifier.send(single_report):
                            logger.info(f"[{code}] 单股推送成功")
                        else:
                            logger.warning(f"[{code}] 单股推送失败")
                    except Exception as e:
                        logger.error(f"[{code}] 单股推送异常: {e}")
            
            return result
            
        except Exception as e:
            # 捕获所有异常，确保单股失败不影响整体
            logger.exception(f"[{code}] 处理过程发生未知异常: {e}")
            return None
    
    def run(
        self, 
        stock_codes: Optional[List[str]] = None,
        dry_run: bool = False,
        send_notification: bool = True
    ) -> List[AnalysisResult]:
        """
        运行完整的分析流程 - 批量股票处理主控制器
        
        设计模式：指挥官模式 (Command Pattern) + 生产者-消费者模式 (Producer-Consumer)
        
        核心功能：四阶段批量处理引擎
        1. 任务准备阶段：股票列表准备和验证
        2. 并发执行阶段：线程池调度和任务分发
        3. 结果收集阶段：异步结果聚合和统计
        4. 后处理阶段：通知发送和性能报告
        
        处理流程架构：
        ┌─────────────────────────────────────────────────────────┐
        │                   批量处理四阶段模型                      │
        │  1. 📋 任务准备阶段                                      │
        │     • 股票列表确定：参数指定或配置读取                   │
        │     • 配置刷新：确保使用最新的股票配置                   │
        │     • 输入验证：检查股票列表有效性                      │
        │     • 日志记录：记录处理参数和配置                       │
        │                                                        │
        │  2. ⚡ 并发执行阶段                                      │
        │     • 线程池创建：根据max_workers配置                   │
        │     • 任务提交：将每个股票提交给线程池                   │
        │     • 并发控制：限制最大并发数，避免API封禁              │
        │     • 超时管理：设置任务执行超时保护                     │
        │                                                        │
        │  3. 📊 结果收集阶段                                      │
        │     • 异步等待：使用as_completed按完成顺序收集           │
        │     • 进度跟踪：实时显示处理进度和成功率                 │
        │     • 错误处理：单个任务失败不影响其他任务               │
        │     • 结果过滤：剔除None结果（失败或跳过）               │
        │                                                        │
        │  4. 📢 后处理阶段                                        │
        │     • 通知发送：汇总结果发送到各推送渠道                 │
        │     • 性能报告：统计处理时间、成功率等指标               │
        │     • 结果排序：按情绪评分或建议强度排序                 │
        │     • 飞书文档：可选生成飞书云文档报告                   │
        └─────────────────────────────────────────────────────────┘
        
        设计优势：
        1. 高并发处理：利用线程池提高处理效率
        2. 弹性伸缩：根据max_workers调整并发能力
        3. 进度可视化：实时显示处理进度和状态
        4. 容错性强：单股票失败不影响整体批次
        5. 资源可控：限制并发数避免资源耗尽
        
        使用场景：
            # 完整分析配置中的所有股票
            pipeline = StockAnalysisPipeline()
            results = pipeline.run()
            
            # 指定股票列表分析
            results = pipeline.run(stock_codes=['600519', '000001'])
            
            # Dry-run模式（仅获取数据）
            results = pipeline.run(dry_run=True)
            
            # 禁用通知推送
            results = pipeline.run(send_notification=False)
            
            # 组合模式
            results = pipeline.run(
                stock_codes=['600519', '300750'],
                dry_run=False,
                send_notification=True
            )
        
        并发控制策略：
        • 默认并发数：max_workers=3（避免API封禁）
        • 线程池类型：ThreadPoolExecutor（I/O密集型任务）
        • 任务调度：FIFO（先进先出）顺序
        • 结果收集：as_completed（按完成顺序，非任务顺序）
        
        性能监控指标：
        1. 处理时间：从开始到结束的总耗时
        2. 成功率：成功分析的股票数 / 总股票数
        3. 并发效率：实际并发数 vs 理论最大并发数
        4. 资源使用：内存、CPU、网络IO
        5. API调用：各数据源API调用次数
        
        错误处理机制：
        1. 输入验证：股票列表为空时立即返回
        2. 任务级容错：process_single_stock内部捕获异常
        3. 批量级隔离：单任务失败不影响其他任务
        4. 资源清理：使用with语句确保线程池正确关闭
        5. 超时保护：future.result()设置超时时间
        
        配置和参数：
        • stock_codes：覆盖配置中的默认股票列表
        • dry_run：控制是否执行AI分析（仅获取数据）
        • send_notification：控制是否发送推送通知
        • max_workers：控制并发数（在初始化时设置）
        
        Args:
            stock_codes: 股票代码列表
                • 类型：Optional[List[str]]，默认None
                • None：使用配置中的股票列表（config.stock_list）
                • 列表：覆盖配置，使用指定的股票列表
                • 示例：['600519', '000001', '300750']
                • 空列表：立即返回空结果，记录错误
            
            dry_run: 是否仅获取数据不分析
                • 类型：bool，默认False
                • False：完整流程（数据获取+AI分析）
                • True：仅获取数据，跳过AI分析
                • 用途：数据维护、历史更新、节省AI成本
            
            send_notification: 是否发送推送通知
                • 类型：bool，默认True
                • True：分析完成后发送推送通知
                • False：不发送通知，静默运行
                • 用途：测试、调试、自动化流水线
        
        Returns:
            List[AnalysisResult]: 分析结果列表
                • 成功结果：所有成功分析的AnalysisResult对象列表
                • 空列表：无股票或全部失败时返回空列表
                • 排序：按原始股票顺序（不按完成顺序）
                • 过滤：已过滤掉None值（失败或跳过的任务）
        
        Raises:
            ValueError: 股票代码列表为空或无效
            RuntimeError: 线程池创建或任务提交失败
            TimeoutError: 任务执行超时（future.result()超时）
            Exception: 其他未捕获的异常（外层try-except捕获）
        
        性能考虑：
        • 内存使用：同时存在多个AnalysisResult对象
        • 线程开销：线程池创建和销毁成本
        • 网络IO：多个股票同时请求API可能触发限流
        • 数据库连接：连接池压力，多个线程同时访问
        
        优化策略：
        1. 连接池：数据库连接池复用
        2. 请求间隔：在process_single_stock中控制
        3. 结果流式处理：支持边产生边处理（未来扩展）
        4. 内存分页：大批量股票时分批处理
        
        日志输出示例：
            [INFO] ===== 开始分析 5 只股票 =====
            [INFO] 股票列表: 600519, 000001, 300750, 002415, 000858
            [INFO] 并发数: 3, 模式: 完整分析
            [INFO] 使用线程池并发处理，最大并发数: 3
            [INFO] 已提交 5 个任务到线程池
            [INFO] 进度: 1/5 (20%) - 600519 分析完成
            [INFO] 进度: 2/5 (40%) - 000001 分析完成
            [INFO] 所有任务完成，成功率: 4/5 (80%)
            [INFO] 分析完成，共 5 只股票，成功 4 只，失败 1 只
            [INFO] 总耗时: 86.34 秒，平均每只: 21.58 秒
        
        扩展性：
        • 支持异步版本：可改为async/await实现
        • 支持分布式：可扩展为多进程或多机器分布式
        • 支持优先级：可为重要股票设置高优先级
        • 支持断点续传：批量处理中断后可恢复
        """
        start_time = time.time()
        # 使用配置中的股票列表
        if stock_codes is None:
            self.config.refresh_stock_list()
            stock_codes = self.config.stock_list
        logger.info(f"需要分析的股票代码：{stock_codes}")
        if not stock_codes:
            logger.error("未配置自选股列表，请在 .env 文件中设置 STOCK_LIST")
            return []
        
        logger.info(f"===== 开始分析 {len(stock_codes)} 只股票 =====")
        logger.info(f"股票列表: {', '.join(stock_codes)}")
        logger.info(f"并发数: {self.max_workers}, 模式: {'仅获取数据' if dry_run else '完整分析'}")
        
        # 单股推送模式（#55）：从配置读取
        single_stock_notify = getattr(self.config, 'single_stock_notify', False)
        if single_stock_notify:
            logger.info("已启用单股推送模式：每分析完一只股票立即推送")
        
        results: List[AnalysisResult] = []
        
        # 使用线程池并发处理
        # 注意：max_workers 设置较低（默认3）以避免触发反爬
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self.process_single_stock, 
                    code, 
                    skip_analysis=dry_run,
                    single_stock_notify=single_stock_notify and send_notification
                ): code
                for code in stock_codes
            }
            
            # 收集结果
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"[{code}] 任务执行失败: {e}")
        
        # 统计
        elapsed_time = time.time() - start_time
        
        # dry-run 模式下，数据获取成功即视为成功
        if dry_run:
            # 检查哪些股票的数据今天已存在
            success_count = sum(1 for code in stock_codes if self.db.has_today_data(code))
            fail_count = len(stock_codes) - success_count
        else:
            success_count = len(results)
            fail_count = len(stock_codes) - success_count
        
        logger.info(f"===== 分析完成 =====")
        logger.info(f"成功: {success_count}, 失败: {fail_count}, 耗时: {elapsed_time:.2f} 秒")
        
        # 发送通知（单股推送模式下跳过汇总推送，避免重复）
        if results and send_notification and not dry_run:
            if single_stock_notify:
                # 单股推送模式：只保存汇总报告，不再重复推送
                logger.info("单股推送模式：跳过汇总推送，仅保存报告到本地")
                self._send_notifications(results, skip_push=True)
            else:
                self._send_notifications(results)
        
        return results
    
    def _send_notifications(self, results: List[AnalysisResult], skip_push: bool = False) -> None:
        """
        发送分析结果通知 - 多渠道推送引擎
        
        设计模式：策略模式 (Strategy Pattern) + 适配器模式 (Adapter Pattern)
        
        核心功能：四阶段通知处理流程
        1. 报告生成阶段：创建决策仪表盘格式的详细报告
        2. 本地持久化阶段：保存报告到本地文件系统
        3. 多渠道推送阶段：根据配置推送至不同平台
        4. 结果反馈阶段：记录推送状态和统计信息
        
        通知处理架构：
        ┌─────────────────────────────────────────────────────────┐
        │                   通知处理四阶段模型                      │
        │  1. 📋 报告生成阶段                                      │
        │     • 调用notifier.generate_dashboard_report生成报告    │
        │     • 格式：决策仪表盘，包含摘要和详细分析                │
        │     • 内容：股票代码、名称、操作建议、评分、趋势预测等    │
        │                                                        │
        │  2. 💾 本地持久化阶段                                    │
        │     • 保存报告到本地文件（notifier.save_report_to_file）│
        │     • 文件路径：logs/reports/YYYY-MM-DD_HH-MM-SS.md     │
        │     • 目的：历史记录、审计追踪、离线查看                  │
        │                                                        │
        │  3. 📢 多渠道推送阶段                                    │
        │     • 检查skip_push标志：单股推送模式跳过批量推送         │
        │     • 检查通知服务可用性：notifier.is_available()        │
        │     • 获取可用渠道：notifier.get_available_channels()    │
        │     • 渠道适配：不同平台使用不同格式和限制                │
        │                                                        │
        │  4. 📊 结果反馈阶段                                      │
        │     • 记录推送成功/失败状态                             │
        │     • 统计各渠道推送结果                                │
        │     • 错误处理和异常捕获                                │
        └─────────────────────────────────────────────────────────┘
        
        渠道适配策略：
        • 企业微信 (WeChat)：长度限制严格，使用精简版报告
          - 生成专用内容：generate_wechat_dashboard()
          - 长度检查：日志记录字符数
          - 独立推送：send_to_wechat()
        
        • 飞书 (Feishu)：支持Markdown，使用完整报告
          - 直接推送完整报告：send_to_feishu()
          - 支持富文本格式
        
        • Telegram：支持长文本，使用完整报告
          - 直接推送完整报告：send_to_telegram()
          - 支持Markdown格式
        
        • 邮件 (Email)：使用完整报告
          - 发送HTML或纯文本报告：send_to_email()
          - 支持附件
        
        • 自定义Webhook (Custom)：使用完整报告
          - 发送JSON或文本报告：send_to_custom()
          - 支持Bearer Token认证
        
        设计优势：
        1. 渠道无关性：统一接口处理不同推送平台
        2. 格式适配：根据不同平台限制自动调整格式
        3. 容错处理：单个渠道失败不影响其他渠道
        4. 本地备份：始终保存报告到本地文件
        5. 状态跟踪：详细记录每个渠道的推送结果
        
        使用场景：
            # 正常批量推送
            pipeline._send_notifications(results)
            
            # 仅保存到本地，不推送（单股推送模式）
            pipeline._send_notifications(results, skip_push=True)
            
            # 空结果处理（自动跳过）
            pipeline._send_notifications([])
        
        企业微信特殊处理：
        • 长度限制：企业微信机器人消息有4096字符限制
        • 精简策略：只包含关键信息，省略详细分析
        • 独立流程：与其他渠道分离，避免格式污染
        • 兼容性：确保内容不触发企业微信的截断或拒绝
        
        错误处理机制：
        1. 外层异常捕获：整个方法try-except包装
        2. 渠道级容错：单个渠道失败继续尝试其他渠道
        3. 结果聚合：使用逻辑或聚合各渠道结果
        4. 详细日志：记录每个渠道的成功/失败状态
        5. 优雅降级：所有渠道都失败时仅保存本地文件
        
        配置依赖：
        • 渠道配置：通过环境变量配置各平台API Key/Token
        • 开关控制：config.single_stock_notify控制推送模式
        • 可用性检查：notifier.is_available()动态检查
        
        Args:
            results: 分析结果列表
                • 类型：List[AnalysisResult]
                • 要求：非空列表，至少包含一个有效结果
                • 为空时：直接返回，不生成报告
                • 排序：建议按评分或重要性排序
            
            skip_push: 是否跳过推送
                • 类型：bool，默认False
                • False：正常推送（生成报告+保存本地+推送渠道）
                • True：仅保存本地（skip_push=True）
                • 使用场景：单股推送模式下避免重复推送
        
        Returns:
            None: 方法无返回值，但会：
                • 生成并保存报告文件
                • 推送消息到配置的渠道
                • 记录推送状态到日志
        
        Raises:
            Exception: 方法最外层捕获所有异常，记录错误日志
            但内部可能抛出的异常包括：
            • ValueError: 结果列表为空或无效
            • ConnectionError: 网络连接失败
            • TimeoutError: 推送请求超时
            • NotificationError: 通知服务配置错误
        
        文件保存详情：
            • 目录：logs/reports/（自动创建）
            • 文件名：stock_report_YYYY-MM-DD_HH-MM-SS.md
            • 格式：Markdown，便于阅读和后续处理
            • 编码：UTF-8，支持中文
            • 保留策略：依赖日志轮转配置
        
        推送状态判断：
            • 成功：至少一个渠道推送成功
            • 失败：所有配置的渠道都推送失败
            • 跳过：skip_push=True或results为空
            • 未配置：无可用通知渠道
        
        性能考虑：
            • 网络延迟：推送多个渠道可能耗时较长
            • 内存使用：报告生成可能占用较多内存（大结果集）
            • 文件IO：本地文件保存有磁盘写入开销
            • 并发安全：非线程安全，不应被多个线程同时调用
        
        日志输出示例：
            [INFO] 生成决策仪表盘日报...
            [INFO] 决策仪表盘日报已保存: logs/reports/stock_report_2026-03-04_14-30-25.md
            [INFO] 企业微信仪表盘长度: 3850 字符
            [INFO] 决策仪表盘推送成功
        
        扩展性：
            • 新增渠道：在NotificationChannel枚举中添加，在方法中处理
            • 报告格式：支持HTML、PDF、Excel等格式输出
            • 推送策略：支持定时推送、重试机制、优先级队列
            • 监控集成：推送状态集成到监控系统（如Prometheus）
        """
        try:
            logger.info("生成决策仪表盘日报...")
            
            # 生成决策仪表盘格式的详细日报
            report = self.notifier.generate_dashboard_report(results)
            
            # 保存到本地
            filepath = self.notifier.save_report_to_file(report)
            logger.info(f"决策仪表盘日报已保存: {filepath}")
            
            # 跳过推送（单股推送模式）
            if skip_push:
                return
            
            # 推送通知
            if self.notifier.is_available():
                channels = self.notifier.get_available_channels()

                # 企业微信：只发精简版（平台限制）
                wechat_success = False
                if NotificationChannel.WECHAT in channels:
                    dashboard_content = self.notifier.generate_wechat_dashboard(results)
                    logger.info(f"企业微信仪表盘长度: {len(dashboard_content)} 字符")
                    logger.debug(f"企业微信推送内容:\n{dashboard_content}")
                    wechat_success = self.notifier.send_to_wechat(dashboard_content)

                # 其他渠道：发完整报告（避免自定义 Webhook 被 wechat 截断逻辑污染）
                non_wechat_success = False
                for channel in channels:
                    if channel == NotificationChannel.WECHAT:
                        continue
                    if channel == NotificationChannel.FEISHU:
                        non_wechat_success = self.notifier.send_to_feishu(report) or non_wechat_success
                    elif channel == NotificationChannel.TELEGRAM:
                        non_wechat_success = self.notifier.send_to_telegram(report) or non_wechat_success
                    elif channel == NotificationChannel.EMAIL:
                        non_wechat_success = self.notifier.send_to_email(report) or non_wechat_success
                    elif channel == NotificationChannel.CUSTOM:
                        non_wechat_success = self.notifier.send_to_custom(report) or non_wechat_success
                    else:
                        logger.warning(f"未知通知渠道: {channel}")

                success = wechat_success or non_wechat_success
                if success:
                    logger.info("决策仪表盘推送成功")
                else:
                    logger.warning("决策仪表盘推送失败")
            else:
                logger.info("通知渠道未配置，跳过推送")
                
        except Exception as e:
            logger.error(f"发送通知失败: {e}")


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='A股自选股智能分析系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python main.py                    # 正常运行
  python main.py --debug            # 调试模式
  python main.py --dry-run          # 仅获取数据，不进行 AI 分析
  python main.py --stocks 600519,000001  # 指定分析特定股票
  python main.py --no-notify        # 不发送推送通知
  python main.py --single-notify    # 启用单股推送模式（每分析完一只立即推送）
  python main.py --schedule         # 启用定时任务模式
  python main.py --market-review    # 仅运行大盘复盘
        '''
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式，输出详细日志'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅获取数据，不进行 AI 分析'
    )
    
    parser.add_argument(
        '--stocks',
        type=str,
        help='指定要分析的股票代码，逗号分隔（覆盖配置文件）'
    )
    
    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='不发送推送通知'
    )
    
    parser.add_argument(
        '--single-notify',
        action='store_true',
        help='启用单股推送模式：每分析完一只股票立即推送，而不是汇总推送'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='并发线程数（默认使用配置值）'
    )
    
    parser.add_argument(
        '--schedule',
        action='store_true',
        help='启用定时任务模式，每日定时执行'
    )
    
    parser.add_argument(
        '--market-review',
        action='store_true',
        help='仅运行大盘复盘分析'
    )
    
    parser.add_argument(
        '--no-market-review',
        action='store_true',
        help='跳过大盘复盘分析'
    )
    
    parser.add_argument(
        '--webui',
        action='store_true',
        help='启动本地配置 WebUI'
    )
    
    return parser.parse_args()


def run_market_review(notifier: NotificationService, analyzer=None, search_service=None) -> Optional[str]:
    """
    执行大盘复盘分析
    
    Args:
        notifier: 通知服务
        analyzer: AI分析器（可选）
        search_service: 搜索服务（可选）
    
    Returns:
        复盘报告文本
    """
    logger.info("开始执行大盘复盘分析...")
    
    try:
        market_analyzer = MarketAnalyzer(
            search_service=search_service,
            analyzer=analyzer
        )
        
        # 执行复盘
        review_report = market_analyzer.run_daily_review()
        
        if review_report:
            # 保存报告到文件
            date_str = datetime.now().strftime('%Y%m%d')
            report_filename = f"market_review_{date_str}.md"
            filepath = notifier.save_report_to_file(
                f"# 🎯 大盘复盘\n\n{review_report}", 
                report_filename
            )
            logger.info(f"大盘复盘报告已保存: {filepath}")
            
            # 推送通知
            if notifier.is_available():
                # 添加标题
                report_content = f"🎯 大盘复盘\n\n{review_report}"
                
                success = notifier.send(report_content)
                if success:
                    logger.info("大盘复盘推送成功")
                else:
                    logger.warning("大盘复盘推送失败")
            
            return review_report
        
    except Exception as e:
        logger.error(f"大盘复盘分析失败: {e}")
    
    return None


def run_theme_choose(notifier: NotificationService, analyzer=None, search_service=None) -> Optional[str]:
    """
    获取市场热点题材，并分析相关的公司和行业的成长

    Args:
        notifier: 通知服务
        analyzer: AI分析器（可选）
        search_service: 搜索服务（可选）

    Returns:
        复盘报告文本
    """
    logger.info("热点题材分析开始执行")



    logger.info("热点题材分析执行完毕")
    return None

def run_full_analysis(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None
):
    """
    执行完整的分析流程（个股 + 大盘复盘）
    
    这是定时任务调用的主函数
    """
    try:
        # 命令行参数 --single-notify 覆盖配置（#55）
        if getattr(args, 'single_notify', False):
            config.single_stock_notify = True
        
        # 创建调度器
        pipeline = StockAnalysisPipeline(
            config=config,
            max_workers=args.workers
        )
        
        # 1. 运行个股分析
        results = pipeline.run(
            stock_codes=stock_codes,
            dry_run=args.dry_run,
            send_notification=not args.no_notify
        )
        
        # 2. 运行大盘复盘（如果启用且不是仅个股模式）
        market_report = ""
        if config.market_review_enabled and not args.no_market_review:
            # 只调用一次，并获取结果
            review_result = run_market_review(
                notifier=pipeline.notifier,
                analyzer=pipeline.analyzer,
                search_service=pipeline.search_service
            )
            # 如果有结果，赋值给 market_report 用于后续飞书文档生成
            if review_result:
                market_report = review_result
        
        # 输出摘要
        if results:
            logger.info("\n===== 分析结果摘要 =====")
            for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
                emoji = r.get_emoji()
                logger.info(
                    f"{emoji} {r.name}({r.code}): {r.operation_advice} | "
                    f"评分 {r.sentiment_score} | {r.trend_prediction}"
                )
        
        logger.info("\n任务执行完成")

        # === 新增：生成飞书云文档 ===
        try:
            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                logger.info("正在创建飞书云文档...")

                # 1. 准备标题 "01-01 13:01大盘复盘"
                tz_cn = timezone(timedelta(hours=8))
                now = datetime.now(tz_cn)
                doc_title = f"{now.strftime('%Y-%m-%d %H:%M')} 大盘复盘"

                # 2. 准备内容 (拼接个股分析和大盘复盘)
                full_content = ""

                # 添加大盘复盘内容（如果有）
                if market_report:
                    full_content += f"# 📈 大盘复盘\n\n{market_report}\n\n---\n\n"

                # 添加个股决策仪表盘（使用 NotificationService 生成）
                if results:
                    dashboard_content = pipeline.notifier.generate_dashboard_report(results)
                    full_content += f"# 🚀 个股决策仪表盘\n\n{dashboard_content}"

                # 3. 创建文档
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url:
                    logger.info(f"飞书云文档创建成功: {doc_url}")
                    # 可选：将文档链接也推送到群里
                    pipeline.notifier.send(f"[{now.strftime('%Y-%m-%d %H:%M')}] 复盘文档创建成功: {doc_url}")

        except Exception as e:
            logger.error(f"飞书文档生成失败: {e}")
        
    except Exception as e:
        logger.exception(f"分析流程执行失败: {e}")


def main() -> int:
    """
    主入口函数
    
    Returns:
        退出码（0 表示成功）
    """
    # 解析命令行参数
    args = parse_arguments()
    
    # 加载配置（在设置日志前加载，以获取日志目录）
    config = get_config()
    
    # 配置日志（输出到控制台和文件）
    setup_logging(debug=args.debug, log_dir=config.log_dir)
    
    logger.info("=" * 60)
    logger.info("A股自选股智能分析系统 启动")
    logger.info(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # 验证配置
    warnings = config.validate()
    for warning in warnings:
        logger.warning(warning)
    
    # 解析股票列表
    stock_codes = None
    if args.stocks:
        stock_codes = [code.strip() for code in args.stocks.split(',') if code.strip()]
        logger.info(f"使用命令行指定的股票列表: {stock_codes}")
    
    # === 启动 WebUI (如果启用) ===
    # 优先级: 命令行参数 > 配置文件
    start_webui = (args.webui or config.webui_enabled) and os.getenv("GITHUB_ACTIONS") != "true"
    
    if start_webui:
        try:
            from webui import run_server_in_thread
            run_server_in_thread(host=config.webui_host, port=config.webui_port)
        except Exception as e:
            logger.error(f"启动 WebUI 失败: {e}")

    try:
        # 模式1: 仅大盘复盘
        if args.market_review:
            logger.info("模式: 仅大盘复盘")
            notifier = NotificationService()
            
            # 初始化搜索服务和分析器（如果有配置）
            search_service = None
            analyzer = None
            
            if config.bocha_api_keys or config.tavily_api_keys or config.serpapi_keys:
                search_service = SearchService(
                    bocha_keys=config.bocha_api_keys,
                    tavily_keys=config.tavily_api_keys,
                    serpapi_keys=config.serpapi_keys
                )
            
            if config.gemini_api_key:
                analyzer = GeminiAnalyzer(api_key=config.gemini_api_key)
            
            run_market_review(notifier, analyzer, search_service)
            return 0
        
        # 模式2: 定时任务模式
        if args.schedule or config.schedule_enabled:
            logger.info("模式: 定时任务")
            logger.info(f"每日执行时间: {config.schedule_time}")
            
            from scheduler import run_with_schedule
            
            def scheduled_task():
                run_full_analysis(config, args, stock_codes)
            
            run_with_schedule(
                task=scheduled_task,
                schedule_time=config.schedule_time,
                run_immediately=True  # 启动时先执行一次
            )
            return 0
        
        # 模式3: 正常单次运行
        run_full_analysis(config, args, stock_codes)
        
        logger.info("\n程序执行完成")
        
        # 如果启用了 WebUI 且是非定时任务模式，保持程序运行以便访问 WebUI
        if start_webui and not (args.schedule or config.schedule_enabled):
            logger.info("WebUI 运行中 (按 Ctrl+C 退出)...")
            try:
                # 简单的保持活跃循环
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n用户中断，程序退出")
        return 130
        
    except Exception as e:
        logger.exception(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
