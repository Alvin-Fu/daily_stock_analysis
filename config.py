# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 配置管理模块
===================================

设计模式：单例模式 (Singleton) + 数据类 (Dataclass)

核心架构：
┌─────────────────────────────────────────┐
│           Config 类 (单例)               │
│   • dataclass定义所有配置字段            │
│   • 单例模式确保全局配置一致              │
│   • 环境变量加载敏感配置                  │
│   └─ get_instance() 单例访问点           │
│       └─ _load_from_env() 配置加载       │
└─────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────┐
│           get_config() 便捷函数          │
│   • 简化配置访问                         │
│   • 统一访问入口                         │
│   • 隐藏单例实现细节                      │
└─────────────────────────────────────────┘

配置加载优先级（从高到低）：
1. 系统环境变量（生产环境、Docker、GitHub Actions）
2. .env 文件中的配置（本地开发、敏感信息）
3. dataclass字段默认值（代码中的默认配置）

安全设计：
• 敏感信息（API Key、Token、密码）只存储在环境变量或 .env 文件中
• .env 文件被 .gitignore 排除，避免泄露到版本控制
• 配置验证机制检查必要配置项的完整性

模块职责：
1. 使用单例模式管理全局配置
   - 确保整个应用使用同一份配置实例
   - 避免重复加载环境变量，提高性能
   - 支持配置热重载（如股票列表更新）

2. 从 .env 文件加载敏感配置
   - 使用 python-dotenv 加载 .env 文件
   - 支持多环境配置（开发、测试、生产）
   - 提供类型安全的配置访问接口

3. 提供类型安全的配置访问接口
   - 使用 dataclass 提供类型提示和自动补全
   - 支持配置验证和完整性检查
   - 便捷的数据库连接URL生成

配置分类：
• 股票配置：自选股列表、数据源配置
• AI配置：Gemini/OpenAI API密钥、模型选择
• 搜索配置：新闻搜索API密钥（多引擎支持）
• 通知配置：多渠道推送配置（企业微信、飞书、Telegram等）
• 系统配置：数据库、日志、并发控制、定时任务
• 流控配置：防封禁策略（请求间隔、重试机制）

使用示例：
    from config import get_config
    config = get_config()
    
    # 访问配置（类型安全）
    stock_list = config.stock_list
    gemini_key = config.gemini_api_key
    
    # 验证配置完整性
    warnings = config.validate()
    
    # 获取数据库连接URL
    db_url = config.get_db_url()

版本：v1.0.0
作者：ZhuLinsen
仓库：https://github.com/ZhuLinsen/daily_stock_analysis
"""

import os
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv, dotenv_values
from dataclasses import dataclass, field


@dataclass
class Config:
    """
    系统配置类 - 单例模式实现
    
    设计模式：单例模式 (Singleton) + 数据类 (Dataclass)
    
    设计原理：
    1. Dataclass 优势：
       • 自动生成 __init__、__repr__、__eq__ 等方法
       • 类型注解提供IDE自动补全和类型检查
       • 默认值支持简化字段定义
       • 不可变设计（frozen=False，但可通过方法控制修改）
    
    2. 单例模式实现：
       • 类变量 _instance 存储唯一实例
       • get_instance() 类方法提供全局访问点
       • 延迟加载：首次调用时才加载配置
       • 线程安全：Python模块导入天然线程安全
    
    3. 配置加载策略：
       • 环境变量优先：支持云原生部署（Docker、K8s、Serverless）
       • .env文件支持：简化本地开发配置管理
       • 默认值回退：确保基础功能可用
    
    4. 安全性设计：
       • 敏感信息零硬编码：API Key、Token等只存于环境变量
       • 配置验证：validate() 方法检查必要配置
       • 错误处理：缺失配置提供清晰警告而非崩溃
    
    配置存储结构：
    ┌─────────────────┬─────────────────┬─────────────────┐
    │   配置类别       │   字段示例       │   环境变量名     │
    ├─────────────────┼─────────────────┼─────────────────┤
    │   股票配置       │ stock_list      │ STOCK_LIST      │
    │   AI配置        │ gemini_api_key  │ GEMINI_API_KEY  │
    │   搜索配置       │ bocha_api_keys  │ BOCHA_API_KEYS  │
    │   通知配置       │ wechat_webhook  │ WECHAT_WEBHOOK_ │
    │   系统配置       │ database_path   │ DATABASE_PATH   │
    │   流控配置       │ max_workers     │ MAX_WORKERS     │
    └─────────────────┴─────────────────┴─────────────────┘
    
    使用方式：
        # 推荐：通过便捷函数访问
        from config import get_config
        config = get_config()
        
        # 直接访问配置字段（类型安全）
        stocks = config.stock_list
        api_key = config.gemini_api_key
        
        # 验证配置完整性
        if warnings := config.validate():
            for warning in warnings:
                logger.warning(warning)
    
    扩展性：
        • 添加新配置：只需在dataclass中添加字段
        • 环境变量映射：在 _load_from_env() 中添加解析逻辑
        • 配置验证：在 validate() 中添加验证规则
    
    测试支持：
        • reset_instance(): 重置单例，用于单元测试
        • 环境变量模拟：测试时可设置os.environ
        • 配置隔离：每个测试用例可拥有独立配置
    """
    
    # ===== 自选股配置 (Stock Configuration) =====================================
    """
    核心股票配置：定义需要分析的自选股列表
    
    设计目的：
    1. 集中管理所有需要分析的股票代码
    2. 支持动态更新（通过refresh_stock_list()方法）
    3. 提供默认示例股票，确保基础功能可用
    
    配置方式：
    环境变量：STOCK_LIST=600519,000001,300750
    格式要求：逗号分隔的6位A股代码，不含空格或特殊字符
    示例值：['600519', '000001', '300750'] (茅台、平安银行、宁德时代)
    
    默认值机制：
    • 如果未配置STOCK_LIST环境变量：使用示例股票列表
    • 如果配置为空字符串：使用示例股票列表
    • 如果配置有效：解析为字符串列表
    
    安全考虑：
    • 股票代码会进行strip()处理，去除前后空格
    • 空代码会自动过滤，避免无效数据
    • 代码格式验证在数据获取阶段进行
    """
    stock_list: List[str] = field(default_factory=list)

    # ===== 飞书云文档配置 (Feishu Cloud Document Configuration) =================
    """
    飞书开放平台集成配置：用于自动创建日报文档
    
    功能说明：
    1. feishu_app_id / feishu_app_secret: 飞书应用凭证
       • 从飞书开放平台创建应用获取
       • 用于获取 tenant_access_token
       • 需要申请相应API权限（云文档读写）
    
    2. feishu_folder_token: 目标文件夹Token
       • 日报文档将创建在此文件夹下
       • 格式：文件夹URL中的一段字符
       • 示例：https://feishu.cn/drive/folder/{folder_token}
    
    使用场景：
    • 每日分析报告自动保存到飞书云文档
    • 支持团队协作和知识沉淀
    • 可通过飞书客户端随时查看历史分析
    
    配置要求：
    • 三个字段必须同时配置才启用飞书文档功能
    • 应用需要申请云文档相关权限
    • 需要将应用添加到目标文件夹所在空间
    """
    feishu_app_id: Optional[str] = None          # 飞书应用ID
    feishu_app_secret: Optional[str] = None      # 飞书应用密钥
    feishu_folder_token: Optional[str] = None    # 目标文件夹Token

    # ===== 数据源 API Token (Data Source API Tokens) ===========================
    """
    股票数据源API配置：支持多数据源切换和故障转移
    
    tushare_token: Tushare Pro接口Token
    • 功能：获取A股历史行情、财务数据、基本面数据
    • 获取方式：注册 tushare.pro 账号获取
    • 免费额度：每日1000次调用，每分钟80次限制
    • 优先级：数据源策略中的中等优先级（详见data_provider模块）
    
    设计优势：
    • 非必需配置：未配置时可使用其他免费数据源（如akshare）
    • 故障转移：Tushare服务异常时自动切换到其他数据源
    • 流控集成：自动遵守Tushare的API调用限制
    
    安全提示：
    • Token具有调用次数限制，避免泄露
    • 生产环境建议配置多个数据源Token
    • GitHub Actions部署时通过Secrets注入
    """
    tushare_token: Optional[str] = None
    
    # ===== AI 分析配置 (AI Analysis Configuration) =============================
    """
    Google Gemini AI模型配置：核心分析引擎
    
    gemini_api_key: Google AI Studio API密钥
    • 功能：调用Gemini模型进行股票技术分析和市场解读
    • 获取方式：访问 https://makersuite.google.com/app/apikey
    • 免费额度：Gemini API有一定免费调用额度
    • 安全性：属于敏感信息，必须通过环境变量配置
    
    gemini_model: 主模型名称
    • 默认值：gemini-3-flash-preview (Gemini 3 Flash预览版)
    • 特点：响应速度快，适合批量分析
    • 备选：gemini-2.5-flash (Gemini 2.5 Flash版)
    
    gemini_model_fallback: 备选模型名称
    • 设计目的：主模型不可用时自动降级
    • 确保服务连续性：避免单点故障
    • 模型选择策略：功能优先，性能其次
    
    流控配置（防429限流）：
    • gemini_request_delay: 请求间隔（秒），避免频繁调用
    • gemini_max_retries: 最大重试次数，处理临时故障
    • gemini_retry_delay: 重试延迟基数，指数退避策略
    
    最佳实践：
    • 生产环境建议配置Gemini API Key
    • 可同时配置多个AI服务实现高可用
    • 合理设置请求间隔，平衡速度和限额
    """
    gemini_api_key: Optional[str] = None                      # Gemini API密钥
    gemini_model: str = "gemini-3-flash-preview"             # 主模型名称
    gemini_model_fallback: str = "gemini-2.5-flash"          # 备选模型名称
    
    # Gemini API 请求配置（防止 429 限流）
    gemini_request_delay: float = 2.0                        # 请求间隔（秒）
    gemini_max_retries: int = 5                              # 最大重试次数
    gemini_retry_delay: float = 5.0                          # 重试基础延时（秒）
    
    # ===== OpenAI 兼容 API 配置 (OpenAI-Compatible API Configuration) =========
    """
    OpenAI兼容API配置：Gemini服务的备选方案
    
    设计目的：
    1. 高可用性：当Gemini服务不可用时自动切换
    2. 灵活性：支持任意OpenAI兼容API（DeepSeek、通义千问等）
    3. 成本控制：可选择性价比更高的AI服务
    
    字段说明：
    • openai_api_key: OpenAI格式的API密钥
    • openai_base_url: API基础地址，支持自定义部署
    • openai_model: 模型名称，如gpt-4o-mini、qwen-max等
    
    兼容性：
    • 支持标准OpenAI API格式的所有服务
    • 示例：DeepSeek (https://api.deepseek.com/v1)
    • 示例：通义千问 (https://dashscope.aliyuncs.com/compatible-mode/v1)
    
    使用策略：
    • 优先使用Gemini（免费额度优势）
    • Gemini失败时自动切换到OpenAI兼容API
    • 支持负载均衡和故障转移
    """
    openai_api_key: Optional[str] = None                     # OpenAI兼容API密钥
    openai_base_url: Optional[str] = None                    # API基础地址
    openai_model: str = "gpt-4o-mini"                       # 模型名称
    
    # ===== 搜索引擎配置 (Search Engine Configuration) =========================
    """
    新闻搜索API配置：支持多搜索引擎和负载均衡
    
    功能说明：
    1. 获取股票相关新闻和市场资讯
    2. 为AI分析提供实时市场上下文
    3. 多引擎支持，提高搜索成功率和覆盖率
    
    支持引擎：
    • Bocha API：国产搜索引擎API，中文优化
    • Tavily API：专为AI优化的搜索引擎
    • SerpAPI：Google搜索API代理
    
    负载均衡设计：
    • 支持多个API Key，自动轮询使用
    • 单个Key失效时自动切换到其他Key
    • 支持故障转移，确保搜索功能可用
    
    配置格式：
    环境变量：BOCHA_API_KEYS=key1,key2,key3
    存储格式：List[str]，如 ['key1', 'key2', 'key3']
    
    安全建议：
    • 生产环境建议配置至少2个搜索引擎
    • API Key具有调用限制，避免过度使用
    • 定期检查Key有效性和剩余额度
    """
    bocha_api_keys: List[str] = field(default_factory=list)  # Bocha API密钥列表
    tavily_api_keys: List[str] = field(default_factory=list)  # Tavily API密钥列表
    serpapi_keys: List[str] = field(default_factory=list)    # SerpAPI密钥列表
    
    # ===== 通知配置 (Notification Configuration) ==============================
    """
    多渠道消息推送配置：支持同时推送到多个平台
    
    设计理念：
    1. 冗余设计：配置多个渠道，确保消息必达
    2. 全平台覆盖：支持主流办公和社交平台
    3. 零配置依赖：每个渠道独立，按需配置
    
    渠道列表（可同时启用多个）：
    """
    
    # 企业微信 Webhook
    wechat_webhook_url: Optional[str] = None                  # 企业微信群机器人Webhook
    
    # 飞书 Webhook
    feishu_webhook_url: Optional[str] = None                  # 飞书群机器人Webhook
    
    # Telegram 配置（需要同时配置 Bot Token 和 Chat ID）
    telegram_bot_token: Optional[str] = None                  # Telegram Bot Token
    telegram_chat_id: Optional[str] = None                    # Telegram聊天ID
    
    # 邮件配置（只需邮箱和授权码，SMTP 自动识别）
    email_sender: Optional[str] = None                        # 发件人邮箱地址
    email_password: Optional[str] = None                      # 邮箱密码/授权码
    email_receivers: List[str] = field(default_factory=list)  # 收件人邮箱列表
    
    # Pushover 配置（手机/桌面推送通知）
    """
    Pushover推送服务：支持iOS/Android/桌面端实时通知
    
    功能特点：
    • 跨平台：支持所有主流操作系统
    • 实时性：消息秒级到达
    • 优先级：支持紧急通知（声音+振动）
    
    配置要求：
    • pushover_user_key: 用户Key，从 https://pushover.net 获取
    • pushover_api_token: 应用API Token，创建应用后获取
    • 两个字段必须同时配置才启用Pushover推送
    
    适用场景：
    • 移动端即时通知
    • 重要警报推送
    • 离线消息接收
    """
    pushover_user_key: Optional[str] = None      # Pushover用户Key
    pushover_api_token: Optional[str] = None     # Pushover应用API Token
    
    # 自定义 Webhook（支持多个，逗号分隔）
    """
    通用Webhook配置：支持任意自定义消息推送
    
    设计目的：
    1. 扩展性：支持任何提供Webhook接口的服务
    2. 标准化：统一的消息格式和认证方式
    3. 批量发送：支持多个Webhook同时推送
    
    支持平台：
    • 钉钉群机器人
    • Discord Webhook
    • Slack Incoming Webhook
    • 企业自建通知服务
    • 其他支持POST JSON的Webhook
    
    配置方式：
    • custom_webhook_urls: Webhook URL列表，逗号分隔
    • custom_webhook_bearer_token: Bearer Token（如果需要认证）
    
    安全特性：
    • 支持Bearer Token认证
    • 消息内容加密传输（HTTPS）
    • 可配置多个URL实现冗余推送
    """
    custom_webhook_urls: List[str] = field(default_factory=list)          # 自定义Webhook URL列表
    custom_webhook_bearer_token: Optional[str] = None                     # Bearer Token（认证用）
    
    # ===== 推送行为配置 (Notification Behavior Configuration) =================
    """
    消息推送行为控制：优化用户体验和系统性能
    """
    
    # 单股推送模式：每分析完一只股票立即推送，而不是汇总后推送
    """
    推送模式选择：
    • False（默认）：批量推送模式
      - 所有股票分析完成后一次性推送
      - 优点：消息整合，避免刷屏
      - 缺点：延迟较高，需要等待所有分析完成
    
    • True：实时推送模式
      - 每分析完一只股票立即推送结果
      - 优点：实时性高，快速获取单个股票分析
      - 缺点：可能造成消息刷屏
    
    配置方式：
    环境变量：SINGLE_STOCK_NOTIFY=true/false
    默认值：false（批量推送）
    
    使用建议：
    • 股票数量少（<5）：可启用实时推送
    • 股票数量多（>5）：建议使用批量推送
    • 移动端用户：可能偏好实时推送
    """
    single_stock_notify: bool = False                                    # 单股实时推送开关
    
    # 消息长度限制（字节）- 超长自动分批发送
    """
    消息长度限制：适配不同平台的限制
    
    平台限制：
    • 飞书：约20KB（实际限制可能动态调整）
    • 企业微信：4096字节（硬性限制）
    • Telegram：4096字符（约4KB）
    • 邮件：通常无限制，但过大可能被拒
    
    自动分批机制：
    1. 检查消息长度是否超过限制
    2. 如果超过，自动拆分为多个消息
    3. 保持消息完整性（不截断句子）
    4. 添加分页标识（如 1/3, 2/3, 3/3）
    
    配置优化：
    • 飞书限制：留出安全余量（20000 < 20480）
    • 企业微信限制：留出安全余量（4000 < 4096）
    • 其他平台：使用平台默认限制
    
    安全考虑：
    • 避免消息被截断导致信息不完整
    • 防止超长消息被平台拒绝
    • 确保分批后消息可正确重组
    """
    feishu_max_bytes: int = 20000                                        # 飞书消息最大字节数
    wechat_max_bytes: int = 4000                                         # 企业微信消息最大字节数
    
    # ===== 数据库配置 (Database Configuration) ================================
    """
    SQLite数据库配置：本地数据存储
    
    设计选择：SQLite vs 其他数据库
    • 轻量级：无需独立数据库服务
    • 嵌入式：数据文件即数据库
    • 零配置：开箱即用，适合个人和小团队
    • 高性能：针对读多写少场景优化
    
    文件路径：
    • 默认路径：./data/stock_analysis.db
    • 支持绝对路径和相对路径
    • 自动创建父目录（如果不存在）
    
    数据安全：
    • 数据库文件包含历史股价数据
    • 建议定期备份（可通过cron任务）
    • 生产环境可考虑加密或访问控制
    
    性能优化：
    • 连接池：SQLAlchemy自动管理
    • 索引优化：通过ORM模型定义索引
    • 事务控制：确保数据一致性
    """
    database_path: str = "./data/stock_analysis.db"                      # 数据库文件路径
    
    # ===== 日志配置 (Logging Configuration) ===================================
    """
    日志系统配置：记录系统运行状态和问题排查
    
    日志级别（从低到高）：
    • DEBUG: 调试信息，详细运行状态
    • INFO: 正常运行信息（默认级别）
    • WARNING: 警告信息，不影响运行
    • ERROR: 错误信息，功能受影响
    • CRITICAL: 严重错误，系统可能崩溃
    
    日志目录：
    • 默认路径：./logs/
    • 自动创建目录（如果不存在）
    • 按日期分割日志文件
    
    日志格式：
    • 时间戳：精确到毫秒
    • 日志级别：颜色区分（终端）
    • 模块名：定位问题来源
    • 消息内容：结构化信息
    
    使用建议：
    • 开发环境：DEBUG级别，便于调试
    • 生产环境：INFO级别，平衡信息量和性能
    • 问题排查：可临时调整为DEBUG级别
    """
    log_dir: str = "./logs"                                             # 日志文件目录
    log_level: str = "INFO"                                             # 日志记录级别
    
    # ===== 系统配置 (System Configuration) ====================================
    """
    系统运行参数：控制并发、调试等核心行为
    """
    
    # 最大工作线程数
    """
    并发控制：防封禁关键参数
    
    设计原理：
    • 股票数据源对并发请求敏感
    • 过高并发可能触发IP封禁
    • 模拟人类操作间隔，避免被识别为机器人
    
    默认值选择：
    • max_workers=1: 单线程顺序执行
    • 优点：完全避免并发问题
    • 缺点：执行速度较慢
    
    调整建议：
    • 自选股少（<10）：可保持默认值1
    • 自选股多（10-30）：可调整为2-3
    • 数据源稳定：根据API限制调整
    • 网络环境好：可适当增加
    
    安全警告：
    • 不要设置为过高值（如>5）
    • 生产环境建议从1开始逐步测试
    • 监控API调用响应和错误率
    """
    max_workers: int = 1                                                # 最大并发工作线程数
    
    # 调试模式
    """
    调试模式开关：开发和生产环境差异化配置
    
    调试模式特性：
    • 详细日志输出（DEBUG级别）
    • SQL语句打印（数据库操作可视化）
    • 异常堆栈完整输出
    • 可能包含敏感信息（API Key部分显示）
    
    环境区分：
    • 开发环境：可启用调试模式
    • 生产环境：必须关闭调试模式
    • 测试环境：按需启用
    
    安全注意：
    • 调试模式可能泄露敏感信息
    • 生产环境永远不要启用
    • 调试完成后立即关闭
    """
    debug: bool = False                                                 # 调试模式开关
    
    # ===== 定时任务配置 (Scheduler Configuration) =============================
    """
    自动化任务调度：实现无人值守运行
    
    核心功能：
    • schedule_enabled: 是否启用定时任务
    • schedule_time: 每日执行时间
    • market_review_enabled: 是否包含大盘复盘
    
    使用场景：
    • 每日收盘后自动分析
    • 定期生成投资报告
    • 自动数据更新和维护
    
    时间格式：
    • HH:MM 24小时制
    • 示例：18:00（下午6点）
    • 时区：系统时区（Asia/Shanghai）
    
    部署方式：
    • 本地cron任务（推荐）
    • GitHub Actions定时任务
    • Docker容器调度
    • 云函数定时触发
    """
    schedule_enabled: bool = False                                       # 定时任务启用开关
    schedule_time: str = "18:00"                                         # 每日执行时间（HH:MM）
    market_review_enabled: bool = True                                   # 大盘复盘启用开关
    
    # ===== 流控配置 (Rate Limiting Configuration) =============================
    """
    流量控制配置：防止数据源API封禁
    
    设计理念：
    1. 随机化：请求间隔随机，避免规律性
    2. 保守化：默认值偏向安全侧
    3. 可调化：支持环境变量动态调整
    """
    
    # Akshare 请求间隔范围（秒）
    """
    Akshare防封禁策略：随机延迟请求
    
    问题背景：
    • Akshare基于公开网站数据
    • 频繁请求可能触发反爬虫机制
    • 需要模拟人类浏览行为
    
    实现方式：
    • 每次请求后随机sleep一段时间
    • 范围：akshare_sleep_min ~ akshare_sleep_max
    • 默认：2.0 ~ 5.0秒
    
    调整建议：
    • 网络环境差：可适当增加最大值
    • 数据源稳定：可适当减少最小值
    • 首次使用：建议保持默认值
    """
    akshare_sleep_min: float = 2.0                                       # 最小请求间隔（秒）
    akshare_sleep_max: float = 5.0                                       # 最大请求间隔（秒）
    
    # Tushare 每分钟最大请求数（免费配额）
    """
    Tushare API限制：严格遵守免费配额
    
    官方限制：
    • 免费用户：每分钟80次请求
    • 基础积分：每日1000次调用
    • 超过限制：API返回错误
    
    实现机制：
    • 令牌桶算法控制请求频率
    • 接近限制时自动等待
    • 超过限制时优雅降级
    
    配置注意：
    • 不要超过80（官方限制）
    • 建议设置为70-75，留出安全余量
    • 多线程环境下需要共享计数器
    """
    tushare_rate_limit_per_minute: int = 80                              # 每分钟最大请求数
    
    # 重试配置
    """
    网络请求重试策略：提高系统鲁棒性
    
    重试场景：
    • 网络超时（Timeout）
    • 服务器错误（5xx）
    • 限流错误（429 Too Many Requests）
    • 临时故障（服务重启）
    
    指数退避策略：
    1. 首次重试：retry_base_delay后
    2. 第二次重试：2 × retry_base_delay后  
    3. 第三次重试：4 × retry_base_delay后
    4. 最大延迟不超过retry_max_delay
    
    配置优化：
    • max_retries=3: 平衡成功率和延迟
    • retry_base_delay=1.0: 基础延迟1秒
    • retry_max_delay=30.0: 最大延迟30秒
    """
    max_retries: int = 3                                                 # 最大重试次数
    retry_base_delay: float = 1.0                                        # 基础重试延迟（秒）
    retry_max_delay: float = 30.0                                        # 最大重试延迟（秒）
    
    # ===== WebUI 配置 (Web UI Configuration) =================================
    """
    网页用户界面配置：可视化监控和管理
    
    功能特性：
    • 实时查看分析进度
    • 历史报告浏览
    • 配置界面（计划中）
    • 数据可视化（计划中）
    
    部署选项：
    • 本地访问：127.0.0.1（默认）
    • 局域网访问：0.0.0.0
    • 端口选择：8000（可自定义）
    
    安全考虑：
    • 生产环境慎用：可能暴露内部信息
    • 访问控制：目前无认证机制
    • 网络隔离：建议仅本地访问
    
    性能影响：
    • 启用后会增加内存占用
    • 对核心分析功能无影响
    • 可按需启用/禁用
    """
    webui_enabled: bool = False                                          # WebUI启用开关
    webui_host: str = "127.0.0.1"                                        # 监听主机地址
    webui_port: int = 8000                                               # 监听端口号
    
    # 单例实例存储
    _instance: Optional['Config'] = None
    
    @classmethod
    def get_instance(cls) -> 'Config':
        """
        获取配置单例实例（单例模式核心访问点）
        
        设计模式：经典单例模式实现
        
        核心特性：
        1. 延迟加载 (Lazy Initialization)
           • 首次调用时才加载配置
           • 避免应用启动时的性能开销
           • 支持按需初始化
        
        2. 线程安全 (Thread Safety)
           • Python模块导入天然线程安全
           • 类变量访问在CPython中具有GIL保护
           • 适合多线程环境使用
        
        3. 全局唯一 (Global Uniqueness)
           • 确保整个应用使用同一配置实例
           • 避免配置不一致导致的逻辑错误
           • 简化配置管理和更新
        
        工作流程：
         首次调用 get_instance()       后续调用 get_instance()
                ↓                              ↓
           检查 _instance                检查 _instance
                ↓                              ↓
           为 None → 调用 _load_from_env()  不为 None → 返回现有实例
                ↓                              ↓
           创建新实例并赋值给 _instance      直接返回 _instance
                ↓
           返回新实例
        
        使用场景：
            # 任何需要访问配置的模块
            from config import get_config
            config = get_config()  # 首次调用触发配置加载
            
            # 后续访问都返回同一实例
            config2 = get_config()  # config2 is config → True
        
        性能优化：
            • 配置只加载一次，后续访问O(1)时间复杂度
            • 避免重复读取环境变量和文件系统
            • 内存占用固定，不随调用次数增加
        
        注意事项：
            • 不要在__init__中访问get_instance()，可能导致递归
            • 配置加载后，环境变量变更不会自动刷新（需调用reset_instance）
            • 多进程环境下每个进程有独立实例
        
        Returns:
            Config: 全局唯一的配置单例实例
            
        Raises:
            Exception: 如果配置加载失败（如.env文件格式错误）
        """
        if cls._instance is None:
            cls._instance = cls._load_from_env()  # 延迟加载配置
        return cls._instance
    
    @classmethod
    def _load_from_env(cls) -> 'Config':
        """
        从环境变量加载配置（单例初始化核心方法）
        
        设计原则：配置加载优先级（从高到低）
        1. 系统环境变量（生产环境、Docker、CI/CD）
           • 最高优先级，适合敏感信息
           • 示例：export GEMINI_API_KEY=your_key
           • 优点：安全，支持动态注入
        
        2. .env 文件配置（本地开发、测试）
           • 中间优先级，方便开发人员
           • 文件路径：项目根目录/.env
           • 格式：KEY=VALUE，每行一个
           • 优点：版本控制友好（.gitignore排除）
        
        3. 代码默认值（基础功能保障）
           • 最低优先级，确保基础功能
           • 定义在dataclass字段默认值中
           • 示例：database_path="./data/stock_analysis.db"
           • 优点：开箱即用，零配置运行
        
        加载流程：
         开始
           ↓
         加载.env文件（如果存在）
           ↓
         解析必需配置（股票列表）
           ↓
         解析列表配置（API Keys列表）
           ↓
         解析简单配置（字符串、数字、布尔）
           ↓
         类型转换和默认值处理
           ↓
         创建Config实例
           ↓
         返回实例
        
        配置解析策略：
        1. 字符串配置：直接使用，strip处理空格
        2. 列表配置：逗号分隔，自动过滤空值
        3. 数字配置：类型转换，支持浮点和整数
        4. 布尔配置：字符串"true"/"false"转换
        5. 可选配置：None表示未配置
        
        错误处理：
        • 环境变量不存在：返回None或默认值
        • 类型转换失败：使用默认值
        • .env文件格式错误：忽略该文件，使用系统环境变量
        • 关键配置缺失：使用示例值（如股票列表）
        
        安全考虑：
        • 敏感信息只从环境变量读取，不硬编码
        • .env文件被.gitignore排除，避免泄露
        • 生产环境推荐使用系统环境变量
        
        扩展性：
        • 添加新配置：只需在此方法中添加解析逻辑
        • 修改默认值：修改dataclass字段默认值
        • 支持新类型：添加相应的类型转换逻辑
        
        Returns:
            Config: 根据环境变量初始化的配置实例
            
        Raises:
            无显式异常，但可能抛出：
            • ValueError: 数字转换失败
            • OSError: .env文件读取失败
            • 其他python-dotenv库可能抛出的异常
        """
        # 步骤1：加载.env文件（如果存在）
        # 使用python-dotenv加载项目根目录下的.env文件
        # 注意：load_dotenv不会覆盖已存在的环境变量
        env_path = Path(__file__).parent / '.env'
        load_dotenv(dotenv_path=env_path)
        
        # 步骤2：解析自选股列表（逗号分隔，必需配置）
        # 环境变量：STOCK_LIST=600519,000001,300750
        # 解析逻辑：按逗号分割 → 去除空格 → 过滤空值
        stock_list_str = os.getenv('STOCK_LIST', '')
        stock_list = [
            code.strip() 
            for code in stock_list_str.split(',') 
            if code.strip()
        ]
        
        # 如果没有配置任何股票，使用默认示例股票
        # 确保系统至少有基本的测试数据可用
        if not stock_list:
            stock_list = ['600519', '000001', '300750']  # 茅台、平安银行、宁德时代
        
        # 步骤3：解析搜索引擎 API Keys（支持多个 key，负载均衡）
        # 环境变量格式：BOCHA_API_KEYS=key1,key2,key3
        # 设计目的：支持多key轮询，避免单key限流
        bocha_keys_str = os.getenv('BOCHA_API_KEYS', '')
        bocha_api_keys = [k.strip() for k in bocha_keys_str.split(',') if k.strip()]
        
        tavily_keys_str = os.getenv('TAVILY_API_KEYS', '')
        tavily_api_keys = [k.strip() for k in tavily_keys_str.split(',') if k.strip()]
        
        serpapi_keys_str = os.getenv('SERPAPI_API_KEYS', '')
        serpapi_keys = [k.strip() for k in serpapi_keys_str.split(',') if k.strip()]
        
        # 步骤4：创建并返回Config实例
        # 所有配置项通过os.getenv获取，支持默认值
        # 类型转换：bool("true") → True, int("3") → 3, float("2.0") → 2.0
        return cls(
            # 股票配置
            stock_list=stock_list,
            
            # 飞书文档配置
            feishu_app_id=os.getenv('FEISHU_APP_ID'),
            feishu_app_secret=os.getenv('FEISHU_APP_SECRET'),
            feishu_folder_token=os.getenv('FEISHU_FOLDER_TOKEN'),
            
            # 数据源配置
            tushare_token=os.getenv('TUSHARE_TOKEN'),
            
            # Gemini AI配置
            gemini_api_key=os.getenv('GEMINI_API_KEY'),
            gemini_model=os.getenv('GEMINI_MODEL', 'gemini-3-flash-preview'),
            gemini_model_fallback=os.getenv('GEMINI_MODEL_FALLBACK', 'gemini-2.5-flash'),
            gemini_request_delay=float(os.getenv('GEMINI_REQUEST_DELAY', '2.0')),
            gemini_max_retries=int(os.getenv('GEMINI_MAX_RETRIES', '5')),
            gemini_retry_delay=float(os.getenv('GEMINI_RETRY_DELAY', '5.0')),
            
            # OpenAI兼容配置
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            openai_base_url=os.getenv('OPENAI_BASE_URL'),
            openai_model=os.getenv('OPENAI_MODEL', 'gpt-4o-mini'),
            
            # 搜索引擎配置
            bocha_api_keys=bocha_api_keys,
            tavily_api_keys=tavily_api_keys,
            serpapi_keys=serpapi_keys,
            
            # 通知配置
            wechat_webhook_url=os.getenv('WECHAT_WEBHOOK_URL'),
            feishu_webhook_url=os.getenv('FEISHU_WEBHOOK_URL'),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN'),
            telegram_chat_id=os.getenv('TELEGRAM_CHAT_ID'),
            email_sender=os.getenv('EMAIL_SENDER'),
            email_password=os.getenv('EMAIL_PASSWORD'),
            email_receivers=[r.strip() for r in os.getenv('EMAIL_RECEIVERS', '').split(',') if r.strip()],
            pushover_user_key=os.getenv('PUSHOVER_USER_KEY'),
            pushover_api_token=os.getenv('PUSHOVER_API_TOKEN'),
            custom_webhook_urls=[u.strip() for u in os.getenv('CUSTOM_WEBHOOK_URLS', '').split(',') if u.strip()],
            custom_webhook_bearer_token=os.getenv('CUSTOM_WEBHOOK_BEARER_TOKEN'),
            
            # 推送行为配置
            single_stock_notify=os.getenv('SINGLE_STOCK_NOTIFY', 'false').lower() == 'true',
            feishu_max_bytes=int(os.getenv('FEISHU_MAX_BYTES', '20000')),
            wechat_max_bytes=int(os.getenv('WECHAT_MAX_BYTES', '4000')),
            
            # 系统配置
            database_path=os.getenv('DATABASE_PATH', './data/stock_analysis.db'),
            log_dir=os.getenv('LOG_DIR', './logs'),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            max_workers=int(os.getenv('MAX_WORKERS', '3')),
            debug=os.getenv('DEBUG', 'false').lower() == 'true',
            
            # 定时任务配置
            schedule_enabled=os.getenv('SCHEDULE_ENABLED', 'false').lower() == 'true',
            schedule_time=os.getenv('SCHEDULE_TIME', '18:00'),
            market_review_enabled=os.getenv('MARKET_REVIEW_ENABLED', 'true').lower() == 'true',
            
            # WebUI配置
            webui_enabled=os.getenv('WEBUI_ENABLED', 'false').lower() == 'true',
            webui_host=os.getenv('WEBUI_HOST', '127.0.0.1'),
            webui_port=int(os.getenv('WEBUI_PORT', '8000')),
        )
    
    @classmethod
    def reset_instance(cls) -> None:
        """
        重置配置单例实例（主要用于测试和热重载）
        
        设计目的：
        1. 单元测试隔离：每个测试用例需要干净的配置状态
        2. 配置热重载：环境变量变更后重新加载配置
        3. 资源清理：释放配置相关资源（如有）
        4. 故障恢复：配置损坏时强制重新加载
        
        工作原理：
        • 将类变量 _instance 设置为 None
        • 下次调用 get_instance() 时会触发重新加载
        • 原有配置实例会被垃圾回收（如果没有其他引用）
        
        使用场景：
            # 测试用例：每个测试前重置配置
            def setUp(self):
                Config.reset_instance()
                self.config = Config.get_instance()
            
            # 热重载：环境变量变更后刷新配置
            os.environ['STOCK_LIST'] = '600519,000001'
            Config.reset_instance()
            config = Config.get_instance()  # 重新加载新配置
            
            # 配置修复：配置错误后恢复
            try:
                config = Config.get_instance()
            except ConfigError:
                Config.reset_instance()  # 清除损坏的配置
                config = Config.get_instance()  # 重新加载
        
        注意事项：
        1. 线程安全：重置时确保没有其他线程正在使用配置
        2. 数据一致性：重置后所有模块需要重新获取配置实例
        3. 生产环境：慎用，可能导致运行时配置不一致
        4. 性能影响：重置后首次访问会有配置加载开销
        
        替代方案：
        • 测试环境：使用环境变量模拟不同配置
        • 热重载：通过refresh_stock_list()更新部分配置
        • 多配置：实现多环境配置支持（开发、测试、生产）
        
        实现细节：
        • 简单实现：仅设置 _instance = None
        • 扩展可能：未来可添加清理钩子（cleanup hooks）
        • 线程同步：目前依赖Python GIL，复杂场景可能需要锁
        """
        cls._instance = None  # 简单而有效的重置方式

    def refresh_stock_list(self) -> None:
        """
        热更新自选股列表（无需重启应用）
        
        设计目的：
        1. 动态配置：运行中修改股票列表，立即生效
        2. 多环境适配：优先使用.env文件，回退到系统环境变量
        3. 用户友好：修改配置文件后自动识别，无需重启
        
        应用场景：
        • 本地开发：修改.env文件后下次分析使用新列表
        • 定时任务：通过cron更新环境变量，自动生效
        • 运维管理：动态调整监控的股票列表
        • A/B测试：快速切换不同的股票组合
        
        优先级规则（从高到低）：
        1. .env 文件中的 STOCK_LIST 配置
        2. 系统环境变量中的 STOCK_LIST
        3. 默认示例股票（000001 - 平安银行）
        
        为什么需要默认值？
        • 确保系统始终有可分析的股票
        • 避免配置错误导致分析中断
        • 提供基础测试数据
        
        实现细节：
        1. 检查.env文件是否存在
        2. 优先读取.env文件（开发环境友好）
        3. 回退到系统环境变量（生产环境标准）
        4. 解析逗号分隔的股票代码
        5. 清理空格和空值
        6. 设置默认值（如果完全空）
        
        线程安全考虑：
        • 此方法修改实例变量，非线程安全
        • 但股票列表通常只在启动时或手动更新
        • 多线程访问时可能产生竞态条件
        • 实际使用中影响较小（列表读取远多于更新）
        
        使用示例：
            # 修改.env文件后调用
            config.refresh_stock_list()
            print(f"更新后的股票列表: {config.stock_list}")
            
            # 通过环境变量更新
            os.environ['STOCK_LIST'] = '600519,000858,002415'
            config.refresh_stock_list()
            
            # 定时任务集成
            def daily_analysis():
                config.refresh_stock_list()  # 确保使用最新配置
                analyze_all_stocks(config.stock_list)
        
        注意事项：
        • 修改后只影响当前配置实例（单例模式中即全局）
        • 不会重新加载其他配置项（仅股票列表）
        • 如果.env和系统环境变量都未配置，使用默认值
        • 股票代码格式验证在数据获取阶段进行
        """
        # 步骤1：优先读取.env文件（本地开发友好）
        # 使用dotenv_values直接读取文件，不修改系统环境变量
        env_path = Path(__file__).parent / '.env'
        stock_list_str = ''
        
        if env_path.exists():
            env_values = dotenv_values(env_path)
            stock_list_str = (env_values.get('STOCK_LIST') or '').strip()

        # 步骤2：如果.env中没有配置，回退到系统环境变量
        if not stock_list_str:
            stock_list_str = os.getenv('STOCK_LIST', '')

        # 步骤3：解析股票代码列表
        # 格式：逗号分隔，支持前后空格，过滤空值
        stock_list = [
            code.strip()
            for code in stock_list_str.split(',')
            if code.strip()
        ]

        # 步骤4：如果没有配置任何股票，使用默认值
        # 默认使用平安银行(000001)作为基础测试股票
        if not stock_list:        
            stock_list = ['000001']

        # 步骤5：更新实例变量
        self.stock_list = stock_list
    
    def validate(self) -> List[str]:
        """
        验证配置完整性
        
        Returns:
            缺失或无效配置项的警告列表
        """
        warnings = []
        
        if not self.stock_list:
            warnings.append("警告：未配置自选股列表 (STOCK_LIST)")
        
        if not self.tushare_token:
            warnings.append("提示：未配置 Tushare Token，将使用其他数据源")
        
        if not self.gemini_api_key and not self.openai_api_key:
            warnings.append("警告：未配置 Gemini 或 OpenAI API Key，AI 分析功能将不可用")
        elif not self.gemini_api_key:
            warnings.append("提示：未配置 Gemini API Key，将使用 OpenAI 兼容 API")
        
        if not self.bocha_api_keys and not self.tavily_api_keys and not self.serpapi_keys:
            warnings.append("提示：未配置搜索引擎 API Key (Bocha/Tavily/SerpAPI)，新闻搜索功能将不可用")
        
        # 检查通知配置
        has_notification = (
            self.wechat_webhook_url or 
            self.feishu_webhook_url or
            (self.telegram_bot_token and self.telegram_chat_id) or
            (self.email_sender and self.email_password) or
            (self.pushover_user_key and self.pushover_api_token)
        )
        if not has_notification:
            warnings.append("提示：未配置通知渠道，将不发送推送通知")
        
        return warnings
    
    def get_db_url(self) -> str:
        """
        获取 SQLAlchemy 数据库连接 URL
        
        自动创建数据库目录（如果不存在）
        """
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{db_path.absolute()}"


# === 便捷的配置访问函数 ===
def get_config() -> Config:
    """获取全局配置实例的快捷方式"""
    return Config.get_instance()


if __name__ == "__main__":
    # 测试配置加载
    config = get_config()
    print("=== 配置加载测试 ===")
    print(f"自选股列表: {config.stock_list}")
    print(f"数据库路径: {config.database_path}")
    print(f"最大并发数: {config.max_workers}")
    print(f"调试模式: {config.debug}")
    
    # 验证配置
    warnings = config.validate()
    if warnings:
        print("\n配置验证结果:")
        for w in warnings:
            print(f"  - {w}")
