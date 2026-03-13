-- 1. 股票/ETF 基础信息表（SQLite兼容版）
CREATE TABLE IF NOT EXISTS stock_basic (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- SQLite自增主键用INTEGER
    ts_code TEXT NOT NULL DEFAULT '', -- '股票代码'
    ts_name TEXT NOT NULL DEFAULT '', --  '股票名称',
    industry TEXT , --  '所属行业',
    list_date TEXT NOT NULL DEFAULT '' , -- '上市日期',
    fullname TEXT , --  '全称',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- 记录插入时间
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- SQLite不支持ON UPDATE，后续代码维护
    CONSTRAINT uk_stock_basic_ts_code UNIQUE (ts_code)  -- 唯一约束（命名规范）
) ;--COMMENT='股票基础信息';  -- SQLite的表注释写法

-- 2. 股票/ETF 价格数据表（SQLite兼容版）
CREATE TABLE IF NOT EXISTS stock_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL DEFAULT '' , --  '股票代码',
    trade_date TEXT NOT NULL DEFAULT '' , --  '交易日期',
    open_price REAL NOT NULL DEFAULT 0 , --  '开盘价',
    high_price REAL NOT NULL DEFAULT 0 , --  '最高价',
    low_price REAL NOT NULL DEFAULT 0 , --  '最低价',
    close_price REAL NOT NULL DEFAULT 0 , --  '收盘价',
    vol REAL NOT NULL DEFAULT 0 , --  '成交量',
    amount REAL NOT NULL DEFAULT 0 , --  '成交额',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_stock_price_ts_date UNIQUE (ts_code, trade_date)  -- 复合唯一约束
) ;--COMMENT='股票价格数据表';

-- 3. 股票/ETF 价格数据表（SQLite兼容版）
CREATE TABLE IF NOT EXISTS stock__weekly_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL DEFAULT '' , --  '股票代码',
    trade_date TEXT NOT NULL DEFAULT '' , --  '交易日期',
    open_price REAL NOT NULL DEFAULT 0 , --  '开盘价',
    high_price REAL NOT NULL DEFAULT 0 , --  '最高价',
    low_price REAL NOT NULL DEFAULT 0 , --  '最低价',
    close_price REAL NOT NULL DEFAULT 0 , --  '收盘价',
    vol REAL NOT NULL DEFAULT 0 , --  '成交量',
    amount REAL NOT NULL DEFAULT 0 , --  '成交额',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_stock_price_ts_date UNIQUE (ts_code, trade_date)  -- 复合唯一约束
) ;--COMMENT='股票周价格数据表';

-- 3. 当天预测的数据（SQLite兼容版）
CREATE TABLE IF NOT EXISTS daily_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL DEFAULT '' , --  '股票代码',
    forecast_date TEXT NOT NULL DEFAULT '' , -- '预测时间',
    forecast_rue TEXT NOT NULL DEFAULT '' , --  '预测结果',
    practical_rue TEXT NOT NULL DEFAULT '' , --  '实际结果',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_daily_forecast_ts_date UNIQUE (ts_code, forecast_date)  -- 复合唯一约束
) ; --COMMENT='每日预测的数据'