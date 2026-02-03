import datetime
import akshare as ak
import pandas as pd

from datetime import datetime, timedelta

def get_start_date(stock_code, end_date: str, days: int) -> str:
    # 获取接口返回的上市时间
    def _get_listing_date(code):
        info_df = ak.stock_individual_info_em(symbol=code)
        info_dict = dict(zip(info_df['item'], info_df['value']))
        listing_date = info_dict.get("上市时间") or info_dict.get("上市日期")
        return pd.to_datetime(str(listing_date)).strftime("%Y%m%d")

    listing_date = _get_listing_date(stock_code)
    # 获取最早日线日期
    trade_date = get_stock_earliest_trade_date(stock_code, listing_date)
    if trade_date is None:
        from datetime import timedelta
        start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days * 2)
        return start_dt.strftime('%Y-%m-%d')
    return str(trade_date)


def get_stock_earliest_trade_date(stock_code: str, listing_date: str) -> str:
    """
    获取股票能查询到的最早日线交易日期（精准，非接口返回的上市时间）

    Args:
        stock_code: A股6位数字代码（如"000738"航发控制、"600000"浦发银行）

    Returns:
        str: 最早日线交易日期（格式：YYYY-MM-DD）

    Raises:
        ValueError: 股票代码错误/无任何日线数据/接口调用失败
    """
    # 1. 验证股票代码格式（必须6位纯数字）
    if not (isinstance(stock_code, str) and stock_code.isdigit() and len(stock_code) == 6):
        raise ValueError(f"股票代码{stock_code}格式错误，需为6位纯数字（如000738）")

    # 2. 定义拉取日线数据的接口列表（主接口+备选接口，兜底）
    data_fetch_functions = [
        # 主接口：东方财富日线（优先）
        lambda code, l_date: ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=l_date,  # A股最早开市时间
            end_date=add_one_day(l_date, 20),
            adjust="qfq"  # 先取原始数据，避免复权导致数据缺失
        )
    ]

    # 3. 依次调用接口，直到获取到非空数据
    df_earliest = None
    l_date = listing_date
    for i in range(10):
        for idx, fetch_func in enumerate(data_fetch_functions):
            df = fetch_func(stock_code, l_date)
            if not df.empty:
                df_earliest = df
                break
        l_date = add_one_day(l_date)
        if not df_earliest.empty:
            break

    print(f"df_earliest:{df_earliest}")
    # 4. 无任何接口返回数据的异常处理
    if df_earliest is None or df_earliest.empty:
        raise ValueError(f"股票{stock_code}无可用的日线数据，请检查代码是否为有效A股")

    # 5. 提取最早日期并标准化格式
    # 不同接口的日期列名可能是"日期"或"trade_date"，兼容处理
    date_col = "日期" if "日期" in df_earliest.columns else "trade_date"
    earliest_date = df_earliest[date_col].iloc[0]  # 取第一行的日期（最早）

    # 统一转换为YYYY-MM-DD格式（处理接口返回的不同格式：str/int/datetime）
    try:
        earliest_date = pd.to_datetime(str(earliest_date)).strftime("%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"日期格式转换失败，原始值：{earliest_date}，错误：{str(e)}")

    return earliest_date


def add_one_day(date_str: str, days: int = 1):
    """
    给8位数字格式的日期加一天，返回同格式结果
    :param date_str: 输入日期，如"20240528"（字符串）或20240528（数字）
    :return: 加一天后的日期，格式与输入类型一致（字符串/整数）
    """
    try:
        # 步骤1：统一转为字符串，再解析成datetime对象（%Y%m%d对应8位日期格式）
        date_obj = datetime.strptime(str(date_str), "%Y%m%d")
        # 步骤2：加一天（timedelta表示时间增量，days=1即加1天）
        new_date_obj = date_obj + timedelta(days=days)
        # 步骤3：转回8位字符串格式
        new_date_str = new_date_obj.strftime("%Y%m%d")

        # 保持输出类型与输入一致（输入是数字则返回整数，否则返回字符串）
        if isinstance(date_str, (int, float)):
            return int(new_date_str)
        return new_date_str

    except ValueError as e:
        # 处理无效日期的情况（比如20240230、20241301等）
        raise ValueError(f"输入的日期格式无效：{date_str}，错误信息：{e}")
