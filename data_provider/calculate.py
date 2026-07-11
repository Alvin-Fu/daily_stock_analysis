import logging
import pandas as pd

# 配置日志
logger = logging.getLogger(__name__)

class CalculateFetcher:
    def calculate_ma_ema(
            self,
            df: pd.DataFrame,
            price_col: str = 'close',
            ma_periods: list = [5, 10, 20, 50, 120, 200],
            ema_periods: list = [5, 10, 20, 50, 120, 200]
    ) -> pd.DataFrame:
        """
        计算MA（均线）和EMA（指数均线）
        :param df: 周线/月线DataFrame（需包含price_col字段）
        :param price_col: 计算基准字段（默认收盘价close）
        :param ma_periods: 要计算的MA周期（如[5,10,20]周/月）
        :return: 新增MA/EMA列的DataFrame
        """
        df = df.copy()
        if df.empty or price_col not in df.columns:
            return df
        df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        # ---------------------- 1. 计算MA（简单移动平均） ----------------------
        for period in ma_periods:
            # rolling(window=period)：固定周期窗口；min_periods=1：数据不足时也计算
            ma = df[price_col].rolling(window=period, min_periods=1).mean().round(2)
            key = f'ma{period}'
            logger.warning(f"ma key: [{key}]")
            df[key] = ma

        # ---------------------- 2. 计算EMA（指数移动平均） ----------------------
        for period in ema_periods:
            # ewm(span=period)：指数加权窗口；adjust=False：使用递归公式（行业标准）
            ema = df[price_col].ewm(span=period, adjust=False, min_periods=1).mean().round(2)
            key = f'ema{period}'
            logger.warning(f"ma key: [{key}]")
            df[key] = ema
        return df

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        计算指标：
        - MA5, MA10, MA20: 移动平均线
        - Volume_Ratio: 量比（今日成交量 / 5日平均成交量）
        """
        df = df.copy()
        if 'date' in df.columns:
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        df = self.calculate_macd_signal(df)
        df = self.calculate_ma_ema(df, "close")
        logging.warning(f"{df['ma200'][-10:]}200天线")
        logging.warning(f"{df['ma5'][-10:]}均线")
        logger.warning(f"df data: [{df.head(1)}]")
        # 量比：当日成交量 / 5日平均成交量
        avg_volume_5 = df['volume'].rolling(window=5, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / avg_volume_5.shift(1)
        df['volume_ratio'] = df['volume_ratio'].fillna(1.0).round(2)
        logger.info(f"calculate indicators success")
        return df


    def calculate_macd_signal(
        self,
        df: pd.DataFrame,
        short_window=12, long_window=26, signal_window=9) -> pd.DataFrame:
        """计算MACD的信号"""
        df = df.copy()

        df['EMA_short'] = df['close'].ewm(span=short_window, adjust=False).mean()
        df['EMA_long'] = df['close'].ewm(span=long_window, adjust=False).mean()
        df['DIF'] = df['EMA_short'] - df['EMA_long']  # 快线
        df['DEA'] = df['DIF'].ewm(span=signal_window, adjust=False).mean()
        df['MACD'] = df['DIF'] - df['DEA']
        # 计算交叉点
        df['macd_signal'] = 0
        df.loc[(df['DIF'].shift(1) <= df['DEA'].shift(1)) & (df['DIF'] > df['DEA']), 'macd_signal'] = 1
        df.loc[(df['DIF'].shift(1) >= df['DEA'].shift(1)) & (df['DIF'] < df['DEA']), 'macd_signal'] = -1
        return df
