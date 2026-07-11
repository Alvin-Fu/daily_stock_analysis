from urllib.parse import urlparse
import os

from typing import Optional

# A股代码前缀 → 交易所（唯一权威映射，main.py 的周/月线闸门和各 fetcher 的代码转换都用它）
# 沪市：600/601/603/605 主板、688 科创板
# 深市：000/001/002/003 主板、300/301 创业板
# 北交所：43x/83x/87x/88x/92x
_SH_PREFIXES = ('600', '601', '603', '605', '688')
_SZ_PREFIXES = ('000', '001', '002', '003', '300', '301')
_BJ_PREFIXES = ('43', '83', '87', '88', '92')


def get_a_share_exchange(stock_code: str) -> Optional[str]:
    """判断 6 位 A 股代码所属交易所，返回 'SH'/'SZ'/'BJ'，非 A 股返回 None"""
    code = stock_code.strip()
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith(_SH_PREFIXES):
        return 'SH'
    if code.startswith(_SZ_PREFIXES):
        return 'SZ'
    if code.startswith(_BJ_PREFIXES):
        return 'BJ'
    return None


def extract_last_segment_standard(url):
    # 解析URL，获取路径部分
    parsed_url = urlparse(url)
    # 拆分路径，取最后一个元素
    path_parts = parsed_url.path.rsplit('/', 1)
    if len(path_parts) < 2:
        return ""  # 路径异常时返回空
    last_part = path_parts[-1]
    # 移除.pdf后缀
    target_str = last_part.rsplit('.', 1)[0] if '.' in last_part else last_part
    return target_str


