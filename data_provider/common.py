from urllib.parse import urlparse
import os


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


