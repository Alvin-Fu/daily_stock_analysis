# -*- coding: utf-8 -*-
"""
查询输入识别：把用户输入解析成「公司（代码）」或「产业名」

规则：
1. 6 位数字（可带 .SH/.SZ/.BJ 后缀）→ 股票代码
2. 其余文本先查 stock_basic 按公司名匹配（精确 → 模糊）
3. 都匹配不到 → 当作产业名
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

QUERY_TYPE_COMPANY = 'company'
QUERY_TYPE_INDUSTRY = 'industry'

_CODE_RE = re.compile(r'^(\d{6})(?:\.(?:SH|SZ|BJ))?$', re.IGNORECASE)


@dataclass
class QueryResolution:
    """输入解析结果"""
    query: str                      # 原始输入
    query_type: str                 # company / industry
    code: Optional[str] = None      # 公司类型时的股票代码
    name: Optional[str] = None      # 公司名（能查到时）
    candidates: List[Dict[str, Any]] = field(default_factory=list)  # 模糊匹配的其他候选

    def describe(self) -> str:
        if self.query_type == QUERY_TYPE_COMPANY:
            return f"公司分析: {self.name or self.query}({self.code})"
        return f"产业分析: {self.query}"


def resolve_query(raw: str, db) -> QueryResolution:
    """
    解析用户输入

    Args:
        raw: 用户输入（代码/公司名/产业名）
        db: DatabaseManager 实例（需要 get_stock_basic / find_stock_by_name）
    """
    query = (raw or '').strip()
    if not query:
        raise ValueError("查询内容为空")

    # 1. 股票代码
    m = _CODE_RE.match(query)
    if m:
        code = m.group(1)
        basic = db.get_stock_basic(code)
        name = basic.name if basic is not None else None
        return QueryResolution(query=query, query_type=QUERY_TYPE_COMPANY, code=code, name=name)

    # 2. 公司名匹配
    matches = db.find_stock_by_name(query)
    if matches:
        best = matches[0]
        candidates = matches[1:]
        if candidates:
            logger.info(
                f"[{query}] 命中 {len(matches)} 个候选，选用 {best['name']}({best['code']})，"
                f"其他候选: {[(c['name'], c['code']) for c in candidates]}"
            )
        return QueryResolution(
            query=query,
            query_type=QUERY_TYPE_COMPANY,
            code=best['code'],
            name=best['name'],
            candidates=candidates,
        )

    # 3. 产业名
    logger.info(f"[{query}] 未匹配到公司名，按产业名处理")
    return QueryResolution(query=query, query_type=QUERY_TYPE_INDUSTRY)
