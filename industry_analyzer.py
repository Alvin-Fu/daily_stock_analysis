# -*- coding: utf-8 -*-
"""
产业链分析

流程：
1. 产业名 → 匹配东财行业/概念板块，拿成分股做「落地依据」
2. LLM 把产业拆成上/中/下游环节，每个环节给出代表公司
3. 公司名对回 stock_basic 校验成 A 股代码（对不上的标记未匹配）
4. 产业链结构缓存进 industry_chain 表（默认 7 天有效）
5. 每个环节挑代表公司复用单股分析链路
6. LLM 汇总各环节+公司结论，产出产业链报告（markdown）
"""
import json
import logging
import re
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# 每类板块最多取几个匹配板块、每个板块最多取多少成分股喂给 LLM
_MAX_BOARDS_PER_TYPE = 2
_MAX_CONSTITUENTS_PER_BOARD = 30

_CHAIN_SYSTEM_PROMPT = "你是一名熟悉A股市场的产业链研究员，输出必须是合法 JSON，不要输出 JSON 以外的任何内容。"

_CHAIN_PROMPT_TEMPLATE = """请对「{industry}」产业做产业链拆解，按上游、中游、下游梳理环节，每个环节给出 2-4 家 A 股上市代表公司。

要求：
1. 公司必须是 A 股上市公司，公司名用 A 股证券简称（如「宁德时代」「天齐锂业」），不要写全称
2. 环节划分要覆盖产业链主要价值环节，宁缺毋滥
3. reason 一句话说明该公司在这个环节的地位
{grounding_section}
严格按以下 JSON 格式输出：
{{
  "industry": "{industry}",
  "overview": "两三句话的产业概述（现状、驱动因素）",
  "segments": [
    {{
      "position": "上游",
      "name": "环节名（如 锂矿/锂盐）",
      "description": "该环节一句话说明",
      "companies": [
        {{"name": "公司A股简称", "reason": "入选理由"}}
      ]
    }}
  ]
}}"""

_SYNTHESIS_PROMPT_TEMPLATE = """你是一名A股产业链研究员。以下是「{industry}」产业链结构和各环节代表公司的最新 AI 个股分析结论，请给出产业层面的综合判断。

## 产业链结构
{chain_text}

## 代表公司个股分析结论
{company_text}

请输出（markdown，不要代码块包裹）：
1. **产业景气度判断**：结合各环节公司的评分和趋势，判断当前产业整体处于什么阶段
2. **环节强弱对比**：上/中/下游哪个环节当前更强势、议价能力和机会在哪
3. **关注建议**：值得优先跟踪的 2-3 家公司及理由
4. **主要风险**：产业层面的共性风险

要求：结论要落在给出的数据上，不要泛泛而谈；总字数 500 字以内。"""


class IndustryChainAnalyzer:
    """产业链分析编排器"""

    def __init__(
        self,
        db,
        analyzer,
        akshare_fetcher,
        analyze_stock_fn: Callable[[str], Any],
        search_service=None,
    ):
        """
        Args:
            db: DatabaseManager
            analyzer: GeminiAnalyzer（需要 generate_text）
            akshare_fetcher: AkshareFetcher（需要 get_board_names / get_board_constituents）
            analyze_stock_fn: 单股分析函数，入参 code，返回 AnalysisResult 或 None
            search_service: 可选，搜索服务（补充产业新闻做拆解依据）
        """
        self.db = db
        self.analyzer = analyzer
        self.akshare_fetcher = akshare_fetcher
        self.analyze_stock_fn = analyze_stock_fn
        self.search_service = search_service

    # ==================== 对外入口 ====================

    def analyze(
        self,
        industry_name: str,
        max_companies_per_segment: int = 2,
        chain_max_age_days: int = 7,
    ) -> str:
        """
        对产业名做上下游分析，返回 markdown 报告

        Raises:
            ValueError: 产业链生成失败（LLM 不可用或返回不可解析）
        """
        industry_name = industry_name.strip()
        logger.info(f"===== 开始产业链分析: {industry_name} =====")

        # 1. 产业链结构（缓存优先）
        chain = self._get_or_build_chain(industry_name, chain_max_age_days)

        # 2. 挑各环节代表公司
        representatives = self._pick_representatives(chain, max_companies_per_segment)
        logger.info(f"[{industry_name}] 代表公司: {[(r['name'], r['code']) for r in representatives]}")

        # 3. 逐个跑单股分析
        results: Dict[str, Any] = {}
        for rep in representatives:
            code = rep['code']
            try:
                results[code] = self.analyze_stock_fn(code)
            except Exception as e:
                logger.error(f"[{code}] 产业链代表公司分析失败: {e} {traceback.format_exc()}")
                results[code] = None

        # 4. LLM 综合判断
        synthesis = self._synthesize(industry_name, chain, representatives, results)

        # 5. 组装报告
        return self._build_report(industry_name, chain, representatives, results, synthesis)

    # ==================== 产业链生成 ====================

    def _get_or_build_chain(self, industry_name: str, max_age_days: int) -> Dict[str, Any]:
        cached = self.db.get_industry_chain(industry_name, max_age_days=max_age_days)
        if cached:
            try:
                chain = json.loads(cached)
                logger.info(f"[{industry_name}] 命中产业链缓存")
                return chain
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[{industry_name}] 产业链缓存解析失败，重新生成: {e}")

        grounding = self._collect_board_grounding(industry_name)
        prompt = _CHAIN_PROMPT_TEMPLATE.format(
            industry=industry_name,
            grounding_section=grounding,
        )
        logger.info(f"[{industry_name}] 调用 LLM 生成产业链结构...")
        response = self.analyzer.generate_text(
            prompt,
            system_prompt=_CHAIN_SYSTEM_PROMPT,
            temperature=0.4,
        )
        chain = self._parse_chain_json(response)
        if not chain.get('segments'):
            raise ValueError(f"产业链生成失败：LLM 未返回有效环节（{industry_name}）")

        self._ground_companies(chain)
        self.db.save_industry_chain(industry_name, json.dumps(chain, ensure_ascii=False), data_source='llm')
        return chain

    def _collect_board_grounding(self, industry_name: str) -> str:
        """匹配相关板块并取成分股，作为 LLM 拆解的落地依据；失败返回空串（LLM 仍可独立拆解）"""
        lines: List[str] = []
        try:
            for board_type, type_label in (('industry', '行业板块'), ('concept', '概念板块')):
                names_df = self.akshare_fetcher.get_board_names(board_type)
                if names_df.empty or '板块名称' not in names_df.columns:
                    continue
                matched = [
                    n for n in names_df['板块名称'].astype(str)
                    if industry_name in n or n in industry_name
                ][:_MAX_BOARDS_PER_TYPE]
                for board_name in matched:
                    cons = self.akshare_fetcher.get_board_constituents(board_name, board_type)
                    if cons.empty or '名称' not in cons.columns:
                        continue
                    stock_names = cons['名称'].astype(str).head(_MAX_CONSTITUENTS_PER_BOARD).tolist()
                    lines.append(f"- {type_label}「{board_name}」成分股: {'、'.join(stock_names)}")
        except Exception as e:
            logger.warning(f"[{industry_name}] 获取板块成分股失败，跳过落地依据: {e}")

        if self.search_service is not None:
            try:
                resp = self.search_service.search(f"{industry_name} 产业链 上游 中游 下游 上市公司", max_results=5)
                context = resp.to_context()
                if context:
                    lines.append(f"- 近期资讯摘要:\n{context}")
            except Exception as e:
                logger.warning(f"[{industry_name}] 产业资讯搜索失败: {e}")

        if not lines:
            return ""
        joined = '\n'.join(lines)
        return f"4. 以下是相关板块成分股和资讯，选公司时优先从中选取，但不必局限于此：\n{joined}\n"

    @staticmethod
    def _parse_chain_json(response: str) -> Dict[str, Any]:
        """解析 LLM 返回的产业链 JSON（容忍 markdown 代码块包裹）"""
        text = (response or '').strip()
        # 去掉 ```json ... ``` 包裹
        fence = re.search(r'```(?:json)?\s*(.*?)```', text, re.DOTALL)
        if fence:
            text = fence.group(1).strip()
        # 兜底：截取首个 { 到末个 }
        if not text.startswith('{'):
            start, end = text.find('{'), text.rfind('}')
            if start == -1 or end <= start:
                raise ValueError(f"LLM 返回中未找到 JSON: {text[:200]}")
            text = text[start:end + 1]
        return json.loads(text)

    def _ground_companies(self, chain: Dict[str, Any]) -> None:
        """把 LLM 给的公司名对回 stock_basic，落地成代码；对不上的 code 置 None"""
        for segment in chain.get('segments', []):
            for company in segment.get('companies', []):
                name = (company.get('name') or '').strip()
                company['code'] = None
                if not name:
                    continue
                matches = self.db.find_stock_by_name(name, limit=3)
                if matches:
                    company['code'] = matches[0]['code']
                    company['name'] = matches[0]['name']  # 用库里的规范简称
                else:
                    logger.info(f"公司名未匹配到A股: {name}")

    # ==================== 代表公司与汇总 ====================

    @staticmethod
    def _pick_representatives(chain: Dict[str, Any], per_segment: int) -> List[Dict[str, Any]]:
        """每个环节取前 N 家已落地代码的公司，跨环节去重"""
        seen: set = set()
        reps: List[Dict[str, Any]] = []
        for segment in chain.get('segments', []):
            count = 0
            for company in segment.get('companies', []):
                code = company.get('code')
                if not code or code in seen or count >= per_segment:
                    continue
                seen.add(code)
                count += 1
                reps.append({
                    'code': code,
                    'name': company.get('name', ''),
                    'segment': f"{segment.get('position', '')}-{segment.get('name', '')}",
                })
        return reps

    @staticmethod
    def _chain_to_text(chain: Dict[str, Any]) -> str:
        lines = [f"产业概述: {chain.get('overview', '')}"]
        for segment in chain.get('segments', []):
            companies = '、'.join(
                f"{c.get('name')}({c.get('code') or '未匹配'})"
                for c in segment.get('companies', [])
            )
            lines.append(
                f"- {segment.get('position', '')}｜{segment.get('name', '')}: "
                f"{segment.get('description', '')}｜公司: {companies}"
            )
        return '\n'.join(lines)

    @staticmethod
    def _result_brief(rep: Dict[str, Any], result) -> str:
        if result is None:
            return f"- {rep['name']}({rep['code']})［{rep['segment']}］: 分析失败或无数据"
        return (
            f"- {rep['name']}({rep['code']})［{rep['segment']}］: "
            f"评分 {getattr(result, 'sentiment_score', 'N/A')}，"
            f"趋势 {getattr(result, 'trend_prediction', 'N/A')}，"
            f"建议 {getattr(result, 'operation_advice', 'N/A')}；"
            f"要点: {getattr(result, 'key_points', '') or getattr(result, 'analysis_summary', '')}"
        )

    def _synthesize(
        self,
        industry_name: str,
        chain: Dict[str, Any],
        representatives: List[Dict[str, Any]],
        results: Dict[str, Any],
    ) -> str:
        company_text = '\n'.join(
            self._result_brief(rep, results.get(rep['code'])) for rep in representatives
        ) or '（无成功的公司分析结果）'
        prompt = _SYNTHESIS_PROMPT_TEMPLATE.format(
            industry=industry_name,
            chain_text=self._chain_to_text(chain),
            company_text=company_text,
        )
        try:
            return self.analyzer.generate_text(prompt, temperature=0.5).strip()
        except Exception as e:
            logger.error(f"[{industry_name}] 产业综合判断生成失败: {e}")
            return "（综合判断生成失败，请参考上方各环节公司分析）"

    # ==================== 报告组装 ====================

    def _build_report(
        self,
        industry_name: str,
        chain: Dict[str, Any],
        representatives: List[Dict[str, Any]],
        results: Dict[str, Any],
        synthesis: str,
    ) -> str:
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        lines = [
            f"# {industry_name} 产业链分析报告",
            "",
            f"生成时间: {now}",
            "",
            "## 产业总览",
            "",
            chain.get('overview', ''),
            "",
            "## 产业链结构",
            "",
        ]
        for segment in chain.get('segments', []):
            lines.append(f"### {segment.get('position', '')}：{segment.get('name', '')}")
            lines.append("")
            if segment.get('description'):
                lines.append(segment['description'])
                lines.append("")
            lines.append("| 公司 | 代码 | 入选理由 | AI评分 | 建议 |")
            lines.append("|------|------|----------|--------|------|")
            for company in segment.get('companies', []):
                code = company.get('code')
                result = results.get(code) if code else None
                score = getattr(result, 'sentiment_score', '-') if result else '-'
                advice = getattr(result, 'operation_advice', '-') if result else '-'
                lines.append(
                    f"| {company.get('name', '')} | {code or '未匹配'} | "
                    f"{company.get('reason', '')} | {score} | {advice} |"
                )
            lines.append("")

        lines.append("## 代表公司分析结论")
        lines.append("")
        for rep in representatives:
            result = results.get(rep['code'])
            lines.append(f"### {rep['name']}（{rep['code']}）｜{rep['segment']}")
            lines.append("")
            if result is None:
                lines.append("分析失败或无数据。")
            else:
                lines.append(
                    f"**评分 {getattr(result, 'sentiment_score', 'N/A')}｜"
                    f"{getattr(result, 'trend_prediction', '')}｜"
                    f"建议 {getattr(result, 'operation_advice', '')}**"
                )
                lines.append("")
                summary = getattr(result, 'analysis_summary', '') or ''
                if summary:
                    lines.append(summary)
                risk = getattr(result, 'risk_warning', '') or ''
                if risk:
                    lines.append("")
                    lines.append(f"风险提示: {risk}")
            lines.append("")

        lines.append("## 产业综合判断")
        lines.append("")
        lines.append(synthesis)
        lines.append("")
        return '\n'.join(lines)
