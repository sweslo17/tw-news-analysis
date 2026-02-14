"""Rule-based filter service for pipeline."""

import json
import re
from dataclasses import dataclass
from typing import Callable

from sqlalchemy.orm import Session

from app.models import (
    NewsArticle,
    FilterRule,
    FilterRuleType,
    FilterDecision,
    ArticleFilterResult,
    PipelineStage,
    ForceIncludeArticle,
)


@dataclass
class RuleFilterResult:
    """Result of rule-based filtering."""

    decision: FilterDecision
    rule_name: str | None = None
    reason: str | None = None


class RuleFilterService:
    """Service for rule-based article filtering."""

    # Default rules configuration
    DEFAULT_RULES = [
        {
            "name": "horoscope_filter",
            "description": "過濾星座運勢、塔羅牌、占卜相關內容",
            "rule_type": FilterRuleType.KEYWORD,
            "config": {
                "keywords": [
                    "星座運勢", "每日星座", "星座運程", "本週星座",
                    "塔羅", "占卜", "運勢分析", "星座解析",
                    "牡羊座", "金牛座", "雙子座", "巨蟹座",
                    "獅子座", "處女座", "天秤座", "天蠍座",
                    "射手座", "摩羯座", "水瓶座", "雙魚座",
                ],
                "match_fields": ["title", "tags"],
            },
        },
        {
            "name": "lottery_filter",
            "description": "過濾彩券開獎、樂透號碼相關內容",
            "rule_type": FilterRuleType.PATTERN,
            "config": {
                "patterns": [
                    r"威力彩.*開獎",
                    r"大樂透.*開獎",
                    r"今彩539.*開獎",
                    r"雙贏彩.*開獎",
                    r"開獎號碼",
                    r"中獎號碼",
                    r"頭獎.*億",
                    r"\d+期.*開獎",
                ],
                "match_fields": ["title"],
            },
        },
        {
            "name": "ad_filter",
            "description": "過濾廣告、業配相關內容",
            "rule_type": FilterRuleType.KEYWORD,
            "config": {
                "keywords": [
                    "[廣告]", "【廣告】", "廣編特輯", "業配文",
                    "贊助內容", "贊助文章", "合作專案",
                ],
                "match_fields": ["title"],
            },
        },
        {
            "name": "weather_routine_filter",
            "description": "過濾例行天氣預報（保留極端天氣）",
            "rule_type": FilterRuleType.PATTERN,
            "config": {
                "patterns": [
                    r"(明日|今日|週末)天氣",
                    r"一週天氣",
                    r"天氣預報",
                ],
                "match_fields": ["title"],
                "exclude_keywords": [  # 包含這些關鍵字時不過濾
                    "颱風", "暴雨", "豪雨", "水災", "地震",
                    "極端", "警報", "停班停課", "災情",
                ],
            },
        },
    ]

    def __init__(self, db: Session):
        self.db = db
        self._rule_handlers: dict[FilterRuleType, Callable] = {
            FilterRuleType.KEYWORD: self._apply_keyword_rule,
            FilterRuleType.PATTERN: self._apply_pattern_rule,
            FilterRuleType.CATEGORY: self._apply_category_rule,
        }
        self._force_include_ids: set[int] | None = None

    def ensure_default_rules(self) -> None:
        """Ensure default rules exist in database."""
        for rule_config in self.DEFAULT_RULES:
            existing = self.db.query(FilterRule).filter(
                FilterRule.name == rule_config["name"]
            ).first()

            if not existing:
                rule = FilterRule(
                    name=rule_config["name"],
                    description=rule_config["description"],
                    rule_type=rule_config["rule_type"],
                    config=json.dumps(rule_config["config"], ensure_ascii=False),
                )
                self.db.add(rule)

        self.db.commit()

    def get_active_rules(self) -> list[FilterRule]:
        """Get all active filter rules."""
        return self.db.query(FilterRule).filter(FilterRule.is_active == True).all()

    def _load_force_include_ids(self) -> set[int]:
        """Load force-include article IDs."""
        if self._force_include_ids is None:
            results = self.db.query(ForceIncludeArticle.article_id).all()
            self._force_include_ids = {r.article_id for r in results}
        return self._force_include_ids

    def filter_article(self, article: NewsArticle) -> RuleFilterResult:
        """
        Apply all active rules to a single article.

        Args:
            article: The article to filter

        Returns:
            RuleFilterResult with decision and details
        """
        # Check force-include first
        force_include_ids = self._load_force_include_ids()
        if article.id in force_include_ids:
            return RuleFilterResult(
                decision=FilterDecision.FORCE_INCLUDE,
                rule_name="force_include",
                reason="文章已被標記為強制納入",
            )

        # Apply each active rule
        for rule in self.get_active_rules():
            config = json.loads(rule.config)
            handler = self._rule_handlers.get(rule.rule_type)

            if handler and handler(article, config):
                # Update rule statistics
                rule.total_filtered_count += 1

                return RuleFilterResult(
                    decision=FilterDecision.FILTER,
                    rule_name=rule.name,
                    reason=rule.description,
                )

        # No rule matched - keep the article
        return RuleFilterResult(
            decision=FilterDecision.KEEP,
            rule_name=None,
            reason="通過所有規則檢查",
        )

    def filter_articles_batch(
        self,
        articles: list[NewsArticle],
        pipeline_run_id: int,
    ) -> tuple[list[NewsArticle], list[ArticleFilterResult]]:
        """
        Filter a batch of articles and create filter results.

        Args:
            articles: List of articles to filter
            pipeline_run_id: ID of the pipeline run

        Returns:
            Tuple of (passed_articles, filter_results)
        """
        passed_articles = []
        filter_results = []

        for article in articles:
            result = self.filter_article(article)

            filter_result = ArticleFilterResult(
                pipeline_run_id=pipeline_run_id,
                article_id=article.id,
                stage=PipelineStage.RULE_FILTER,
                decision=result.decision,
                rule_name=result.rule_name,
                reason=result.reason,
            )
            filter_results.append(filter_result)

            if result.decision in (FilterDecision.KEEP, FilterDecision.FORCE_INCLUDE):
                passed_articles.append(article)

        return passed_articles, filter_results

    def _get_field_value(self, article: NewsArticle, field: str) -> str:
        """Get field value from article as string."""
        if field == "title":
            return article.title or ""
        elif field == "tags":
            if article.tags:
                try:
                    tags = json.loads(article.tags)
                    return " ".join(tags) if isinstance(tags, list) else str(tags)
                except json.JSONDecodeError:
                    return article.tags
            return ""
        elif field == "category":
            return article.category or ""
        elif field == "sub_category":
            return article.sub_category or ""
        elif field == "summary":
            return article.summary or ""
        elif field == "content":
            return article.content or ""
        return ""

    def _apply_keyword_rule(
        self, article: NewsArticle, config: dict
    ) -> bool:
        """
        Apply keyword matching rule.

        Returns True if article should be filtered.
        """
        keywords = config.get("keywords", [])
        match_fields = config.get("match_fields", ["title"])

        for field in match_fields:
            field_value = self._get_field_value(article, field)
            for keyword in keywords:
                if keyword in field_value:
                    return True

        return False

    def _apply_pattern_rule(
        self, article: NewsArticle, config: dict
    ) -> bool:
        """
        Apply regex pattern matching rule.

        Returns True if article should be filtered.
        """
        patterns = config.get("patterns", [])
        match_fields = config.get("match_fields", ["title"])
        exclude_keywords = config.get("exclude_keywords", [])

        # Check exclude keywords first
        for field in match_fields:
            field_value = self._get_field_value(article, field)
            for keyword in exclude_keywords:
                if keyword in field_value:
                    return False  # Don't filter if exclude keyword found

        # Check patterns
        for field in match_fields:
            field_value = self._get_field_value(article, field)
            for pattern in patterns:
                if re.search(pattern, field_value, re.IGNORECASE):
                    return True

        return False

    def _apply_category_rule(
        self, article: NewsArticle, config: dict
    ) -> bool:
        """
        Apply category-based rule.

        Returns True if article should be filtered.
        """
        categories = config.get("categories", [])
        sub_categories = config.get("sub_categories", [])

        if article.category and article.category in categories:
            return True

        if article.sub_category and article.sub_category in sub_categories:
            return True

        return False
