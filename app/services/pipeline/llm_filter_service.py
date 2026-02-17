"""LLM-based filter service for pipeline."""

import asyncio
import json
from typing import Type

from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    NewsArticle,
    ArticleFilterResult,
    PipelineStage,
    FilterDecision,
    ForceIncludeArticle,
)
from .llm_providers.base import BaseLLMProvider, ArticleInput, LLMFilterResponse
from .llm_providers.groq_provider import GroqProvider
from .llm_providers.anthropic_provider import AnthropicProvider
from .llm_providers.openai_provider import OpenAIProvider
from .llm_providers.google_provider import GoogleProvider


# Provider registry
PROVIDERS: dict[str, Type[BaseLLMProvider]] = {
    "groq": GroqProvider,
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "google": GoogleProvider,
}


class LLMFilterService:
    """Service for LLM-based article filtering."""

    def __init__(
        self,
        db: Session,
        provider_name: str | None = None,
        model: str | None = None,
    ):
        self.db = db
        self.provider_name = provider_name or settings.default_llm_provider
        self.model = model or settings.llm_model
        self._provider: BaseLLMProvider | None = None
        self._force_include_ids: set[int] | None = None

    def _get_provider(self) -> BaseLLMProvider:
        """Get or create the LLM provider instance."""
        if self._provider is None:
            provider_class = PROVIDERS.get(self.provider_name)
            if not provider_class:
                raise ValueError(
                    f"Unknown provider: {self.provider_name}. "
                    f"Available: {list(PROVIDERS.keys())}"
                )
            self._provider = provider_class()
        return self._provider

    def _load_force_include_ids(self) -> set[int]:
        """Load force-include article IDs."""
        if self._force_include_ids is None:
            results = self.db.query(ForceIncludeArticle.article_id).all()
            self._force_include_ids = {r.article_id for r in results}
        return self._force_include_ids

    def _article_to_input(self, article: NewsArticle) -> ArticleInput:
        """Convert NewsArticle to ArticleInput for LLM."""
        tags = None
        if article.tags:
            try:
                tags = json.loads(article.tags)
                if not isinstance(tags, list):
                    tags = [str(tags)]
            except json.JSONDecodeError:
                tags = [article.tags]

        return ArticleInput(
            article_id=article.id,
            title=article.title,
            tags=tags,
            category=article.category,
            sub_category=article.sub_category,
            summary=article.summary,
        )

    async def filter_article(
        self, article: NewsArticle
    ) -> tuple[FilterDecision, LLMFilterResponse]:
        """
        Filter a single article using LLM.

        Args:
            article: The article to filter

        Returns:
            Tuple of (FilterDecision, LLMFilterResponse)
        """
        # Check force-include first
        force_include_ids = self._load_force_include_ids()
        if article.id in force_include_ids:
            return (
                FilterDecision.FORCE_INCLUDE,
                LLMFilterResponse(
                    decision="keep",
                    confidence=1.0,
                    reason="文章已被標記為強制納入",
                ),
            )

        provider = self._get_provider()
        article_input = self._article_to_input(article)
        response = await provider.filter_article(article_input, self.model)

        # Convert LLM decision to FilterDecision
        if response.decision == "filter":
            decision = FilterDecision.FILTER
        else:
            decision = FilterDecision.KEEP

        return decision, response

    async def filter_articles_batch(
        self,
        articles: list[NewsArticle],
        pipeline_run_id: int,
        batch_size: int | None = None,
    ) -> tuple[list[NewsArticle], list[ArticleFilterResult]]:
        """
        Filter a batch of articles using LLM.

        Args:
            articles: List of articles to filter
            pipeline_run_id: ID of the pipeline run
            batch_size: Number of concurrent requests

        Returns:
            Tuple of (passed_articles, filter_results)
        """
        batch_size = batch_size or settings.llm_batch_size
        passed_articles = []
        filter_results = []

        # Process in batches to avoid rate limiting
        for i in range(0, len(articles), batch_size):
            batch = articles[i : i + batch_size]

            # Create tasks for concurrent processing
            tasks = [self.filter_article(article) for article in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for article, result in zip(batch, results):
                if isinstance(result, Exception):
                    # On error, default to keep
                    decision = FilterDecision.KEEP
                    response = LLMFilterResponse(
                        decision="keep",
                        confidence=0.3,
                        reason=f"LLM 處理錯誤: {str(result)}，預設保留",
                    )
                else:
                    decision, response = result

                filter_result = ArticleFilterResult(
                    pipeline_run_id=pipeline_run_id,
                    article_id=article.id,
                    stage=PipelineStage.LLM_FILTER,
                    decision=decision,
                    confidence=response.confidence,
                    reason=response.reason,
                )
                filter_results.append(filter_result)

                if decision in (FilterDecision.KEEP, FilterDecision.FORCE_INCLUDE):
                    passed_articles.append(article)

            # Small delay between batches to avoid rate limiting
            if i + batch_size < len(articles):
                await asyncio.sleep(0.5)

        return passed_articles, filter_results

    @staticmethod
    def get_available_providers() -> list[str]:
        """Get list of available provider names."""
        return list(PROVIDERS.keys())
