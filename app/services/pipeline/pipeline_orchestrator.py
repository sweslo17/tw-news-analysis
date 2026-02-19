"""Pipeline orchestrator for coordinating filtering stages."""

import asyncio
from datetime import datetime, timedelta
from typing import Callable

from loguru import logger
from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    PipelineRun,
    PipelineRunStatus,
    PipelineStage,
    ForceIncludeArticle,
    NewsArticle,
)
from .article_fetcher import ArticleFetcher
from .rule_filter_service import RuleFilterService
from .llm_analysis_service import LLMAnalysisService
from .pipeline_run_store import PipelineRunStore
from .statistics_service import StatisticsService


class PipelineOrchestrator:
    """Orchestrates the multi-stage filtering pipeline."""

    def __init__(self, db: Session):
        self.db = db
        self._fetcher: ArticleFetcher | None = None
        self._rule_filter: RuleFilterService | None = None
        self._store: PipelineRunStore | None = None
        self._stats: StatisticsService | None = None

    @property
    def fetcher(self) -> ArticleFetcher:
        if self._fetcher is None:
            self._fetcher = ArticleFetcher(self.db)
        return self._fetcher

    @property
    def rule_filter(self) -> RuleFilterService:
        if self._rule_filter is None:
            self._rule_filter = RuleFilterService(self.db)
        return self._rule_filter

    @property
    def store(self) -> PipelineRunStore:
        if self._store is None:
            self._store = PipelineRunStore(self.db)
        return self._store

    @property
    def stats(self) -> StatisticsService:
        if self._stats is None:
            self._stats = StatisticsService(self.db)
        return self._stats

    def get_analysis_service(self) -> LLMAnalysisService:
        """Get LLM analysis service."""
        return LLMAnalysisService(self.db)

    def create_pipeline_run(
        self,
        name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> PipelineRun:
        """
        Create a new pipeline run.

        Args:
            name: Name for this pipeline run
            date_from: Start date for article fetching
            date_to: End date for article fetching

        Returns:
            Created PipelineRun
        """
        run = PipelineRun(
            name=name,
            status=PipelineRunStatus.PENDING,
            date_from=date_from,
            date_to=date_to,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def create_quick_run(self, days: int | None = None) -> PipelineRun:
        """
        Create a quick pipeline run for recent articles.

        Args:
            days: Number of days to look back (default from settings)

        Returns:
            Created PipelineRun
        """
        days = days or settings.pipeline_default_days
        date_from = datetime.utcnow() - timedelta(days=days)
        name = f"Quick run - last {days} day(s) - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        return self.create_pipeline_run(name=name, date_from=date_from)

    def get_pipeline_run(self, run_id: int) -> PipelineRun | None:
        """Get a pipeline run by ID."""
        return self.db.get(PipelineRun, run_id)

    async def run_pipeline(
        self,
        run_id: int,
        until_stage: PipelineStage | None = None,
        progress_callback: Callable[[str, int, int], None] | None = None,
        limit: int | None = None,
    ) -> PipelineRun:
        """
        Run the pipeline for a given run.

        Args:
            run_id: Pipeline run ID
            until_stage: Stop after this stage (default: run all stages)
            progress_callback: Optional callback for progress updates
                (stage_name, current, total)
            limit: Maximum number of articles to process (None = no limit)

        Returns:
            Updated PipelineRun
        """
        run = self.get_pipeline_run(run_id)
        if not run:
            raise ValueError(f"Pipeline run {run_id} not found")

        # Ensure default rules exist
        self.rule_filter.ensure_default_rules()

        try:
            # Update status to running
            self.store.update_pipeline_run_status(
                run, PipelineRunStatus.RUNNING, PipelineStage.FETCH
            )

            # Stage 1: FETCH
            if progress_callback:
                progress_callback("fetch", 0, 0)

            total_articles = self.fetcher.count_articles_for_run(run)
            if limit is not None:
                total_articles = min(total_articles, limit)
            run.total_articles = total_articles
            self.db.commit()

            if until_stage == PipelineStage.FETCH:
                self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                return run

            # Stage 2: RULE_FILTER
            self.store.update_pipeline_run_status(
                run, PipelineRunStatus.RUNNING, PipelineStage.RULE_FILTER
            )

            processed = 0
            all_passed_articles = []

            for batch in self.fetcher.fetch_articles_for_run(run, batch_size=100, limit=limit):
                passed, filter_results = self.rule_filter.filter_articles_batch(
                    batch, run.id
                )
                self.store.save_filter_results(filter_results)
                all_passed_articles.extend(passed)

                processed += len(batch)
                if progress_callback:
                    progress_callback("rule_filter", processed, total_articles)

            # Update stats after rule filter
            self.store.update_pipeline_run_stats(run)

            if until_stage == PipelineStage.RULE_FILTER:
                self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                return run

            # Stage 3: LLM_ANALYSIS
            if all_passed_articles:
                self.store.update_pipeline_run_status(
                    run, PipelineRunStatus.RUNNING, PipelineStage.LLM_ANALYSIS
                )

                analysis_service = self.get_analysis_service()

                try:
                    success_count, fail_count = await analysis_service.analyze_articles(
                        all_passed_articles, run, progress_callback=progress_callback
                    )
                    run.analyzed_count = success_count
                    self.db.commit()
                except TimeoutError:
                    logger.warning(f"Batch polling timed out for run {run.id}, pausing")
                    self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                    return run

            if until_stage == PipelineStage.LLM_ANALYSIS:
                self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                return run

            # Stage 4: STORE (finalize)
            self.store.update_pipeline_run_status(
                run, PipelineRunStatus.RUNNING, PipelineStage.STORE
            )
            self.store.update_pipeline_run_stats(run)
            self.store.update_pipeline_run_status(run, PipelineRunStatus.COMPLETED)

            return run

        except Exception as e:
            self.store.update_pipeline_run_status(
                run, PipelineRunStatus.FAILED, error_log=str(e)
            )
            raise

    async def run_quick_pipeline(
        self,
        days: int | None = None,
        until_stage: PipelineStage = PipelineStage.RULE_FILTER,
        progress_callback: Callable[[str, int, int], None] | None = None,
        limit: int | None = None,
    ) -> PipelineRun:
        """
        Create and run a quick pipeline.

        Args:
            days: Number of days to look back
            until_stage: Stop after this stage (default: RULE_FILTER)
            progress_callback: Optional progress callback
            limit: Maximum number of articles to process (None = no limit)

        Returns:
            Completed PipelineRun
        """
        run = self.create_quick_run(days)
        return await self.run_pipeline(
            run.id, until_stage=until_stage, progress_callback=progress_callback, limit=limit
        )

    async def run_quick_pipeline_with_range(
        self,
        date_from: datetime,
        date_to: datetime | None = None,
        name_suffix: str = "",
        until_stage: PipelineStage = PipelineStage.RULE_FILTER,
        progress_callback: Callable[[str, int, int], None] | None = None,
        limit: int | None = None,
    ) -> PipelineRun:
        """
        Create and run a pipeline with specific date range.

        Args:
            date_from: Start date for fetching articles
            date_to: End date for fetching articles (optional)
            name_suffix: Suffix for the run name
            until_stage: Stop after this stage (default: RULE_FILTER)
            progress_callback: Optional progress callback
            limit: Maximum number of articles to process (None = no limit)

        Returns:
            Completed PipelineRun
        """
        name = f"Quick run - {name_suffix} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        run = self.create_pipeline_run(name=name, date_from=date_from, date_to=date_to)
        return await self.run_pipeline(
            run.id, until_stage=until_stage, progress_callback=progress_callback, limit=limit
        )

    def reset_pipeline_run(
        self, run_id: int, from_stage: PipelineStage
    ) -> PipelineRun:
        """
        Reset a pipeline run to re-execute from a specific stage.

        Args:
            run_id: Pipeline run ID
            from_stage: Stage to reset from

        Returns:
            Reset PipelineRun
        """
        run = self.get_pipeline_run(run_id)
        if not run:
            raise ValueError(f"Pipeline run {run_id} not found")

        # Delete results from the specified stage onwards
        stages_to_delete = []
        stage_order = [
            PipelineStage.RULE_FILTER,
            PipelineStage.LLM_ANALYSIS,
            PipelineStage.STORE,
        ]

        found = False
        for stage in stage_order:
            if stage == from_stage:
                found = True
            if found:
                stages_to_delete.append(stage)

        for stage in stages_to_delete:
            self.store.delete_filter_results_from_stage(run.id, stage, commit=False)

        if PipelineStage.LLM_ANALYSIS in stages_to_delete:
            self.store.delete_analysis_results(run.id, commit=False)

        # Reset statistics
        if PipelineStage.RULE_FILTER in stages_to_delete:
            run.rule_filtered_count = 0
            run.rule_passed_count = 0
        if PipelineStage.LLM_ANALYSIS in stages_to_delete:
            run.analyzed_count = 0

        # Reset status
        run.status = PipelineRunStatus.PENDING
        run.current_stage = None
        run.completed_at = None
        run.error_log = None

        self.db.commit()
        return run

    def add_force_include(
        self, article_id: int, reason: str, added_by: str | None = None
    ) -> ForceIncludeArticle:
        """
        Add an article to the force-include list.

        Args:
            article_id: Article ID to force include
            reason: Reason for force including
            added_by: User who added this entry

        Returns:
            Created ForceIncludeArticle
        """
        # Check if article exists
        article = self.db.get(NewsArticle, article_id)
        if not article:
            raise ValueError(f"Article {article_id} not found")

        # Check if already force-included
        existing = (
            self.db.query(ForceIncludeArticle)
            .filter(ForceIncludeArticle.article_id == article_id)
            .first()
        )
        if existing:
            raise ValueError(f"Article {article_id} is already force-included")

        force_include = ForceIncludeArticle(
            article_id=article_id, reason=reason, added_by=added_by
        )
        self.db.add(force_include)
        self.db.commit()
        self.db.refresh(force_include)
        return force_include

    def remove_force_include(self, article_id: int) -> bool:
        """
        Remove an article from the force-include list.

        Args:
            article_id: Article ID to remove

        Returns:
            True if removed, False if not found
        """
        deleted = (
            self.db.query(ForceIncludeArticle)
            .filter(ForceIncludeArticle.article_id == article_id)
            .delete()
        )
        self.db.commit()
        return deleted > 0

    def list_force_includes(self) -> list[dict]:
        """
        List all force-included articles.

        Returns:
            List of force-include entries with article info
        """
        results = (
            self.db.query(ForceIncludeArticle, NewsArticle)
            .join(NewsArticle, ForceIncludeArticle.article_id == NewsArticle.id)
            .all()
        )

        return [
            {
                "id": fi.id,
                "article_id": fi.article_id,
                "title": article.title,
                "source": article.source,
                "reason": fi.reason,
                "added_by": fi.added_by,
                "created_at": fi.created_at.isoformat(),
            }
            for fi, article in results
        ]
