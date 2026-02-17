"""Statistics service for pipeline."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.models import (
    PipelineRun,
    ArticleFilterResult,
    ArticleAnalysisResult,
    PipelineStage,
    FilterDecision,
    FilterRule,
    NewsArticle,
)


@dataclass
class PipelineRunStats:
    """Statistics for a pipeline run."""

    run_id: int
    name: str
    status: str
    total_articles: int
    rule_filtered_count: int
    rule_passed_count: int
    analyzed_count: int
    force_included_count: int
    rule_filter_rate: float  # Percentage filtered by rules
    started_at: datetime | None
    completed_at: datetime | None
    duration_seconds: float | None


@dataclass
class RuleStats:
    """Statistics for a filter rule."""

    rule_name: str
    description: str
    rule_type: str
    is_active: bool
    total_filtered_count: int


@dataclass
class OverallStats:
    """Overall pipeline statistics."""

    total_runs: int
    completed_runs: int
    total_articles_processed: int
    total_rule_filtered: int
    total_analyzed: int
    avg_rule_filter_rate: float


class StatisticsService:
    """Service for pipeline statistics and reporting."""

    def __init__(self, db: Session):
        self.db = db

    def get_pipeline_run_stats(self, run_id: int) -> PipelineRunStats | None:
        """
        Get detailed statistics for a pipeline run.

        Args:
            run_id: Pipeline run ID

        Returns:
            PipelineRunStats or None if not found
        """
        run = self.db.get(PipelineRun, run_id)
        if not run:
            return None

        # Calculate rates
        rule_filter_rate = 0.0
        if run.total_articles > 0:
            rule_filter_rate = (run.rule_filtered_count / run.total_articles) * 100

        # Calculate duration
        duration = None
        if run.started_at and run.completed_at:
            duration = (run.completed_at - run.started_at).total_seconds()

        return PipelineRunStats(
            run_id=run.id,
            name=run.name,
            status=run.status.value,
            total_articles=run.total_articles,
            rule_filtered_count=run.rule_filtered_count,
            rule_passed_count=run.rule_passed_count,
            analyzed_count=run.analyzed_count,
            force_included_count=run.force_included_count,
            rule_filter_rate=round(rule_filter_rate, 2),
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=round(duration, 2) if duration else None,
        )

    def get_rule_stats(self) -> list[RuleStats]:
        """
        Get statistics for all filter rules.

        Returns:
            List of RuleStats
        """
        rules = self.db.query(FilterRule).all()
        return [
            RuleStats(
                rule_name=rule.name,
                description=rule.description or "",
                rule_type=rule.rule_type.value,
                is_active=rule.is_active,
                total_filtered_count=rule.total_filtered_count,
            )
            for rule in rules
        ]

    def get_overall_stats(self) -> OverallStats:
        """
        Get overall pipeline statistics across all runs.

        Returns:
            OverallStats
        """
        from app.models import PipelineRunStatus

        # Count runs
        total_runs = self.db.query(func.count(PipelineRun.id)).scalar() or 0
        completed_runs = (
            self.db.query(func.count(PipelineRun.id))
            .filter(PipelineRun.status == PipelineRunStatus.COMPLETED)
            .scalar()
            or 0
        )

        # Sum statistics
        stats = self.db.query(
            func.sum(PipelineRun.total_articles),
            func.sum(PipelineRun.rule_filtered_count),
            func.sum(PipelineRun.analyzed_count),
        ).first()

        total_articles = stats[0] or 0
        total_rule_filtered = stats[1] or 0
        total_analyzed = stats[2] or 0

        # Calculate average rates
        avg_rule_filter_rate = 0.0

        if total_articles > 0:
            avg_rule_filter_rate = (total_rule_filtered / total_articles) * 100

        return OverallStats(
            total_runs=total_runs,
            completed_runs=completed_runs,
            total_articles_processed=total_articles,
            total_rule_filtered=total_rule_filtered,
            total_analyzed=total_analyzed,
            avg_rule_filter_rate=round(avg_rule_filter_rate, 2),
        )

    def get_filtered_articles(
        self,
        run_id: int,
        stage: PipelineStage | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Get articles that were filtered in a pipeline run.

        Args:
            run_id: Pipeline run ID
            stage: Optional stage to filter by
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of article info dictionaries
        """
        query = (
            self.db.query(ArticleFilterResult, NewsArticle)
            .join(NewsArticle, ArticleFilterResult.article_id == NewsArticle.id)
            .filter(
                ArticleFilterResult.pipeline_run_id == run_id,
                ArticleFilterResult.decision == FilterDecision.FILTER,
            )
        )

        if stage:
            query = query.filter(ArticleFilterResult.stage == stage)

        results = query.offset(offset).limit(limit).all()

        return [
            {
                "article_id": fr.article_id,
                "title": article.title,
                "source": article.source,
                "category": article.category,
                "stage": fr.stage.value,
                "rule_name": fr.rule_name,
                "reason": fr.reason,
                "confidence": fr.confidence,
            }
            for fr, article in results
        ]

    def get_passed_articles(
        self,
        run_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Get articles that passed all filters in a pipeline run.

        Args:
            run_id: Pipeline run ID
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of article info dictionaries
        """
        # Get articles that passed all filter stages
        # An article passes if its last filter result is KEEP or FORCE_INCLUDE

        subquery = (
            self.db.query(
                ArticleFilterResult.article_id,
                func.max(ArticleFilterResult.id).label("max_id"),
            )
            .filter(ArticleFilterResult.pipeline_run_id == run_id)
            .group_by(ArticleFilterResult.article_id)
            .subquery()
        )

        query = (
            self.db.query(ArticleFilterResult, NewsArticle)
            .join(
                subquery,
                and_(
                    ArticleFilterResult.article_id == subquery.c.article_id,
                    ArticleFilterResult.id == subquery.c.max_id,
                ),
            )
            .join(NewsArticle, ArticleFilterResult.article_id == NewsArticle.id)
            .filter(
                ArticleFilterResult.decision.in_(
                    [FilterDecision.KEEP, FilterDecision.FORCE_INCLUDE]
                )
            )
        )

        results = query.offset(offset).limit(limit).all()

        return [
            {
                "article_id": fr.article_id,
                "title": article.title,
                "source": article.source,
                "category": article.category,
                "published_at": article.published_at.isoformat() if article.published_at else None,
                "decision": fr.decision.value,
            }
            for fr, article in results
        ]

    def get_recent_runs(self, limit: int = 10) -> list[dict]:
        """
        Get recent pipeline runs.

        Args:
            limit: Maximum number of results

        Returns:
            List of run info dictionaries
        """
        runs = (
            self.db.query(PipelineRun)
            .order_by(PipelineRun.created_at.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "id": run.id,
                "name": run.name,
                "status": run.status.value,
                "current_stage": run.current_stage.value if run.current_stage else None,
                "total_articles": run.total_articles,
                "rule_filtered": run.rule_filtered_count,
                "created_at": run.created_at.isoformat(),
            }
            for run in runs
        ]
