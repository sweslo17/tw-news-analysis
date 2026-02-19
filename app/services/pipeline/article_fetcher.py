"""Article fetcher service for pipeline."""

from datetime import datetime, timedelta
from typing import Generator

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.models import NewsArticle, PipelineRun, ForceIncludeArticle


class ArticleFetcher:
    """Service for fetching articles from database."""

    def __init__(self, db: Session):
        self.db = db

    def fetch_articles_for_run(
        self,
        pipeline_run: PipelineRun,
        batch_size: int = 100,
        limit: int | None = None,
    ) -> Generator[list[NewsArticle], None, None]:
        """
        Fetch articles for a pipeline run in batches.

        Args:
            pipeline_run: The pipeline run with date range
            batch_size: Number of articles per batch
            limit: Maximum total articles to fetch (None = no limit)

        Yields:
            Batches of NewsArticle objects
        """
        query = select(NewsArticle)

        conditions = []

        if pipeline_run.date_from:
            conditions.append(NewsArticle.published_at >= pipeline_run.date_from)

        if pipeline_run.date_to:
            conditions.append(NewsArticle.published_at <= pipeline_run.date_to)

        if conditions:
            query = query.where(and_(*conditions))

        query = query.order_by(NewsArticle.published_at.desc())

        offset = 0
        remaining = limit
        while True:
            fetch_size = min(batch_size, remaining) if remaining is not None else batch_size
            batch_query = query.offset(offset).limit(fetch_size)
            articles = list(self.db.execute(batch_query).scalars().all())

            if not articles:
                break

            yield articles
            offset += len(articles)

            if remaining is not None:
                remaining -= len(articles)
                if remaining <= 0:
                    break

    def fetch_articles_by_days(
        self,
        days: int = 1,
        batch_size: int = 100,
    ) -> Generator[list[NewsArticle], None, None]:
        """
        Fetch articles from the last N days in batches.

        Args:
            days: Number of days to look back
            batch_size: Number of articles per batch

        Yields:
            Batches of NewsArticle objects
        """
        date_from = datetime.utcnow() - timedelta(days=days)

        query = (
            select(NewsArticle)
            .where(NewsArticle.published_at >= date_from)
            .order_by(NewsArticle.published_at.desc())
        )

        offset = 0
        while True:
            batch_query = query.offset(offset).limit(batch_size)
            articles = list(self.db.execute(batch_query).scalars().all())

            if not articles:
                break

            yield articles
            offset += batch_size

    def count_articles_for_run(self, pipeline_run: PipelineRun) -> int:
        """
        Count total articles for a pipeline run.

        Args:
            pipeline_run: The pipeline run with date range

        Returns:
            Total count of articles
        """
        from sqlalchemy import func

        query = select(func.count(NewsArticle.id))

        conditions = []

        if pipeline_run.date_from:
            conditions.append(NewsArticle.published_at >= pipeline_run.date_from)

        if pipeline_run.date_to:
            conditions.append(NewsArticle.published_at <= pipeline_run.date_to)

        if conditions:
            query = query.where(and_(*conditions))

        result = self.db.execute(query).scalar()
        return result or 0

    def count_articles_by_days(self, days: int = 1) -> int:
        """
        Count articles from the last N days.

        Args:
            days: Number of days to look back

        Returns:
            Total count of articles
        """
        from sqlalchemy import func

        date_from = datetime.utcnow() - timedelta(days=days)

        query = select(func.count(NewsArticle.id)).where(
            NewsArticle.published_at >= date_from
        )

        result = self.db.execute(query).scalar()
        return result or 0

    def get_article_by_id(self, article_id: int) -> NewsArticle | None:
        """
        Get a single article by ID.

        Args:
            article_id: Article ID

        Returns:
            NewsArticle or None
        """
        return self.db.get(NewsArticle, article_id)

    def get_force_include_article_ids(self) -> set[int]:
        """
        Get all article IDs that are force-included.

        Returns:
            Set of article IDs
        """
        query = select(ForceIncludeArticle.article_id)
        result = self.db.execute(query).scalars().all()
        return set(result)
