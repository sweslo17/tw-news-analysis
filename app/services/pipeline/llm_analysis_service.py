"""LLM analysis service for the pipeline."""

import asyncio

from loguru import logger
from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    NewsArticle,
    PipelineRun,
    ArticleAnalysisTracking,
    AnalysisStatus,
)
from .analysis.base_provider import (
    BaseAnalysisProvider,
    AnalysisRequest,
    AnalysisResponse,
    BatchStatus,
)
from .analysis.openai_batch_provider import OpenAIBatchProvider


class LLMAnalysisService:
    """Orchestrates LLM-based article analysis with batch processing."""

    def __init__(
        self,
        db: Session,
        provider: BaseAnalysisProvider | None = None,
    ):
        self.db = db
        self._provider = provider

    @property
    def provider(self) -> BaseAnalysisProvider:
        if self._provider is None:
            self._provider = OpenAIBatchProvider()
        return self._provider

    # ── Tracking queries ─────────────────────────────────────

    def get_analyzed_article_ids(self) -> set[int]:
        """Get article IDs that have been successfully analyzed."""
        rows = (
            self.db.query(ArticleAnalysisTracking.article_id)
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.SUCCESS)
            .all()
        )
        return {r[0] for r in rows}

    def get_failed_article_ids(self) -> set[int]:
        """Get article IDs that failed analysis."""
        rows = (
            self.db.query(ArticleAnalysisTracking.article_id)
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.FAILED)
            .all()
        )
        return {r[0] for r in rows}

    def get_tracking_stats(self) -> dict:
        """Get analysis tracking statistics."""
        from sqlalchemy import func

        stats = (
            self.db.query(
                ArticleAnalysisTracking.status,
                func.count(ArticleAnalysisTracking.id),
            )
            .group_by(ArticleAnalysisTracking.status)
            .all()
        )
        result = {"pending": 0, "success": 0, "failed": 0, "total": 0}
        for status, count in stats:
            result[status.value] = count
            result["total"] += count
        return result

    # ── Tracking mutations ───────────────────────────────────

    def _create_tracking_records(
        self, article_ids: list[int], batch_id: str
    ) -> None:
        """Create pending tracking records for a batch."""
        for article_id in article_ids:
            record = ArticleAnalysisTracking(
                article_id=article_id,
                batch_id=batch_id,
                status=AnalysisStatus.PENDING,
            )
            self.db.add(record)
        self.db.commit()
        logger.info(
            f"Created {len(article_ids)} tracking records for batch {batch_id}"
        )

    def _update_tracking_from_responses(
        self, responses: list[AnalysisResponse]
    ) -> tuple[int, int]:
        """Update tracking records from batch responses. Returns (success, failed) counts."""
        success_count = 0
        fail_count = 0

        for resp in responses:
            article_id = self._parse_article_id(resp.custom_id)
            if article_id is None:
                logger.warning(f"Cannot parse article_id from custom_id: {resp.custom_id}")
                fail_count += 1
                continue

            tracking = (
                self.db.query(ArticleAnalysisTracking)
                .filter(
                    ArticleAnalysisTracking.article_id == article_id,
                    ArticleAnalysisTracking.status == AnalysisStatus.PENDING,
                )
                .order_by(ArticleAnalysisTracking.created_at.desc())
                .first()
            )

            if not tracking:
                logger.warning(f"No pending tracking for article {article_id}")
                continue

            if resp.success:
                tracking.status = AnalysisStatus.SUCCESS
                success_count += 1
            else:
                tracking.status = AnalysisStatus.FAILED
                tracking.error_message = resp.error_message
                fail_count += 1
                logger.warning(
                    f"Article {article_id} analysis failed: {resp.error_message}"
                )

        self.db.commit()
        return success_count, fail_count

    def clear_tracking(
        self,
        *,
        all_records: bool = False,
        failed_only: bool = False,
        article_id: int | None = None,
        batch_id: str | None = None,
    ) -> int:
        """Clear tracking records. Returns number of deleted records."""
        query = self.db.query(ArticleAnalysisTracking)

        if all_records:
            pass  # no filter
        elif failed_only:
            query = query.filter(
                ArticleAnalysisTracking.status == AnalysisStatus.FAILED
            )
        elif article_id is not None:
            query = query.filter(
                ArticleAnalysisTracking.article_id == article_id
            )
        elif batch_id is not None:
            query = query.filter(
                ArticleAnalysisTracking.batch_id == batch_id
            )
        else:
            return 0

        count = query.count()
        query.delete(synchronize_session=False)
        self.db.commit()
        logger.info(f"Cleared {count} tracking records")
        return count

    # ── Core analysis flow ───────────────────────────────────

    async def analyze_articles(
        self,
        articles: list[NewsArticle],
        pipeline_run: PipelineRun,
        progress_callback=None,
    ) -> tuple[int, int]:
        """Analyze articles via batch API. Returns (success_count, fail_count).

        Handles:
        - Skipping already-analyzed articles
        - Submitting batch
        - Polling until completion
        - Updating tracking records
        - Resuming from existing batch_id
        """
        # Filter out already analyzed
        analyzed_ids = self.get_analyzed_article_ids()
        to_analyze = [a for a in articles if a.id not in analyzed_ids]

        if not to_analyze:
            logger.info("All articles already analyzed, skipping")
            return 0, 0

        logger.info(
            f"Analyzing {len(to_analyze)} articles "
            f"(skipped {len(articles) - len(to_analyze)} already analyzed)"
        )

        # Check for existing batch (resume)
        batch_id = pipeline_run.batch_id

        if batch_id:
            logger.info(f"Resuming existing batch: {batch_id}")
        else:
            # Submit new batch
            requests = [
                AnalysisRequest(
                    custom_id=f"article_{a.id}",
                    article=a,
                )
                for a in to_analyze
            ]

            batch_id = await self.provider.submit_batch(requests)

            # Persist batch_id for resume
            pipeline_run.batch_id = batch_id
            self.db.commit()

            # Create tracking records
            self._create_tracking_records(
                [a.id for a in to_analyze], batch_id
            )

        # Poll until completion
        responses = await self._poll_batch(
            batch_id, progress_callback=progress_callback
        )

        # Update tracking
        success_count, fail_count = self._update_tracking_from_responses(
            responses
        )

        # Store results (placeholder for future DB integration)
        successful_responses = [r for r in responses if r.success]
        self.store_results(successful_responses)

        logger.info(
            f"Analysis complete: {success_count} success, {fail_count} failed"
        )
        return success_count, fail_count

    async def retry_failed(self, progress_callback=None) -> tuple[str, int]:
        """Re-submit failed articles as a new batch.

        Returns:
            Tuple of (batch_id, article_count)
        """
        failed_ids = self.get_failed_article_ids()
        if not failed_ids:
            logger.info("No failed articles to retry")
            return "", 0

        # Load articles
        articles = (
            self.db.query(NewsArticle)
            .filter(NewsArticle.id.in_(failed_ids))
            .all()
        )

        if not articles:
            return "", 0

        # Clear old failed records
        self.clear_tracking(failed_only=True)

        # Submit new batch
        requests = [
            AnalysisRequest(custom_id=f"article_{a.id}", article=a)
            for a in articles
        ]

        batch_id = await self.provider.submit_batch(requests)
        self._create_tracking_records([a.id for a in articles], batch_id)

        # Poll
        responses = await self._poll_batch(
            batch_id, progress_callback=progress_callback
        )
        self._update_tracking_from_responses(responses)

        return batch_id, len(articles)

    # ── Polling ──────────────────────────────────────────────

    async def _poll_batch(
        self,
        batch_id: str,
        progress_callback=None,
    ) -> list[AnalysisResponse]:
        """Poll batch until completion or timeout."""
        poll_interval = settings.llm_analysis_poll_interval
        max_wait = settings.llm_analysis_max_wait
        elapsed = 0

        while elapsed < max_wait:
            status_result = await self.provider.check_batch_status(batch_id)
            logger.debug(
                f"Batch {batch_id}: {status_result.status.value} "
                f"({status_result.completed}/{status_result.total})"
            )

            if progress_callback:
                progress_callback(
                    "llm_analysis",
                    status_result.completed + status_result.failed,
                    status_result.total,
                )

            if status_result.status == BatchStatus.COMPLETED:
                return await self.provider.retrieve_results(batch_id)

            if status_result.status in (
                BatchStatus.FAILED,
                BatchStatus.EXPIRED,
                BatchStatus.CANCELLED,
            ):
                raise RuntimeError(
                    f"Batch {batch_id} {status_result.status.value}"
                )

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Batch {batch_id} did not complete within {max_wait}s"
        )

    # ── Storage hook ────────────────────────────────────────────

    def store_results(self, responses: list[AnalysisResponse]) -> None:
        """Store successful analysis results.

        Placeholder for future integration with a separate results DB.
        Override or extend this method when the storage layer is ready.
        """
        if responses:
            logger.info(
                f"{len(responses)} analysis results ready for storage (not persisted yet)"
            )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse_article_id(custom_id: str) -> int | None:
        """Extract article_id from custom_id like 'article_123'."""
        try:
            return int(custom_id.split("_", 1)[1])
        except (IndexError, ValueError):
            return None
