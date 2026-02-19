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
    parse_article_id,
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
        self._result_store = None

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
        result = {"pending": 0, "success": 0, "failed": 0, "store_failed": 0, "total": 0}
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
            article_id = parse_article_id(resp.custom_id)
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
    ) -> tuple[int, int]:
        """Clear tracking records and associated TimescaleDB data.

        Returns (tracking_records_deleted, timescaledb_articles_deleted).
        """
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
            return 0, 0

        # Clean TimescaleDB for SUCCESS records (failed ones were never stored)
        ts_deleted = 0
        if not failed_only:
            ts_deleted = self._clear_timescaledb(query)

        count = query.count()
        query.delete(synchronize_session=False)
        self.db.commit()
        logger.info(f"Cleared {count} tracking records, {ts_deleted} TimescaleDB articles")
        return count, ts_deleted

    def _clear_timescaledb(self, tracking_query) -> int:
        """Delete corresponding articles from TimescaleDB for SUCCESS records."""
        if not settings.timescale_url:
            return 0

        # Get article_ids with SUCCESS status from the query scope
        success_rows = (
            tracking_query
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.SUCCESS)
            .with_entities(ArticleAnalysisTracking.article_id)
            .all()
        )
        if not success_rows:
            return 0

        article_ids = [r[0] for r in success_rows]

        # Look up url_hash (= external_id in TimescaleDB) for these articles
        articles = (
            self.db.query(NewsArticle.url_hash)
            .filter(NewsArticle.id.in_(article_ids))
            .all()
        )
        external_ids = [a[0] for a in articles if a[0]]
        if not external_ids:
            return 0

        try:
            if self._result_store is None:
                from .analysis.timescale_store import TimescaleStore
                self._result_store = TimescaleStore()

            return self._result_store.delete_by_external_ids(external_ids)
        except Exception as e:
            logger.warning(f"TimescaleDB cleanup failed (non-fatal): {e}")
            return 0

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

        # Store results to TimescaleDB
        successful_responses = [r for r in responses if r.success]
        articles_map = {a.id: a for a in to_analyze}
        self.store_results(successful_responses, articles_map)

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

        # Store results to TimescaleDB
        successful_responses = [r for r in responses if r.success]
        articles_map = {a.id: a for a in articles}
        self.store_results(successful_responses, articles_map)

        return batch_id, len(articles)

    def retry_store_failed(self) -> tuple[int, int]:
        """Retry TimescaleDB storage for STORE_FAILED articles (no LLM re-analysis).

        Returns:
            (success_count, still_failed_count)
        """
        records = (
            self.db.query(ArticleAnalysisTracking)
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.STORE_FAILED)
            .all()
        )

        if not records:
            logger.info("No STORE_FAILED articles to retry")
            return 0, 0

        if not settings.timescale_url:
            logger.warning("TIMESCALE_URL not configured, cannot retry storage")
            return 0, len(records)

        # Load articles for these tracking records
        article_ids = [r.article_id for r in records]
        articles = (
            self.db.query(NewsArticle)
            .filter(NewsArticle.id.in_(article_ids))
            .all()
        )
        articles_map = {a.id: a for a in articles}

        # Build AnalysisResponse-like objects from saved result_json
        from .analysis.base_provider import AnalysisResponse as AR

        responses = []
        for r in records:
            if not r.result_json:
                logger.warning(
                    f"Article {r.article_id} STORE_FAILED but no result_json saved, "
                    "reverting to FAILED"
                )
                r.status = AnalysisStatus.FAILED
                r.error_message = "No result_json saved for storage retry"
                continue
            responses.append(
                AR(
                    custom_id=f"article_{r.article_id}",
                    success=True,
                    result_json=r.result_json,
                )
            )

        if not responses:
            self.db.commit()
            return 0, 0

        # Build response_json_map for _mark_storage_failures
        response_json_map = {
            parse_article_id(r.custom_id): r.result_json
            for r in responses
            if r.result_json
        }

        # Reset to SUCCESS before store attempt (so _mark_storage_failures can find them)
        for r in records:
            if r.status == AnalysisStatus.STORE_FAILED and r.result_json:
                r.status = AnalysisStatus.SUCCESS
        self.db.commit()

        # Attempt storage
        try:
            if self._result_store is None:
                from .analysis.timescale_store import TimescaleStore
                self._result_store = TimescaleStore()

            stored, failures = self._result_store.store_batch(articles_map, responses)

            if failures:
                self._mark_storage_failures(failures, response_json_map)

            # Clear result_json for successfully stored articles
            for r in records:
                if r.status == AnalysisStatus.SUCCESS:
                    r.result_json = None
            self.db.commit()

            still_failed = len(failures)
            logger.info(
                f"Storage retry: {stored} stored, {still_failed} still failed"
            )
            return stored, still_failed

        except Exception as e:
            logger.error(f"Storage retry failed: {e}")
            from .analysis.timescale_store import StoreFailure

            all_transient = [
                StoreFailure(aid, f"TimescaleDB connection error: {e}", True)
                for aid in response_json_map
            ]
            self._mark_storage_failures(all_transient, response_json_map)
            return 0, len(responses)

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

    def store_results(
        self,
        responses: list[AnalysisResponse],
        articles_map: dict[int, NewsArticle] | None = None,
    ) -> None:
        """Store successful analysis results to TimescaleDB.

        Gracefully degrades: if timescale_url is not configured or storage
        fails, the pipeline continues without interruption.
        """
        if not responses:
            return

        if not settings.timescale_url:
            logger.info(
                f"{len(responses)} analysis results ready "
                "(TIMESCALE_URL not configured, skipping storage)"
            )
            return

        if articles_map is None:
            logger.warning("articles_map not provided, cannot store results")
            return

        # Build {article_id: result_json} lookup for saving on transient failures
        response_json_map: dict[int, str] = {}
        for r in responses:
            aid = parse_article_id(r.custom_id)
            if aid is not None and r.result_json:
                response_json_map[aid] = r.result_json

        try:
            if self._result_store is None:
                from .analysis.timescale_store import TimescaleStore

                self._result_store = TimescaleStore()

            stored, failures = self._result_store.store_batch(articles_map, responses)
            logger.info(
                f"TimescaleDB: {stored} stored, {len(failures)} failed "
                f"out of {len(responses)} responses"
            )

            if failures:
                self._mark_storage_failures(failures, response_json_map)

        except Exception as e:
            # Entire storage call failed (e.g. connection) — all are transient
            logger.error(f"TimescaleDB storage failed: {e}")
            from .analysis.timescale_store import StoreFailure

            all_transient = [
                StoreFailure(aid, f"TimescaleDB connection error: {e}", True)
                for aid in response_json_map
            ]
            self._mark_storage_failures(all_transient, response_json_map)

    def _mark_storage_failures(
        self,
        failures: list,
        response_json_map: dict[int, str],
    ) -> None:
        """Revert SUCCESS tracking records based on failure type.

        - is_transient=True  → STORE_FAILED + save result_json (retry storage only)
        - is_transient=False → FAILED (needs LLM re-analysis)
        """
        for failure in failures:
            tracking = (
                self.db.query(ArticleAnalysisTracking)
                .filter(
                    ArticleAnalysisTracking.article_id == failure.article_id,
                    ArticleAnalysisTracking.status == AnalysisStatus.SUCCESS,
                )
                .order_by(ArticleAnalysisTracking.created_at.desc())
                .first()
            )
            if not tracking:
                continue

            if failure.is_transient:
                tracking.status = AnalysisStatus.STORE_FAILED
                tracking.result_json = response_json_map.get(failure.article_id)
                logger.warning(
                    f"Article {failure.article_id} → STORE_FAILED: {failure.error_message}"
                )
            else:
                tracking.status = AnalysisStatus.FAILED
                logger.warning(
                    f"Article {failure.article_id} → FAILED: {failure.error_message}"
                )
            tracking.error_message = failure.error_message

        self.db.commit()
