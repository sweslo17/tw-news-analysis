"""Reparse service for re-processing articles with updated parsers."""

import asyncio
import logging
import threading
import uuid
from datetime import datetime

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import ArchiveStatus, NewsArticle, RawHtmlArchive, ReparseJob, ReparseJobStatus
from app.schemas import ReparseJobStatusSchema, ReparsePreview
from app.services.data_management_service import DataManagementService
from crawlers.registry import get_article_crawler_by_source

logger = logging.getLogger(__name__)

# Global dict to track running jobs and cancellation flags
_running_jobs: dict[str, threading.Event] = {}


class ReparseService:
    """Service for re-parsing articles."""

    def __init__(self, session: Session):
        self.session = session

    def get_reparse_preview(self, source: str) -> ReparsePreview:
        """
        Get preview of how many articles can be reparsed for a source.

        Returns counts of:
        - Articles with raw_html still in database
        - Articles with raw_html archived to files
        """
        # Count articles with raw_html in database
        in_db_count = (
            self.session.query(func.count(NewsArticle.id))
            .filter(
                NewsArticle.source == source,
                NewsArticle.raw_html.isnot(None),
                NewsArticle.raw_html != "",
            )
            .scalar()
        ) or 0

        # Count archived articles
        archived_count = (
            self.session.query(func.count(RawHtmlArchive.id))
            .filter(
                RawHtmlArchive.source == source,
                RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
            )
            .scalar()
        ) or 0

        return ReparsePreview(
            source=source,
            total_available=in_db_count + archived_count,
            in_db_count=in_db_count,
            archived_count=archived_count,
        )

    def start_reparse_job(self, source: str) -> ReparseJob:
        """
        Start a background reparse job for a source.

        Args:
            source: News source to reparse

        Returns:
            ReparseJob record
        """
        # Create job record
        job_id = str(uuid.uuid4())
        preview = self.get_reparse_preview(source)

        job = ReparseJob(
            id=job_id,
            source=source,
            status=ReparseJobStatus.PENDING,
            total_count=preview.total_available,
            processed_count=0,
            failed_count=0,
        )
        self.session.add(job)
        self.session.commit()

        # Create cancellation event
        cancel_event = threading.Event()
        _running_jobs[job_id] = cancel_event

        # Start background thread
        thread = threading.Thread(
            target=self._run_reparse_job_sync,
            args=(job_id, source, cancel_event),
            daemon=True,
        )
        thread.start()

        return job

    def _run_reparse_job_sync(
        self,
        job_id: str,
        source: str,
        cancel_event: threading.Event,
    ) -> None:
        """Synchronous wrapper to run the async reparse job."""
        try:
            asyncio.run(self._run_reparse_job(job_id, source, cancel_event))
        except Exception as e:
            logger.error(f"Reparse job {job_id} failed: {e}")
            self._update_job_status(job_id, ReparseJobStatus.FAILED, error=str(e))
        finally:
            # Clean up
            if job_id in _running_jobs:
                del _running_jobs[job_id]

    async def _run_reparse_job(
        self,
        job_id: str,
        source: str,
        cancel_event: threading.Event,
    ) -> None:
        """Run the reparse job asynchronously."""
        # Get the appropriate crawler
        crawler = get_article_crawler_by_source(source)
        if not crawler:
            self._update_job_status(
                job_id,
                ReparseJobStatus.FAILED,
                error=f"No crawler found for source: {source}",
            )
            return

        # Update job status to running
        self._update_job_status(job_id, ReparseJobStatus.RUNNING)

        # Create a new session for this thread
        session = SessionLocal()
        try:
            data_service = DataManagementService(session)

            # Get articles with raw_html in database
            articles_in_db = (
                session.query(NewsArticle)
                .filter(
                    NewsArticle.source == source,
                    NewsArticle.raw_html.isnot(None),
                    NewsArticle.raw_html != "",
                )
                .all()
            )

            # Get archived article IDs
            archived_records = (
                session.query(RawHtmlArchive)
                .filter(
                    RawHtmlArchive.source == source,
                    RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
                )
                .all()
            )

            processed = 0
            failed = 0
            errors = []

            # Process articles with raw_html in database
            for article in articles_in_db:
                if cancel_event.is_set():
                    self._update_job_status(job_id, ReparseJobStatus.CANCELLED)
                    return

                try:
                    parsed = crawler.parse_html(article.raw_html, article.url)

                    # Update article with parsed data
                    article.title = parsed.title
                    article.content = parsed.content
                    article.summary = parsed.summary
                    article.author = parsed.author
                    article.category = parsed.category
                    article.sub_category = parsed.sub_category
                    if parsed.tags:
                        article.tags = ",".join(parsed.tags)
                    article.published_at = parsed.published_at
                    if parsed.images:
                        import json
                        article.images = json.dumps(parsed.images)

                    processed += 1

                except Exception as e:
                    failed += 1
                    errors.append(f"Article {article.id}: {str(e)}")
                    logger.warning(f"Failed to reparse article {article.id}: {e}")

                # Update progress periodically
                if processed % 10 == 0:
                    session.commit()
                    self._update_job_progress(job_id, processed, failed)

            # Process archived articles
            for record in archived_records:
                if cancel_event.is_set():
                    self._update_job_status(job_id, ReparseJobStatus.CANCELLED)
                    return

                try:
                    # Get raw_html from archive
                    raw_html = data_service.get_raw_html_from_archive(record.article_id)
                    if not raw_html:
                        failed += 1
                        errors.append(f"Article {record.article_id}: Could not retrieve from archive")
                        continue

                    # Get article
                    article = (
                        session.query(NewsArticle)
                        .filter(NewsArticle.id == record.article_id)
                        .first()
                    )
                    if not article:
                        failed += 1
                        errors.append(f"Article {record.article_id}: Article not found in database")
                        continue

                    # Parse and update
                    parsed = crawler.parse_html(raw_html, article.url)

                    article.title = parsed.title
                    article.content = parsed.content
                    article.summary = parsed.summary
                    article.author = parsed.author
                    article.category = parsed.category
                    article.sub_category = parsed.sub_category
                    if parsed.tags:
                        article.tags = ",".join(parsed.tags)
                    article.published_at = parsed.published_at
                    if parsed.images:
                        import json
                        article.images = json.dumps(parsed.images)

                    processed += 1

                except Exception as e:
                    failed += 1
                    errors.append(f"Article {record.article_id}: {str(e)}")
                    logger.warning(f"Failed to reparse archived article {record.article_id}: {e}")

                # Update progress periodically
                if processed % 10 == 0:
                    session.commit()
                    self._update_job_progress(job_id, processed, failed)

            session.commit()

            # Update final status
            error_log = "\n".join(errors[:100]) if errors else None  # Limit error log size
            self._update_job_status(
                job_id,
                ReparseJobStatus.COMPLETED,
                processed=processed,
                failed=failed,
                error=error_log,
            )

        finally:
            session.close()

    def _update_job_status(
        self,
        job_id: str,
        status: ReparseJobStatus,
        processed: int | None = None,
        failed: int | None = None,
        error: str | None = None,
    ) -> None:
        """Update job status in a new session."""
        session = SessionLocal()
        try:
            job = session.query(ReparseJob).filter(ReparseJob.id == job_id).first()
            if job:
                job.status = status
                if processed is not None:
                    job.processed_count = processed
                if failed is not None:
                    job.failed_count = failed
                if error:
                    job.error_log = error

                if status == ReparseJobStatus.RUNNING:
                    job.started_at = datetime.utcnow()
                elif status in (
                    ReparseJobStatus.COMPLETED,
                    ReparseJobStatus.FAILED,
                    ReparseJobStatus.CANCELLED,
                ):
                    job.completed_at = datetime.utcnow()

                session.commit()
        finally:
            session.close()

    def _update_job_progress(
        self,
        job_id: str,
        processed: int,
        failed: int,
    ) -> None:
        """Update job progress counters."""
        session = SessionLocal()
        try:
            job = session.query(ReparseJob).filter(ReparseJob.id == job_id).first()
            if job:
                job.processed_count = processed
                job.failed_count = failed
                session.commit()
        finally:
            session.close()

    def get_job_status(self, job_id: str) -> ReparseJobStatusSchema | None:
        """Get the status of a reparse job."""
        job = self.session.query(ReparseJob).filter(ReparseJob.id == job_id).first()
        if not job:
            return None

        progress_percent = 0.0
        if job.total_count > 0:
            progress_percent = (job.processed_count + job.failed_count) / job.total_count * 100

        return ReparseJobStatusSchema(
            job_id=job.id,
            source=job.source,
            status=job.status.value,
            total_count=job.total_count,
            processed_count=job.processed_count,
            failed_count=job.failed_count,
            progress_percent=round(progress_percent, 1),
            error_log=job.error_log,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running reparse job."""
        if job_id in _running_jobs:
            _running_jobs[job_id].set()
            return True
        return False

    def get_recent_jobs(self, limit: int = 10) -> list[ReparseJobStatusSchema]:
        """Get recent reparse jobs."""
        jobs = (
            self.session.query(ReparseJob)
            .order_by(ReparseJob.created_at.desc())
            .limit(limit)
            .all()
        )

        results = []
        for job in jobs:
            progress_percent = 0.0
            if job.total_count > 0:
                progress_percent = (job.processed_count + job.failed_count) / job.total_count * 100

            results.append(
                ReparseJobStatusSchema(
                    job_id=job.id,
                    source=job.source,
                    status=job.status.value,
                    total_count=job.total_count,
                    processed_count=job.processed_count,
                    failed_count=job.failed_count,
                    progress_percent=round(progress_percent, 1),
                    error_log=job.error_log,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                )
            )

        return results
