"""Pending URL queue management service."""

import hashlib
from datetime import datetime
from typing import Set

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.models import NewsArticle, PendingUrl, UrlStatus


def compute_url_hash(url: str) -> str:
    """
    Compute MD5 hash of URL for fast lookup.

    Args:
        url: The URL to hash.

    Returns:
        MD5 hash string (32 characters).
    """
    return hashlib.md5(url.encode("utf-8")).hexdigest()


class PendingUrlService:
    """
    Service for managing the pending URL queue.

    Handles URL deduplication, queue operations, and status updates.
    """

    def __init__(self, session: Session) -> None:
        """
        Initialize the service.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def add_urls(self, urls: list[str], source: str) -> int:
        """
        Add new URLs to the pending queue after deduplication.

        Checks both NewsArticle (already crawled) and PendingUrl (already queued)
        tables to avoid duplicates.

        Args:
            urls: List of URLs to potentially add.
            source: News source name.

        Returns:
            Number of new URLs added to queue.
        """
        if not urls:
            return 0

        # Compute hashes
        url_hash_map = {compute_url_hash(url): url for url in urls}
        hashes = list(url_hash_map.keys())

        # Check NewsArticle table (already crawled)
        stmt = select(NewsArticle.url_hash).where(NewsArticle.url_hash.in_(hashes))
        existing_article_hashes: Set[str] = {
            row[0] for row in self.session.execute(stmt)
        }

        # Check PendingUrl table (already queued)
        stmt = select(PendingUrl.url_hash).where(PendingUrl.url_hash.in_(hashes))
        existing_pending_hashes: Set[str] = {
            row[0] for row in self.session.execute(stmt)
        }

        # Filter out existing URLs
        existing_hashes = existing_article_hashes | existing_pending_hashes
        new_hashes = [h for h in hashes if h not in existing_hashes]

        # Add new URLs to queue
        added_count = 0
        for url_hash in new_hashes:
            url = url_hash_map[url_hash]
            pending_url = PendingUrl(
                url=url,
                url_hash=url_hash,
                source=source,
                status=UrlStatus.PENDING,
                discovered_at=datetime.utcnow(),
            )
            self.session.add(pending_url)
            added_count += 1

        if added_count > 0:
            self.session.commit()

        return added_count

    def get_pending_urls(self, source: str, limit: int = 0) -> list[PendingUrl]:
        """
        Get pending URLs for a specific source.

        Also marks them as PROCESSING to prevent duplicate processing.

        Args:
            source: News source name.
            limit: Maximum number of URLs to return. 0 means no limit.

        Returns:
            List of PendingUrl objects.
        """
        # Select pending URLs
        stmt = (
            select(PendingUrl)
            .where(
                PendingUrl.source == source,
                PendingUrl.status == UrlStatus.PENDING,
            )
            .order_by(PendingUrl.discovered_at)
        )
        if limit > 0:
            stmt = stmt.limit(limit)
        pending_urls = list(self.session.execute(stmt).scalars().all())

        # Mark as processing
        if pending_urls:
            url_ids = [u.id for u in pending_urls]
            stmt = (
                update(PendingUrl)
                .where(PendingUrl.id.in_(url_ids))
                .values(status=UrlStatus.PROCESSING, updated_at=datetime.utcnow())
            )
            self.session.execute(stmt)
            self.session.commit()

            # Refresh to get updated status
            for url in pending_urls:
                self.session.refresh(url)

        return pending_urls

    def mark_completed(self, url_id: int) -> None:
        """
        Mark a URL as completed.

        Args:
            url_id: The PendingUrl ID.
        """
        stmt = (
            update(PendingUrl)
            .where(PendingUrl.id == url_id)
            .values(
                status=UrlStatus.COMPLETED,
                processed_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        self.session.execute(stmt)
        self.session.commit()

    def mark_failed(self, url_id: int, error_message: str) -> None:
        """
        Mark a URL as failed and increment retry count.

        If max retries exceeded, keeps status as FAILED.
        Otherwise, resets to PENDING for retry.

        Args:
            url_id: The PendingUrl ID.
            error_message: Error message to store.
        """
        pending_url = self.session.get(PendingUrl, url_id)
        if not pending_url:
            return

        pending_url.retry_count += 1
        pending_url.error_message = error_message[:4096] if error_message else None
        pending_url.updated_at = datetime.utcnow()

        if pending_url.retry_count >= pending_url.max_retries:
            pending_url.status = UrlStatus.FAILED
            pending_url.processed_at = datetime.utcnow()
        else:
            # Reset to pending for retry
            pending_url.status = UrlStatus.PENDING

        self.session.commit()

    def reset_stale_processing(self, minutes: int = 30) -> int:
        """
        Reset URLs that have been in PROCESSING state for too long.

        This handles cases where a crawler crashed mid-processing.

        Args:
            minutes: Minutes after which to consider a URL stale.

        Returns:
            Number of URLs reset.
        """
        from datetime import timedelta

        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)

        stmt = (
            update(PendingUrl)
            .where(
                PendingUrl.status == UrlStatus.PROCESSING,
                PendingUrl.updated_at < cutoff_time,
            )
            .values(status=UrlStatus.PENDING, updated_at=datetime.utcnow())
        )
        result = self.session.execute(stmt)
        self.session.commit()

        return result.rowcount

    def force_reset_all_processing(self, source: str | None = None) -> int:
        """
        Force reset ALL URLs in PROCESSING state back to PENDING.

        Use this for manual intervention when URLs are stuck.

        Args:
            source: Optional source filter. If None, resets all sources.

        Returns:
            Number of URLs reset.
        """
        stmt = (
            update(PendingUrl)
            .where(PendingUrl.status == UrlStatus.PROCESSING)
            .values(status=UrlStatus.PENDING, updated_at=datetime.utcnow())
        )

        if source:
            stmt = stmt.where(PendingUrl.source == source)

        result = self.session.execute(stmt)
        self.session.commit()

        return result.rowcount

    def get_queue_stats(self, source: str | None = None) -> dict:
        """
        Get queue statistics.

        Args:
            source: Optional source filter.

        Returns:
            Dictionary with queue statistics.
        """
        base_query = select(PendingUrl)
        if source:
            base_query = base_query.where(PendingUrl.source == source)

        # Count by status
        stats = {}
        for status in UrlStatus:
            stmt = base_query.where(PendingUrl.status == status)
            count = len(list(self.session.execute(stmt).scalars().all()))
            stats[status.value] = count

        stats["total"] = sum(stats.values())
        return stats

    def url_exists_in_queue(self, url: str) -> bool:
        """
        Check if a URL exists in the pending queue.

        Args:
            url: The URL to check.

        Returns:
            True if URL exists in queue, False otherwise.
        """
        url_hash = compute_url_hash(url)
        stmt = select(PendingUrl.id).where(PendingUrl.url_hash == url_hash).limit(1)
        result = self.session.execute(stmt).first()
        return result is not None

    def url_exists_in_articles(self, url: str) -> bool:
        """
        Check if a URL exists in the articles table.

        Args:
            url: The URL to check.

        Returns:
            True if URL exists in articles, False otherwise.
        """
        url_hash = compute_url_hash(url)
        stmt = select(NewsArticle.id).where(NewsArticle.url_hash == url_hash).limit(1)
        result = self.session.execute(stmt).first()
        return result is not None
