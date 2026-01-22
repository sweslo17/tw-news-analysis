"""URL deduplication service for news articles."""

import hashlib
from typing import Set

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import NewsArticle


def compute_url_hash(url: str) -> str:
    """
    Compute MD5 hash of URL for fast lookup.

    Args:
        url: The URL to hash.

    Returns:
        MD5 hash string (32 characters).
    """
    return hashlib.md5(url.encode("utf-8")).hexdigest()


class DeduplicationService:
    """
    Service for URL deduplication of news articles.

    Uses MD5 hash of URLs for fast database lookups to determine
    which articles have already been crawled.
    """

    def __init__(self, session: Session) -> None:
        """
        Initialize the deduplication service.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def url_exists(self, url: str) -> bool:
        """
        Check if a URL already exists in the database.

        Uses url_hash for fast indexed lookup.

        Args:
            url: The URL to check.

        Returns:
            True if URL exists, False otherwise.
        """
        url_hash = compute_url_hash(url)
        stmt = select(NewsArticle.id).where(NewsArticle.url_hash == url_hash).limit(1)
        result = self.session.execute(stmt).first()
        return result is not None

    def filter_new_urls(self, urls: list[str]) -> list[str]:
        """
        Filter out URLs that already exist in the database.

        Efficiently checks multiple URLs in a single query.

        Args:
            urls: List of URLs to check.

        Returns:
            List of URLs that don't exist in the database.
        """
        if not urls:
            return []

        # Compute hashes for all URLs
        url_hash_map = {compute_url_hash(url): url for url in urls}
        hashes = list(url_hash_map.keys())

        # Query existing hashes
        stmt = select(NewsArticle.url_hash).where(NewsArticle.url_hash.in_(hashes))
        existing_hashes: Set[str] = {row[0] for row in self.session.execute(stmt)}

        # Return URLs whose hashes don't exist
        return [
            url_hash_map[h] for h in hashes if h not in existing_hashes
        ]

    def get_existing_urls(self, urls: list[str]) -> list[str]:
        """
        Get URLs that already exist in the database.

        Args:
            urls: List of URLs to check.

        Returns:
            List of URLs that exist in the database.
        """
        if not urls:
            return []

        url_hash_map = {compute_url_hash(url): url for url in urls}
        hashes = list(url_hash_map.keys())

        stmt = select(NewsArticle.url_hash).where(NewsArticle.url_hash.in_(hashes))
        existing_hashes: Set[str] = {row[0] for row in self.session.execute(stmt)}

        return [url_hash_map[h] for h in hashes if h in existing_hashes]

    def count_by_crawler(self, crawler_name: str) -> int:
        """
        Count articles crawled by a specific crawler.

        Args:
            crawler_name: The crawler name to count articles for.

        Returns:
            Number of articles crawled by this crawler.
        """
        stmt = (
            select(NewsArticle.id)
            .where(NewsArticle.crawler_name == crawler_name)
        )
        result = self.session.execute(stmt)
        return len(result.all())
