"""Base crawler abstract classes and result dataclasses."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class CrawlerType(str, Enum):
    """Type of crawler - must match app.models.CrawlerType."""

    LIST = "list"
    ARTICLE = "article"


@dataclass
class CrawlerResult:
    """Result of a crawler execution."""

    success: bool
    message: str
    data: Any = None
    error: str | None = None
    items_processed: int = 0
    new_items: int = 0
    execution_time_seconds: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ArticleData:
    """Data structure for a news article before saving to database."""

    url: str
    title: str
    content: str
    summary: str | None = None
    author: str | None = None
    category: str | None = None
    sub_category: str | None = None
    tags: list[str] | None = None
    published_at: datetime | None = None
    raw_html: str | None = None
    images: list[str] | None = None


class BaseCrawler(ABC):
    """
    Abstract base class for all crawlers.

    Provides common properties and hooks for both list and article crawlers.

    SOLID Principles:
    - Single Responsibility: Only defines common crawling contract
    - Open/Closed: Extend via inheritance, don't modify base
    - Liskov Substitution: All subclasses can replace BaseCrawler
    - Interface Segregation: Minimal abstract methods required
    - Dependency Inversion: Services depend on BaseCrawler abstraction
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Unique identifier for this crawler.
        Used as job ID in scheduler and for database tracking.
        """
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name for dashboard display."""
        pass

    @property
    @abstractmethod
    def source(self) -> str:
        """
        News source name.
        e.g., "ETtoday", "UDN", "Google News"
        """
        pass

    @property
    @abstractmethod
    def crawler_type(self) -> CrawlerType:
        """Type of crawler: LIST or ARTICLE."""
        pass

    @property
    def default_interval_minutes(self) -> int:
        """Default scheduling interval in minutes. Override in subclass."""
        return 60

    @property
    def default_timeout_seconds(self) -> int:
        """Default execution timeout in seconds. Override in subclass."""
        return 300

    @abstractmethod
    async def run(self) -> CrawlerResult:
        """
        Execute the crawler.

        Returns:
            CrawlerResult with execution details.
        """
        pass

    async def on_success(self, result: CrawlerResult) -> None:
        """
        Hook called after successful execution.
        Override for custom post-success logic.
        """
        pass

    async def on_failure(self, result: CrawlerResult) -> None:
        """
        Hook called after failed execution.
        Override for custom post-failure logic.
        """
        pass


class BaseListCrawler(BaseCrawler):
    """
    Abstract base class for list crawlers.

    List crawlers are responsible for:
    1. Fetching article listing pages (RSS, sitemap, index pages)
    2. Extracting article URLs
    3. Adding new URLs to the pending queue

    The actual article fetching is done by ArticleCrawler.
    """

    @property
    def crawler_type(self) -> CrawlerType:
        return CrawlerType.LIST

    @property
    def default_interval_minutes(self) -> int:
        """List crawlers typically run less frequently (e.g., every 30 min)."""
        return 30

    @abstractmethod
    async def get_article_urls(self) -> list[str]:
        """
        Get list of article URLs from the source.

        This method should fetch the latest article URLs from the source
        (e.g., from RSS feed, sitemap, or webpage listing).

        Returns:
            List of article URLs discovered.
        """
        pass

    async def run(self) -> CrawlerResult:
        """
        Execute the list crawler.

        Fetches article URLs from the source. The deduplication and
        queue insertion is handled by the CrawlerService.

        Returns:
            CrawlerResult with discovered URLs in data field.
        """
        import time

        start_time = time.time()

        try:
            urls = await self.get_article_urls()
            execution_time = time.time() - start_time

            return CrawlerResult(
                success=True,
                message=f"Discovered {len(urls)} article URLs",
                data=urls,  # List of URLs
                items_processed=len(urls),
                new_items=len(urls),  # Will be updated after dedup
                execution_time_seconds=execution_time,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            return CrawlerResult(
                success=False,
                message="List crawler execution failed",
                error=str(e),
                execution_time_seconds=execution_time,
            )


class BaseArticleCrawler(BaseCrawler):
    """
    Abstract base class for article crawlers.

    Article crawlers are responsible for:
    1. Fetching article content from URLs in the pending queue
    2. Parsing the article (title, content, author, etc.)
    3. Saving parsed articles to the database

    Each news source should have its own ArticleCrawler because
    the parsing logic is different for each source.
    """

    @property
    def crawler_type(self) -> CrawlerType:
        return CrawlerType.ARTICLE

    @property
    def default_interval_minutes(self) -> int:
        """Article crawlers typically run more frequently (e.g., every 5 min)."""
        return 5

    @property
    def batch_size(self) -> int:
        """Number of URLs to process per run. 0 means no limit. Override in subclass."""
        return 0

    @abstractmethod
    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """
        Parse article data from raw HTML without making network requests.

        This method is used for re-parsing archived articles. It extracts
        the same data as fetch_article but from provided HTML instead of
        fetching it from the network.

        Args:
            raw_html: The raw HTML content of the article page.
            url: The original URL of the article (for reference).

        Returns:
            ArticleData object containing parsed article data.
            Note: raw_html field will NOT be set (caller should set if needed).

        Raises:
            Exception if parsing fails.
        """
        pass

    @abstractmethod
    async def fetch_article(self, url: str) -> ArticleData:
        """
        Fetch and parse a single article.

        Args:
            url: The article URL to fetch.

        Returns:
            ArticleData object containing parsed article data.

        Raises:
            Exception if fetching or parsing fails.
        """
        pass

    async def run(self, urls: list[str] | None = None) -> CrawlerResult:
        """
        Execute the article crawler.

        Fetches and parses articles from the provided URLs.
        The URL selection from queue is handled by CrawlerService.

        Args:
            urls: List of URLs to fetch. If None, returns empty result.

        Returns:
            CrawlerResult with parsed ArticleData objects in data field.
        """
        import time

        start_time = time.time()
        items_processed = 0
        new_items = 0
        articles: list[ArticleData] = []
        failed_urls: list[tuple[str, str]] = []  # (url, error)

        if not urls:
            return CrawlerResult(
                success=True,
                message="No URLs to process",
                data=[],
                items_processed=0,
                new_items=0,
                execution_time_seconds=0.0,
            )

        try:
            for url in urls:
                items_processed += 1
                try:
                    article = await self.fetch_article(url)
                    articles.append(article)
                    new_items += 1
                except Exception as e:
                    # Log individual article failures but continue
                    print(f"[{self.name}] Failed to fetch {url}: {e}")
                    failed_urls.append((url, str(e)))

            execution_time = time.time() - start_time

            return CrawlerResult(
                success=True,
                message=f"Fetched {new_items}/{items_processed} articles",
                data={
                    "articles": articles,
                    "failed_urls": failed_urls,
                },
                items_processed=items_processed,
                new_items=new_items,
                execution_time_seconds=execution_time,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            return CrawlerResult(
                success=False,
                message="Article crawler execution failed",
                error=str(e),
                items_processed=items_processed,
                new_items=new_items,
                execution_time_seconds=execution_time,
            )
