"""Crawler management service."""

import asyncio
import json
import time
from datetime import datetime
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db_session
from app.models import CrawlerConfig, CrawlerStatus, CrawlerType, NewsArticle
from app.scheduler import scheduler_manager
from app.services.pending_url_service import PendingUrlService, compute_url_hash
from crawlers.base import (
    ArticleData,
    BaseArticleCrawler,
    BaseCrawler,
    BaseListCrawler,
    CrawlerResult,
)
from crawlers.registry import crawler_registry


class CrawlerService:
    """
    Service layer for crawler operations.

    Handles business logic and coordinates between database and scheduler.
    Supports both ListCrawler and ArticleCrawler types.
    """

    def __init__(self, session: Session) -> None:
        """
        Initialize the crawler service.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def reset_running_crawlers(self) -> int:
        """
        Reset all crawlers with RUNNING status to IDLE.

        Called on application startup to handle interrupted crawlers
        from previous runs (e.g., server crash, restart).

        Returns:
            Number of crawlers reset.
        """
        from sqlalchemy import update

        stmt = (
            update(CrawlerConfig)
            .where(CrawlerConfig.last_run_status == CrawlerStatus.RUNNING)
            .values(
                last_run_status=CrawlerStatus.IDLE,
                updated_at=datetime.utcnow(),
            )
        )
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def sync_crawlers_to_db(self) -> list[CrawlerConfig]:
        """
        Synchronize discovered crawlers with database.

        Called on application startup to register new crawlers
        and update existing ones.

        Returns:
            List of synchronized crawler configurations.
        """
        crawlers = crawler_registry.discover_crawlers()
        synced_configs = []

        for name, crawler in crawlers.items():
            stmt = select(CrawlerConfig).where(CrawlerConfig.name == name)
            existing = self.session.execute(stmt).scalar_one_or_none()

            # Convert crawler type
            crawler_type = CrawlerType(crawler.crawler_type.value)

            if existing:
                # Update fields if changed
                existing.display_name = crawler.display_name
                existing.crawler_type = crawler_type
                existing.source = crawler.source
                existing.updated_at = datetime.utcnow()
                self.session.add(existing)
                synced_configs.append(existing)
            else:
                # Create new config
                config = CrawlerConfig(
                    name=name,
                    display_name=crawler.display_name,
                    crawler_type=crawler_type,
                    source=crawler.source,
                    interval_minutes=crawler.default_interval_minutes,
                    timeout_seconds=crawler.default_timeout_seconds,
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                self.session.add(config)
                synced_configs.append(config)

        self.session.commit()
        return synced_configs

    def get_all_configs(self) -> list[CrawlerConfig]:
        """Get all crawler configurations."""
        stmt = select(CrawlerConfig).order_by(
            CrawlerConfig.source, CrawlerConfig.crawler_type, CrawlerConfig.name
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_configs_by_type(self, crawler_type: CrawlerType) -> list[CrawlerConfig]:
        """Get crawler configurations by type."""
        stmt = (
            select(CrawlerConfig)
            .where(CrawlerConfig.crawler_type == crawler_type)
            .order_by(CrawlerConfig.source, CrawlerConfig.name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_config(self, crawler_id: int) -> CrawlerConfig | None:
        """Get single crawler configuration by ID."""
        return self.session.get(CrawlerConfig, crawler_id)

    def get_config_by_name(self, name: str) -> CrawlerConfig | None:
        """Get crawler configuration by name."""
        stmt = select(CrawlerConfig).where(CrawlerConfig.name == name)
        return self.session.execute(stmt).scalar_one_or_none()

    def toggle_active(self, crawler_id: int) -> CrawlerConfig | None:
        """
        Toggle crawler active status and update scheduler.

        Args:
            crawler_id: The crawler ID to toggle.

        Returns:
            Updated crawler config or None if not found.
        """
        config = self.get_config(crawler_id)
        if not config:
            return None

        config.is_active = not config.is_active
        config.updated_at = datetime.utcnow()

        if config.is_active:
            # Re-add job to scheduler
            self._schedule_crawler(config)
            config.next_run_time = scheduler_manager.get_next_run_time(config.name)
        else:
            # Remove from scheduler
            scheduler_manager.remove_job(config.name)
            config.next_run_time = None

        self.session.commit()
        self.session.refresh(config)

        return config

    def update_interval(
        self, crawler_id: int, interval_minutes: int
    ) -> CrawlerConfig | None:
        """
        Update crawler interval and reschedule if active.

        Args:
            crawler_id: The crawler ID to update.
            interval_minutes: New interval in minutes.

        Returns:
            Updated crawler config or None if not found.
        """
        config = self.get_config(crawler_id)
        if not config:
            return None

        config.interval_minutes = interval_minutes
        config.updated_at = datetime.utcnow()

        if config.is_active:
            scheduler_manager.reschedule_job(config.name, interval_minutes)
            config.next_run_time = scheduler_manager.get_next_run_time(config.name)

        self.session.commit()
        self.session.refresh(config)

        return config

    def run_now(self, crawler_id: int) -> bool:
        """
        Trigger immediate crawler execution.

        Args:
            crawler_id: The crawler ID to run.

        Returns:
            True if successfully triggered, False otherwise.
        """
        config = self.get_config(crawler_id)
        if not config:
            return False

        crawler = crawler_registry.get_crawler(config.name)
        if not crawler:
            return False

        # Update status to running
        config.last_run_status = CrawlerStatus.RUNNING
        self.session.commit()

        # Create the job function
        job_func = self._create_job_function(config.name)

        # Trigger immediate execution via scheduler
        if scheduler_manager.job_exists(config.name):
            scheduler_manager.run_job_now(config.name)
        else:
            # If job doesn't exist (crawler is inactive), run directly
            scheduler_manager._scheduler.add_job(
                job_func,
                id=f"{config.name}_immediate_{datetime.utcnow().timestamp()}",
            )

        return True

    def _schedule_crawler(self, config: CrawlerConfig) -> None:
        """Add crawler to scheduler."""
        crawler = crawler_registry.get_crawler(config.name)
        if not crawler:
            return

        job_func = self._create_job_function(config.name)

        scheduler_manager.add_job(
            job_id=config.name,
            func=job_func,
            interval_minutes=config.interval_minutes,
        )

    def _create_job_function(self, crawler_name: str) -> Callable:
        """Create a job function for the scheduler."""

        def job_func():
            """Execute crawler in isolation."""
            execute_crawler_isolated(crawler_name)

        return job_func

    def schedule_all_active(self) -> None:
        """Schedule all active crawlers. Called on startup."""
        configs = self.get_all_configs()
        for config in configs:
            if config.is_active:
                self._schedule_crawler(config)
                # Update next_run_time
                config.next_run_time = scheduler_manager.get_next_run_time(config.name)

        self.session.commit()


def execute_crawler_isolated(crawler_name: str) -> None:
    """
    Execute a crawler in isolation.

    This function runs in a separate thread/process and uses its own
    database session to ensure crawler isolation.

    Args:
        crawler_name: The name of the crawler to execute.
    """
    with get_db_session() as session:
        try:
            # Get config and crawler
            stmt = select(CrawlerConfig).where(CrawlerConfig.name == crawler_name)
            config = session.execute(stmt).scalar_one_or_none()

            if not config:
                print(f"Crawler config not found: {crawler_name}")
                return

            crawler = crawler_registry.get_crawler(crawler_name)
            if not crawler:
                print(f"Crawler not found: {crawler_name}")
                return

            # Update status to running
            config.last_run_status = CrawlerStatus.RUNNING
            session.commit()

            # Execute based on crawler type
            start_time = time.time()
            try:
                if isinstance(crawler, BaseListCrawler):
                    result = asyncio.run(
                        _execute_list_crawler(session, config, crawler)
                    )
                elif isinstance(crawler, BaseArticleCrawler):
                    result = asyncio.run(
                        _execute_article_crawler(session, config, crawler)
                    )
                else:
                    raise ValueError(f"Unknown crawler type: {type(crawler)}")

                result.execution_time_seconds = time.time() - start_time

                if result.success:
                    config.last_run_status = CrawlerStatus.SUCCESS
                    config.error_log = None
                    # Update statistics
                    items_count = result.new_items or 0
                    config.last_run_items_count = items_count
                    config.total_items_count += items_count
                    asyncio.run(crawler.on_success(result))
                else:
                    config.last_run_status = CrawlerStatus.FAILED
                    config.error_log = (
                        result.error[:4096] if result.error else result.message[:4096]
                    )
                    config.last_run_items_count = 0
                    asyncio.run(crawler.on_failure(result))

            except asyncio.TimeoutError:
                config.last_run_status = CrawlerStatus.FAILED
                config.error_log = f"Execution timeout after {config.timeout_seconds}s"

            except Exception as e:
                config.last_run_status = CrawlerStatus.FAILED
                config.error_log = str(e)[:4096]

            config.last_run_time = datetime.utcnow()
            config.updated_at = datetime.utcnow()
            config.next_run_time = scheduler_manager.get_next_run_time(config.name)
            session.commit()

        except Exception as e:
            print(f"Error executing crawler {crawler_name}: {e}")
            session.rollback()


async def _execute_list_crawler(
    session: Session,
    config: CrawlerConfig,
    crawler: BaseListCrawler,
) -> CrawlerResult:
    """
    Execute a list crawler.

    1. Run the crawler to get URLs
    2. Deduplicate against existing articles and queue
    3. Add new URLs to pending queue

    Args:
        session: Database session.
        config: Crawler configuration.
        crawler: List crawler instance.

    Returns:
        CrawlerResult with execution details.
    """
    # Run the crawler
    result = await asyncio.wait_for(
        crawler.run(),
        timeout=config.timeout_seconds,
    )

    if not result.success:
        return result

    # Get discovered URLs
    urls = result.data or []
    if not urls:
        result.message = "No URLs discovered"
        result.new_items = 0
        return result

    # Add to pending queue (deduplication happens inside)
    pending_service = PendingUrlService(session)
    new_count = pending_service.add_urls(urls, config.source)

    result.new_items = new_count
    result.message = f"Discovered {len(urls)} URLs, {new_count} new added to queue"

    return result


async def _execute_article_crawler(
    session: Session,
    config: CrawlerConfig,
    crawler: BaseArticleCrawler,
) -> CrawlerResult:
    """
    Execute an article crawler.

    1. Reset stale PROCESSING URLs (handles crashed crawlers)
    2. Get pending URLs from queue for this source
    3. Run the crawler to fetch articles
    4. Save articles to database
    5. Update queue status

    Args:
        session: Database session.
        config: Crawler configuration.
        crawler: Article crawler instance.

    Returns:
        CrawlerResult with execution details.
    """
    pending_service = PendingUrlService(session)

    # Reset stale PROCESSING URLs before fetching new ones
    # This handles cases where previous crawler run crashed
    reset_count = pending_service.reset_stale_processing(minutes=10)
    if reset_count > 0:
        print(f"[{crawler.name}] Reset {reset_count} stale PROCESSING URLs")

    # Get pending URLs from queue
    pending_urls = pending_service.get_pending_urls(
        source=config.source,
        limit=crawler.batch_size,
    )

    if not pending_urls:
        return CrawlerResult(
            success=True,
            message="No pending URLs to process",
            items_processed=0,
            new_items=0,
        )

    # Create URL mapping for later status updates
    url_to_pending = {p.url: p for p in pending_urls}
    urls = list(url_to_pending.keys())

    # Run the crawler
    result = await asyncio.wait_for(
        crawler.run(urls=urls),
        timeout=config.timeout_seconds,
    )

    if not result.success:
        # Mark all as failed
        for pending in pending_urls:
            pending_service.mark_failed(pending.id, result.error or "Crawler failed")
        return result

    # Process results
    data = result.data or {}
    articles = data.get("articles", [])
    failed_urls = data.get("failed_urls", [])

    # Save successful articles
    for article_data in articles:
        article = NewsArticle(
            url=article_data.url,
            url_hash=compute_url_hash(article_data.url),
            title=article_data.title,
            content=article_data.content,
            summary=article_data.summary,
            author=article_data.author,
            source=crawler.source,
            crawler_name=crawler.name,
            category=article_data.category,
            sub_category=article_data.sub_category,
            tags=json.dumps(article_data.tags, ensure_ascii=False) if article_data.tags else None,
            published_at=article_data.published_at,
            crawled_at=datetime.utcnow(),
            raw_html=article_data.raw_html,
            images=json.dumps(article_data.images, ensure_ascii=False) if article_data.images else None,
        )
        session.add(article)

        # Mark URL as completed
        if article_data.url in url_to_pending:
            pending_service.mark_completed(url_to_pending[article_data.url].id)

    # Mark failed URLs
    for url, error in failed_urls:
        if url in url_to_pending:
            pending_service.mark_failed(url_to_pending[url].id, error)

    session.commit()

    # Update statistics
    result.new_items = len(articles)
    result.message = f"Fetched {len(articles)} articles, {len(failed_urls)} failed"

    return result
