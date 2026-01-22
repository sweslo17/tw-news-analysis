"""Archive scheduler for automatic daily archiving of raw HTML."""

import logging
from datetime import date, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import Settings
from app.database import SessionLocal
from app.services.data_management_service import DataManagementService

logger = logging.getLogger(__name__)


class ArchiveScheduler:
    """Scheduler for automatic daily archiving of raw HTML."""

    _instance: "ArchiveScheduler | None" = None
    _scheduler: BackgroundScheduler | None = None

    def __new__(cls, config: Settings | None = None) -> "ArchiveScheduler":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: Settings | None = None) -> None:
        if hasattr(self, "_initialized") and self._initialized:
            return

        from app.config import settings

        self.config = config or settings
        self._scheduler = BackgroundScheduler(
            timezone=self.config.scheduler_timezone,
        )
        self._initialized = True

    def start(self) -> None:
        """Start the archive scheduler."""
        if not self.config.auto_archive_enabled:
            logger.info("Auto archive is disabled, skipping scheduler start")
            return

        if self._scheduler and self._scheduler.running:
            logger.warning("Archive scheduler is already running")
            return

        # Add daily archive job
        self._scheduler.add_job(
            self._run_daily_archive,
            CronTrigger(
                hour=self.config.auto_archive_hour,
                minute=self.config.auto_archive_minute,
                timezone=self.config.scheduler_timezone,
            ),
            id="daily_archive",
            replace_existing=True,
            name="Daily Raw HTML Archive",
        )

        self._scheduler.start()
        logger.info(
            f"Archive scheduler started. "
            f"Daily archive runs at {self.config.auto_archive_hour:02d}:{self.config.auto_archive_minute:02d}"
        )

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the archive scheduler."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Archive scheduler shutdown")

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._scheduler is not None and self._scheduler.running

    def _run_daily_archive(self) -> None:
        """
        Run daily archive task.

        Archives all sources' raw HTML from yesterday.
        Uses additive mode - only archives articles not yet archived.
        """
        yesterday = date.today() - timedelta(days=1)
        logger.info(f"Starting daily archive for {yesterday}")

        session = SessionLocal()
        try:
            service = DataManagementService(session)
            sources = service.get_all_sources()

            total_archived = 0
            for source in sources:
                try:
                    result = service.archive_source(
                        source=source,
                        target_date=yesterday,
                    )
                    if result.archived_count > 0:
                        logger.info(
                            f"Archived {result.archived_count} articles for {source} "
                            f"({result.freed_space_mb:.2f} MB freed)"
                        )
                        total_archived += result.archived_count
                except Exception as e:
                    logger.error(f"Failed to archive {source}: {e}")

            logger.info(f"Daily archive completed. Total archived: {total_archived} articles")

        except Exception as e:
            logger.error(f"Daily archive failed: {e}")
        finally:
            session.close()

    def run_archive_now(self, source: str | None = None, target_date: date | None = None) -> dict:
        """
        Manually trigger archive operation.

        Args:
            source: Specific source to archive (None = all sources)
            target_date: Date to archive (None = yesterday)

        Returns:
            Dict with results
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        session = SessionLocal()
        try:
            service = DataManagementService(session)

            if source:
                sources = [source]
            else:
                sources = service.get_all_sources()

            results = []
            for src in sources:
                try:
                    result = service.archive_source(
                        source=src,
                        target_date=target_date,
                    )
                    results.append({
                        "source": src,
                        "archived_count": result.archived_count,
                        "freed_space_mb": result.freed_space_mb,
                    })
                except Exception as e:
                    results.append({
                        "source": src,
                        "error": str(e),
                    })

            return {"date": str(target_date), "results": results}

        finally:
            session.close()


# Global singleton instance
archive_scheduler = ArchiveScheduler()
