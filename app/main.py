"""FastAPI application with HTMX support for Crawler Admin Dashboard."""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from app.config import settings
from app.database import create_db_and_tables, get_db
from app.models import CrawlerStatus, CrawlerType, ReparseJobStatus
from app.scheduler import scheduler_manager
from app.services.crawler_service import CrawlerService
from app.services.pending_url_service import PendingUrlService
from app.services.data_management_service import DataManagementService
from app.services.reparse_service import ReparseService
from app.services.archive_scheduler import archive_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Templates directory
TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Timezone for display
LOCAL_TZ = ZoneInfo(settings.scheduler_timezone)


def to_local_time(dt: datetime | None) -> datetime | None:
    """Convert UTC datetime to local timezone."""
    if dt is None:
        return None
    # If datetime is naive (no timezone), assume it's UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)


def format_local_datetime(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime in local timezone (assumes input is UTC naive)."""
    if dt is None:
        return ""
    local_dt = to_local_time(dt)
    return local_dt.strftime(fmt) if local_dt else ""


def format_datetime_direct(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime directly without timezone conversion (for already-local times)."""
    if dt is None:
        return ""
    # If timezone-aware, convert to local timezone first
    if dt.tzinfo is not None:
        dt = dt.astimezone(LOCAL_TZ)
    return dt.strftime(fmt)


# Register Jinja2 filters
templates.env.filters["local_time"] = to_local_time
templates.env.filters["local_datetime"] = format_local_datetime
templates.env.filters["format_datetime"] = format_datetime_direct


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting up Crawler Admin Dashboard...")

    # Create database tables
    create_db_and_tables()

    # Get a database session for startup operations
    from app.database import SessionLocal

    db = SessionLocal()
    try:
        service = CrawlerService(db)
        pending_service = PendingUrlService(db)

        # Reset interrupted states from previous run
        reset_crawlers = service.reset_running_crawlers()
        if reset_crawlers > 0:
            logger.info(f"Reset {reset_crawlers} interrupted crawlers to IDLE")

        reset_urls = pending_service.force_reset_all_processing()
        if reset_urls > 0:
            logger.info(f"Reset {reset_urls} interrupted URLs to PENDING")

        # Sync crawlers to database
        synced = service.sync_crawlers_to_db()
        logger.info(f"Synced {len(synced)} crawlers to database")

        # Start scheduler
        scheduler_manager.start()

        # Schedule all active crawlers
        service.schedule_all_active()
        logger.info("Scheduled all active crawlers")

        # Start archive scheduler
        archive_scheduler.start()
        logger.info("Archive scheduler started")

    finally:
        db.close()

    logger.info("Crawler Admin Dashboard started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Crawler Admin Dashboard...")
    archive_scheduler.shutdown(wait=True)
    scheduler_manager.shutdown(wait=True)
    logger.info("Shutdown complete")


app = FastAPI(
    title="Crawler Admin Dashboard",
    description="Web interface for managing news crawlers",
    version="1.0.0",
    lifespan=lifespan,
)


# Dependencies
def get_crawler_service(db: Session = Depends(get_db)) -> CrawlerService:
    """Dependency for getting crawler service."""
    return CrawlerService(db)


def get_data_management_service(db: Session = Depends(get_db)) -> DataManagementService:
    """Dependency for getting data management service."""
    return DataManagementService(db)


def get_reparse_service(db: Session = Depends(get_db)) -> ReparseService:
    """Dependency for getting reparse service."""
    return ReparseService(db)


ServiceDep = Annotated[CrawlerService, Depends(get_crawler_service)]
DataManagementServiceDep = Annotated[DataManagementService, Depends(get_data_management_service)]
ReparseServiceDep = Annotated[ReparseService, Depends(get_reparse_service)]


# ============== HTML Routes ==============


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, service: ServiceDep):
    """Main dashboard page."""
    crawlers = service.get_all_configs()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "crawlers": crawlers,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


# ============== HTMX Partial Routes ==============


@app.get("/partials/crawler-table", response_class=HTMLResponse)
async def crawler_table_partial(request: Request, service: ServiceDep):
    """HTMX partial: Full crawler table (for polling refresh)."""
    crawlers = service.get_all_configs()
    return templates.TemplateResponse(
        "partials/crawler_table.html",
        {
            "request": request,
            "crawlers": crawlers,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


@app.get("/partials/crawler-row/{crawler_id}", response_class=HTMLResponse)
async def crawler_row_partial(
    request: Request,
    crawler_id: int,
    service: ServiceDep,
):
    """HTMX partial: Single crawler row."""
    config = service.get_config(crawler_id)
    if not config:
        raise HTTPException(status_code=404, detail="Crawler not found")

    return templates.TemplateResponse(
        "partials/crawler_row.html",
        {
            "request": request,
            "crawler": config,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


# ============== API Routes ==============


@app.patch("/api/crawlers/{crawler_id}/toggle", response_class=HTMLResponse)
async def toggle_crawler(
    request: Request,
    crawler_id: int,
    service: ServiceDep,
):
    """Toggle crawler active status. Returns updated row HTML."""
    config = service.toggle_active(crawler_id)
    if not config:
        raise HTTPException(status_code=404, detail="Crawler not found")

    return templates.TemplateResponse(
        "partials/crawler_row.html",
        {
            "request": request,
            "crawler": config,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


@app.patch("/api/crawlers/{crawler_id}/interval", response_class=HTMLResponse)
async def update_interval(
    request: Request,
    crawler_id: int,
    service: ServiceDep,
    interval_minutes: int = Form(...),
):
    """Update crawler interval. Returns updated row HTML."""
    if interval_minutes < 1:
        raise HTTPException(status_code=400, detail="Interval must be >= 1")

    config = service.update_interval(crawler_id, interval_minutes)
    if not config:
        raise HTTPException(status_code=404, detail="Crawler not found")

    return templates.TemplateResponse(
        "partials/crawler_row.html",
        {
            "request": request,
            "crawler": config,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


@app.post("/api/crawlers/{crawler_id}/run", response_class=HTMLResponse)
async def run_crawler_now(
    request: Request,
    crawler_id: int,
    service: ServiceDep,
):
    """Trigger immediate crawler execution. Returns updated row HTML."""
    config = service.get_config(crawler_id)
    if not config:
        raise HTTPException(status_code=404, detail="Crawler not found")

    success = service.run_now(crawler_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to start crawler")

    # Refresh config to get running status
    config = service.get_config(crawler_id)

    return templates.TemplateResponse(
        "partials/crawler_row.html",
        {
            "request": request,
            "crawler": config,
            "CrawlerStatus": CrawlerStatus,
            "CrawlerType": CrawlerType,
        },
    )


# ============== Health Check ==============


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "scheduler_running": scheduler_manager.is_running,
        "archive_scheduler_running": archive_scheduler.is_running,
    }


# ============== Data Management Routes ==============


@app.get("/data-management", response_class=HTMLResponse)
async def data_management_page(
    request: Request,
    dm_service: DataManagementServiceDep,
    reparse_service: ReparseServiceDep,
):
    """Data management page."""
    stats = dm_service.get_source_statistics()
    sources = dm_service.get_all_sources()
    recent_jobs = reparse_service.get_recent_jobs(limit=5)

    return templates.TemplateResponse(
        "data_management/index.html",
        {
            "request": request,
            "stats": stats,
            "sources": sources,
            "recent_jobs": recent_jobs,
            "ReparseJobStatus": ReparseJobStatus,
        },
    )


@app.get("/api/data/statistics")
async def get_statistics(dm_service: DataManagementServiceDep):
    """Get statistics for all sources."""
    stats = dm_service.get_source_statistics()
    return [stat.model_dump() for stat in stats]


@app.get("/partials/data/statistics-tab", response_class=HTMLResponse)
async def statistics_tab_partial(
    request: Request,
    dm_service: DataManagementServiceDep,
):
    """HTMX partial: Statistics tab content."""
    stats = dm_service.get_source_statistics()
    return templates.TemplateResponse(
        "data_management/partials/statistics_tab.html",
        {
            "request": request,
            "stats": stats,
        },
    )


@app.get("/partials/data/archive-tab", response_class=HTMLResponse)
async def archive_tab_partial(
    request: Request,
    dm_service: DataManagementServiceDep,
):
    """HTMX partial: Archive tab content."""
    sources = dm_service.get_all_sources()
    return templates.TemplateResponse(
        "data_management/partials/archive_tab.html",
        {
            "request": request,
            "sources": sources,
        },
    )


@app.get("/partials/data/reparse-tab", response_class=HTMLResponse)
async def reparse_tab_partial(
    request: Request,
    dm_service: DataManagementServiceDep,
    reparse_service: ReparseServiceDep,
):
    """HTMX partial: Reparse tab content."""
    sources = dm_service.get_all_sources()
    recent_jobs = reparse_service.get_recent_jobs(limit=5)
    return templates.TemplateResponse(
        "data_management/partials/reparse_tab.html",
        {
            "request": request,
            "sources": sources,
            "recent_jobs": recent_jobs,
            "ReparseJobStatus": ReparseJobStatus,
        },
    )


@app.post("/api/data/archive", response_class=HTMLResponse)
async def archive_source(
    request: Request,
    dm_service: DataManagementServiceDep,
    source: str = Form(...),
    date_filter: str = Form(default="days"),
    before_days: int = Form(default=30),
):
    """Archive raw HTML for a source or all sources."""
    # Determine before_date based on date_filter
    if date_filter == "all":
        before_date = None  # No date limit
    else:
        before_date = date.today() - timedelta(days=before_days)

    if source == "__all__":
        # Archive all sources
        results = dm_service.archive_all_sources(before_date=before_date)
        return templates.TemplateResponse(
            "data_management/partials/archive_result_all.html",
            {
                "request": request,
                "results": results,
            },
        )
    else:
        result = dm_service.archive_source(source=source, before_date=before_date)
        return templates.TemplateResponse(
            "data_management/partials/archive_result.html",
            {
                "request": request,
                "result": result,
            },
        )


@app.post("/api/data/restore", response_class=HTMLResponse)
async def restore_source(
    request: Request,
    dm_service: DataManagementServiceDep,
    source: str = Form(...),
):
    """Restore raw HTML from archive for a source."""
    # Get archive info first
    archive_info = dm_service.get_archive_info(source)
    if archive_info.total_archived_articles == 0:
        return templates.TemplateResponse(
            "data_management/partials/restore_result.html",
            {
                "request": request,
                "result": {"restored_count": 0, "failed_count": 0, "message": "No archived articles found"},
            },
        )

    # Get article IDs from archive (simplified - would need to query RawHtmlArchive)
    from app.models import RawHtmlArchive, ArchiveStatus
    archives = (
        dm_service.session.query(RawHtmlArchive.article_id)
        .filter(
            RawHtmlArchive.source == source,
            RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
        )
        .all()
    )
    article_ids = [a[0] for a in archives]

    result = dm_service.restore_raw_html(article_ids)

    return templates.TemplateResponse(
        "data_management/partials/restore_result.html",
        {
            "request": request,
            "result": result,
        },
    )


@app.get("/api/data/reparse/preview")
async def reparse_preview(
    source: str,
    reparse_service: ReparseServiceDep,
):
    """Preview reparse operation for a source."""
    preview = reparse_service.get_reparse_preview(source)
    return preview.model_dump()


@app.post("/api/data/reparse/start", response_class=HTMLResponse)
async def start_reparse(
    request: Request,
    reparse_service: ReparseServiceDep,
    source: str = Form(...),
):
    """Start a reparse job for a source."""
    job = reparse_service.start_reparse_job(source)

    # Return the job progress component
    job_status = reparse_service.get_job_status(job.id)
    return templates.TemplateResponse(
        "data_management/partials/job_progress.html",
        {
            "request": request,
            "job": job_status,
            "ReparseJobStatus": ReparseJobStatus,
        },
    )


@app.get("/api/data/reparse/status/{job_id}", response_class=HTMLResponse)
async def reparse_status(
    request: Request,
    job_id: str,
    reparse_service: ReparseServiceDep,
):
    """Get reparse job status (HTMX polling)."""
    job_status = reparse_service.get_job_status(job_id)
    if not job_status:
        raise HTTPException(status_code=404, detail="Job not found")

    return templates.TemplateResponse(
        "data_management/partials/job_progress.html",
        {
            "request": request,
            "job": job_status,
            "ReparseJobStatus": ReparseJobStatus,
        },
    )


@app.post("/api/data/reparse/cancel/{job_id}")
async def cancel_reparse(
    job_id: str,
    reparse_service: ReparseServiceDep,
):
    """Cancel a running reparse job."""
    success = reparse_service.cancel_job(job_id)
    return {"success": success}
