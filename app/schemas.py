"""Pydantic V2 schemas for request/response validation."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models import CrawlerStatus, CrawlerType, UrlStatus


class CrawlerConfigBase(BaseModel):
    """Base schema for crawler configuration."""

    name: str
    display_name: str
    crawler_type: CrawlerType
    source: str
    interval_minutes: int = Field(default=60, ge=1)


class CrawlerConfigCreate(CrawlerConfigBase):
    """Schema for creating a new crawler configuration."""

    is_active: bool = True
    timeout_seconds: int = Field(default=300, ge=1)


class CrawlerConfigUpdate(BaseModel):
    """Schema for updating crawler configuration."""

    is_active: bool | None = None
    interval_minutes: int | None = Field(default=None, ge=1)
    timeout_seconds: int | None = Field(default=None, ge=1)


class CrawlerConfigResponse(CrawlerConfigBase):
    """Schema for crawler configuration response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    is_active: bool
    timeout_seconds: int
    last_run_status: CrawlerStatus
    last_run_time: datetime | None
    next_run_time: datetime | None
    error_log: str | None
    last_run_items_count: int
    total_items_count: int
    created_at: datetime
    updated_at: datetime


class IntervalUpdateRequest(BaseModel):
    """Schema for updating crawler interval."""

    interval_minutes: int = Field(ge=1)


class ToggleResponse(BaseModel):
    """Schema for toggle response."""

    success: bool
    is_active: bool


class RunNowResponse(BaseModel):
    """Schema for run now response."""

    success: bool
    message: str


class NewsArticleBase(BaseModel):
    """Base schema for news article."""

    url: str
    title: str
    content: str
    summary: str | None = None
    author: str | None = None
    source: str
    category: str | None = None
    sub_category: str | None = None
    tags: list[str] | None = None
    published_at: datetime | None = None
    images: list[str] | None = None


class NewsArticleCreate(NewsArticleBase):
    """Schema for creating a new article."""

    pass


class NewsArticleResponse(NewsArticleBase):
    """Schema for article response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    url_hash: str
    crawler_name: str
    crawled_at: datetime


class PendingUrlBase(BaseModel):
    """Base schema for pending URL."""

    url: str
    source: str


class PendingUrlResponse(PendingUrlBase):
    """Schema for pending URL response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    url_hash: str
    status: UrlStatus
    retry_count: int
    error_message: str | None
    discovered_at: datetime
    processed_at: datetime | None


class QueueStatsResponse(BaseModel):
    """Schema for queue statistics response."""

    pending: int
    processing: int
    completed: int
    failed: int
    total: int


# ============== Data Management Schemas ==============


class SourceStats(BaseModel):
    """Statistics for a single news source."""

    source: str
    total_count: int
    yesterday_count: int
    archived_count: int
    has_raw_html_count: int


class ArchiveResult(BaseModel):
    """Result of an archive operation."""

    source: str
    archived_count: int
    freed_space_mb: float
    archive_path: str


class ReparsePreview(BaseModel):
    """Preview of reparse operation."""

    source: str
    total_available: int  # Total reparseable articles (in_db + archived)
    in_db_count: int  # Articles with raw_html still in database
    archived_count: int  # Articles with raw_html in archive files


class ReparseJobStatusSchema(BaseModel):
    """Status of a reparse job."""

    model_config = ConfigDict(from_attributes=True)

    job_id: str
    source: str
    status: str
    total_count: int
    processed_count: int
    failed_count: int
    progress_percent: float
    error_log: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ArchiveInfo(BaseModel):
    """Archive status information for a source."""

    source: str
    total_batches: int
    total_archived_articles: int
    total_size_mb: float
    months: list[str]  # List of months with archives (e.g., ["2025-01", "2025-02"])
