"""SQLAlchemy ORM models."""

import enum
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Enum, Integer, String, Text, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class CrawlerStatus(str, enum.Enum):
    """Crawler execution status."""

    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class CrawlerType(str, enum.Enum):
    """Type of crawler."""

    LIST = "list"  # 列表爬蟲 - 抓取文章 URL 列表
    ARTICLE = "article"  # 文章爬蟲 - 抓取文章內容


class UrlStatus(str, enum.Enum):
    """Status of pending URL in queue."""

    PENDING = "pending"  # 等待處理
    PROCESSING = "processing"  # 處理中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"  # 失敗


class CrawlerConfig(Base):
    """Crawler configuration and status."""

    __tablename__ = "crawler_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    display_name = Column(String(200), nullable=False)
    crawler_type = Column(
        Enum(CrawlerType), nullable=False, index=True
    )  # LIST or ARTICLE
    source = Column(String(100), nullable=False, index=True)  # 新聞來源名稱
    is_active = Column(Boolean, default=True, nullable=False)
    interval_minutes = Column(Integer, default=60, nullable=False)
    timeout_seconds = Column(Integer, default=300, nullable=False)
    last_run_status = Column(
        Enum(CrawlerStatus), default=CrawlerStatus.IDLE, nullable=False
    )
    last_run_time = Column(DateTime, nullable=True)
    next_run_time = Column(DateTime, nullable=True)
    error_log = Column(Text, nullable=True)
    # Statistics fields
    last_run_items_count = Column(Integer, default=0, nullable=False)  # 上次執行處理的項目數
    total_items_count = Column(Integer, default=0, nullable=False)  # 總共處理的項目數
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def __repr__(self) -> str:
        return f"<CrawlerConfig(name={self.name}, type={self.crawler_type}, is_active={self.is_active})>"


class NewsArticle(Base):
    """News article storage."""

    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(2048), unique=True, nullable=False)
    url_hash = Column(String(32), nullable=False, index=True)  # MD5 hash for fast lookup
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=False)
    summary = Column(Text, nullable=True)
    author = Column(String(200), nullable=True)
    source = Column(String(100), nullable=False)  # e.g., "ETtoday", "UDN"
    crawler_name = Column(String(100), nullable=False, index=True)
    category = Column(String(100), nullable=True)
    sub_category = Column(String(100), nullable=True)
    tags = Column(Text, nullable=True)  # JSON array string
    published_at = Column(DateTime, nullable=True)
    crawled_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    raw_html = Column(Text, nullable=True)
    images = Column(Text, nullable=True)  # JSON array string

    def __repr__(self) -> str:
        return f"<NewsArticle(title={self.title[:30]}..., source={self.source})>"


class PendingUrl(Base):
    """Queue of URLs waiting to be crawled."""

    __tablename__ = "pending_urls"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(2048), unique=True, nullable=False)
    url_hash = Column(String(32), nullable=False, index=True)  # MD5 hash for fast lookup
    source = Column(String(100), nullable=False, index=True)  # 新聞來源名稱
    status = Column(Enum(UrlStatus), default=UrlStatus.PENDING, nullable=False, index=True)
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    error_message = Column(Text, nullable=True)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def __repr__(self) -> str:
        return f"<PendingUrl(url={self.url[:50]}..., source={self.source}, status={self.status})>"


class ArchiveStatus(str, enum.Enum):
    """Status of raw HTML archive."""

    ACTIVE = "active"      # raw_html still in database
    ARCHIVED = "archived"  # raw_html moved to file system
    DELETED = "deleted"    # raw_html permanently deleted


class RawHtmlArchive(Base):
    """Track archived raw HTML for articles."""

    __tablename__ = "raw_html_archives"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, nullable=False, index=True)
    source = Column(String(100), nullable=False, index=True)
    archive_path = Column(String(500), nullable=True)  # Path to archive file
    status = Column(Enum(ArchiveStatus), default=ArchiveStatus.ACTIVE, nullable=False)
    original_size = Column(Integer, nullable=False)  # Original size in bytes
    compressed_size = Column(Integer, nullable=True)  # Compressed size in bytes
    archived_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<RawHtmlArchive(article_id={self.article_id}, status={self.status})>"


class ReparseJobStatus(str, enum.Enum):
    """Status of reparse job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReparseJob(Base):
    """Track reparse job progress."""

    __tablename__ = "reparse_jobs"

    id = Column(String(36), primary_key=True)  # UUID
    source = Column(String(100), nullable=False, index=True)
    status = Column(Enum(ReparseJobStatus), default=ReparseJobStatus.PENDING, nullable=False)
    total_count = Column(Integer, default=0, nullable=False)
    processed_count = Column(Integer, default=0, nullable=False)
    failed_count = Column(Integer, default=0, nullable=False)
    error_log = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<ReparseJob(id={self.id}, source={self.source}, status={self.status})>"


# ============================================================================
# Pipeline Models
# ============================================================================


class PipelineStage(str, enum.Enum):
    """Pipeline processing stages."""

    FETCH = "fetch"  # 從 DB 取得文章
    RULE_FILTER = "rule_filter"  # Rule-based 篩選
    LLM_ANALYSIS = "llm_analysis"  # LLM 分析
    STORE = "store"  # 儲存結果


class FilterDecision(str, enum.Enum):
    """Filter decision for articles."""

    KEEP = "keep"  # 保留文章
    FILTER = "filter"  # 過濾掉文章
    FORCE_INCLUDE = "force_include"  # 強制納入


class PipelineRunStatus(str, enum.Enum):
    """Status of pipeline run."""

    PENDING = "pending"  # 等待執行
    RUNNING = "running"  # 執行中
    PAUSED = "paused"  # 暫停
    COMPLETED = "completed"  # 完成
    FAILED = "failed"  # 失敗


class PipelineRun(Base):
    """Track pipeline execution batches and statistics."""

    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    status = Column(
        Enum(PipelineRunStatus), default=PipelineRunStatus.PENDING, nullable=False
    )
    current_stage = Column(Enum(PipelineStage), nullable=True)

    # Date range for fetching articles
    date_from = Column(DateTime, nullable=True)
    date_to = Column(DateTime, nullable=True)

    # Statistics
    total_articles = Column(Integer, default=0, nullable=False)
    rule_filtered_count = Column(Integer, default=0, nullable=False)
    rule_passed_count = Column(Integer, default=0, nullable=False)
    analyzed_count = Column(Integer, default=0, nullable=False)
    force_included_count = Column(Integer, default=0, nullable=False)

    # Timestamps
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Error log
    error_log = Column(Text, nullable=True)

    # Relationships
    filter_results = relationship(
        "ArticleFilterResult", back_populates="pipeline_run", cascade="all, delete-orphan"
    )
    analysis_results = relationship(
        "ArticleAnalysisResult", back_populates="pipeline_run", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<PipelineRun(id={self.id}, name={self.name}, status={self.status})>"


class ArticleFilterResult(Base):
    """Filter decision for each article at each stage."""

    __tablename__ = "article_filter_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=False, index=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), nullable=False, index=True)

    # Stage and decision
    stage = Column(Enum(PipelineStage), nullable=False, index=True)
    decision = Column(Enum(FilterDecision), nullable=False, index=True)
    confidence = Column(Float, nullable=True)  # 0.0-1.0 for LLM decisions

    # Rule/LLM details
    rule_name = Column(String(100), nullable=True)  # For rule-based filter
    reason = Column(Text, nullable=True)  # Reason for decision

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    pipeline_run = relationship("PipelineRun", back_populates="filter_results")

    def __repr__(self) -> str:
        return f"<ArticleFilterResult(article_id={self.article_id}, stage={self.stage}, decision={self.decision})>"


class ForceIncludeArticle(Base):
    """Articles that should always be included regardless of filter results."""

    __tablename__ = "force_include_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), nullable=False, unique=True, index=True)
    reason = Column(Text, nullable=False)
    added_by = Column(String(100), nullable=True)  # User who added
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<ForceIncludeArticle(article_id={self.article_id})>"


class FilterRuleType(str, enum.Enum):
    """Type of filter rule."""

    KEYWORD = "keyword"  # Exact keyword match
    PATTERN = "pattern"  # Regex pattern match
    CATEGORY = "category"  # Category-based filter


class FilterRule(Base):
    """Configurable rule-based filter rules."""

    __tablename__ = "filter_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    rule_type = Column(Enum(FilterRuleType), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Rule configuration (JSON)
    # For KEYWORD: {"keywords": ["星座", "運勢"], "match_fields": ["title", "tags"]}
    # For PATTERN: {"pattern": "\\d{4}-\\d{2}-\\d{2}.*開獎", "match_fields": ["title"]}
    # For CATEGORY: {"categories": ["星座"], "sub_categories": ["運勢"]}
    config = Column(Text, nullable=False)  # JSON string

    # Statistics
    total_filtered_count = Column(Integer, default=0, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def __repr__(self) -> str:
        return f"<FilterRule(name={self.name}, type={self.rule_type}, active={self.is_active})>"


class ArticleAnalysisResult(Base):
    """LLM analysis results for articles (framework only)."""

    __tablename__ = "article_analysis_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=False, index=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), nullable=False, index=True)

    # Analysis result (JSON)
    analysis_result = Column(Text, nullable=True)  # JSON string for flexible schema

    # LLM metadata
    llm_provider = Column(String(50), nullable=True)
    llm_model = Column(String(100), nullable=True)
    tokens_used = Column(Integer, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    pipeline_run = relationship("PipelineRun", back_populates="analysis_results")

    def __repr__(self) -> str:
        return f"<ArticleAnalysisResult(article_id={self.article_id}, pipeline_run_id={self.pipeline_run_id})>"
