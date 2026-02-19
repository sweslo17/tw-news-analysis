"""Application configuration using Pydantic Settings."""

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Crawler Admin Dashboard"
    debug: bool = False
    database_url: str = "sqlite:///./crawler_admin.db"

    # Scheduler settings
    scheduler_timezone: str = "Asia/Taipei"

    # Crawler default settings
    default_crawler_interval_minutes: int = 60
    default_crawler_timeout_seconds: int = 300

    # Archive settings
    archive_base_path: str = "./data/archives"
    archive_batch_size: int = 500  # Articles per batch file
    archive_compression: str = "gzip"  # gzip or none

    # Auto archive scheduler settings
    auto_archive_enabled: bool = True
    auto_archive_hour: int = 1  # Hour to run (24h format)
    auto_archive_minute: int = 0  # Minute to run

    # OpenAI API Key (for LLM analysis batch API)
    openai_api_key: str | None = None

    # LLM Analysis Settings (OpenAI Batch API)
    llm_analysis_model: str = "gpt-4o-mini"  # Model for structured analysis
    llm_analysis_poll_interval: int = 30  # Seconds between batch status checks
    llm_analysis_max_wait: int = 7200  # Max seconds to wait for batch (2 hours)

    # TimescaleDB (analysis results storage)
    timescale_url: str | None = None  # Set TIMESCALE_URL in .env

    # Pipeline Settings
    pipeline_default_days: int = 1  # Default days to fetch for quick run

    @model_validator(mode="after")
    def _normalize_timescale_url(self) -> "Settings":
        """Auto-convert postgres:// to postgresql+psycopg2:// for SQLAlchemy."""
        if self.timescale_url and self.timescale_url.startswith("postgres://"):
            self.timescale_url = self.timescale_url.replace(
                "postgres://", "postgresql+psycopg2://", 1
            )
        return self


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()


settings = get_settings()
