"""Application configuration using Pydantic Settings."""

from functools import lru_cache

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


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()


settings = get_settings()
