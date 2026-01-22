"""Data management service for statistics, archiving, and restoration."""

import gzip
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from app.config import settings
from app.models import ArchiveStatus, NewsArticle, RawHtmlArchive
from app.schemas import ArchiveInfo, ArchiveResult, SourceStats

logger = logging.getLogger(__name__)


class DataManagementService:
    """Service for managing data: statistics, archiving, and restoration."""

    def __init__(self, session: Session):
        self.session = session
        self.archive_base_path = Path(settings.archive_base_path)
        self.batch_size = settings.archive_batch_size
        self.compression = settings.archive_compression

    def get_source_statistics(self) -> list[SourceStats]:
        """Get statistics for each news source."""
        # Get yesterday's date range (in UTC)
        today = datetime.utcnow().date()
        yesterday_start = datetime.combine(today - timedelta(days=1), datetime.min.time())
        yesterday_end = datetime.combine(today, datetime.min.time())

        # Get all distinct sources
        sources = (
            self.session.query(NewsArticle.source)
            .distinct()
            .all()
        )

        results = []
        for (source,) in sources:
            # Total count
            total_count = (
                self.session.query(func.count(NewsArticle.id))
                .filter(NewsArticle.source == source)
                .scalar()
            )

            # Yesterday's count (based on crawled_at)
            yesterday_count = (
                self.session.query(func.count(NewsArticle.id))
                .filter(
                    NewsArticle.source == source,
                    NewsArticle.crawled_at >= yesterday_start,
                    NewsArticle.crawled_at < yesterday_end,
                )
                .scalar()
            )

            # Count with raw_html still in database
            has_raw_html_count = (
                self.session.query(func.count(NewsArticle.id))
                .filter(
                    NewsArticle.source == source,
                    NewsArticle.raw_html.isnot(None),
                    NewsArticle.raw_html != "",
                )
                .scalar()
            )

            # Archived count
            archived_count = (
                self.session.query(func.count(RawHtmlArchive.id))
                .filter(
                    RawHtmlArchive.source == source,
                    RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
                )
                .scalar()
            )

            results.append(
                SourceStats(
                    source=source,
                    total_count=total_count or 0,
                    yesterday_count=yesterday_count or 0,
                    archived_count=archived_count or 0,
                    has_raw_html_count=has_raw_html_count or 0,
                )
            )

        return results

    def archive_source(
        self,
        source: str,
        before_date: date | None = None,
        target_date: date | None = None,
    ) -> ArchiveResult:
        """
        Archive raw_html for a source in additive mode.

        Only archives articles that haven't been archived yet.
        New data goes into new batch files without modifying existing ones.

        Args:
            source: News source to archive
            before_date: Archive articles crawled before this date
            target_date: Archive articles crawled on this specific date

        Returns:
            ArchiveResult with details of the operation
        """
        # Build query for articles to archive
        query = (
            self.session.query(NewsArticle)
            .filter(
                NewsArticle.source == source,
                NewsArticle.raw_html.isnot(None),
                NewsArticle.raw_html != "",
            )
        )

        # Apply date filters
        if target_date:
            target_start = datetime.combine(target_date, datetime.min.time())
            target_end = datetime.combine(target_date + timedelta(days=1), datetime.min.time())
            query = query.filter(
                NewsArticle.crawled_at >= target_start,
                NewsArticle.crawled_at < target_end,
            )
        elif before_date:
            query = query.filter(NewsArticle.crawled_at < datetime.combine(before_date, datetime.min.time()))

        # Exclude already archived articles
        archived_article_ids = (
            self.session.query(RawHtmlArchive.article_id)
            .filter(RawHtmlArchive.status == ArchiveStatus.ARCHIVED)
        )
        query = query.filter(~NewsArticle.id.in_(archived_article_ids))

        # Get articles to archive
        articles = query.all()

        if not articles:
            return ArchiveResult(
                source=source,
                archived_count=0,
                freed_space_mb=0.0,
                archive_path="",
            )

        # Determine archive path
        now = datetime.utcnow()
        month_str = now.strftime("%Y-%m")
        archive_dir = self.archive_base_path / "raw_html" / source / month_str
        archive_dir.mkdir(parents=True, exist_ok=True)

        # Find next batch number
        next_batch = self._get_next_batch_number(archive_dir)

        # Archive in batches
        total_original_size = 0
        total_compressed_size = 0
        archived_count = 0

        for i in range(0, len(articles), self.batch_size):
            batch_articles = articles[i : i + self.batch_size]
            batch_num = next_batch + (i // self.batch_size)
            batch_filename = f"batch_{batch_num:03d}.json.gz"
            batch_path = archive_dir / batch_filename

            # Prepare batch data
            batch_data = {
                "articles": [
                    {
                        "article_id": article.id,
                        "url_hash": article.url_hash,
                        "raw_html": article.raw_html,
                    }
                    for article in batch_articles
                ]
            }

            # Calculate original size
            batch_original_size = sum(
                len(article.raw_html.encode("utf-8")) for article in batch_articles
            )
            total_original_size += batch_original_size

            # Write compressed file
            json_bytes = json.dumps(batch_data, ensure_ascii=False).encode("utf-8")
            with gzip.open(batch_path, "wt", encoding="utf-8") as f:
                json.dump(batch_data, f, ensure_ascii=False)

            # Get compressed size
            compressed_size = batch_path.stat().st_size
            total_compressed_size += compressed_size

            # Create archive records and clear raw_html
            for article in batch_articles:
                # Create archive record
                archive_record = RawHtmlArchive(
                    article_id=article.id,
                    source=source,
                    archive_path=str(batch_path),
                    status=ArchiveStatus.ARCHIVED,
                    original_size=len(article.raw_html.encode("utf-8")),
                    compressed_size=compressed_size // len(batch_articles),  # Approximate
                    archived_at=now,
                )
                self.session.add(archive_record)

                # Clear raw_html from article
                article.raw_html = None
                archived_count += 1

            # Update manifest
            self._update_manifest(archive_dir, batch_filename, batch_articles)

        self.session.commit()

        logger.info(
            f"Archived {archived_count} articles for {source}. "
            f"Freed {total_original_size / 1024 / 1024:.2f} MB"
        )

        return ArchiveResult(
            source=source,
            archived_count=archived_count,
            freed_space_mb=total_original_size / 1024 / 1024,
            archive_path=str(archive_dir),
        )

    def archive_all_sources(
        self,
        before_date: date | None = None,
    ) -> list[ArchiveResult]:
        """
        Archive raw_html for all sources in additive mode.

        Args:
            before_date: Archive articles crawled before this date

        Returns:
            List of ArchiveResult for each source
        """
        sources = self.get_all_sources()
        results = []

        for source in sources:
            try:
                result = self.archive_source(source=source, before_date=before_date)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to archive {source}: {e}")
                results.append(
                    ArchiveResult(
                        source=source,
                        archived_count=0,
                        freed_space_mb=0.0,
                        archive_path=f"Error: {str(e)}",
                    )
                )

        return results

    def restore_raw_html(self, article_ids: list[int]) -> dict:
        """
        Restore raw_html from archive for specified articles.

        Args:
            article_ids: List of article IDs to restore

        Returns:
            Dict with restored_count and failed_count
        """
        restored_count = 0
        failed_count = 0

        # Get archive records for these articles
        archive_records = (
            self.session.query(RawHtmlArchive)
            .filter(
                RawHtmlArchive.article_id.in_(article_ids),
                RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
            )
            .all()
        )

        # Group by archive path for efficient reading
        records_by_path: dict[str, list[RawHtmlArchive]] = {}
        for record in archive_records:
            if record.archive_path not in records_by_path:
                records_by_path[record.archive_path] = []
            records_by_path[record.archive_path].append(record)

        # Process each archive file
        for archive_path, records in records_by_path.items():
            try:
                with gzip.open(archive_path, "rt", encoding="utf-8") as f:
                    batch_data = json.load(f)

                # Build lookup of article_id -> raw_html
                html_lookup = {
                    item["article_id"]: item["raw_html"]
                    for item in batch_data.get("articles", [])
                }

                # Restore raw_html for each record
                for record in records:
                    if record.article_id in html_lookup:
                        article = (
                            self.session.query(NewsArticle)
                            .filter(NewsArticle.id == record.article_id)
                            .first()
                        )
                        if article:
                            article.raw_html = html_lookup[record.article_id]
                            record.status = ArchiveStatus.ACTIVE
                            restored_count += 1
                        else:
                            failed_count += 1
                    else:
                        failed_count += 1

            except Exception as e:
                logger.error(f"Failed to read archive {archive_path}: {e}")
                failed_count += len(records)

        self.session.commit()
        return {"restored_count": restored_count, "failed_count": failed_count}

    def get_archive_info(self, source: str) -> ArchiveInfo:
        """Get archive information for a source."""
        archive_dir = self.archive_base_path / "raw_html" / source

        total_batches = 0
        total_size = 0
        months = []

        if archive_dir.exists():
            for month_dir in archive_dir.iterdir():
                if month_dir.is_dir():
                    months.append(month_dir.name)
                    for batch_file in month_dir.glob("batch_*.json.gz"):
                        total_batches += 1
                        total_size += batch_file.stat().st_size

        # Get total archived count from database
        archived_count = (
            self.session.query(func.count(RawHtmlArchive.id))
            .filter(
                RawHtmlArchive.source == source,
                RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
            )
            .scalar()
        ) or 0

        return ArchiveInfo(
            source=source,
            total_batches=total_batches,
            total_archived_articles=archived_count,
            total_size_mb=total_size / 1024 / 1024,
            months=sorted(months),
        )

    def get_raw_html_from_archive(self, article_id: int) -> str | None:
        """
        Get raw_html for an article from archive.

        Args:
            article_id: The article ID

        Returns:
            Raw HTML string or None if not found
        """
        # Find archive record
        record = (
            self.session.query(RawHtmlArchive)
            .filter(
                RawHtmlArchive.article_id == article_id,
                RawHtmlArchive.status == ArchiveStatus.ARCHIVED,
            )
            .first()
        )

        if not record or not record.archive_path:
            return None

        try:
            with gzip.open(record.archive_path, "rt", encoding="utf-8") as f:
                batch_data = json.load(f)

            for item in batch_data.get("articles", []):
                if item["article_id"] == article_id:
                    return item["raw_html"]

        except Exception as e:
            logger.error(f"Failed to read archive for article {article_id}: {e}")

        return None

    def get_all_sources(self) -> list[str]:
        """Get list of all news sources."""
        sources = (
            self.session.query(NewsArticle.source)
            .distinct()
            .all()
        )
        return [source for (source,) in sources]

    def _get_next_batch_number(self, archive_dir: Path) -> int:
        """Get the next batch number for the archive directory."""
        existing_batches = list(archive_dir.glob("batch_*.json.gz"))
        if not existing_batches:
            return 1

        max_num = 0
        for batch_file in existing_batches:
            try:
                num = int(batch_file.stem.replace("batch_", "").replace(".json", ""))
                max_num = max(max_num, num)
            except ValueError:
                continue

        return max_num + 1

    def _update_manifest(
        self,
        archive_dir: Path,
        batch_filename: str,
        articles: list[NewsArticle],
    ) -> None:
        """Update or create manifest.json for the archive directory."""
        manifest_path = archive_dir / "manifest.json"

        # Load existing manifest or create new one
        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        else:
            manifest = {
                "source": articles[0].source if articles else "",
                "month": archive_dir.name,
                "batches": [],
            }

        # Add new batch info
        manifest["batches"].append(
            {
                "filename": batch_filename,
                "article_ids": [article.id for article in articles],
                "count": len(articles),
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
        )

        # Write updated manifest
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
