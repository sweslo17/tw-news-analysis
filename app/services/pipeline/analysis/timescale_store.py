"""TimescaleDB result storage for LLM analysis output."""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.models import NewsArticle
from .schemas import NewsAnalysisResult

from .base_provider import AnalysisResponse, parse_article_id


@dataclass
class StoreFailure:
    """A single article storage failure."""

    article_id: int
    error_message: str
    is_transient: bool  # True = connection issue, False = data error


class TimescaleStore:
    """Persist LLM analysis results to TimescaleDB.

    One transaction per article — failures are logged and skipped
    so a single bad record never blocks the rest of the batch.
    """

    def __init__(self, db_url: str | None = None):
        url = db_url or settings.timescale_url
        if not url:
            raise ValueError("timescale_url is not configured")

        self._engine = create_engine(url, pool_pre_ping=True, pool_recycle=300)
        self._session_factory = sessionmaker(bind=self._engine)

    # ── Public API ────────────────────────────────────────────

    def store_batch(
        self,
        articles_map: dict[int, NewsArticle],
        responses: list[AnalysisResponse],
    ) -> tuple[int, list[StoreFailure]]:
        """Store a batch of successful analysis responses.

        Returns:
            (stored_count, failures) where failures distinguishes
            transient (connection) vs data (enum/CHECK) errors.
        """
        stored = 0
        failures: list[StoreFailure] = []

        for resp in responses:
            article_id = parse_article_id(resp.custom_id)
            if article_id is None:
                logger.warning(f"Cannot parse article_id from: {resp.custom_id}")
                continue

            article = articles_map.get(article_id)
            if article is None:
                logger.warning(f"Article {article_id} not in articles_map, skipping")
                failures.append(StoreFailure(article_id, "article not found in articles_map", False))
                continue

            if not resp.result_json:
                logger.warning(f"Article {article_id} has no result_json, skipping")
                failures.append(StoreFailure(article_id, "no result_json", False))
                continue

            try:
                analysis = NewsAnalysisResult.model_validate_json(resp.result_json)
            except Exception as e:
                msg = f"JSON parse failed: {e}"
                logger.warning(f"Article {article_id} {msg}")
                failures.append(StoreFailure(article_id, msg, False))
                continue

            try:
                self._store_single_article(article, analysis)
                stored += 1
            except OperationalError as e:
                # Connection / timeout — transient, retry storage only
                msg = f"DB connection error: {e}"
                logger.error(f"Article {article_id} {msg}")
                failures.append(StoreFailure(article_id, msg, True))
            except Exception as e:
                # Data error (CHECK violation, etc.) — needs LLM re-analysis
                msg = f"DB data error: {e}"
                logger.error(f"Article {article_id} {msg}")
                failures.append(StoreFailure(article_id, msg, False))

        logger.info(
            f"TimescaleDB store complete: {stored} stored, {len(failures)} failed"
        )
        return stored, failures

    # ── Per-article storage ───────────────────────────────────

    def _store_single_article(
        self, article: NewsArticle, analysis: NewsAnalysisResult
    ) -> None:
        """Insert one article and all related records in a single transaction."""
        session = self._session_factory()
        try:
            published_at = article.published_at or article.crawled_at
            # Ensure timezone-aware for TIMESTAMPTZ — source SQLite stores as naive
            if published_at and published_at.tzinfo is None:
                published_at = published_at.replace(tzinfo=timezone.utc)

            # Dedup check — TimescaleDB hypertable cannot have unique on external_id
            if self._article_exists(session, article.url_hash, published_at):
                logger.debug(
                    f"Article already exists (external_id={article.url_hash}), skipping"
                )
                return

            # 1. INSERT article
            article_uuid = self._insert_article(
                session, article, analysis, published_at
            )

            # 2. Upsert entities → {name_normalized: uuid}
            entity_map = self._upsert_entities(session, analysis)

            # 3. Upsert events → {name_normalized: uuid}
            event_map = self._upsert_events(session, analysis)

            # 4. INSERT sub_events → {(event_name, sub_event_name): uuid}
            sub_event_map = self._insert_sub_events(session, analysis, event_map)

            # 5. INSERT article_entities
            self._insert_article_entities(
                session, article_uuid, published_at, analysis, entity_map
            )

            # 6. INSERT article_events
            self._insert_article_events(
                session, article_uuid, published_at, analysis,
                event_map, sub_event_map,
            )

            # 7. Upsert entity_relations
            self._upsert_entity_relations(session, analysis, entity_map)

            # 8. Upsert event_relations
            self._upsert_event_relations(session, analysis, entity_map, event_map)

            session.commit()
            logger.debug(f"Stored article {article.id} → {article_uuid}")

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── Dedup ─────────────────────────────────────────────────

    def _article_exists(
        self, session: Session, external_id: str, published_at: datetime
    ) -> bool:
        """Check if article with same external_id exists (scan ±7 days)."""
        min_ts = published_at - timedelta(days=7)
        max_ts = published_at + timedelta(days=7)
        row = session.execute(
            text(
                "SELECT 1 FROM articles "
                "WHERE external_id = :eid "
                "AND published_at >= :min_ts AND published_at <= :max_ts "
                "LIMIT 1"
            ),
            {"eid": external_id, "min_ts": min_ts, "max_ts": max_ts},
        ).fetchone()
        return row is not None

    # ── Article INSERT ────────────────────────────────────────

    def _insert_article(
        self,
        session: Session,
        article: NewsArticle,
        analysis: NewsAnalysisResult,
        published_at: datetime,
    ) -> str:
        """INSERT into articles, return the generated UUID."""
        keywords = self._parse_keywords(article.tags)

        row = session.execute(
            text("""
                INSERT INTO articles (
                    published_at, external_id, url, title, source, author,
                    keywords_original,
                    sentiment_polarity, sentiment_intensity, sentiment_tone,
                    framing_angle, framing_narrative_type,
                    is_exclusive, is_opinion, has_update, key_claims, virality_score,
                    category_normalized
                ) VALUES (
                    :published_at, :external_id, :url, :title, :source, :author,
                    :keywords_original,
                    :sentiment_polarity, :sentiment_intensity, :sentiment_tone,
                    :framing_angle, :framing_narrative_type,
                    :is_exclusive, :is_opinion, :has_update, :key_claims, :virality_score,
                    :category_normalized
                )
                RETURNING id
            """),
            {
                "published_at": published_at,
                "external_id": article.url_hash,
                "url": article.url,
                "title": article.title,
                "source": article.source,
                "author": article.author,
                "keywords_original": keywords,
                "sentiment_polarity": analysis.sentiment.polarity,
                "sentiment_intensity": analysis.sentiment.intensity,
                "sentiment_tone": analysis.sentiment.tone.value,
                "framing_angle": analysis.framing.angle,
                "framing_narrative_type": analysis.framing.narrative_type.value,
                "is_exclusive": analysis.signals.is_exclusive,
                "is_opinion": analysis.signals.is_opinion,
                "has_update": analysis.signals.has_update,
                "key_claims": analysis.signals.key_claims,
                "virality_score": analysis.signals.virality_score,
                "category_normalized": analysis.category_normalized.value,
            },
        )
        return str(row.fetchone()[0])

    # ── Entities ──────────────────────────────────────────────

    def _upsert_entities(
        self, session: Session, analysis: NewsAnalysisResult
    ) -> dict[str, str]:
        """Upsert all entities, return {name_normalized: uuid}."""
        entity_map: dict[str, str] = {}
        for ent in analysis.entities:
            alias = ent.name if ent.name != ent.name_normalized else None
            row = session.execute(
                text("SELECT upsert_entity(:name, :type, :alias)"),
                {
                    "name": ent.name_normalized,
                    "type": ent.type.value,
                    "alias": alias,
                },
            ).fetchone()
            entity_map[ent.name_normalized] = str(row[0])
        return entity_map

    # ── Events ────────────────────────────────────────────────

    def _upsert_events(
        self, session: Session, analysis: NewsAnalysisResult
    ) -> dict[str, str]:
        """Upsert all events, return {name_normalized: uuid}."""
        event_map: dict[str, str] = {}
        for evt in analysis.events:
            row = session.execute(
                text("SELECT upsert_event(:topic, :name, :type, :tags)"),
                {
                    "topic": evt.topic_normalized,
                    "name": evt.name_normalized,
                    "type": evt.type.value,
                    "tags": evt.tags,
                },
            ).fetchone()
            event_map[evt.name_normalized] = str(row[0])
        return event_map

    # ── Sub-events ────────────────────────────────────────────

    def _insert_sub_events(
        self,
        session: Session,
        analysis: NewsAnalysisResult,
        event_map: dict[str, str],
    ) -> dict[tuple[str, str], str]:
        """INSERT sub_events, return {(event_name, sub_event_name): uuid}."""
        sub_event_map: dict[tuple[str, str], str] = {}
        for evt in analysis.events:
            if not evt.sub_event_normalized:
                continue
            event_id = event_map.get(evt.name_normalized)
            if not event_id:
                continue

            event_time = self._parse_event_date(evt.event_time)

            row = session.execute(
                text("""
                    INSERT INTO sub_events (event_id, name_normalized, event_time)
                    VALUES (:event_id, :name, :event_time)
                    ON CONFLICT (event_id, name_normalized) DO UPDATE
                        SET event_time = COALESCE(EXCLUDED.event_time, sub_events.event_time)
                    RETURNING id
                """),
                {
                    "event_id": event_id,
                    "name": evt.sub_event_normalized,
                    "event_time": event_time,
                },
            ).fetchone()
            sub_event_map[(evt.name_normalized, evt.sub_event_normalized)] = str(row[0])
        return sub_event_map

    # ── Article-Entity junction ───────────────────────────────

    def _insert_article_entities(
        self,
        session: Session,
        article_uuid: str,
        published_at: datetime,
        analysis: NewsAnalysisResult,
        entity_map: dict[str, str],
    ) -> None:
        for ent in analysis.entities:
            entity_id = entity_map.get(ent.name_normalized)
            if not entity_id:
                continue
            session.execute(
                text("""
                    INSERT INTO article_entities (
                        published_at, article_id, entity_id,
                        name_in_article, role, sentiment_toward
                    ) VALUES (
                        :published_at, :article_id, :entity_id,
                        :name_in_article, :role, :sentiment_toward
                    )
                    ON CONFLICT (published_at, article_id, entity_id) DO NOTHING
                """),
                {
                    "published_at": published_at,
                    "article_id": article_uuid,
                    "entity_id": entity_id,
                    "name_in_article": ent.name,
                    "role": ent.role.value,
                    "sentiment_toward": ent.sentiment_toward,
                },
            )

    # ── Article-Event junction ────────────────────────────────

    def _insert_article_events(
        self,
        session: Session,
        article_uuid: str,
        published_at: datetime,
        analysis: NewsAnalysisResult,
        event_map: dict[str, str],
        sub_event_map: dict[tuple[str, str], str],
    ) -> None:
        for evt in analysis.events:
            event_id = event_map.get(evt.name_normalized)
            if not event_id:
                continue

            sub_event_id = None
            if evt.sub_event_normalized:
                sub_event_id = sub_event_map.get(
                    (evt.name_normalized, evt.sub_event_normalized)
                )

            event_time = self._parse_event_date(evt.event_time)

            session.execute(
                text("""
                    INSERT INTO article_events (
                        published_at, article_id, event_id, sub_event_id,
                        is_main, article_type, event_time, temporal_cues
                    ) VALUES (
                        :published_at, :article_id, :event_id, :sub_event_id,
                        :is_main, :article_type, :event_time, :temporal_cues
                    )
                    ON CONFLICT (published_at, article_id, event_id) DO NOTHING
                """),
                {
                    "published_at": published_at,
                    "article_id": article_uuid,
                    "event_id": event_id,
                    "sub_event_id": sub_event_id,
                    "is_main": evt.is_main,
                    "article_type": evt.article_type.value,
                    "event_time": event_time,
                    "temporal_cues": evt.temporal_cues,
                },
            )

    # ── Entity relations ──────────────────────────────────────

    def _upsert_entity_relations(
        self,
        session: Session,
        analysis: NewsAnalysisResult,
        entity_map: dict[str, str],
    ) -> None:
        for rel in analysis.entity_relations:
            source_id = entity_map.get(rel.source)
            target_id = entity_map.get(rel.target)
            if not source_id or not target_id:
                logger.debug(
                    f"Skipping entity_relation: {rel.source} → {rel.target} "
                    f"(missing entity)"
                )
                continue
            session.execute(
                text(
                    "SELECT upsert_entity_relation(:source, :target, :type)"
                ),
                {
                    "source": source_id,
                    "target": target_id,
                    "type": rel.type.value,
                },
            )

    # ── Event relations ───────────────────────────────────────

    def _upsert_event_relations(
        self,
        session: Session,
        analysis: NewsAnalysisResult,
        entity_map: dict[str, str],
        event_map: dict[str, str],
    ) -> None:
        for rel in analysis.event_relations:
            entity_id = entity_map.get(rel.entity)
            event_id = event_map.get(rel.event)
            if not entity_id or not event_id:
                logger.debug(
                    f"Skipping event_relation: {rel.entity} → {rel.event} "
                    f"(missing entity/event)"
                )
                continue
            session.execute(
                text(
                    "SELECT upsert_event_relation(:entity, :event, :type)"
                ),
                {
                    "entity": entity_id,
                    "event": event_id,
                    "type": rel.type.value,
                },
            )

    # ── Deletion ──────────────────────────────────────────────

    def delete_by_external_ids(self, external_ids: list[str]) -> int:
        """Delete articles and junction data from TimescaleDB by external_id.

        Deletes: article_entities, article_events (by article_id),
        then articles themselves. Does NOT delete shared entities/events/relations.

        Returns number of articles deleted.
        """
        if not external_ids:
            return 0

        session = self._session_factory()
        try:
            # Find article UUIDs + published_at for these external_ids
            rows = session.execute(
                text(
                    "SELECT id, published_at FROM articles "
                    "WHERE external_id = ANY(:eids)"
                ),
                {"eids": external_ids},
            ).fetchall()

            if not rows:
                logger.debug("No matching articles found in TimescaleDB")
                return 0

            article_uuids = [str(r[0]) for r in rows]

            # Delete junction tables first (need article_id for hypertable)
            session.execute(
                text(
                    "DELETE FROM article_entities "
                    "WHERE article_id = ANY(:ids::uuid[])"
                ),
                {"ids": article_uuids},
            )
            session.execute(
                text(
                    "DELETE FROM article_events "
                    "WHERE article_id = ANY(:ids::uuid[])"
                ),
                {"ids": article_uuids},
            )

            # Delete articles
            result = session.execute(
                text(
                    "DELETE FROM articles "
                    "WHERE id = ANY(:ids::uuid[])"
                ),
                {"ids": article_uuids},
            )
            deleted = result.rowcount

            session.commit()
            logger.info(f"TimescaleDB: deleted {deleted} articles and junction data")
            return deleted

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse_event_date(date_str: str | None) -> date | None:
        """Parse 'YYYY-MM-DD' string to date, or None."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _parse_keywords(tags: str | None) -> list[str]:
        """Parse tags string to list. Supports JSON array or comma-separated."""
        if not tags:
            return []
        # Try JSON array first (e.g. '["tag1", "tag2"]')
        try:
            parsed = json.loads(tags)
            if isinstance(parsed, list):
                return [str(t).strip() for t in parsed if str(t).strip()]
        except (json.JSONDecodeError, TypeError):
            pass
        # Fallback: comma-separated (e.g. "一鍵看世界,美國,川普,白宮")
        return [t.strip() for t in tags.split(",") if t.strip()]
