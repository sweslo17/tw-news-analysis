# LLM Analysis Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the LLM Analysis pipeline stage using GPT-4o-mini + OpenAI Batch API with structured output, article tracking, and CLI management commands.

**Architecture:** Analysis provider is abstracted behind `BaseAnalysisProvider`. `OpenAIBatchProvider` implements the OpenAI Batch API flow (upload JSONL → create batch → poll → retrieve). `LLMAnalysisService` orchestrates the flow and manages `ArticleAnalysisTracking` records. Pipeline orchestrator wires the stage in. CLI exposes `analysis` subcommands.

**Tech Stack:** OpenAI Python SDK v2.15+, Pydantic V2 structured output, SQLAlchemy 2.0, Typer CLI, loguru logging.

---

### Task 1: Create prompts module

**Files:**
- Create: `prompts/__init__.py`
- Create: `prompts/system_prompt.py`

**Step 1: Create `prompts/__init__.py`**

```python
```

(Empty `__init__.py`)

**Step 2: Create `prompts/system_prompt.py`**

```python
SYSTEM_PROMPT = """# 角色
你是專業的台灣新聞結構化分析器，負責將新聞文章轉換為標準化 JSON 格式。

# 核心原則：名稱歸一化
本系統需跨新聞聚合分析，「名稱歸一化」極為重要。

## 人物歸一化規則
- 去除所有頭銜（總統、前市長、董事長、立委、議員、部長等）
- 使用本名全名，不用暱稱
- 外國人名使用最常見的中文譯名
- 範例：
  - 「柯P」「柯市長」「前台北市長柯文哲」→「柯文哲」
  - 「小英」「蔡總統」「總統蔡英文」→「蔡英文」
  - 「郭董」「郭台銘董事長」→「郭台銘」
  - 「川普」「乙川普」→「川普」
  - 「習大大」「習主席」→「習近平」

## 組織歸一化規則
- 使用正式全名，不用簡稱或英文縮寫
- 範例：
  - 「民眾黨」「白營」「TPP」→「台灣民眾黨」
  - 「國民黨」「藍營」「KMT」→「中國國民黨」
  - 「民進黨」「綠營」「DPP」→「民主進步黨」
  - 「台積電」「TSMC」→「台灣積體電路製造股份有限公司」
  - 「北市府」→「臺北市政府」

## 事件歸一化規則
- 去除時間詞（今、最新、昨日、稍早）
- 去除情緒詞（爆、驚傳、震撼、竟然）
- 去除媒體主觀詞（獨家、直擊、踢爆）
- 使用「主體+核心事件」格式（3-8字）
- 範例：
  - 「京華城弊案最新」「柯文哲京華城案」→「京華城案」
  - 「賴清德今出訪」→「賴清德出訪」
  - 「台積電熊本廠動工」→「台積電熊本設廠」

## 主題歸一化規則
- 主題為事件上層分類（2-6字）
- 範例：
  - 「京華城案」「政治獻金案」的主題→「柯文哲司法案件」
  - 「賴清德出訪」「蕭美琴訪美」的主題→「臺灣外交」

# 欄位定義

## sentiment
- polarity：-10（極負面）到+10（極正面），0為中性
- intensity：1（平淡）到10（強烈）
- tone：neutral/supportive/critical/sensational/analytical

## framing
- angle：報導切入角度（2-5字）
- narrative_type：conflict/human_interest/economic/moral/attribution/procedural

## entities
- name：原文名稱
- name_normalized：歸一化名稱
- type：person/organization/location/product/concept
- role：subject/object/source/mentioned
- sentiment_toward：報導對該實體的態度（-10到+10）

## events
- topic_normalized：主題名（2-6字）
- name_normalized：事件名（3-8字）
- sub_event_normalized：子事件名（可null）
- tags：關鍵標籤（用歸一化名稱）
- type：policy/scandal/legal/election/disaster/protest/business/international/society/entertainment/sports/technology/health/environment/crime/other
- is_main：是否主要事件
- event_time：YYYY-MM-DD 或 null
- article_type：breaking/first_report/follow_up/retrospective/analysis/standard
- temporal_cues：時間訊號詞

## entity_relations
- source/target：實體的 name_normalized
- type：supports/opposes/member_of/leads/allied_with/conflicts_with/related_to

## event_relations
- entity：實體的 name_normalized
- event：事件的 name_normalized
- type：accused_in/victim_in/investigates/comments_on/causes/responds_to/involved_in

## signals
- is_exclusive：是否獨家
- is_opinion：是否評論/社論
- has_update：是否有最新進展
- key_claims：關鍵主張（最多3個）
- virality_score：傳播潛力（1-10）

## category_normalized
politics/business/technology/entertainment/sports/society/international/local/opinion/lifestyle/health/education/environment/crime/other

# 處理原則
1. 使用台灣繁體中文
2. 嚴格遵守歸一化規則
3. sentiment_toward 是「報導對實體的態度」
4. 空陣列輸出 []
5. 不認識的人名保留原文作為 name_normalized"""


USER_PROMPT_TEMPLATE = """分析以下新聞：

<news>
標題：{title}
內容：{content}
原始分類：{category}
作者：{author}
媒體：{media}
發稿時間：{published_at}
</news>"""
```

**Step 3: Verify import**

Run: `poetry run python -c "from prompts.system_prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE; print(f'System prompt: {len(SYSTEM_PROMPT)} chars')"`
Expected: prints character count (~1500+)

**Step 4: Commit**

```bash
git add prompts/
git commit -m "feat: add LLM analysis prompts"
```

---

### Task 2: Create schemas module

**Files:**
- Create: `schemas/__init__.py`
- Create: `schemas/llm_output.py`

**Step 1: Create `schemas/__init__.py`**

```python
```

(Empty `__init__.py`)

**Step 2: Create `schemas/llm_output.py`**

```python
from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class Tone(str, Enum):
    neutral = "neutral"
    supportive = "supportive"
    critical = "critical"
    sensational = "sensational"
    analytical = "analytical"


class NarrativeType(str, Enum):
    conflict = "conflict"
    human_interest = "human_interest"
    economic = "economic"
    moral = "moral"
    attribution = "attribution"
    procedural = "procedural"


class EntityType(str, Enum):
    person = "person"
    organization = "organization"
    location = "location"
    product = "product"
    concept = "concept"


class EntityRole(str, Enum):
    subject = "subject"
    object = "object"
    source = "source"
    mentioned = "mentioned"


class EventType(str, Enum):
    policy = "policy"
    scandal = "scandal"
    legal = "legal"
    election = "election"
    disaster = "disaster"
    protest = "protest"
    business = "business"
    international = "international"
    society = "society"
    entertainment = "entertainment"
    sports = "sports"
    technology = "technology"
    health = "health"
    environment = "environment"
    crime = "crime"
    other = "other"


class ArticleType(str, Enum):
    breaking = "breaking"
    first_report = "first_report"
    follow_up = "follow_up"
    retrospective = "retrospective"
    analysis = "analysis"
    standard = "standard"


class EntityRelationType(str, Enum):
    supports = "supports"
    opposes = "opposes"
    member_of = "member_of"
    leads = "leads"
    allied_with = "allied_with"
    conflicts_with = "conflicts_with"
    related_to = "related_to"


class EventRelationType(str, Enum):
    accused_in = "accused_in"
    victim_in = "victim_in"
    investigates = "investigates"
    comments_on = "comments_on"
    causes = "causes"
    responds_to = "responds_to"
    involved_in = "involved_in"


class CategoryNormalized(str, Enum):
    politics = "politics"
    business = "business"
    technology = "technology"
    entertainment = "entertainment"
    sports = "sports"
    society = "society"
    international = "international"
    local = "local"
    opinion = "opinion"
    lifestyle = "lifestyle"
    health = "health"
    education = "education"
    environment = "environment"
    crime = "crime"
    other = "other"


# Sub-structures

class Sentiment(BaseModel):
    polarity: int = Field(..., ge=-10, le=10)
    intensity: int = Field(..., ge=1, le=10)
    tone: Tone


class Framing(BaseModel):
    angle: str = Field(..., min_length=2, max_length=10)
    narrative_type: NarrativeType


class Entity(BaseModel):
    name: str
    name_normalized: str
    type: EntityType
    role: EntityRole
    sentiment_toward: int = Field(..., ge=-10, le=10)


class Event(BaseModel):
    topic_normalized: str = Field(..., min_length=2, max_length=12)
    name_normalized: str = Field(..., min_length=3, max_length=16)
    sub_event_normalized: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    type: EventType
    is_main: bool
    event_time: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    article_type: ArticleType
    temporal_cues: list[str] = Field(default_factory=list)


class EntityRelation(BaseModel):
    source: str
    target: str
    type: EntityRelationType


class EventRelation(BaseModel):
    entity: str
    event: str
    type: EventRelationType


class Signals(BaseModel):
    is_exclusive: bool = False
    is_opinion: bool = False
    has_update: bool = False
    key_claims: list[str] = Field(default_factory=list, max_length=3)
    virality_score: int = Field(..., ge=1, le=10)


# Main structure

class NewsAnalysisResult(BaseModel):
    """LLM structured output for news analysis."""
    sentiment: Sentiment
    framing: Framing
    entities: list[Entity] = Field(default_factory=list)
    events: list[Event] = Field(default_factory=list)
    entity_relations: list[EntityRelation] = Field(default_factory=list)
    event_relations: list[EventRelation] = Field(default_factory=list)
    signals: Signals
    category_normalized: CategoryNormalized
```

**Step 3: Verify schema generates valid JSON schema**

Run: `poetry run python -c "from schemas.llm_output import NewsAnalysisResult; import json; print(json.dumps(NewsAnalysisResult.model_json_schema(), indent=2)[:200])"`
Expected: prints JSON schema fragment

**Step 4: Commit**

```bash
git add schemas/
git commit -m "feat: add LLM analysis output schema"
```

---

### Task 3: Add DB models and config

**Files:**
- Modify: `app/models.py` — add `AnalysisStatus` enum, `ArticleAnalysisTracking` model, `batch_id` to `PipelineRun`
- Modify: `app/config.py` — add `llm_analysis_poll_interval`

**Step 1: Add `AnalysisStatus` enum to `app/models.py`**

Insert after the last existing enum (find `class FilterRuleType`), add:

```python
class AnalysisStatus(str, enum.Enum):
    """Status of article analysis."""

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
```

**Step 2: Add `batch_id` column to `PipelineRun`**

In the `PipelineRun` class, after `force_included_count`, add:

```python
    # Batch processing
    batch_id = Column(String(200), nullable=True)  # OpenAI batch ID for resume
```

**Step 3: Add `ArticleAnalysisTracking` model**

Insert after the `ArticleAnalysisResult` class:

```python
class ArticleAnalysisTracking(Base):
    """Track which articles have been analyzed by LLM."""

    __tablename__ = "article_analysis_tracking"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(
        Integer, ForeignKey("news_articles.id"), nullable=False, index=True
    )
    batch_id = Column(String(200), nullable=False, index=True)
    status = Column(
        Enum(AnalysisStatus), default=AnalysisStatus.PENDING, nullable=False, index=True
    )
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def __repr__(self) -> str:
        return f"<ArticleAnalysisTracking(article_id={self.article_id}, status={self.status})>"
```

**Step 4: Add config setting to `app/config.py`**

In the `Settings` class, after `llm_max_retries`, add:

```python
    # LLM Analysis Settings
    llm_analysis_poll_interval: int = 30  # Seconds between batch status checks
    llm_analysis_max_wait: int = 7200  # Max seconds to wait for batch (2 hours)
```

**Step 5: Verify models load**

Run: `poetry run python -c "from app.models import ArticleAnalysisTracking, AnalysisStatus, PipelineRun; print('Models OK')"`
Expected: `Models OK`

**Step 6: Commit**

```bash
git add app/models.py app/config.py
git commit -m "feat: add analysis tracking model and batch_id to PipelineRun"
```

---

### Task 4: Create analysis provider abstraction

**Files:**
- Create: `app/services/pipeline/analysis/__init__.py`
- Create: `app/services/pipeline/analysis/base_provider.py`

**Step 1: Create `__init__.py`**

```python
```

(Empty)

**Step 2: Create `base_provider.py`**

```python
"""Abstract base for LLM analysis providers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

from app.models import NewsArticle


class BatchStatus(str, Enum):
    """Status of a batch job."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


@dataclass
class AnalysisRequest:
    """A single article analysis request."""

    custom_id: str  # "article_{article_id}"
    article: NewsArticle


@dataclass
class AnalysisResponse:
    """A single article analysis response."""

    custom_id: str
    success: bool
    result_json: str | None = None  # Raw JSON string of the analysis
    error_message: str | None = None


@dataclass
class BatchStatusResult:
    """Result of checking batch status."""

    status: BatchStatus
    total: int = 0
    completed: int = 0
    failed: int = 0


class BaseAnalysisProvider(ABC):
    """Abstract base class for analysis providers.

    Implement submit_batch, check_batch_status, and retrieve_results
    to support any batch-capable LLM API.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""

    @abstractmethod
    async def submit_batch(self, requests: list[AnalysisRequest]) -> str:
        """Submit a batch of analysis requests.

        Args:
            requests: List of analysis requests.

        Returns:
            batch_id for tracking.
        """

    @abstractmethod
    async def check_batch_status(self, batch_id: str) -> BatchStatusResult:
        """Check the status of a batch.

        Args:
            batch_id: The batch identifier.

        Returns:
            BatchStatusResult with current status and counts.
        """

    @abstractmethod
    async def retrieve_results(self, batch_id: str) -> list[AnalysisResponse]:
        """Retrieve results from a completed batch.

        Args:
            batch_id: The batch identifier.

        Returns:
            List of AnalysisResponse, one per request.
        """
```

**Step 3: Verify import**

Run: `poetry run python -c "from app.services.pipeline.analysis.base_provider import BaseAnalysisProvider, BatchStatus; print('Base provider OK')"`
Expected: `Base provider OK`

**Step 4: Commit**

```bash
git add app/services/pipeline/analysis/
git commit -m "feat: add analysis provider abstraction"
```

---

### Task 5: Implement OpenAI Batch provider

**Files:**
- Create: `app/services/pipeline/analysis/openai_batch_provider.py`

**Step 1: Create `openai_batch_provider.py`**

```python
"""OpenAI Batch API provider for news analysis."""

import json
import tempfile
from io import BytesIO

from loguru import logger
from openai import OpenAI

from app.config import settings
from prompts.system_prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from schemas.llm_output import NewsAnalysisResult
from .base_provider import (
    BaseAnalysisProvider,
    AnalysisRequest,
    AnalysisResponse,
    BatchStatus,
    BatchStatusResult,
)


# Map OpenAI batch status strings to our enum
_STATUS_MAP = {
    "validating": BatchStatus.PENDING,
    "in_progress": BatchStatus.IN_PROGRESS,
    "finalizing": BatchStatus.IN_PROGRESS,
    "completed": BatchStatus.COMPLETED,
    "failed": BatchStatus.FAILED,
    "expired": BatchStatus.EXPIRED,
    "cancelling": BatchStatus.CANCELLING,
    "cancelled": BatchStatus.CANCELLED,
}


class OpenAIBatchProvider(BaseAnalysisProvider):
    """OpenAI Batch API implementation for structured news analysis."""

    def __init__(self, model: str | None = None, api_key: str | None = None):
        self.model = model or settings.llm_model
        self.api_key = api_key or settings.openai_api_key
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")
        self.client = OpenAI(api_key=self.api_key)
        self._json_schema = self._build_json_schema()

    @property
    def name(self) -> str:
        return "openai_batch"

    def _build_json_schema(self) -> dict:
        """Build the JSON schema for structured output from Pydantic model."""
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "news_analysis",
                "strict": True,
                "schema": NewsAnalysisResult.model_json_schema(),
            },
        }

    def _build_request_body(self, request: AnalysisRequest) -> dict:
        """Build a single JSONL request line for the batch."""
        article = request.article
        user_content = USER_PROMPT_TEMPLATE.format(
            title=article.title or "",
            content=article.content or "",
            category=article.category or "",
            author=article.author or "",
            media=article.source or "",
            published_at=(
                article.published_at.isoformat() if article.published_at else ""
            ),
        )

        return {
            "custom_id": request.custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                "response_format": self._json_schema,
                "temperature": 0.1,
            },
        }

    async def submit_batch(self, requests: list[AnalysisRequest]) -> str:
        """Upload JSONL and create an OpenAI batch."""
        # Build JSONL content
        lines = []
        for req in requests:
            line = json.dumps(self._build_request_body(req), ensure_ascii=False)
            lines.append(line)
        jsonl_content = "\n".join(lines).encode("utf-8")

        logger.info(f"Uploading batch with {len(requests)} requests")

        # Upload file
        file_obj = self.client.files.create(
            file=("batch_input.jsonl", BytesIO(jsonl_content)),
            purpose="batch",
        )
        logger.info(f"Uploaded file: {file_obj.id}")

        # Create batch
        batch = self.client.batches.create(
            input_file_id=file_obj.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        logger.info(f"Created batch: {batch.id}")

        return batch.id

    async def check_batch_status(self, batch_id: str) -> BatchStatusResult:
        """Check OpenAI batch status."""
        batch = self.client.batches.retrieve(batch_id)
        status = _STATUS_MAP.get(batch.status, BatchStatus.PENDING)
        counts = batch.request_counts

        return BatchStatusResult(
            status=status,
            total=counts.total if counts else 0,
            completed=counts.completed if counts else 0,
            failed=counts.failed if counts else 0,
        )

    async def retrieve_results(self, batch_id: str) -> list[AnalysisResponse]:
        """Download and parse batch results."""
        batch = self.client.batches.retrieve(batch_id)

        if batch.status != "completed":
            raise RuntimeError(
                f"Batch {batch_id} is not completed (status: {batch.status})"
            )

        responses: list[AnalysisResponse] = []

        # Process successful results
        if batch.output_file_id:
            content = self.client.files.content(batch.output_file_id)
            for line in content.text.strip().split("\n"):
                if not line.strip():
                    continue
                responses.append(self._parse_result_line(line))

        # Process error results
        if batch.error_file_id:
            content = self.client.files.content(batch.error_file_id)
            for line in content.text.strip().split("\n"):
                if not line.strip():
                    continue
                responses.append(self._parse_error_line(line))

        return responses

    def _parse_result_line(self, line: str) -> AnalysisResponse:
        """Parse a single result line from the output file."""
        try:
            data = json.loads(line)
            custom_id = data.get("custom_id", "")
            response = data.get("response", {})
            body = response.get("body", {})

            choices = body.get("choices", [])
            if not choices:
                return AnalysisResponse(
                    custom_id=custom_id,
                    success=False,
                    error_message="No choices in response",
                )

            message_content = choices[0].get("message", {}).get("content", "")

            # Validate with Pydantic
            NewsAnalysisResult.model_validate_json(message_content)

            return AnalysisResponse(
                custom_id=custom_id,
                success=True,
                result_json=message_content,
            )
        except Exception as e:
            custom_id = ""
            try:
                custom_id = json.loads(line).get("custom_id", "")
            except Exception:
                pass
            return AnalysisResponse(
                custom_id=custom_id,
                success=False,
                error_message=f"Parse error: {str(e)}",
            )

    def _parse_error_line(self, line: str) -> AnalysisResponse:
        """Parse a single error line from the error file."""
        try:
            data = json.loads(line)
            custom_id = data.get("custom_id", "")
            error = data.get("response", {}).get("body", {}).get("error", {})
            error_msg = error.get("message", "Unknown error")
            return AnalysisResponse(
                custom_id=custom_id,
                success=False,
                error_message=error_msg,
            )
        except Exception as e:
            return AnalysisResponse(
                custom_id="",
                success=False,
                error_message=f"Error line parse failure: {str(e)}",
            )
```

**Step 2: Verify import**

Run: `poetry run python -c "from app.services.pipeline.analysis.openai_batch_provider import OpenAIBatchProvider; print('OpenAI provider OK')"`
Expected: `OpenAI provider OK` (will warn about API key if not set, but import succeeds)

**Step 3: Commit**

```bash
git add app/services/pipeline/analysis/openai_batch_provider.py
git commit -m "feat: implement OpenAI Batch API provider"
```

---

### Task 6: Rewrite LLM Analysis Service

**Files:**
- Modify: `app/services/pipeline/llm_analysis_service.py` — full rewrite

**Step 1: Replace the entire file**

Replace `app/services/pipeline/llm_analysis_service.py` with:

```python
"""LLM analysis service for the pipeline."""

import asyncio

from loguru import logger
from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    NewsArticle,
    PipelineRun,
    ArticleAnalysisTracking,
    AnalysisStatus,
)
from .analysis.base_provider import (
    BaseAnalysisProvider,
    AnalysisRequest,
    AnalysisResponse,
    BatchStatus,
)
from .analysis.openai_batch_provider import OpenAIBatchProvider


class LLMAnalysisService:
    """Orchestrates LLM-based article analysis with batch processing."""

    def __init__(
        self,
        db: Session,
        provider: BaseAnalysisProvider | None = None,
    ):
        self.db = db
        self._provider = provider

    @property
    def provider(self) -> BaseAnalysisProvider:
        if self._provider is None:
            self._provider = OpenAIBatchProvider()
        return self._provider

    # ── Tracking queries ─────────────────────────────────────

    def get_analyzed_article_ids(self) -> set[int]:
        """Get article IDs that have been successfully analyzed."""
        rows = (
            self.db.query(ArticleAnalysisTracking.article_id)
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.SUCCESS)
            .all()
        )
        return {r[0] for r in rows}

    def get_failed_article_ids(self) -> set[int]:
        """Get article IDs that failed analysis."""
        rows = (
            self.db.query(ArticleAnalysisTracking.article_id)
            .filter(ArticleAnalysisTracking.status == AnalysisStatus.FAILED)
            .all()
        )
        return {r[0] for r in rows}

    def get_tracking_stats(self) -> dict:
        """Get analysis tracking statistics."""
        from sqlalchemy import func

        stats = (
            self.db.query(
                ArticleAnalysisTracking.status,
                func.count(ArticleAnalysisTracking.id),
            )
            .group_by(ArticleAnalysisTracking.status)
            .all()
        )
        result = {"pending": 0, "success": 0, "failed": 0, "total": 0}
        for status, count in stats:
            result[status.value] = count
            result["total"] += count
        return result

    # ── Tracking mutations ───────────────────────────────────

    def _create_tracking_records(
        self, article_ids: list[int], batch_id: str
    ) -> None:
        """Create pending tracking records for a batch."""
        for article_id in article_ids:
            record = ArticleAnalysisTracking(
                article_id=article_id,
                batch_id=batch_id,
                status=AnalysisStatus.PENDING,
            )
            self.db.add(record)
        self.db.commit()
        logger.info(
            f"Created {len(article_ids)} tracking records for batch {batch_id}"
        )

    def _update_tracking_from_responses(
        self, responses: list[AnalysisResponse]
    ) -> tuple[int, int]:
        """Update tracking records from batch responses. Returns (success, failed) counts."""
        success_count = 0
        fail_count = 0

        for resp in responses:
            article_id = self._parse_article_id(resp.custom_id)
            if article_id is None:
                logger.warning(f"Cannot parse article_id from custom_id: {resp.custom_id}")
                fail_count += 1
                continue

            tracking = (
                self.db.query(ArticleAnalysisTracking)
                .filter(
                    ArticleAnalysisTracking.article_id == article_id,
                    ArticleAnalysisTracking.status == AnalysisStatus.PENDING,
                )
                .order_by(ArticleAnalysisTracking.created_at.desc())
                .first()
            )

            if not tracking:
                logger.warning(f"No pending tracking for article {article_id}")
                continue

            if resp.success:
                tracking.status = AnalysisStatus.SUCCESS
                success_count += 1
            else:
                tracking.status = AnalysisStatus.FAILED
                tracking.error_message = resp.error_message
                fail_count += 1
                logger.warning(
                    f"Article {article_id} analysis failed: {resp.error_message}"
                )

        self.db.commit()
        return success_count, fail_count

    def clear_tracking(
        self,
        *,
        all_records: bool = False,
        failed_only: bool = False,
        article_id: int | None = None,
        batch_id: str | None = None,
    ) -> int:
        """Clear tracking records. Returns number of deleted records."""
        query = self.db.query(ArticleAnalysisTracking)

        if all_records:
            pass  # no filter
        elif failed_only:
            query = query.filter(
                ArticleAnalysisTracking.status == AnalysisStatus.FAILED
            )
        elif article_id is not None:
            query = query.filter(
                ArticleAnalysisTracking.article_id == article_id
            )
        elif batch_id is not None:
            query = query.filter(
                ArticleAnalysisTracking.batch_id == batch_id
            )
        else:
            return 0

        count = query.count()
        query.delete(synchronize_session=False)
        self.db.commit()
        logger.info(f"Cleared {count} tracking records")
        return count

    # ── Core analysis flow ───────────────────────────────────

    async def analyze_articles(
        self,
        articles: list[NewsArticle],
        pipeline_run: PipelineRun,
        progress_callback=None,
    ) -> tuple[int, int]:
        """Analyze articles via batch API. Returns (success_count, fail_count).

        Handles:
        - Skipping already-analyzed articles
        - Submitting batch
        - Polling until completion
        - Updating tracking records
        - Resuming from existing batch_id
        """
        # Filter out already analyzed
        analyzed_ids = self.get_analyzed_article_ids()
        to_analyze = [a for a in articles if a.id not in analyzed_ids]

        if not to_analyze:
            logger.info("All articles already analyzed, skipping")
            return 0, 0

        logger.info(
            f"Analyzing {len(to_analyze)} articles "
            f"(skipped {len(articles) - len(to_analyze)} already analyzed)"
        )

        # Check for existing batch (resume)
        batch_id = pipeline_run.batch_id

        if batch_id:
            logger.info(f"Resuming existing batch: {batch_id}")
        else:
            # Submit new batch
            requests = [
                AnalysisRequest(
                    custom_id=f"article_{a.id}",
                    article=a,
                )
                for a in to_analyze
            ]

            batch_id = await self.provider.submit_batch(requests)

            # Persist batch_id for resume
            pipeline_run.batch_id = batch_id
            self.db.commit()

            # Create tracking records
            self._create_tracking_records(
                [a.id for a in to_analyze], batch_id
            )

        # Poll until completion
        responses = await self._poll_batch(
            batch_id, progress_callback=progress_callback
        )

        # Update tracking
        success_count, fail_count = self._update_tracking_from_responses(
            responses
        )

        logger.info(
            f"Analysis complete: {success_count} success, {fail_count} failed"
        )
        return success_count, fail_count

    async def retry_failed(self, progress_callback=None) -> tuple[str, int]:
        """Re-submit failed articles as a new batch.

        Returns:
            Tuple of (batch_id, article_count)
        """
        failed_ids = self.get_failed_article_ids()
        if not failed_ids:
            logger.info("No failed articles to retry")
            return "", 0

        # Load articles
        articles = (
            self.db.query(NewsArticle)
            .filter(NewsArticle.id.in_(failed_ids))
            .all()
        )

        if not articles:
            return "", 0

        # Clear old failed records
        self.clear_tracking(failed_only=True)

        # Submit new batch
        requests = [
            AnalysisRequest(custom_id=f"article_{a.id}", article=a)
            for a in articles
        ]

        batch_id = await self.provider.submit_batch(requests)
        self._create_tracking_records([a.id for a in articles], batch_id)

        # Poll
        responses = await self._poll_batch(
            batch_id, progress_callback=progress_callback
        )
        self._update_tracking_from_responses(responses)

        return batch_id, len(articles)

    # ── Polling ──────────────────────────────────────────────

    async def _poll_batch(
        self,
        batch_id: str,
        progress_callback=None,
    ) -> list[AnalysisResponse]:
        """Poll batch until completion or timeout."""
        poll_interval = settings.llm_analysis_poll_interval
        max_wait = settings.llm_analysis_max_wait
        elapsed = 0

        while elapsed < max_wait:
            status_result = await self.provider.check_batch_status(batch_id)
            logger.debug(
                f"Batch {batch_id}: {status_result.status.value} "
                f"({status_result.completed}/{status_result.total})"
            )

            if progress_callback:
                progress_callback(
                    "llm_analysis",
                    status_result.completed + status_result.failed,
                    status_result.total,
                )

            if status_result.status == BatchStatus.COMPLETED:
                return await self.provider.retrieve_results(batch_id)

            if status_result.status in (
                BatchStatus.FAILED,
                BatchStatus.EXPIRED,
                BatchStatus.CANCELLED,
            ):
                raise RuntimeError(
                    f"Batch {batch_id} {status_result.status.value}"
                )

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Batch {batch_id} did not complete within {max_wait}s"
        )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse_article_id(custom_id: str) -> int | None:
        """Extract article_id from custom_id like 'article_123'."""
        try:
            return int(custom_id.split("_", 1)[1])
        except (IndexError, ValueError):
            return None
```

**Step 2: Verify import**

Run: `poetry run python -c "from app.services.pipeline.llm_analysis_service import LLMAnalysisService; print('Analysis service OK')"`
Expected: `Analysis service OK`

**Step 3: Commit**

```bash
git add app/services/pipeline/llm_analysis_service.py
git commit -m "feat: implement LLM analysis service with batch processing"
```

---

### Task 7: Wire LLM Analysis into pipeline orchestrator

**Files:**
- Modify: `app/services/pipeline/pipeline_orchestrator.py`

**Step 1: Update `get_analysis_service` method**

Replace the existing `get_analysis_service` to no longer take provider_name/model (the provider is configured internally):

```python
    def get_analysis_service(self) -> LLMAnalysisService:
        """Get LLM analysis service."""
        return LLMAnalysisService(self.db)
```

**Step 2: Replace the Stage 3 placeholder block in `run_pipeline`**

Find the current placeholder:

```python
            # Stage 3: LLM_ANALYSIS (framework only)
            if until_stage == PipelineStage.LLM_ANALYSIS:
                self.store.update_pipeline_run_status(
                    run, PipelineRunStatus.RUNNING, PipelineStage.LLM_ANALYSIS
                )
                # Analysis is a placeholder for now
                self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                return run
```

Replace with:

```python
            # Stage 3: LLM_ANALYSIS
            if all_passed_articles:
                self.store.update_pipeline_run_status(
                    run, PipelineRunStatus.RUNNING, PipelineStage.LLM_ANALYSIS
                )

                analysis_service = self.get_analysis_service()

                try:
                    success_count, fail_count = await analysis_service.analyze_articles(
                        all_passed_articles, run, progress_callback=progress_callback
                    )
                    run.analyzed_count = success_count
                    self.db.commit()
                except TimeoutError:
                    logger.warning(f"Batch polling timed out for run {run.id}, pausing")
                    self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                    return run

            if until_stage == PipelineStage.LLM_ANALYSIS:
                self.store.update_pipeline_run_status(run, PipelineRunStatus.PAUSED)
                return run
```

**Step 3: Add `logger` import at top of file**

Add after existing imports:

```python
from loguru import logger
```

**Step 4: Verify import**

Run: `poetry run python -c "from app.services.pipeline.pipeline_orchestrator import PipelineOrchestrator; print('Orchestrator OK')"`
Expected: `Orchestrator OK`

**Step 5: Commit**

```bash
git add app/services/pipeline/pipeline_orchestrator.py
git commit -m "feat: wire LLM analysis stage into pipeline orchestrator"
```

---

### Task 8: Add CLI analysis commands

**Files:**
- Modify: `cli/pipeline.py` — add `analysis` Typer sub-app with `retry-failed`, `clear`, `status` commands

**Step 1: Create the analysis sub-app**

After the existing `providers` command (before `_display_run_stats`), add:

```python
# ── Analysis subcommands ─────────────────────────────────

analysis_app = typer.Typer(help="LLM analysis management commands")
app.add_typer(analysis_app, name="analysis")


@analysis_app.command("status")
def analysis_status():
    """Show analysis tracking statistics."""
    db = get_db()
    from app.services.pipeline.llm_analysis_service import LLMAnalysisService

    service = LLMAnalysisService(db)
    stats = service.get_tracking_stats()

    table = Table(show_header=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Total Tracked", f"{stats['total']:,}")
    table.add_row("[green]Success[/green]", f"{stats['success']:,}")
    table.add_row("[red]Failed[/red]", f"{stats['failed']:,}")
    table.add_row("[yellow]Pending[/yellow]", f"{stats['pending']:,}")

    console.print(Panel("[bold]Analysis Tracking Statistics[/bold]"))
    console.print(table)


@analysis_app.command("retry-failed")
def analysis_retry_failed():
    """Re-submit all failed articles for analysis."""
    db = get_db()
    from app.services.pipeline.llm_analysis_service import LLMAnalysisService

    service = LLMAnalysisService(db)
    stats = service.get_tracking_stats()

    if stats["failed"] == 0:
        console.print("[green]No failed articles to retry[/green]")
        return

    console.print(f"Retrying {stats['failed']} failed articles...")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task("Submitting batch...", total=100)

        def update_progress(stage: str, current: int, total: int):
            if total > 0:
                pct = (current / total) * 100
                progress.update(
                    task,
                    description=f"[{stage}] {current}/{total}",
                    completed=pct,
                )

        batch_id, count = asyncio.run(
            service.retry_failed(progress_callback=update_progress)
        )

    console.print(f"[green]Retried {count} articles in batch {batch_id}[/green]")

    # Show updated stats
    new_stats = service.get_tracking_stats()
    console.print(
        f"Success: {new_stats['success']}, "
        f"Failed: {new_stats['failed']}, "
        f"Pending: {new_stats['pending']}"
    )


@analysis_app.command("clear")
def analysis_clear(
    all_records: bool = typer.Option(
        False, "--all", help="Clear all tracking records"
    ),
    failed: bool = typer.Option(
        False, "--failed", help="Clear only failed records"
    ),
    article_id: Optional[int] = typer.Option(
        None, "--article-id", "-a", help="Clear records for specific article"
    ),
    batch_id: Optional[str] = typer.Option(
        None, "--batch-id", "-b", help="Clear records for specific batch"
    ),
):
    """Clear analysis tracking records."""
    options_set = sum([all_records, failed, article_id is not None, batch_id is not None])
    if options_set == 0:
        console.print("[red]Specify one of: --all, --failed, --article-id, --batch-id[/red]")
        raise typer.Exit(1)
    if options_set > 1:
        console.print("[red]Specify only one option[/red]")
        raise typer.Exit(1)

    db = get_db()
    from app.services.pipeline.llm_analysis_service import LLMAnalysisService

    service = LLMAnalysisService(db)
    count = service.clear_tracking(
        all_records=all_records,
        failed_only=failed,
        article_id=article_id,
        batch_id=batch_id,
    )

    console.print(f"[green]Cleared {count} tracking records[/green]")
```

**Step 2: Verify CLI loads**

Run: `poetry run python -m cli analysis status`
Expected: Shows empty stats table (0 for all)

Run: `poetry run python -m cli analysis --help`
Expected: Shows help for analysis subcommands

**Step 3: Commit**

```bash
git add cli/pipeline.py
git commit -m "feat: add CLI analysis commands (status, retry-failed, clear)"
```

---

### Task 9: Update pipeline __init__.py exports

**Files:**
- Modify: `app/services/pipeline/__init__.py`

**Step 1: Verify current exports still work**

The `__init__.py` already exports `LLMAnalysisService` and `PipelineOrchestrator`. No changes needed since we modified the existing `LLMAnalysisService` in place.

Run: `poetry run python -c "from app.services.pipeline import LLMAnalysisService, PipelineOrchestrator; print('Exports OK')"`
Expected: `Exports OK`

---

### Task 10: Full verification

**Step 1: Import chain verification**

Run:
```bash
poetry run python -c "
from app.models import ArticleAnalysisTracking, AnalysisStatus, PipelineRun
from app.services.pipeline import PipelineOrchestrator, LLMAnalysisService
from app.services.pipeline.analysis.base_provider import BaseAnalysisProvider
from app.services.pipeline.analysis.openai_batch_provider import OpenAIBatchProvider
from schemas.llm_output import NewsAnalysisResult
from prompts.system_prompt import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
print('All imports OK')
print(f'System prompt: {len(SYSTEM_PROMPT)} chars')
print(f'Schema fields: {list(NewsAnalysisResult.model_fields.keys())}')
print(f'Tracking status values: {[s.value for s in AnalysisStatus]}')
print(f'PipelineRun has batch_id: {hasattr(PipelineRun, \"batch_id\")}')
"
```

**Step 2: CLI verification**

Run:
```bash
poetry run python -m cli analysis --help
poetry run python -m cli analysis status
poetry run python -m cli --help
```

**Step 3: DB table creation test**

Run:
```bash
poetry run python -c "
from sqlalchemy import create_engine, inspect
from app.models import Base
engine = create_engine('sqlite:///./test_analysis.db')
Base.metadata.create_all(engine)
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f'Tables created: {tables}')
assert 'article_analysis_tracking' in tables
print('article_analysis_tracking columns:', [c['name'] for c in inspector.get_columns('article_analysis_tracking')])
import os; os.remove('./test_analysis.db')
print('Verification passed')
"
```

**Step 4: Final commit**

```bash
git add -A
git commit -m "feat: complete LLM analysis stage implementation"
```
