# LLM Analysis Stage Design

## Overview

Implement the LLM Analysis stage in the pipeline: `Fetch → Rule Filter → LLM Analysis → Store`.

Uses GPT-4o-mini + OpenAI Batch API with structured output to analyze news articles. The provider framework is swappable. Analysis results are NOT stored in the existing DB (will go to a separate DB later); only tracking records are persisted locally.

## Pipeline Flow

```
Rule Filter passed articles
       ↓
  Query tracking table → skip already-analyzed (success)
       ↓
  Assemble JSONL (one request per article)
       ↓
  Upload to OpenAI → Create Batch
       ↓
  Persist batch_id to PipelineRun
       ↓
  Write tracking records (status=pending) per article
       ↓
  Poll until batch completes (interruptible, resumable)
       ↓
  Download results → parse structured output via Pydantic
       ↓
  Update tracking: success or failed (with error_message)
       ↓
  Log statistics (success/fail counts)
```

## New Files

| File | Purpose |
|------|---------|
| `prompts/system_prompt.py` | System prompt + user prompt template |
| `schemas/llm_output.py` | Pydantic V2 structured output schema (`NewsAnalysisResult`) |
| `app/services/pipeline/analysis/base_provider.py` | Abstract base for analysis providers |
| `app/services/pipeline/analysis/openai_batch_provider.py` | OpenAI Batch API implementation |

## Modified Files

| File | Changes |
|------|---------|
| `app/models.py` | Add `ArticleAnalysisTracking` model; add `batch_id` field to `PipelineRun` |
| `app/config.py` | Add `llm_analysis_poll_interval` setting |
| `app/services/pipeline/llm_analysis_service.py` | Replace placeholder with real implementation |
| `app/services/pipeline/pipeline_orchestrator.py` | Wire LLM_ANALYSIS stage to actual logic |
| `cli/pipeline.py` | Add `analysis` subcommand group with retry-failed, clear, status |

## Provider Abstraction

```python
class BaseAnalysisProvider(ABC):
    @abstractmethod
    async def submit_batch(self, requests: list[AnalysisRequest]) -> str:
        """Submit batch, return batch_id."""

    @abstractmethod
    async def check_batch_status(self, batch_id: str) -> BatchStatus:
        """Check batch status."""

    @abstractmethod
    async def retrieve_results(self, batch_id: str) -> list[AnalysisResponse]:
        """Retrieve batch results."""
```

Swappable: implement these 3 methods for any future provider (Anthropic, Gemini, etc.).

## OpenAI Batch API Implementation

1. **Assemble JSONL**: Each article → one request with `response_format={"type": "json_schema", ...}` for structured output.
2. **System prompt first**: Leverages GPT-4o-mini automatic prompt caching (identical prefix >= 1024 tokens).
3. **Upload + create batch**: `client.files.create()` → `client.batches.create()`.
4. **Polling**: Check `client.batches.retrieve(batch_id)` every N seconds.
5. **Retrieve results**: Download output file → parse line-by-line → validate with `NewsAnalysisResult`.

## Resume Mechanism

- `PipelineRun.batch_id` persists the OpenAI batch ID.
- When entering LLM_ANALYSIS stage, check if `batch_id` already exists:
  - **Exists** → skip submission, go straight to polling (resume).
  - **Does not exist** → assemble + submit new batch.
- Interrupted service can resume by re-running the same pipeline run.

## Tracking Table: `article_analysis_tracking`

```
article_analysis_tracking
├── id (PK)
├── article_id (FK → news_articles.id, indexed)
├── batch_id (OpenAI batch ID, indexed)
├── status: pending / success / failed
├── error_message (nullable)
├── created_at
├── updated_at
```

- On batch submit → insert `status=pending` per article.
- On batch complete → update to `success` or `failed` per article.
- On next analysis run → exclude articles with `status=success`.

## CLI Commands

### Existing (modified)

- `run <RUN_ID> --until llm_analysis` — runs analysis with sync polling + progress display.

### New: `analysis` subcommand group

| Command | Description |
|---------|-------------|
| `analysis retry-failed` | Re-submit all `failed` articles as a new batch |
| `analysis clear --all` | Delete all tracking records |
| `analysis clear --failed` | Delete only failed tracking records |
| `analysis clear --article-id <ID>` | Delete tracking for specific article |
| `analysis clear --batch-id <ID>` | Delete tracking for specific batch |
| `analysis status` | Show analysis statistics (success/failed/pending counts) |

## Error Handling

- **Per-article parse failure** → mark `failed` with error_message in tracking, skip and continue.
- **Batch-level failure** (OpenAI error) → Pipeline marks `FAILED` + error_log.
- **Polling timeout** (configurable max wait) → Pipeline marks `PAUSED`, resumable.

## Storage

Analysis results are NOT stored in the existing DB. The `LLMAnalysisService` returns results in memory and logs statistics. A `store_results()` hook is reserved for future integration with a separate DB.

## Prompt Caching Strategy

GPT-4o-mini automatically caches identical prefixes >= 1024 tokens. The system prompt (~1500+ tokens) is identical across all requests in a batch, ensuring cache hits for every request after the first.
