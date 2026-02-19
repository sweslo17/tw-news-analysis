"""OpenAI Batch API provider for news analysis."""

import json
from io import BytesIO

from loguru import logger
from openai import AsyncOpenAI

from app.config import settings
from .prompts import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from .schemas import NewsAnalysisResult
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

# Keywords not supported by OpenAI strict mode JSON schema
_UNSUPPORTED_KEYWORDS = {
    "minLength", "maxLength", "pattern", "minimum", "maximum",
    "exclusiveMinimum", "exclusiveMaximum", "minItems", "maxItems",
    "uniqueItems", "format", "multipleOf", "default", "title",
}


def _make_strict_node(node: dict) -> dict:
    """Recursively process a JSON schema node for OpenAI strict mode.

    - Strips unsupported keywords (minLength, maxLength, pattern, etc.)
    - Adds additionalProperties: false to all objects
    - Makes all properties required
    """
    node = {k: v for k, v in node.items() if k not in _UNSUPPORTED_KEYWORDS}

    if "$defs" in node:
        node["$defs"] = {
            k: _make_strict_node(v) for k, v in node["$defs"].items()
        }

    if node.get("type") == "object" and "properties" in node:
        node["additionalProperties"] = False
        node["required"] = list(node["properties"].keys())
        node["properties"] = {
            k: _make_strict_node(v) for k, v in node["properties"].items()
        }

    if node.get("type") == "array" and "items" in node:
        node["items"] = _make_strict_node(node["items"])

    if "anyOf" in node:
        node["anyOf"] = [_make_strict_node(opt) for opt in node["anyOf"]]

    return node


class OpenAIBatchProvider(BaseAnalysisProvider):
    """OpenAI Batch API implementation for structured news analysis."""

    def __init__(self, model: str | None = None, api_key: str | None = None):
        self.model = model or settings.llm_analysis_model
        self.api_key = api_key or settings.openai_api_key
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")
        self.client = AsyncOpenAI(api_key=self.api_key)
        self._json_schema = self._build_json_schema()

    @property
    def name(self) -> str:
        return "openai_batch"

    def _build_json_schema(self) -> dict:
        """Build the JSON schema for structured output from Pydantic model.

        Post-processes the Pydantic schema to satisfy OpenAI strict mode:
        additionalProperties: false on all objects, all properties required,
        unsupported validation keywords stripped.
        """
        raw_schema = NewsAnalysisResult.model_json_schema()
        strict_schema = _make_strict_node(raw_schema)
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "news_analysis",
                "strict": True,
                "schema": strict_schema,
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
        file_obj = await self.client.files.create(
            file=("batch_input.jsonl", BytesIO(jsonl_content)),
            purpose="batch",
        )
        logger.info(f"Uploaded file: {file_obj.id}")

        # Create batch
        batch = await self.client.batches.create(
            input_file_id=file_obj.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        logger.info(f"Created batch: {batch.id}")

        return batch.id

    async def check_batch_status(self, batch_id: str) -> BatchStatusResult:
        """Check OpenAI batch status."""
        batch = await self.client.batches.retrieve(batch_id)
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
        batch = await self.client.batches.retrieve(batch_id)

        if batch.status != "completed":
            raise RuntimeError(
                f"Batch {batch_id} is not completed (status: {batch.status})"
            )

        responses: list[AnalysisResponse] = []

        # Process successful results
        if batch.output_file_id:
            content = await self.client.files.content(batch.output_file_id)
            for line in content.text.strip().split("\n"):
                if not line.strip():
                    continue
                responses.append(self._parse_result_line(line))

        # Process error results
        if batch.error_file_id:
            content = await self.client.files.content(batch.error_file_id)
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
