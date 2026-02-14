"""Anthropic LLM provider implementation."""

from anthropic import AsyncAnthropic

from app.config import settings
from .base import BaseLLMProvider, LLMFilterResponse, ArticleInput


class AnthropicProvider(BaseLLMProvider):
    """Anthropic Claude LLM provider."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.anthropic_api_key
        if not self.api_key:
            raise ValueError("Anthropic API key not configured")
        self.client = AsyncAnthropic(api_key=self.api_key)

    @property
    def name(self) -> str:
        return "anthropic"

    @property
    def default_model(self) -> str:
        return "claude-3-haiku-20240307"

    async def filter_article(
        self, article: ArticleInput, model: str | None = None
    ) -> LLMFilterResponse:
        """Filter article using Anthropic API."""
        model = model or self.default_model
        prompt = self._build_filter_prompt(article)

        try:
            response = await self.client.messages.create(
                model=model,
                max_tokens=200,
                system="你是一個專業的新聞篩選助手，只輸出 JSON 格式的回應。",
                messages=[{"role": "user", "content": prompt}],
            )

            response_text = response.content[0].text if response.content else ""
            return self._parse_filter_response(response_text)

        except Exception as e:
            return LLMFilterResponse(
                decision="keep",
                confidence=0.3,
                reason=f"Anthropic API 錯誤: {str(e)}，預設保留",
                raw_response={"error": str(e)},
            )
