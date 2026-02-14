"""Groq LLM provider implementation."""

from groq import AsyncGroq

from app.config import settings
from .base import BaseLLMProvider, LLMFilterResponse, ArticleInput


class GroqProvider(BaseLLMProvider):
    """Groq LLM provider for fast inference."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.groq_api_key
        if not self.api_key:
            raise ValueError("Groq API key not configured")
        self.client = AsyncGroq(api_key=self.api_key)

    @property
    def name(self) -> str:
        return "groq"

    @property
    def default_model(self) -> str:
        return "llama-3.1-8b-instant"

    async def filter_article(
        self, article: ArticleInput, model: str | None = None
    ) -> LLMFilterResponse:
        """Filter article using Groq API."""
        model = model or self.default_model
        prompt = self._build_filter_prompt(article)

        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一個專業的新聞篩選助手，只輸出 JSON 格式的回應。",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=200,
            )

            response_text = response.choices[0].message.content or ""
            return self._parse_filter_response(response_text)

        except Exception as e:
            # On error, default to keep with low confidence
            return LLMFilterResponse(
                decision="keep",
                confidence=0.3,
                reason=f"Groq API 錯誤: {str(e)}，預設保留",
                raw_response={"error": str(e)},
            )
