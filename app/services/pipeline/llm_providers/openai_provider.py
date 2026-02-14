"""OpenAI LLM provider implementation."""

from openai import AsyncOpenAI

from app.config import settings
from .base import BaseLLMProvider, LLMFilterResponse, ArticleInput


class OpenAIProvider(BaseLLMProvider):
    """OpenAI GPT LLM provider."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.openai_api_key
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")
        self.client = AsyncOpenAI(api_key=self.api_key)

    @property
    def name(self) -> str:
        return "openai"

    @property
    def default_model(self) -> str:
        return "gpt-4o-mini"

    async def filter_article(
        self, article: ArticleInput, model: str | None = None
    ) -> LLMFilterResponse:
        """Filter article using OpenAI API."""
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
            return LLMFilterResponse(
                decision="keep",
                confidence=0.3,
                reason=f"OpenAI API 錯誤: {str(e)}，預設保留",
                raw_response={"error": str(e)},
            )
