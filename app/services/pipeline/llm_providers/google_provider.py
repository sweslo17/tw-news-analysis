"""Google Gemini LLM provider implementation."""

from app.config import settings
from .base import BaseLLMProvider, LLMFilterResponse, ArticleInput


class GoogleProvider(BaseLLMProvider):
    """Google Gemini LLM provider."""

    _genai = None  # Lazy load to avoid deprecation warning at import time

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.google_api_key
        if not self.api_key:
            raise ValueError("Google API key not configured")

        # Lazy import to avoid deprecation warning when not using this provider
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            import google.generativeai as genai
            GoogleProvider._genai = genai

        self._genai.configure(api_key=self.api_key)

    @property
    def name(self) -> str:
        return "google"

    @property
    def default_model(self) -> str:
        return "gemini-1.5-flash"

    async def filter_article(
        self, article: ArticleInput, model: str | None = None
    ) -> LLMFilterResponse:
        """Filter article using Google Gemini API."""
        model_name = model or self.default_model
        prompt = self._build_filter_prompt(article)

        try:
            genai = self._genai
            model_instance = genai.GenerativeModel(model_name)

            # Gemini API is synchronous, wrap in executor for async
            import asyncio

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: model_instance.generate_content(
                    f"你是一個專業的新聞篩選助手，只輸出 JSON 格式的回應。\n\n{prompt}",
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.1,
                        max_output_tokens=200,
                    ),
                ),
            )

            response_text = response.text if response.text else ""
            return self._parse_filter_response(response_text)

        except Exception as e:
            return LLMFilterResponse(
                decision="keep",
                confidence=0.3,
                reason=f"Google API 錯誤: {str(e)}，預設保留",
                raw_response={"error": str(e)},
            )
