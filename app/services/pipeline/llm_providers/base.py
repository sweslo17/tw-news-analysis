"""Base LLM provider interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class LLMFilterResponse:
    """Response from LLM filter."""

    decision: str  # "keep" or "filter"
    confidence: float  # 0.0 to 1.0
    reason: str
    raw_response: dict[str, Any] | None = None


@dataclass
class ArticleInput:
    """Input data for LLM filtering."""

    article_id: int
    title: str
    tags: list[str] | None
    category: str | None
    sub_category: str | None
    summary: str | None


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""
        pass

    @property
    @abstractmethod
    def default_model(self) -> str:
        """Default model for this provider."""
        pass

    @abstractmethod
    async def filter_article(
        self, article: ArticleInput, model: str | None = None
    ) -> LLMFilterResponse:
        """
        Filter a single article.

        Args:
            article: Article data for filtering
            model: Optional model override

        Returns:
            LLMFilterResponse with decision and reason
        """
        pass

    async def filter_articles_batch(
        self, articles: list[ArticleInput], model: str | None = None
    ) -> list[tuple[int, LLMFilterResponse]]:
        """
        Filter multiple articles.

        Default implementation processes one at a time.
        Subclasses can override for batch optimization.

        Args:
            articles: List of articles to filter
            model: Optional model override

        Returns:
            List of (article_id, LLMFilterResponse) tuples
        """
        results = []
        for article in articles:
            response = await self.filter_article(article, model)
            results.append((article.article_id, response))
        return results

    def _build_filter_prompt(self, article: ArticleInput) -> str:
        """Build the filtering prompt for an article."""
        tags_str = ", ".join(article.tags) if article.tags else "無"

        prompt = f"""你是一個新聞文章篩選助手。請判斷以下新聞文章是否值得進一步分析。

文章資訊：
- 標題：{article.title}
- 分類：{article.category or '無'}
- 子分類：{article.sub_category or '無'}
- 標籤：{tags_str}
- 摘要：{article.summary or '無'}

請根據以下標準判斷：

【應該過濾的文章類型】
1. 星座運勢、塔羅牌、占卜相關
2. 彩券開獎號碼、樂透結果
3. 明顯的廣告、業配文
4. 例行天氣預報（非極端天氣事件）
5. 純娛樂八卦（無社會意義）

【應該保留的文章類型】
1. 政治、經濟、社會新聞
2. 國際關係、外交新聞
3. 科技、醫療重大發展
4. 有社會影響的娛樂新聞（如藝人政治表態）
5. 有社會影響的體育新聞（如運動員抗議）

【重要原則】
- 如果不確定，請偏向保留 (keep)
- 只有非常確定不重要時才過濾 (filter)

請以 JSON 格式回覆，不要包含任何其他文字：
{{"decision": "keep 或 filter", "confidence": 0.0到1.0的數字, "reason": "簡短說明理由"}}"""

        return prompt

    def _parse_filter_response(self, response_text: str) -> LLMFilterResponse:
        """Parse the LLM response into LLMFilterResponse."""
        import json
        import re

        # Try to extract JSON from the response
        json_match = re.search(r'\{[^{}]*\}', response_text)
        if json_match:
            try:
                data = json.loads(json_match.group())
                decision = data.get("decision", "keep").lower()
                if decision not in ("keep", "filter"):
                    decision = "keep"  # Default to keep if unclear

                confidence = float(data.get("confidence", 0.5))
                confidence = max(0.0, min(1.0, confidence))  # Clamp to [0, 1]

                reason = data.get("reason", "未提供理由")

                return LLMFilterResponse(
                    decision=decision,
                    confidence=confidence,
                    reason=reason,
                    raw_response=data,
                )
            except (json.JSONDecodeError, ValueError, KeyError):
                pass

        # Fallback: default to keep with low confidence
        return LLMFilterResponse(
            decision="keep",
            confidence=0.3,
            reason="無法解析 LLM 回應，預設保留",
            raw_response={"raw_text": response_text},
        )
