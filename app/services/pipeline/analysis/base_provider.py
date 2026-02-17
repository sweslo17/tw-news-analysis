"""Abstract base for LLM analysis providers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
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
