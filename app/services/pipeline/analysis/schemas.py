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
