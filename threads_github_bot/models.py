from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    return ensure_utc(datetime.fromisoformat(cleaned))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RepositoryCandidate:
    repo_id: int
    full_name: str
    name: str
    owner: str
    description: str
    html_url: str
    homepage: str
    topics: Tuple[str, ...]
    matched_topics: Tuple[str, ...]
    language: Optional[str]
    stargazers_count: int
    forks_count: int
    archived: bool
    fork: bool
    pushed_at: datetime
    updated_at: datetime
    score: float = 0.0
    discoverability_score: float = 0.0
    trend_score: float = 0.0
    novelty_score: float = 0.0
    content_fit_score: float = 0.0
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def primary_topic(self) -> Optional[str]:
        if self.matched_topics:
            return self.matched_topics[0]
        if self.topics:
            return self.topics[0]
        return None


@dataclass
class PostedRepositorySnapshot:
    repo_id: int
    full_name: str
    repo_name: str
    owner: str
    topics: Tuple[str, ...]
    similarity_key: str
    homepage: str
    posted_at: datetime
    post_text: str
    series_slug: Optional[str] = None
    thread_text: str = ""
    pillar_slug: Optional[str] = None


@dataclass
class GeneratedPost:
    repo: RepositoryCandidate
    text: str
    language: str
    model: str
    raw_response: Dict[str, Any]
    prompt_version: str
    mode: str


@dataclass
class SeriesChoice:
    slug: str
    label: str
    number: Optional[int] = None
    description: str = ""
    display_label: Optional[str] = None

    def __post_init__(self) -> None:
        if self.display_label:
            return
        if self.number is None:
            self.display_label = self.label
            return
        self.display_label = "{0} #{1}".format(self.label, self.number)


@dataclass
class ThreadPost:
    position: int
    role: str
    text: str


@dataclass
class GeneratedThread:
    repo: RepositoryCandidate
    posts: Tuple[ThreadPost, ...]
    language: str
    model: str
    raw_response: Dict[str, Any]
    prompt_version: str
    mode: str
    series_slug: str
    series_label: str
    series_number: Optional[int] = None
    pillar_slug: str = "repo-discovery"

    @property
    def flattened_text(self) -> str:
        return "\n\n".join(post.text for post in self.posts)


@dataclass
class ValidationResult:
    is_valid: bool
    reasons: List[str]


@dataclass
class PublishResult:
    success: bool
    container_id: Optional[str] = None
    media_id: Optional[str] = None
    response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None


@dataclass
class ThreadPublishPostResult:
    success: bool
    container_id: Optional[str] = None
    media_id: Optional[str] = None
    reply_to_id: Optional[str] = None
    response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None


@dataclass
class PublishThreadResult:
    success: bool
    posts: List[ThreadPublishPostResult]
    error: Optional[str] = None


@dataclass
class ScheduleSlotPlan:
    slot_key: str
    local_date: str
    slot_name: str
    base_local: datetime
    planned_local: datetime
    planned_at_utc: datetime
    jitter_minutes: int
    status: str = "planned"
    actual_publish_at_utc: Optional[datetime] = None


@dataclass
class PipelineRunResult:
    status: str
    selected_repo: Optional[str] = None
    reasons: Sequence[str] = ()
    selected_series: Optional[str] = None
    scheduled_slot_key: Optional[str] = None
