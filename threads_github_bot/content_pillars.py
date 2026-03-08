"""Content pillar rotation system.

Defines the available content pillars and selects the next pillar based on
recent posting history to ensure variety.  Repo-based pillars use the existing
GitHub discovery pipeline.  Standalone pillars generate content without a
specific repository candidate.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from threads_github_bot.config import Settings
from threads_github_bot.models import PostedRepositorySnapshot

# ---------------------------------------------------------------------------
# Pillar definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ContentPillar:
    slug: str
    label: str
    description: str
    needs_repo: bool
    weight: float  # higher = selected more often
    temperature: float = 0.6  # per-pillar LLM temperature


PILLAR_CATALOG: Dict[str, ContentPillar] = {
    "repo-discovery": ContentPillar(
        slug="repo-discovery",
        label="Repo Discovery",
        description="Discover and present an interesting GitHub repo.",
        needs_repo=True,
        weight=0.40,
        temperature=0.6,
    ),
    "hot-take": ContentPillar(
        slug="hot-take",
        label="Operator Hot Take",
        description="A short, opinionated take on developer workflows, tools, or building.",
        needs_repo=False,
        weight=0.20,
        temperature=0.85,
    ),
    "tool-comparison": ContentPillar(
        slug="tool-comparison",
        label="Tool Comparison",
        description="Compare two approaches, tools, or repos in the same space.",
        needs_repo=False,
        weight=0.15,
        temperature=0.7,
    ),
    "question": ContentPillar(
        slug="question",
        label="Question Post",
        description="An engagement-first post that asks the audience a concrete question.",
        needs_repo=False,
        weight=0.15,
        temperature=0.85,
    ),
    "workflow-breakdown": ContentPillar(
        slug="workflow-breakdown",
        label="Workflow Breakdown",
        description="Break down a real workflow, experiment, or build-in-public observation.",
        needs_repo=False,
        weight=0.10,
        temperature=0.7,
    ),
}

DEFAULT_PILLAR_ORDER: Tuple[str, ...] = (
    "repo-discovery",
    "hot-take",
    "repo-discovery",
    "question",
    "repo-discovery",
    "tool-comparison",
    "repo-discovery",
    "workflow-breakdown",
)


# ---------------------------------------------------------------------------
# Selection logic
# ---------------------------------------------------------------------------

def select_next_pillar(
    settings: Settings,
    history: Sequence[PostedRepositorySnapshot],
    now: Optional[datetime] = None,
) -> ContentPillar:
    """Pick the best next pillar given recent history."""
    now = now or datetime.now(timezone.utc)
    allowed = _allowed_pillars(settings)
    if not allowed:
        return PILLAR_CATALOG["repo-discovery"]

    recent_slugs = _extract_recent_pillar_slugs(history, limit=8)

    # Score each pillar: higher = more deserving of selection
    scored: List[Tuple[float, str]] = []
    for slug in allowed:
        pillar = PILLAR_CATALOG[slug]
        score = pillar.weight

        # Recency penalty — if this pillar was posted recently, penalize it
        recency_penalty = _recency_penalty(slug, recent_slugs)
        score -= recency_penalty

        # Streak penalty — avoid 3+ of the same pillar in a row
        if len(recent_slugs) >= 2 and recent_slugs[0] == slug and recent_slugs[1] == slug:
            score -= 1.0

        # Consecutive penalty
        if recent_slugs and recent_slugs[0] == slug:
            score -= 0.25

        # Deterministic tiebreak using date
        tiebreak = _deterministic_tiebreak(settings, slug, now)
        score += tiebreak * 0.01

        scored.append((score, slug))

    scored.sort(key=lambda item: item[0], reverse=True)
    return PILLAR_CATALOG[scored[0][1]]


def _allowed_pillars(settings: Settings) -> Tuple[str, ...]:
    """Return the pillar slugs that are enabled."""
    configured = settings.content.pillar_slugs
    if configured:
        return tuple(slug for slug in configured if slug in PILLAR_CATALOG)
    return tuple(PILLAR_CATALOG.keys())


def _extract_recent_pillar_slugs(
    history: Sequence[PostedRepositorySnapshot],
    limit: int = 8,
) -> List[str]:
    """Extract the pillar slug from each recent post.

    Older posts without a pillar_slug are assumed to be repo-discovery.
    """
    slugs: List[str] = []
    for item in history[:limit]:
        pillar_slug = getattr(item, "pillar_slug", None) or "repo-discovery"
        slugs.append(pillar_slug)
    return slugs


def _recency_penalty(slug: str, recent_slugs: List[str]) -> float:
    """Penalize pillars that appeared recently.  More recent = bigger penalty."""
    penalty = 0.0
    for index, recent_slug in enumerate(recent_slugs):
        if recent_slug == slug:
            penalty += 0.3 / (index + 1)
    return penalty


def _deterministic_tiebreak(settings: Settings, slug: str, now: datetime) -> float:
    """Small deterministic jitter so identical situations still pick consistently."""
    token = "{0}:{1}:{2}".format(
        settings.schedule.jitter_seed,
        now.strftime("%Y-%m-%d"),
        slug,
    )
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    return (int.from_bytes(digest[:4], "big") % 100) / 100.0
