from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple

from threads_github_bot.config import Settings
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate, SeriesChoice

AI_TOPICS = {"ai", "agents", "llm", "rag"}
DEV_TOOL_TOPICS = {"developer-tools", "dev-infra", "productivity", "saas"}

SERIES_CATALOG: Dict[str, Dict[str, object]] = {
    "hidden-github-gem": {
        "label": "Hidden GitHub Gem",
        "description": "A discovery-first format for repos that feel early and shareable.",
    },
    "ai-repo-worth-trying": {
        "label": "AI Repo Worth Trying",
        "description": "An AI-focused format for practical repos with builder appeal.",
    },
    "open-source-tool-of-the-day": {
        "label": "Open Source Tool of the Day",
        "description": "A broad format for useful OSS picks.",
    },
    "dev-tool-i-didnt-know-existed": {
        "label": "Dev Tool I Didn't Know Existed",
        "description": "A format for developer tools that are easy to explain.",
    },
    "trending-ai-repo": {
        "label": "Trending AI Repo",
        "description": "A format for AI repos with stronger momentum.",
    },
}


def select_series_for_candidate(
    candidate: RepositoryCandidate,
    settings: Settings,
    recent_history: Iterable[PostedRepositorySnapshot],
) -> SeriesChoice:
    history = [item for item in recent_history if item.series_slug]
    last_series_slug = history[0].series_slug if history else None
    allowed_templates = _allowed_templates(settings)
    scored = []
    for slug in allowed_templates:
        compatibility = _compatibility_score(slug, candidate, settings)
        if compatibility <= 0:
            continue
        penalty = 0.0
        if last_series_slug == slug and not settings.series.allow_consecutive:
            penalty -= 10.0
        recent_penalty = _recent_series_penalty(slug, history, settings)
        total = compatibility + recent_penalty + (candidate.discoverability_score * 0.2)
        scored.append((total, slug, penalty))

    if not scored:
        fallback_slug = allowed_templates[0]
        return _build_choice(fallback_slug, history, settings)

    scored.sort(key=lambda item: (item[0], item[2]), reverse=True)
    return _build_choice(scored[0][1], history, settings)


def _allowed_templates(settings: Settings) -> Tuple[str, ...]:
    slugs = tuple(slug for slug in settings.series.templates if slug in SERIES_CATALOG)
    if settings.series.whitelist:
        slugs = tuple(slug for slug in slugs if slug in settings.series.whitelist)
    if settings.series.blacklist:
        slugs = tuple(slug for slug in slugs if slug not in settings.series.blacklist)
    return slugs or ("hidden-github-gem",)


def _compatibility_score(slug: str, candidate: RepositoryCandidate, settings: Settings) -> float:
    topics = set(candidate.matched_topics or candidate.topics)
    if slug == "trending-ai-repo":
        if topics.intersection(AI_TOPICS):
            return 0.85 + candidate.trend_score
        return 0.0
    if slug == "ai-repo-worth-trying":
        return 0.8 if topics.intersection(AI_TOPICS) else 0.0
    if slug == "dev-tool-i-didnt-know-existed":
        return 0.75 if topics.intersection(DEV_TOOL_TOPICS) else 0.0
    if slug == "hidden-github-gem":
        if candidate.stargazers_count <= max(25000, settings.github.discoverability_star_ceiling * 2):
            return 0.78
        return 0.55
    if slug == "open-source-tool-of-the-day":
        return 0.7
    return 0.0


def _recent_series_penalty(
    slug: str,
    history: Iterable[PostedRepositorySnapshot],
    settings: Settings,
) -> float:
    now = datetime.now(timezone.utc)
    penalty = 0.0
    for item in history:
        if item.series_slug != slug:
            continue
        age_days = (now - item.posted_at).total_seconds() / 86400.0
        if age_days < settings.cooldown.series_days:
            penalty -= 0.5
    return penalty


def _build_choice(
    slug: str,
    history: Iterable[PostedRepositorySnapshot],
    settings: Settings,
) -> SeriesChoice:
    count = sum(1 for item in history if item.series_slug == slug)
    number = count + 1 if settings.series.enable_numbering else None
    metadata = SERIES_CATALOG[slug]
    return SeriesChoice(
        slug=slug,
        label=str(metadata["label"]),
        number=number,
        description=str(metadata["description"]),
    )
