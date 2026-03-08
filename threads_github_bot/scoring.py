from __future__ import annotations

import math
import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

from threads_github_bot.config import Settings
from threads_github_bot.deduplication import similarity_key
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate

ENTERPRISE_PATTERNS = (
    re.compile(r"\benterprise\b", re.IGNORECASE),
    re.compile(r"\binternal\b", re.IGNORECASE),
    re.compile(r"\bgovernance\b", re.IGNORECASE),
    re.compile(r"\borganization(s)?\b", re.IGNORECASE),
    re.compile(r"\bat scale\b", re.IGNORECASE),
    re.compile(r"\bplatform\b", re.IGNORECASE),
)

USE_CASE_PATTERNS = (
    re.compile(r"\breview\b", re.IGNORECASE),
    re.compile(r"\bdebug\b", re.IGNORECASE),
    re.compile(r"\btest\b", re.IGNORECASE),
    re.compile(r"\bdeploy\b", re.IGNORECASE),
    re.compile(r"\bsearch\b", re.IGNORECASE),
    re.compile(r"\bworkflow(s)?\b", re.IGNORECASE),
    re.compile(r"\bpull request(s)?\b", re.IGNORECASE),
    re.compile(r"\brepo(sitory)?\b", re.IGNORECASE),
    re.compile(r"\bcode\b", re.IGNORECASE),
)

OPERATOR_AUDIENCE_PATTERNS = (
    re.compile(r"\bbuilder(s)?\b", re.IGNORECASE),
    re.compile(r"\bdeveloper(s)?\b", re.IGNORECASE),
    re.compile(r"\bengineer(s)?\b", re.IGNORECASE),
    re.compile(r"\bteam(s)?\b", re.IGNORECASE),
)

GENERIC_PLATFORM_PATTERNS = (
    re.compile(r"\bplatform\b", re.IGNORECASE),
    re.compile(r"\bflexible\b", re.IGNORECASE),
    re.compile(r"\borchestration\b", re.IGNORECASE),
    re.compile(r"\borganization(s)?\b", re.IGNORECASE),
    re.compile(r"\bat scale\b", re.IGNORECASE),
    re.compile(r"\bsolution(s)?\b", re.IGNORECASE),
)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _description_quality(description: str) -> float:
    cleaned = description.strip()
    if not cleaned:
        return 0.0
    word_count = len(re.findall(r"[A-Za-z0-9][A-Za-z0-9\-+.#]*", cleaned))
    length_score = _clamp(len(cleaned) / 140.0)
    word_score = _clamp(word_count / 18.0)
    penalty = 0.35 if word_count < 4 else 0.0
    return _clamp((length_score * 0.55) + (word_score * 0.45) - penalty)


def _topic_relevance(candidate: RepositoryCandidate, preferred_topics) -> float:
    topic_set = set(candidate.matched_topics or candidate.topics)
    if not topic_set:
        return 0.0
    overlap = len(topic_set.intersection(set(preferred_topics)))
    return _clamp(overlap / max(1, min(4, len(topic_set))))


def _url_quality(candidate: RepositoryCandidate) -> float:
    if candidate.homepage:
        domain = urlparse(candidate.homepage if "://" in candidate.homepage else "https://" + candidate.homepage).netloc
        if domain and "github.com" not in domain:
            return 1.0
    return 0.6 if candidate.html_url.startswith("https://github.com/") else 0.0


def _docs_signal(candidate: RepositoryCandidate) -> float:
    homepage = (candidate.homepage or "").lower()
    description = (candidate.description or "").lower()
    if homepage and "github.com" not in homepage:
        if any(token in homepage for token in ("demo", "docs", "play", "app", "studio")):
            return 1.0
        return 0.85
    if any(token in description for token in ("demo", "docs", "playground", "sdk", "cli", "api")):
        return 0.7
    return 0.45


def _explainability_score(candidate: RepositoryCandidate) -> float:
    description = candidate.description or ""
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-+.#]*", description)
    if not words:
        return 0.0
    length_penalty = 0.0
    if len(words) > 28:
        length_penalty = 0.2
    if len(words) < 6:
        length_penalty = 0.25
    specificity_tokens = (
        "developer",
        "agent",
        "tool",
        "workflow",
        "open source",
        "repo",
    )
    specificity = 1.0 if any(token in description.lower() for token in specificity_tokens) else 0.6
    return _clamp((_description_quality(description) * 0.6) + (specificity * 0.4) - length_penalty)


def _operator_angle_score(candidate: RepositoryCandidate) -> float:
    description = candidate.description or ""
    if not description.strip():
        return 0.0
    use_case_hits = sum(1 for pattern in USE_CASE_PATTERNS if pattern.search(description))
    audience_hits = sum(1 for pattern in OPERATOR_AUDIENCE_PATTERNS if pattern.search(description))
    generic_hits = sum(1 for pattern in GENERIC_PLATFORM_PATTERNS if pattern.search(description))
    contrast_tokens = ("instead of", "without adding", "rather than")
    contrast_bonus = 0.08 if any(t in description.lower() for t in contrast_tokens) else 0.0
    score = (
        0.20
        + min(0.42, use_case_hits * 0.12)
        + min(0.18, audience_hits * 0.07)
        + (_description_quality(description) * 0.18)
        + contrast_bonus
        - min(0.40, generic_hits * 0.10)
    )
    return _clamp(score)


def _enterprise_penalty(candidate: RepositoryCandidate) -> float:
    haystack = "{0} {1}".format(candidate.name, candidate.description or "")
    matches = sum(1 for pattern in ENTERPRISE_PATTERNS if pattern.search(haystack))
    return _clamp(matches / 3.0)


def _recency_score(published_at: datetime, now: datetime, recent_window_days: int) -> float:
    age_days = max(0.0, (now - published_at).total_seconds() / 86400.0)
    return _clamp(1.0 - (age_days / max(1, recent_window_days)))


def _star_sweet_spot_score(stars: int, floor: int, ceiling: int) -> float:
    if stars <= 0:
        return 0.0
    if floor <= stars <= ceiling:
        return 1.0
    if stars < floor:
        return _clamp(stars / max(1, floor))
    return _clamp(ceiling / max(stars, ceiling + 1))


def _stars_score(stars: int) -> float:
    return _clamp(math.log10(max(stars, 1)) / 5.0)


def _velocity_score(candidate: RepositoryCandidate, previous_snapshot: Optional[Dict], now: datetime) -> float:
    if not previous_snapshot:
        return 0.5
    previous_stars = int(previous_snapshot.get("stargazers_count") or 0)
    captured_at = previous_snapshot.get("captured_at")
    if not captured_at or not isinstance(captured_at, datetime):
        return 0.5
    days = max(1.0 / 24.0, (now - captured_at).total_seconds() / 86400.0)
    delta = max(0, candidate.stargazers_count - previous_stars)
    velocity = delta / days
    return _clamp(math.log10(velocity + 1.0) / 2.5)


def _novelty_score(
    candidate: RepositoryCandidate,
    history: Iterable[PostedRepositorySnapshot],
    settings: Settings,
    now: datetime,
) -> float:
    penalty = 0.0
    candidate_topics = set(candidate.matched_topics or candidate.topics)
    candidate_similarity = similarity_key(candidate)
    for posted in history:
        age_days = max(0.0, (now - posted.posted_at).total_seconds() / 86400.0)
        overlap = candidate_topics.intersection(set(posted.topics))
        if overlap and age_days < settings.cooldown.topic_days:
            penalty += 0.35
        name_similarity = SequenceMatcher(None, posted.repo_name.lower(), candidate.name.lower()).ratio()
        if age_days < settings.cooldown.similarity_days:
            if (
                posted.similarity_key == candidate_similarity
                or name_similarity >= settings.cooldown.similarity_threshold
            ):
                penalty += 0.45
    return _clamp(1.0 - penalty)


def score_candidate(
    candidate: RepositoryCandidate,
    settings: Settings,
    now: datetime,
    history: Iterable[PostedRepositorySnapshot],
    previous_snapshots: Dict[int, Dict],
) -> RepositoryCandidate:
    previous_snapshot = previous_snapshots.get(candidate.repo_id)
    recency = _recency_score(candidate.pushed_at, now, settings.github.recent_activity_days)
    activity = _recency_score(candidate.updated_at, now, settings.github.recent_activity_days)
    description_quality = _description_quality(candidate.description)
    topic_relevance = _topic_relevance(candidate, settings.github.topics)
    url_quality = _url_quality(candidate)
    docs_signal = _docs_signal(candidate)
    explainability = _explainability_score(candidate)
    operator_angle = _operator_angle_score(candidate)
    enterprise_penalty = _enterprise_penalty(candidate)
    star_sweet_spot = _star_sweet_spot_score(
        candidate.stargazers_count,
        settings.github.discoverability_star_floor,
        settings.github.discoverability_star_ceiling,
    )
    velocity = _velocity_score(candidate, previous_snapshot, now)
    novelty = _novelty_score(candidate, history, settings, now)

    discoverability = _clamp(
        (
            star_sweet_spot
            + docs_signal
            + description_quality
            + topic_relevance
            + explainability
            + (1.0 - enterprise_penalty)
        )
        / 6.0
    )
    trend = _clamp((recency * 0.35) + (activity * 0.35) + (velocity * 0.30))
    content_fit = _clamp(
        (topic_relevance * 0.30)
        + (explainability * 0.25)
        + (url_quality * 0.15)
        + (operator_angle * 0.30)
    )
    stars_score = _stars_score(candidate.stargazers_count)
    total = _clamp(
        (discoverability * 0.40)
        + (trend * 0.24)
        + (novelty * 0.20)
        + (content_fit * 0.10)
        + (stars_score * 0.06)
    )

    candidate.discoverability_score = round(discoverability, 6)
    candidate.trend_score = round(trend, 6)
    candidate.novelty_score = round(novelty, 6)
    candidate.content_fit_score = round(content_fit, 6)
    candidate.score = round(total, 6)
    candidate.metrics = {
        "star_velocity_score": round(velocity, 6),
        "star_sweet_spot": round(star_sweet_spot, 6),
        "docs_signal": round(docs_signal, 6),
        "operator_angle": round(operator_angle, 6),
        "enterprise_penalty": round(enterprise_penalty, 6),
    }
    candidate.score_breakdown = {
        "discoverability": candidate.discoverability_score,
        "trend": candidate.trend_score,
        "novelty": candidate.novelty_score,
        "content_fit": candidate.content_fit_score,
        "recency": round(recency, 6),
        "activity": round(activity, 6),
        "topic_relevance": round(topic_relevance, 6),
        "description_quality": round(description_quality, 6),
        "url_quality": round(url_quality, 6),
        "docs_signal": round(docs_signal, 6),
        "operator_angle": round(operator_angle, 6),
        "star_velocity": round(velocity, 6),
        "star_sweet_spot": round(star_sweet_spot, 6),
        "enterprise_penalty": round(enterprise_penalty, 6),
        "total": candidate.score,
    }
    return candidate


def rank_candidates(
    candidates: Iterable[RepositoryCandidate],
    settings: Settings,
    now: datetime,
    history: Iterable[PostedRepositorySnapshot] = (),
    previous_snapshots: Optional[Dict[int, Dict]] = None,
) -> List[RepositoryCandidate]:
    snapshots = previous_snapshots or {}
    scored = [
        score_candidate(candidate, settings, now, history=history, previous_snapshots=snapshots)
        for candidate in candidates
    ]
    return sorted(
        scored,
        key=lambda candidate: (
            candidate.score,
            candidate.discoverability_score,
            candidate.trend_score,
            candidate.updated_at,
        ),
        reverse=True,
    )
