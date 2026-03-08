from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable, List, Tuple
from urllib.parse import urlparse

from threads_github_bot.config import Settings
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _homepage_domain(homepage: str) -> str:
    if not homepage:
        return ""
    parsed = urlparse(homepage if "://" in homepage else "https://" + homepage)
    return parsed.netloc.lower().removeprefix("www.")


def similarity_key(candidate: RepositoryCandidate) -> str:
    parts = [_normalize_name(candidate.name)]
    domain = _homepage_domain(candidate.homepage)
    if domain and "github.com" not in domain:
        parts.append(domain)
    return "|".join(part for part in parts if part)


def evaluate_cooldown(
    candidate: RepositoryCandidate,
    history: Iterable[PostedRepositorySnapshot],
    settings: Settings,
    now,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    candidate_key = similarity_key(candidate)
    candidate_topics = set(candidate.matched_topics or candidate.topics)
    topic_families = settings.cooldown.topic_families

    for posted in history:
        age_days = (now - posted.posted_at).total_seconds() / 86400.0

        if posted.full_name == candidate.full_name and age_days < settings.cooldown.repo_days:
            reasons.append(
                "repo_cooldown:{name}:{days:.1f}".format(name=posted.full_name, days=age_days)
            )

        name_similarity = SequenceMatcher(
            None,
            _normalize_name(posted.repo_name),
            _normalize_name(candidate.name),
        ).ratio()
        if candidate_key and age_days < settings.cooldown.similarity_days:
            if posted.similarity_key == candidate_key or name_similarity >= settings.cooldown.similarity_threshold:
                reasons.append(
                    "similarity_cooldown:{name}:{days:.1f}".format(name=posted.full_name, days=age_days)
                )

        overlap = candidate_topics.intersection(set(posted.topics))
        if overlap and age_days < settings.cooldown.topic_days:
            reasons.append(
                "topic_cooldown:{topic}:{days:.1f}".format(topic=sorted(overlap)[0], days=age_days)
            )

        if topic_families and age_days < settings.cooldown.topic_days:
            posted_topics = set(posted.topics)
            for family_name, members in topic_families.items():
                family_set = set(members)
                if candidate_topics.intersection(family_set) and posted_topics.intersection(family_set):
                    reasons.append(
                        "topic_family_cooldown:{family}:{days:.1f}".format(
                            family=family_name,
                            days=age_days,
                        )
                    )
                    break

    return (len(reasons) == 0, reasons)
