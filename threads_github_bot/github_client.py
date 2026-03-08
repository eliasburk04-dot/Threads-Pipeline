from __future__ import annotations

import logging
from typing import Dict, List, Optional

import httpx

from threads_github_bot.config import Settings
from threads_github_bot.models import RepositoryCandidate, parse_datetime

LOGGER = logging.getLogger(__name__)


class GitHubDiscoveryClient:
    def __init__(self, settings: Settings, http_client: Optional[httpx.Client] = None) -> None:
        self.settings = settings
        if http_client is not None:
            self.http_client = http_client
            self._owns_client = False
        else:
            headers = {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": settings.runtime.app_name,
            }
            if settings.github.token:
                headers["Authorization"] = "Bearer {0}".format(settings.github.token)
            self.http_client = httpx.Client(
                base_url="https://api.github.com",
                headers=headers,
                timeout=settings.github.timeout_seconds,
            )
            self._owns_client = True

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()

    def fetch_candidates(self) -> List[RepositoryCandidate]:
        candidates: Dict[str, RepositoryCandidate] = {}
        for topic in self.settings.github.topics:
            params = {
                "q": "topic:{topic} archived:false fork:false stars:>={stars}".format(
                    topic=topic,
                    stars=self.settings.github.min_stars,
                ),
                "sort": "updated",
                "order": "desc",
                "per_page": str(self.settings.github.fetch_limit_per_topic),
            }
            response = self.http_client.get("/search/repositories", params=params)
            response.raise_for_status()
            payload = response.json()
            for item in payload.get("items", []):
                candidate = self._normalize_item(item, topic)
                existing = candidates.get(candidate.full_name)
                if existing:
                    merged_topics = tuple(
                        sorted(set(existing.topics).union(candidate.topics).union(candidate.matched_topics))
                    )
                    merged_matches = tuple(
                        sorted(set(existing.matched_topics).union(candidate.matched_topics))
                    )
                    existing.topics = merged_topics
                    existing.matched_topics = merged_matches
                    continue
                candidates[candidate.full_name] = candidate

        LOGGER.info("github_fetch_complete", extra={"candidate_count": len(candidates)})
        return list(candidates.values())

    def _normalize_item(self, item, matched_topic: str) -> RepositoryCandidate:
        topics = tuple(sorted(set(item.get("topics") or (matched_topic,))))
        matched_topics = tuple(sorted(set((matched_topic,)).union(topics)))
        return RepositoryCandidate(
            repo_id=int(item["id"]),
            full_name=item["full_name"],
            name=item["name"],
            owner=item["owner"]["login"],
            description=(item.get("description") or "").strip(),
            html_url=item["html_url"],
            homepage=(item.get("homepage") or "").strip(),
            topics=topics,
            matched_topics=matched_topics,
            language=item.get("language"),
            stargazers_count=int(item.get("stargazers_count") or 0),
            forks_count=int(item.get("forks_count") or 0),
            archived=bool(item.get("archived")),
            fork=bool(item.get("fork")),
            pushed_at=parse_datetime(item["pushed_at"]),
            updated_at=parse_datetime(item["updated_at"]),
        )

