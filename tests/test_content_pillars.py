"""Tests for the content pillar rotation system."""
from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.content_pillars import (
    PILLAR_CATALOG,
    _extract_recent_pillar_slugs,
    select_next_pillar,
)
from threads_github_bot.models import PostedRepositorySnapshot


def _posted(
    pillar_slug: str,
    days_ago: int,
    series_slug: str = "hidden-github-gem",
) -> PostedRepositorySnapshot:
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
    return PostedRepositorySnapshot(
        repo_id=1,
        full_name="acme/old",
        repo_name="old",
        owner="acme",
        topics=("ai",),
        similarity_key="old",
        homepage="",
        posted_at=now - timedelta(days=days_ago),
        post_text="older thread",
        series_slug=series_slug,
        thread_text="older thread",
        pillar_slug=pillar_slug,
    )


def test_pillar_selection_avoids_consecutive_same_pillar(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
    history = [
        _posted("repo-discovery", 0),
        _posted("repo-discovery", 1),
    ]

    pillar = select_next_pillar(settings, history, now=now)

    assert pillar.slug != "repo-discovery"
    assert pillar.slug in PILLAR_CATALOG


def test_pillar_selection_prefers_repo_discovery_with_empty_history(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)

    pillar = select_next_pillar(settings, [], now=now)

    # With no history, repo-discovery should score highest because it has the highest weight
    assert pillar.slug == "repo-discovery"


def test_pillar_selection_rotates_through_different_pillars(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
    history = [
        _posted("repo-discovery", 0),
        _posted("hot-take", 1),
        _posted("repo-discovery", 2),
        _posted("question", 3),
    ]

    pillar = select_next_pillar(settings, history, now=now)

    # Should not repeat the most recent pillar (repo-discovery) immediately
    assert pillar.slug != "repo-discovery"
    assert pillar.slug in PILLAR_CATALOG


def test_pillar_selection_respects_configured_pillar_slugs(tmp_path) -> None:
    settings = Settings.from_env({
        "APP_BASE_DIR": str(tmp_path),
        "CONTENT_PILLAR_SLUGS": "repo-discovery,hot-take",
    })
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)

    pillar = select_next_pillar(settings, [], now=now)

    assert pillar.slug in ("repo-discovery", "hot-take")


def test_extract_recent_pillar_slugs_defaults_to_repo_discovery() -> None:
    history = [
        PostedRepositorySnapshot(
            repo_id=1,
            full_name="acme/old",
            repo_name="old",
            owner="acme",
            topics=("ai",),
            similarity_key="old",
            homepage="",
            posted_at=datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc),
            post_text="older thread",
            series_slug="hidden-github-gem",
            thread_text="older thread",
            # no pillar_slug attribute — simulates legacy data
        ),
    ]

    slugs = _extract_recent_pillar_slugs(history)

    assert slugs == ["repo-discovery"]


def test_pillar_catalog_has_expected_pillars() -> None:
    expected = {"repo-discovery", "hot-take", "tool-comparison", "question", "workflow-breakdown"}
    assert set(PILLAR_CATALOG.keys()) == expected
    for pillar in PILLAR_CATALOG.values():
        assert pillar.weight > 0
        assert pillar.label


def test_repo_discovery_is_the_only_pillar_needing_repo() -> None:
    for slug, pillar in PILLAR_CATALOG.items():
        if slug == "repo-discovery":
            assert pillar.needs_repo is True
        else:
            assert pillar.needs_repo is False
