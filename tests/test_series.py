from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate
from threads_github_bot.series import select_series_for_candidate


def _candidate(full_name: str, topics: tuple[str, ...], trend_score: float = 0.8) -> RepositoryCandidate:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        description="A clear AI repo with a developer-friendly use case and strong docs signal.",
        html_url=f"https://github.com/{full_name}",
        homepage="https://example.dev",
        topics=topics,
        matched_topics=topics,
        language="Python",
        stargazers_count=1_200,
        forks_count=40,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=1),
        updated_at=now - timedelta(days=1),
        trend_score=trend_score,
        discoverability_score=0.85,
        novelty_score=0.9,
    )


def _posted(series_slug: str, days_ago: int) -> PostedRepositorySnapshot:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
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
    )


def test_series_selection_rotates_away_from_recent_series(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path), "SERIES_ENABLE_NUMBERING": "true"})

    series = select_series_for_candidate(
        _candidate("acme/fast-ai", ("ai", "llm")),
        settings,
        recent_history=[_posted("hidden-github-gem", 1), _posted("ai-repo-worth-trying", 2)],
    )

    assert series.slug != "hidden-github-gem"
    assert series.display_label.startswith(series.label)


def test_series_selection_prefers_trending_ai_format_for_high_trend_candidate(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})

    series = select_series_for_candidate(
        _candidate("acme/trending-ai", ("ai", "agents"), trend_score=0.95),
        settings,
        recent_history=[],
    )

    assert series.slug == "trending-ai-repo"
