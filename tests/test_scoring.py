from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate
from threads_github_bot.scoring import rank_candidates


def _candidate(
    full_name: str,
    stars: int,
    pushed_days_ago: int,
    description: str,
    topics: tuple[str, ...],
    homepage: str = "",
) -> RepositoryCandidate:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        description=description,
        html_url=f"https://github.com/{full_name}",
        homepage=homepage,
        topics=topics,
        matched_topics=topics,
        language="Python",
        stargazers_count=stars,
        forks_count=10,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=pushed_days_ago),
        updated_at=now - timedelta(days=pushed_days_ago),
    )


def _posted(full_name: str, days_ago: int, topics: tuple[str, ...]) -> PostedRepositorySnapshot:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return PostedRepositorySnapshot(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        repo_name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        topics=topics,
        similarity_key=full_name.split("/", 1)[1],
        homepage="",
        posted_at=now - timedelta(days=days_ago),
        post_text="older thread",
        series_slug="hidden-github-gem",
        thread_text="older thread",
    )


def test_rank_candidates_prefers_discoverable_repositories_over_generic_large_ones(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)

    candidates = [
        _candidate(
            "octo/solid-tooling",
            stars=2_400,
            pushed_days_ago=2,
            description="An open source developer workflow agent for AI-assisted code review with a clear demo site.",
            topics=("ai", "developer-tools", "agents"),
            homepage="https://solid-tooling.dev",
        ),
        _candidate(
            "octo/huge-enterprise-platform",
            stars=80_000,
            pushed_days_ago=2,
            description=(
                "Enterprise orchestration platform for organizations managing "
                "internal governance workflows at scale."
            ),
            topics=("ai",),
        ),
        _candidate(
            "octo/noisy-project",
            stars=600,
            pushed_days_ago=10,
            description="Misc stuff.",
            topics=("productivity",),
        ),
    ]

    ranked = rank_candidates(candidates, settings, now, history=[], previous_snapshots={})

    assert [candidate.full_name for candidate in ranked][:2] == [
        "octo/solid-tooling",
        "octo/huge-enterprise-platform",
    ]
    assert ranked[0].discoverability_score > ranked[1].discoverability_score
    assert ranked[0].score > ranked[1].score > ranked[2].score
    assert {"discoverability", "trend", "novelty", "content_fit", "total"} <= set(ranked[0].score_breakdown)


def test_rank_candidates_penalizes_recent_topic_exposure(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    history = [_posted("acme/recent-agent-tool", 1, ("agents", "ai"))]

    candidates = [
        _candidate(
            "acme/new-agent-tool",
            stars=1_300,
            pushed_days_ago=1,
            description="An agent framework with a concrete developer workflow use case and public demo.",
            topics=("agents", "ai"),
            homepage="https://agent-tool.dev",
        ),
        _candidate(
            "acme/new-rag-tool",
            stars=1_200,
            pushed_days_ago=1,
            description="A RAG workflow tool with a concrete developer workflow use case and public demo.",
            topics=("rag", "developer-tools"),
            homepage="https://rag-tool.dev",
        ),
    ]

    ranked = rank_candidates(candidates, settings, now, history=history, previous_snapshots={})

    assert ranked[0].full_name == "acme/new-rag-tool"
    assert ranked[0].novelty_score > ranked[1].novelty_score


def test_rank_candidates_prefers_clear_operator_angle_over_generic_platform_language(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)

    candidates = [
        _candidate(
            "acme/workflow-focused-tool",
            stars=1_700,
            pushed_days_ago=1,
            description=(
                "A developer tool that helps teams review pull requests faster and test agent workflows "
                "without adding a broader platform."
            ),
            topics=("ai", "developer-tools"),
            homepage="https://workflow-focused.dev",
        ),
        _candidate(
            "acme/generic-ai-platform",
            stars=1_700,
            pushed_days_ago=1,
            description="A flexible AI platform for organizations managing orchestration workflows at scale.",
            topics=("ai", "developer-tools"),
            homepage="https://generic-ai.dev",
        ),
    ]

    ranked = rank_candidates(candidates, settings, now, history=[], previous_snapshots={})

    assert ranked[0].full_name == "acme/workflow-focused-tool"
    assert ranked[0].content_fit_score > ranked[1].content_fit_score
    assert ranked[0].score_breakdown["operator_angle"] > ranked[1].score_breakdown["operator_angle"]
