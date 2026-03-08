from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.deduplication import evaluate_cooldown
from threads_github_bot.models import PostedRepositorySnapshot, RepositoryCandidate


def _candidate(full_name: str, topics: tuple[str, ...], homepage: str = "") -> RepositoryCandidate:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        description="An AI automation tool for developer workflows and repo discovery.",
        html_url=f"https://github.com/{full_name}",
        homepage=homepage,
        topics=topics,
        matched_topics=topics,
        language="Python",
        stargazers_count=500,
        forks_count=20,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=2),
        updated_at=now - timedelta(days=2),
    )


def _posted(
    full_name: str,
    posted_days_ago: int,
    topics: tuple[str, ...],
    similarity_key: str,
    series_slug: str = "hidden-github-gem",
) -> PostedRepositorySnapshot:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return PostedRepositorySnapshot(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        repo_name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        topics=topics,
        similarity_key=similarity_key,
        homepage="",
        posted_at=now - timedelta(days=posted_days_ago),
        post_text="A clean post",
        series_slug=series_slug,
        thread_text="A clean thread",
    )


def test_evaluate_cooldown_flags_same_repo(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path), "COOLDOWN_REPO_DAYS": "7"})
    candidate = _candidate("octo/agentic-tools", ("ai", "agents"))
    history = [
        _posted(
            "octo/agentic-tools",
            posted_days_ago=2,
            topics=("ai", "agents"),
            similarity_key="agentic-tools",
        )
    ]

    allowed, reasons = evaluate_cooldown(candidate, history, settings, datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc))

    assert allowed is False
    assert "repo_cooldown" in reasons[0]


def test_evaluate_cooldown_flags_similarity_and_topic_overlap(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "COOLDOWN_SIMILARITY_DAYS": "14",
            "COOLDOWN_TOPIC_DAYS": "5",
        }
    )
    candidate = _candidate(
        "acme/agentic-toolkit",
        ("agents", "developer-tools"),
        homepage="https://agentic.dev",
    )
    history = [
        _posted(
            "acme/agent-toolkit",
            posted_days_ago=3,
            topics=("agents", "developer-tools"),
            similarity_key="agentic-toolkit|agentic.dev",
        )
    ]

    allowed, reasons = evaluate_cooldown(candidate, history, settings, datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc))

    assert allowed is False
    assert any(reason.startswith("similarity_cooldown") for reason in reasons)
    assert any(reason.startswith("topic_cooldown") for reason in reasons)


def test_evaluate_cooldown_allows_fresh_candidate(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    candidate = _candidate("acme/fresh-find", ("rag", "saas"))
    history = [
        _posted(
            "acme/old-find",
            posted_days_ago=45,
            topics=("developer-tools",),
            similarity_key="old-find",
        )
    ]

    allowed, reasons = evaluate_cooldown(candidate, history, settings, datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc))

    assert allowed is True
    assert reasons == []


def test_evaluate_cooldown_flags_topic_family_overlap(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "COOLDOWN_TOPIC_FAMILIES": "agents=agents|coding-agents|ai-agents",
            "COOLDOWN_TOPIC_DAYS": "7",
        }
    )
    candidate = _candidate("acme/coding-agent-kit", ("coding-agents", "developer-tools"))
    history = [
        _posted(
            "acme/agent-runtime",
            posted_days_ago=2,
            topics=("agents", "ai"),
            similarity_key="agent-runtime",
        )
    ]

    allowed, reasons = evaluate_cooldown(candidate, history, settings, datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc))

    assert allowed is False
    assert any(reason.startswith("topic_family_cooldown:agents") for reason in reasons)
