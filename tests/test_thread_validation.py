from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.models import RepositoryCandidate, ThreadPost
from threads_github_bot.validation import ThreadValidator


def _candidate() -> RepositoryCandidate:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=123,
        full_name="acme/quiet-automation",
        name="quiet-automation",
        owner="acme",
        description="A Python tool that discovers useful open source repos and drafts grounded Threads threads.",
        html_url="https://github.com/acme/quiet-automation",
        homepage="https://quiet-automation.dev",
        topics=("ai", "developer-tools", "automation"),
        matched_topics=("ai", "developer-tools"),
        language="Python",
        stargazers_count=320,
        forks_count=12,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=2),
        updated_at=now - timedelta(days=2),
    )


def test_thread_validator_accepts_grounded_five_post_thread(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = ThreadValidator(settings, duplicate_lookup=lambda _text: False)

    posts = (
        ThreadPost(
            position=1,
            role="hook",
            text=(
                "I keep seeing repo discovery turn into noisy browsing. quiet-automation is a more focused take."
            ),
        ),
        ThreadPost(
            position=2,
            role="use_case",
            text=(
                "It is a Python project that turns repo metadata into grounded draft Threads content "
                "without turning the workflow into another generic platform."
            ),
        ),
        ThreadPost(
            position=3,
            role="operator_take",
            text=(
                "My bias is toward tools with one clear job, and this one is easier to reason about than a broad suite."
            ),
        ),
        ThreadPost(
            position=4,
            role="who_its_for",
            text=(
                "This feels useful for builders who want a quieter workflow around developer tooling."
            ),
        ),
        ThreadPost(
            position=5,
            role="soft_cta",
            text=(
                "If this is close to your stack, repo is here: https://github.com/acme/quiet-automation. "
                "I would mostly test whether it fits an existing workflow."
            ),
        ),
    )

    result = validator.validate(_candidate(), posts)

    assert result.is_valid is True
    assert result.reasons == []


def test_thread_validator_rejects_repetitive_or_missing_url_threads(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path), "THREAD_POST_COUNT_MIN": "4"})
    validator = ThreadValidator(settings, duplicate_lookup=lambda _text: False)

    posts = (
        ThreadPost(position=1, role="hook", text="Best tool ever!!!"),
        ThreadPost(position=2, role="use_case", text="Best tool ever!!!"),
        ThreadPost(position=3, role="operator_take", text="Best tool ever!!!"),
        ThreadPost(position=4, role="who_its_for", text="Best tool ever!!!"),
    )

    result = validator.validate(_candidate(), posts)

    assert result.is_valid is False
    assert any(reason.startswith("thread_repetition") for reason in result.reasons)
    assert any("missing_repo_url_in_thread" in reason for reason in result.reasons)
