from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.models import RepositoryCandidate
from threads_github_bot.validation import PostValidator


def _candidate() -> RepositoryCandidate:
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=123,
        full_name="acme/quiet-automation",
        name="quiet-automation",
        owner="acme",
        description=(
            "A Python tool that discovers useful open source repos and drafts grounded social posts."
        ),
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


def test_validator_accepts_grounded_post(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = PostValidator(settings, duplicate_lookup=lambda _text: False)

    result = validator.validate(
        _candidate(),
        (
            "I keep bookmarking tools like this.\n\n"
            "quiet-automation surfaces promising open source repos and turns the metadata into "
            "grounded draft posts.\n\n"
            "What stood out to me:\n"
            "- Python-first setup\n"
            "- clear focus on repo discovery\n"
            "- practical automation angle\n\n"
            "Worth a look if you track developer tools.\n"
            "https://github.com/acme/quiet-automation"
        ),
    )

    assert result.is_valid is True
    assert result.reasons == []


def test_validator_rejects_duplicates_spam_and_length(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path), "CONTENT_MAX_TEXT_BYTES": "100"})
    validator = PostValidator(settings, duplicate_lookup=lambda _text: True)

    text = (
        "THIS WILL CHANGE EVERYTHING!!!\n\n"
        "Best tool ever!!! Best tool ever!!! Best tool ever!!!\n"
        "https://github.com/acme/quiet-automation"
    )
    result = validator.validate(_candidate(), text)

    assert result.is_valid is False
    assert "duplicate_text" in result.reasons
    assert any(reason.startswith("spam_pattern") for reason in result.reasons)
    assert any(reason.startswith("text_too_long") for reason in result.reasons)


def test_validator_rejects_unsupported_claims(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = PostValidator(settings, duplicate_lookup=lambda _text: False)
    text = (
        "quiet-automation ships with a built-in vector database and Slack approval flows.\n\n"
        "That makes it interesting for teams automating repo discovery.\n"
        "https://github.com/acme/quiet-automation"
    )

    result = validator.validate(_candidate(), text)

    assert result.is_valid is False
    assert any(reason.startswith("unsupported_claim") for reason in result.reasons)
