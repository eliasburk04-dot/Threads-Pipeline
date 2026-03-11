"""Tests for standalone content generation and the standalone pipeline path."""
from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.content_pillars import PILLAR_CATALOG
from threads_github_bot.models import (
    GeneratedThread,
    PublishThreadResult,
    RepositoryCandidate,
    ThreadPost,
    ThreadPublishPostResult,
    ValidationResult,
)
from threads_github_bot.pipeline import ThreadsGitHubPipeline
from threads_github_bot.standalone_generation import build_standalone_placeholder
from threads_github_bot.state import SQLiteStateStore

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeDiscoveryClient:
    def __init__(self, candidates=None):
        self._candidates = candidates or []

    def fetch_candidates(self):
        return list(self._candidates)


class FakeGenerator:
    def __init__(self):
        self.calls = []

    def generate(self, candidate, series, mode, validation_reasons=()):
        self.calls.append((candidate.full_name, series.slug, mode))
        return GeneratedThread(
            repo=candidate,
            posts=(
                ThreadPost(
                    position=1,
                    role="hook",
                    text="I keep noticing tool sprawl. {0} is a more focused take.".format(candidate.name),
                ),
                ThreadPost(
                    position=2,
                    role="use_case",
                    text="It is an open source repo for developer workflows.",
                ),
                ThreadPost(
                    position=3,
                    role="operator_take",
                    text="My bias is toward tools with a narrow job and a clear entry point.",
                ),
                ThreadPost(
                    position=4,
                    role="who_its_for",
                    text="Useful for builders who want a sharper workflow fit, not a giant suite.",
                ),
            ),
            language="en",
            model="test-model",
            raw_response={"ok": True},
            prompt_version="v2",
            mode=mode,
            series_slug=series.slug,
            series_label=series.label,
            series_number=series.number,
        )


class FakePublisher:
    def __init__(self):
        self.calls = []

    def publish_thread(self, posts):
        self.calls.append(posts)
        return PublishThreadResult(
            success=True,
            posts=[
                ThreadPublishPostResult(
                    success=True,
                    container_id="container_{0}".format(i),
                    media_id="thread_{0}".format(i),
                    response={"ok": True},
                )
                for i, _ in enumerate(posts, start=1)
            ],
        )


class FakeStandaloneGenerator:
    """Fake standalone generator that returns a fixed thread."""

    def __init__(self, settings):
        self.settings = settings
        self.calls = []

    def generate(self, pillar, mode, topic_hint=None):
        self.calls.append((pillar.slug, mode, topic_hint))
        placeholder = build_standalone_placeholder(pillar, "AI agents")
        return GeneratedThread(
            repo=placeholder,
            posts=(
                ThreadPost(
                    position=1,
                    role="hot_take",
                    text=(
                        "Most AI agent frameworks are solving the wrong problem. "
                        "The bottleneck is not orchestration — it is knowing when to stop adding layers."
                    ),
                ),
                ThreadPost(
                    position=2,
                    role="follow_up",
                    text=(
                        "I have tested a few and the ones that actually ship do one thing well. "
                        "The rest are demo-ware."
                    ),
                ),
            ),
            language="en",
            model="test-model",
            raw_response={"ok": True},
            prompt_version="v2",
            mode=mode,
            series_slug=pillar.slug,
            series_label=pillar.label,
            series_number=None,
            pillar_slug=pillar.slug,
        )


class AlwaysValidValidator:
    def validate(self, candidate, posts):
        return ValidationResult(is_valid=True, reasons=[])

    def validate_standalone(self, posts):
        return ValidationResult(is_valid=True, reasons=[])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _candidate(full_name, stars=1_400, pushed_days_ago=1, topics=("ai", "developer-tools")):
    now = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        description="A useful AI tool for developers with a clear repo description.",
        html_url="https://github.com/{0}".format(full_name),
        homepage="https://example.dev",
        topics=topics,
        matched_topics=topics,
        language="Python",
        stargazers_count=stars,
        forks_count=20,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=pushed_days_ago),
        updated_at=now - timedelta(days=pushed_days_ago),
    )


def test_pipeline_runs_standalone_for_hot_take_pillar(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    standalone_gen = FakeStandaloneGenerator(settings)
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([]),
        generator=FakeGenerator(),
        publisher=FakePublisher(),
        validator=AlwaysValidValidator(),
        standalone_generator=standalone_gen,
    )

    result = pipeline.run(mode="dry_run", pillar_override="hot-take")

    assert result.status == "dry_run_ready"
    assert result.selected_repo == "standalone/hot-take"
    assert result.selected_series == "Operator Hot Take"
    assert standalone_gen.calls
    assert standalone_gen.calls[0][0] == "hot-take"


def test_pipeline_falls_back_to_repo_based_when_no_standalone_generator(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    candidate = _candidate("acme/repo-tool")
    generator = FakeGenerator()
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([candidate]),
        generator=generator,
        publisher=FakePublisher(),
        validator=AlwaysValidValidator(),
        standalone_generator=None,  # no standalone generator
    )

    # Even if pillar selection picks a standalone pillar, the pipeline
    # should fall back to repo-based because standalone_generator is None
    result = pipeline.run(mode="dry_run", pillar_override="hot-take")

    # Falls through to repo-based path
    assert result.status == "dry_run_ready"
    assert result.selected_repo == "acme/repo-tool"


def test_pipeline_standalone_publishes_posts(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    publisher = FakePublisher()
    standalone_gen = FakeStandaloneGenerator(settings)
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([]),
        generator=FakeGenerator(),
        publisher=publisher,
        validator=AlwaysValidValidator(),
        standalone_generator=standalone_gen,
    )

    result = pipeline.run(mode="post_now", pillar_override="hot-take")

    assert result.status == "published"
    assert publisher.calls
    assert len(publisher.calls[0]) == 2  # hot take produces 2 posts


def test_standalone_placeholder_has_valid_structure() -> None:
    pillar = PILLAR_CATALOG["hot-take"]
    placeholder = build_standalone_placeholder(pillar, "AI agents")

    assert placeholder.repo_id >= 900_000_000
    assert placeholder.full_name == "standalone/hot-take"
    assert placeholder.owner == "standalone"
    assert "ai-agents" in placeholder.topics


def test_standalone_validation_accepts_clean_posts(tmp_path) -> None:
    from threads_github_bot.validation import ThreadValidator

    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = ThreadValidator(settings, duplicate_lookup=lambda _: False)

    posts = (
        ThreadPost(
            position=1,
            role="hot_take",
            text="Most AI agent frameworks solve the wrong problem. The bottleneck is taste, not orchestration.",
        ),
    )

    result = validator.validate_standalone(posts)

    assert result.is_valid is True
    assert result.reasons == []


def test_standalone_validation_rejects_empty_thread(tmp_path) -> None:
    from threads_github_bot.validation import ThreadValidator

    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = ThreadValidator(settings, duplicate_lookup=lambda _: False)

    result = validator.validate_standalone([])

    assert result.is_valid is False
    assert "standalone_empty" in result.reasons


def test_standalone_validation_rejects_spam(tmp_path) -> None:
    from threads_github_bot.validation import ThreadValidator

    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    validator = ThreadValidator(settings, duplicate_lookup=lambda _: False)

    posts = (
        ThreadPost(
            position=1,
            role="hot_take",
            text="This will CHANGE EVERYTHING!!! Best tool ever!!! GAME CHANGER!!!",
        ),
    )

    result = validator.validate_standalone(posts)

    assert result.is_valid is False
    assert any("spam_pattern" in r for r in result.reasons)


def test_pillar_slug_persisted_in_recent_threads(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    standalone_gen = FakeStandaloneGenerator(settings)
    publisher = FakePublisher()
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([]),
        generator=FakeGenerator(),
        publisher=publisher,
        validator=AlwaysValidValidator(),
        standalone_generator=standalone_gen,
    )

    result = pipeline.run(mode="post_now", pillar_override="hot-take")

    assert result.status == "published"
    recent = store.list_recent_threads(limit=5)
    assert len(recent) == 1
    assert recent[0]["pillar_slug"] == "hot-take"


def test_config_parses_pillar_slugs_and_standalone_topics(tmp_path) -> None:
    settings = Settings.from_env({
        "APP_BASE_DIR": str(tmp_path),
        "CONTENT_PILLAR_SLUGS": "repo-discovery,hot-take,question",
        "CONTENT_STANDALONE_TOPICS": "AI agents,developer productivity,open source",
    })

    assert settings.content.pillar_slugs == ("repo-discovery", "hot-take", "question")
    assert settings.content.standalone_topics == ("AI agents", "developer productivity", "open source")


def test_config_defaults_include_all_pillars(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})

    assert "repo-discovery" in settings.content.pillar_slugs
    assert "hot-take" in settings.content.pillar_slugs
    assert "question" in settings.content.pillar_slugs
    assert "tool-comparison" in settings.content.pillar_slugs
    assert "workflow-breakdown" in settings.content.pillar_slugs
    assert len(settings.content.standalone_topics) >= 5


# ---------------------------------------------------------------------------
# Regression tests for March 9-10 2026 outage
# ---------------------------------------------------------------------------

def test_parse_standalone_posts_handles_non_integer_position() -> None:
    """Regression: OpenAI sometimes returns 'founder' / 'neutral' as position value.
    _parse_standalone_posts must not raise ValueError in that case.
    """
    from threads_github_bot.standalone_generation import _parse_standalone_posts

    payload = {
        "posts": [
            {"position": "founder", "role": "hook", "text": "First post text here."},
            {"position": "neutral", "role": "follow_up", "text": "Second post text here."},
            {"position": None, "role": "cta", "text": "Third post text here."},
            {"position": 4, "role": "cta", "text": "Fourth post text here."},
        ]
    }
    posts = _parse_standalone_posts(payload)

    assert len(posts) == 4
    # Fallback positions are assigned sequentially when value is unparseable
    assert posts[0].position == 1
    assert posts[1].position == 2
    assert posts[2].position == 3
    assert posts[3].position == 4  # integer passes through as-is


def test_pipeline_falls_back_to_repo_when_standalone_generate_raises(tmp_path) -> None:
    """Regression: when standalone generator raises an exception the pipeline must
    fall back to the repo-based path instead of returning standalone_generation_failed.
    """
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    class BrokenStandaloneGenerator:
        def generate(self, pillar, mode, topic_hint=None):
            raise ValueError("invalid literal for int() with base 10: 'founder'")

    candidate = _candidate("acme/fallback-repo")
    generator = FakeGenerator()
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([candidate]),
        generator=generator,
        publisher=FakePublisher(),
        validator=AlwaysValidValidator(),
        standalone_generator=BrokenStandaloneGenerator(),
    )

    result = pipeline.run(mode="dry_run", pillar_override="question")

    # Must NOT return standalone_generation_failed — must fall through to repo-based
    assert result.status != "standalone_generation_failed"
    assert result.status == "dry_run_ready"
    assert result.selected_repo == "acme/fallback-repo"


def test_pipeline_falls_back_to_repo_when_standalone_publish_fails(tmp_path) -> None:
    """Regression: when standalone publish fails (e.g. API content block) the pipeline
    must fall back to the repo-based path instead of returning publish_failed.
    """
    from threads_github_bot.models import PublishThreadResult, ThreadPublishPostResult

    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    standalone_gen = FakeStandaloneGenerator(settings)
    candidate = _candidate("acme/repo-after-block")
    generator = FakeGenerator()

    # First publish call (standalone) fails with API blocked.
    # Second call (repo-based) succeeds.
    publish_calls: list = []

    class SwitchingPublisher:
        def publish_thread(self, posts):
            publish_calls.append(len(posts))
            if len(publish_calls) == 1:
                return PublishThreadResult(
                    success=False,
                    posts=[ThreadPublishPostResult(
                        success=False,
                        error='{"error":{"message":"API access blocked.","type":"OAuthException","code":200}}',
                        status_code=400,
                    )],
                    error="API access blocked",
                )
            return PublishThreadResult(
                success=True,
                posts=[ThreadPublishPostResult(success=True, container_id="c1", media_id="m1", response={})
                       for _ in posts],
            )

    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([candidate]),
        generator=generator,
        publisher=SwitchingPublisher(),
        validator=AlwaysValidValidator(),
        standalone_generator=standalone_gen,
    )

    # Must use post_now — dry_run bypasses the publisher entirely
    result = pipeline.run(mode="post_now", pillar_override="question")

    # After standalone publish block, must succeed via repo-based fallback
    assert result.status == "published"
    assert result.selected_repo == "acme/repo-after-block"
    assert len(publish_calls) == 2  # tried standalone, then repo-based
