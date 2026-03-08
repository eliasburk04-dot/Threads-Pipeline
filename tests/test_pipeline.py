from datetime import datetime, timedelta, timezone

from threads_github_bot.config import Settings
from threads_github_bot.models import (
    GeneratedThread,
    PublishThreadResult,
    RepositoryCandidate,
    ThreadPost,
    ThreadPublishPostResult,
)
from threads_github_bot.pipeline import ThreadsGitHubPipeline
from threads_github_bot.scheduler import build_slot_plan
from threads_github_bot.scoring import rank_candidates
from threads_github_bot.state import SQLiteStateStore


class FakeDiscoveryClient:
    def __init__(self, candidates):
        self._candidates = candidates

    def fetch_candidates(self):
        return list(self._candidates)


class FakeGenerator:
    def __init__(self):
        self.calls = []

    def generate(self, candidate, series, mode, validation_reasons=()):
        self.calls.append((candidate.full_name, series.slug, mode, tuple(validation_reasons)))
        return GeneratedThread(
            repo=candidate,
            posts=(
                ThreadPost(
                    position=1,
                    role="hook",
                    text=(
                        f"I keep noticing how much builder time disappears into tool sprawl. "
                        f"{candidate.name} is a more focused take."
                    ),
                ),
                ThreadPost(
                    position=2,
                    role="use_case",
                    text=(
                        "It is an open source repo for developer workflows, especially when you want "
                        "something concrete instead of another broad platform."
                    ),
                ),
                ThreadPost(
                    position=3,
                    role="operator_take",
                    text=(
                        "My bias is toward tools with a narrow job and a clear entry point, because "
                        "they are easier to test in a real workflow."
                    ),
                ),
                ThreadPost(
                    position=4,
                    role="who_its_for",
                    text=(
                        "This feels most useful for builders who want a sharper workflow fit, not a giant suite."
                    ),
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
                    container_id=f"container_{index}",
                    media_id=f"thread_{index}",
                    response={"ok": True},
                )
                for index, _post in enumerate(posts, start=1)
            ],
        )


class SequencedValidator:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def validate(self, candidate, posts):
        from threads_github_bot.models import ValidationResult

        index = min(self.calls, len(self.results) - 1)
        self.calls += 1
        is_valid, reasons = self.results[index]
        return ValidationResult(is_valid=is_valid, reasons=list(reasons))


def _candidate(full_name: str, stars: int, pushed_days_ago: int, topics):
    now = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
    return RepositoryCandidate(
        repo_id=abs(hash(full_name)) % 10_000,
        full_name=full_name,
        name=full_name.split("/", 1)[1],
        owner=full_name.split("/", 1)[0],
        description="A useful AI tool for developers with a clear repo description and practical use case.",
        html_url=f"https://github.com/{full_name}",
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


def test_pipeline_dry_run_builds_thread_and_persists_queue_and_schedule(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "APP_TIMEZONE": "Europe/Berlin",
            "SCHEDULE_MORNING_TIME": "08:30",
            "SCHEDULE_JITTER_MINUTES": "10",
        }
    )
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    first = _candidate("acme/discoverable-tool", 1_400, 1, ("ai", "developer-tools"))
    second = _candidate("acme/less-clear-tool", 18_000, 2, ("ai",))

    generator = FakeGenerator()
    publisher = FakePublisher()
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([first, second]),
        generator=generator,
        publisher=publisher,
    )

    slot = build_slot_plan(settings, local_date="2026-03-10", slot_name="morning")
    result = pipeline.run(mode="dry_run", scheduled_slot=slot)

    assert result.status == "dry_run_ready"
    assert result.selected_repo == "acme/discoverable-tool"
    assert result.selected_series
    assert generator.calls
    assert publisher.calls == []

    queue_items = store.list_queue(limit=5)
    assert len(queue_items) == 2
    assert queue_items[0]["repo_full_name"] == "acme/discoverable-tool"

    recent_threads = store.list_recent_threads(limit=5)
    assert len(recent_threads) == 1
    assert recent_threads[0]["post_count"] == 5
    assert recent_threads[0]["series_slug"]

    persisted_slot = store.get_schedule_slot(slot.slot_key)
    assert persisted_slot is not None
    assert persisted_slot.status == "dry_run_ready"


def test_pipeline_repairs_invalid_thread_before_accepting_candidate(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    candidate = _candidate("acme/discoverable-tool", 1_400, 1, ("ai", "developer-tools"))
    generator = FakeGenerator()
    publisher = FakePublisher()
    validator = SequencedValidator(
        [
            (False, ("3:unsupported_claim:docs", "4:ai_grounding:No_information_about_documentation_quality")),
            (True, ()),
        ]
    )
    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([candidate]),
        generator=generator,
        publisher=publisher,
        validator=validator,
    )

    result = pipeline.run(mode="dry_run")

    assert result.status == "dry_run_ready"
    assert len(generator.calls) == 2
    assert generator.calls[0][3] == ()
    assert "unsupported_claim:docs" in generator.calls[1][3][0]
    recent_threads = store.list_recent_threads(limit=5)
    assert len(recent_threads) == 1
    assert recent_threads[0]["validation_status"] == "passed"


def test_cooldown_history_ignores_non_published_threads(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    dry_candidate = _candidate("acme/dry-run-developer-toolkit", 1_400, 1, ("ai", "developer-tools"))
    test_candidate = _candidate("acme/test-run-developer-toolkit", 1_450, 1, ("ai", "developer-tools"))
    live_candidate = _candidate("acme/live-developer-toolkit", 1_500, 1, ("ai", "developer-tools"))

    dry_pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([dry_candidate]),
        generator=FakeGenerator(),
        publisher=FakePublisher(),
        validator=SequencedValidator([(True, ())]),
    )
    test_pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([test_candidate]),
        generator=FakeGenerator(),
        publisher=FakePublisher(),
        validator=SequencedValidator([(True, ())]),
    )
    live_publisher = FakePublisher()
    live_pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([live_candidate]),
        generator=FakeGenerator(),
        publisher=live_publisher,
        validator=SequencedValidator([(True, ())]),
    )

    assert dry_pipeline.run(mode="dry_run").status == "dry_run_ready"
    assert test_pipeline.run(mode="test_run").status == "test_run_ready"
    assert store.fetch_recent_thread_history(limit=10) == []

    result = live_pipeline.run(mode="post_now")

    assert result.status == "published"
    assert live_publisher.calls
    assert len(live_publisher.calls[0]) == 5
    assert live_candidate.html_url in live_publisher.calls[0][-1]
    history = store.fetch_recent_thread_history(limit=10)
    assert [item.full_name for item in history] == ["acme/live-developer-toolkit"]


def test_refresh_queue_populates_reserve_with_non_shortlisted_candidates(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "GITHUB_QUEUE_SIZE": "1",
            "RESERVE_SIZE": "2",
            "RESERVE_MIN_SCORE": "0.0",
        }
    )
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    first = _candidate("acme/live-queue-pick", 2_400, 1, ("ai",))
    second = _candidate("acme/reserve-pick-one", 1_900, 1, ("productivity",))
    third = _candidate("acme/reserve-pick-two", 1_500, 1, ("saas",))

    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([first, second, third]),
        generator=FakeGenerator(),
        publisher=FakePublisher(),
        validator=SequencedValidator([(True, ())]),
    )

    refreshed = pipeline.refresh_queue()

    assert refreshed["ranked"][0].full_name == "acme/live-queue-pick"
    queue_items = store.list_queue(limit=5)
    assert [item["repo_full_name"] for item in queue_items] == ["acme/live-queue-pick"]
    reserve_items = store.list_reserve(limit=5)
    assert [item["repo_full_name"] for item in reserve_items] == [
        "acme/reserve-pick-one",
        "acme/reserve-pick-two",
    ]


def test_pipeline_falls_back_to_reserve_when_live_queue_is_blocked(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "GITHUB_QUEUE_SIZE": "1",
            "RESERVE_SIZE": "3",
            "RESERVE_MIN_SCORE": "0.0",
            "COOLDOWN_TOPIC_DAYS": "7",
        }
    )
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    history_candidate = _candidate("acme/history-ai-tool", 2_000, 1, ("ai",))
    history_pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([history_candidate]),
        generator=FakeGenerator(),
        publisher=FakePublisher(),
        validator=SequencedValidator([(True, ())]),
    )
    assert history_pipeline.run(mode="post_now").status == "published"

    blocked_live = _candidate("acme/blocked-live-ai-tool", 3_200, 1, ("ai",))
    reserve_candidate = _candidate("acme/reserve-productivity-tool", 900, 1, ("productivity",))
    scored_reserve = rank_candidates(
        [reserve_candidate],
        settings,
        datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc),
        history=store.fetch_recent_thread_history(),
        previous_snapshots={},
    )[0]
    store.refresh_reserve(
        [scored_reserve],
        max_items=settings.reserve.size,
        min_score=settings.reserve.min_score,
        max_age_days=settings.reserve.max_age_days,
    )
    fallback_publisher = FakePublisher()
    fallback_pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=FakeDiscoveryClient([blocked_live]),
        generator=FakeGenerator(),
        publisher=fallback_publisher,
        validator=SequencedValidator([(True, ())]),
    )

    result = fallback_pipeline.run(mode="post_now")

    assert result.status == "published"
    assert result.selected_repo == "acme/reserve-productivity-tool"
    assert fallback_publisher.calls
    queue_items = store.list_queue(limit=5)
    assert [item["repo_full_name"] for item in queue_items] == ["acme/blocked-live-ai-tool"]
    assert queue_items[0]["queue_status"] == "skipped_cooldown"
    assert store.list_reserve(limit=5) == []
