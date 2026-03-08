from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv

from threads_github_bot.config import Settings
from threads_github_bot.generation import (
    OpenAIGroundingValidator,
    OpenAIResponsesClient,
    ThreadsThreadGenerator,
)
from threads_github_bot.github_client import GitHubDiscoveryClient
from threads_github_bot.logging_utils import configure_logging
from threads_github_bot.models import (
    GeneratedThread,
    PipelineRunResult,
    PublishThreadResult,
    RepositoryCandidate,
    SeriesChoice,
    ThreadPost,
    ThreadPublishPostResult,
)
from threads_github_bot.pipeline import ThreadsGitHubPipeline, compose_publishable_thread
from threads_github_bot.scheduler import iter_today_slots, plan_next_slots, slot_is_due
from threads_github_bot.series import select_series_for_candidate
from threads_github_bot.standalone_generation import StandaloneThreadGenerator
from threads_github_bot.state import SQLiteStateStore
from threads_github_bot.threads_client import ThreadsPublisherClient
from threads_github_bot.validation import ThreadValidator

LOGGER = logging.getLogger(__name__)


class StaticDiscoveryClient:
    def __init__(self, candidate: RepositoryCandidate) -> None:
        self.candidate = candidate

    def fetch_candidates(self):
        return [self.candidate]


class FixtureThreadGenerator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def generate(
        self,
        repo: RepositoryCandidate,
        series: SeriesChoice,
        mode: str,
        validation_reasons=(),
    ) -> GeneratedThread:
        posts = (
            ThreadPost(
                position=1,
                role="hook",
                text=(
                    "I keep seeing discovery workflows get noisier than they need to be. "
                    "{0} feels more focused.".format(repo.name)
                ),
            ),
            ThreadPost(
                position=2,
                role="use_case",
                text=(
                    "{0} is an open source project for builders who want a clearer workflow around {1}."
                ).format(
                    repo.name,
                    repo.description.lower().rstrip("."),
                ),
            ),
            ThreadPost(
                position=3,
                role="operator_take",
                text="My bias is toward tools with one clear job, because they are easier to test in a real workflow.",
            ),
            ThreadPost(
                position=4,
                role="who_its_for",
                text="This feels most useful if you track new tooling but do not want another giant suite.",
            ),
        )
        return GeneratedThread(
            repo=repo,
            posts=posts,
            language=self.settings.content.language,
            model="fixture-generator",
            raw_response={"fixture": True},
            prompt_version="fixture-v2",
            mode=mode,
            series_slug=series.slug,
            series_label=series.label,
            series_number=series.number,
        )


class NoopPublisher:
    def publish_thread(self, posts) -> PublishThreadResult:  # pragma: no cover - trivial
        return PublishThreadResult(
            success=True,
            posts=[
                ThreadPublishPostResult(
                    success=True,
                    container_id="noop_{0}".format(index),
                    media_id="noop_{0}".format(index),
                    response={"noop": True},
                )
                for index, _item in enumerate(posts, start=1)
            ],
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover GitHub repos and publish branded Threads threads."
    )
    parser.add_argument("--env-file", default=None, help="Optional path to a .env file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("discovery-only", help="Refresh the shortlist queue without generating or publishing")
    subparsers.add_parser("dry-run", help="Run live discovery and thread generation without publishing")
    subparsers.add_parser("test-run", help="Run the pipeline with a fixture repo and no live publish")
    subparsers.add_parser("scheduled-run", help="Auto-select test-run, dry-run, or post-now immediately")
    subparsers.add_parser("scheduled-check", help="Check whether a jittered slot is due and run at most once per slot")
    subparsers.add_parser("post-now", help="Run the live publish flow immediately")

    preview = subparsers.add_parser(
        "preview-thread",
        help="Generate and print the next branded thread without publishing",
    )
    preview.add_argument("--repo", default=None, help="Optional owner/name to preview if already in the queue")

    list_queue = subparsers.add_parser("list-queue", help="List queued candidate repos")
    list_queue.add_argument("--limit", type=int, default=10)

    list_recent = subparsers.add_parser("list-recent", help="List recent generated threads from SQLite")
    list_recent.add_argument("--limit", type=int, default=10)

    explain = subparsers.add_parser("explain-ranking", help="Refresh discovery and print ranking breakdowns")
    explain.add_argument("--limit", type=int, default=5)

    show_schedule = subparsers.add_parser("show-schedule", help="Show upcoming jittered publish slots")
    show_schedule.add_argument("--count", type=int, default=6)

    plan_next = subparsers.add_parser("plan-next-run", help="Show the next planned jittered publish slot")
    plan_next.add_argument("--count", type=int, default=2)

    status = subparsers.add_parser("status", help="Show health, recent threads, queue, and next schedule slots")
    status.add_argument("--limit", type=int, default=5)
    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.env_file:
        load_dotenv(args.env_file, override=False)
    settings = Settings.from_env()
    configure_logging(settings)
    store = SQLiteStateStore(settings.runtime.db_path)
    store.initialize()

    LOGGER.info("cli_command_start", extra={"command": args.command})

    if args.command == "list-queue":
        print(json.dumps(store.list_queue(limit=args.limit), indent=2))
        return 0

    if args.command == "list-recent":
        print(json.dumps(store.list_recent_threads(limit=args.limit), indent=2))
        return 0

    if args.command == "show-schedule":
        payload = _show_schedule(settings, store, count=args.count)
        print(json.dumps(payload, indent=2))
        return 0

    if args.command == "plan-next-run":
        payload = _show_schedule(settings, store, count=args.count)
        print(json.dumps(payload[:1], indent=2))
        return 0

    if args.command == "status":
        status_payload = {}
        if settings.runtime.status_path.exists():
            status_payload = json.loads(settings.runtime.status_path.read_text(encoding="utf-8"))
        print(
            json.dumps(
                {
                    "status_file": status_payload,
                    "last_discovery_run": store.get_last_discovery_run(),
                    "recent_threads": store.list_recent_threads(limit=args.limit),
                    "queue": store.list_queue(limit=args.limit),
                    "next_slots": _show_schedule(settings, store, count=max(2, args.limit)),
                },
                indent=2,
            )
        )
        return 0

    if args.command == "discovery-only":
        payload = run_discovery_only(settings, store)
        print(json.dumps(payload, indent=2))
        return 0

    if args.command == "explain-ranking":
        payload = run_discovery_only(settings, store, explain_limit=args.limit)
        print(json.dumps(payload, indent=2))
        return 0

    if args.command == "preview-thread":
        payload = run_preview_thread(settings, store, repo_full_name=args.repo)
        print(json.dumps(payload, indent=2))
        return 0

    if args.command == "test-run":
        result = run_test_mode(settings, store)
    elif args.command == "scheduled-run":
        result = run_scheduled_command(settings, store)
    elif args.command == "scheduled-check":
        result = run_scheduled_check(settings, store)
    elif args.command == "dry-run":
        missing = _missing_required(settings, ("OPENAI_API_KEY",))
        if missing:
            LOGGER.error("dry_run_missing_config", extra={"missing": missing})
            return 2
        result = run_live_pipeline(settings, store, mode="dry_run")
    else:
        missing = _missing_required(
            settings,
            ("OPENAI_API_KEY", "THREADS_ACCESS_TOKEN", "THREADS_USER_ID"),
        )
        if missing:
            LOGGER.error("post_now_missing_config", extra={"missing": missing})
            return 2
        result = run_live_pipeline(settings, store, mode="post_now")

    LOGGER.info(
        "cli_command_complete",
        extra={
            "command": args.command,
            "status": result.status,
            "selected_repo": result.selected_repo,
            "selected_series": result.selected_series,
        },
    )
    print(
        json.dumps(
            {
                "status": result.status,
                "selected_repo": result.selected_repo,
                "selected_series": result.selected_series,
                "scheduled_slot_key": result.scheduled_slot_key,
                "reasons": list(result.reasons),
            },
            indent=2,
        )
    )
    return 0 if result.status in {"dry_run_ready", "test_run_ready", "published", "not_due"} else 1


def run_live_pipeline(
    settings: Settings,
    store: SQLiteStateStore,
    mode: str,
    scheduled_slot=None,
) -> PipelineRunResult:
    openai_client = OpenAIResponsesClient(settings)
    github_client = GitHubDiscoveryClient(settings)
    threads_client = ThreadsPublisherClient(settings)
    try:
        validator = build_validator(
            settings,
            duplicate_lookup=store.has_duplicate_post,
            openai_client=openai_client,
        )
        standalone_gen = StandaloneThreadGenerator(settings, openai_client)
        pipeline = ThreadsGitHubPipeline(
            settings=settings,
            store=store,
            discovery_client=github_client,
            generator=ThreadsThreadGenerator(settings, openai_client),
            publisher=threads_client,
            validator=validator,
            standalone_generator=standalone_gen,
        )
        return pipeline.run(mode=mode, scheduled_slot=scheduled_slot)
    finally:
        github_client.close()
        openai_client.close()
        threads_client.close()


def run_discovery_only(settings: Settings, store: SQLiteStateStore, explain_limit: int = 5) -> dict:
    github_client = GitHubDiscoveryClient(settings)
    try:
        pipeline = ThreadsGitHubPipeline(
            settings=settings,
            store=store,
            discovery_client=github_client,
            generator=FixtureThreadGenerator(settings),
            publisher=NoopPublisher(),
        )
        refreshed = pipeline.refresh_queue()
    finally:
        github_client.close()

    return {
        "status": "queue_refreshed",
        "queued_count": len(refreshed["ranked"]),
        "top_candidates": [
            {
                "repo_full_name": candidate.full_name,
                "queue_score": candidate.score,
                "discoverability_score": candidate.discoverability_score,
                "trend_score": candidate.trend_score,
                "novelty_score": candidate.novelty_score,
                "content_fit_score": candidate.content_fit_score,
                "score_breakdown": candidate.score_breakdown,
            }
            for candidate in refreshed["ranked"][:explain_limit]
        ],
    }


def run_preview_thread(settings: Settings, store: SQLiteStateStore, repo_full_name: Optional[str] = None) -> dict:
    candidate = store.load_queued_candidate(repo_full_name=repo_full_name)
    if candidate is None:
        candidate = _build_fixture_candidate()
    history = store.fetch_recent_thread_history(limit=10)
    if repo_full_name and candidate.full_name != repo_full_name:
        raise SystemExit("Requested repo is not present in the current queue")
    series = select_series_for_candidate(candidate, settings, recent_history=history)

    if settings.openai.api_key:
        openai_client = OpenAIResponsesClient(settings)
        try:
            generator = ThreadsThreadGenerator(settings, openai_client)
            thread = generator.generate(candidate, series, mode="dry_run")
        finally:
            openai_client.close()
    else:
        thread = FixtureThreadGenerator(settings).generate(candidate, series, mode="test_run")
    thread = compose_publishable_thread(settings, thread)

    return {
        "repo_full_name": thread.repo.full_name,
        "series": (
            thread.series_label
            if thread.series_number is None
            else "{0} #{1}".format(thread.series_label, thread.series_number)
        ),
        "posts": [
            {"position": post.position, "role": post.role, "text": post.text}
            for post in thread.posts
        ],
    }


def run_test_mode(settings: Settings, store: SQLiteStateStore) -> PipelineRunResult:
    candidate = _build_fixture_candidate()

    def duplicate_lookup(_text: str) -> bool:
        return False

    if settings.openai.api_key:
        openai_client = OpenAIResponsesClient(settings)
        try:
            generator = ThreadsThreadGenerator(settings, openai_client)
            validator = build_validator(
                settings,
                duplicate_lookup=duplicate_lookup,
                openai_client=openai_client,
            )
            pipeline = ThreadsGitHubPipeline(
                settings=settings,
                store=store,
                discovery_client=StaticDiscoveryClient(candidate),
                generator=generator,
                publisher=NoopPublisher(),
                validator=validator,
            )
            return pipeline.run(mode="test_run")
        finally:
            openai_client.close()

    pipeline = ThreadsGitHubPipeline(
        settings=settings,
        store=store,
        discovery_client=StaticDiscoveryClient(candidate),
        generator=FixtureThreadGenerator(settings),
        publisher=NoopPublisher(),
        validator=ThreadValidator(settings=settings, duplicate_lookup=duplicate_lookup),
    )
    return pipeline.run(mode="test_run")


def run_scheduled_command(settings: Settings, store: SQLiteStateStore) -> PipelineRunResult:
    selected_command, missing = resolve_scheduled_command(settings)
    LOGGER.info(
        "scheduled_run_selected_command",
        extra={"selected_command": selected_command, "missing": missing},
    )
    if selected_command == "test-run":
        return run_test_mode(settings, store)
    if selected_command == "dry-run":
        return run_live_pipeline(settings, store, mode="dry_run")
    return run_live_pipeline(settings, store, mode="post_now")


def run_scheduled_check(settings: Settings, store: SQLiteStateStore) -> PipelineRunResult:
    now = datetime.now(timezone.utc)
    for slot in iter_today_slots(settings, now):
        store.upsert_schedule_slot(slot)
        persisted = store.get_schedule_slot(slot.slot_key)
        if persisted and persisted.status not in {"planned", "not_due"}:
            continue
        if slot_is_due(slot, now, settings):
            selected_command, missing = resolve_scheduled_command(settings)
            LOGGER.info(
                "scheduled_check_due",
                extra={"slot_key": slot.slot_key, "selected_command": selected_command, "missing": missing},
            )
            if selected_command == "test-run":
                result = run_test_mode(settings, store)
                store.update_schedule_slot(slot.slot_key, result.status, actual_publish_at_utc=now.isoformat())
                return PipelineRunResult(
                    status=result.status,
                    selected_repo=result.selected_repo,
                    selected_series=result.selected_series,
                    reasons=result.reasons,
                    scheduled_slot_key=slot.slot_key,
                )
            mode = "dry_run" if selected_command == "dry-run" else "post_now"
            return run_live_pipeline(settings, store, mode=mode, scheduled_slot=slot)
    return PipelineRunResult(status="not_due")


def resolve_scheduled_command(settings: Settings) -> tuple[str, list[str]]:
    openai_missing = _missing_required(settings, ("OPENAI_API_KEY",))
    if openai_missing:
        return "test-run", openai_missing

    publish_missing = _missing_required(
        settings,
        ("THREADS_ACCESS_TOKEN", "THREADS_USER_ID"),
    )
    if publish_missing:
        return "dry-run", publish_missing

    return "post-now", []


def build_validator(
    settings: Settings,
    duplicate_lookup,
    openai_client: OpenAIResponsesClient,
) -> ThreadValidator:
    grounding_validator = None
    if settings.content.enable_grounding_validator and settings.openai.api_key:
        grounding_validator = OpenAIGroundingValidator(settings, openai_client)
    return ThreadValidator(
        settings=settings,
        duplicate_lookup=duplicate_lookup,
        grounding_validator=grounding_validator,
    )


def _build_fixture_candidate() -> RepositoryCandidate:
    now = datetime.now(timezone.utc)
    return RepositoryCandidate(
        repo_id=9_999_001,
        full_name="example/threads-github-bot-fixture",
        name="threads-github-bot-fixture",
        owner="example",
        description="discovering interesting open source repositories and drafting grounded Threads threads",
        html_url="https://github.com/example/threads-github-bot-fixture",
        homepage="https://example.com/threads-github-bot-fixture",
        topics=("ai", "developer-tools", "automation"),
        matched_topics=("ai", "developer-tools"),
        language="Python",
        stargazers_count=420,
        forks_count=12,
        archived=False,
        fork=False,
        pushed_at=now - timedelta(days=1),
        updated_at=now - timedelta(days=1),
        discoverability_score=0.82,
        trend_score=0.75,
        novelty_score=0.9,
        content_fit_score=0.8,
        score_breakdown={"discoverability": 0.82, "trend": 0.75, "novelty": 0.9, "content_fit": 0.8, "total": 0.82},
    )


def _missing_required(settings: Settings, variables) -> list:
    mapping = {
        "OPENAI_API_KEY": settings.openai.api_key,
        "THREADS_ACCESS_TOKEN": settings.threads.access_token,
        "THREADS_USER_ID": settings.threads.user_id,
    }
    return [name for name in variables if not mapping.get(name)]


def _show_schedule(settings: Settings, store: SQLiteStateStore, count: int) -> list:
    planned = plan_next_slots(settings, now=datetime.now(timezone.utc), count=count)
    payload = []
    for slot in planned:
        persisted = store.get_schedule_slot(slot.slot_key)
        payload.append(
            {
                "slot_key": slot.slot_key,
                "slot_name": slot.slot_name,
                "local_date": slot.local_date,
                "base_local": slot.base_local.isoformat(),
                "planned_local": slot.planned_local.isoformat(),
                "planned_publish_at_utc": slot.planned_at_utc.isoformat(),
                "jitter_minutes": slot.jitter_minutes,
                "status": persisted.status if persisted else "planned",
            }
        )
    return payload


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
