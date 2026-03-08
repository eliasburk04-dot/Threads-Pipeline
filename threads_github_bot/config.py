from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional, Tuple

from dotenv import load_dotenv

DEFAULT_TOPICS = (
    "ai",
    "developer-tools",
    "open-source",
    "agents",
    "llm",
    "rag",
    "productivity",
    "saas",
    "dev-infra",
)


def _parse_csv(value: Optional[str], default: Tuple[str, ...] = ()) -> Tuple[str, ...]:
    if value is None:
        return default
    items = [item.strip() for item in value.split(",")]
    cleaned = tuple(item for item in items if item)
    return cleaned or default


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _parse_float(value: Optional[str], default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _parse_int_csv(value: Optional[str], default: Tuple[int, ...] = ()) -> Tuple[int, ...]:
    if value is None:
        return default
    items = [item.strip() for item in value.split(",")]
    cleaned = tuple(int(item) for item in items if item)
    return cleaned or default


def _parse_topic_families(value: Optional[str]) -> Dict[str, Tuple[str, ...]]:
    if not value:
        return {}
    families: Dict[str, Tuple[str, ...]] = {}
    for item in value.split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        family_name, tokens = item.split("=", 1)
        members = tuple(token.strip() for token in tokens.split("|") if token.strip())
        if members:
            families[family_name.strip()] = members
    return families


@dataclass(frozen=True)
class RuntimeSettings:
    app_name: str
    base_dir: Path
    data_dir: Path
    log_dir: Path
    db_path: Path
    status_path: Path
    log_level: str
    timezone: str


@dataclass(frozen=True)
class GitHubSettings:
    token: Optional[str]
    topics: Tuple[str, ...]
    whitelist_topics: Tuple[str, ...]
    blacklist_topics: Tuple[str, ...]
    blacklist_repos: Tuple[str, ...]
    whitelist_repos: Tuple[str, ...]
    min_stars: int
    fetch_limit_per_topic: int
    queue_size: int
    recent_activity_days: int
    min_description_length: int
    timeout_seconds: int
    discoverability_star_floor: int
    discoverability_star_ceiling: int


@dataclass(frozen=True)
class RankingWeights:
    recency: float
    stars: float
    description_quality: float
    topic_relevance: float
    url_quality: float
    activity: float


@dataclass(frozen=True)
class RankingSettings:
    weights: RankingWeights


@dataclass(frozen=True)
class ContentSettings:
    language: str
    min_text_bytes: int
    max_text_bytes: int
    thread_post_count_min: int
    thread_post_count_max: int
    thread_structure: Tuple[str, ...]
    enable_grounding_validator: bool
    temperature: float
    max_output_tokens: int
    pillar_slugs: Tuple[str, ...]
    standalone_topics: Tuple[str, ...]


@dataclass(frozen=True)
class OpenAISettings:
    api_key: Optional[str]
    base_url: str
    model: str
    validation_model: str
    timeout_seconds: int


@dataclass(frozen=True)
class ThreadsSettings:
    access_token: Optional[str]
    user_id: Optional[str]
    base_url: str
    timeout_seconds: int
    retry_count: int
    retry_backoff_seconds: float
    publish_delay_seconds_min: int
    publish_delay_seconds_max: int


@dataclass(frozen=True)
class CooldownSettings:
    repo_days: int
    similarity_days: int
    topic_days: int
    similarity_threshold: float
    series_days: int
    topic_families: Dict[str, Tuple[str, ...]]


@dataclass(frozen=True)
class SeriesSettings:
    templates: Tuple[str, ...]
    enable_numbering: bool
    allow_consecutive: bool
    whitelist: Tuple[str, ...]
    blacklist: Tuple[str, ...]


@dataclass(frozen=True)
class ScheduleSettings:
    morning_time: str
    evening_time: str
    jitter_minutes: int
    check_grace_minutes: int
    allowed_weekdays: Tuple[int, ...]
    jitter_seed: str
    enable_adaptive: bool
    strategy: str
    active_slots: Tuple[str, ...]  # ("morning", "evening") or ("evening",)


@dataclass(frozen=True)
class ReserveSettings:
    size: int
    min_score: float
    max_age_days: int


@dataclass(frozen=True)
class Settings:
    runtime: RuntimeSettings
    github: GitHubSettings
    ranking: RankingSettings
    content: ContentSettings
    openai: OpenAISettings
    threads: ThreadsSettings
    cooldown: CooldownSettings
    series: SeriesSettings
    schedule: ScheduleSettings
    reserve: ReserveSettings

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Settings":
        if env is None:
            load_dotenv(override=False)
            source = os.environ
        else:
            source = env

        def get(key: str, default: Optional[str] = None) -> Optional[str]:
            return source.get(key, default)

        base_dir = Path(get("APP_BASE_DIR", str(Path.cwd()))).expanduser().resolve()
        data_dir = Path(get("APP_DATA_DIR", str(base_dir / "var" / "data"))).expanduser()
        log_dir = Path(get("APP_LOG_DIR", str(base_dir / "var" / "logs"))).expanduser()
        db_path = Path(get("APP_DB_PATH", str(data_dir / "threads_github_bot.sqlite3"))).expanduser()
        status_path = Path(get("APP_STATUS_PATH", str(data_dir / "status.json"))).expanduser()

        runtime = RuntimeSettings(
            app_name=get("APP_NAME", "threads-github-bot"),
            base_dir=base_dir,
            data_dir=data_dir,
            log_dir=log_dir,
            db_path=db_path,
            status_path=status_path,
            log_level=get("APP_LOG_LEVEL", "INFO").upper(),
            timezone=get("APP_TIMEZONE", "Europe/Berlin"),
        )

        github = GitHubSettings(
            token=get("GITHUB_TOKEN"),
            topics=_parse_csv(get("GITHUB_TOPICS"), DEFAULT_TOPICS),
            whitelist_topics=_parse_csv(get("GITHUB_WHITELIST_TOPICS")),
            blacklist_topics=_parse_csv(get("GITHUB_BLACKLIST_TOPICS")),
            blacklist_repos=_parse_csv(get("GITHUB_BLACKLIST_REPOS")),
            whitelist_repos=_parse_csv(get("GITHUB_WHITELIST_REPOS")),
            min_stars=_parse_int(get("GITHUB_MIN_STARS"), 150),
            fetch_limit_per_topic=_parse_int(get("GITHUB_FETCH_LIMIT_PER_TOPIC"), 20),
            queue_size=_parse_int(get("GITHUB_QUEUE_SIZE"), 12),
            recent_activity_days=_parse_int(get("GITHUB_RECENT_ACTIVITY_DAYS"), 45),
            min_description_length=_parse_int(get("GITHUB_MIN_DESCRIPTION_LENGTH"), 24),
            timeout_seconds=_parse_int(get("GITHUB_TIMEOUT_SECONDS"), 20),
            discoverability_star_floor=_parse_int(get("GITHUB_DISCOVERABILITY_STAR_FLOOR"), 500),
            discoverability_star_ceiling=_parse_int(get("GITHUB_DISCOVERABILITY_STAR_CEILING"), 10000),
        )

        ranking = RankingSettings(
            weights=RankingWeights(
                recency=_parse_float(get("RANK_WEIGHT_RECENCY"), 0.24),
                stars=_parse_float(get("RANK_WEIGHT_STARS"), 0.20),
                description_quality=_parse_float(get("RANK_WEIGHT_DESCRIPTION"), 0.18),
                topic_relevance=_parse_float(get("RANK_WEIGHT_TOPICS"), 0.16),
                url_quality=_parse_float(get("RANK_WEIGHT_URL"), 0.10),
                activity=_parse_float(get("RANK_WEIGHT_ACTIVITY"), 0.12),
            )
        )

        content = ContentSettings(
            language=get("CONTENT_LANGUAGE", "en"),
            min_text_bytes=_parse_int(get("CONTENT_MIN_TEXT_BYTES"), 80),
            max_text_bytes=_parse_int(get("CONTENT_MAX_TEXT_BYTES"), 500),
            thread_post_count_min=_parse_int(get("THREAD_POST_COUNT_MIN"), 4),
            thread_post_count_max=_parse_int(get("THREAD_POST_COUNT_MAX"), 6),
            thread_structure=_parse_csv(
                get("THREAD_STRUCTURE"),
                (
                    "hook",
                    "use_case",
                    "operator_take",
                    "who_its_for",
                ),
            ),
            enable_grounding_validator=_parse_bool(get("OPENAI_ENABLE_GROUNDING_VALIDATOR"), True),
            temperature=_parse_float(get("OPENAI_TEMPERATURE"), 0.6),
            max_output_tokens=_parse_int(get("OPENAI_MAX_OUTPUT_TOKENS"), 280),
            pillar_slugs=_parse_csv(
                get("CONTENT_PILLAR_SLUGS"),
                (
                    "repo-discovery",
                    "hot-take",
                    "tool-comparison",
                    "question",
                    "workflow-breakdown",
                ),
            ),
            standalone_topics=_parse_csv(
                get("CONTENT_STANDALONE_TOPICS"),
                (
                    "when an AI agent is overkill vs when it actually saves time",
                    "the gap between demo-quality and production-quality in LLM apps",
                    "why most developer tools fail at onboarding",
                    "building a side project in public as a solo dev",
                    "the trade-off between using a framework vs rolling your own",
                    "what code review actually catches vs what people think it catches",
                    "shipping fast vs shipping correctly in early-stage startups",
                    "the real cost of adding another dependency to your stack",
                    "open source maintainer burnout and what users get wrong",
                    "RAG vs fine-tuning for small teams — which is actually easier",
                    "why your CI pipeline is slow and what to try first",
                    "the tools I actually use daily vs the ones I recommend",
                ),
            ),
        )

        openai = OpenAISettings(
            api_key=get("OPENAI_API_KEY"),
            base_url=get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            model=get("OPENAI_MODEL", "gpt-4.1-mini"),
            validation_model=get("OPENAI_VALIDATION_MODEL", get("OPENAI_MODEL", "gpt-4.1-mini")),
            timeout_seconds=_parse_int(get("OPENAI_TIMEOUT_SECONDS"), 30),
        )

        legacy_publish_delay = get("THREADS_PUBLISH_DELAY_SECONDS")
        delay_min_env = get("THREADS_PUBLISH_DELAY_MIN_SECONDS")
        delay_max_env = get("THREADS_PUBLISH_DELAY_MAX_SECONDS")
        if delay_min_env is None and delay_max_env is None and legacy_publish_delay is not None:
            publish_delay_min = max(0, _parse_int(legacy_publish_delay, 0))
            publish_delay_max = publish_delay_min
        else:
            publish_delay_min = max(0, _parse_int(delay_min_env, 15))
            publish_delay_max = max(0, _parse_int(delay_max_env, 30))
        if publish_delay_max < publish_delay_min:
            publish_delay_min, publish_delay_max = publish_delay_max, publish_delay_min

        threads = ThreadsSettings(
            access_token=get("THREADS_ACCESS_TOKEN"),
            user_id=get("THREADS_USER_ID"),
            base_url=get("THREADS_API_BASE_URL", "https://graph.threads.net/v1.0"),
            timeout_seconds=_parse_int(get("THREADS_TIMEOUT_SECONDS"), 30),
            retry_count=_parse_int(get("THREADS_RETRY_COUNT"), 3),
            retry_backoff_seconds=_parse_float(get("THREADS_RETRY_BACKOFF_SECONDS"), 2.0),
            publish_delay_seconds_min=publish_delay_min,
            publish_delay_seconds_max=publish_delay_max,
        )

        cooldown = CooldownSettings(
            repo_days=_parse_int(get("COOLDOWN_REPO_DAYS"), 21),
            similarity_days=_parse_int(get("COOLDOWN_SIMILARITY_DAYS"), 30),
            topic_days=_parse_int(get("COOLDOWN_TOPIC_DAYS"), 7),
            similarity_threshold=_parse_float(get("COOLDOWN_SIMILARITY_THRESHOLD"), 0.82),
            series_days=_parse_int(get("COOLDOWN_SERIES_DAYS"), 3),
            topic_families=_parse_topic_families(get("COOLDOWN_TOPIC_FAMILIES")),
        )

        series = SeriesSettings(
            templates=_parse_csv(
                get("SERIES_TEMPLATES"),
                (
                    "hidden-github-gem",
                    "ai-repo-worth-trying",
                    "open-source-tool-of-the-day",
                    "dev-tool-i-didnt-know-existed",
                    "trending-ai-repo",
                ),
            ),
            enable_numbering=_parse_bool(get("SERIES_ENABLE_NUMBERING"), False),
            allow_consecutive=_parse_bool(get("SERIES_ALLOW_CONSECUTIVE"), False),
            whitelist=_parse_csv(get("SERIES_WHITELIST")),
            blacklist=_parse_csv(get("SERIES_BLACKLIST")),
        )

        schedule = ScheduleSettings(
            morning_time=get("SCHEDULE_MORNING_TIME", "08:30"),
            evening_time=get("SCHEDULE_EVENING_TIME", "19:30"),
            jitter_minutes=_parse_int(get("SCHEDULE_JITTER_MINUTES"), 10),
            check_grace_minutes=_parse_int(get("SCHEDULE_CHECK_GRACE_MINUTES"), 6),
            allowed_weekdays=_parse_int_csv(get("SCHEDULE_ALLOWED_WEEKDAYS"), (0, 1, 2, 3, 4, 5, 6)),
            jitter_seed=get("SCHEDULE_JITTER_SEED", "threads-github-bot"),
            enable_adaptive=_parse_bool(get("SCHEDULE_ENABLE_ADAPTIVE"), False),
            strategy=get("SCHEDULE_STRATEGY", "windowed-jitter-v1"),
            active_slots=_parse_csv(get("SCHEDULE_ACTIVE_SLOTS"), ("evening",)),
        )

        reserve = ReserveSettings(
            size=_parse_int(get("RESERVE_SIZE"), 10),
            min_score=_parse_float(get("RESERVE_MIN_SCORE"), 0.68),
            max_age_days=_parse_int(get("RESERVE_MAX_AGE_DAYS"), 21),
        )

        return cls(
            runtime=runtime,
            github=github,
            ranking=ranking,
            content=content,
            openai=openai,
            threads=threads,
            cooldown=cooldown,
            series=series,
            schedule=schedule,
            reserve=reserve,
        )
