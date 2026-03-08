from pathlib import Path

from threads_github_bot.config import Settings


def test_settings_from_env_parses_lists_paths_and_flags(tmp_path: Path) -> None:
    env = {
        "APP_BASE_DIR": str(tmp_path),
        "APP_TIMEZONE": "Europe/Berlin",
        "GITHUB_TOPICS": "ai,agents,rag",
        "GITHUB_WHITELIST_TOPICS": "ai,dev-infra",
        "GITHUB_BLACKLIST_TOPICS": "crypto,gambling",
        "GITHUB_BLACKLIST_REPOS": "owner1/repo1, owner2/repo2 ",
        "GITHUB_WHITELIST_REPOS": "owner3/repo3",
        "GITHUB_MIN_STARS": "125",
        "GITHUB_FETCH_LIMIT_PER_TOPIC": "15",
        "CONTENT_LANGUAGE": "de",
        "OPENAI_ENABLE_GROUNDING_VALIDATOR": "false",
        "THREADS_PUBLISH_DELAY_MIN_SECONDS": "5",
        "THREADS_PUBLISH_DELAY_MAX_SECONDS": "11",
        "THREAD_POST_COUNT_MIN": "4",
        "THREAD_POST_COUNT_MAX": "6",
        "SCHEDULE_MORNING_TIME": "08:15",
        "SCHEDULE_EVENING_TIME": "19:20",
        "SCHEDULE_JITTER_MINUTES": "10",
        "SCHEDULE_ALLOWED_WEEKDAYS": "0,1,2,3,4",
        "SERIES_ENABLE_NUMBERING": "true",
        "RESERVE_SIZE": "9",
        "RESERVE_MIN_SCORE": "0.61",
        "RESERVE_MAX_AGE_DAYS": "18",
    }

    settings = Settings.from_env(env)

    assert settings.runtime.base_dir == tmp_path
    assert settings.runtime.data_dir == tmp_path / "var" / "data"
    assert settings.runtime.log_dir == tmp_path / "var" / "logs"
    assert settings.runtime.db_path == tmp_path / "var" / "data" / "threads_github_bot.sqlite3"
    assert settings.github.topics == ("ai", "agents", "rag")
    assert settings.github.whitelist_topics == ("ai", "dev-infra")
    assert settings.github.blacklist_topics == ("crypto", "gambling")
    assert settings.github.blacklist_repos == ("owner1/repo1", "owner2/repo2")
    assert settings.github.whitelist_repos == ("owner3/repo3",)
    assert settings.github.min_stars == 125
    assert settings.github.fetch_limit_per_topic == 15
    assert settings.content.language == "de"
    assert settings.content.enable_grounding_validator is False
    assert settings.threads.publish_delay_seconds_min == 5
    assert settings.threads.publish_delay_seconds_max == 11
    assert settings.runtime.timezone == "Europe/Berlin"
    assert settings.content.thread_post_count_min == 4
    assert settings.content.thread_post_count_max == 6
    assert settings.schedule.morning_time == "08:15"
    assert settings.schedule.evening_time == "19:20"
    assert settings.schedule.jitter_minutes == 10
    assert settings.schedule.allowed_weekdays == (0, 1, 2, 3, 4)
    assert settings.series.enable_numbering is True
    assert settings.reserve.size == 9
    assert settings.reserve.min_score == 0.61
    assert settings.reserve.max_age_days == 18


def test_settings_defaults_cover_required_topics(tmp_path: Path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})

    assert "ai" in settings.github.topics
    assert "developer-tools" in settings.github.topics
    assert "agents" in settings.github.topics
    assert settings.content.max_text_bytes == 500
    assert settings.cooldown.repo_days >= 1
    assert settings.schedule.jitter_minutes == 10
    assert settings.threads.publish_delay_seconds_min == 15
    assert settings.threads.publish_delay_seconds_max == 30
    assert settings.reserve.size == 10


def test_settings_legacy_publish_delay_env_preserves_fixed_spacing(tmp_path: Path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "THREADS_PUBLISH_DELAY_SECONDS": "7",
        }
    )

    assert settings.threads.publish_delay_seconds_min == 7
    assert settings.threads.publish_delay_seconds_max == 7
