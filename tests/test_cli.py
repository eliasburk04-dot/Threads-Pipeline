from threads_github_bot import cli
from threads_github_bot.config import Settings
from threads_github_bot.models import PipelineRunResult


def test_resolve_scheduled_command_uses_test_run_without_openai(tmp_path) -> None:
    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path)})

    command, missing = cli.resolve_scheduled_command(settings)

    assert command == "test-run"
    assert missing == ["OPENAI_API_KEY"]


def test_resolve_scheduled_command_uses_dry_run_without_threads_publish_credentials(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "OPENAI_API_KEY": "test-key",
        }
    )

    command, missing = cli.resolve_scheduled_command(settings)

    assert command == "dry-run"
    assert missing == ["THREADS_ACCESS_TOKEN", "THREADS_USER_ID"]


def test_scheduled_run_executes_live_publish_when_all_credentials_exist(tmp_path, monkeypatch) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "OPENAI_API_KEY": "test-key",
            "THREADS_ACCESS_TOKEN": "threads-token",
            "THREADS_USER_ID": "12345",
        }
    )
    executed = []

    def fake_run_live_pipeline(passed_settings, _store, mode: str) -> PipelineRunResult:
        executed.append((passed_settings, mode))
        return PipelineRunResult(status="published", selected_repo="acme/repo-one")

    monkeypatch.setattr(cli, "run_live_pipeline", fake_run_live_pipeline)

    result = cli.run_scheduled_command(settings, store=object())

    assert result.status == "published"
    assert executed == [(settings, "post_now")]
