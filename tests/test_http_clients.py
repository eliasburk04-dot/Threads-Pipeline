import json
from datetime import datetime, timezone
from unittest.mock import call, patch

import httpx

from threads_github_bot.config import Settings
from threads_github_bot.generation import OpenAIResponsesClient, ThreadsThreadGenerator
from threads_github_bot.github_client import GitHubDiscoveryClient
from threads_github_bot.models import RepositoryCandidate, SeriesChoice
from threads_github_bot.threads_client import ThreadsPublisherClient


def test_github_client_fetches_and_normalizes_candidates(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/search/repositories"
        assert "topic:ai" in request.url.params["q"]
        payload = {
            "items": [
                {
                    "id": 1,
                    "full_name": "acme/repo-one",
                    "name": "repo-one",
                    "owner": {"login": "acme"},
                    "description": "A useful AI repo for developers.",
                    "html_url": "https://github.com/acme/repo-one",
                    "homepage": "https://repo-one.dev",
                    "topics": ["ai", "developer-tools"],
                    "language": "Python",
                    "stargazers_count": 250,
                    "forks_count": 10,
                    "archived": False,
                    "fork": False,
                    "pushed_at": "2026-03-05T12:00:00Z",
                    "updated_at": "2026-03-05T12:00:00Z",
                }
            ]
        }
        return httpx.Response(200, json=payload)

    settings = Settings.from_env({"APP_BASE_DIR": str(tmp_path), "GITHUB_TOPICS": "ai"})
    client = GitHubDiscoveryClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.github.com"),
    )

    candidates = client.fetch_candidates()

    assert len(candidates) == 1
    assert candidates[0].full_name == "acme/repo-one"
    assert "ai" in candidates[0].matched_topics


def test_openai_thread_generator_extracts_structured_thread(tmp_path) -> None:
    repo = RepositoryCandidate(
        repo_id=1,
        full_name="acme/repo-one",
        name="repo-one",
        owner="acme",
        description="A useful AI repo for developers.",
        html_url="https://github.com/acme/repo-one",
        homepage="https://repo-one.dev",
        topics=("ai", "developer-tools"),
        matched_topics=("ai",),
        language="Python",
        stargazers_count=250,
        forks_count=10,
        archived=False,
        fork=False,
        pushed_at=datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc),
    )
    series = SeriesChoice(slug="hidden-github-gem", label="Hidden GitHub Gem", number=12)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/responses")
        payload = request.content.decode("utf-8")
        assert '"type":"json_object"' in payload
        assert "makes someone stop scrolling" in payload
        assert "The publishing system will append a soft follow-up reply with the repo URL" in payload
        data = {
            "id": "resp_123",
            "model": "gpt-4.1-mini",
            "output": [
                {
                    "content": [
                        {
                            "type": "output_text",
                            "text": """
{
  "series_hook": "Hidden GitHub Gem #12",
  "posts": [
    {
      "position": 1,
      "role": "hook",
      "text": "I keep seeing AI tooling get broader while workflows stay messy. repo-one is a more focused take."
    },
    {
      "position": 2,
      "role": "use_case",
      "text": "repo-one is an open source AI project for developers who want a more concrete workflow."
    },
    {
      "position": 3,
      "role": "operator_take",
      "text": "My bias is toward tools with a narrow job and a clean setup path, and this looks easier to test."
    },
    {
      "position": 4,
      "role": "who_its_for",
      "text": "This feels most useful for builders who want a sharper AI workflow without extra sprawl."
    }
  ]
}
""".strip(),
                        }
                    ]
                }
            ],
        }
        return httpx.Response(200, json=data)

    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "OPENAI_API_KEY": "test-key",
            "OPENAI_MODEL": "gpt-4.1-mini",
        }
    )
    client = OpenAIResponsesClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.openai.com/v1"),
    )
    generator = ThreadsThreadGenerator(settings, client)

    result = generator.generate(repo, series=series, mode="dry_run")

    assert len(result.posts) == 4
    assert result.series_slug == "hidden-github-gem"
    assert result.posts[0].role == "hook"
    assert all("https://github.com/acme/repo-one" not in post.text for post in result.posts)


def test_openai_thread_generator_retries_invalid_json_with_larger_budget(tmp_path) -> None:
    repo = RepositoryCandidate(
        repo_id=1,
        full_name="acme/repo-one",
        name="repo-one",
        owner="acme",
        description="A useful AI repo for developers.",
        html_url="https://github.com/acme/repo-one",
        homepage="https://repo-one.dev",
        topics=("ai", "developer-tools"),
        matched_topics=("ai",),
        language="Python",
        stargazers_count=250,
        forks_count=10,
        archived=False,
        fork=False,
        pushed_at=datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc),
    )
    series = SeriesChoice(slug="hidden-github-gem", label="Hidden GitHub Gem", number=12)
    payloads = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        payloads.append(body)
        if len(payloads) == 1:
            return httpx.Response(
                200,
                json={
                    "id": "resp_invalid",
                    "model": "gpt-4.1-mini",
                    "output": [
                        {
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": (
                                        '{"series_hook":"Hidden GitHub Gem #12","posts":[{"position":1,"role":"hook",'
                                        '"text":"This response gets truncated'
                                    ),
                                }
                            ]
                        }
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "resp_valid",
                "model": "gpt-4.1-mini",
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": """
{
  "series_hook": "Hidden GitHub Gem #12",
  "posts": [
    {
      "position": 1,
      "role": "hook",
      "text": "AI tooling keeps expanding before solving a concrete workflow. repo-one feels more specific."
    },
    {
      "position": 2,
      "role": "use_case",
      "text": "repo-one is an open source AI project for devs who want a clearer workflow starting point."
    },
    {
      "position": 3,
      "role": "operator_take",
      "text": "The operator angle is simple: a focused workflow is easier to test than a broad platform claim."
    },
    {
      "position": 4,
      "role": "who_its_for",
      "text": "This looks best for builders who care about a practical AI workflow more than category sprawl."
    }
  ]
}
""".strip(),
                            }
                        ]
                    }
                ],
            },
        )

    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "OPENAI_API_KEY": "test-key",
            "OPENAI_MODEL": "gpt-4.1-mini",
            "OPENAI_MAX_OUTPUT_TOKENS": "280",
        }
    )
    client = OpenAIResponsesClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.openai.com/v1"),
    )
    generator = ThreadsThreadGenerator(settings, client)

    result = generator.generate(repo, series=series, mode="dry_run")

    assert len(result.posts) == 4
    assert len(payloads) == 2
    assert payloads[0]["text"]["format"]["type"] == "json_object"
    assert payloads[1]["text"]["format"]["type"] == "json_object"
    assert payloads[0]["max_output_tokens"] == 280
    assert payloads[1]["max_output_tokens"] > payloads[0]["max_output_tokens"]
    second_prompt = payloads[1]["input"][1]["content"][0]["text"]
    assert "Previous response was invalid or incomplete JSON." in second_prompt


def test_threads_client_publishes_connected_thread(tmp_path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.content.decode()))
        if request.url.path.endswith("/threads"):
            if len(calls) == 1:
                return httpx.Response(200, json={"id": "container_1"})
            if len(calls) == 3:
                assert "reply_to_id=thread_1" in request.content.decode()
                return httpx.Response(200, json={"id": "container_2"})
            return httpx.Response(200, json={"id": "container_3"})
        if request.url.path.endswith("/threads_publish"):
            if len(calls) == 2:
                return httpx.Response(200, json={"id": "thread_1"})
            if len(calls) == 4:
                return httpx.Response(200, json={"id": "thread_2"})
            return httpx.Response(200, json={"id": "thread_3"})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "THREADS_ACCESS_TOKEN": "threads-token",
            "THREADS_USER_ID": "123456",
            "THREADS_PUBLISH_DELAY_SECONDS": "0",
        }
    )
    client = ThreadsPublisherClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler), base_url="https://graph.threads.net/v1.0"),
    )

    result = client.publish_thread(
        [
            "Post one",
            "Post two",
            "Post three",
        ]
    )

    assert result.success is True
    assert len(result.posts) == 3
    assert result.posts[0].media_id == "thread_1"
    assert result.posts[1].reply_to_id == "thread_1"
    assert calls[0][0].endswith("/threads")
    assert calls[1][0].endswith("/threads_publish")


def test_threads_client_randomizes_publish_delay_between_bounds(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/threads"):
            if request.content.decode().count("reply_to_id") == 0:
                return httpx.Response(200, json={"id": "container_1"})
            return httpx.Response(200, json={"id": "container_2"})
        if request.url.path.endswith("/threads_publish"):
            if "creation_id=container_1" in request.content.decode():
                return httpx.Response(200, json={"id": "thread_1"})
            return httpx.Response(200, json={"id": "thread_2"})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "THREADS_ACCESS_TOKEN": "threads-token",
            "THREADS_USER_ID": "123456",
            "THREADS_PUBLISH_DELAY_MIN_SECONDS": "15",
            "THREADS_PUBLISH_DELAY_MAX_SECONDS": "30",
        }
    )
    client = ThreadsPublisherClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler), base_url="https://graph.threads.net/v1.0"),
    )

    with patch("threads_github_bot.threads_client.random.randint", side_effect=[16, 28]) as randint_mock:
        with patch("threads_github_bot.threads_client.time.sleep") as sleep_mock:
            result = client.publish_thread(["Post one", "Post two"])

    assert result.success is True
    assert randint_mock.call_args_list == [call(15, 30), call(15, 30)]
    assert sleep_mock.call_args_list == [call(16), call(28)]
