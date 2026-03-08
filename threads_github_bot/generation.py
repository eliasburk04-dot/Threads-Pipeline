from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Tuple

import httpx

from threads_github_bot.config import Settings
from threads_github_bot.models import GeneratedPost, GeneratedThread, RepositoryCandidate, SeriesChoice, ThreadPost

PROMPT_VERSION = "2026-03-08.3"
THREAD_GENERATION_MAX_ATTEMPTS = 3
THREAD_GENERATION_RETRY_TOKEN_FLOOR = 800


class OpenAIResponsesClient:
    def __init__(self, settings: Settings, http_client: Optional[httpx.Client] = None) -> None:
        self.settings = settings
        if http_client is not None:
            self.http_client = http_client
            self._owns_client = False
        else:
            headers = {
                "Authorization": "Bearer {0}".format(settings.openai.api_key or ""),
                "Content-Type": "application/json",
            }
            self.http_client = httpx.Client(
                base_url=settings.openai.base_url,
                headers=headers,
                timeout=settings.openai.timeout_seconds,
            )
            self._owns_client = True

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()

    def create_text(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_output_tokens: int,
        json_mode: bool = False,
    ) -> Tuple[str, Dict]:
        payload = {
            "model": model,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
            ],
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        if json_mode:
            payload["text"] = {"format": {"type": "json_object"}}
        response = self.http_client.post("/responses", json=payload)
        response.raise_for_status()
        body = response.json()
        return self._extract_output_text(body), body

    def _extract_output_text(self, body: Dict) -> str:
        texts: List[str] = []
        for output_item in body.get("output", []):
            for content_item in output_item.get("content", []):
                if content_item.get("type") == "output_text" and content_item.get("text"):
                    texts.append(content_item["text"])
        combined = "\n".join(texts).strip()
        if not combined:
            raise ValueError("OpenAI response did not include output_text content")
        return combined


class ThreadsPostGenerator:
    def __init__(self, settings: Settings, client: OpenAIResponsesClient) -> None:
        self.settings = settings
        self.client = client

    def generate(self, repo: RepositoryCandidate, mode: str) -> GeneratedPost:
        system_prompt = (
            "You write natural Instagram Threads posts about open source software. "
            "Be curious, grounded, readable, slightly personal, and never hypey or spammy. "
            "Use only the metadata provided. Do not invent features, integrations, users, benchmarks, or outcomes. "
            "Avoid hashtags unless clearly useful. Avoid emoji spam. Return only the final post text."
        )
        metadata = {
            "full_name": repo.full_name,
            "description": repo.description,
            "topics": list(repo.topics),
            "matched_topics": list(repo.matched_topics),
            "language": repo.language,
            "stars": repo.stargazers_count,
            "homepage": repo.homepage,
            "html_url": repo.html_url,
            "pushed_at": repo.pushed_at.isoformat(),
            "updated_at": repo.updated_at.isoformat(),
            "language_target": self.settings.content.language,
        }
        user_prompt = (
            "Write one Threads-ready post in {language}. "
            "Use short paragraphs. Include a strong hook, a short explanation of what the project does, "
            "3 to 5 interesting points, a brief personal take, and end with the repo URL on its own line.\n\n"
            "Ground every factual claim in this metadata only:\n{metadata}\n\n"
            "Hard constraints:\n"
            "- No invented features.\n"
            "- No aggressive growth language.\n"
            "- No more than one hashtag, and only if clearly useful.\n"
            "- Keep within {max_bytes} UTF-8 bytes.\n"
        ).format(
            language=self.settings.content.language,
            metadata=json.dumps(metadata, indent=2, sort_keys=True),
            max_bytes=self.settings.content.max_text_bytes,
        )
        text, raw_response = self.client.create_text(
            model=self.settings.openai.model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=self.settings.content.temperature,
            max_output_tokens=self.settings.content.max_output_tokens,
        )
        return GeneratedPost(
            repo=repo,
            text=text.strip(),
            language=self.settings.content.language,
            model=raw_response.get("model", self.settings.openai.model),
            raw_response=raw_response,
            prompt_version=PROMPT_VERSION,
            mode=mode,
        )


class ThreadsThreadGenerator:
    def __init__(self, settings: Settings, client: OpenAIResponsesClient) -> None:
        self.settings = settings
        self.client = client

    def generate(
        self,
        repo: RepositoryCandidate,
        series: SeriesChoice,
        mode: str,
        validation_reasons: Tuple[str, ...] = (),
    ) -> GeneratedThread:
        metadata = _build_thread_metadata(self.settings, repo, series)
        effective_max_posts = max(
            self.settings.content.thread_post_count_min,
            self.settings.content.thread_post_count_max - 1,
        )
        system_prompt = (
            "You write Threads posts as a builder who actually uses developer tools — not as a content bot. "
            "Sound like a founder or operator sharing something they found, with a real opinion. "
            "Return strict JSON only. "
            "Use only the metadata provided. "
            "Do not invent features, integrations, users, benchmarks, analytics, or outcomes. "
            "Each post must stand on its own, but the sequence must still flow as one thread. "
            "Tone: curious, sharp, slightly opinionated, conversational. "
            "No emoji spam. No hashtags unless clearly useful."
        )
        user_prompt = (
            "Create a thread in {language} about this repo. "
            "Return a JSON object with keys series_hook and posts. "
            "posts must be an array of objects with keys position, role, and text. "
            "Generate between {min_posts} and {max_posts} posts.\n\n"
            "The publishing system will append a soft follow-up reply with the repo URL, so the main thread "
            "should focus on the idea, not a hard link drop. "
            "Avoid hashtags unless clearly useful and never use more than one.\n\n"
            "Requirements:\n"
            "- Post 1 must open on a specific tension, pain point, surprising pattern, or a strong opinion "
            "that makes someone stop scrolling. Do NOT start with the repo name, a series label, or a summary.\n"
            "- Avoid directory tone ('Let us look at...', 'Exploring a...', 'Looking for an open source...'). "
            "Write like a person talking to a friend, not a catalog.\n"
            "- Avoid bullet lists. Prefer short, punchy paragraphs.\n"
            "- Avoid stock phrases like 'What stood out to me', 'Three things stand out', 'worth checking out', "
            "'Let us take a closer look', 'for those interested in'.\n"
            "- Include at least one grounded operator opinion: what you actually think about the repo, "
            "who it is really for, or what you would test first.\n"
            "- Make the reader feel like they discovered something, not like they are being sold something.\n"
            "- Mention the repo URL in the main thread only if it genuinely improves the flow.\n\n"
            "Ground every factual claim in this metadata only:\n{metadata}\n"
        ).format(
            language=self.settings.content.language,
            min_posts=self.settings.content.thread_post_count_min,
            max_posts=effective_max_posts,
            metadata=_json_dumps(metadata),
        )
        if validation_reasons:
            user_prompt = (
                "{0}\n\n"
                "The previous draft was rejected for these reasons:\n- {1}\n\n"
                "Rewrite the entire thread from scratch and remove those issues. "
                "Do not add claims about documentation quality, maintenance quality, scalability, "
                "performance, privacy, integrations, platform breadth, or bundled capabilities unless "
                "they are explicit in the metadata. "
                "Prefer plain descriptions over persuasive phrasing. "
                "Keep the voice observational and operator-led."
            ).format(user_prompt, "\n- ".join(validation_reasons))
        attempts = _build_thread_generation_attempts(self.settings.content.max_output_tokens)
        last_error: Optional[Exception] = None

        for attempt_index, max_output_tokens in enumerate(attempts, start=1):
            attempt_prompt = user_prompt
            if attempt_index > 1:
                attempt_prompt = (
                    "{0}\n\n"
                    "Previous response was invalid or incomplete JSON. "
                    "Return one complete JSON object only, with no markdown fences, no commentary, "
                    "no trailing prose, and fully escaped JSON strings."
                ).format(user_prompt)

            payload_text, raw_response = self.client.create_text(
                model=self.settings.openai.model,
                system_prompt=system_prompt,
                user_prompt=attempt_prompt,
                temperature=0.2 if validation_reasons else self.settings.content.temperature,
                max_output_tokens=max_output_tokens,
                json_mode=True,
            )
            try:
                payload = _load_json_payload(payload_text)
                posts = _parse_thread_posts(payload)
            except ValueError as exc:
                last_error = exc
                continue

            return GeneratedThread(
                repo=repo,
                posts=tuple(posts),
                language=self.settings.content.language,
                model=raw_response.get("model", self.settings.openai.model),
                raw_response=raw_response,
                prompt_version=PROMPT_VERSION,
                mode=mode,
                series_slug=series.slug,
                series_label=series.label,
                series_number=series.number,
            )

        if last_error is None:
            raise ValueError("OpenAI response did not include thread posts")
        raise ValueError("OpenAI thread JSON generation failed after retries") from last_error


class OpenAIGroundingValidator:
    def __init__(self, settings: Settings, client: OpenAIResponsesClient) -> None:
        self.settings = settings
        self.client = client

    def __call__(self, repo: RepositoryCandidate, text: str):
        metadata = {
            "full_name": repo.full_name,
            "description": repo.description,
            "topics": list(repo.topics),
            "matched_topics": list(repo.matched_topics),
            "language": repo.language,
            "stars": repo.stargazers_count,
            "homepage": repo.homepage,
            "html_url": repo.html_url,
            "pushed_at": repo.pushed_at.isoformat(),
            "updated_at": repo.updated_at.isoformat(),
        }
        system_prompt = (
            "You validate whether a proposed social post is fully grounded in repository metadata. "
            "Return strict JSON with keys valid (boolean) and reasons (array of short strings). "
            "Reject invented features, integrations, user claims, performance claims, or platform claims."
        )
        user_prompt = (
            "Repository metadata:\n{metadata}\n\n"
            "Candidate post:\n{text}\n\n"
            "Return JSON only."
        ).format(metadata=json.dumps(metadata, indent=2, sort_keys=True), text=text)
        try:
            payload_text, _raw = self.client.create_text(
                model=self.settings.openai.validation_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
                max_output_tokens=180,
                json_mode=True,
            )
        except Exception as exc:  # pragma: no cover - exercised in runtime
            return ["grounding_validator_error:{0}".format(exc.__class__.__name__)]

        try:
            payload = _load_json_payload(payload_text)
        except ValueError:
            return ["grounding_validator_error:invalid_json"]

        if payload.get("valid") is True:
            return []

        reasons = payload.get("reasons") or ["model_rejected_post"]
        return [
            "ai_grounding:{0}".format(str(reason).strip().replace(" ", "_")[:120])
            for reason in reasons
        ]


def _load_json_payload(value: str) -> Dict:
    stripped = value.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    payload = json.loads(stripped)
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object")
    return payload


def _parse_thread_posts(payload: Dict) -> List[ThreadPost]:
    posts = []
    for item in payload.get("posts") or []:
        posts.append(
            ThreadPost(
                position=int(item.get("position") or len(posts) + 1),
                role=str(item.get("role") or "thread_post").strip(),
                text=str(item.get("text") or "").strip(),
            )
        )
    if not posts:
        raise ValueError("OpenAI response did not include thread posts")
    return posts


def _build_thread_generation_attempts(base_max_output_tokens: int) -> Tuple[int, ...]:
    normalized_base = max(180, int(base_max_output_tokens))
    attempts = [normalized_base]
    retry_budget = max(normalized_base * 2, THREAD_GENERATION_RETRY_TOKEN_FLOOR)
    while len(attempts) < THREAD_GENERATION_MAX_ATTEMPTS:
        if retry_budget <= attempts[-1]:
            retry_budget = attempts[-1] + 200
        attempts.append(retry_budget)
        retry_budget += 200
    return tuple(attempts)


def _build_thread_metadata(settings: Settings, repo: RepositoryCandidate, series: SeriesChoice) -> Dict:
    metadata = {
        "series": {
            "slug": series.slug,
            "label": series.label,
            "display_label": series.display_label,
        },
        "repo": {
            "full_name": _sanitize_metadata_value(repo.full_name),
            "description": _sanitize_metadata_value(repo.description),
            "topics": [_sanitize_metadata_value(topic) for topic in repo.topics],
            "matched_topics": [_sanitize_metadata_value(topic) for topic in repo.matched_topics],
            "language": _sanitize_metadata_value(repo.language or ""),
            "stars": repo.stargazers_count,
            "homepage": _sanitize_metadata_value(repo.homepage),
            "html_url": _sanitize_metadata_value(repo.html_url),
            "pushed_at": repo.pushed_at.isoformat(),
            "updated_at": repo.updated_at.isoformat(),
        },
        "scores": {
            "discoverability": repo.discoverability_score,
            "trend": repo.trend_score,
            "novelty": repo.novelty_score,
            "content_fit": repo.content_fit_score,
            "breakdown": repo.score_breakdown,
        },
        "language_target": settings.content.language,
    }
    if repo.metrics:
        metadata["metrics"] = repo.metrics
    return metadata


def _sanitize_metadata_value(value: str) -> str:
    cleaned = re.sub(r"[\x00-\x1f\x7f]+", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:600]


def _json_dumps(value: Dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True)
