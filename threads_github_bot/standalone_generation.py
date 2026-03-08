"""Standalone content generators for non-repo pillar types.

Each pillar has its own prompt template that produces a short Threads thread
(1-3 posts) without needing a GitHub repository.  The generators return a
``GeneratedThread`` using a synthetic ``RepositoryCandidate`` placeholder so
the rest of the pipeline (validation, persistence, publishing) works unchanged.
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional

from threads_github_bot.config import Settings
from threads_github_bot.content_pillars import ContentPillar
from threads_github_bot.generation import PROMPT_VERSION, OpenAIResponsesClient, _load_json_payload
from threads_github_bot.models import GeneratedThread, RepositoryCandidate, ThreadPost

# Synthetic repo ID range — avoids collisions with real GitHub repo IDs
_STANDALONE_REPO_ID_BASE = 900_000_000


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class StandaloneThreadGenerator:
    """Generate threads for non-repo content pillars."""

    def __init__(self, settings: Settings, client: OpenAIResponsesClient) -> None:
        self.settings = settings
        self.client = client

    def generate(
        self,
        pillar: ContentPillar,
        mode: str,
        topic_hint: Optional[str] = None,
    ) -> GeneratedThread:
        if pillar.slug == "hot-take":
            return self._generate_hot_take(pillar, mode, topic_hint)
        if pillar.slug == "tool-comparison":
            return self._generate_comparison(pillar, mode, topic_hint)
        if pillar.slug == "question":
            return self._generate_question(pillar, mode, topic_hint)
        if pillar.slug == "workflow-breakdown":
            return self._generate_workflow(pillar, mode, topic_hint)
        raise ValueError("Unsupported standalone pillar: {0}".format(pillar.slug))

    # ------------------------------------------------------------------
    # Hot Take
    # ------------------------------------------------------------------
    def _generate_hot_take(self, pillar: ContentPillar, mode: str, topic_hint: Optional[str]) -> GeneratedThread:
        topic = topic_hint or _pick_topic(self.settings)
        system_prompt = (
            "You are a technical founder who posts sharp, opinionated takes on Threads. "
            "Write like a person with real experience — not a brand. Be direct, slightly contrarian, "
            "and conversational. Never use hashtags. No emoji spam. No corporate speak. "
            "Return strict JSON only."
        )
        user_prompt = (
            "Write a short hot-take thread (1 to 2 posts) about: {topic}.\n\n"
            "Return a JSON object with key posts — an array of objects with keys position, role, text.\n\n"
            "Requirements:\n"
            "- Post 1: A bold, specific claim or observation. Not generic. Start with a tension or contrarian angle.\n"
            "- Optional post 2: A follow-up that adds nuance, a personal example, or invites a reply.\n"
            "- Sound like a real person. Use 'I' and 'my' naturally.\n"
            "- Each post must be under {max_bytes} UTF-8 bytes.\n"
            "- Language: {language}\n"
            "- Do NOT mention any specific tools or repos unless you are certain they exist.\n"
        ).format(
            topic=topic,
            max_bytes=self.settings.content.max_text_bytes,
            language=self.settings.content.language,
        )
        return self._call_and_parse(system_prompt, user_prompt, pillar, mode, topic)

    # ------------------------------------------------------------------
    # Tool Comparison
    # ------------------------------------------------------------------
    def _generate_comparison(self, pillar: ContentPillar, mode: str, topic_hint: Optional[str]) -> GeneratedThread:
        topic = topic_hint or _pick_topic(self.settings)
        system_prompt = (
            "You are a developer who writes concise, balanced comparisons on Threads. "
            "Compare approaches, patterns, or trade-offs — not specific product names unless "
            "you are certain they exist. Be practical, not salesy. Return strict JSON only."
        )
        user_prompt = (
            "Write a short comparison thread (2 to 3 posts) about a trade-off or choice in: {topic}.\n\n"
            "Return a JSON object with key posts — an array of objects with keys position, role, text.\n\n"
            "Requirements:\n"
            "- Post 1: Frame the comparison clearly. What two approaches/patterns are you weighing?\n"
            "- Post 2: Give the honest trade-offs for each side.\n"
            "- Optional post 3: Your personal lean and why.\n"
            "- Sound like a builder sharing a real decision, not a blog post.\n"
            "- Each post must be under {max_bytes} UTF-8 bytes.\n"
            "- Language: {language}\n"
        ).format(
            topic=topic,
            max_bytes=self.settings.content.max_text_bytes,
            language=self.settings.content.language,
        )
        return self._call_and_parse(system_prompt, user_prompt, pillar, mode, topic)

    # ------------------------------------------------------------------
    # Question Post
    # ------------------------------------------------------------------
    def _generate_question(self, pillar: ContentPillar, mode: str, topic_hint: Optional[str]) -> GeneratedThread:
        topic = topic_hint or _pick_topic(self.settings)
        system_prompt = (
            "You write engaging question posts for Threads that get developers to reply. "
            "Be specific, not generic. Frame questions that have no obvious right answer "
            "so people actually want to share their take. Return strict JSON only."
        )
        user_prompt = (
            "Write a single question post (1 post only) about: {topic}.\n\n"
            "Return a JSON object with key posts — an array with one object with keys position, role, text.\n\n"
            "Requirements:\n"
            "- Ask a specific, debatable question — not 'what do you think about X?'\n"
            "- Add 1-2 sentences of context so people understand why the question matters.\n"
            "- End with the question, not a statement.\n"
            "- Under {max_bytes} UTF-8 bytes.\n"
            "- Language: {language}\n"
            "- No hashtags.\n"
        ).format(
            topic=topic,
            max_bytes=self.settings.content.max_text_bytes,
            language=self.settings.content.language,
        )
        return self._call_and_parse(system_prompt, user_prompt, pillar, mode, topic)

    # ------------------------------------------------------------------
    # Workflow Breakdown
    # ------------------------------------------------------------------
    def _generate_workflow(self, pillar: ContentPillar, mode: str, topic_hint: Optional[str]) -> GeneratedThread:
        topic = topic_hint or _pick_topic(self.settings)
        system_prompt = (
            "You are a builder sharing real workflow observations on Threads. "
            "Write like someone who just tested something and is sharing what happened. "
            "Be concrete, not abstract. Return strict JSON only."
        )
        user_prompt = (
            "Write a short workflow breakdown thread (2 to 3 posts) about: {topic}.\n\n"
            "Return a JSON object with key posts — an array of objects with keys position, role, text.\n\n"
            "Requirements:\n"
            "- Post 1: Describe the workflow or experiment concretely. What did you try?\n"
            "- Post 2: What happened? What was surprising or useful?\n"
            "- Optional post 3: What you would do differently next time.\n"
            "- Sound like a builder's log, not a tutorial.\n"
            "- Each post must be under {max_bytes} UTF-8 bytes.\n"
            "- Language: {language}\n"
        ).format(
            topic=topic,
            max_bytes=self.settings.content.max_text_bytes,
            language=self.settings.content.language,
        )
        return self._call_and_parse(system_prompt, user_prompt, pillar, mode, topic)

    # ------------------------------------------------------------------
    # Shared generation & parsing
    # ------------------------------------------------------------------
    def _call_and_parse(
        self,
        system_prompt: str,
        user_prompt: str,
        pillar: ContentPillar,
        mode: str,
        topic: str,
    ) -> GeneratedThread:
        payload_text, raw_response = self.client.create_text(
            model=self.settings.openai.model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=self.settings.content.temperature,
            max_output_tokens=self.settings.content.max_output_tokens,
            json_mode=True,
        )
        payload = _load_json_payload(payload_text)
        posts = _parse_standalone_posts(payload)

        placeholder_repo = build_standalone_placeholder(pillar, topic)
        return GeneratedThread(
            repo=placeholder_repo,
            posts=tuple(posts),
            language=self.settings.content.language,
            model=raw_response.get("model", self.settings.openai.model),
            raw_response=raw_response,
            prompt_version=PROMPT_VERSION,
            mode=mode,
            series_slug=pillar.slug,
            series_label=pillar.label,
            series_number=None,
            pillar_slug=pillar.slug,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_standalone_placeholder(pillar: ContentPillar, topic: str) -> RepositoryCandidate:
    """Build a synthetic RepositoryCandidate for standalone content.

    This allows standalone threads to flow through the same persistence
    and publishing paths that expect a RepositoryCandidate.
    """
    slug_hash = int(hashlib.sha256(
        "{0}:{1}".format(pillar.slug, topic).encode()
    ).hexdigest()[:8], 16)
    repo_id = _STANDALONE_REPO_ID_BASE + (slug_hash % 1_000_000)
    now = datetime.now(timezone.utc)

    return RepositoryCandidate(
        repo_id=repo_id,
        full_name="standalone/{0}".format(pillar.slug),
        name=pillar.slug,
        owner="standalone",
        description="{0}: {1}".format(pillar.label, topic),
        html_url="https://threads.net",  # no real repo
        homepage="",
        topics=tuple(t.strip().lower().replace(" ", "-") for t in topic.split(",")),
        matched_topics=(),
        language="",
        stargazers_count=0,
        forks_count=0,
        archived=False,
        fork=False,
        pushed_at=now,
        updated_at=now,
    )


def _pick_topic(settings: Settings) -> str:
    """Pick a random topic from the configured standalone topics."""
    topics = settings.content.standalone_topics
    if not topics:
        return "developer workflows"
    return random.choice(topics)


def _parse_standalone_posts(payload: Dict) -> List[ThreadPost]:
    posts: List[ThreadPost] = []
    for item in payload.get("posts") or []:
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        posts.append(
            ThreadPost(
                position=int(item.get("position") or len(posts) + 1),
                role=str(item.get("role") or "standalone").strip(),
                text=text,
            )
        )
    if not posts:
        raise ValueError("Standalone generation did not produce posts")
    return posts
