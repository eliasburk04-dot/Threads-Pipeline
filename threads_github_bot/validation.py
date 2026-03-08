from __future__ import annotations

import hashlib
import re
from typing import Callable, Iterable, Optional, Sequence

from threads_github_bot.config import Settings
from threads_github_bot.models import RepositoryCandidate, ThreadPost, ValidationResult

SPAM_PATTERNS = (
    re.compile(r"![!]{1,}"),
    re.compile(r"\b(change everything|best tool ever|must[- ]try|game changer)\b", re.IGNORECASE),
    re.compile(r"(.)\1{4,}"),
)

UNSUPPORTED_CLAIM_PATTERNS = (
    re.compile(r"\bships with\b(?P<claim>[^.!?]+)", re.IGNORECASE),
    re.compile(r"\bincludes\b(?P<claim>[^.!?]+)", re.IGNORECASE),
    re.compile(r"\bsupports\b(?P<claim>[^.!?]+)", re.IGNORECASE),
    re.compile(r"\bbuilt[- ]in\b(?P<claim>[^.!?]+)", re.IGNORECASE),
    re.compile(r"\bcomes with\b(?P<claim>[^.!?]+)", re.IGNORECASE),
)

STOP_WORDS = {
    "a",
    "an",
    "and",
    "automation",
    "based",
    "clear",
    "developer",
    "developers",
    "discovery",
    "drafts",
    "flow",
    "for",
    "grounded",
    "helps",
    "interesting",
    "it",
    "look",
    "matters",
    "open",
    "option",
    "post",
    "posts",
    "practical",
    "python",
    "readable",
    "repo",
    "repos",
    "source",
    "specific",
    "teams",
    "that",
    "the",
    "this",
    "tool",
    "useful",
    "workflows",
    "worth",
}


def normalize_post_text(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    return cleaned


def post_hash(value: str) -> str:
    return hashlib.sha256(normalize_post_text(value).encode("utf-8")).hexdigest()


class PostValidator:
    def __init__(
        self,
        settings: Settings,
        duplicate_lookup: Callable[[str], bool],
        grounding_validator: Optional[Callable[[RepositoryCandidate, str], Sequence[str]]] = None,
    ) -> None:
        self.settings = settings
        self.duplicate_lookup = duplicate_lookup
        self.grounding_validator = grounding_validator

    def validate(
        self,
        candidate: RepositoryCandidate,
        text: str,
        require_repo_url: bool = True,
        min_bytes: Optional[int] = None,
    ) -> ValidationResult:
        reasons = []
        encoded_length = len(text.encode("utf-8"))
        minimum_bytes = self.settings.content.min_text_bytes if min_bytes is None else min_bytes

        if not candidate.html_url.startswith("https://github.com/"):
            reasons.append("missing_repo_url")
        if require_repo_url and candidate.html_url not in text:
            reasons.append("missing_repo_url_in_post")
        if encoded_length < minimum_bytes:
            reasons.append("text_too_short:{0}".format(encoded_length))
        if encoded_length > self.settings.content.max_text_bytes:
            reasons.append("text_too_long:{0}".format(encoded_length))
        if self.duplicate_lookup(text):
            reasons.append("duplicate_text")

        for pattern in SPAM_PATTERNS:
            if pattern.search(text):
                reasons.append("spam_pattern:{0}".format(pattern.pattern))

        if self._looks_like_shouting(text):
            reasons.append("spam_pattern:uppercase_ratio")

        unsupported_claims = self._unsupported_claim_reasons(candidate, text)
        reasons.extend(unsupported_claims)

        if self.grounding_validator:
            reasons.extend(self.grounding_validator(candidate, text))

        return ValidationResult(is_valid=len(reasons) == 0, reasons=reasons)

    def _looks_like_shouting(self, text: str) -> bool:
        letters = [character for character in text if character.isalpha()]
        if len(letters) < 20:
            return False
        uppercase_letters = [character for character in letters if character.isupper()]
        return (len(uppercase_letters) / len(letters)) > 0.35

    def _unsupported_claim_reasons(self, candidate: RepositoryCandidate, text: str):
        allowed_tokens = self._allowed_tokens(candidate)
        reasons = []
        lowered = text.lower()

        for pattern in UNSUPPORTED_CLAIM_PATTERNS:
            for match in pattern.finditer(lowered):
                claim = match.group("claim")
                claim_tokens = {
                    token
                    for token in re.findall(r"[a-z0-9][a-z0-9\-+.#]*", claim)
                    if token not in STOP_WORDS and len(token) > 2
                }
                unknown = sorted(token for token in claim_tokens if token not in allowed_tokens)
                if unknown:
                    reasons.append("unsupported_claim:{0}".format(",".join(unknown[:4])))

        return reasons

    def _allowed_tokens(self, candidate: RepositoryCandidate):
        values = [
            candidate.owner,
            candidate.name,
            candidate.description,
            candidate.language or "",
            candidate.html_url,
            candidate.homepage,
            " ".join(candidate.topics),
            " ".join(candidate.matched_topics),
        ]
        tokens = {
            token
            for value in values
            for token in re.findall(r"[a-z0-9][a-z0-9\-+.#]*", value.lower())
            if len(token) > 2
        }
        tokens.add(str(candidate.stargazers_count))
        return tokens


class ThreadValidator:
    def __init__(
        self,
        settings: Settings,
        duplicate_lookup: Callable[[str], bool],
        grounding_validator: Optional[Callable[[RepositoryCandidate, str], Sequence[str]]] = None,
    ) -> None:
        self.settings = settings
        self.post_validator = PostValidator(
            settings=settings,
            duplicate_lookup=duplicate_lookup,
            grounding_validator=grounding_validator,
        )

    def validate(self, candidate: RepositoryCandidate, posts: Iterable[ThreadPost]) -> ValidationResult:
        post_list = tuple(posts)
        reasons = []
        if len(post_list) < self.settings.content.thread_post_count_min:
            reasons.append("thread_too_short:{0}".format(len(post_list)))
        if len(post_list) > self.settings.content.thread_post_count_max:
            reasons.append("thread_too_long:{0}".format(len(post_list)))

        normalized_posts = [normalize_post_text(post.text) for post in post_list]
        if len(set(normalized_posts)) != len(normalized_posts):
            reasons.append("thread_repetition:duplicate_posts")

        if post_list:
            thread_post_minimum = max(40, min(self.settings.content.min_text_bytes, 50))
            for index, post in enumerate(post_list, start=1):
                # soft_cta posts are URL-only follow-ups; skip normal length rules
                post_min = 10 if getattr(post, "role", "") == "soft_cta" else thread_post_minimum
                validation = self.post_validator.validate(
                    candidate,
                    post.text,
                    require_repo_url=False,
                    min_bytes=post_min,
                )
                reasons.extend(
                    "{0}:{1}".format(index, reason)
                    for reason in validation.reasons
                )
            if not any(candidate.html_url in post.text for post in post_list):
                reasons.append("missing_repo_url_in_thread")

        if _thread_is_repetitive(normalized_posts):
            reasons.append("thread_repetition:low_variation")

        return ValidationResult(is_valid=len(reasons) == 0, reasons=reasons)

    def validate_standalone(self, posts: Iterable[ThreadPost]) -> ValidationResult:
        """Validate a standalone thread (no repo URL requirement)."""
        post_list = tuple(posts)
        reasons: list[str] = []

        if not post_list:
            reasons.append("standalone_empty")
            return ValidationResult(is_valid=False, reasons=reasons)

        # Standalone threads can be shorter (1 post is fine for question/hot-take)
        max_posts = self.settings.content.thread_post_count_max
        if len(post_list) > max_posts:
            reasons.append("thread_too_long:{0}".format(len(post_list)))

        normalized_posts = [normalize_post_text(post.text) for post in post_list]
        if len(set(normalized_posts)) != len(normalized_posts):
            reasons.append("thread_repetition:duplicate_posts")

        thread_post_minimum = max(30, min(self.settings.content.min_text_bytes, 40))
        for index, post in enumerate(post_list, start=1):
            encoded_length = len(post.text.encode("utf-8"))
            if encoded_length < thread_post_minimum:
                reasons.append("{0}:text_too_short:{1}".format(index, encoded_length))
            if encoded_length > self.settings.content.max_text_bytes:
                reasons.append("{0}:text_too_long:{1}".format(index, encoded_length))
            for pattern in SPAM_PATTERNS:
                if pattern.search(post.text):
                    reasons.append("{0}:spam_pattern:{1}".format(index, pattern.pattern))

        if _thread_is_repetitive(normalized_posts):
            reasons.append("thread_repetition:low_variation")

        return ValidationResult(is_valid=len(reasons) == 0, reasons=reasons)


def _thread_is_repetitive(normalized_posts: Sequence[str]) -> bool:
    if len(normalized_posts) < 2:
        return False
    unique_ratio = len(set(normalized_posts)) / len(normalized_posts)
    return unique_ratio < 0.75
