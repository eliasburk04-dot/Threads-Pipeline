from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import timedelta
from typing import List, Optional

from threads_github_bot.config import Settings
from threads_github_bot.content_pillars import PILLAR_CATALOG, ContentPillar, select_next_pillar
from threads_github_bot.deduplication import evaluate_cooldown
from threads_github_bot.models import (
    GeneratedThread,
    PipelineRunResult,
    PublishThreadResult,
    RepositoryCandidate,
    ScheduleSlotPlan,
    ThreadPost,
    ThreadPublishPostResult,
    utcnow,
)
from threads_github_bot.scoring import rank_candidates
from threads_github_bot.series import select_series_for_candidate
from threads_github_bot.state import SQLiteStateStore
from threads_github_bot.validation import ThreadValidator

LOGGER = logging.getLogger(__name__)
VALIDATION_REPAIR_ATTEMPTS = 2
FOLLOW_UP_REPLY_TEMPLATES = (
    "{url}",
    "repo: {url}",
    "here's the repo: {url}",
)


class ThreadsGitHubPipeline:
    def __init__(
        self,
        settings: Settings,
        store: SQLiteStateStore,
        discovery_client,
        generator,
        publisher,
        validator: Optional[ThreadValidator] = None,
        standalone_generator=None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.discovery_client = discovery_client
        self.generator = generator
        self.publisher = publisher
        self.standalone_generator = standalone_generator
        self.validator = validator or ThreadValidator(
            settings=settings,
            duplicate_lookup=self.store.has_duplicate_post,
        )

    def run(
        self,
        mode: str,
        scheduled_slot: Optional[ScheduleSlotPlan] = None,
        now=None,
        pillar_override: Optional[str] = None,
    ) -> PipelineRunResult:
        self.store.initialize()
        run_at = now or utcnow()
        history = [] if mode == "test_run" else self.store.fetch_recent_thread_history()

        # --- Pillar selection ---
        if pillar_override and pillar_override in PILLAR_CATALOG:
            pillar = PILLAR_CATALOG[pillar_override]
        else:
            pillar = select_next_pillar(self.settings, history, now=run_at)

        LOGGER.info(
            "pipeline_pillar_selected",
            extra={"pillar": pillar.slug, "needs_repo": pillar.needs_repo, "mode": mode},
        )

        # --- Standalone pillar path ---
        if not pillar.needs_repo and self.standalone_generator is not None:
            return self._run_standalone(pillar, mode, scheduled_slot, run_at)

        # --- Repo-based pillar path (existing flow) ---
        return self._run_repo_based(mode, scheduled_slot, run_at, history)

    def _run_standalone(
        self,
        pillar: ContentPillar,
        mode: str,
        scheduled_slot: Optional[ScheduleSlotPlan],
        run_at,
    ) -> PipelineRunResult:
        """Generate and publish a standalone (non-repo) thread."""
        if scheduled_slot:
            self.store.upsert_schedule_slot(scheduled_slot)
        try:
            generated_thread = self.standalone_generator.generate(pillar, mode)
        except Exception:
            LOGGER.exception(
                "pipeline_standalone_generation_failed",
                extra={"pillar": pillar.slug, "mode": mode},
            )
            result = PipelineRunResult(
                status="standalone_generation_failed",
                reasons=["Standalone generation failed for {0}".format(pillar.slug)],
                scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
            )
            self._write_status(result)
            return result

        # Validate standalone content (lighter — no repo URL required)
        validation = self.validator.validate_standalone(generated_thread.posts)
        generated_thread_id, item_ids = self.store.record_generated_thread(
            generated_thread,
            selected_rank=0,
            scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
        )
        self.store.update_generated_thread_validation(
            generated_thread_id,
            "passed" if validation.is_valid else "failed",
            validation.reasons,
        )

        if not validation.is_valid:
            LOGGER.info(
                "pipeline_standalone_validation_failed",
                extra={"pillar": pillar.slug, "reasons": validation.reasons},
            )
            # Fall back to repo-based
            history = self.store.fetch_recent_thread_history()
            return self._run_repo_based(mode, scheduled_slot, run_at, history)

        publish_result = self._publish_or_simulate(generated_thread.posts, mode)
        success_status, result_status = self._publish_statuses_for_mode(mode)
        self.store.record_thread_publish_attempts(
            generated_thread_id,
            item_ids=item_ids,
            repo_id=generated_thread.repo.repo_id,
            mode=mode,
            result=publish_result,
            success_status=success_status,
        )

        if publish_result.success:
            if scheduled_slot:
                self.store.update_schedule_slot(
                    scheduled_slot.slot_key,
                    result_status,
                    generated_thread_id=generated_thread_id,
                    actual_publish_at_utc=run_at.isoformat(),
                )
            LOGGER.info(
                "pipeline_standalone_ready",
                extra={"pillar": pillar.slug, "mode": mode},
            )
            result = PipelineRunResult(
                status=result_status,
                selected_repo="standalone/{0}".format(pillar.slug),
                selected_series=pillar.label,
                scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
            )
            self._write_status(result)
            return result

        result = PipelineRunResult(
            status="publish_failed",
            selected_repo="standalone/{0}".format(pillar.slug),
            selected_series=pillar.label,
            reasons=[publish_result.error or "Unknown publish failure"],
            scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
        )
        self._write_status(result)
        return result

    def _run_repo_based(
        self,
        mode: str,
        scheduled_slot: Optional[ScheduleSlotPlan],
        run_at,
        history,
    ) -> PipelineRunResult:
        """Run the original repo-discovery pipeline flow."""
        reserve_candidates: List[RepositoryCandidate] = []
        if scheduled_slot:
            self.store.upsert_schedule_slot(scheduled_slot)
        if mode == "test_run":
            candidates = self.discovery_client.fetch_candidates()
            filtered = self._filter_candidates(candidates, run_at)
            previous_snapshots = self.store.fetch_previous_snapshots(candidate.repo_id for candidate in filtered)
            ranked = rank_candidates(
                filtered,
                self.settings,
                run_at,
                history=history,
                previous_snapshots=previous_snapshots,
            )
            discovered_count = len(candidates)
            filtered_count = len(filtered)
        else:
            refreshed = self.refresh_queue(now=run_at, history=history)
            ranked = refreshed["ranked"]
            discovered_count = refreshed["discovered_count"]
            filtered_count = refreshed["filtered_count"]
            reserve_candidates = self.store.load_reserve_candidates(limit=self.settings.reserve.size)
        LOGGER.info(
            "pipeline_candidates_ranked",
            extra={
                "mode": mode,
                "discovered_count": discovered_count,
                "filtered_count": filtered_count,
                "history_count": len(history),
                "top_candidates": [candidate.full_name for candidate in ranked[: min(5, len(ranked))]],
            },
        )
        if reserve_candidates:
            LOGGER.info(
                "pipeline_reserve_loaded",
                extra={
                    "mode": mode,
                    "reserve_count": len(reserve_candidates),
                    "top_candidates": [
                        candidate.full_name
                        for candidate in reserve_candidates[: min(5, len(reserve_candidates))]
                    ],
                },
            )

        if not ranked and not reserve_candidates:
            result = PipelineRunResult(
                status="no_candidates",
                reasons=["No candidates matched filters"],
                scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
            )
            if scheduled_slot:
                self.store.update_schedule_slot(scheduled_slot.slot_key, "no_candidates")
            self._write_status(result)
            return result

        result = self._attempt_candidates(
            candidates=ranked,
            history=history,
            mode=mode,
            scheduled_slot=scheduled_slot,
            run_at=run_at,
            source="queue",
            rank_offset=0,
        )
        if result:
            self._write_status(result)
            return result

        if reserve_candidates:
            result = self._attempt_candidates(
                candidates=reserve_candidates,
                history=history,
                mode=mode,
                scheduled_slot=scheduled_slot,
                run_at=run_at,
                source="reserve",
                rank_offset=len(ranked),
            )
            if result:
                self._write_status(result)
                return result

        result = PipelineRunResult(
            status="no_valid_candidate",
            reasons=["No queued or reserve candidate cleared cooldown and validation"],
            scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
        )
        if scheduled_slot:
            self.store.update_schedule_slot(scheduled_slot.slot_key, "no_valid_candidate")
        LOGGER.warning("pipeline_no_valid_candidate", extra={"mode": mode})
        self._write_status(result)
        return result

    def _attempt_candidates(
        self,
        candidates: List[RepositoryCandidate],
        history,
        mode: str,
        scheduled_slot: Optional[ScheduleSlotPlan],
        run_at,
        source: str,
        rank_offset: int,
    ) -> Optional[PipelineRunResult]:
        for index, candidate in enumerate(candidates, start=rank_offset + 1):
            allowed, cooldown_reasons = evaluate_cooldown(candidate, history, self.settings, run_at)
            if not allowed:
                self._mark_candidate_status(candidate.repo_id, source, "skipped_cooldown")
                LOGGER.info(
                    "pipeline_candidate_skipped",
                    extra={
                        "repo": candidate.full_name,
                        "reasons": cooldown_reasons,
                        "source": source,
                    },
                )
                continue

            series = select_series_for_candidate(candidate, self.settings, recent_history=history)
            try:
                generated_thread, validation = self._generate_validated_thread(candidate, series, mode)
            except Exception:  # pragma: no cover - runtime
                LOGGER.exception(
                    "pipeline_generation_failed",
                    extra={"repo": candidate.full_name, "rank": index, "source": source},
                )
                self._mark_candidate_status(candidate.repo_id, source, "generation_failed")
                continue

            generated_thread_id, item_ids = self.store.record_generated_thread(
                generated_thread,
                selected_rank=index,
                scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
            )
            self.store.update_generated_thread_validation(
                generated_thread_id,
                "passed" if validation.is_valid else "failed",
                validation.reasons,
            )

            if not validation.is_valid:
                self._mark_candidate_status(candidate.repo_id, source, "validation_failed")
                LOGGER.info(
                    "pipeline_validation_failed",
                    extra={
                        "repo": candidate.full_name,
                        "reasons": validation.reasons,
                        "source": source,
                    },
                )
                continue

            publish_result = self._publish_or_simulate(generated_thread.posts, mode)
            success_status, result_status = self._publish_statuses_for_mode(mode)
            self.store.record_thread_publish_attempts(
                generated_thread_id,
                item_ids=item_ids,
                repo_id=candidate.repo_id,
                mode=mode,
                result=publish_result,
                success_status=success_status,
            )

            if publish_result.success:
                self.store.mark_repo_selected(candidate.repo_id)
                self._mark_candidate_status(candidate.repo_id, source, success_status)
                if scheduled_slot:
                    self.store.update_schedule_slot(
                        scheduled_slot.slot_key,
                        result_status,
                        generated_thread_id=generated_thread_id,
                        actual_publish_at_utc=run_at.isoformat(),
                    )
                LOGGER.info(
                    "pipeline_ready",
                    extra={
                        "repo": candidate.full_name,
                        "mode": mode,
                        "series": series.display_label,
                        "source": source,
                    },
                )
                return PipelineRunResult(
                    status=result_status,
                    selected_repo=candidate.full_name,
                    selected_series=series.display_label,
                    scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
                )

            self._mark_candidate_status(candidate.repo_id, source, "publish_failed")
            if scheduled_slot:
                self.store.update_schedule_slot(
                    scheduled_slot.slot_key,
                    "publish_failed",
                    generated_thread_id=generated_thread_id,
                    last_error=publish_result.error,
                )
            LOGGER.error(
                "pipeline_publish_failed",
                extra={
                    "repo": candidate.full_name,
                    "error": publish_result.error,
                    "source": source,
                },
            )
            return PipelineRunResult(
                status="publish_failed",
                selected_repo=candidate.full_name,
                selected_series=series.display_label,
                reasons=[publish_result.error or "Unknown publish failure"],
                scheduled_slot_key=scheduled_slot.slot_key if scheduled_slot else None,
            )

        return None

    def _mark_candidate_status(self, repo_id: int, source: str, status: str) -> None:
        if source == "queue":
            self.store.mark_queue_status(repo_id, status)
            return

        reserve_status = None
        if status in {"generation_failed", "validation_failed"}:
            reserve_status = "invalidated"
        elif status == "publish_failed":
            reserve_status = "publish_failed"
        elif status == "published":
            reserve_status = "selected"

        self.store.update_reserve_candidate(
            repo_id,
            reserve_status=reserve_status,
            last_result=status,
        )

    def _publish_statuses_for_mode(self, mode: str) -> tuple[str, str]:
        if mode == "dry_run":
            return "dry_run", "dry_run_ready"
        if mode == "test_run":
            return "test_run", "test_run_ready"
        return "published", "published"

    def _generate_validated_thread(self, candidate, series, mode):
        validation_reasons = ()
        generated_thread = None
        validation = None

        for _attempt in range(VALIDATION_REPAIR_ATTEMPTS + 1):
            generated_thread = self.generator.generate(
                candidate,
                series,
                mode,
                validation_reasons=validation_reasons,
            )
            generated_thread = compose_publishable_thread(self.settings, generated_thread)
            validation = self.validator.validate(candidate, generated_thread.posts)
            if validation.is_valid:
                return generated_thread, validation
            validation_reasons = tuple(validation.reasons)

        if generated_thread is None or validation is None:
            raise ValueError("Thread generation did not produce a candidate")
        return generated_thread, validation

    def refresh_queue(self, now=None, history=None) -> dict:
        run_at = now or utcnow()
        history = history if history is not None else self.store.fetch_recent_thread_history()
        candidates = self.discovery_client.fetch_candidates()
        filtered = self._filter_candidates(candidates, run_at)
        previous_snapshots = self.store.fetch_previous_snapshots(candidate.repo_id for candidate in filtered)
        ranked = rank_candidates(
            filtered,
            self.settings,
            run_at,
            history=history,
            previous_snapshots=previous_snapshots,
        )
        shortlisted = ranked[: self.settings.github.queue_size]
        for candidate in shortlisted:
            self.store.upsert_repository(candidate)
        self.store.refresh_queue(shortlisted)
        self.store.refresh_reserve(
            ranked[self.settings.github.queue_size :],
            max_items=self.settings.reserve.size,
            min_score=self.settings.reserve.min_score,
            max_age_days=self.settings.reserve.max_age_days,
            exclude_repo_ids=(candidate.repo_id for candidate in shortlisted),
        )
        return {
            "ranked": shortlisted,
            "discovered_count": len(candidates),
            "filtered_count": len(filtered),
        }

    def _publish_or_simulate(self, posts, mode: str) -> PublishThreadResult:
        if mode in {"dry_run", "test_run"}:
            return PublishThreadResult(
                success=True,
                posts=[
                    ThreadPublishPostResult(
                        success=True,
                        container_id="dry_run_{0}".format(index),
                        media_id="dry_run_{0}".format(index),
                        response={"mode": mode},
                    )
                    for index, _item in enumerate(posts, start=1)
                ],
            )
        return self.publisher.publish_thread([post.text for post in posts])

    def _filter_candidates(self, candidates: List[RepositoryCandidate], now) -> List[RepositoryCandidate]:
        filtered = []
        whitelist = set(self.settings.github.whitelist_repos)
        blacklist = set(self.settings.github.blacklist_repos)
        whitelist_topics = set(self.settings.github.whitelist_topics)
        blacklist_topics = set(self.settings.github.blacklist_topics)

        for candidate in candidates:
            topics = set(candidate.topics).union(set(candidate.matched_topics))
            description = (candidate.description or "").strip()
            age = now - candidate.pushed_at

            if candidate.archived or candidate.fork:
                continue
            if whitelist and candidate.full_name not in whitelist:
                continue
            if candidate.full_name in blacklist:
                continue
            if whitelist_topics and not topics.intersection(whitelist_topics):
                continue
            if blacklist_topics and topics.intersection(blacklist_topics):
                continue
            if len(description) < self.settings.github.min_description_length:
                continue
            if candidate.stargazers_count < self.settings.github.min_stars:
                continue
            if age > timedelta(days=self.settings.github.recent_activity_days):
                continue
            filtered.append(candidate)

        return filtered

    def _write_status(self, result: PipelineRunResult) -> None:
        self.settings.runtime.status_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": utcnow().isoformat(),
            "status": result.status,
            "selected_repo": result.selected_repo,
            "selected_series": result.selected_series,
            "scheduled_slot_key": result.scheduled_slot_key,
            "reasons": list(result.reasons),
        }
        self.settings.runtime.status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def compose_publishable_thread(settings: Settings, generated_thread: GeneratedThread) -> GeneratedThread:
    if any(generated_thread.repo.html_url in post.text for post in generated_thread.posts):
        return generated_thread
    if len(generated_thread.posts) >= settings.content.thread_post_count_max:
        return generated_thread
    follow_up = ThreadPost(
        position=len(generated_thread.posts) + 1,
        role="soft_cta",
        text=_build_soft_follow_up_reply(generated_thread.repo),
    )
    return replace(generated_thread, posts=generated_thread.posts + (follow_up,))


def _build_soft_follow_up_reply(candidate: RepositoryCandidate) -> str:
    template = FOLLOW_UP_REPLY_TEMPLATES[candidate.repo_id % len(FOLLOW_UP_REPLY_TEMPLATES)]
    return template.format(url=candidate.html_url)
