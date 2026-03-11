from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from threads_github_bot.deduplication import similarity_key
from threads_github_bot.models import (
    GeneratedPost,
    GeneratedThread,
    PostedRepositorySnapshot,
    PublishResult,
    PublishThreadResult,
    RepositoryCandidate,
    ScheduleSlotPlan,
    utcnow,
)
from threads_github_bot.validation import post_hash

MIGRATIONS = (
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS repositories_seen (
            repo_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            owner TEXT NOT NULL,
            description TEXT NOT NULL,
            html_url TEXT NOT NULL,
            homepage TEXT NOT NULL,
            topics_json TEXT NOT NULL,
            matched_topics_json TEXT NOT NULL,
            language TEXT,
            stargazers_count INTEGER NOT NULL,
            forks_count INTEGER NOT NULL,
            archived INTEGER NOT NULL,
            fork INTEGER NOT NULL,
            pushed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_score REAL,
            last_score_breakdown_json TEXT,
            similarity_key TEXT NOT NULL,
            last_selected_at TEXT
        );

        CREATE TABLE IF NOT EXISTS generated_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            repo_full_name TEXT NOT NULL,
            language TEXT NOT NULL,
            mode TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            post_text TEXT NOT NULL,
            normalized_text_hash TEXT NOT NULL,
            generator_response_json TEXT NOT NULL DEFAULT '{}',
            validation_status TEXT NOT NULL DEFAULT 'pending',
            validation_reasons_json TEXT NOT NULL DEFAULT '[]',
            dry_run INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            selected_rank INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_generated_posts_hash
            ON generated_posts(normalized_text_hash);

        CREATE TABLE IF NOT EXISTS publish_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_post_id INTEGER NOT NULL,
            repo_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            threads_container_id TEXT,
            threads_media_id TEXT,
            http_status INTEGER,
            response_body TEXT,
            error_normalized TEXT,
            created_at TEXT NOT NULL,
            published_at TEXT
        );

        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        """
        CREATE TABLE IF NOT EXISTS repository_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            stargazers_count INTEGER NOT NULL,
            forks_count INTEGER NOT NULL,
            captured_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_repository_snapshots_repo_time
            ON repository_snapshots(repo_id, captured_at DESC);

        CREATE TABLE IF NOT EXISTS candidate_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL UNIQUE,
            repo_full_name TEXT NOT NULL,
            queue_score REAL NOT NULL,
            discoverability_score REAL NOT NULL,
            trend_score REAL NOT NULL,
            novelty_score REAL NOT NULL,
            content_fit_score REAL NOT NULL,
            score_breakdown_json TEXT NOT NULL,
            topics_json TEXT NOT NULL,
            matched_topics_json TEXT NOT NULL,
            candidate_json TEXT NOT NULL,
            queue_status TEXT NOT NULL DEFAULT 'queued',
            enqueued_at TEXT NOT NULL,
            last_considered_at TEXT
        );

        CREATE TABLE IF NOT EXISTS generated_threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            repo_full_name TEXT NOT NULL,
            language TEXT NOT NULL,
            mode TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            series_slug TEXT NOT NULL,
            series_label TEXT NOT NULL,
            series_number INTEGER,
            flattened_text TEXT NOT NULL,
            normalized_thread_hash TEXT NOT NULL,
            generator_response_json TEXT NOT NULL DEFAULT '{}',
            validation_status TEXT NOT NULL DEFAULT 'pending',
            validation_reasons_json TEXT NOT NULL DEFAULT '[]',
            dry_run INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            selected_rank INTEGER NOT NULL,
            scheduled_slot_key TEXT,
            discoverability_score REAL NOT NULL DEFAULT 0,
            trend_score REAL NOT NULL DEFAULT 0,
            novelty_score REAL NOT NULL DEFAULT 0,
            content_fit_score REAL NOT NULL DEFAULT 0,
            score_breakdown_json TEXT NOT NULL DEFAULT '{}',
            engagement_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE INDEX IF NOT EXISTS idx_generated_threads_hash
            ON generated_threads(normalized_thread_hash);

        CREATE TABLE IF NOT EXISTS generated_thread_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_thread_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            role TEXT NOT NULL,
            post_text TEXT NOT NULL,
            normalized_text_hash TEXT NOT NULL,
            validation_status TEXT NOT NULL DEFAULT 'pending',
            validation_reasons_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_generated_thread_items_hash
            ON generated_thread_items(normalized_text_hash);

        CREATE TABLE IF NOT EXISTS thread_publish_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_thread_id INTEGER NOT NULL,
            generated_thread_item_id INTEGER NOT NULL,
            repo_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            reply_to_id TEXT,
            threads_container_id TEXT,
            threads_media_id TEXT,
            http_status INTEGER,
            response_body TEXT,
            error_normalized TEXT,
            created_at TEXT NOT NULL,
            published_at TEXT
        );

        CREATE TABLE IF NOT EXISTS scheduled_slots (
            slot_key TEXT PRIMARY KEY,
            local_date TEXT NOT NULL,
            slot_name TEXT NOT NULL,
            base_local_iso TEXT NOT NULL,
            planned_local_iso TEXT NOT NULL,
            planned_publish_at_utc TEXT NOT NULL,
            jitter_minutes INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'planned',
            generated_thread_id INTEGER,
            actual_publish_at_utc TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS reserve_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL UNIQUE,
            repo_full_name TEXT NOT NULL,
            reserve_score REAL NOT NULL,
            discoverability_score REAL NOT NULL,
            trend_score REAL NOT NULL,
            novelty_score REAL NOT NULL,
            content_fit_score REAL NOT NULL,
            score_breakdown_json TEXT NOT NULL,
            topics_json TEXT NOT NULL,
            matched_topics_json TEXT NOT NULL,
            candidate_json TEXT NOT NULL,
            reserve_status TEXT NOT NULL DEFAULT 'reserved',
            added_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_considered_at TEXT,
            last_result TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_reserve_candidates_status_score
            ON reserve_candidates(reserve_status, reserve_score DESC, updated_at DESC);
        """,
    ),
    (
        4,
        """
        ALTER TABLE generated_threads ADD COLUMN pillar_slug TEXT NOT NULL DEFAULT 'repo-discovery';
        """,
    ),
)


class SQLiteStateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for version, sql in MIGRATIONS:
                already_applied = connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?",
                    (version,),
                ).fetchone()
                if already_applied:
                    continue
                connection.executescript(sql)
                connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                    (version, utcnow().isoformat()),
                )
            connection.commit()

    def upsert_repository(self, candidate: RepositoryCandidate) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            # Check by repo_id OR full_name so that standalone placeholders whose
            # repo_id changed between runs (e.g. topic-based → pillar-based hash)
            # are correctly found and updated instead of causing a UNIQUE violation.
            existing = connection.execute(
                "SELECT repo_id FROM repositories_seen WHERE repo_id = ? OR full_name = ?",
                (candidate.repo_id, candidate.full_name),
            ).fetchone()
            db_repo_id = existing[0] if existing else candidate.repo_id
            if existing:
                connection.execute(
                    """
                    UPDATE repositories_seen
                    SET full_name = ?, name = ?, owner = ?, description = ?, html_url = ?, homepage = ?,
                        topics_json = ?, matched_topics_json = ?, language = ?, stargazers_count = ?, forks_count = ?,
                        archived = ?, fork = ?, pushed_at = ?, updated_at = ?, last_seen_at = ?, last_score = ?,
                        last_score_breakdown_json = ?, similarity_key = ?
                    WHERE repo_id = ?
                    """,
                    (
                        candidate.full_name,
                        candidate.name,
                        candidate.owner,
                        candidate.description or "",
                        candidate.html_url,
                        candidate.homepage or "",
                        json.dumps(candidate.topics),
                        json.dumps(candidate.matched_topics),
                        candidate.language,
                        candidate.stargazers_count,
                        candidate.forks_count,
                        int(candidate.archived),
                        int(candidate.fork),
                        candidate.pushed_at.isoformat(),
                        candidate.updated_at.isoformat(),
                        now,
                        candidate.score,
                        json.dumps(candidate.score_breakdown),
                        similarity_key(candidate),
                        db_repo_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO repositories_seen (
                        repo_id, full_name, name, owner, description, html_url, homepage,
                        topics_json, matched_topics_json, language, stargazers_count, forks_count,
                        archived, fork, pushed_at, updated_at, first_seen_at, last_seen_at,
                        last_score, last_score_breakdown_json, similarity_key, last_selected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.repo_id,
                        candidate.full_name,
                        candidate.name,
                        candidate.owner,
                        candidate.description or "",
                        candidate.html_url,
                        candidate.homepage or "",
                        json.dumps(candidate.topics),
                        json.dumps(candidate.matched_topics),
                        candidate.language,
                        candidate.stargazers_count,
                        candidate.forks_count,
                        int(candidate.archived),
                        int(candidate.fork),
                        candidate.pushed_at.isoformat(),
                        candidate.updated_at.isoformat(),
                        now,
                        now,
                        candidate.score,
                        json.dumps(candidate.score_breakdown),
                        similarity_key(candidate),
                        None,
                    ),
                )
            connection.execute(
                """
                INSERT INTO repository_snapshots (repo_id, stargazers_count, forks_count, captured_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    db_repo_id,
                    candidate.stargazers_count,
                    candidate.forks_count,
                    now,
                ),
            )
            connection.commit()

    def fetch_previous_snapshots(self, repo_ids: Iterable[int]) -> Dict[int, Dict]:
        repo_id_list = tuple(sorted(set(repo_ids)))
        if not repo_id_list:
            return {}
        placeholders = ",".join("?" for _ in repo_id_list)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT rs.repo_id, rs.stargazers_count, rs.forks_count, rs.captured_at
                FROM repository_snapshots rs
                JOIN (
                    SELECT repo_id, MAX(captured_at) AS captured_at
                    FROM repository_snapshots
                    WHERE repo_id IN ({0})
                    GROUP BY repo_id
                ) latest
                  ON latest.repo_id = rs.repo_id
                 AND latest.captured_at = rs.captured_at
                """.format(placeholders),
                repo_id_list,
            ).fetchall()
        return {
            row["repo_id"]: {
                "stargazers_count": row["stargazers_count"],
                "forks_count": row["forks_count"],
                "captured_at": _parse_datetime(row["captured_at"]),
            }
            for row in rows
        }

    def refresh_queue(self, candidates: Sequence[RepositoryCandidate]) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            connection.execute("DELETE FROM candidate_queue")
            for candidate in candidates:
                connection.execute(
                    """
                    INSERT INTO candidate_queue (
                        repo_id, repo_full_name, queue_score, discoverability_score, trend_score,
                        novelty_score, content_fit_score, score_breakdown_json, topics_json,
                        matched_topics_json, candidate_json, queue_status, enqueued_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
                    """,
                    (
                        candidate.repo_id,
                        candidate.full_name,
                        candidate.score,
                        candidate.discoverability_score,
                        candidate.trend_score,
                        candidate.novelty_score,
                        candidate.content_fit_score,
                        json.dumps(candidate.score_breakdown),
                        json.dumps(candidate.topics),
                        json.dumps(candidate.matched_topics),
                        json.dumps(_candidate_to_json(candidate)),
                        now,
                    ),
                )
            self._set_metadata(
                connection,
                "last_discovery_run",
                json.dumps({"timestamp": now, "count": len(candidates)}),
            )
            connection.commit()

    def mark_queue_status(self, repo_id: int, status: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE candidate_queue
                SET queue_status = ?, last_considered_at = ?
                WHERE repo_id = ?
                """,
                (status, utcnow().isoformat(), repo_id),
            )
            connection.commit()

    def list_queue(self, limit: int = 10) -> List[Dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT repo_full_name, queue_score, discoverability_score, trend_score,
                       novelty_score, content_fit_score, score_breakdown_json, queue_status, enqueued_at
                FROM candidate_queue
                ORDER BY queue_score DESC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "repo_full_name": row["repo_full_name"],
                "queue_score": row["queue_score"],
                "discoverability_score": row["discoverability_score"],
                "trend_score": row["trend_score"],
                "novelty_score": row["novelty_score"],
                "content_fit_score": row["content_fit_score"],
                "score_breakdown": json.loads(row["score_breakdown_json"]),
                "queue_status": row["queue_status"],
                "enqueued_at": row["enqueued_at"],
            }
            for row in rows
        ]

    def load_queued_candidate(self, repo_full_name: Optional[str] = None) -> Optional[RepositoryCandidate]:
        with self._connect() as connection:
            if repo_full_name:
                row = connection.execute(
                    """
                    SELECT candidate_json
                    FROM candidate_queue
                    WHERE repo_full_name = ?
                    ORDER BY queue_score DESC
                    LIMIT 1
                    """,
                    (repo_full_name,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT candidate_json
                    FROM candidate_queue
                    ORDER BY queue_score DESC, id ASC
                    LIMIT 1
                    """
                ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["candidate_json"])
        return _candidate_from_json(payload)

    def refresh_reserve(
        self,
        candidates: Sequence[RepositoryCandidate],
        max_items: int,
        min_score: float,
        max_age_days: int,
        exclude_repo_ids: Iterable[int] = (),
    ) -> None:
        now_dt = utcnow()
        now = now_dt.isoformat()
        cutoff = (now_dt - timedelta(days=max(1, max_age_days))).isoformat()
        exclude_ids = set(exclude_repo_ids)
        active_candidates = [
            candidate
            for candidate in candidates
            if candidate.repo_id not in exclude_ids and candidate.score >= min_score
        ]

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE reserve_candidates
                SET reserve_status = 'expired', updated_at = ?
                WHERE reserve_status = 'reserved' AND added_at < ?
                """,
                (now, cutoff),
            )
            existing_rows = connection.execute(
                """
                SELECT repo_id, candidate_json, added_at
                FROM reserve_candidates
                WHERE reserve_status = 'reserved' AND added_at >= ?
                """,
                (cutoff,),
            ).fetchall()

            pool: Dict[int, Dict[str, object]] = {}
            for row in existing_rows:
                repo_id = int(row["repo_id"])
                if repo_id in exclude_ids:
                    continue
                pool[repo_id] = {
                    "candidate": _candidate_from_json(json.loads(row["candidate_json"])),
                    "added_at": row["added_at"],
                }

            for candidate in active_candidates:
                existing = pool.get(candidate.repo_id)
                pool[candidate.repo_id] = {
                    "candidate": candidate,
                    "added_at": existing["added_at"] if existing else now,
                }

            selected = _select_reserve_candidates(
                [item["candidate"] for item in pool.values()],
                max_items=max_items,
            )
            selected_ids = {candidate.repo_id for candidate in selected}
            for candidate in selected:
                added_at = str(pool[candidate.repo_id]["added_at"])
                connection.execute(
                    """
                    INSERT INTO reserve_candidates (
                        repo_id, repo_full_name, reserve_score, discoverability_score, trend_score,
                        novelty_score, content_fit_score, score_breakdown_json, topics_json,
                        matched_topics_json, candidate_json, reserve_status, added_at, updated_at,
                        last_considered_at, last_result
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?, NULL, NULL)
                    ON CONFLICT(repo_id) DO UPDATE SET
                        repo_full_name = excluded.repo_full_name,
                        reserve_score = excluded.reserve_score,
                        discoverability_score = excluded.discoverability_score,
                        trend_score = excluded.trend_score,
                        novelty_score = excluded.novelty_score,
                        content_fit_score = excluded.content_fit_score,
                        score_breakdown_json = excluded.score_breakdown_json,
                        topics_json = excluded.topics_json,
                        matched_topics_json = excluded.matched_topics_json,
                        candidate_json = excluded.candidate_json,
                        reserve_status = 'reserved',
                        added_at = excluded.added_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        candidate.repo_id,
                        candidate.full_name,
                        candidate.score,
                        candidate.discoverability_score,
                        candidate.trend_score,
                        candidate.novelty_score,
                        candidate.content_fit_score,
                        json.dumps(candidate.score_breakdown),
                        json.dumps(candidate.topics),
                        json.dumps(candidate.matched_topics),
                        json.dumps(_candidate_to_json(candidate)),
                        added_at,
                        now,
                    ),
                )

            if selected_ids:
                placeholders = ",".join("?" for _ in selected_ids)
                connection.execute(
                    """
                    UPDATE reserve_candidates
                    SET reserve_status = 'trimmed', updated_at = ?
                    WHERE reserve_status = 'reserved'
                      AND repo_id NOT IN ({0})
                    """.format(placeholders),
                    (now, *selected_ids),
                )
            else:
                connection.execute(
                    """
                    UPDATE reserve_candidates
                    SET reserve_status = 'trimmed', updated_at = ?
                    WHERE reserve_status = 'reserved'
                    """,
                    (now,),
                )
            connection.commit()

    def list_reserve(self, limit: int = 10) -> List[Dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT repo_full_name, reserve_score, discoverability_score, trend_score,
                       novelty_score, content_fit_score, score_breakdown_json, reserve_status,
                       added_at, updated_at, last_considered_at, last_result
                FROM reserve_candidates
                WHERE reserve_status = 'reserved'
                ORDER BY reserve_score DESC, updated_at DESC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "repo_full_name": row["repo_full_name"],
                "reserve_score": row["reserve_score"],
                "discoverability_score": row["discoverability_score"],
                "trend_score": row["trend_score"],
                "novelty_score": row["novelty_score"],
                "content_fit_score": row["content_fit_score"],
                "score_breakdown": json.loads(row["score_breakdown_json"]),
                "reserve_status": row["reserve_status"],
                "added_at": row["added_at"],
                "updated_at": row["updated_at"],
                "last_considered_at": row["last_considered_at"],
                "last_result": row["last_result"],
            }
            for row in rows
        ]

    def load_reserve_candidates(self, limit: int = 10) -> List[RepositoryCandidate]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT candidate_json
                FROM reserve_candidates
                WHERE reserve_status = 'reserved'
                ORDER BY reserve_score DESC, updated_at DESC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_candidate_from_json(json.loads(row["candidate_json"])) for row in rows]

    def update_reserve_candidate(
        self,
        repo_id: int,
        reserve_status: Optional[str] = None,
        last_result: Optional[str] = None,
    ) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE reserve_candidates
                SET reserve_status = COALESCE(?, reserve_status),
                    last_result = COALESCE(?, last_result),
                    last_considered_at = ?,
                    updated_at = ?
                WHERE repo_id = ?
                """,
                (
                    reserve_status,
                    last_result,
                    now,
                    now,
                    repo_id,
                ),
            )
            connection.commit()

    def record_generated_thread(
        self,
        generated_thread: GeneratedThread,
        selected_rank: int,
        scheduled_slot_key: Optional[str] = None,
    ) -> Tuple[int, List[int]]:
        self.upsert_repository(generated_thread.repo)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO generated_threads (
                    repo_id, repo_full_name, language, mode, model, prompt_version,
                    series_slug, series_label, series_number, flattened_text, normalized_thread_hash,
                    generator_response_json, validation_status, validation_reasons_json, dry_run, created_at,
                    selected_rank, scheduled_slot_key, discoverability_score, trend_score, novelty_score,
                    content_fit_score, score_breakdown_json, pillar_slug
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generated_thread.repo.repo_id,
                    generated_thread.repo.full_name,
                    generated_thread.language,
                    generated_thread.mode,
                    generated_thread.model,
                    generated_thread.prompt_version,
                    generated_thread.series_slug,
                    generated_thread.series_label,
                    generated_thread.series_number,
                    generated_thread.flattened_text,
                    post_hash(generated_thread.flattened_text),
                    json.dumps(generated_thread.raw_response or {}),
                    int(generated_thread.mode != "post_now"),
                    utcnow().isoformat(),
                    selected_rank,
                    scheduled_slot_key,
                    generated_thread.repo.discoverability_score,
                    generated_thread.repo.trend_score,
                    generated_thread.repo.novelty_score,
                    generated_thread.repo.content_fit_score,
                    json.dumps(generated_thread.repo.score_breakdown),
                    generated_thread.pillar_slug,
                ),
            )
            thread_id = int(cursor.lastrowid)
            item_ids: List[int] = []
            for item in generated_thread.posts:
                item_cursor = connection.execute(
                    """
                    INSERT INTO generated_thread_items (
                        generated_thread_id, position, role, post_text, normalized_text_hash,
                        validation_status, validation_reasons_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', '[]', ?)
                    """,
                    (
                        thread_id,
                        item.position,
                        item.role,
                        item.text,
                        post_hash(item.text),
                        utcnow().isoformat(),
                    ),
                )
                item_ids.append(int(item_cursor.lastrowid))
            connection.commit()
            return thread_id, item_ids

    def update_generated_thread_validation(
        self,
        generated_thread_id: int,
        status: str,
        reasons: Sequence[str],
    ) -> None:
        grouped = _group_item_reasons(reasons)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE generated_threads
                SET validation_status = ?, validation_reasons_json = ?
                WHERE id = ?
                """,
                (status, json.dumps(list(reasons)), generated_thread_id),
            )
            if grouped:
                rows = connection.execute(
                    """
                    SELECT id, position FROM generated_thread_items
                    WHERE generated_thread_id = ?
                    """,
                    (generated_thread_id,),
                ).fetchall()
                for row in rows:
                    item_reasons = grouped.get(row["position"], [])
                    connection.execute(
                        """
                        UPDATE generated_thread_items
                        SET validation_status = ?, validation_reasons_json = ?
                        WHERE id = ?
                        """,
                        (
                            "failed" if item_reasons else "passed",
                            json.dumps(item_reasons),
                            row["id"],
                        ),
                    )
            else:
                connection.execute(
                    """
                    UPDATE generated_thread_items
                    SET validation_status = ?
                    WHERE generated_thread_id = ?
                    """,
                    ("passed" if status == "passed" else "pending", generated_thread_id),
                )
            connection.commit()

    def record_thread_publish_attempts(
        self,
        generated_thread_id: int,
        item_ids: Sequence[int],
        repo_id: int,
        mode: str,
        result: PublishThreadResult,
        success_status: str,
    ) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            for item_id, post_result in zip(item_ids, result.posts):
                connection.execute(
                    """
                    INSERT INTO thread_publish_attempts (
                        generated_thread_id, generated_thread_item_id, repo_id, mode, status, retry_count,
                        reply_to_id, threads_container_id, threads_media_id, http_status, response_body,
                        error_normalized, created_at, published_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        generated_thread_id,
                        item_id,
                        repo_id,
                        mode,
                        success_status if post_result.success else "failed",
                        post_result.reply_to_id,
                        post_result.container_id,
                        post_result.media_id,
                        post_result.status_code,
                        json.dumps(post_result.response or {}),
                        post_result.error,
                        now,
                        now if post_result.success else None,
                    ),
                )
            connection.commit()

    def upsert_schedule_slot(self, slot: ScheduleSlotPlan) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO scheduled_slots (
                    slot_key, local_date, slot_name, base_local_iso, planned_local_iso,
                    planned_publish_at_utc, jitter_minutes, status, generated_thread_id,
                    actual_publish_at_utc, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                ON CONFLICT(slot_key) DO UPDATE SET
                    base_local_iso = excluded.base_local_iso,
                    planned_local_iso = excluded.planned_local_iso,
                    planned_publish_at_utc = excluded.planned_publish_at_utc,
                    jitter_minutes = excluded.jitter_minutes,
                    updated_at = excluded.updated_at
                """,
                (
                    slot.slot_key,
                    slot.local_date,
                    slot.slot_name,
                    slot.base_local.isoformat(),
                    slot.planned_local.isoformat(),
                    slot.planned_at_utc.isoformat(),
                    slot.jitter_minutes,
                    slot.status,
                    now,
                    now,
                ),
            )
            connection.commit()

    def update_schedule_slot(
        self,
        slot_key: str,
        status: str,
        generated_thread_id: Optional[int] = None,
        actual_publish_at_utc: Optional[str] = None,
        last_error: Optional[str] = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_slots
                SET status = ?, generated_thread_id = COALESCE(?, generated_thread_id),
                    actual_publish_at_utc = COALESCE(?, actual_publish_at_utc),
                    last_error = ?, updated_at = ?
                WHERE slot_key = ?
                """,
                (
                    status,
                    generated_thread_id,
                    actual_publish_at_utc,
                    last_error,
                    utcnow().isoformat(),
                    slot_key,
                ),
            )
            connection.commit()

    def get_schedule_slot(self, slot_key: str) -> Optional[ScheduleSlotPlan]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT slot_key, local_date, slot_name, base_local_iso, planned_local_iso,
                       planned_publish_at_utc, jitter_minutes, status, actual_publish_at_utc
                FROM scheduled_slots
                WHERE slot_key = ?
                """,
                (slot_key,),
            ).fetchone()
        if row is None:
            return None
        return ScheduleSlotPlan(
            slot_key=row["slot_key"],
            local_date=row["local_date"],
            slot_name=row["slot_name"],
            base_local=_parse_datetime(row["base_local_iso"]),
            planned_local=_parse_datetime(row["planned_local_iso"]),
            planned_at_utc=_parse_datetime(row["planned_publish_at_utc"]),
            jitter_minutes=row["jitter_minutes"],
            status=row["status"],
            actual_publish_at_utc=(
                _parse_datetime(row["actual_publish_at_utc"])
                if row["actual_publish_at_utc"]
                else None
            ),
        )

    def list_schedule_slots(self, limit: int = 6) -> List[Dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT slot_key, local_date, slot_name, planned_local_iso, planned_publish_at_utc,
                       jitter_minutes, status, actual_publish_at_utc
                FROM scheduled_slots
                ORDER BY planned_publish_at_utc ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "slot_key": row["slot_key"],
                "local_date": row["local_date"],
                "slot_name": row["slot_name"],
                "planned_local": row["planned_local_iso"],
                "planned_publish_at_utc": row["planned_publish_at_utc"],
                "jitter_minutes": row["jitter_minutes"],
                "status": row["status"],
                "actual_publish_at_utc": row["actual_publish_at_utc"],
            }
            for row in rows
        ]

    def mark_repo_selected(self, repo_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE repositories_seen SET last_selected_at = ? WHERE repo_id = ?",
                (utcnow().isoformat(), repo_id),
            )
            connection.commit()

    def has_duplicate_post(self, text: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM (
                    SELECT normalized_text_hash AS text_hash FROM generated_posts
                    UNION ALL
                    SELECT normalized_text_hash AS text_hash FROM generated_thread_items
                )
                WHERE text_hash = ?
                LIMIT 1
                """,
                (post_hash(text),),
            ).fetchone()
            return row is not None

    def fetch_recent_post_history(self, limit: int = 50) -> List[PostedRepositorySnapshot]:
        snapshots = self.fetch_recent_thread_history(limit=limit)
        if len(snapshots) >= limit:
            return snapshots[:limit]
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT rs.repo_id, rs.full_name, rs.name, rs.owner, rs.topics_json, rs.similarity_key,
                       rs.homepage, COALESCE(pa.published_at, gp.created_at) AS posted_at, gp.post_text
                FROM generated_posts gp
                JOIN repositories_seen rs ON rs.repo_id = gp.repo_id
                JOIN publish_attempts pa ON pa.generated_post_id = gp.id
                WHERE gp.validation_status = 'passed'
                  AND pa.status = 'published'
                ORDER BY posted_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        for row in rows:
            snapshots.append(
                PostedRepositorySnapshot(
                    repo_id=row["repo_id"],
                    full_name=row["full_name"],
                    repo_name=row["name"],
                    owner=row["owner"],
                    topics=tuple(json.loads(row["topics_json"])),
                    similarity_key=row["similarity_key"],
                    homepage=row["homepage"],
                    posted_at=_parse_datetime(row["posted_at"]),
                    post_text=row["post_text"],
                    thread_text=row["post_text"],
                )
            )
        snapshots.sort(key=lambda item: item.posted_at, reverse=True)
        return snapshots[:limit]

    def fetch_recent_thread_history(self, limit: int = 50) -> List[PostedRepositorySnapshot]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT rs.repo_id, rs.full_name, rs.name, rs.owner, rs.topics_json, rs.similarity_key,
                       rs.homepage, MAX(tpa.published_at) AS posted_at,
                       gt.flattened_text, gt.series_slug, gt.pillar_slug
                FROM generated_threads gt
                JOIN repositories_seen rs ON rs.repo_id = gt.repo_id
                JOIN thread_publish_attempts tpa
                    ON tpa.generated_thread_id = gt.id
                   AND tpa.status = 'published'
                WHERE gt.validation_status = 'passed'
                GROUP BY gt.id
                ORDER BY posted_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        snapshots = []
        for row in rows:
            snapshots.append(
                PostedRepositorySnapshot(
                    repo_id=row["repo_id"],
                    full_name=row["full_name"],
                    repo_name=row["name"],
                    owner=row["owner"],
                    topics=tuple(json.loads(row["topics_json"])),
                    similarity_key=row["similarity_key"],
                    homepage=row["homepage"],
                    posted_at=_parse_datetime(row["posted_at"]),
                    post_text=row["flattened_text"],
                    series_slug=row["series_slug"],
                    thread_text=row["flattened_text"],
                    pillar_slug=row["pillar_slug"],
                )
            )
        return snapshots

    def list_recent_posts(self, limit: int = 10) -> List[Dict]:
        return self.list_recent_threads(limit=limit)

    def list_recent_threads(self, limit: int = 10) -> List[Dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT repo_full_name, mode, model, validation_status, validation_reasons_json,
                       created_at, series_slug, pillar_slug,
                       (
                           SELECT COUNT(*)
                           FROM generated_thread_items gti
                           WHERE gti.generated_thread_id = gt.id
                       ) AS post_count
                FROM generated_threads gt
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "repo_full_name": row["repo_full_name"],
                "mode": row["mode"],
                "model": row["model"],
                "validation_status": row["validation_status"],
                "validation_reasons": json.loads(row["validation_reasons_json"]),
                "created_at": row["created_at"],
                "series_slug": row["series_slug"],
                "pillar_slug": row["pillar_slug"],
                "post_count": row["post_count"],
            }
            for row in rows
        ]

    def get_last_discovery_run(self) -> Dict:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'last_discovery_run'"
            ).fetchone()
        if row is None:
            return {}
        return json.loads(row["value"])

    def record_generated_post(self, generated_post: GeneratedPost, selected_rank: int) -> int:
        self.upsert_repository(generated_post.repo)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO generated_posts (
                    repo_id, repo_full_name, language, mode, model, prompt_version,
                    post_text, normalized_text_hash, generator_response_json,
                    validation_status, validation_reasons_json,
                    dry_run, created_at, selected_rank
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '[]', ?, ?, ?)
                """,
                (
                    generated_post.repo.repo_id,
                    generated_post.repo.full_name,
                    generated_post.language,
                    generated_post.mode,
                    generated_post.model,
                    generated_post.prompt_version,
                    generated_post.text,
                    post_hash(generated_post.text),
                    json.dumps(generated_post.raw_response or {}),
                    int(generated_post.mode != "post_now"),
                    utcnow().isoformat(),
                    selected_rank,
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def record_generation_failure(
        self,
        candidate: RepositoryCandidate,
        mode: str,
        selected_rank: int,
        reason: str,
    ) -> int:
        failure_post = GeneratedPost(
            repo=candidate,
            text="",
            language="n/a",
            model="generation_failed",
            raw_response={},
            prompt_version="n/a",
            mode=mode,
        )
        row_id = self.record_generated_post(failure_post, selected_rank)
        self.update_generated_post_validation(row_id, "generation_failed", [reason])
        return row_id

    def update_generated_post_validation(self, generated_post_id: int, status: str, reasons: Sequence[str]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE generated_posts
                SET validation_status = ?, validation_reasons_json = ?
                WHERE id = ?
                """,
                (status, json.dumps(list(reasons)), generated_post_id),
            )
            connection.commit()

    def record_publish_attempt(
        self,
        generated_post_id: int,
        repo_id: int,
        mode: str,
        status: str,
        result: PublishResult,
        retry_count: int = 0,
    ) -> None:
        now = utcnow().isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO publish_attempts (
                    generated_post_id, repo_id, mode, status, retry_count,
                    threads_container_id, threads_media_id, http_status,
                    response_body, error_normalized, created_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generated_post_id,
                    repo_id,
                    mode,
                    status,
                    retry_count,
                    result.container_id,
                    result.media_id,
                    result.status_code,
                    json.dumps(result.response or {}),
                    result.error,
                    now,
                    now if result.success else None,
                ),
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        return connection

    def _set_metadata(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            """
            INSERT INTO metadata (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, utcnow().isoformat()),
        )


def _parse_datetime(value: str):
    from threads_github_bot.models import parse_datetime

    return parse_datetime(value)


def _candidate_to_json(candidate: RepositoryCandidate) -> Dict:
    return {
        "repo_id": candidate.repo_id,
        "full_name": candidate.full_name,
        "name": candidate.name,
        "owner": candidate.owner,
        "description": candidate.description,
        "html_url": candidate.html_url,
        "homepage": candidate.homepage,
        "topics": list(candidate.topics),
        "matched_topics": list(candidate.matched_topics),
        "language": candidate.language,
        "stargazers_count": candidate.stargazers_count,
        "forks_count": candidate.forks_count,
        "archived": candidate.archived,
        "fork": candidate.fork,
        "pushed_at": candidate.pushed_at.isoformat(),
        "updated_at": candidate.updated_at.isoformat(),
        "score": candidate.score,
        "discoverability_score": candidate.discoverability_score,
        "trend_score": candidate.trend_score,
        "novelty_score": candidate.novelty_score,
        "content_fit_score": candidate.content_fit_score,
        "score_breakdown": candidate.score_breakdown,
        "metrics": candidate.metrics,
    }


def _candidate_from_json(payload: Dict) -> RepositoryCandidate:
    return RepositoryCandidate(
        repo_id=payload["repo_id"],
        full_name=payload["full_name"],
        name=payload["name"],
        owner=payload["owner"],
        description=payload["description"],
        html_url=payload["html_url"],
        homepage=payload["homepage"],
        topics=tuple(payload["topics"]),
        matched_topics=tuple(payload["matched_topics"]),
        language=payload.get("language"),
        stargazers_count=payload["stargazers_count"],
        forks_count=payload["forks_count"],
        archived=bool(payload["archived"]),
        fork=bool(payload["fork"]),
        pushed_at=_parse_datetime(payload["pushed_at"]),
        updated_at=_parse_datetime(payload["updated_at"]),
        score=payload.get("score", 0.0),
        discoverability_score=payload.get("discoverability_score", 0.0),
        trend_score=payload.get("trend_score", 0.0),
        novelty_score=payload.get("novelty_score", 0.0),
        content_fit_score=payload.get("content_fit_score", 0.0),
        score_breakdown=payload.get("score_breakdown") or {},
        metrics=payload.get("metrics") or {},
    )


def _select_reserve_candidates(
    candidates: Sequence[RepositoryCandidate],
    max_items: int,
) -> List[RepositoryCandidate]:
    if max_items <= 0:
        return []

    ranked = sorted(
        candidates,
        key=lambda candidate: (
            candidate.score,
            candidate.content_fit_score,
            candidate.trend_score,
            candidate.novelty_score,
            candidate.discoverability_score,
            candidate.pushed_at,
        ),
        reverse=True,
    )
    selected: List[RepositoryCandidate] = []
    seen_topics = set()

    for candidate in ranked:
        topic = candidate.primary_topic()
        if topic and topic not in seen_topics:
            selected.append(candidate)
            seen_topics.add(topic)
        if len(selected) >= max_items:
            return selected

    selected_ids = {candidate.repo_id for candidate in selected}
    for candidate in ranked:
        if candidate.repo_id in selected_ids:
            continue
        selected.append(candidate)
        if len(selected) >= max_items:
            break

    return selected


def _group_item_reasons(reasons: Sequence[str]) -> Dict[int, List[str]]:
    grouped: Dict[int, List[str]] = {}
    for reason in reasons:
        if ":" not in reason:
            continue
        prefix, item_reason = reason.split(":", 1)
        if not prefix.isdigit():
            continue
        grouped.setdefault(int(prefix), []).append(item_reason)
    return grouped
