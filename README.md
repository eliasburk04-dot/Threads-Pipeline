# Threads GitHub Bot

Production-focused Python automation for Raspberry Pi that discovers high-discoverability GitHub repositories, builds branded 4-6 post Threads threads with the OpenAI API, validates them, queues them in SQLite, and publishes through the official Threads API on a jittered schedule.

## Architecture overview

The code stays small and shell-friendly:

- `threads_github_bot/config.py`: environment parsing and runtime defaults
- `threads_github_bot/models.py`: normalized domain models for repos, threads, series, and schedule slots
- `threads_github_bot/github_client.py`: GitHub repository discovery
- `threads_github_bot/scoring.py`: discoverability-first, trend-aware, novelty-aware ranking
- `threads_github_bot/deduplication.py`: repo/topic/topic-family cooldown logic
- `threads_github_bot/series.py`: branded series rotation and numbering
- `threads_github_bot/generation.py`: OpenAI Responses generation for structured multi-post thread JSON
- `threads_github_bot/validation.py`: per-post and whole-thread validation
- `threads_github_bot/threads_client.py`: official Threads publishing flow for connected threads
- `threads_github_bot/scheduler.py`: deterministic jittered slot planning
- `threads_github_bot/state.py`: SQLite migrations, queue state, thread records, publish attempts, and schedule slots
- `threads_github_bot/pipeline.py`: discovery, queue refresh, selection, generation, validation, and publish orchestration
- `threads_github_bot/cli.py`: operator commands for discovery, preview, queue inspection, ranking explanation, schedule inspection, dry-runs, and publishing
- `deployment/systemd/`: production systemd units
- `scripts/`: install, update, and remote deploy helpers

## What changed in this version

- Single-post output was replaced with multi-post threads by default.
- Discovery now prioritizes repos that feel discoverable early, not just broadly popular.
- Branded series formats rotate over time and are stored with each generated thread.
- Exact `08:30` and `19:30` timers were replaced with deterministic jitter around configurable base times.
- Discovery now produces a SQLite-backed shortlist queue before selection and posting.
- The scheduler persists planned slot time, actual run time, and slot status so duplicate slot runs are blocked.

## Pipeline flow

1. Fetch candidate repositories from the GitHub Search API for the configured topics.
2. Filter out forks, archived repos, weak descriptions, stale repos, blacklisted repos, and low-star repos.
3. Score candidates using separate discoverability, trend, novelty, and content-fit signals.
4. Refresh the SQLite-backed shortlist queue with the best candidates.
5. Select the next candidate from the queue while enforcing repo, similarity, topic, topic-family, and series cooldowns.
6. Choose a branded series format for the candidate.
7. Generate a structured 4-6 post thread as JSON.
8. Validate each post and the thread as a whole.
9. Publish the connected thread through the Threads API or record a dry-run/test-run instead.
10. Persist thread metadata, child posts, queue state, publish attempts, repository snapshots, and schedule slot state.

## Discoverability-first selection logic

The bot does not rank purely by stars.

The current ranking combines:

- `discoverability`: star sweet-spot preference, docs/homepage/demo signal, description specificity, topical relevance, explainability, and anti-enterprise penalty
- `trend`: recent activity plus star velocity when historical snapshots exist
- `novelty`: penalties for recent similar repos, overlapping topics, and near-duplicate themes
- `content_fit`: how easy the repo is to explain cleanly in a short branded thread

Default discoverability bias:

- the sweet spot is roughly `500` to `10000` stars
- stronger outliers can still rank, but they are penalized when they start looking overexposed
- recent AI, agents, LLM, RAG, devtools, productivity, SaaS, and dev-infra repos are favored when the description is concrete and easy to explain

The queue and ranking explanation commands expose the full score breakdown so selection is inspectable from the shell.

## Branded series format system

Built-in default series:

- `Hidden GitHub Gem`
- `AI Repo Worth Trying`
- `Open Source Tool of the Day`
- `Dev Tool I Didn't Know Existed`
- `Trending AI Repo`

Series selection rotates intelligently:

- compatible formats are chosen from repo attributes and trend context
- consecutive repetition is blocked by default
- numbering can be enabled, for example `Hidden GitHub Gem #12`
- the chosen series is stored with the generated thread and publish records

## Multi-post thread behavior

The generator now produces structured thread JSON instead of one blob of text.

Default structure:

- post 1: hook / why the repo matters now
- post 2: what the project is
- post 3: standout points
- post 4: why builders should care
- post 5: repo URL / explore prompt

Each post is validated individually for:

- byte-length limits
- spammy phrasing
- unsupported claims
- duplicate text

The thread is also validated as a whole for:

- minimum and maximum post count
- repetition across child posts
- missing repo URL in the final post

## Thread publishing details

The publish layer creates a connected thread as:

1. create the first post container
2. publish it
3. create each later child post with `reply_to_id` pointing to the previously published post
4. publish each child post in order

Per-post container IDs, media IDs, responses, and failures are persisted in SQLite. Partial failures are preserved for inspection instead of being collapsed into one opaque error.

## Why exact 08:30 and 19:30 are not hardcoded anymore

Threads timing guidance is not stable enough to justify exact fixed minutes forever. Recent third-party analysis from Buffer shows stronger performance clusters around weekday morning windows, especially around `7-9 a.m.`, with some strong late-morning/noon windows as well. This bot therefore uses configurable base times and deterministic jitter instead of exact static timestamps. The default base times remain `08:30` and `19:30` to preserve the existing operational intent, but they are now treated as centers of publish windows, not literal fixed minutes.

## Scheduling and jitter behavior

Scheduling now uses a lightweight checker model:

- systemd wakes the scheduler service every 5 minutes
- the app computes deterministic morning and evening slot times in `Europe/Berlin`
- each slot is offset by a deterministic jitter in the range `-10` to `+10` minutes by default
- each slot key is stored in SQLite, so each morning slot and evening slot can run at most once per day
- operators can inspect the exact planned times with CLI commands before the slot fires

Why this design was chosen:

- it is simpler and safer on Raspberry Pi than regenerating timer units for every randomized occurrence
- it keeps systemd as the process supervisor
- deterministic jitter is easy to debug because the same date, slot, and seed always produce the same offset
- future adaptive timing can be layered on top of the same planner without rewriting deployment

Adaptive timing status:

- `SCHEDULE_ENABLE_ADAPTIVE=false` by default
- the code and schema are ready for future timing optimization once real engagement data is available
- current production behavior is deterministic jitter around configured base times

## Requirements

- Python `3.11+`
- Raspberry Pi OS or another Debian-based Linux
- outbound HTTPS access to GitHub, OpenAI, and Threads
- OpenAI API key
- Threads user access token and Threads user ID
- optional GitHub token for better rate limits

## Required secrets and how to obtain/set them

### OpenAI

- `OPENAI_API_KEY`: required for live thread generation
- `OPENAI_MODEL`: Responses-compatible model, default `gpt-4.1-mini`
- `OPENAI_VALIDATION_MODEL`: optional validation model, default matches the primary model

Set these only in `/etc/threads-github-bot/threads-github-bot.env`.

### Threads

Publishing requires a Meta app configured for Threads publishing with the relevant permissions, including `threads_basic` and `threads_content_publish`.

Set:

- `THREADS_ACCESS_TOKEN`
- `THREADS_USER_ID`

Keep them only in `/etc/threads-github-bot/threads-github-bot.env`.

### GitHub

- `GITHUB_TOKEN` is optional
- unauthenticated discovery works for public repos but rate limits are lower

## Configuration

Copy `.env.example` and fill in the required values. Every runtime variable is documented there.

The most important tuning areas are:

- discovery: `GITHUB_TOPICS`, `GITHUB_MIN_STARS`, `GITHUB_QUEUE_SIZE`, `GITHUB_DISCOVERABILITY_STAR_FLOOR`, `GITHUB_DISCOVERABILITY_STAR_CEILING`
- cooldowns: `COOLDOWN_REPO_DAYS`, `COOLDOWN_SIMILARITY_DAYS`, `COOLDOWN_TOPIC_DAYS`, `COOLDOWN_SERIES_DAYS`, `COOLDOWN_TOPIC_FAMILIES`
- content: `THREAD_POST_COUNT_MIN`, `THREAD_POST_COUNT_MAX`, `THREAD_STRUCTURE`, `CONTENT_LANGUAGE`
- series: `SERIES_TEMPLATES`, `SERIES_ENABLE_NUMBERING`, `SERIES_ALLOW_CONSECUTIVE`
- timing: `SCHEDULE_MORNING_TIME`, `SCHEDULE_EVENING_TIME`, `SCHEDULE_JITTER_MINUTES`, `SCHEDULE_ALLOWED_WEEKDAYS`, `SCHEDULE_ENABLE_ADAPTIVE`

## Local usage

Create a venv and install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

Refresh discovery and build the queue:

```bash
.venv/bin/python -m threads_github_bot discovery-only
```

Preview the next branded thread:

```bash
.venv/bin/python -m threads_github_bot preview-thread
```

Explain the current ranking:

```bash
.venv/bin/python -m threads_github_bot explain-ranking --limit 5
```

Inspect queue and recent threads:

```bash
.venv/bin/python -m threads_github_bot list-queue --limit 10
.venv/bin/python -m threads_github_bot list-recent --limit 10
```

Inspect schedule planning:

```bash
.venv/bin/python -m threads_github_bot show-schedule --count 6
.venv/bin/python -m threads_github_bot plan-next-run
```

Run safe execution modes:

```bash
.venv/bin/python -m threads_github_bot test-run
.venv/bin/python -m threads_github_bot dry-run
```

Run live publishing immediately:

```bash
.venv/bin/python -m threads_github_bot post-now
```

Run the same immediate auto-mode used for manual systemd starts:

```bash
.venv/bin/python -m threads_github_bot scheduled-run
```

## How scheduling works

Production scheduling now uses:

- `deployment/systemd/threads-github-bot.timer`: runs every 5 minutes
- `deployment/systemd/threads-github-bot.service`: executes `scheduled-check`
- `deployment/systemd/threads-github-bot-run-now.service`: manual immediate systemd-triggered run

The checker service:

- computes today’s morning and evening jittered targets
- records them in SQLite
- checks whether the current run falls inside the active slot grace window
- blocks duplicate execution for the same slot key
- chooses `test-run`, `dry-run`, or `post-now` based on available credentials

## Dry-run and test mode

- `test-run`: safe fallback; uses a built-in fixture thread if OpenAI is not configured
- `dry-run`: performs live GitHub discovery and live generation but does not publish to Threads
- `post-now`: full live publish flow
- `scheduled-run`: immediate auto-mode based on current credentials
- `scheduled-check`: the production checker used by systemd

## Logs and DB inspection

Structured logs:

- production: `/var/log/threads-github-bot/threads-github-bot.log`
- local default: `var/logs/threads-github-bot.log`

Useful commands:

```bash
journalctl -u threads-github-bot.service -n 100 --no-pager
journalctl -u threads-github-bot-run-now.service -n 100 --no-pager
tail -n 100 /var/log/threads-github-bot/threads-github-bot.log
sqlite3 /var/lib/threads-github-bot/threads_github_bot.sqlite3 '.tables'
sqlite3 /var/lib/threads-github-bot/threads_github_bot.sqlite3 'select repo_full_name, series_slug, validation_status, created_at from generated_threads order by id desc limit 10;'
sqlite3 /var/lib/threads-github-bot/threads_github_bot.sqlite3 'select slot_key, status, planned_publish_at_utc, actual_publish_at_utc from scheduled_slots order by planned_publish_at_utc desc limit 10;'
cat /var/lib/threads-github-bot/status.json
```

## Raspberry Pi setup

Install locations:

- app code: `/opt/threads-github-bot/app`
- venv: `/opt/threads-github-bot/venv`
- config: `/etc/threads-github-bot/threads-github-bot.env`
- database and schedule state: `/var/lib/threads-github-bot`
- logs: `/var/log/threads-github-bot`
- systemd units: `/etc/systemd/system/threads-github-bot.service`, `/etc/systemd/system/threads-github-bot.timer`, `/etc/systemd/system/threads-github-bot-run-now.service`

The installer:

- ensures Python `3.11+`
- creates the `threadsbot` service user
- copies code into `/opt/threads-github-bot/app`
- creates the venv and installs runtime dependencies
- installs all systemd units from `deployment/systemd/`
- creates the env file from `.env.example` if it does not exist
- enables the primary timer

Run on the Pi:

```bash
sudo bash scripts/install_pi.sh
```

## Operational and security notes

- Secrets are never hardcoded and should live only in `/etc/threads-github-bot/threads-github-bot.env`.
- Recommended permissions:
  - `/etc/threads-github-bot`: `750`
  - `/etc/threads-github-bot/threads-github-bot.env`: `640`
  - `/var/lib/threads-github-bot`: `750`
  - `/var/log/threads-github-bot`: `750`
- The service runs as `threadsbot`.
- Threads access tokens are masked before response bodies are persisted.
- GitHub metadata is treated as untrusted input and normalized before being injected into prompts.
- The prompt contract forbids invented features and the validator rejects unsupported claims.
- Retry behavior for Threads publishing is bounded by `THREADS_RETRY_COUNT` and `THREADS_RETRY_BACKOFF_SECONDS`.
- Schedule slot state is persisted so the same morning/evening slot cannot fire twice accidentally.

## Troubleshooting

- `Missing OPENAI_API_KEY`: the scheduled flow falls back to `test-run`; add the key to enable live generation.
- `Missing THREADS_ACCESS_TOKEN or THREADS_USER_ID`: the scheduled flow falls back to `dry-run`; add both values to enable publishing.
- `no_valid_candidate`: inspect `list-queue`, `explain-ranking`, cooldown settings, and `generated_threads`.
- `publish_failed`: inspect `thread_publish_attempts` and `journalctl`.
- `scheduled-check` never posting: inspect `show-schedule`, `scheduled_slots`, and the system timezone.
- queue looks stale: run `discovery-only` manually and inspect `candidate_queue`.

## Update procedure

On the Pi after syncing updated source:

```bash
cd /tmp/threads-github-bot-src
sudo bash scripts/update_pi.sh
sudo systemctl status threads-github-bot.timer --no-pager
sudo systemctl status threads-github-bot.service --no-pager
```

## How to deploy to milkathedog@100.69.69.19

Initial deploy from your workstation:

```bash
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.ruff_cache/' \
  ./ milkathedog@100.69.69.19:/tmp/threads-github-bot-src/

ssh milkathedog@100.69.69.19 'cd /tmp/threads-github-bot-src && sudo bash scripts/install_pi.sh'
```

Future updates:

```bash
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.ruff_cache/' \
  ./ milkathedog@100.69.69.19:/tmp/threads-github-bot-src/

ssh milkathedog@100.69.69.19 'cd /tmp/threads-github-bot-src && sudo bash scripts/update_pi.sh'
```

One-command wrappers:

```bash
bash scripts/deploy_remote.sh
bash scripts/redeploy_remote.sh
```

Post-deploy checks on the Pi:

```bash
ssh milkathedog@100.69.69.19 'systemctl status threads-github-bot.timer --no-pager'
ssh milkathedog@100.69.69.19 'systemctl list-timers threads-github-bot.timer --all --no-pager'
ssh milkathedog@100.69.69.19 'sudo systemctl start threads-github-bot-run-now.service'
ssh milkathedog@100.69.69.19 'sudo -u threadsbot bash -lc "cd /opt/threads-github-bot/app && /opt/threads-github-bot/venv/bin/python -m threads_github_bot --env-file /etc/threads-github-bot/threads-github-bot.env show-schedule --count 4"'
ssh milkathedog@100.69.69.19 'journalctl -u threads-github-bot.service -n 100 --no-pager'
```

## Source references

- Threads API publish flow: <https://developers.facebook.com/docs/threads/posts/>
- Threads API permissions and setup: <https://developers.facebook.com/docs/threads/get-started/>
- Meta official Threads Postman collection, including chained publishing fields such as `reply_to_id`: <https://www.postman.com/meta/threads-api/documentation/dht3l68/threads-api>
- GitHub Search REST API: <https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28>
- OpenAI Responses API: <https://platform.openai.com/docs/api-reference/responses>
- Threads timing research overview: <https://buffer.com/resources/the-best-time-to-post-on-threads/>
