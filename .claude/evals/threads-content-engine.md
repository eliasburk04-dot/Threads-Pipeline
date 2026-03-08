## EVAL: threads-content-engine

### Capability Evals
- [ ] Generate a branded 4-6 post thread with structured JSON output and per-post validation.
- [ ] Rank candidates with distinct discoverability, trend, novelty, and total score breakdowns.
- [ ] Rotate series labels without repeating the same series consecutively unless configured.
- [ ] Plan deterministic jittered morning/evening slots within configured +/- minutes.
- [ ] Gate scheduled execution so each slot publishes at most once per day.
- [ ] Publish a multi-post thread through the Threads client using chained replies.
- [ ] Persist thread records, child posts, queue entries, schedule slots, and publish attempts in SQLite.
- [ ] Expose discovery, preview, queue, ranking explanation, and schedule inspection through CLI commands.

### Regression Evals
- [ ] `test-run` still works without live credentials.
- [ ] `dry-run` still works without publishing.
- [ ] Existing duplicate/cooldown protections still block repeated repos and near-duplicates.
- [ ] API responses and logs do not leak secrets.
- [ ] Pi deployment remains systemd-based and operable from the shell.

### Success Metrics
- Capability evals: pass@1 for the implemented MVP.
- Regression evals: pass^1 for the full local test suite.
