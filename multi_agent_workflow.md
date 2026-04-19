# Multi-Agent Workflow Log

## Orchestration Context
Date: April 19 2026
Goal: Rebuild Project 2 crawler/search system with filesystem-sharded JSONL storage, concurrent search-while-indexing, watermark back-pressure, and resumability.

## 1) System Architect Phase
Design decisions proposed:
- Use async frontier processing with bounded `asyncio.Queue`.
- Set back-pressure watermarks to 80% high and 20% low.
- Keep append-only letter-sharded files (`a.data`...`z.data`, `num.data`, `other.data`).
- Use per-shard writer lock strategy and read-only search file handles.

Architect prompt (used by orchestrator):
- "Design a single-machine crawler/indexer/search architecture that supports search-while-indexing, bounded back-pressure (80/20 queue watermarks), filesystem-sharded JSONL writes, and resume-after-interruption without sqlite."

## 2) Senior Developer Phase
Implemented modules:
- `crawler_job.py`
- `search_module.py`

Implementation details:
- Replaced thread-queue crawl loop with async frontier flow running inside crawler thread.
- Added watermark gate with explicit pause/resume logs.
- Implemented per-shard append locking in `IndexShardStore`.
- Updated search to use read-only shard scanning and tolerant JSONL parsing.
- Added periodic checkpoints with `visited_urls` and `frontier` snapshot.
- Added resume-on-restart path for matching unfinished origin checkpoint.

Developer prompt (used by orchestrator):
- "Implement the approved architecture with native Python only (asyncio, urllib, html.parser, json, pathlib/os). Preserve existing Flask UI endpoints and make minimal-risk code changes."

## 3) QA & Security Review Phase
Checks performed:
- Static problems scan (`get_errors`) after refactor: no errors.
- Race-risk review:
- Writer critical sections scoped per shard lock.
- Search readers do not mutate shared state.
- Async locks protect visited/pending/rate/state internals.
- Crawl trap guards include URL length and query parameter bounds.

Identified conflict and resolution:
- Conflict: previous search module depended on a global crawler lock export.
- Resolution: removed import coupling and moved to lock-free read-only scanning with JSON decode tolerance.

Residual risk:
- Very high-frequency concurrent appends can increase transient partial-line parse skips; behavior is safe but may slightly delay visibility of newest token rows until next query.

QA/Security prompt (used by orchestrator):
- "Review for race conditions in concurrent crawl/search paths, checkpoint integrity, crawl traps, and read-during-append behavior. Provide blocking issues first, then residual risks."

## 4) Documentation Specialist Phase
Produced/updated:
- `product_prd.md`
- `multi_agent_workflow.md`
- `readme.md`
- `recommendation.md`
- Optional prompts under `agents/`

Documentation prompt (used by orchestrator):
- "Write implementation-aligned docs that explicitly cover requirements traceability: index depth behavior, search tuple output, back-pressure policy, resumability approach, and multi-agent process evidence."

## 5) Agent Interaction Protocol
- Orchestrator created a strict handoff order: Architect -> Developer -> QA/Security -> Documentation.
- Each downstream agent consumed the previous agent output as immutable input unless a conflict was raised.
- Conflict handling rule: if QA flagged a race-risk, changes were routed back to Developer with minimal-diff remediation, then rechecked.
- Acceptance gate: no static errors, no sqlite dependency, required artifacts present, and runtime UI flow verified from `/crawler` -> `/status` -> `/search`.

## Final Decision Record
- Storage architecture is fully filesystem-based and sqlite-free.
- Concurrency supports real-time search during indexing.
- Back-pressure and resumability implemented per updated constraints.
