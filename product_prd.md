# Product Requirements Document (PRD)
## Project: Build Google in a Day (Python, Native Libraries Only)

## 1. Document Control
- **Version:** 1.0
- **Date:** 2026-03-21
- **Authoring Role:** System Architect
- **Status:** Draft for implementation kickoff

## 2. Vision and Problem Statement
Build a minimal, production-minded, concurrent web crawler and real-time search engine in one day, using only Python native/standard libraries. The system must discover pages recursively, index content quickly, and return search results in near real time through a long-polling user interface.

This project prioritizes:
- Correctness under concurrency
- Deterministic, thread-safe behavior
- Efficient Unix/macOS file I/O for index-heavy workloads
- Simplicity and inspectability over framework abstraction

## 3. Scope
### In Scope
- Recursive, domain-aware crawler
- Concurrent fetch + parse + index pipeline
- Thread-safe inverted index and metadata store
- File-system-backed persistence optimized for macOS/Unix
- Search API with low-latency query execution
- Long-polling API/UI for near real-time result updates
- Back-pressure controls to prevent overload
- Observability (basic metrics, queue depth, throughput)

### Out of Scope
- Distributed crawling across multiple machines
- JavaScript-rendered page execution
- Advanced ranking (PageRank, ML ranking)
- Full duplicate detection at internet scale
- Full-text snippet semantic summarization

## 4. Non-Negotiable Constraints
1. No heavy scraping libraries (e.g., Scrapy, BeautifulSoup, Selenium).
2. Use standard/native libraries only (e.g., `urllib.request`, `html.parser`, `threading`, `queue`, `concurrent.futures`, `sqlite3`, `http.server`, `json`, `re`, `os`, `pathlib`).
3. System must be thread-safe by design.
4. File operations are expected to be heavy and must be optimized for Unix/macOS.

## 5. Primary Users and Use Cases
### Users
- Developer/operator running local crawler and search service
- End-user querying indexed pages from browser UI

### Core Use Cases
1. Seed URLs are provided, crawler recursively discovers links.
2. Pages are fetched and parsed concurrently.
3. Parsed text is tokenized and indexed.
4. User submits a query; system returns ranked matching URLs.
5. UI issues long-poll requests and updates when index changes.
6. System resists overload via bounded queues and throttling.

## 6. Functional Requirements
### FR-1: Recursive Crawler
- Accept one or more seed URLs.
- Restrict crawl by configurable policy:
  - Same-domain only (default) or allowlist domains.
  - Max depth.
  - Max pages.
- Parse links from HTML (`<a href="...">`) via `html.parser`.
- Normalize URLs:
  - Resolve relative paths.
  - Remove fragments.
  - Canonicalize scheme/host casing.
- Respect `robots.txt` (minimum support via `urllib.robotparser`).
- Track visited URLs thread-safely to avoid duplicate crawl.

### FR-2: Concurrent Fetch/Parse Pipeline
- Use bounded work queues for each stage:
  - Frontier queue (URL discovery)
  - Fetch queue
  - Parse queue
  - Index queue
- Worker threads per stage consume/produce queue items.
- Handle timeouts, transient errors, and retries with jitter.
- Enforce global politeness delay per host.

### FR-3: Thread-Safe Indexing
- Build inverted index: `term -> posting list(doc_id, term_freq, positions optional)`.
- Maintain document store: `doc_id -> {url, title, path, fetched_at, content_hash}`.
- Ensure atomic updates to shared state using locks or single-writer pattern.
- Persist index segments on disk and support safe reads during writes.

### FR-4: Search
- Support basic query syntax:
  - Term query (`python`)
  - Multi-term AND (`python threading`)
- Ranking baseline:
  - TF-IDF-like scoring or normalized term frequency + document length factor
- Return top-K results with URL, title, and short snippet.
- Query endpoint must remain responsive during active crawling.

### FR-5: Long-Polling UI
- Serve minimal HTML/CSS/JS UI from Python HTTP server.
- Client sends long-poll request including last seen index version.
- Server blocks request up to timeout (e.g., 20-30s) until:
  - New indexed documents are available, or
  - Timeout reached
- Return incremental update payload:
  - New version
  - Result deltas or fresh top-K

### FR-6: Back-Pressure and Overload Protection
- Every queue must be bounded (`queue.Queue(maxsize=N)`).
- Producers block or drop low-priority items when queues fill.
- Enforce per-host fetch concurrency limits.
- Apply adaptive throttling:
  - If parse/index queues exceed threshold, slow frontier expansion.
- System must avoid unbounded memory growth.

### FR-7: Persistence and Recovery
- Persist index and metadata in append-friendly files and/or `sqlite3`.
- On restart, load latest consistent snapshot/segment manifest.
- Use write-then-atomic-rename for durable metadata updates.

## 7. Quality Attributes (Non-Functional Requirements)
### NFR-1: Thread Safety
- No data races in visited set, index map, or doc metadata.
- Shared mutable structures guarded by `threading.Lock`/`RLock` or single-writer queues.

### NFR-2: Performance
- Crawl throughput target (local dev): 20-100 pages/minute depending on network.
- Search p95 latency under load: < 200 ms for medium local corpus (~10k docs).

### NFR-3: Reliability
- Worker failures isolated; system keeps running.
- Corrupt partial writes avoided via temp files + `os.replace`.

### NFR-4: File I/O Efficiency (Unix/macOS)
- Prefer sequential appends over random writes.
- Batch flush index updates.
- Use directory sharding to avoid too many files per folder.
- Use `os.scandir` for directory iteration.
- Use buffered I/O and avoid frequent fsync except for checkpoints.

### NFR-5: Observability
- Emit structured logs (JSON lines) per subsystem.
- Expose internal metrics endpoint (queue depth, docs indexed, errors, version).

## 8. Proposed System Architecture
### 8.1 High-Level Components
1. **Frontier Manager**
   - Maintains URL frontier and dedupe checks.
2. **Fetcher Pool**
   - Downloads pages via `urllib.request`.
3. **Parser Pool**
   - Extracts text + links via `html.parser` and regex/tokenizer.
4. **Indexer**
   - Builds in-memory segment and periodically writes on-disk segment files.
5. **Search Service**
   - Loads active segments and answers queries.
6. **Realtime Gateway**
   - Long-poll endpoint keyed by index version.
7. **Storage Layer**
   - Segment files, manifests, doc table (`sqlite3` optional for metadata).

### 8.2 Concurrency Model
- Recommended pattern: **multi-stage producer/consumer** with bounded queues.
- Alternative safe pattern: **single-writer indexer** thread + many parser/fetch workers.
- Synchronization primitives:
  - `threading.Lock` for visited URL set
  - `threading.Condition` for long-poll waiting on index version
  - `threading.Semaphore` per-host concurrency
  - `threading.Event` for graceful shutdown

### 8.3 Data Flow
1. Seed URLs enter frontier queue.
2. Fetch workers retrieve HTML and enqueue parse jobs.
3. Parse workers extract links/text and enqueue index jobs + new URLs.
4. Indexer updates in-memory structures and increments `index_version`.
5. Search endpoint reads stable snapshot references.
6. Long-poll clients awaken when `index_version` advances.

## 9. Data Model
### Core Entities
- **Document**
  - `doc_id: int`
  - `url: str`
  - `title: str`
  - `path: str` (local content file path)
  - `fetched_at: int`
  - `content_hash: str`
  - `length: int`

- **Posting**
  - `doc_id: int`
  - `tf: int`
  - `positions: list[int]` (optional)

- **Term Dictionary**
  - `term -> {df, segment_offsets...}`

- **Index Version**
  - Monotonic integer incremented on each successful segment commit.

## 10. API Requirements
### 10.1 Crawl Control
- `POST /crawl/start`
  - Body: seeds, max_depth, max_pages, allowed_domains
- `POST /crawl/stop`
- `GET /crawl/status`
  - Returns queue depths, active workers, pages fetched/indexed

### 10.2 Search
- `GET /search?q=<query>&k=<topK>&since=<version?>`
  - Returns result list + current `index_version`

### 10.3 Long Poll
- `GET /updates?last_version=<v>&q=<query>&k=<topK>&timeout=<sec>`
  - Blocks until `index_version > v` or timeout
  - Returns `{updated: bool, index_version, results}`

## 11. Long-Polling Behavior Details
- Use shared `threading.Condition` around `index_version`.
- Request flow:
  1. Acquire condition lock.
  2. If version unchanged, wait with timeout.
  3. On notify/timeout, recompute query result and return payload.
- Notify behavior:
  - After index commit, indexer calls `condition.notify_all()`.
- Safety:
  - Always re-check condition in loop to avoid spurious wakeups.

## 12. Back-Pressure Design
### Triggers
- Queue occupancy ratio > 0.8
- Excessive retry/error rates
- Slow disk write latency spikes

### Actions
- Reduce frontier producer rate.
- Temporarily cap new URL expansion per parsed page.
- Lower fetch worker effective concurrency.
- Drop duplicate/low-value URLs early (query params, media files).

### Guarantees
- No unbounded queue growth.
- Throughput degrades gracefully instead of crashing.

## 13. File-System Strategy (Unix/macOS)
### Directory Layout
- `data/raw/aa/bb/<doc_id>.html` (sharded)
- `data/index/segments/<segment_id>.idx`
- `data/index/manifest.json`
- `data/meta/docs.sqlite` or `data/meta/docs.jsonl`
- `data/logs/events.jsonl`

### I/O Optimizations
- Batch writes in memory and flush on thresholds:
  - every N docs or T seconds
- Use append-only segment strategy; avoid in-place rewrite.
- Use `tempfile.NamedTemporaryFile(delete=False)` + `os.replace` for atomic metadata swap.
- Keep file descriptors reused where practical.
- Minimize stat calls by caching known paths and using `os.scandir`.

## 14. Security and Safety
- Validate URL schemes (`http`, `https` only).
- Enforce max response size to avoid memory abuse.
- Timeout all network operations.
- Sanitize UI rendering of snippets to avoid XSS in local UI.

## 15. Testing Strategy
### Unit Tests
- URL normalization and dedupe
- HTML parsing/link extraction
- Tokenizer correctness
- Posting merge logic
- Ranking function determinism

### Concurrency Tests
- High-contention visited set updates
- Index commit/read race checks
- Long-poll notify/wait correctness
- Queue saturation/back-pressure behavior

### Integration Tests
- Crawl from local fixture site, validate indexed docs and links
- End-to-end search + updates flow
- Restart recovery from persisted data

### Performance Tests
- Controlled corpus benchmark (1k, 10k docs)
- Measure queue latency, indexing throughput, search p95

## 16. Day-1 Build Plan ("Google in a Day")
### Phase 1 (Hours 0-2)
- Project skeleton, config, queue topology, logging.
- Basic URL frontier + fetch workers.

### Phase 2 (Hours 2-4)
- HTML parser + recursive link extraction.
- Visited dedupe + depth tracking.

### Phase 3 (Hours 4-6)
- Inverted index in memory.
- Segment persistence and manifest.

### Phase 4 (Hours 6-8)
- Search endpoint + ranking.
- Basic UI with query form and results list.

### Phase 5 (Hours 8-10)
- Long-poll updates and index version condition variable.
- Back-pressure tuning + queue limits.

### Phase 6 (Hours 10-12)
- Hardening: tests, metrics, error handling, restart recovery.

## 17. Acceptance Criteria
1. Given seed URLs, crawler discovers and indexes linked pages recursively within configured limits.
2. Under concurrent load, no data corruption or race-condition crashes occur.
3. Search returns relevant top-K results during active crawling.
4. Long-poll endpoint delivers updates when new content is indexed.
5. Queue saturation triggers back-pressure and keeps memory bounded.
6. Index and metadata survive restart with no manifest corruption.

## 18. Risks and Mitigations
- **Risk:** Lock contention slows indexing.
  - **Mitigation:** Use single-writer index thread and lock-free queues between stages.
- **Risk:** Disk bottlenecks under heavy writes.
  - **Mitigation:** Batch and segment writes; reduce sync frequency.
- **Risk:** Crawl traps/infinite URL spaces.
  - **Mitigation:** URL normalization, max depth/pages, parameter filtering.
- **Risk:** Long-poll thread exhaustion.
  - **Mitigation:** Bound server worker threads; tune timeout and keep-alive settings.

## 19. Implementation Guidance (Library Boundary)
Allowed examples:
- `urllib.request`, `urllib.parse`, `urllib.robotparser`
- `html.parser`, `re`, `json`, `sqlite3`
- `threading`, `queue`, `concurrent.futures`
- `http.server`, `socketserver`
- `pathlib`, `os`, `tempfile`, `time`, `logging`

Disallowed examples:
- Scrapy, BeautifulSoup, Selenium, Playwright, requests-html, pyppeteer

## 20. Definition of Done
- PRD-approved architecture implemented.
- All core endpoints operational.
- Concurrency and integration tests passing.
- Documented run instructions and operational limits.
- Demonstrated live crawl + real-time search update in browser.
