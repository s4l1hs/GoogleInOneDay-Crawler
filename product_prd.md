# Product Requirements Document (Project 2)

## 1. Objective
Build a native Python Web Crawler + Search System that supports concurrent indexing and querying on a single machine, without sqlite3 or external crawler frameworks.

Core APIs:
- `index(origin, k)`: recursively crawl from `origin` up to depth `k`.
- `search(query)`: return `(relevant_url, origin_url, depth)` triples while indexing is still running.

## 2. Constraints
- Native-first implementation.
- Storage is filesystem-backed, letter-sharded `.data` files in JSONL format.
- No sqlite3.
- Use Python standard tooling for network and parsing (`urllib`, `html.parser`) plus `asyncio` for queue/workflow orchestration.

## 3. Functional Requirements
1. Recursive indexing by depth:
- Start from origin.
- Discover links recursively until depth `k`.
- Normalize URLs and filter invalid schemes.

2. Real-time concurrent search:
- Search requests must execute during active indexing.
- Search scans relevant shard files with read-only pointers.
- Partial trailing JSON lines from concurrent appends must be tolerated.

3. Sharded inverted index:
- Token bucket strategy:
- `a.data` ... `z.data` for alpha-leading tokens.
- `num.data` for digit-leading tokens.
- `other.data` for other token prefixes.
- JSONL append records include at least: `word`, `origin_url`, `current_url`, `depth`, `frequency`, `ts`.

4. Back-pressure with watermarks:
- Use bounded `asyncio.Queue` for frontier.
- Pause discovery when queue occupancy reaches 80%.
- Resume discovery when occupancy drops below 20%.
- Emit status logs for active/released back-pressure.

5. Resumability:
- Periodically persist full crawl checkpoint to state file:
- Visited URL set.
- Frontier queue snapshot.
- Progress counters and logs.
- On restart of a non-finished job state, resume from checkpoint.

6. Monitoring/UI integration:
- State includes queue size, visited count, pages processed/indexed/failed, and watermark state.
- Existing web status page polls state snapshots and renders progress.

## 4. Non-Functional Requirements
1. Thread safety and race control:
- Writer-side shard append protected with per-shard locks.
- Search uses read-only file descriptors, minimizing lock contention.
- State writes use temp file + atomic `os.replace`.

2. Single-machine scalability:
- Keep memory bounded by queue capacity.
- Shard writes are append-only and sequential.
- Search scans only needed shard files per query term.

3. Failure tolerance:
- Network errors and decode errors must not stop the crawl.
- Corrupt/partial JSONL lines are skipped in search path.

## 5. Data and Directory Layout
- `storage/*.data`: append-only shard files (`a.data`, `b.data`, `num.data`, etc.).
- `crawler_states/[crawler_id].data`: checkpoint + runtime state for each crawler.
- `visited_urls.data`: periodically refreshed visited URL snapshot.

## 6. API/Behavior Summary
- Index API: start crawler thread; async pipeline handles crawl/index/checkpoint.
- Search API: parse query terms, map to shards, aggregate frequency, return ranked triples.
- Status API: long-poll state snapshots and logs.

## 7. Acceptance Criteria
- Crawler can run and index pages recursively to target depth.
- Search returns valid triples while crawler is still appending data.
- Queue back-pressure shows active at >=80% and released at <=20%.
- Interrupted crawl can resume from saved frontier + visited state.
- No sqlite3 usage exists in code path.
