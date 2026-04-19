# GoogleInOneDay-Crawler (Project 2)

Filesystem-sharded crawler and search system with concurrent indexing/search, asyncio watermark back-pressure, and resumable checkpoints.

## Features
- Recursive crawl: `index(origin, k)` behavior.
- Real-time `search(query)` while indexing is active.
- Sharded JSONL inverted index in `storage/*.data`.
- High/low watermark back-pressure (80% / 20%).
- Periodic checkpoint persistence for resume in `crawler_states/*.data`.

## Storage Model
- `a.data` ... `z.data` for alphabetic-leading tokens.
- `num.data` for digit-leading tokens.
- `other.data` for remaining tokens.
- JSONL fields per row: `word`, `origin_url`, `current_url`, `depth`, `frequency`, `ts`.

## Run
```bash
python main.py
```

Open:
- `http://127.0.0.1:8000/crawler`
- `http://127.0.0.1:8000/status`
- `http://127.0.0.1:8000/search`

## Back-Pressure Rules
- Discovery pauses when frontier queue is >= 80% full.
- Discovery resumes when frontier queue is <= 20% full.
- Status logs include back-pressure activation/release entries.

## Search During Indexing
- Search opens shard files read-only.
- Indexer appends rows under per-shard write locks.
- Search ignores partial/corrupt trailing JSONL rows safely.

## Resumability
- State snapshots persist visited URLs and frontier queue.
- If a non-finished checkpoint for the same origin exists, the crawler resumes from it.

## Main Files
- `main.py`: Flask routes and UI integration.
- `crawler_job.py`: Async crawler/indexer with checkpoints and watermarks.
- `search_module.py`: Query parsing, shard scanning, ranking, pagination.
- `product_prd.md`: Requirements and architecture.
- `multi_agent_workflow.md`: Team workflow and decision log.
- `recommendation.md`: Production scaling recommendations.
