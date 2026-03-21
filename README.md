# Concurrent Crawler + Real-Time Search (Python)

A lightweight, thread-safe web crawler and search engine built with Python, native libraries for crawling/indexing, and a minimal Flask UI.

## What This Project Includes
- Recursive crawler jobs with configurable depth, hit rate, and queue capacity.
- Thread-safe visited URL tracking persisted to `visited_urls.data`.
- File-backed inverted index sharded by first-letter buckets in `storage/[letter].data`.
- Concurrent-safe search module with pagination and frequency-based ranking.
- Web UI endpoints:
  - `/crawler` to start jobs and view crawl history.
  - `/status` for long-polling status/log streaming.
  - `/search` for query and paginated results.

## Project Structure
- `crawler_job.py`: crawler job lifecycle, extraction, indexing, logging snapshots.
- `search_module.py`: query parsing, shard reads, ranking, pagination.
- `web_ui.py`: Flask backend + HTML/JS pages for crawler/status/search.
- `product_prd.md`: system architecture and requirements.
- `.cursorrules`: implementation constraints for thread safety and native-library-first behavior.

## Prerequisites
- Python 3.11+
- Flask installed in the active environment

If needed, install Flask:
```bash
/opt/homebrew/bin/python3.11 -m pip install flask
```

## Start the Application
From the project root:
```bash
/opt/homebrew/bin/python3.11 web_ui.py
```

Then open:
- `http://127.0.0.1:8000/crawler`
- `http://127.0.0.1:8000/status`
- `http://127.0.0.1:8000/search`

## Initiate Indexing (Crawler Job)
1. Open `/crawler`.
2. Enter:
   - Origin URL (seed URL)
   - Depth (recursive crawl limit)
   - Hit Rate (requests/sec)
   - Queue Capacity (frontier queue max size)
3. Click **Start Crawler**.
4. You will get a crawler ID in format `[EpochTimeCreated_ThreadID]` and can open `/status?crawler_id=...`.

## Monitor Real-Time Status
Use `/status` and provide/select a crawler ID.

The page uses **long polling** (`/status/poll`) to continuously fetch updates from the crawler state file `crawler_states/[crawlerId].data`.

Displayed metrics:
- pages processed
- pages indexed
- pages failed
- visited count
- queue depth
- back-pressure status (`normal` or `active`)
- live log stream

Job state transitions are shown clearly as `running`, `finished`, or `interrupted`.

## Perform Searches
1. Open `/search`.
2. Submit a query string.
3. The backend reads only relevant index shards based on query terms.
4. Results are ranked by aggregated frequency (max hits first).
5. Output rows are triples: `(relevant_url, origin_url, depth)`.
6. Use pagination controls (`Prev`/`Next`) to navigate pages.

## Concurrency Model
- Crawler runs in a dedicated thread per job.
- Shared mutable state is protected with synchronization primitives (`threading.Lock`).
- Search can run while indexing is active by using the same index-file lock for reads/writes, reducing read/write race risks.
- State snapshots are atomically written to avoid partial status-file corruption.

## Back-Pressure Behavior
To avoid overload and unbounded memory growth:
- Frontier queue is bounded (`queue_capacity`).
- Producer-side enqueue throttles when queue occupancy is high.
- Additional adaptive delays are applied when queue depth crosses thresholds.
- Back-pressure events are logged and surfaced in the `/status` page.

## Data Outputs
- `visited_urls.data`: persistent visited URL list.
- `storage/*.data`: sharded inverted index JSONL records.
- `crawler_states/[crawlerId].data`: continuous crawler status snapshot.

## Notes
- This is a local, lightweight system for educational and prototyping use.
- For production-scale evolution, see `recommendation.md`.
