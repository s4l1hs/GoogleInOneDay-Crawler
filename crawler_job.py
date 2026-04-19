"""Async crawler + filesystem-sharded JSONL indexer.

Native-only implementation using asyncio, urllib, html.parser, json and pathlib/os.
The crawler supports:
- Recursive crawl to depth k.
- Search-while-indexing via append-only shard files.
- Back-pressure with 80/20 queue watermarks.
- Periodic resumable snapshots of visited set and frontier queue.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urldefrag, urljoin, urlparse
from urllib.request import Request, urlopen

WORD_RE = re.compile(r"[A-Za-z0-9]+")


@dataclass
class CrawlLimits:
    hit_rate: float = 5.0
    queue_capacity: int = 500
    fetch_timeout: float = 8.0
    max_pages: Optional[int] = None
    checkpoint_interval_sec: float = 2.0
    max_url_length: int = 2048
    max_query_params: int = 25


class _TextAndLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: List[str] = []
        self.text_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)


class IndexShardStore:
    """Append-only JSONL shard storage with per-file locks for writers."""

    def __init__(self, storage_dir: Path) -> None:
        self.storage_dir = storage_dir
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._guard = threading.Lock()
        self._locks: Dict[str, threading.Lock] = {}

    def _get_lock(self, bucket: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(bucket)
            if lock is None:
                lock = threading.Lock()
                self._locks[bucket] = lock
            return lock

    def append_rows(self, bucket: str, rows: Iterable[Dict[str, object]]) -> None:
        path = self.storage_dir / f"{bucket}.data"
        lock = self._get_lock(bucket)
        with lock:
            with path.open("a", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=True) + "\n")
                fh.flush()


class CrawlerJob:
    def __init__(
        self,
        origin: str,
        depth: int,
        crawler_id: str,
        limits: CrawlLimits,
        visited_file: Path,
        storage_dir: Path,
        state_dir: Path,
    ) -> None:
        self.origin = origin.strip()
        self.depth = max(0, depth)
        self.crawler_id = crawler_id
        self.limits = limits
        self.visited_file = visited_file
        self.storage_dir = storage_dir
        self.state_dir = state_dir
        self.state_path = self.state_dir / f"{self.crawler_id}.data"

        self.frontier: asyncio.Queue[Tuple[str, int]] = asyncio.Queue(maxsize=max(10, limits.queue_capacity))
        self.visited: Set[str] = set()
        self.pending: Set[str] = set()
        self.visited_lock = asyncio.Lock()
        self.pending_lock = asyncio.Lock()
        self.state_lock = asyncio.Lock()
        self.rate_lock = asyncio.Lock()

        self.log_buffer: Deque[str] = deque(maxlen=500)
        self.shards = IndexShardStore(storage_dir)

        self.state = "initializing"
        self.pages_processed = 0
        self.pages_indexed = 0
        self.pages_failed = 0
        self.last_fetch_ts = 0.0
        self.discovery_paused = False
        self._resume_loaded = False

        self.high_watermark = max(1, int(self.frontier.maxsize * 0.80))
        self.low_watermark = max(0, int(self.frontier.maxsize * 0.20))

    async def run(self) -> None:
        await self._load_resume_state()
        if self.frontier.qsize() == 0:
            await self._enqueue(self.origin, 0)

        self.state = "running"
        self._append_log(
            f"started crawler origin={self.origin} depth={self.depth} "
            f"queue_capacity={self.frontier.maxsize}"
        )
        await self._write_state_snapshot()

        checkpoint_task = asyncio.create_task(self._periodic_checkpoint())
        try:
            while True:
                if self.limits.max_pages is not None and self.pages_processed >= self.limits.max_pages:
                    self._append_log("max_pages reached, stopping crawl")
                    break

                if self.frontier.empty():
                    async with self.pending_lock:
                        if not self.pending:
                            self._append_log("frontier empty and no pending URLs")
                            break

                try:
                    current_url, current_depth = await asyncio.wait_for(self.frontier.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                async with self.pending_lock:
                    self.pending.discard(current_url)

                if not await self._mark_visited(current_url):
                    self.frontier.task_done()
                    continue

                self.pages_processed += 1
                html = await self._fetch_html(current_url)
                if html is None:
                    self.pages_failed += 1
                    self.frontier.task_done()
                    continue

                words, links = self._extract_words_and_links(current_url, html)
                await self._persist_word_frequencies(
                    origin_url=self.origin,
                    current_url=current_url,
                    depth=current_depth,
                    words=words,
                )
                self.pages_indexed += 1

                if current_depth < self.depth:
                    for next_url in links:
                        await self._enqueue(next_url, current_depth + 1)

                self.frontier.task_done()
        finally:
            checkpoint_task.cancel()
            try:
                await checkpoint_task
            except asyncio.CancelledError:
                pass

        self.state = "finished"
        self._append_log("crawler finished")
        await self._write_state_snapshot(force=True)

    async def _periodic_checkpoint(self) -> None:
        while True:
            await asyncio.sleep(max(0.5, self.limits.checkpoint_interval_sec))
            await self._write_state_snapshot()

    async def _fetch_html(self, url: str) -> Optional[str]:
        await self._respect_hit_rate()
        req = Request(
            url,
            headers={
                "User-Agent": "CrawlerJob/2.0 (+native-python-standard-lib)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )

        def _blocking_fetch() -> Optional[str]:
            try:
                with urlopen(req, timeout=self.limits.fetch_timeout) as response:
                    status = getattr(response, "status", response.getcode())
                    if status != 200:
                        return None
                    payload = response.read()
                    charset = response.headers.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
            except Exception:
                return None

        html = await asyncio.to_thread(_blocking_fetch)
        if html is None:
            self._append_log(f"fetch failed or skipped: {url}")
        return html

    async def _respect_hit_rate(self) -> None:
        if self.limits.hit_rate <= 0:
            return
        min_interval = 1.0 / self.limits.hit_rate
        async with self.rate_lock:
            elapsed = time.time() - self.last_fetch_ts
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self.last_fetch_ts = time.time()

    def _extract_words_and_links(self, base_url: str, html: str) -> Tuple[List[str], List[str]]:
        parser = _TextAndLinkParser()
        parser.feed(html)

        text = " ".join(parser.text_parts)
        words = [w.lower() for w in WORD_RE.findall(text)]

        links: List[str] = []
        for href in parser.links:
            normalized = self._normalize_url(base_url, href)
            if normalized:
                links.append(normalized)

        return words, links

    def _normalize_url(self, base_url: str, href: str) -> Optional[str]:
        candidate = href.strip()
        if not candidate:
            return None

        absolute = urljoin(base_url, candidate)
        absolute, _fragment = urldefrag(absolute)
        parsed = urlparse(absolute)

        if parsed.scheme not in ("http", "https"):
            return None
        if not parsed.netloc:
            return None
        if len(absolute) > self.limits.max_url_length:
            return None
        if len(parse_qsl(parsed.query, keep_blank_values=True)) > self.limits.max_query_params:
            return None

        return parsed.geturl()

    async def _persist_word_frequencies(
        self,
        origin_url: str,
        current_url: str,
        depth: int,
        words: Iterable[str],
    ) -> None:
        freqs: Dict[str, int] = {}
        for word in words:
            if not word:
                continue
            freqs[word] = freqs.get(word, 0) + 1

        if not freqs:
            return

        by_bucket: Dict[str, List[Dict[str, object]]] = {}
        ts = int(time.time())
        for word, frequency in freqs.items():
            bucket = self._bucket_for_word(word)
            by_bucket.setdefault(bucket, []).append(
                {
                    "word": word,
                    "origin_url": origin_url,
                    "current_url": current_url,
                    "depth": depth,
                    "frequency": int(frequency),
                    "ts": ts,
                }
            )

        for bucket, rows in by_bucket.items():
            await asyncio.to_thread(self.shards.append_rows, bucket, rows)

    async def _enqueue(self, url: str, depth: int) -> None:
        if depth > self.depth:
            return
        if not url:
            return

        async with self.visited_lock:
            if url in self.visited:
                return

        async with self.pending_lock:
            if url in self.pending:
                return
            self.pending.add(url)

        await self._apply_watermark_back_pressure()
        await self.frontier.put((url, depth))

    async def _apply_watermark_back_pressure(self) -> None:
        qsize = self.frontier.qsize()
        if qsize >= self.high_watermark and not self.discovery_paused:
            self.discovery_paused = True
            self._append_log(
                f"back-pressure active: queue={qsize}/{self.frontier.maxsize} "
                f"high={self.high_watermark} low={self.low_watermark}"
            )

        while self.discovery_paused:
            if self.frontier.qsize() <= self.low_watermark:
                self.discovery_paused = False
                self._append_log(
                    f"back-pressure released: queue={self.frontier.qsize()}/{self.frontier.maxsize}"
                )
                break
            await asyncio.sleep(0.05)

    async def _mark_visited(self, url: str) -> bool:
        async with self.visited_lock:
            if url in self.visited:
                return False
            self.visited.add(url)
            return True

    def _append_log(self, message: str) -> None:
        self.log_buffer.append(f"{int(time.time())} {message}")

    def _queue_snapshot(self, max_items: int = 250) -> List[Dict[str, object]]:
        items = list(self.frontier._queue)[:max_items]
        return [{"url": item[0], "depth": item[1]} for item in items]

    async def _write_state_snapshot(self, force: bool = False) -> None:
        del force
        async with self.state_lock:
            async with self.visited_lock:
                visited_urls = sorted(self.visited)

            payload = {
                "crawler_id": self.crawler_id,
                "origin": self.origin,
                "state": self.state,
                "depth_limit": self.depth,
                "queue_size": self.frontier.qsize(),
                "queue_capacity": self.frontier.maxsize,
                "watermark": {
                    "high": self.high_watermark,
                    "low": self.low_watermark,
                    "paused": self.discovery_paused,
                },
                "frontier": self._queue_snapshot(),
                "visited_count": len(visited_urls),
                "visited_urls": visited_urls,
                "pages_processed": self.pages_processed,
                "pages_indexed": self.pages_indexed,
                "pages_failed": self.pages_failed,
                "resume_loaded": self._resume_loaded,
                "logs": list(self.log_buffer),
                "updated_at": int(time.time()),
            }

            self.state_dir.mkdir(parents=True, exist_ok=True)
            self.visited_file.parent.mkdir(parents=True, exist_ok=True)

            state_tmp = self.state_path.with_suffix(".tmp")
            with state_tmp.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=True, indent=2)
            os.replace(state_tmp, self.state_path)

            visited_tmp = self.visited_file.with_suffix(".tmp")
            with visited_tmp.open("w", encoding="utf-8") as fh:
                for item in visited_urls:
                    fh.write(item + "\n")
            os.replace(visited_tmp, self.visited_file)

    async def _load_resume_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            with self.state_path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (OSError, json.JSONDecodeError):
            return

        if str(payload.get("origin", "")).strip() != self.origin:
            return

        if str(payload.get("state", "")).strip().lower() == "finished":
            return

        saved_visited = payload.get("visited_urls", [])
        if isinstance(saved_visited, list):
            for item in saved_visited:
                if isinstance(item, str) and item:
                    self.visited.add(item)

        saved_frontier = payload.get("frontier", [])
        if isinstance(saved_frontier, list):
            for item in saved_frontier:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url", "")).strip()
                depth = int(item.get("depth", 0))
                if not url or depth > self.depth:
                    continue
                self.pending.add(url)
                await self.frontier.put((url, depth))

        if self.frontier.qsize() > 0:
            self._resume_loaded = True
            self._append_log(
                f"resumed from checkpoint with visited={len(self.visited)} frontier={self.frontier.qsize()}"
            )

    @staticmethod
    def _bucket_for_word(word: str) -> str:
        first = word[0].lower()
        if "a" <= first <= "z":
            return first
        if "0" <= first <= "9":
            return "num"
        return "other"


def start_crawler_job(
    origin: str,
    depth: int,
    hit_rate: float = 5.0,
    queue_capacity: int = 500,
    max_pages: Optional[int] = None,
    visited_file: str = "visited_urls.data",
    storage_dir: str = "storage",
    state_dir: str = "crawler_states",
) -> Tuple[str, threading.Thread]:
    """Start crawler in a dedicated thread and run async pipeline inside it."""
    created_epoch = int(time.time())
    ready = threading.Event()
    holder: Dict[str, object] = {}

    def _runner() -> None:
        thread_id = threading.get_ident()
        crawler_id = f"[{created_epoch}_{thread_id}]"
        holder["crawler_id"] = crawler_id
        ready.set()

        limits = CrawlLimits(
            hit_rate=max(0.1, float(hit_rate)),
            queue_capacity=max(10, int(queue_capacity)),
            max_pages=max_pages,
        )
        job = CrawlerJob(
            origin=origin,
            depth=depth,
            crawler_id=crawler_id,
            limits=limits,
            visited_file=Path(visited_file),
            storage_dir=Path(storage_dir),
            state_dir=Path(state_dir),
        )
        asyncio.run(job.run())

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    ready.wait(timeout=2.0)

    crawler_id = str(holder.get("crawler_id", f"[{created_epoch}_{thread.ident or 0}]"))
    return crawler_id, thread
