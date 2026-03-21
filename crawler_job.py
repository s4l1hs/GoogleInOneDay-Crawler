"""Core crawler job module.

This module provides a thread-based crawler job that:
- Crawls pages recursively up to a depth limit.
- Persists a thread-safe visited URL set to disk.
- Extracts words and outlinks from HTML.
- Appends inverted-index records into letter-sharded files.
- Applies queue-based back-pressure and hit-rate throttling.
- Continuously writes crawler state snapshots to a JSON data file.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from queue import Empty, Queue
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.request import Request, urlopen

WORD_RE = re.compile(r"[A-Za-z0-9]+")

_VISITED_FILE_LOCK = threading.Lock()
INDEX_FILE_LOCK = threading.Lock()
_INDEX_FILE_LOCK = INDEX_FILE_LOCK


@dataclass
class CrawlLimits:
    hit_rate: float = 5.0
    queue_capacity: int = 500
    fetch_timeout: float = 8.0
    max_pages: Optional[int] = None


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


class PersistentVisitedSet:
    """Thread-safe visited set persisted in a line-based data file."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self._lock = threading.Lock()
        self._urls: Set[str] = set()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_from_disk()
        self._file_handle = self.file_path.open("a", encoding="utf-8")

    def _load_from_disk(self) -> None:
        if not self.file_path.exists():
            return
        with self.file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                url = line.strip()
                if url:
                    self._urls.add(url)

    def add_if_absent(self, url: str) -> bool:
        """Add URL if missing and append to disk atomically enough for local usage."""
        with self._lock:
            if url in self._urls:
                return False
            self._urls.add(url)
            with _VISITED_FILE_LOCK:
                self._file_handle.write(url + "\n")
                self._file_handle.flush()
            return True

    def close(self) -> None:
        with self._lock:
            file_handle = getattr(self, "_file_handle", None)
            if file_handle and not file_handle.closed:
                file_handle.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def snapshot(self) -> List[str]:
        with self._lock:
            return sorted(self._urls)

    def __len__(self) -> int:
        with self._lock:
            return len(self._urls)


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
        self.origin = origin
        self.depth = max(0, depth)
        self.crawler_id = crawler_id
        self.limits = limits
        self.visited = PersistentVisitedSet(visited_file)
        self.storage_dir = storage_dir
        self.state_dir = state_dir

        self.frontier: Queue[Tuple[str, int]] = Queue(maxsize=max(1, limits.queue_capacity))
        self.state_lock = threading.Lock()
        self.log_buffer: Deque[str] = deque(maxlen=500)

        self.state = "initializing"
        self.pages_processed = 0
        self.pages_indexed = 0
        self.pages_failed = 0
        self.last_fetch_ts = 0.0

    def run(self) -> None:
        self.state = "running"
        self.frontier.put((self.origin, 0))
        self._append_log(f"started crawler with origin={self.origin} depth={self.depth}")
        self._write_state_snapshot()

        while True:
            if self.limits.max_pages is not None and self.pages_processed >= self.limits.max_pages:
                self._append_log("max_pages reached, stopping crawl")
                break

            try:
                current_url, current_depth = self.frontier.get(timeout=0.4)
            except Empty:
                if self.frontier.empty():
                    self._append_log("frontier empty, crawl completed")
                    break
                continue

            self._apply_back_pressure()

            if not self.visited.add_if_absent(current_url):
                self.frontier.task_done()
                continue

            self.pages_processed += 1
            self._respect_hit_rate()

            html = self._fetch_html(current_url)
            if html is None:
                self.pages_failed += 1
                self.frontier.task_done()
                self._write_state_snapshot()
                continue

            words, links = self._extract_words_and_links(current_url, html)
            self._persist_word_frequencies(
                origin_url=self.origin,
                current_url=current_url,
                depth=current_depth,
                words=words,
            )
            self.pages_indexed += 1

            if current_depth < self.depth:
                for next_url in links:
                    self._enqueue_with_back_pressure((next_url, current_depth + 1))

            self.frontier.task_done()
            self._write_state_snapshot()

        self.state = "finished"
        self._append_log("crawler finished")
        self._write_state_snapshot()

    def _respect_hit_rate(self) -> None:
        if self.limits.hit_rate <= 0:
            return
        min_interval = 1.0 / self.limits.hit_rate
        elapsed = time.time() - self.last_fetch_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_fetch_ts = time.time()

    def _fetch_html(self, url: str) -> Optional[str]:
        req = Request(
            url,
            headers={
                "User-Agent": "CrawlerJob/1.0 (+native-python-standard-lib)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        try:
            with urlopen(req, timeout=self.limits.fetch_timeout) as response:
                status = getattr(response, "status", response.getcode())
                if status != 200:
                    self._append_log(f"skip non-200 response: {url} status={status}")
                    return None
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
        except Exception as exc:
            self._append_log(f"fetch failed: {url} error={exc}")
            return None

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
        absolute = urljoin(base_url, href.strip())
        absolute, _fragment = urldefrag(absolute)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            return None
        if not parsed.netloc:
            return None
        return parsed.geturl()

    def _persist_word_frequencies(
        self,
        origin_url: str,
        current_url: str,
        depth: int,
        words: Iterable[str],
    ) -> None:
        freqs = Counter(words)
        if not freqs:
            return

        self.storage_dir.mkdir(parents=True, exist_ok=True)

        bucket_rows: Dict[str, List[Tuple[str, int]]] = {}
        for word, freq in freqs.items():
            bucket = self._bucket_for_word(word)
            bucket_rows.setdefault(bucket, []).append((word, int(freq)))

        with _INDEX_FILE_LOCK:
            for bucket, rows in bucket_rows.items():
                data_file = self.storage_dir / f"{bucket}.data"
                with data_file.open("a", encoding="utf-8") as fh:
                    for word, freq in rows:
                        record = {
                            "word": word,
                            "origin_url": origin_url,
                            "current_url": current_url,
                            "depth": depth,
                            "frequency": freq,
                            "ts": int(time.time()),
                        }
                        fh.write(json.dumps(record, ensure_ascii=True) + "\n")

    @staticmethod
    def _bucket_for_word(word: str) -> str:
        first = word[0].lower()
        if "a" <= first <= "z":
            return first
        if "0" <= first <= "9":
            return "num"
        return "other"

    def _enqueue_with_back_pressure(self, item: Tuple[str, int]) -> None:
        # Back-pressure with adaptive wait while queue is near full.
        while self.frontier.qsize() >= int(self.frontier.maxsize * 0.9):
            self._append_log("back-pressure active: queue near capacity")
            self._write_state_snapshot()
            time.sleep(0.05)
        try:
            self.frontier.put(item, timeout=0.2)
        except Exception:
            self._append_log("queue put timeout; dropping low-priority URL")

    def _apply_back_pressure(self) -> None:
        occupancy = self.frontier.qsize() / float(self.frontier.maxsize)
        if occupancy < 0.7:
            return
        delay = min(0.4, (occupancy - 0.7) * 0.8)
        self._append_log(f"throttle: occupancy={occupancy:.2f} delay={delay:.3f}s")
        time.sleep(delay)

    def _append_log(self, message: str) -> None:
        timestamp = int(time.time())
        with self.state_lock:
            self.log_buffer.append(f"{timestamp} {message}")

    def _queue_snapshot(self, max_items: int = 200) -> List[Dict[str, object]]:
        with self.frontier.mutex:
            items = list(self.frontier.queue)[:max_items]
        return [{"url": url, "depth": depth} for (url, depth) in items]

    def _write_state_snapshot(self) -> None:
        with self.state_lock:
            state_payload = {
                "crawler_id": self.crawler_id,
                "origin": self.origin,
                "state": self.state,
                "depth_limit": self.depth,
                "queue_size": self.frontier.qsize(),
                "queue": self._queue_snapshot(),
                "visited_count": len(self.visited),
                "visited_pages": self.visited.snapshot(),
                "pages_processed": self.pages_processed,
                "pages_indexed": self.pages_indexed,
                "pages_failed": self.pages_failed,
                "logs": list(self.log_buffer),
                "updated_at": int(time.time()),
            }

        self.state_dir.mkdir(parents=True, exist_ok=True)
        target = self.state_dir / f"{self.crawler_id}.data"
        tmp = target.with_suffix(".tmp")

        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(state_payload, fh, ensure_ascii=True, indent=2)
        os.replace(tmp, target)


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
    """Start a crawler job in a dedicated thread.

    Returns:
        Tuple of (crawler_id, thread)
    """
    created_epoch = int(time.time())
    ready = threading.Event()
    holder: Dict[str, object] = {}

    def _runner() -> None:
        thread_id = threading.get_ident()
        crawler_id = f"[{created_epoch}_{thread_id}]"
        holder["crawler_id"] = crawler_id
        ready.set()

        limits = CrawlLimits(
            hit_rate=hit_rate,
            queue_capacity=queue_capacity,
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
        job.run()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    ready.wait(timeout=2.0)

    crawler_id = str(holder.get("crawler_id", f"[{created_epoch}_{thread.ident or 0}]"))
    return crawler_id, thread
