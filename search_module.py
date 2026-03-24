"""Search module for filesystem-backed letter-sharded inverted index data.

Design goals:
- Safe concurrent reads while crawler/indexer writes are in progress.
- Efficient reads by touching only shard files relevant to query terms.
- Frequency-based ranking with frontend-ready pagination.
"""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from crawler_job import INDEX_FILE_LOCK

_WORD_RE = re.compile(r"[A-Za-z0-9]+")


@dataclass
class SearchItem:
    relevant_url: str
    origin_url: str
    depth: int
    score: int


class SearchEngine:
    def __init__(self, storage_dir: str = "storage", read_lock: Optional[threading.Lock] = None) -> None:
        self.storage_dir = Path(storage_dir)
        self.read_lock = read_lock or INDEX_FILE_LOCK

    def search(
        self,
        query: str,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, object]:
        """Search query terms and return paginated triples.

        Output payload:
            {
              "query": str,
              "total": int,
              "page": int,
              "page_size": int,
              "total_pages": int,
              "results": [(relevant_url, origin_url, depth), ...]
            }
        """
        terms = self._parse_query(query)
        if not terms:
            return {
                "query": query,
                "total": 0,
                "page": 1,
                "page_size": max(1, page_size),
                "total_pages": 0,
                "results": [],
            }

        ranked = self._rank_for_terms(terms)
        triples = [(item.relevant_url, item.origin_url, item.depth) for item in ranked]
        paged, safe_page, total_pages = self._paginate(triples, page=page, page_size=page_size)

        return {
            "query": query,
            "total": len(triples),
            "page": safe_page,
            "page_size": max(1, page_size),
            "total_pages": total_pages,
            "results": paged,
        }

    def search_triples(
        self,
        query: str,
        page: int = 1,
        page_size: int = 20,
    ) -> List[Tuple[str, str, int]]:
        """Convenience API returning only the required triples list."""
        payload = self.search(query=query, page=page, page_size=page_size)
        return payload["results"]  # type: ignore[return-value]

    def search_api(
        self,
        query: str,
        sort_by: str = "relevance",
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, object]:
        """JSON-friendly API payload with frequency and relevance score."""
        terms = self._parse_query(query)
        safe_page_size = max(1, int(page_size))
        if not terms:
            return {
                "query": query,
                "sortBy": sort_by,
                "page": 1,
                "page_size": safe_page_size,
                "total": 0,
                "total_pages": 0,
                "results": [],
            }

        ranked = self._rank_for_terms(terms)
        rows: List[Dict[str, object]] = []
        for item in ranked:
            rows.append(
                {
                    "url": item.relevant_url,
                    "origin_url": item.origin_url,
                    "depth": item.depth,
                    "frequency": item.score,
                    "relevance_score": self._relevance_score(item.score, item.depth),
                }
            )

        sort_key = (sort_by or "relevance").strip().lower()
        if sort_key == "frequency":
            rows.sort(key=lambda row: (-int(row["frequency"]), int(row["depth"]), str(row["url"])))
        else:
            rows.sort(
                key=lambda row: (
                    -int(row["relevance_score"]),
                    -int(row["frequency"]),
                    int(row["depth"]),
                    str(row["url"]),
                )
            )

        total = len(rows)
        if total == 0:
            return {
                "query": query,
                "sortBy": sort_key,
                "page": 1,
                "page_size": safe_page_size,
                "total": 0,
                "total_pages": 0,
                "results": [],
            }

        total_pages = (total + safe_page_size - 1) // safe_page_size
        safe_page = max(1, min(int(page), total_pages))
        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size

        return {
            "query": query,
            "sortBy": sort_key,
            "page": safe_page,
            "page_size": safe_page_size,
            "total": total,
            "total_pages": total_pages,
            "results": rows[start:end],
        }

    def _parse_query(self, query: str) -> List[str]:
        terms = [term.lower() for term in _WORD_RE.findall(query)]
        # Keep insertion order while removing duplicates.
        deduped = list(dict.fromkeys(terms))
        return deduped

    def _rank_for_terms(self, terms: Iterable[str]) -> List[SearchItem]:
        term_set = set(terms)
        candidate_files = self._candidate_shards(term_set)

        scores: Dict[str, int] = {}
        origins: Dict[str, str] = {}
        depths: Dict[str, int] = {}

        for shard_path in candidate_files:
            if not shard_path.exists():
                continue
            with self.read_lock:
                with shard_path.open("r", encoding="utf-8") as fh:
                    for raw in fh:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            # Tolerate occasional partially written/corrupt lines.
                            continue

                        word = str(row.get("word", "")).lower()
                        if word not in term_set:
                            continue

                        relevant_url = str(row.get("current_url", ""))
                        origin_url = str(row.get("origin_url", ""))
                        depth = int(row.get("depth", 0))
                        frequency = int(row.get("frequency", 0))
                        if not relevant_url or not origin_url or frequency <= 0:
                            continue

                        scores[relevant_url] = scores.get(relevant_url, 0) + frequency

                        # Keep stable origin and smallest known depth for same URL.
                        if relevant_url not in origins:
                            origins[relevant_url] = origin_url
                        depths[relevant_url] = min(depths.get(relevant_url, depth), depth)

        ranked = [
            SearchItem(
                relevant_url=url,
                origin_url=origins.get(url, ""),
                depth=depths.get(url, 0),
                score=score,
            )
            for url, score in scores.items()
        ]

        # "Maximum hits" ranking: highest aggregated frequency first.
        ranked.sort(key=lambda item: (-item.score, item.depth, item.relevant_url))
        return ranked

    def _candidate_shards(self, terms: Set[str]) -> List[Path]:
        buckets = {self._bucket_for_word(term) for term in terms if term}
        return [self.storage_dir / f"{bucket}.data" for bucket in sorted(buckets)]

    @staticmethod
    def _relevance_score(frequency: int, depth: int) -> int:
        # Assignment-aligned scoring formula.
        return (int(frequency) * 10) + 1000 - (int(depth) * 5)

    @staticmethod
    def _bucket_for_word(word: str) -> str:
        first = word[0].lower()
        if "a" <= first <= "z":
            return first
        if "0" <= first <= "9":
            return "num"
        return "other"

    @staticmethod
    def _paginate(
        records: List[Tuple[str, str, int]],
        page: int,
        page_size: int,
    ) -> Tuple[List[Tuple[str, str, int]], int, int]:
        safe_page_size = max(1, int(page_size))
        total = len(records)
        if total == 0:
            return [], 1, 0

        total_pages = (total + safe_page_size - 1) // safe_page_size
        safe_page = max(1, min(int(page), total_pages))

        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        return records[start:end], safe_page, total_pages


def search(
    query: str,
    page: int = 1,
    page_size: int = 20,
    storage_dir: str = "storage",
) -> Dict[str, object]:
    """Functional helper for one-off search calls."""
    engine = SearchEngine(storage_dir=storage_dir)
    return engine.search(query=query, page=page, page_size=page_size)
