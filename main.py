from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, url_for

from crawler_job import start_crawler_job
from search_module import SearchEngine

app = Flask(__name__, template_folder="templates", static_folder="static")

STATE_DIR = Path("crawler_states")
VISITED_FILE = "visited_urls.data"
STORAGE_DIR = "storage"

JOBS_LOCK = threading.Lock()
JOBS: Dict[str, threading.Thread] = {}


def _active_crawler_ids() -> List[str]:
    active: List[str] = []
    with JOBS_LOCK:
        stale_ids: List[str] = []
        for crawler_id, thread in JOBS.items():
            if thread.is_alive():
                active.append(crawler_id)
            else:
                stale_ids.append(crawler_id)
        for crawler_id in stale_ids:
            JOBS.pop(crawler_id, None)
    return active


def _clear_crawler_data() -> Tuple[bool, str]:
    active_ids = _active_crawler_ids()
    if active_ids:
        return False, "Cannot clear data while crawler jobs are still running."

    deleted_states = 0
    deleted_storage = 0
    deleted_visited = 0

    if STATE_DIR.exists():
        for state_file in STATE_DIR.glob("*.data"):
            try:
                state_file.unlink()
                deleted_states += 1
            except OSError:
                continue
        for tmp_file in STATE_DIR.glob("*.tmp"):
            try:
                tmp_file.unlink()
            except OSError:
                continue

    storage_path = Path(STORAGE_DIR)
    if storage_path.exists():
        for index_file in storage_path.glob("*.data"):
            try:
                index_file.unlink()
                deleted_storage += 1
            except OSError:
                continue

    visited_path = Path(VISITED_FILE)
    if visited_path.exists():
        try:
            visited_path.unlink()
            deleted_visited = 1
        except OSError:
            deleted_visited = 0

    return (
        True,
        (
            "Crawler data cleared. "
            f"states={deleted_states}, index_files={deleted_storage}, visited_file={deleted_visited}"
        ),
    )


def _read_state(crawler_id: str) -> Optional[Dict[str, object]]:
    state_file = STATE_DIR / f"{crawler_id}.data"
    if not state_file.exists():
        return None

    try:
        with state_file.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None

    payload["_state_file_mtime"] = int(state_file.stat().st_mtime)
    return payload


def _thread_alive(crawler_id: str) -> bool:
    with JOBS_LOCK:
        thread = JOBS.get(crawler_id)
    return bool(thread and thread.is_alive())


def _effective_state(payload: Dict[str, object], crawler_id: str) -> str:
    raw_state = str(payload.get("state", "unknown"))
    if raw_state == "finished":
        return "finished"

    if _thread_alive(crawler_id):
        return raw_state

    updated_at = int(payload.get("updated_at", 0) or 0)
    stale_seconds = int(time.time()) - updated_at if updated_at else 999999

    if raw_state in ("running", "initializing") and stale_seconds > 20:
        return "interrupted"
    return raw_state


def _back_pressure_status(payload: Dict[str, object]) -> str:
    logs = payload.get("logs", [])
    if not isinstance(logs, list) or not logs:
        return "unknown"

    tail = [str(x).lower() for x in logs[-25:]]
    if any("back-pressure active" in line or "throttle:" in line for line in tail):
        return "active"
    return "normal"


def _history() -> List[Dict[str, object]]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, object]] = []
    for state_file in STATE_DIR.glob("*.data"):
        try:
            with state_file.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue

        crawler_id = str(payload.get("crawler_id", state_file.stem))
        updated_at = int(payload.get("updated_at", int(state_file.stat().st_mtime)))
        state = _effective_state(payload, crawler_id)

        rows.append(
            {
                "crawler_id": crawler_id,
                "origin": str(payload.get("origin", "")),
                "state": state,
                "updated_at": updated_at,
                "pages_indexed": int(payload.get("pages_indexed", 0)),
                "queue_size": int(payload.get("queue_size", 0)),
            }
        )

    rows.sort(key=lambda item: int(item["updated_at"]), reverse=True)
    return rows


@app.route("/")
def index() -> object:
    return redirect(url_for("crawler_page"))


@app.route("/crawler", methods=["GET", "POST"])
def crawler_page() -> object:
    started_id = None
    error = None
    clear_message = None
    clear_error = None

    if request.method == "POST":
        action = (request.form.get("action") or "start").strip().lower()
        if action == "clear":
            ok, message = _clear_crawler_data()
            if ok:
                clear_message = message
            else:
                clear_error = message
        else:
            origin = (request.form.get("origin") or "").strip()
            depth_raw = (request.form.get("depth") or "0").strip()
            hit_rate_raw = (request.form.get("hit_rate") or "5.0").strip()
            queue_capacity_raw = (request.form.get("queue_capacity") or "500").strip()

            try:
                depth = max(0, int(depth_raw))
                hit_rate = max(0.1, float(hit_rate_raw))
                queue_capacity = max(10, int(queue_capacity_raw))
                if not origin:
                    raise ValueError("Origin URL is required")

                crawler_id, thread = start_crawler_job(
                    origin=origin,
                    depth=depth,
                    hit_rate=hit_rate,
                    queue_capacity=queue_capacity,
                    visited_file=VISITED_FILE,
                    storage_dir=STORAGE_DIR,
                    state_dir=str(STATE_DIR),
                )
                with JOBS_LOCK:
                    JOBS[crawler_id] = thread
                started_id = crawler_id
            except Exception as exc:
                error = str(exc)

    history = _history()

    return render_template(
        "crawler.html",
        started_id=started_id,
        error=error,
        clear_message=clear_message,
        clear_error=clear_error,
        history=history,
    )


@app.route("/status")
def status_page() -> object:
    crawler_id = (request.args.get("crawler_id") or "").strip()
    if not crawler_id:
        rows = _history()
        if rows:
            crawler_id = str(rows[0]["crawler_id"])

    return render_template("status.html", crawler_id=crawler_id)


@app.route("/status/poll")
def status_poll() -> object:
    crawler_id = (request.args.get("crawler_id") or "").strip()
    if not crawler_id:
        return jsonify({"error": "crawler_id is required"}), 400

    timeout = max(1, min(30, int(request.args.get("timeout", "20"))))
    since_updated = int(request.args.get("since_updated", "0") or 0)
    since_log_index = int(request.args.get("since_log_index", "0") or 0)

    deadline = time.time() + timeout

    while time.time() < deadline:
        payload = _read_state(crawler_id)
        if payload is None:
            time.sleep(0.25)
            continue

        updated_at = int(payload.get("updated_at", 0) or 0)
        logs = payload.get("logs", [])
        if not isinstance(logs, list):
            logs = []

        has_update = updated_at > since_updated or len(logs) > since_log_index
        eff_state = _effective_state(payload, crawler_id)
        if eff_state in ("finished", "interrupted"):
            has_update = True

        if has_update:
            new_logs = [str(x) for x in logs[since_log_index:]]
            response = {
                "crawler_id": crawler_id,
                "state": eff_state,
                "origin": str(payload.get("origin", "")),
                "updated_at": updated_at,
                "queue_size": int(payload.get("queue_size", 0)),
                "visited_count": int(payload.get("visited_count", 0)),
                "pages_processed": int(payload.get("pages_processed", 0)),
                "pages_indexed": int(payload.get("pages_indexed", 0)),
                "pages_failed": int(payload.get("pages_failed", 0)),
                "depth_limit": int(payload.get("depth_limit", 0)),
                "back_pressure": _back_pressure_status(payload),
                "new_logs": new_logs,
                "log_count": len(logs),
                "done": eff_state in ("finished", "interrupted"),
            }
            return jsonify(response)

        time.sleep(0.35)

    payload = _read_state(crawler_id)
    if payload is None:
        return jsonify(
            {
                "crawler_id": crawler_id,
                "state": "unknown",
                "done": True,
                "new_logs": [],
                "log_count": 0,
            }
        )

    logs = payload.get("logs", [])
    if not isinstance(logs, list):
        logs = []

    eff_state = _effective_state(payload, crawler_id)
    return jsonify(
        {
            "crawler_id": crawler_id,
            "state": eff_state,
            "origin": str(payload.get("origin", "")),
            "updated_at": int(payload.get("updated_at", 0) or 0),
            "queue_size": int(payload.get("queue_size", 0)),
            "visited_count": int(payload.get("visited_count", 0)),
            "pages_processed": int(payload.get("pages_processed", 0)),
            "pages_indexed": int(payload.get("pages_indexed", 0)),
            "pages_failed": int(payload.get("pages_failed", 0)),
            "depth_limit": int(payload.get("depth_limit", 0)),
            "back_pressure": _back_pressure_status(payload),
            "new_logs": [],
            "log_count": len(logs),
            "done": eff_state in ("finished", "interrupted"),
        }
    )


@app.route("/search")
def search_page() -> object:
    query_api = (request.args.get("query") or "").strip()
    sort_by = (request.args.get("sortBy") or "relevance").strip().lower()

    if query_api:
        page = max(1, int(request.args.get("page", "1")))
        page_size = max(1, min(100, int(request.args.get("page_size", "20"))) )
        engine = SearchEngine(storage_dir=STORAGE_DIR)
        return jsonify(
            engine.search_api(
                query=query_api,
                sort_by=sort_by,
                page=page,
                page_size=page_size,
            )
        )

    q = (request.args.get("q") or "").strip()
    page = max(1, int(request.args.get("page", "1")))
    page_size = max(1, min(100, int(request.args.get("page_size", "20"))))

    results_payload = {
        "query": q,
        "total": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 0,
        "results": [],
    }

    if q:
        engine = SearchEngine(storage_dir=STORAGE_DIR)
        results_payload = engine.search_api(
            query=q,
            sort_by="relevance",
            page=page,
            page_size=page_size,
        )

    return render_template("search.html", data=results_payload)


if __name__ == "__main__":
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    app.run(host="127.0.0.1", port=8000, debug=True)
