from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template_string, request, url_for

from crawler_job import start_crawler_job
from search_module import SearchEngine

app = Flask(__name__)

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

    return render_template_string(
        CRAWLER_HTML,
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

    return render_template_string(STATUS_HTML, crawler_id=crawler_id)


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
        return jsonify({"crawler_id": crawler_id, "state": "unknown", "done": True, "new_logs": [], "log_count": 0})

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
        results_payload = engine.search(query=q, page=page, page_size=page_size)

    return render_template_string(SEARCH_HTML, data=results_payload)


CRAWLER_HTML = """
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>Crawler Jobs</title>
  <style>
    body { font-family: Menlo, Monaco, monospace; margin: 24px; background: #f7f7f2; color: #222; }
    h1, h2 { margin: 0 0 12px 0; }
    .card { background: #fff; border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
    label { display: block; margin: 8px 0 4px; font-weight: bold; }
    input { width: 100%; max-width: 520px; padding: 8px; border: 1px solid #bbb; border-radius: 6px; }
    button { margin-top: 12px; padding: 10px 14px; border: 0; border-radius: 8px; background: #145da0; color: white; cursor: pointer; }
    .danger { background: #b32020; }
    .danger:hover { background: #8a1717; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ececec; }
    .ok { color: #1f7a1f; }
    .warn { color: #8a6000; }
    .err { color: #a32020; }
    .nav a { margin-right: 10px; }
  </style>
</head>
<body>
  <div class=\"nav\">
    <a href=\"/crawler\">Crawler</a>
    <a href=\"/status\">Status</a>
    <a href=\"/search\">Search</a>
  </div>
  <h1>New Crawler Job</h1>
  <div class=\"card\">
    <form method=\"post\" action=\"/crawler\">
      <input type=\"hidden\" name=\"action\" value=\"start\" />
      <label>Origin URL</label>
      <input name=\"origin\" placeholder=\"https://example.com\" required />

      <label>Depth</label>
      <input name=\"depth\" type=\"number\" value=\"1\" min=\"0\" />

      <label>Hit Rate (requests/sec)</label>
      <input name=\"hit_rate\" type=\"number\" step=\"0.1\" value=\"5.0\" min=\"0.1\" />

      <label>Queue Capacity</label>
      <input name=\"queue_capacity\" type=\"number\" value=\"500\" min=\"10\" />

      <button type=\"submit\">Start Crawler</button>
    </form>

    <form method=\"post\" action=\"/crawler\" onsubmit=\"return confirm('Delete all crawler states, index shards, and visited URL data?');\">
      <input type=\"hidden\" name=\"action\" value=\"clear\" />
      <button type=\"submit\" class=\"danger\">Clear Crawler Data</button>
    </form>

    {% if started_id %}
      <p class=\"ok\">Started crawler: <strong>{{ started_id }}</strong></p>
      <p><a href=\"/status?crawler_id={{ started_id }}\">Open status page</a></p>
    {% endif %}
    {% if clear_message %}
      <p class=\"ok\">{{ clear_message }}</p>
    {% endif %}
    {% if error %}
      <p class=\"err\">{{ error }}</p>
    {% endif %}
    {% if clear_error %}
      <p class=\"err\">{{ clear_error }}</p>
    {% endif %}
  </div>

  <h2>Crawler History (Newest First)</h2>
  <div class=\"card\">
    <table>
      <thead>
        <tr>
          <th>Crawler ID</th>
          <th>Origin</th>
          <th>State</th>
          <th>Indexed</th>
          <th>Queue</th>
          <th>Updated</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {% for row in history %}
          <tr>
            <td>{{ row.crawler_id }}</td>
            <td>{{ row.origin }}</td>
            <td>
              {% if row.state == 'finished' %}<span class=\"ok\">finished</span>
              {% elif row.state == 'interrupted' %}<span class=\"err\">interrupted</span>
              {% else %}<span class=\"warn\">{{ row.state }}</span>{% endif %}
            </td>
            <td>{{ row.pages_indexed }}</td>
            <td>{{ row.queue_size }}</td>
            <td>{{ row.updated_at }}</td>
            <td><a href=\"/status?crawler_id={{ row.crawler_id }}\">View</a></td>
          </tr>
        {% else %}
          <tr><td colspan=\"7\">No crawler history found.</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


STATUS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Crawler Status</title>
  <style>
    body { font-family: Menlo, Monaco, monospace; margin: 24px; background: #f6fbff; color: #1c1c1c; }
    .card { background: white; border: 1px solid #d8e8f5; border-radius: 10px; padding: 16px; margin-bottom: 14px; }
    .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .metric { background: #fafcff; border: 1px solid #e5eef5; border-radius: 8px; padding: 10px; }
    .logs { height: 340px; overflow: auto; background: #0f1720; color: #dbe6f5; padding: 10px; border-radius: 8px; white-space: pre-wrap; word-wrap: break-word;}
    .ok { color: #1f7a1f; font-weight: bold; }
    .warn { color: #8a6000; font-weight: bold; }
    .err { color: #a32020; font-weight: bold; }
    .nav a { margin-right: 10px; font-weight: bold; color: #145da0; text-decoration: none;}
    .nav a:hover { text-decoration: underline; }
    button { padding: 8px 12px; border: 0; border-radius: 6px; background: #145da0; color: white; cursor: pointer; }
    button:hover { background: #0f467a; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/crawler">Crawler</a>
    <a href="/status">Status</a>
    <a href="/search">Search</a>
  </div>

  <h1>Crawler Status</h1>
  <div class="card">
    <label style="font-weight: bold;">Crawler ID:</label>
    <div style="display: flex; gap: 8px; margin-top: 6px;">
        <input id="crawlerId" value="{{ crawler_id }}" style="flex-grow: 1; max-width: 650px; padding: 8px; border: 1px solid #bbb; border-radius: 4px;" placeholder="Enter Crawler ID (e.g. [171101...])" />
        <button id="watchBtn">Watch / Refresh</button>
    </div>
  </div>

  <div class="card">
    <div id="stateLine" style="font-size: 1.1em; margin-bottom: 10px;">State: <strong>unknown</strong></div>
    <div class="grid">
      <div class="metric">Pages Processed: <strong id="pagesProcessed">0</strong></div>
      <div class="metric">Pages Indexed: <strong id="pagesIndexed">0</strong></div>
      <div class="metric">Pages Failed: <strong id="pagesFailed">0</strong></div>
      <div class="metric">Visited Count: <strong id="visitedCount">0</strong></div>
      <div class="metric">Queue Depth: <strong id="queueSize">0</strong></div>
      <div class="metric">Back-pressure: <strong id="backPressure">unknown</strong></div>
    </div>
  </div>

  <div class="card">
    <h3 style="margin-top: 0;">State Logs (Long Polling)</h3>
    <div id="logs" class="logs">Waiting for logs...</div>
  </div>

  <script>
    const crawlerIdInput = document.getElementById('crawlerId');
    const watchBtn = document.getElementById('watchBtn');
    const logsEl = document.getElementById('logs');

    let sinceUpdated = 0;
    let sinceLogIndex = 0;
    let stopped = false;
    let currentPollController = null;

    function setStateLine(state) {
      const line = document.getElementById('stateLine');
      let css = 'warn';
      if (state === 'finished') css = 'ok';
      if (state === 'interrupted') css = 'err';
      line.innerHTML = 'State: <span class="' + css + '">' + state + '</span>';
    }

    function updateMetrics(payload) {
      if (!payload) return;
      setStateLine(payload.state || 'unknown');
      document.getElementById('pagesProcessed').textContent = payload.pages_processed || 0;
      document.getElementById('pagesIndexed').textContent = payload.pages_indexed || 0;
      document.getElementById('pagesFailed').textContent = payload.pages_failed || 0;
      document.getElementById('visitedCount').textContent = payload.visited_count || 0;
      document.getElementById('queueSize').textContent = payload.queue_size || 0;
      document.getElementById('backPressure').textContent = payload.back_pressure || 'unknown';
    }

    function appendLogs(newLogs) {
      if (!Array.isArray(newLogs) || newLogs.length === 0) return;
      if (logsEl.textContent.includes("Waiting for logs...")) {
          logsEl.textContent = ""; // Clear initial message
      }
      const text = newLogs.map(x => String(x)).join('\\n') + '\\n';
      logsEl.textContent += text;
      logsEl.scrollTop = logsEl.scrollHeight; // Auto-scroll to bottom
    }

    async function longPoll() {
      if (stopped) return;
      const crawlerId = crawlerIdInput.value.trim();
      if (!crawlerId) {
        setTimeout(longPoll, 2000);
        return;
      }

      const params = new URLSearchParams({
        crawler_id: crawlerId,
        since_updated: String(sinceUpdated),
        since_log_index: String(sinceLogIndex),
        timeout: '20'
      });

      // Abort previous request if a new one is forced
      if (currentPollController) {
          currentPollController.abort();
      }
      currentPollController = new AbortController();

      try {
        const res = await fetch('/status/poll?' + params.toString(), { 
            cache: 'no-store',
            signal: currentPollController.signal
        });
        
        if (!res.ok) throw new Error("HTTP " + res.status);
        
        const payload = await res.json();
        updateMetrics(payload);
        appendLogs(payload.new_logs || []);
        
        sinceUpdated = Number(payload.updated_at || sinceUpdated);
        sinceLogIndex = Number(payload.log_count || sinceLogIndex);

        if (payload.done) {
          appendLogs(["[System] Crawler job has ended."]);
          stopped = true;
          return;
        }
      } catch (err) {
        if (err.name !== 'AbortError') {
            console.error("Poll error:", err);
            // Don't flood the UI with errors, just wait and retry
        }
      }

      setTimeout(longPoll, 500);
    }

    function resetAndWatch() {
        logsEl.textContent = 'Waiting for logs...';
        sinceUpdated = 0;
        sinceLogIndex = 0;
        stopped = false;
        
        // Reset metrics
        updateMetrics({state: 'unknown'});
        
        if (currentPollController) {
            currentPollController.abort();
        }
        longPoll();
    }

    watchBtn.addEventListener('click', resetAndWatch);

    // Auto-start if ID is present on load
    if (crawlerIdInput.value.trim()) {
      resetAndWatch();
    }
  </script>
</body>
</html>
"""


SEARCH_HTML = """
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>Search</title>
  <style>
    body { font-family: Menlo, Monaco, monospace; margin: 24px; background: #fffaf2; color: #1f1f1f; }
    .card { background: white; border: 1px solid #eadfca; border-radius: 10px; padding: 16px; margin-bottom: 14px; }
    input { width: 100%; max-width: 700px; padding: 8px; border: 1px solid #bbb; border-radius: 6px; }
    button { margin-top: 8px; padding: 8px 12px; border: 0; border-radius: 8px; background: #a54c00; color: #fff; cursor: pointer; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #efefef; }
    .nav a { margin-right: 10px; }
    .pager a { margin-right: 8px; }
  </style>
</head>
<body>
  <div class=\"nav\">
    <a href=\"/crawler\">Crawler</a>
    <a href=\"/status\">Status</a>
    <a href=\"/search\">Search</a>
  </div>

  <h1>Search Index</h1>
  <div class=\"card\">
    <form method=\"get\" action=\"/search\">
      <input name=\"q\" value=\"{{ data.query }}\" placeholder=\"Enter query terms\" />
      <input type=\"hidden\" name=\"page\" value=\"1\" />
      <input type=\"hidden\" name=\"page_size\" value=\"{{ data.page_size }}\" />
      <button type=\"submit\">Search</button>
    </form>
  </div>

  <div class=\"card\">
    <div>Total Results: {{ data.total }} | Page {{ data.page }} / {{ data.total_pages }}</div>
    <table>
      <thead>
        <tr>
          <th>Relevant URL</th>
          <th>Origin URL</th>
          <th>Depth</th>
        </tr>
      </thead>
      <tbody>
        {% for row in data.results %}
          <tr>
            <td><a href=\"{{ row[0] }}\" target=\"_blank\">{{ row[0] }}</a></td>
            <td><a href=\"{{ row[1] }}\" target=\"_blank\">{{ row[1] }}</a></td>
            <td>{{ row[2] }}</td>
          </tr>
        {% else %}
          <tr><td colspan=\"3\">No results.</td></tr>
        {% endfor %}
      </tbody>
    </table>

    <div class=\"pager\" style=\"margin-top: 10px;\">
      {% if data.page > 1 %}
        <a href=\"/search?q={{ data.query }}&page={{ data.page - 1 }}&page_size={{ data.page_size }}\">Prev</a>
      {% endif %}
      {% if data.page < data.total_pages %}
        <a href=\"/search?q={{ data.query }}&page={{ data.page + 1 }}&page_size={{ data.page_size }}\">Next</a>
      {% endif %}
    </div>
  </div>
</body>
</html>
"""


if __name__ == "__main__":
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    app.run(host="127.0.0.1", port=8000, debug=True)
