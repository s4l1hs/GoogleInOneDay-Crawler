(() => {
  const crawlerIdInput = document.getElementById("crawlerId");
  const watchBtn = document.getElementById("watchBtn");
  const logsEl = document.getElementById("logs");

  let sinceUpdated = 0;
  let sinceLogIndex = 0;
  let stopped = false;
  let currentPollController = null;

  function setStateLine(state) {
    const line = document.getElementById("stateLine");
    let css = "warn";
    if (state === "finished") css = "ok";
    if (state === "interrupted") css = "err";
    line.innerHTML = 'State: <span class="' + css + '">' + state + "</span>";
  }

  function updateMetrics(payload) {
    if (!payload) return;
    setStateLine(payload.state || "unknown");
    document.getElementById("pagesProcessed").textContent = payload.pages_processed || 0;
    document.getElementById("pagesIndexed").textContent = payload.pages_indexed || 0;
    document.getElementById("pagesFailed").textContent = payload.pages_failed || 0;
    document.getElementById("visitedCount").textContent = payload.visited_count || 0;
    document.getElementById("queueSize").textContent = payload.queue_size || 0;
    document.getElementById("backPressure").textContent = payload.back_pressure || "unknown";
  }

  function appendLogs(newLogs) {
    if (!Array.isArray(newLogs) || newLogs.length === 0) return;
    if (logsEl.textContent.includes("Waiting for logs...")) {
      logsEl.textContent = "";
    }
    logsEl.textContent += newLogs.map((x) => String(x)).join("\n") + "\n";
    logsEl.scrollTop = logsEl.scrollHeight;
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
      timeout: "20",
    });

    if (currentPollController) {
      currentPollController.abort();
    }
    currentPollController = new AbortController();

    try {
      const res = await fetch("/status/poll?" + params.toString(), {
        cache: "no-store",
        signal: currentPollController.signal,
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
      if (err.name !== "AbortError") {
        console.error("Poll error:", err);
      }
    }

    setTimeout(longPoll, 500);
  }

  function resetAndWatch() {
    logsEl.textContent = "Waiting for logs...";
    sinceUpdated = 0;
    sinceLogIndex = 0;
    stopped = false;
    updateMetrics({ state: "unknown" });

    if (currentPollController) {
      currentPollController.abort();
    }
    longPoll();
  }

  watchBtn.addEventListener("click", resetAndWatch);

  if (crawlerIdInput.value.trim()) {
    resetAndWatch();
  }
})();
