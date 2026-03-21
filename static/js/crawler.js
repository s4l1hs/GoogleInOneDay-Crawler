(() => {
  const clearForm = document.getElementById("clearForm");
  const crawlerForm = document.getElementById("crawlerForm");

  if (clearForm) {
    clearForm.addEventListener("submit", (event) => {
      const ok = window.confirm("Delete all crawler states, index shards, and visited URL data?");
      if (!ok) {
        event.preventDefault();
      }
    });
  }

  if (crawlerForm) {
    crawlerForm.addEventListener("submit", () => {
      const btn = crawlerForm.querySelector("button[type='submit']");
      if (btn) {
        btn.setAttribute("disabled", "disabled");
        btn.textContent = "Starting...";
      }
    });
  }
})();
