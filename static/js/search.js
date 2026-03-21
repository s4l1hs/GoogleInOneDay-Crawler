(() => {
  const input = document.getElementById("searchInput");
  const form = document.getElementById("searchForm");

  if (!input || !form) {
    return;
  }

  input.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      input.value = "";
    }
  });

  form.addEventListener("submit", () => {
    const btn = form.querySelector("button[type='submit']");
    if (btn) {
      btn.textContent = "Searching...";
    }
  });
})();
