"use strict";

(() => {
  function initTheme() {
    const saved = localStorage.getItem("anyagents_admin_theme");
    if (saved === "light" || saved === "dark") {
      document.documentElement.setAttribute("data-theme", saved);
    } else {
      document.documentElement.setAttribute("data-theme", "dark");
    }
    const toggle = document.getElementById("theme-toggle");
    if (!toggle) return;
    toggle.addEventListener("click", () => {
      const current = document.documentElement.getAttribute("data-theme") || "dark";
      const next = current === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("anyagents_admin_theme", next);
    });
  }

  function initSignInPage() {
    initTheme();

    const codeEl = document.getElementById("cli-login-cmd");
    const copyBtn = document.getElementById("copy-cli-btn");
    if (!codeEl) return;

    const configured = (codeEl.getAttribute("data-coordinator-url") || "").trim();
    const resolvedUrl = configured || window.location.origin;
    const command = `anyagents auth login --url ${resolvedUrl}`;
    codeEl.textContent = command;

    if (!copyBtn) return;
    let timer = null;
    copyBtn.addEventListener("click", async () => {
      if (timer) clearTimeout(timer);
      const original = "Copy";
      try {
        await navigator.clipboard.writeText(command);
        copyBtn.textContent = "Copied";
        copyBtn.classList.add("is-copied");
      } catch {
        const range = document.createRange();
        range.selectNodeContents(codeEl);
        const sel = window.getSelection();
        if (sel) {
          sel.removeAllRanges();
          sel.addRange(range);
        }
        copyBtn.textContent = "Selected";
      }
      timer = setTimeout(() => {
        copyBtn.textContent = original;
        copyBtn.classList.remove("is-copied");
      }, 2000);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSignInPage);
  } else {
    initSignInPage();
  }
})();
