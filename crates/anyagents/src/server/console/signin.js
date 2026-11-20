"use strict";

(() => {
  function initSignInPage() {
    const codeEl = document.getElementById("cli-login-cmd");
    const copyBtn = document.getElementById("copy-cli-btn");
    if (!codeEl) return;

    const configured = (codeEl.getAttribute("data-coordinator-url") || "").trim();
    const resolvedUrl = configured || window.location.origin;
    const command = `anyagents auth login --url ${resolvedUrl}`;
    codeEl.textContent = command;

    if (!copyBtn) return;
    copyBtn.addEventListener("click", async () => {
      const original = copyBtn.textContent;
      try {
        await navigator.clipboard.writeText(command);
        copyBtn.textContent = "Copied";
        setTimeout(() => {
          copyBtn.textContent = original;
        }, 2000);
      } catch {
        const range = document.createRange();
        range.selectNodeContents(codeEl);
        const sel = window.getSelection();
        if (sel) {
          sel.removeAllRanges();
          sel.addRange(range);
        }
        copyBtn.textContent = "Selected";
        setTimeout(() => {
          copyBtn.textContent = original;
        }, 2000);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSignInPage);
  } else {
    initSignInPage();
  }
})();
