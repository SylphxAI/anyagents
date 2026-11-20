"use strict";

(() => {
  const profilesState = {
    profiles: [],
    signInAbort: null,
  };

  function bridge() {
    return window.AnyAgentsAdmin || null;
  }

  const $ = (id) => document.getElementById(id);

  function parseQuotaSegments(usageStr) {
    if (!usageStr) return [];
    const results = [];
    // Matches patterns like "5h: 72% left (resets in 2h)", "7d: 40% used", "3p: 95% left"
    const re = /\b(5h|7d|3p|daily|weekly|monthly|quota)\s*:\s*(\d{1,3})%\s*(left|used)?([^,;]*)/gi;
    let match;
    while ((match = re.exec(usageStr)) !== null) {
      const label = match[1];
      const pct = Math.max(0, Math.min(100, Number(match[2])));
      const mode = (match[3] || "left").toLowerCase();
      const leftPct = mode === "used" ? 100 - pct : pct;
      const detail = (match[4] || "").trim().replace(/^[(\s]+|[)\s]+$/g, "");
      results.push({ label, pct, mode, leftPct, detail });
    }
    return results;
  }

  async function loadProfiles(fresh) {
    const ui = bridge();
    const grid = $("profiles-grid");
    if (!ui || !grid) return;
    try {
      const url = fresh ? "/v1/profiles?fresh=1" : "/v1/profiles";
      const list = await ui.api(url);
      profilesState.profiles = Array.isArray(list) ? list : [];
      renderProfiles();
      if (fresh) ui.toast("Refreshed live provider quota pool", "success");
    } catch (err) {
      ui.clearNode(grid);
      grid.appendChild(
        ui.el("div", { class: "empty-state", text: `Failed to load profiles: ${err.message}` })
      );
    }
  }

  function renderProfiles() {
    const ui = bridge();
    const grid = $("profiles-grid");
    if (!ui || !grid) return;
    ui.clearNode(grid);
    if (profilesState.profiles.length === 0) {
      $("profiles-age").textContent = "";
      grid.appendChild(
        ui.el("div", {
          class: "empty-state",
          text: "No provider profiles configured yet. Click + Add Profile to add an API key or run a native subscription login.",
        })
      );
      return;
    }

    const firstAge = profilesState.profiles[0].usage_age_ms;
    $("profiles-age").textContent =
      firstAge !== undefined && firstAge !== null
        ? `snapshot age: ${ui.fmtRelativeMs(firstAge)}`
        : "";

    for (const p of profilesState.profiles) {
      const card = ui.el("article", { class: "profile-card" });
      card.appendChild(
        ui.el(
          "div",
          { class: "profile-card-head" },
          ui.el(
            "div",
            {},
            ui.el("div", { class: "profile-title mono", text: `${p.provider}/${p.profile}` }),
            ui.el("div", {
              class: "muted text-sm",
              text: p.account ? `Account: ${p.account}` : "No account label",
            })
          ),
          ui.el(
            "div",
            { class: "badge-row" },
            ui.pill(p.kind || "unknown", p.kind === "subscription" ? "purple" : "accent")
          )
        )
      );

      const segments = parseQuotaSegments(p.usage);
      if (segments.length > 0) {
        const bars = ui.el("div", { class: "quota-bars" });
        for (const seg of segments) {
          const toneClass = seg.leftPct <= 15 ? "crit" : seg.leftPct <= 35 ? "warn" : "";
          const fill = ui.el("div", { class: `quota-fill ${toneClass}` });
          fill.style.width = `${seg.leftPct}%`;
          bars.appendChild(
            ui.el(
              "div",
              {},
              ui.el(
                "div",
                { class: "quota-row-head mono" },
                ui.el("span", { text: `${seg.label}: ${seg.leftPct}% left` }),
                ui.el("span", { class: "muted", text: seg.detail || "" })
              ),
              ui.el("div", { class: "quota-track" }, fill)
            )
          );
        }
        card.appendChild(bars);
      }

      if (p.usage) {
        card.appendChild(ui.el("div", { class: "text-sm mono muted", text: p.usage }));
      }

      const assigned =
        p.assigned_agents && p.assigned_agents.length > 0
          ? p.assigned_agents.join(", ")
          : "unassigned";
      const harnesses = (p.compatible_harnesses || []).join(", ") || "-";

      card.appendChild(
        ui.el(
          "div",
          { class: "text-sm muted" },
          ui.el("div", { text: `Harnesses: ${harnesses}` }),
          ui.el("div", {
            text: `Assigned roots: ${assigned} • Subagents: ${p.subagent_count || 0}`,
          })
        )
      );

      grid.appendChild(card);
    }
  }

  function switchProfileModalTab(tab) {
    document.querySelectorAll(".modal-tab").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-profile-tab") === tab);
    });
    $("form-profile-apikey").classList.toggle("hidden", tab !== "apikey");
    $("panel-profile-native").classList.toggle("hidden", tab !== "native");
  }

  function cancelNativeSignIn() {
    if (profilesState.signInAbort) {
      profilesState.signInAbort.abort();
      profilesState.signInAbort = null;
    }
    const startBtn = $("native-start-btn");
    if (startBtn) startBtn.disabled = false;
  }

  function initProfileActions() {
    const refreshBtn = $("profiles-refresh-btn");
    if (refreshBtn) refreshBtn.addEventListener("click", () => loadProfiles(false));
    const freshBtn = $("profiles-fresh-btn");
    if (freshBtn) freshBtn.addEventListener("click", () => loadProfiles(true));

    const addBtn = $("add-profile-btn");
    if (addBtn) {
      addBtn.addEventListener("click", () => {
        const ui = bridge();
        if (!ui) return;
        $("form-profile-apikey").reset();
        $("apikey-custom-provider-wrap").classList.add("hidden");
        $("native-signin-stage").classList.add("hidden");
        ui.clearNode($("native-links"));
        ui.clearNode($("native-codes"));
        $("native-status").textContent = "";
        $("native-log").textContent = "";
        switchProfileModalTab("apikey");
        ui.openModal("modal-profile");
      });
    }

    document.querySelectorAll(".modal-tab").forEach((btn) => {
      btn.addEventListener("click", () => {
        switchProfileModalTab(btn.getAttribute("data-profile-tab"));
      });
    });

    const presetSel = $("apikey-provider-preset");
    if (presetSel) {
      presetSel.addEventListener("change", (evt) => {
        const isCustom = evt.target.value === "custom";
        $("apikey-custom-provider-wrap").classList.toggle("hidden", !isCustom);
        $("apikey-custom-provider").required = isCustom;
      });
    }

    const apikeyForm = $("form-profile-apikey");
    if (apikeyForm) {
      apikeyForm.addEventListener("submit", async (evt) => {
        evt.preventDefault();
        const ui = bridge();
        if (!ui) return;
        const preset = $("apikey-provider-preset").value;
        const provider = preset === "custom" ? $("apikey-custom-provider").value.trim() : preset;
        const profile = $("apikey-profile-name").value.trim();
        const key = $("apikey-secret").value.trim();
        $("apikey-secret").value = "";
        if (!provider || !profile || !key) {
          ui.toast("Provider, profile name, and API key are required", "error");
          return;
        }
        try {
          await ui.api("/v1/profiles", {
            method: "POST",
            body: { profile: `${provider}/${profile}`, key },
          });
          ui.closeModal("modal-profile");
          ui.toast(`Saved profile ${provider}/${profile}`, "success");
          await loadProfiles(true);
        } catch (err) {
          ui.toast(`Save profile failed: ${err.message}`, "error");
        }
      });
    }

    const cancelBtn = $("native-cancel-btn");
    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => {
        const ui = bridge();
        cancelNativeSignIn();
        if (ui) ui.closeModal("modal-profile");
      });
    }

    const nativeForm = $("form-profile-native");
    if (nativeForm) {
      nativeForm.addEventListener("submit", async (evt) => {
        evt.preventDefault();
        const ui = bridge();
        if (!ui) return;
        cancelNativeSignIn();

        const harness = $("native-harness").value;
        const profile = $("native-profile-name").value.trim();
        if (!profile) {
          ui.toast("Profile name is required", "error");
          return;
        }

        const stage = $("native-signin-stage");
        const linksEl = $("native-links");
        const codesEl = $("native-codes");
        const statusEl = $("native-status");
        const logEl = $("native-log");
        stage.classList.remove("hidden");
        ui.clearNode(linksEl);
        ui.clearNode(codesEl);
        logEl.textContent = "";
        statusEl.textContent = `Starting native ${harness} sign-in for profile "${profile}"...`;

        const controller = new AbortController();
        profilesState.signInAbort = controller;
        $("native-start-btn").disabled = true;

        try {
          const resp = await fetch("/v1/profiles/native-signin", {
            method: "POST",
            credentials: "same-origin",
            headers: {
              [ui.CSRF_HEADER]: "1",
              "content-type": "application/json",
              accept: "application/x-ndjson",
            },
            body: JSON.stringify({ harness, profile }),
            signal: controller.signal,
          });

          if (resp.status === 401) {
            ui.handleUnauthorized();
            throw new Error("Unauthorized");
          }
          if (!resp.ok) {
            const text = await resp.text();
            let msg = `HTTP ${resp.status}`;
            try {
              const parsed = JSON.parse(text);
              if (parsed && parsed.error) msg = parsed.error;
            } catch {
              // use status
            }
            throw new Error(msg);
          }

          const reader = resp.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";

          const handleLine = (rawLine) => {
            const trimmed = rawLine.trim();
            if (!trimmed) return;
            let msg;
            try {
              msg = JSON.parse(trimmed);
            } catch {
              logEl.textContent += `${trimmed}\n`;
              return;
            }
            if (msg.kind === "link" && ui.isSafeExternalLink(msg.text)) {
              linksEl.appendChild(
                ui.el(
                  "div",
                  { class: "signin-link-box" },
                  ui.el("span", { class: "text-sm", text: "Open sign-in URL in your browser:" }),
                  ui.el("a", {
                    class: "btn btn-primary btn-sm",
                    href: msg.text,
                    target: "_blank",
                    rel: "noopener noreferrer",
                    text: "Open Login Page",
                  })
                )
              );
              logEl.textContent += `[link] ${msg.text}\n`;
            } else if (msg.kind === "code" && msg.text) {
              const codeVal = String(msg.text);
              codesEl.appendChild(
                ui.el(
                  "div",
                  { class: "signin-code-box" },
                  ui.el(
                    "div",
                    {},
                    ui.el("div", { class: "text-sm muted", text: "Device Code" }),
                    ui.el("div", { class: "signin-code-value", text: codeVal })
                  ),
                  ui.el("button", {
                    class: "btn btn-secondary btn-sm",
                    type: "button",
                    text: "Copy Code",
                    onClick: async () => {
                      try {
                        await navigator.clipboard.writeText(codeVal);
                        ui.toast("Copied device code", "success");
                      } catch {
                        ui.toast("Copy manually from the highlighted box", "error");
                      }
                    },
                  })
                )
              );
              logEl.textContent += `[code] ${codeVal}\n`;
            } else if (msg.kind === "prompt") {
              logEl.textContent += `${msg.text}\n`;
            } else if (msg.kind === "done") {
              statusEl.textContent = `Signed in successfully${msg.text ? ` (${msg.text})` : ""}.`;
              ui.toast(`Signed in ${harness} profile "${profile}"`, "success");
              loadProfiles(true);
            } else if (msg.kind === "error") {
              statusEl.textContent = `Sign-in error: ${msg.text}`;
              ui.toast(`Sign-in error: ${msg.text}`, "error");
            }
            logEl.scrollTop = logEl.scrollHeight;
          };

          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let idx;
            while ((idx = buffer.indexOf("\n")) >= 0) {
              const line = buffer.slice(0, idx);
              buffer = buffer.slice(idx + 1);
              handleLine(line);
            }
          }
          if (buffer.trim()) {
            handleLine(buffer);
          }
        } catch (err) {
          if (err.name !== "AbortError") {
            statusEl.textContent = `Sign-in failed: ${err.message}`;
            ui.toast(`Sign-in failed: ${err.message}`, "error");
          }
        } finally {
          profilesState.signInAbort = null;
          $("native-start-btn").disabled = false;
        }
      });
    }

    window.addEventListener("anyagents:tab", (evt) => {
      if (evt.detail === "profiles") {
        loadProfiles(false);
      }
    });

    window.addEventListener("anyagents:modal-close", (evt) => {
      if (evt.detail === "modal-profile") {
        cancelNativeSignIn();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initProfileActions);
  } else {
    initProfileActions();
  }
})();
