"use strict";

(() => {
  const CSRF_HEADER = "x-anyagents-admin-csrf";

  const state = {
    auth: null,
    activeTab: "fleet",
    agents: [],
    agentsMeta: {},
    overview: null,
    selectedAgentName: null,
    collapsedNodes: new Set(),
    convTimer: null,
    profiles: [],
    signInAbort: null,
  };

  // ---------- DOM helpers (strictly textContent-safe, zero innerHTML XSS) ----------

  const $ = (id) => document.getElementById(id);

  function el(tag, attrs = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, val] of Object.entries(attrs)) {
      if (val === undefined || val === null || val === false) continue;
      if (key === "class") {
        node.className = val;
      } else if (key === "text") {
        node.textContent = String(val);
      } else if (key.startsWith("on") && typeof val === "function") {
        node.addEventListener(key.slice(2).toLowerCase(), val);
      } else if (key === "disabled" || key === "checked" || key === "selected") {
        node[key] = Boolean(val);
      } else {
        node.setAttribute(key, String(val));
      }
    }
    for (const child of children) {
      if (child === undefined || child === null || child === false) continue;
      if (typeof child === "string" || typeof child === "number") {
        node.appendChild(document.createTextNode(String(child)));
      } else {
        node.appendChild(child);
      }
    }
    return node;
  }

  function clearNode(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function pill(text, tone = "muted") {
    return el("span", { class: `pill pill-${tone}`, text });
  }

  function toast(message, kind = "info") {
    const container = $("toast-container");
    const item = el("div", {
      class: `toast ${kind === "error" ? "toast-error" : kind === "success" ? "toast-success" : ""}`,
      text: message,
    });
    container.appendChild(item);
    setTimeout(() => item.remove(), 4500);
  }

  // ---------- Formatting helpers ----------

  function fmtNumber(n) {
    if (n === undefined || n === null) return "-";
    return Number(n).toLocaleString();
  }

  function fmtUsdFromMicros(micros) {
    if (micros === undefined || micros === null) return "-";
    const usd = Number(micros) / 1_000_000;
    return `$${usd.toFixed(usd >= 1 ? 2 : 4)}`;
  }

  function fmtTime(ms) {
    if (!ms) return "-";
    const d = new Date(ms);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleString();
  }

  function fmtRelativeMs(ms) {
    if (ms === undefined || ms === null) return "-";
    const sec = Math.max(0, Math.round(ms / 1000));
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ${sec % 60}s`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ${min % 60}m`;
    const days = Math.floor(hr / 24);
    return `${days}d ${hr % 24}h`;
  }

  function stateTone(s) {
    switch (s) {
      case "running":
        return "success";
      case "paused":
        return "warning";
      case "faulted":
        return "danger";
      case "archived":
        return "muted";
      default:
        return "accent";
    }
  }

  function isSafeExternalLink(url) {
    return typeof url === "string" && (url.startsWith("https://") || url.startsWith("http://localhost"));
  }

  // ---------- HTTP helper ----------

  async function api(path, options = {}) {
    const headers = Object.assign(
      {
        accept: "application/json",
        [CSRF_HEADER]: "1",
      },
      options.headers || {}
    );
    const fetchOpts = {
      method: options.method || "GET",
      credentials: "same-origin",
      headers,
      signal: options.signal,
    };
    if (options.body !== undefined) {
      headers["content-type"] = "application/json";
      fetchOpts.body = JSON.stringify(options.body);
    }
    const resp = await fetch(path, fetchOpts);
    if (resp.status === 401) {
      handleUnauthorized();
      throw new Error("Session expired or unauthorized");
    }
    const text = await resp.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }
    if (!resp.ok) {
      const msg = (data && (data.error || data.message)) || `HTTP ${resp.status}`;
      throw new Error(msg);
    }
    return data;
  }

  function handleUnauthorized() {
    stopConversationAutoRefresh();
    if (state.auth && state.auth.mode === "oidc") {
      state.auth.authenticated = false;
      renderAuthState();
    }
  }

  // ---------- Theme ----------

  function initTheme() {
    const saved = localStorage.getItem("anyagents_admin_theme");
    if (saved === "light" || saved === "dark") {
      document.documentElement.setAttribute("data-theme", saved);
    }
    $("theme-toggle").addEventListener("click", () => {
      const current = document.documentElement.getAttribute("data-theme");
      const prefersLight = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches;
      const effective = current || (prefersLight ? "light" : "dark");
      const next = effective === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("anyagents_admin_theme", next);
    });
  }

  // ---------- Modals ----------

  function openModal(id) {
    $(id).classList.remove("hidden");
  }

  function closeModal(id) {
    $(id).classList.add("hidden");
  }

  function initModals() {
    document.querySelectorAll(".modal-close").forEach((btn) => {
      btn.addEventListener("click", () => closeModal(btn.getAttribute("data-close")));
    });
    document.querySelectorAll(".modal-backdrop").forEach((backdrop) => {
      backdrop.addEventListener("click", (evt) => {
        if (evt.target === backdrop) {
          if (backdrop.id === "modal-profile") {
            cancelNativeSignIn();
          }
          backdrop.classList.add("hidden");
        }
      });
    });
  }

  // ---------- Auth boot ----------

  async function checkAuth() {
    try {
      const resp = await fetch("/admin/auth/me", { credentials: "same-origin" });
      state.auth = await resp.json();
    } catch (err) {
      state.auth = { authenticated: false, mode: "unconfigured", serverless: false };
      toast(`Failed to check auth: ${err.message}`, "error");
    }
    renderAuthState();
  }

  function renderAuthState() {
    const auth = state.auth || { authenticated: false, mode: "unconfigured" };
    const envBadge = $("env-badge");
    const authBadge = $("auth-badge");
    const logoutBtn = $("logout-btn");
    const nav = $("main-nav");
    const gate = $("auth-gate");
    const main = $("console-main");

    envBadge.classList.remove("hidden");
    envBadge.textContent = auth.serverless ? "Serverless" : "Local";
    envBadge.className = `pill ${auth.serverless ? "pill-purple" : "pill-muted"}`;

    if (auth.authenticated) {
      gate.classList.add("hidden");
      main.classList.remove("hidden");
      nav.classList.remove("hidden");
      if (auth.mode === "oidc") {
        authBadge.textContent = auth.email || auth.name || "OIDC Operator";
        authBadge.className = "pill pill-success";
        logoutBtn.classList.remove("hidden");
      } else {
        authBadge.textContent = "Local Trusted";
        authBadge.className = "pill pill-warning";
        logoutBtn.classList.add("hidden");
      }
      loadActiveTab();
    } else {
      main.classList.add("hidden");
      nav.classList.add("hidden");
      gate.classList.remove("hidden");
      logoutBtn.classList.add("hidden");
      authBadge.textContent = auth.mode === "oidc" ? "Signed out" : "Auth unconfigured";
      authBadge.className = "pill pill-danger";

      const msgEl = $("auth-gate-message");
      const actionsEl = $("auth-gate-actions");
      clearNode(actionsEl);

      if (auth.mode === "oidc") {
        msgEl.textContent =
          "Sign in with your configured OpenID Connect identity provider to manage agents, profiles, and telemetry.";
        actionsEl.appendChild(
          el("a", {
            class: "btn btn-primary",
            href: "/admin/auth/login",
            text: "Sign in with OIDC",
          })
        );
      } else {
        msgEl.textContent =
          "Admin OIDC authentication is not configured for this confined/serverless coordinator. Set ANYAGENTS_ADMIN_OIDC_ISSUER, ANYAGENTS_ADMIN_OIDC_CLIENT_ID, and ANYAGENTS_ADMIN_EMAILS to enable web console sign-in.";
        actionsEl.appendChild(
          el("button", {
            class: "btn btn-secondary",
            type: "button",
            text: "Re-check Status",
            onClick: () => checkAuth(),
          })
        );
      }
    }
  }

  async function doLogout() {
    try {
      await fetch("/admin/auth/logout", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          [CSRF_HEADER]: "1",
          "x-requested-with": "fetch",
        },
      });
    } catch {
      // Ignore network errors and re-check auth state.
    }
    stopConversationAutoRefresh();
    await checkAuth();
  }

  // ---------- Navigation ----------

  function switchTab(tab) {
    state.activeTab = tab;
    document.querySelectorAll(".nav-tab").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === tab);
    });
    document.querySelectorAll(".tab-panel").forEach((panel) => {
      panel.classList.toggle("active", panel.id === `tab-${tab}`);
    });
    if (tab !== "fleet") {
      stopConversationAutoRefresh();
    } else if (state.selectedAgentName && $("conv-autorefresh").checked) {
      startConversationAutoRefresh();
    }
    loadActiveTab();
  }

  function loadActiveTab() {
    const loaders = {
      fleet: loadFleet,
      profiles: () => loadProfiles(false),
      telemetry: loadTelemetry,
      automations: loadAutomations,
      channels: loadChannelsAndGroups,
    };
    if (loaders[state.activeTab]) loaders[state.activeTab]();
    window.dispatchEvent(new CustomEvent("anyagents:tab", { detail: state.activeTab }));
  }

  // ---------- Tab 1: Fleet Tree & Inspector ----------

  async function loadFleet() {
    try {
      const [agentsResp, overview] = await Promise.all([
        api("/v1/agents?state=all"),
        api("/admin/api/overview"),
      ]);
      state.agents = (agentsResp && agentsResp.agents) || [];
      state.overview = overview || {};
      state.agentsMeta = (overview && overview.agents_meta) || {};
      renderFleetTree();
      populateScheduleAgentSelect();
      if (state.selectedAgentName) {
        const stillExists = state.agents.find((a) => a.name === state.selectedAgentName);
        if (stillExists) {
          renderInspector(stillExists);
        } else {
          state.selectedAgentName = null;
          renderInspector(null);
        }
      }
    } catch (err) {
      toast(`Failed to load fleet: ${err.message}`, "error");
    }
  }

  function displayState(agent) {
    if (agent.state === "paused" && agent.turn_errors && agent.turn_errors.faulted_at_ms) {
      return "faulted";
    }
    return agent.state;
  }

  function filterAgents(agents) {
    const query = ($("fleet-search").value || "").trim().toLowerCase();
    const stateFilter = $("fleet-state-filter").value;
    return agents.filter((a) => {
      if (stateFilter === "active" && a.state === "archived") return false;
      if (stateFilter === "running" && a.state !== "running") return false;
      if (stateFilter === "paused" && a.state !== "paused") return false;
      if (stateFilter === "archived" && a.state !== "archived") return false;
      if (!query) return true;
      const hay = `${a.name} ${a.id} ${a.role || ""} ${a.harness} ${a.model || ""} ${a.profile || ""}`.toLowerCase();
      return hay.includes(query);
    });
  }

  function renderFleetTree() {
    const container = $("fleet-tree");
    clearNode(container);

    const filtered = filterAgents(state.agents);
    $("fleet-count").textContent = `${filtered.length} / ${state.agents.length} agents`;

    if (filtered.length === 0) {
      container.appendChild(el("div", { class: "empty-state", text: "No agents match the current filter." }));
      return;
    }

    const byId = new Map();
    const childrenByParent = new Map();
    for (const agent of filtered) {
      byId.set(agent.id, agent);
    }
    const roots = [];
    for (const agent of filtered) {
      if (agent.parent && byId.has(agent.parent)) {
        if (!childrenByParent.has(agent.parent)) {
          childrenByParent.set(agent.parent, []);
        }
        childrenByParent.get(agent.parent).push(agent);
      } else {
        roots.push(agent);
      }
    }

    const sortFn = (a, b) => a.name.localeCompare(b.name);
    roots.sort(sortFn);
    for (const list of childrenByParent.values()) {
      list.sort(sortFn);
    }

    function buildNode(agent) {
      const kids = childrenByParent.get(agent.id) || [];
      const meta = state.agentsMeta[agent.id] || {};
      const collapsed = state.collapsedNodes.has(agent.id);
      const st = displayState(agent);

      const toggleBtn = el("button", {
        class: "tree-toggle",
        type: "button",
        text: kids.length > 0 ? (collapsed ? "▸" : "▾") : "•",
        onClick: (evt) => {
          evt.stopPropagation();
          if (kids.length === 0) return;
          if (state.collapsedNodes.has(agent.id)) {
            state.collapsedNodes.delete(agent.id);
          } else {
            state.collapsedNodes.add(agent.id);
          }
          renderFleetTree();
        },
      });

      const badges = el("div", { class: "badge-row" });
      badges.appendChild(pill(st, stateTone(st)));
      if (agent.has_open_turn) badges.appendChild(pill("OPEN TURN", "accent"));
      if (meta.unread_count > 0) badges.appendChild(pill(`${meta.unread_count} unread`, "warning"));
      if (meta.sandbox_lease && meta.sandbox_lease.lease_state) {
        const leaseTone = meta.sandbox_lease.lease_state === "ready" ? "success" : "muted";
        badges.appendChild(
          pill(`${meta.sandbox_lease.provider}:${meta.sandbox_lease.lease_state}`, leaseTone)
        );
      }

      const metaLine = el(
        "div",
        { class: "tree-meta mono" },
        el("span", { text: agent.harness }),
        agent.model ? el("span", { text: `/ ${agent.model}` }) : null,
        agent.profile ? el("span", { text: `[${agent.profile}]` }) : null,
        agent.turn_state ? el("span", { text: `turn:${agent.turn_state}` }) : null
      );

      const row = el(
        "div",
        {
          class: `tree-row ${state.selectedAgentName === agent.name ? "selected" : ""}`,
          onClick: () => selectAgent(agent),
        },
        toggleBtn,
        el(
          "div",
          { class: "tree-row-body" },
          el(
            "div",
            { class: "tree-row-top" },
            el("span", { class: "tree-name", text: agent.name }),
            badges
          ),
          metaLine
        )
      );

      const wrapper = el("div", { class: "tree-node" }, row);
      if (kids.length > 0 && !collapsed) {
        const kidsWrap = el("div", { class: "tree-children" });
        for (const kid of kids) {
          kidsWrap.appendChild(buildNode(kid));
        }
        wrapper.appendChild(kidsWrap);
      }
      return wrapper;
    }

    for (const root of roots) {
      container.appendChild(buildNode(root));
    }
  }

  function selectAgent(agent) {
    state.selectedAgentName = agent.name;
    renderFleetTree();
    renderInspector(agent);
    loadConversation(agent.name);
    if ($("conv-autorefresh").checked) {
      startConversationAutoRefresh();
    }
  }

  function renderInspector(agent) {
    const empty = $("inspector-empty");
    const content = $("inspector-content");
    if (!agent) {
      empty.classList.remove("hidden");
      content.classList.add("hidden");
      stopConversationAutoRefresh();
      return;
    }
    empty.classList.add("hidden");
    content.classList.remove("hidden");

    const meta = state.agentsMeta[agent.id] || {};
    const st = displayState(agent);
    $("insp-name").textContent = agent.name;
    $("insp-subtitle").textContent = `${agent.id} • scope:${agent.scope || "-"} • lifecycle:${agent.lifecycle || "-"}${agent.role ? ` • role:${agent.role}` : ""}`;

    const badgesEl = $("insp-badges");
    clearNode(badgesEl);
    badgesEl.appendChild(pill(st, stateTone(st)));
    if (agent.runtime) badgesEl.appendChild(pill(`${agent.harness}@${agent.runtime}`, agent.runtime === "cloud" ? "accent" : "muted"));
    if (agent.turn_state) badgesEl.appendChild(pill(`turn:${agent.turn_state}`, "muted"));
    if (agent.has_open_turn) badgesEl.appendChild(pill(agent.open_turn_age_ms ? `open turn (${fmtRelativeMs(agent.open_turn_age_ms)})` : "open turn", "accent"));
    if (meta.unread_count > 0) badgesEl.appendChild(pill(`${meta.unread_count} unread`, "warning"));
    if (meta.sandbox_lease && meta.sandbox_lease.lease_state) {
      badgesEl.appendChild(pill(`sandbox ${meta.sandbox_lease.provider}:${meta.sandbox_lease.lease_state}`, meta.sandbox_lease.lease_state === "ready" ? "success" : "muted"));
    }

    // Button visibility & state
    $("btn-pause-agent").disabled = agent.state === "paused" || agent.state === "archived";
    $("btn-wake-agent").disabled = agent.state === "archived";
    $("btn-archive-agent").disabled = agent.state === "archived";

    const errBanner = $("insp-error-banner");
    const errText = agent.last_error || (agent.turn_errors && agent.turn_errors.last_error);
    if (errText) {
      errBanner.textContent = `Last error: ${errText}`;
      errBanner.classList.remove("hidden");
    } else {
      errBanner.classList.add("hidden");
    }

    const metaGrid = $("insp-meta-grid");
    clearNode(metaGrid);
    const parentAgent = agent.parent ? state.agents.find((a) => a.id === agent.parent) : null;
    const lease = meta.sandbox_lease;
    const fields = [
      ["Harness", agent.runtime ? `${agent.harness} (${agent.runtime})` : agent.harness],
      ["Model", agent.model || "(default)"],
      ...(agent.remote_budget_micros ? [["Budget Cap", `$${(agent.remote_budget_micros / 1e6).toFixed(2)}`]] : []),
      ["Profile", agent.profile || "(auto)"],
      ["Parent", parentAgent ? parentAgent.name : agent.parent || "(root)"],
      ["Process", agent.process || "-"],
      ["Unread Mail", String(meta.unread_count || 0)],
      [
        "Sandbox Lease",
        lease
          ? `${lease.provider} / ${lease.lease_state}${lease.lease_id ? ` (${lease.lease_id})` : ""}`
          : "none",
      ],
      ["Workdir", agent.workdir || "-"],
      ["Automations", `${(agent.automations && agent.automations.total) || 0} active`],
      ["Description", agent.description || "-"],
    ];
    for (const [label, val] of fields) {
      metaGrid.appendChild(
        el(
          "div",
          { class: "meta-item" },
          el("div", { class: "meta-label", text: label }),
          el("div", { class: "meta-value mono", text: val })
        )
      );
    }
    window.dispatchEvent(new CustomEvent("anyagents:agent", { detail: agent }));
  }

  async function loadConversation(agentName) {
    if (!agentName) return;
    const viewer = $("conv-viewer");
    try {
      const data = await api(`/v1/agents/${encodeURIComponent(agentName)}/conversation?limit=50`);
      const turns = (data && data.turns) || [];
      clearNode(viewer);
      if (turns.length === 0) {
        viewer.appendChild(el("div", { class: "empty-state", text: "No conversation turns yet." }));
        return;
      }
      for (const t of turns) {
        const dirClass = t.direction === "out" ? "turn-out" : "turn-in";
        const tools = t.tool_names && t.tool_names.length > 0 ? ` • tools: ${t.tool_names.join(", ")}` : "";
        const headLeft = `${t.direction === "out" ? "OUT" : "IN"} • ${t.from || "-"} → ${t.to || "-"}${t.role ? ` (${t.role})` : ""}${tools}`;
        const headRight = t.timestamp_ms ? fmtTime(t.timestamp_ms) : `#${t.seq}`;
        viewer.appendChild(
          el(
            "div",
            { class: `conv-turn ${dirClass}` },
            el(
              "div",
              { class: "conv-turn-head mono" },
              el("span", { text: headLeft }),
              el("span", { text: headRight })
            ),
            el("pre", { class: "conv-turn-body", text: t.text || "" })
          )
        );
      }
      viewer.scrollTop = viewer.scrollHeight;
    } catch (err) {
      clearNode(viewer);
      viewer.appendChild(el("div", { class: "empty-state", text: `Failed to load conversation: ${err.message}` }));
    }
  }

  function startConversationAutoRefresh() {
    stopConversationAutoRefresh();
    state.convTimer = setInterval(() => {
      if (state.activeTab === "fleet" && state.selectedAgentName) {
        loadConversation(state.selectedAgentName);
      }
    }, 5000);
  }

  function stopConversationAutoRefresh() {
    if (state.convTimer) {
      clearInterval(state.convTimer);
      state.convTimer = null;
    }
  }

  function initFleetActions() {
    $("fleet-search").addEventListener("input", renderFleetTree);
    $("fleet-state-filter").addEventListener("change", renderFleetTree);
    $("fleet-refresh-btn").addEventListener("click", loadFleet);

    $("spawn-root-btn").addEventListener("click", () => {
      $("form-spawn").reset();
      $("spawn-parent").value = "";
      openModal("modal-spawn");
    });

    $("btn-spawn-child").addEventListener("click", () => {
      $("form-spawn").reset();
      $("spawn-parent").value = state.selectedAgentName || "";
      openModal("modal-spawn");
    });

    const parseBudgetMicros = (id) => {
      const raw = $(id).value.trim();
      if (!raw) return null;
      const usd = Number(raw);
      return Number.isFinite(usd) && usd >= 0 ? Math.round(usd * 1e6) : null;
    };

    $("form-spawn").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const body = { name: $("spawn-name").value.trim(), harness: $("spawn-harness").value, runtime: $("spawn-runtime").value };
      const cap = parseBudgetMicros("spawn-budget-usd");
      if (cap !== null && cap > 0) body.budget_cap_micros = cap;
      for (const [key, id] of [["parent", "spawn-parent"], ["model", "spawn-model"], ["profile", "spawn-profile"], ["role", "spawn-role"], ["description", "spawn-description"], ["instructions", "spawn-instructions"], ["prompt", "spawn-prompt"]]) {
        const v = $(id).value.trim();
        if (v) body[key] = v;
      }
      try {
        await api("/v1/agents", { method: "POST", body });
        closeModal("modal-spawn");
        toast(`Spawned agent "${body.name}"`, "success");
        state.selectedAgentName = body.name;
        await loadFleet();
      } catch (err) {
        toast(`Spawn failed: ${err.message}`, "error");
      }
    });

    $("btn-switch-runtime").addEventListener("click", () => {
      const agent = state.agents.find((a) => a.name === state.selectedAgentName);
      if (!agent) return;
      $("runtime-agent-name").value = agent.name;
      $("runtime-harness").value = agent.harness;
      const rt = (agent.runtime || "local").split(":")[0];
      $("runtime-runtime").value = ["sandbox", "local", "cloud"].includes(rt) ? rt : "local";
      $("runtime-budget-usd").value = agent.remote_budget_micros ? (agent.remote_budget_micros / 1e6).toFixed(2) : "";
      $("runtime-model").value = agent.model || "";
      $("runtime-profile").value = agent.profile || "";
      openModal("modal-runtime");
    });

    $("form-runtime").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const name = $("runtime-agent-name").value;
      const agent = state.agents.find((a) => a.name === name);
      if (!agent) return;
      const harness = $("runtime-harness").value;
      const runtime = $("runtime-runtime").value;
      const cap = parseBudgetMicros("runtime-budget-usd");
      const clearBudget = !$("runtime-budget-usd").value.trim() && Boolean(agent.remote_budget_micros);
      const model = $("runtime-model").value.trim() || null;
      const profile = $("runtime-profile").value.trim() || null;
      const curRt = (agent.runtime || "local").split(":")[0];
      try {
        if (harness !== agent.harness || runtime !== curRt || cap !== null || clearBudget) {
          const body = { harness, runtime: runtime !== curRt ? runtime : (agent.runtime || runtime), model, profile };
          if (cap !== null) body.budget_cap_micros = cap;
          if (clearBudget) body.clear_budget = true;
          await api(`/v1/agents/${encodeURIComponent(name)}/runtime`, { method: "POST", body });
        } else {
          await api(`/v1/agents/${encodeURIComponent(name)}/model`, { method: "POST", body: { harness, model, profile } });
        }
        closeModal("modal-runtime");
        toast(`Updated runtime for "${name}"`, "success");
        await loadFleet();
      } catch (err) {
        toast(`Runtime switch failed: ${err.message}`, "error");
      }
    });

    $("btn-pause-agent").addEventListener("click", async () => {
      const name = state.selectedAgentName;
      if (!name) return;
      try {
        await api(`/v1/agents/${encodeURIComponent(name)}/pause`, { method: "POST" });
        toast(`Paused "${name}"`, "success");
        await loadFleet();
      } catch (err) {
        toast(`Pause failed: ${err.message}`, "error");
      }
    });

    $("btn-wake-agent").addEventListener("click", async () => {
      const name = state.selectedAgentName;
      if (!name) return;
      try {
        await api(`/v1/agents/${encodeURIComponent(name)}/wake`, { method: "POST" });
        toast(`Woke "${name}"`, "success");
        await loadFleet();
      } catch (err) {
        toast(`Wake failed: ${err.message}`, "error");
      }
    });

    $("btn-archive-agent").addEventListener("click", async () => {
      const name = state.selectedAgentName;
      if (!name) return;
      if (!window.confirm(`Archive agent "${name}"? Its sandbox compute will be released.`)) {
        return;
      }
      try {
        await api(`/v1/agents/${encodeURIComponent(name)}/archive`, { method: "POST" });
        toast(`Archived "${name}"`, "success");
        await loadFleet();
      } catch (err) {
        toast(`Archive failed: ${err.message}`, "error");
      }
    });

    $("send-msg-form").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const name = state.selectedAgentName;
      const text = $("send-msg-input").value.trim();
      if (!name || !text) return;
      try {
        await api("/v1/send", {
          method: "POST",
          body: {
            sender: "operator",
            to: [`agent:${name}`],
            text,
          },
        });
        $("send-msg-input").value = "";
        toast(`Message sent to "${name}"`, "success");
        await loadConversation(name);
      } catch (err) {
        toast(`Send failed: ${err.message}`, "error");
      }
    });

    $("conv-refresh-btn").addEventListener("click", () => {
      if (state.selectedAgentName) loadConversation(state.selectedAgentName);
    });

    $("conv-autorefresh").addEventListener("change", (evt) => {
      if (evt.target.checked && state.selectedAgentName) {
        startConversationAutoRefresh();
      } else {
        stopConversationAutoRefresh();
      }
    });
  }

  // ---------- Tab 2: Profiles & Live Quota Pool ----------

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
    const grid = $("profiles-grid");
    try {
      const url = fresh ? "/v1/profiles?fresh=1" : "/v1/profiles";
      const list = await api(url);
      state.profiles = Array.isArray(list) ? list : [];
      renderProfiles();
      if (fresh) toast("Refreshed live provider quota pool", "success");
    } catch (err) {
      clearNode(grid);
      grid.appendChild(el("div", { class: "empty-state", text: `Failed to load profiles: ${err.message}` }));
    }
  }

  function renderProfiles() {
    const grid = $("profiles-grid");
    clearNode(grid);
    if (state.profiles.length === 0) {
      $("profiles-age").textContent = "";
      grid.appendChild(
        el("div", {
          class: "empty-state",
          text: "No provider profiles configured yet. Click + Add Profile to add an API key or run a native subscription login.",
        })
      );
      return;
    }

    const firstAge = state.profiles[0].usage_age_ms;
    $("profiles-age").textContent =
      firstAge !== undefined && firstAge !== null ? `snapshot age: ${fmtRelativeMs(firstAge)}` : "";

    for (const p of state.profiles) {
      const card = el("article", { class: "profile-card" });
      card.appendChild(
        el(
          "div",
          { class: "profile-card-head" },
          el(
            "div",
            {},
            el("div", { class: "profile-title mono", text: `${p.provider}/${p.profile}` }),
            el("div", {
              class: "muted text-sm",
              text: p.account ? `Account: ${p.account}` : "No account label",
            })
          ),
          el(
            "div",
            { class: "badge-row" },
            pill(p.kind || "unknown", p.kind === "subscription" ? "purple" : "accent")
          )
        )
      );

      // Quota bars
      const segments = parseQuotaSegments(p.usage);
      if (segments.length > 0) {
        const bars = el("div", { class: "quota-bars" });
        for (const seg of segments) {
          const toneClass = seg.leftPct <= 15 ? "crit" : seg.leftPct <= 35 ? "warn" : "";
          const fill = el("div", { class: `quota-fill ${toneClass}` });
          fill.style.width = `${seg.leftPct}%`;
          bars.appendChild(
            el(
              "div",
              {},
              el(
                "div",
                { class: "quota-row-head mono" },
                el("span", { text: `${seg.label}: ${seg.leftPct}% left` }),
                el("span", { class: "muted", text: seg.detail || "" })
              ),
              el("div", { class: "quota-track" }, fill)
            )
          );
        }
        card.appendChild(bars);
      }

      if (p.usage) {
        card.appendChild(el("div", { class: "text-sm mono muted", text: p.usage }));
      }

      const assigned = (p.assigned_agents && p.assigned_agents.length > 0)
        ? p.assigned_agents.join(", ")
        : "unassigned";
      const harnesses = (p.compatible_harnesses || []).join(", ") || "-";

      card.appendChild(
        el(
          "div",
          { class: "text-sm muted" },
          el("div", { text: `Harnesses: ${harnesses}` }),
          el("div", {
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
    if (state.signInAbort) {
      state.signInAbort.abort();
      state.signInAbort = null;
    }
    $("native-start-btn").disabled = false;
  }

  function initProfileActions() {
    $("profiles-refresh-btn").addEventListener("click", () => loadProfiles(false));
    $("profiles-fresh-btn").addEventListener("click", () => loadProfiles(true));

    $("add-profile-btn").addEventListener("click", () => {
      $("form-profile-apikey").reset();
      $("apikey-custom-provider-wrap").classList.add("hidden");
      $("native-signin-stage").classList.add("hidden");
      clearNode($("native-links"));
      clearNode($("native-codes"));
      $("native-status").textContent = "";
      $("native-log").textContent = "";
      switchProfileModalTab("apikey");
      openModal("modal-profile");
    });

    document.querySelectorAll(".modal-tab").forEach((btn) => {
      btn.addEventListener("click", () => {
        switchProfileModalTab(btn.getAttribute("data-profile-tab"));
      });
    });

    $("apikey-provider-preset").addEventListener("change", (evt) => {
      const isCustom = evt.target.value === "custom";
      $("apikey-custom-provider-wrap").classList.toggle("hidden", !isCustom);
      $("apikey-custom-provider").required = isCustom;
    });

    $("form-profile-apikey").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const preset = $("apikey-provider-preset").value;
      const provider = preset === "custom" ? $("apikey-custom-provider").value.trim() : preset;
      const profile = $("apikey-profile-name").value.trim();
      const key = $("apikey-secret").value.trim();
      $("apikey-secret").value = "";
      if (!provider || !profile || !key) {
        toast("Provider, profile name, and API key are required", "error");
        return;
      }
      try {
        await api("/v1/profiles", {
          method: "POST",
          body: { profile: `${provider}/${profile}`, key },
        });
        closeModal("modal-profile");
        toast(`Saved profile ${provider}/${profile}`, "success");
        await loadProfiles(true);
      } catch (err) {
        toast(`Save profile failed: ${err.message}`, "error");
      }
    });

    $("native-cancel-btn").addEventListener("click", () => {
      cancelNativeSignIn();
      closeModal("modal-profile");
    });

    $("form-profile-native").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      cancelNativeSignIn();

      const harness = $("native-harness").value;
      const profile = $("native-profile-name").value.trim();
      if (!profile) {
        toast("Profile name is required", "error");
        return;
      }

      const stage = $("native-signin-stage");
      const linksEl = $("native-links");
      const codesEl = $("native-codes");
      const statusEl = $("native-status");
      const logEl = $("native-log");
      stage.classList.remove("hidden");
      clearNode(linksEl);
      clearNode(codesEl);
      logEl.textContent = "";
      statusEl.textContent = `Starting native ${harness} sign-in for profile "${profile}"...`;

      const controller = new AbortController();
      state.signInAbort = controller;
      $("native-start-btn").disabled = true;

      try {
        const resp = await fetch("/v1/profiles/native-signin", {
          method: "POST",
          credentials: "same-origin",
          headers: {
            [CSRF_HEADER]: "1",
            "content-type": "application/json",
            accept: "application/x-ndjson",
          },
          body: JSON.stringify({ harness, profile }),
          signal: controller.signal,
        });

        if (resp.status === 401) {
          handleUnauthorized();
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
          if (msg.kind === "link" && isSafeExternalLink(msg.text)) {
            linksEl.appendChild(
              el(
                "div",
                { class: "signin-link-box" },
                el("span", { class: "text-sm", text: "Open sign-in URL in your browser:" }),
                el("a", {
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
              el(
                "div",
                { class: "signin-code-box" },
                el(
                  "div",
                  {},
                  el("div", { class: "text-sm muted", text: "Device Code" }),
                  el("div", { class: "signin-code-value", text: codeVal })
                ),
                el("button", {
                  class: "btn btn-secondary btn-sm",
                  type: "button",
                  text: "Copy Code",
                  onClick: async () => {
                    try {
                      await navigator.clipboard.writeText(codeVal);
                      toast("Copied device code", "success");
                    } catch {
                      toast("Copy manually from the highlighted box", "error");
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
            toast(`Signed in ${harness} profile "${profile}"`, "success");
            loadProfiles(true);
          } else if (msg.kind === "error") {
            statusEl.textContent = `Sign-in error: ${msg.text}`;
            toast(`Sign-in error: ${msg.text}`, "error");
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
          toast(`Sign-in failed: ${err.message}`, "error");
        }
      } finally {
        state.signInAbort = null;
        $("native-start-btn").disabled = false;
      }
    });
  }

  // ---------- Tab 3: Usage Telemetry ----------

  async function loadTelemetry() {
    const days = Number($("telemetry-days").value) || 7;
    try {
      const [usageByAgent, usageByModel] = await Promise.all([
        api(`/v1/usage?days=${days}&group_by=agent`),
        api(`/v1/usage?days=${days}&group_by=model`),
      ]);
      renderTelemetry(usageByAgent, usageByModel, days);
    } catch (err) {
      toast(`Failed to load telemetry: ${err.message}`, "error");
    }
  }

  function renderTelemetry(usageAgent, usageModel, days) {
    const totals = (usageAgent && usageAgent.totals) || {};

    const cards = $("telemetry-cards");
    clearNode(cards);
    const kpis = [
      ["Total Turns", fmtNumber(totals.turns || 0), `Window: ${days}d`],
      ["Input Tokens", fmtNumber(totals.input_tokens || 0), `Cache read: ${fmtNumber(totals.cache_read_tokens || 0)}`],
      ["Output Tokens", fmtNumber(totals.output_tokens || 0), `Cache write: ${fmtNumber(totals.cache_write_tokens || 0)}`],
      ["Reported Cost", fmtUsdFromMicros(totals.reported_cost_micros), "API billed turns"],
    ];
    for (const [label, value, sub] of kpis) {
      cards.appendChild(
        el(
          "div",
          { class: "kpi-card" },
          el("div", { class: "kpi-label", text: label }),
          el("div", { class: "kpi-value mono", text: value }),
          el("div", { class: "kpi-sub", text: sub })
        )
      );
    }

    // Usage by Agent table
    renderUsageTable($("usage-by-agent-table"), (usageAgent && usageAgent.buckets) || [], "agent");
    // Usage by Model table
    renderUsageTable($("usage-by-model-table"), (usageModel && usageModel.buckets) || [], "model");
  }

  function renderUsageTable(container, buckets, mode) {
    clearNode(container);
    if (buckets.length === 0) {
      container.appendChild(el("div", { class: "empty-state", text: "No usage records in this window." }));
      return;
    }
    const tbl = el(
      "table",
      {},
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          el("th", { text: mode === "agent" ? "Agent" : "Agent / Model" }),
          el("th", { text: "Turns" }),
          el("th", { text: "Input" }),
          el("th", { text: "Output" }),
          el("th", { text: "Cache Read" }),
          el("th", { text: "Cost" })
        )
      ),
      el(
        "tbody",
        {},
        ...buckets.map((b) => {
          const label =
            mode === "agent"
              ? b.agent_name || b.agent_id
              : `${b.agent_name || b.agent_id} • ${b.model || "default"}`;
          return el(
            "tr",
            {},
            el("td", { class: "mono", text: label }),
            el("td", { class: "mono", text: fmtNumber(b.turns) }),
            el("td", { class: "mono", text: fmtNumber(b.input_tokens) }),
            el("td", { class: "mono", text: fmtNumber(b.output_tokens) }),
            el("td", { class: "mono", text: fmtNumber(b.cache_read_tokens) }),
            el("td", { class: "mono", text: fmtUsdFromMicros(b.reported_cost_micros) })
          );
        })
      )
    );
    container.appendChild(tbl);
  }

  function initTelemetryActions() {
    $("telemetry-days").addEventListener("change", loadTelemetry);
    $("telemetry-refresh-btn").addEventListener("click", loadTelemetry);
  }

  // ---------- Tab 4: Automations, Outbound Queue & Ticks ----------

  function populateScheduleAgentSelect() {
    const sel = $("schedules-agent-select");
    const prev = sel.value || "all";
    clearNode(sel);
    sel.appendChild(el("option", { value: "all", text: "All agents" }));
    const sorted = [...state.agents].filter((a) => a.state !== "archived").sort((a, b) => a.name.localeCompare(b.name));
    for (const a of sorted) {
      sel.appendChild(el("option", { value: a.name, text: a.name }));
    }
    if ([...sel.options].some((o) => o.value === prev)) {
      sel.value = prev;
    }
  }

  async function loadAutomations() {
    const target = $("schedules-agent-select").value || "all";
    try {
      const [overview, schedResp, outboundResp] = await Promise.all([
        api("/admin/api/overview"),
        api(`/v1/agents/${encodeURIComponent(target)}/schedules`),
        api("/v1/outbound"),
      ]);
      state.overview = overview || {};
      renderTickStatus(overview || {});
      renderSchedules((schedResp && schedResp.schedules) || []);
      renderOutbound((outboundResp && outboundResp.unsettled) || []);
    } catch (err) {
      toast(`Failed to load automations: ${err.message}`, "error");
    }
  }

  function renderTickStatus(overview) {
    const authText = [];
    if (overview.tick_token_configured) authText.push("ANYAGENTS_TICK_TOKEN");
    if (overview.tick_jwks_configured) authText.push("ANYAGENTS_TICK_JWKS_URL");
    $("tick-auth-status").textContent =
      authText.length > 0 ? `Tick auth: ${authText.join(" + ")}` : "External /v1/internal/tick auth unconfigured";

    const body = $("tick-status-body");
    clearNode(body);
    const activeTurns = overview.active_remote_turns || [];

    body.appendChild(
      el(
        "div",
        { class: "badge-row" },
        pill(overview.serverless ? "Serverless Mode" : "Local Mode", overview.serverless ? "purple" : "muted"),
        pill(`Sandbox provider: ${overview.sandbox_provider || "local"}`, "accent"),
        pill(`Active ready sandboxes: ${overview.active_sandboxes_count || 0}`, "success"),
        pill(`Active remote turns: ${activeTurns.length}`, activeTurns.length > 0 ? "warning" : "muted")
      )
    );

    if (activeTurns.length > 0) {
      const tbl = el(
        "table",
        { class: "mt-16" },
        el(
          "thead",
          {},
          el(
            "tr",
            {},
            el("th", { text: "Turn ID" }),
            el("th", { text: "Agent" }),
            el("th", { text: "Harness" }),
            el("th", { text: "Status" }),
            el("th", { text: "Last Event" })
          )
        ),
        el(
          "tbody",
          {},
          ...activeTurns.map((t) =>
            el(
              "tr",
              {},
              el("td", { class: "mono", text: t.turn_id }),
              el("td", { class: "mono", text: t.agent_name || t.agent_id }),
              el("td", { class: "mono", text: `${t.harness_kind}${t.provider_kind ? ` (${t.provider_kind})` : ""}` }),
              el("td", {}, pill(t.status, "accent")),
              el("td", { class: "mono text-sm", text: fmtTime(t.last_event_at_ms) })
            )
          )
        )
      );
      body.appendChild(tbl);
    }
  }

  function renderSchedules(schedules) {
    const wrap = $("schedules-table");
    clearNode(wrap);
    if (schedules.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No schedules found." }));
      return;
    }
    const tbl = el(
      "table",
      {},
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          el("th", { text: "Agent" }),
          el("th", { text: "Kind" }),
          el("th", { text: "Spec" }),
          el("th", { text: "State" }),
          el("th", { text: "Next Fire" }),
          el("th", { text: "Last Fired" }),
          el("th", { text: "Prompt" })
        )
      ),
      el(
        "tbody",
        {},
        ...schedules.map((s) =>
          el(
            "tr",
            {},
            el("td", { class: "mono", text: s.agent || s.agent_id }),
            el("td", {}, pill(s.kind, "accent")),
            el("td", { class: "mono", text: s.spec }),
            el("td", {}, pill(s.state, s.state === "active" ? "success" : "muted")),
            el("td", { class: "mono text-sm", text: fmtTime(s.next_fire_at_ms) }),
            el("td", { class: "mono text-sm", text: fmtTime(s.last_fired_at_ms) }),
            el("td", { text: s.prompt })
          )
        )
      )
    );
    wrap.appendChild(tbl);
  }

  function renderOutbound(unsettled) {
    $("outbound-count").textContent = `${unsettled.length} held`;
    const wrap = $("outbound-table");
    clearNode(wrap);
    if (unsettled.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No unsettled outbound messages." }));
      return;
    }
    const tbl = el(
      "table",
      {},
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          el("th", { text: "Key" }),
          el("th", { text: "Agent" }),
          el("th", { text: "Destination" }),
          el("th", { text: "Since" }),
          el("th", { text: "Error" }),
          el("th", { text: "Excerpt" }),
          el("th", { text: "Actions" })
        )
      ),
      el(
        "tbody",
        {},
        ...unsettled.map((item) =>
          el(
            "tr",
            {},
            el("td", { class: "mono text-sm", text: item.key }),
            el("td", { class: "mono", text: item.agent_id }),
            el("td", { class: "mono", text: item.destination }),
            el("td", { class: "mono text-sm", text: fmtTime(item.since_ms) }),
            el("td", { class: "text-sm", text: item.error || "-" }),
            el("td", { text: item.excerpt || "" }),
            el(
              "td",
              {},
              el(
                "div",
                { class: "action-bar" },
                el("button", {
                  class: "btn btn-success btn-sm",
                  type: "button",
                  text: "Retry",
                  onClick: async () => {
                    try {
                      await api(`/v1/outbound/${encodeURIComponent(item.key)}/retry`, { method: "POST" });
                      toast(`Retried outbound ${item.key}`, "success");
                      await loadAutomations();
                    } catch (err) {
                      toast(`Retry failed: ${err.message}`, "error");
                    }
                  },
                }),
                el("button", {
                  class: "btn btn-danger btn-sm",
                  type: "button",
                  text: "Discard",
                  onClick: async () => {
                    try {
                      await api(`/v1/outbound/${encodeURIComponent(item.key)}/discard`, { method: "POST" });
                      toast(`Discarded outbound ${item.key}`, "success");
                      await loadAutomations();
                    } catch (err) {
                      toast(`Discard failed: ${err.message}`, "error");
                    }
                  },
                })
              )
            )
          )
        )
      )
    );
    wrap.appendChild(tbl);
  }

  function initAutomationActions() {
    $("automations-refresh-btn").addEventListener("click", loadAutomations);
    $("schedules-agent-select").addEventListener("change", loadAutomations);
    $("run-tick-btn").addEventListener("click", async () => {
      const btn = $("run-tick-btn");
      btn.disabled = true;
      try {
        const summary = await api("/admin/api/tick", { method: "POST" });
        const box = $("tick-last-result");
        box.classList.remove("hidden");
        clearNode(box);
        box.appendChild(
          el("div", {
            class: "mono text-sm",
            text: `Manual tick pass completed: schedules_fired=${summary.schedules_fired}, cloud_turns_polled=${summary.cloud_turns_polled}, turns_settled=${summary.turns_settled}, stale_open_turns_cleared=${summary.stale_open_turns_cleared || 0}, backlog_dispatched=${summary.backlog_dispatched || 0}, leases_renewed=${summary.leases_renewed}, sandboxes_swept=${summary.sandboxes_swept}`,
          })
        );
        toast("Tick pass completed", "success");
        await loadAutomations();
      } catch (err) {
        toast(`Tick pass failed: ${err.message}`, "error");
      } finally {
        btn.disabled = false;
      }
    });
  }

  // ---------- Tab 5: Channels & Groups ----------

  async function loadChannelsAndGroups() {
    try {
      const [channelsResp, groupsResp] = await Promise.all([
        api("/v1/channels"),
        api("/v1/groups"),
      ]);
      renderChannels((channelsResp && channelsResp.channels) || []);
      renderGroups((groupsResp && groupsResp.groups) || []);
    } catch (err) {
      toast(`Failed to load channels & groups: ${err.message}`, "error");
    }
  }

  function renderChannels(channels) {
    $("channels-count").textContent = `${channels.length} channel(s)`;
    const wrap = $("channels-table");
    clearNode(wrap);
    if (channels.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No channel bindings configured." }));
      return;
    }
    const tbl = el(
      "table",
      {},
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          el("th", { text: "ID" }),
          el("th", { text: "Kind" }),
          el("th", { text: "Bot" }),
          el("th", { text: "Bound Agents" })
        )
      ),
      el(
        "tbody",
        {},
        ...channels.map((c) =>
          el(
            "tr",
            {},
            el("td", { class: "mono text-sm", text: c.id }),
            el("td", {}, pill(c.kind || "channel", "accent")),
            el("td", { class: "mono", text: c.bot || "-" }),
            el("td", { class: "mono", text: (c.agents || []).join(", ") || "-" })
          )
        )
      )
    );
    wrap.appendChild(tbl);
  }

  function renderGroups(groups) {
    $("groups-count").textContent = `${groups.length} group(s)`;
    const wrap = $("groups-list");
    clearNode(wrap);
    if (groups.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No multi-agent groups configured." }));
      return;
    }
    for (const g of groups) {
      const card = el(
        "div",
        { class: "group-card" },
        el(
          "div",
          { class: "group-card-head" },
          el("strong", { class: "mono", text: g.name }),
          el(
            "div",
            { class: "badge-row" },
            pill(g.public ? "public" : "private", g.public ? "success" : "muted"),
            pill(`${(g.members || []).length} members`, "accent")
          )
        )
      );
      if (g.roster && g.roster.length > 0) {
        const list = el("div", { class: "badge-row" });
        for (const m of g.roster) {
          list.appendChild(pill(`${m.name} (${m.harness})`, stateTone(m.state)));
        }
        card.appendChild(list);
      } else {
        card.appendChild(
          el("div", { class: "mono text-sm muted", text: (g.members || []).join(", ") || "empty" })
        );
      }
      wrap.appendChild(card);
    }
  }

  function initChannelsActions() {
    $("channels-refresh-btn").addEventListener("click", loadChannelsAndGroups);
  }

  // ---------- Boot ----------

  function init() {
    initTheme();
    initModals();
    initFleetActions();
    initProfileActions();
    initTelemetryActions();
    initAutomationActions();
    initChannelsActions();

    document.querySelectorAll(".nav-tab").forEach((btn) => {
      btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab")));
    });
    $("logout-btn").addEventListener("click", doLogout);

    window.AnyAgentsAdmin = { api, el, clearNode, pill, toast, fmtTime, state, switchTab, loadFleet };
    checkAuth();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
