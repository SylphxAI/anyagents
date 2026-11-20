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
    convSignature: "",
    cmdkItems: [],
    cmdkActiveIndex: 0,
  };

  // ---------- DOM helpers (strictly textContent-safe, zero innerHTML) ----------

  const $ = (id) => document.getElementById(id);

  function el(tag, attrs = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, val] of Object.entries(attrs)) {
      if (val === undefined || val === null || val === false) continue;
      if (key === "class") node.className = val;
      else if (key === "text") node.textContent = String(val);
      else if (key.startsWith("on") && typeof val === "function") node.addEventListener(key.slice(2).toLowerCase(), val);
      else if (key === "disabled" || key === "checked" || key === "selected") node[key] = Boolean(val);
      else node.setAttribute(key, String(val));
    }
    for (const child of children) {
      if (child === undefined || child === null || child === false) continue;
      node.appendChild(typeof child === "string" || typeof child === "number" ? document.createTextNode(String(child)) : child);
    }
    return node;
  }

  function clearNode(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  function pill(text, tone = "muted") {
    return el("span", { class: `pill pill-${tone}`, text });
  }

  function statusDot(tone, pulse = false) {
    return el("span", { class: `status-dot tone-${tone}${pulse ? " is-pulse" : ""}`, "aria-hidden": "true" });
  }

  function toast(message, kind = "info") {
    const container = $("toast-container");
    if (!container) return;
    const item = el("div", {
      class: `toast ${kind === "error" ? "toast-error" : kind === "success" ? "toast-success" : ""}`,
      text: message,
    });
    container.appendChild(item);
    setTimeout(() => item.remove(), 4200);
  }

  async function copyText(text, label = "Value") {
    try {
      await navigator.clipboard.writeText(text);
      toast(`Copied ${label}`, "success");
      return true;
    } catch {
      toast(`Could not access clipboard for ${label}`, "error");
      return false;
    }
  }

  // ---------- Formatting & Status Taxonomy ----------

  function fmtNumber(n) {
    return n === undefined || n === null ? "-" : Number(n).toLocaleString();
  }

  function fmtCompact(n) {
    if (n === undefined || n === null) return "-";
    const num = Number(n);
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
    if (num >= 10_000) return `${(num / 1_000).toFixed(1)}k`;
    return num.toLocaleString();
  }

  function fmtUsdFromMicros(micros) {
    if (micros === undefined || micros === null) return "-";
    const usd = Number(micros) / 1_000_000;
    if (usd === 0 || usd >= 0.01) return `$${usd.toFixed(2)}`;
    return `$${usd.toFixed(4)}`;
  }

  function fmtTime(ms) {
    if (!ms) return "-";
    const d = new Date(ms);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function fmtRelativeMs(ms) {
    if (ms === undefined || ms === null) return "-";
    const sec = Math.max(0, Math.round(ms / 1000));
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ${sec % 60}s`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ${min % 60}m`;
    return `${Math.floor(hr / 24)}d ${hr % 24}h`;
  }

  function fmtFutureTime(ms) {
    if (!ms) return "-";
    const diff = ms - Date.now();
    return diff > 0 && diff < 86_400_000 * 7 ? `${fmtTime(ms)} (in ${fmtRelativeMs(diff)})` : fmtTime(ms);
  }

  function agentStatus(agent) {
    if (!agent || agent.state === "archived") return { key: "archived", label: agent ? "archived" : "unknown", tone: "muted", pulse: false };
    const isFaulted =
      (agent.turn_errors && agent.turn_errors.faulted_at_ms) ||
      agent.turn_state === "faulted" ||
      agent.turn_state === "errored" ||
      Boolean(agent.last_error);
    if (isFaulted) {
      return { key: "errored", label: agent.state === "paused" || agent.turn_state === "faulted" ? "faulted" : "errored", tone: "danger", pulse: false };
    }
    if (agent.state === "paused") return { key: "paused", label: "paused", tone: "warning", pulse: false };
    if (agent.has_open_turn || agent.turn_state === "working" || agent.turn_state === "in_turn") {
      return { key: "working", label: "working", tone: "success", pulse: true };
    }
    return { key: "idle", label: "idle", tone: "sky", pulse: false };
  }

  function stateTone(s) {
    if (s === "working" || s === "running") return "success";
    if (s === "idle") return "sky";
    if (s === "paused") return "warning";
    if (s === "faulted" || s === "errored") return "danger";
    return s === "archived" ? "muted" : "accent";
  }

  function isSafeExternalLink(url) {
    return typeof url === "string" && (url.startsWith("https://") || url.startsWith("http://localhost"));
  }

  function renderTable(headers, rows) {
    return el(
      "table",
      {},
      el("thead", {}, el("tr", {}, ...headers.map((h) => (typeof h === "string" ? el("th", { text: h }) : el("th", { class: h.class || "", text: h.label }))))),
      el("tbody", {}, ...rows)
    );
  }

  // ---------- HTTP & Auth ----------

  async function api(path, options = {}) {
    const headers = Object.assign({ accept: "application/json", [CSRF_HEADER]: "1" }, options.headers || {});
    const fetchOpts = { method: options.method || "GET", credentials: "same-origin", headers, signal: options.signal };
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
    if (!resp.ok) throw new Error((data && (data.error || data.message)) || `HTTP ${resp.status}`);
    return data;
  }

  function handleUnauthorized() {
    stopConversationAutoRefresh();
    if (state.auth && state.auth.mode === "oidc") {
      state.auth.authenticated = false;
      renderAuthState();
    }
  }

  function applyTheme(theme) {
    const normalized = theme === "light" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", normalized);
    localStorage.setItem("anyagents_admin_theme", normalized);
    const btn = $("theme-toggle");
    if (btn) {
      const label = normalized === "dark" ? "Switch to light theme" : "Switch to dark theme";
      btn.setAttribute("aria-label", label);
      btn.title = label;
    }
  }

  function toggleTheme() {
    applyTheme((document.documentElement.getAttribute("data-theme") || "dark") === "dark" ? "light" : "dark");
  }

  function initTheme() {
    applyTheme(localStorage.getItem("anyagents_admin_theme") === "light" ? "light" : "dark");
    $("theme-toggle").addEventListener("click", toggleTheme);
  }

  function openModal(id) {
    $(id).classList.remove("hidden");
  }

  function closeModal(id) {
    const node = $(id);
    if (!node || node.classList.contains("hidden")) return;
    node.classList.add("hidden");
    window.dispatchEvent(new CustomEvent("anyagents:modal-close", { detail: id }));
  }

  function initModals() {
    document.querySelectorAll(".modal-close").forEach((btn) => {
      btn.addEventListener("click", () => closeModal(btn.getAttribute("data-close")));
    });
    document.querySelectorAll(".modal-backdrop").forEach((backdrop) => {
      backdrop.addEventListener("click", (evt) => {
        if (evt.target === backdrop) closeModal(backdrop.id);
      });
    });
  }

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
    const cmdkBtn = $("cmdk-trigger");
    const nav = $("main-nav");
    const gate = $("auth-gate");
    const main = $("console-main");

    envBadge.classList.remove("hidden");
    envBadge.textContent = auth.serverless ? "Serverless" : "Local";
    envBadge.className = `pill ${auth.serverless ? "pill-sky" : "pill-muted"}`;

    if (auth.authenticated) {
      gate.classList.add("hidden");
      main.classList.remove("hidden");
      nav.classList.remove("hidden");
      cmdkBtn.classList.remove("hidden");
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
      return;
    }

    main.classList.add("hidden");
    nav.classList.add("hidden");
    cmdkBtn.classList.add("hidden");
    gate.classList.remove("hidden");
    logoutBtn.classList.add("hidden");
    authBadge.textContent = auth.mode === "oidc" ? "Signed out" : "Auth unconfigured";
    authBadge.className = "pill pill-danger";

    const msgEl = $("auth-gate-message");
    const actionsEl = $("auth-gate-actions");
    clearNode(actionsEl);
    if (auth.mode === "oidc") {
      msgEl.textContent = "Sign in with your configured OpenID Connect identity provider to manage agents, profiles, and telemetry.";
      actionsEl.appendChild(el("a", { class: "btn btn-primary btn-lg", href: "/admin/auth/login", text: "Sign in with OIDC" }));
    } else {
      msgEl.textContent =
        "Admin OIDC authentication is not configured for this confined/serverless coordinator. Set ANYAGENTS_ADMIN_OIDC_ISSUER, ANYAGENTS_ADMIN_OIDC_CLIENT_ID, and ANYAGENTS_ADMIN_EMAILS to enable web console sign-in.";
      actionsEl.appendChild(el("button", { class: "btn btn-secondary", type: "button", text: "Re-check Status", onClick: checkAuth }));
    }
  }

  async function doLogout() {
    const wasOidc = state.auth && state.auth.mode === "oidc";
    try {
      await fetch("/admin/auth/logout", {
        method: "POST",
        credentials: "same-origin",
        headers: { [CSRF_HEADER]: "1", "x-requested-with": "fetch" },
      });
    } catch {
      // Ignore network errors and re-check auth state.
    }
    stopConversationAutoRefresh();
    if (wasOidc) {
      window.location.assign("/");
      return;
    }
    await checkAuth();
  }

  // ---------- Navigation & Command Palette (⌘K / Ctrl+K) ----------

  function switchTab(tab) {
    state.activeTab = tab;
    document.querySelectorAll(".nav-tab").forEach((btn) => btn.classList.toggle("active", btn.getAttribute("data-tab") === tab));
    document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.id === `tab-${tab}`));
    if (tab !== "fleet") stopConversationAutoRefresh();
    else if (state.selectedAgentName && $("conv-autorefresh").checked) startConversationAutoRefresh();
    loadActiveTab();
  }

  function loadActiveTab() {
    const loaders = { fleet: loadFleet, telemetry: loadTelemetry, automations: loadAutomations, channels: loadChannelsAndGroups };
    if (loaders[state.activeTab]) loaders[state.activeTab]();
    window.dispatchEvent(new CustomEvent("anyagents:tab", { detail: state.activeTab }));
  }

  function jumpToAgent(agentName) {
    const found = state.agents.find((a) => a.name === agentName || a.id === agentName);
    if (state.activeTab !== "fleet") switchTab("fleet");
    if (found) {
      if (found.state === "archived" && $("fleet-state-filter").value === "active") $("fleet-state-filter").value = "all";
      selectAgent(found);
    } else {
      state.selectedAgentName = agentName;
      loadFleet();
    }
  }

  function openCmdk() {
    if (!state.auth || !state.auth.authenticated) return;
    openModal("modal-cmdk");
    const input = $("cmdk-input");
    input.value = "";
    state.cmdkActiveIndex = 0;
    renderCmdkResults("");
    setTimeout(() => input.focus(), 10);
  }

  function buildCmdkCatalog() {
    const tabs = [
      ["fleet", "Fleet Hierarchy & Inspector", "Section · Live agents"],
      ["profiles", "Profiles & Quota Pool", "Section · Provider seats & keys"],
      ["telemetry", "Usage Telemetry", "Section · Tokens & cost"],
      ["automations", "Automations & Ticks", "Section · Schedules & outbound queue"],
      ["channels", "Channels & Groups", "Section · Bindings & rosters"],
      ["secrets", "Encrypted Deployment Secrets", "Section · Write-only bundles"],
    ];
    const actions = [
      {
        group: "Actions",
        title: "Spawn New Agent",
        sub: "Create a root or child agent",
        run: () => {
          if (state.activeTab !== "fleet") switchTab("fleet");
          $("form-spawn").reset();
          $("spawn-parent").value = "";
          openModal("modal-spawn");
        },
      },
      { group: "Actions", title: "Refresh Current View", sub: `Reload ${state.activeTab}`, run: () => { loadActiveTab(); toast("Refreshed view", "success"); } },
      { group: "Actions", title: "Toggle Dark / Light Theme", sub: "Switch control plane appearance", run: toggleTheme },
    ];
    const navItems = tabs.map(([id, title, sub]) => ({ group: "Sections", title, sub, run: () => switchTab(id) }));
    const agentItems = [...state.agents]
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((a) => {
        const st = agentStatus(a);
        return {
          group: "Agents",
          title: a.name,
          sub: `${a.harness}${a.model ? ` · ${a.model}` : ""}${a.role ? ` · ${a.role}` : ""}`,
          badge: st.label,
          tone: st.tone,
          pulse: st.pulse,
          search: `${a.name} ${a.id} ${a.harness} ${a.model || ""} ${a.role || ""} ${st.label}`,
          run: () => jumpToAgent(a.name),
        };
      });
    return [...agentItems, ...navItems, ...actions];
  }

  function renderCmdkResults(rawQuery) {
    const query = rawQuery.trim().toLowerCase();
    const container = $("cmdk-results");
    clearNode(container);
    const catalog = buildCmdkCatalog();
    const matches = query
      ? catalog.filter((item) => `${item.title} ${item.sub} ${item.search || ""}`.toLowerCase().includes(query))
      : catalog;
    state.cmdkItems = matches.slice(0, 30);
    if (state.cmdkActiveIndex >= state.cmdkItems.length) state.cmdkActiveIndex = 0;

    if (state.cmdkItems.length === 0) {
      container.appendChild(el("div", { class: "empty-state", text: "No matching agents, sections, or commands." }));
      return;
    }

    let lastGroup = "";
    state.cmdkItems.forEach((item, idx) => {
      if (item.group !== lastGroup) {
        lastGroup = item.group;
        container.appendChild(el("div", { class: "cmdk-group-title", text: item.group }));
      }
      container.appendChild(
        el(
          "button",
          {
            class: `cmdk-item${idx === state.cmdkActiveIndex ? " is-active" : ""}`,
            type: "button",
            role: "option",
            "aria-selected": idx === state.cmdkActiveIndex ? "true" : "false",
            onMouseEnter: () => { state.cmdkActiveIndex = idx; updateCmdkActiveClass(); },
            onClick: () => executeCmdkItem(idx),
          },
          el(
            "div",
            { class: "cmdk-item-left" },
            item.tone ? statusDot(item.tone, item.pulse) : null,
            el("span", { class: "cmdk-item-title", text: item.title }),
            el("span", { class: "cmdk-item-sub mono", text: item.sub })
          ),
          item.badge ? pill(item.badge, item.tone) : null
        )
      );
    });
  }

  function updateCmdkActiveClass() {
    $("cmdk-results").querySelectorAll(".cmdk-item").forEach((btn, i) => {
      const active = i === state.cmdkActiveIndex;
      btn.classList.toggle("is-active", active);
      btn.setAttribute("aria-selected", active ? "true" : "false");
      if (active) btn.scrollIntoView({ block: "nearest" });
    });
  }

  function executeCmdkItem(idx) {
    const item = state.cmdkItems[idx];
    if (!item) return;
    closeModal("modal-cmdk");
    item.run();
  }

  function initCommandPalette() {
    $("cmdk-trigger").addEventListener("click", openCmdk);
    $("cmdk-input").addEventListener("input", (evt) => { state.cmdkActiveIndex = 0; renderCmdkResults(evt.target.value); });
    document.addEventListener("keydown", (evt) => {
      if ((evt.metaKey || evt.ctrlKey) && evt.key.toLowerCase() === "k") {
        evt.preventDefault();
        if ($("modal-cmdk").classList.contains("hidden")) openCmdk();
        else closeModal("modal-cmdk");
        return;
      }
      if (evt.key === "Escape") {
        document.querySelectorAll(".modal-backdrop:not(.hidden)").forEach((m) => closeModal(m.id));
        return;
      }
      if (!$("modal-cmdk").classList.contains("hidden")) {
        if (evt.key === "ArrowDown" && state.cmdkItems.length > 0) {
          evt.preventDefault();
          state.cmdkActiveIndex = (state.cmdkActiveIndex + 1) % state.cmdkItems.length;
          updateCmdkActiveClass();
        } else if (evt.key === "ArrowUp" && state.cmdkItems.length > 0) {
          evt.preventDefault();
          state.cmdkActiveIndex = (state.cmdkActiveIndex - 1 + state.cmdkItems.length) % state.cmdkItems.length;
          updateCmdkActiveClass();
        } else if (evt.key === "Enter") {
          evt.preventDefault();
          executeCmdkItem(state.cmdkActiveIndex);
        }
      }
    });
  }

  // ---------- Tab 1: Fleet Pulse Strip, Tree & Inspector ----------

  async function loadFleet() {
    try {
      const [agentsResp, overview] = await Promise.all([api("/v1/agents?state=all"), api("/admin/api/overview")]);
      state.agents = (agentsResp && agentsResp.agents) || [];
      state.overview = overview || {};
      state.agentsMeta = (overview && overview.agents_meta) || {};
      renderPulseStrip();
      renderFleetTree();
      populateScheduleAgentSelect();
      if (state.selectedAgentName) {
        const stillExists = state.agents.find((a) => a.name === state.selectedAgentName);
        if (stillExists) renderInspector(stillExists);
        else { state.selectedAgentName = null; renderInspector(null); }
      }
    } catch (err) {
      toast(`Failed to load fleet: ${err.message}`, "error");
    }
  }

  function renderPulseStrip() {
    const strip = $("fleet-pulse-strip");
    if (!strip) return;
    clearNode(strip);
    const counts = { total: state.agents.length, working: 0, idle: 0, paused: 0, errored: 0, archived: 0 };
    for (const a of state.agents) {
      const st = agentStatus(a);
      if (counts[st.key] !== undefined) counts[st.key] += 1;
    }

    const activeFilter = $("fleet-state-filter").value;
    const tiles = [
      ["all", "Total", counts.total, "muted", false],
      ["working", "Working", counts.working, "success", counts.working > 0],
      ["idle", "Idle", counts.idle, "sky", false],
      ["paused", "Paused", counts.paused, "warning", false],
      ["errored", "Errored", counts.errored, "danger", false],
      ["archived", "Archived", counts.archived, "muted", false],
    ];
    for (const [key, label, value, tone, pulse] of tiles) {
      const isSelected = activeFilter === key;
      strip.appendChild(
        el(
          "button",
          {
            class: `pulse-tile${isSelected ? " is-active" : ""}`,
            type: "button",
            "aria-pressed": isSelected ? "true" : "false",
            onClick: () => {
              const sel = $("fleet-state-filter");
              sel.value = sel.value === key ? "active" : key;
              renderPulseStrip();
              renderFleetTree();
            },
          },
          el("div", { class: "pulse-tile-head" }, statusDot(tone, pulse), el("span", { text: label })),
          el("div", { class: "pulse-tile-value tabular", text: fmtNumber(value) })
        )
      );
    }
  }

  function filterAgents(agents) {
    const query = ($("fleet-search").value || "").trim().toLowerCase();
    const stateFilter = $("fleet-state-filter").value;
    return agents.filter((a) => {
      const st = agentStatus(a);
      if (stateFilter === "active" && a.state === "archived") return false;
      if (stateFilter === "working" && st.key !== "working") return false;
      if (stateFilter === "idle" && st.key !== "idle") return false;
      if (stateFilter === "running" && a.state !== "running") return false;
      if (stateFilter === "paused" && st.key !== "paused") return false;
      if (stateFilter === "errored" && st.key !== "errored") return false;
      if (stateFilter === "archived" && st.key !== "archived") return false;
      if (!query) return true;
      return `${a.name} ${a.id} ${a.role || ""} ${a.harness} ${a.runtime || ""} ${a.model || ""} ${a.profile || ""}`.toLowerCase().includes(query);
    });
  }

  function renderFleetTree() {
    const container = $("fleet-tree");
    clearNode(container);
    const filtered = filterAgents(state.agents);
    $("fleet-count").textContent = `${filtered.length} / ${state.agents.length}`;

    if (filtered.length === 0) {
      container.appendChild(el("div", { class: "empty-state", text: "No agents match the current filter." }));
      return;
    }

    const byId = new Map(filtered.map((a) => [a.id, a]));
    const childrenByParent = new Map();
    const roots = [];
    for (const agent of filtered) {
      if (agent.parent && byId.has(agent.parent)) {
        if (!childrenByParent.has(agent.parent)) childrenByParent.set(agent.parent, []);
        childrenByParent.get(agent.parent).push(agent);
      } else {
        roots.push(agent);
      }
    }
    const sortFn = (a, b) => a.name.localeCompare(b.name);
    roots.sort(sortFn);
    for (const list of childrenByParent.values()) list.sort(sortFn);

    function buildNode(agent) {
      const kids = childrenByParent.get(agent.id) || [];
      const meta = state.agentsMeta[agent.id] || {};
      const collapsed = state.collapsedNodes.has(agent.id);
      const st = agentStatus(agent);

      const toggleBtn = el("button", {
        class: "tree-toggle",
        type: "button",
        "aria-label": kids.length > 0 ? (collapsed ? "Expand children" : "Collapse children") : "Leaf agent",
        text: kids.length > 0 ? (collapsed ? "▸" : "▾") : "·",
        onClick: (evt) => {
          evt.stopPropagation();
          if (kids.length === 0) return;
          if (state.collapsedNodes.has(agent.id)) state.collapsedNodes.delete(agent.id);
          else state.collapsedNodes.add(agent.id);
          renderFleetTree();
        },
      });

      const badges = el("div", { class: "badge-row" }, pill(st.label, st.tone));
      if (meta.unread_count > 0) badges.appendChild(pill(`${meta.unread_count} unread`, "warning"));

      const chips = el(
        "div",
        { class: "tree-chips" },
        el("span", { class: "spec-chip", text: agent.harness }),
        agent.runtime ? el("span", { class: "spec-chip", text: agent.runtime }) : null,
        agent.model ? el("span", { class: "spec-chip", text: agent.model }) : null,
        meta.sandbox_lease && meta.sandbox_lease.lease_state
          ? el("span", { class: "spec-chip", text: `${meta.sandbox_lease.provider}:${meta.sandbox_lease.lease_state}` })
          : null
      );

      const row = el(
        "div",
        { class: `tree-row${state.selectedAgentName === agent.name ? " selected" : ""}`, onClick: () => selectAgent(agent) },
        toggleBtn,
        el(
          "div",
          { class: "tree-row-body" },
          el(
            "div",
            { class: "tree-row-top" },
            el("div", { class: "tree-name-wrap" }, statusDot(st.tone, st.pulse), el("span", { class: "tree-name", text: agent.name }), agent.role ? el("span", { class: "tree-role", text: agent.role }) : null),
            badges
          ),
          chips
        )
      );

      const wrapper = el("div", { class: "tree-node" }, row);
      if (kids.length > 0 && !collapsed) {
        wrapper.appendChild(el("div", { class: "tree-children" }, ...kids.map(buildNode)));
      }
      return wrapper;
    }

    for (const root of roots) container.appendChild(buildNode(root));
  }

  function selectAgent(agent) {
    const changed = state.selectedAgentName !== agent.name;
    state.selectedAgentName = agent.name;
    if (changed) state.convSignature = "";
    renderFleetTree();
    renderInspector(agent);
    loadConversation(agent.name, { forceScroll: changed });
    if ($("conv-autorefresh").checked) startConversationAutoRefresh();
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
    const st = agentStatus(agent);
    $("insp-status-dot").className = `status-dot tone-${st.tone}${st.pulse ? " is-pulse" : ""}`;
    $("insp-name").textContent = agent.name;
    $("insp-id-text").textContent = agent.id;
    $("insp-copy-id").dataset.agentId = agent.id;
    $("insp-subtitle").textContent = `scope:${agent.scope || "-"} · lifecycle:${agent.lifecycle || "-"}${agent.role ? ` · role:${agent.role}` : ""}`;

    const badgesEl = $("insp-badges");
    clearNode(badgesEl);
    badgesEl.appendChild(pill(st.label, st.tone));
    if (agent.runtime) badgesEl.appendChild(pill(`${agent.harness}@${agent.runtime}`, agent.runtime === "cloud" ? "sky" : "muted"));
    if (agent.has_open_turn) badgesEl.appendChild(pill(agent.open_turn_age_ms ? `open turn · ${fmtRelativeMs(agent.open_turn_age_ms)}` : "open turn", "success"));
    if (meta.unread_count > 0) badgesEl.appendChild(pill(`${meta.unread_count} unread`, "warning"));
    if (meta.sandbox_lease && meta.sandbox_lease.lease_state) {
      badgesEl.appendChild(pill(`sandbox ${meta.sandbox_lease.provider}:${meta.sandbox_lease.lease_state}`, meta.sandbox_lease.lease_state === "ready" ? "success" : "muted"));
    }

    $("btn-pause-agent").disabled = agent.state === "paused" || agent.state === "archived";
    $("btn-wake-agent").disabled = agent.state === "archived";
    $("btn-archive-agent").disabled = agent.state === "archived";

    const errBanner = $("insp-error-banner");
    const errText = agent.last_error || (agent.turn_errors && agent.turn_errors.last_error);
    clearNode(errBanner);
    if (errText) {
      errBanner.appendChild(el("div", {}, el("span", { class: "alert-label", text: st.label === "faulted" ? "Agent Faulted" : "Last Error" }), el("div", { class: "mono", text: errText })));
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
      ["Profile", agent.profile || "(auto)"],
      ...(agent.remote_budget_micros ? [["Budget Cap", `$${(agent.remote_budget_micros / 1e6).toFixed(2)}`]] : []),
      ["Parent", parentAgent ? { link: parentAgent.name } : agent.parent || "(root)"],
      ["Process", agent.process || "-"],
      ["Unread Mail", String(meta.unread_count || 0)],
      ["Sandbox Lease", lease ? `${lease.provider} / ${lease.lease_state}${lease.lease_id ? ` (${lease.lease_id})` : ""}` : "none"],
      ["Automations", `${(agent.automations && agent.automations.total) || 0} active`],
      ["Workdir", agent.workdir || "-"],
      ["Description", agent.description || "-", true],
    ];

    for (const [label, val, wide] of fields) {
      const valueNode =
        val && typeof val === "object" && val.link
          ? el("button", { class: "meta-link", type: "button", text: val.link, onClick: () => jumpToAgent(val.link) })
          : el("div", { class: `meta-value${wide ? "" : " mono"}`, text: val });
      metaGrid.appendChild(el("div", { class: `meta-item${wide ? " meta-item-wide" : ""}` }, el("div", { class: "meta-label", text: label }), valueNode));
    }
    window.dispatchEvent(new CustomEvent("anyagents:agent", { detail: agent }));
  }

  async function loadConversation(agentName, opts = {}) {
    if (!agentName) return;
    const viewer = $("conv-viewer");
    const countChip = $("conv-turn-count");
    try {
      const data = await api(`/v1/agents/${encodeURIComponent(agentName)}/conversation?limit=50`);
      if (state.selectedAgentName !== agentName) return;
      const turns = (data && data.turns) || [];
      if (countChip) countChip.textContent = `${turns.length} turn${turns.length === 1 ? "" : "s"}`;

      const last = turns[turns.length - 1];
      const sig = `${agentName}:${turns.length}:${last ? `${last.seq || last.timestamp_ms || ""}:${(last.text || "").length}` : "empty"}`;
      if (!opts.force && sig === state.convSignature) return;

      const prevTop = viewer.scrollTop;
      const wasAtBottom = opts.forceScroll || viewer.scrollHeight - viewer.scrollTop - viewer.clientHeight < 48;
      state.convSignature = sig;
      clearNode(viewer);

      if (turns.length === 0) {
        viewer.appendChild(el("div", { class: "empty-state", text: "No conversation turns recorded yet." }));
        return;
      }

      for (const t of turns) {
        const isSys = t.role === "system";
        const dirClass = isSys ? "turn-sys" : t.direction === "out" ? "turn-out" : "turn-in";
        const dirTag = isSys ? "SYS" : t.direction === "out" ? "OUT" : "IN";
        const routeText = `${t.from || "-"} → ${t.to || "-"}${t.role && !isSys ? ` (${t.role})` : ""}`;
        const timeText = t.timestamp_ms ? fmtTime(t.timestamp_ms) : `#${t.seq}`;

        const copyBtn = el("button", {
          class: "conv-copy-btn",
          type: "button",
          text: "Copy",
          onClick: async (evt) => {
            const btn = evt.currentTarget;
            if (await copyText(t.text || "", "message")) {
              btn.textContent = "Copied";
              setTimeout(() => { btn.textContent = "Copy"; }, 1600);
            }
          },
        });

        let toolsBlock = null;
        if (t.tool_names && t.tool_names.length > 0) {
          const list = el("div", { class: "conv-tools-list hidden" }, ...t.tool_names.map((n) => el("span", { class: "spec-chip", text: n })));
          const toggle = el("button", {
            class: "conv-tools-toggle",
            type: "button",
            text: `${t.tool_names.length} tool${t.tool_names.length === 1 ? "" : "s"} ▸`,
            onClick: () => {
              const hidden = list.classList.toggle("hidden");
              toggle.textContent = `${t.tool_names.length} tool${t.tool_names.length === 1 ? "" : "s"} ${hidden ? "▸" : "▾"}`;
            },
          });
          toolsBlock = el("div", { class: "conv-tools-wrap" }, toggle, list);
        }

        viewer.appendChild(
          el(
            "div",
            { class: `conv-turn ${dirClass}` },
            el(
              "div",
              { class: "conv-turn-head mono" },
              el("div", { class: "conv-turn-meta" }, el("span", { class: "conv-dir", text: dirTag }), el("span", { text: routeText })),
              el("div", { class: "conv-turn-right" }, el("span", { class: "tabular", text: timeText }), copyBtn)
            ),
            toolsBlock,
            el("pre", { class: "conv-turn-body", text: t.text || "" })
          )
        );
      }
      viewer.scrollTop = wasAtBottom ? viewer.scrollHeight : prevTop;
    } catch (err) {
      clearNode(viewer);
      viewer.appendChild(el("div", { class: "empty-state", text: `Failed to load conversation: ${err.message}` }));
    }
  }

  function startConversationAutoRefresh() {
    stopConversationAutoRefresh();
    state.convTimer = setInterval(() => {
      if (state.activeTab === "fleet" && state.selectedAgentName) loadConversation(state.selectedAgentName, { forceScroll: false });
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
    $("fleet-state-filter").addEventListener("change", () => { renderPulseStrip(); renderFleetTree(); });
    $("fleet-refresh-btn").addEventListener("click", loadFleet);

    $("insp-copy-id").addEventListener("click", async () => {
      const btn = $("insp-copy-id");
      const id = btn.dataset.agentId || "";
      if (id && (await copyText(id, "Agent ID"))) {
        btn.classList.add("is-copied");
        setTimeout(() => btn.classList.remove("is-copied"), 1600);
      }
    });

    $("spawn-root-btn").addEventListener("click", () => { $("form-spawn").reset(); $("spawn-parent").value = ""; openModal("modal-spawn"); });
    $("btn-spawn-child").addEventListener("click", () => { $("form-spawn").reset(); $("spawn-parent").value = state.selectedAgentName || ""; openModal("modal-spawn"); });

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
          const body = { harness, runtime: runtime !== curRt ? runtime : agent.runtime || runtime, model, profile };
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

    const lifecycleAction = (btnId, action, label, confirmMsg) => {
      $(btnId).addEventListener("click", async () => {
        const name = state.selectedAgentName;
        if (!name) return;
        if (confirmMsg && !window.confirm(confirmMsg(name))) return;
        try {
          await api(`/v1/agents/${encodeURIComponent(name)}/${action}`, { method: "POST" });
          toast(`${label} "${name}"`, "success");
          await loadFleet();
        } catch (err) {
          toast(`${label} failed: ${err.message}`, "error");
        }
      });
    };
    lifecycleAction("btn-pause-agent", "pause", "Paused");
    lifecycleAction("btn-wake-agent", "wake", "Woke");
    lifecycleAction("btn-archive-agent", "archive", "Archived", (n) => `Archive agent "${n}"? Its sandbox compute will be released.`);

    $("send-msg-form").addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const name = state.selectedAgentName;
      const input = $("send-msg-input");
      const btn = $("send-msg-submit");
      const text = input.value.trim();
      if (!name || !text) return;
      btn.disabled = true;
      try {
        await api("/v1/send", { method: "POST", body: { sender: "operator", to: [`agent:${name}`], text } });
        input.value = "";
        toast(`Message dispatched to "${name}"`, "success");
        await loadConversation(name, { force: true, forceScroll: true });
      } catch (err) {
        toast(`Send failed: ${err.message}`, "error");
      } finally {
        btn.disabled = false;
      }
    });

    $("conv-refresh-btn").addEventListener("click", () => { if (state.selectedAgentName) loadConversation(state.selectedAgentName, { force: true }); });
    $("conv-autorefresh").addEventListener("change", (evt) => {
      if (evt.target.checked && state.selectedAgentName) startConversationAutoRefresh();
      else stopConversationAutoRefresh();
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
    const totalIn = Number(totals.input_tokens || 0);
    const cacheRead = Number(totals.cache_read_tokens || 0);
    const cacheRatio = totalIn + cacheRead > 0 ? Math.round((cacheRead / (totalIn + cacheRead)) * 100) : 0;

    const kpis = [
      ["Total Turns", fmtNumber(totals.turns || 0), `Window: last ${days}d`],
      ["Input Tokens", fmtCompact(totalIn), `Cache read: ${fmtCompact(cacheRead)} (${cacheRatio}% hit)`],
      ["Output Tokens", fmtCompact(totals.output_tokens || 0), `Cache write: ${fmtCompact(totals.cache_write_tokens || 0)}`],
      ["Reported Cost", fmtUsdFromMicros(totals.reported_cost_micros), "API-billed turns"],
    ];
    for (const [label, value, sub] of kpis) {
      cards.appendChild(el("div", { class: "kpi-card" }, el("div", { class: "kpi-label", text: label }), el("div", { class: "kpi-value mono tabular", text: value }), el("div", { class: "kpi-sub", text: sub })));
    }

    const agentBuckets = (usageAgent && usageAgent.buckets) || [];
    const modelBuckets = (usageModel && usageModel.buckets) || [];
    $("usage-agent-count").textContent = `${agentBuckets.length} agent${agentBuckets.length === 1 ? "" : "s"}`;
    $("usage-model-count").textContent = `${modelBuckets.length} bucket${modelBuckets.length === 1 ? "" : "s"}`;
    renderUsageTable($("usage-by-agent-table"), agentBuckets, "agent");
    renderUsageTable($("usage-by-model-table"), modelBuckets, "model");
  }

  function renderUsageTable(container, buckets, mode) {
    clearNode(container);
    if (buckets.length === 0) {
      container.appendChild(el("div", { class: "empty-state", text: "No usage records in this window." }));
      return;
    }
    const totalVolume = (b) => Number(b.input_tokens || 0) + Number(b.output_tokens || 0) + Number(b.cache_read_tokens || 0);
    const maxVol = Math.max(1, ...buckets.map(totalVolume));
    const headers = [
      mode === "agent" ? "Agent" : "Agent / Model",
      { label: "Turns", class: "num-col" },
      { label: "Input", class: "num-col" },
      { label: "Output", class: "num-col" },
      { label: "Cache Read", class: "num-col" },
      { label: "Cost", class: "num-col" },
    ];
    const rows = buckets.map((b) => {
      const label = mode === "agent" ? b.agent_name || b.agent_id : `${b.agent_name || b.agent_id} · ${b.model || "default"}`;
      const fill = el("div", { class: "usage-bar-fill" });
      fill.style.width = `${Math.max(2, Math.round((totalVolume(b) / maxVol) * 100))}%`;
      return el(
        "tr",
        {},
        el("td", { class: "usage-label-cell" }, el("div", { class: "mono", text: label }), el("div", { class: "usage-bar-track" }, fill)),
        el("td", { class: "mono num-col", text: fmtNumber(b.turns) }),
        el("td", { class: "mono num-col", text: fmtCompact(b.input_tokens) }),
        el("td", { class: "mono num-col", text: fmtCompact(b.output_tokens) }),
        el("td", { class: "mono num-col", text: fmtCompact(b.cache_read_tokens) }),
        el("td", { class: "mono num-col", text: fmtUsdFromMicros(b.reported_cost_micros) })
      );
    });
    container.appendChild(renderTable(headers, rows));
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
    for (const a of sorted) sel.appendChild(el("option", { value: a.name, text: a.name }));
    if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
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
    if (overview.timer_jwks_configured) authText.push("JWKS");
    if (overview.timer_token_configured) authText.push("TIMER_TOKEN");
    $("tick-auth-status").textContent = authText.length > 0 ? `/v1/internal/fire auth: ${authText.join(" + ")}` : "Timer fire auth unconfigured";

    const body = $("tick-status-body");
    clearNode(body);
    const activeTurns = overview.active_remote_turns || [];
    body.appendChild(
      el(
        "div",
        { class: "badge-row" },
        pill(overview.serverless ? "Serverless Mode" : "Local Mode", overview.serverless ? "sky" : "muted"),
        pill(`Sandbox: ${overview.sandbox_provider || "local"}`, "muted"),
        pill(`Ready sandboxes: ${overview.active_sandboxes_count || 0}`, "success"),
        pill(`Active remote turns: ${activeTurns.length}`, activeTurns.length > 0 ? "warning" : "muted")
      )
    );
    if (activeTurns.length > 0) {
      const rows = activeTurns.map((t) =>
        el(
          "tr",
          {},
          el("td", { class: "mono", text: t.turn_id }),
          el("td", { class: "mono", text: t.agent_name || t.agent_id }),
          el("td", { class: "mono", text: `${t.harness_kind}${t.provider_kind ? ` (${t.provider_kind})` : ""}` }),
          el("td", {}, pill(t.status, "sky")),
          el("td", { class: "mono text-sm tabular", text: fmtTime(t.last_event_at_ms) })
        )
      );
      body.appendChild(el("div", { class: "table-wrap mt-16" }, renderTable(["Turn ID", "Agent", "Harness", "Status", "Last Event"], rows)));
    }
  }

  function renderSchedules(schedules) {
    $("schedules-count").textContent = `${schedules.length} schedule${schedules.length === 1 ? "" : "s"}`;
    const wrap = $("schedules-table");
    clearNode(wrap);
    if (schedules.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No schedules configured for this filter." }));
      return;
    }
    const rows = schedules.map((s) =>
      el(
        "tr",
        {},
        el("td", { class: "mono", text: s.agent || s.agent_id }),
        el("td", {}, pill(s.kind, "sky")),
        el("td", {}, el("span", { class: "spec-chip", text: s.spec })),
        el("td", {}, pill(s.state, s.state === "active" ? "success" : "muted")),
        el("td", { class: "mono text-sm tabular", text: fmtFutureTime(s.next_fire_at_ms) }),
        el("td", { class: "mono text-sm tabular", text: fmtTime(s.last_fired_at_ms) }),
        el("td", { text: s.prompt })
      )
    );
    wrap.appendChild(renderTable(["Agent", "Kind", "Spec", "State", "Next Fire", "Last Fired", "Prompt"], rows));
  }

  function renderOutbound(unsettled) {
    $("outbound-count").textContent = `${unsettled.length} held`;
    const wrap = $("outbound-table");
    clearNode(wrap);
    if (unsettled.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "Outbound delivery queue is clear." }));
      return;
    }
    const outboundBtn = (label, cls, key, action) =>
      el("button", {
        class: `btn ${cls} btn-sm`,
        type: "button",
        text: label,
        onClick: async () => {
          try {
            await api(`/v1/outbound/${encodeURIComponent(key)}/${action}`, { method: "POST" });
            toast(`${label} succeeded for ${key}`, "success");
            await loadAutomations();
          } catch (err) {
            toast(`${label} failed: ${err.message}`, "error");
          }
        },
      });
    const rows = unsettled.map((item) =>
      el(
        "tr",
        {},
        el("td", { class: "mono text-sm", text: item.key }),
        el("td", { class: "mono", text: item.agent_id }),
        el("td", { class: "mono", text: item.destination }),
        el("td", { class: "mono text-sm tabular", text: fmtTime(item.since_ms) }),
        el("td", { class: "text-sm", text: item.error || "-" }),
        el("td", { text: item.excerpt || "" }),
        el("td", {}, el("div", { class: "action-bar" }, outboundBtn("Retry", "btn-success", item.key, "retry"), outboundBtn("Discard", "btn-danger", item.key, "discard")))
      )
    );
    wrap.appendChild(renderTable(["Key", "Agent", "Destination", "Since", "Error", "Excerpt", "Actions"], rows));
  }

  function initAutomationActions() {
    $("automations-refresh-btn").addEventListener("click", loadAutomations);
    $("schedules-agent-select").addEventListener("change", loadAutomations);
    $("run-tick-btn").addEventListener("click", async () => {
      const btn = $("run-tick-btn");
      btn.disabled = true;
      try {
        const s = await api("/admin/api/tick", { method: "POST" });
        const box = $("tick-last-result");
        box.classList.remove("hidden");
        clearNode(box);
        box.appendChild(
          el("div", {
            class: "mono text-sm tabular",
            text: `Tick pass completed: schedules_fired=${s.schedules_fired}, cloud_turns_polled=${s.cloud_turns_polled}, turns_settled=${s.turns_settled}, stale_open_turns_cleared=${s.stale_open_turns_cleared || 0}, backlog_dispatched=${s.backlog_dispatched || 0}, leases_renewed=${s.leases_renewed}, sandboxes_swept=${s.sandboxes_swept}`,
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
      const [channelsResp, groupsResp] = await Promise.all([api("/v1/channels"), api("/v1/groups")]);
      renderChannels((channelsResp && channelsResp.channels) || []);
      renderGroups((groupsResp && groupsResp.groups) || []);
    } catch (err) {
      toast(`Failed to load channels & groups: ${err.message}`, "error");
    }
  }

  function renderChannels(channels) {
    $("channels-count").textContent = `${channels.length} channel${channels.length === 1 ? "" : "s"}`;
    const wrap = $("channels-table");
    clearNode(wrap);
    if (channels.length === 0) {
      wrap.appendChild(el("div", { class: "empty-state", text: "No channel bindings configured." }));
      return;
    }
    const rows = channels.map((c) => {
      const agents = c.agents || [];
      const agentNodes =
        agents.length > 0
          ? el("div", { class: "badge-row" }, ...agents.map((name) => el("button", { class: "pill pill-muted pill-btn", type: "button", text: name, onClick: () => jumpToAgent(name) })))
          : el("span", { class: "muted", text: "-" });
      return el("tr", {}, el("td", { class: "mono text-sm", text: c.id }), el("td", {}, pill(c.kind || "channel", "sky")), el("td", { class: "mono", text: c.bot || "-" }), el("td", {}, agentNodes));
    });
    wrap.appendChild(renderTable(["ID", "Kind", "Bot", "Bound Agents"], rows));
  }

  function renderGroups(groups) {
    $("groups-count").textContent = `${groups.length} group${groups.length === 1 ? "" : "s"}`;
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
          el("div", { class: "badge-row" }, pill(g.public ? "public" : "private", g.public ? "success" : "muted"), pill(`${(g.members || []).length} members`, "sky"))
        )
      );
      if (g.roster && g.roster.length > 0) {
        card.appendChild(
          el("div", { class: "badge-row" }, ...g.roster.map((m) => el("button", { class: `pill pill-${stateTone(m.state)} pill-btn`, type: "button", text: `${m.name} (${m.harness})`, onClick: () => jumpToAgent(m.name) })))
        );
      } else {
        card.appendChild(el("div", { class: "mono text-sm muted", text: (g.members || []).join(", ") || "empty" }));
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
    initCommandPalette();
    initFleetActions();
    initTelemetryActions();
    initAutomationActions();
    initChannelsActions();

    document.querySelectorAll(".nav-tab").forEach((btn) => btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab"))));
    $("logout-btn").addEventListener("click", doLogout);

    window.AnyAgentsAdmin = {
      CSRF_HEADER,
      api,
      el,
      clearNode,
      pill,
      statusDot,
      toast,
      copyText,
      fmtNumber,
      fmtTime,
      fmtRelativeMs,
      isSafeExternalLink,
      agentStatus,
      openModal,
      closeModal,
      handleUnauthorized,
      state,
      switchTab,
      jumpToAgent,
      toggleTheme,
      loadFleet,
    };
    checkAuth();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
