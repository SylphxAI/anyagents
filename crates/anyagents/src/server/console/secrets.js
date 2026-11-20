"use strict";

(() => {
  const secretsState = {
    secrets: [],
    agents: [],
    selectedGrantAgent: "",
    selectedGrantSecrets: new Set(),
  };

  function bridge() {
    return window.AnyAgentsAdmin || null;
  }

  const $ = (id) => document.getElementById(id);

  function parseKeyValueLines(raw) {
    const env = {};
    const lines = raw.split(/\r?\n/);
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = line.indexOf("=");
      if (eq <= 0) {
        throw new Error(`Line ${i + 1}: expected KEY=VALUE`);
      }
      const key = line.slice(0, eq).trim();
      const val = line.slice(eq + 1);
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
        throw new Error(`Line ${i + 1}: invalid env var name "${key}"`);
      }
      env[key] = val;
    }
    if (Object.keys(env).length === 0) {
      throw new Error("Enter at least one KEY=VALUE line");
    }
    return env;
  }

  async function loadSecretsTab() {
    const ui = bridge();
    if (!ui) return;
    try {
      const [secretsResp, agentsResp] = await Promise.all([
        ui.api("/v1/secrets"),
        ui.api("/v1/agents?state=all"),
      ]);
      secretsState.secrets = (secretsResp && secretsResp.secrets) || [];
      secretsState.agents = (agentsResp && agentsResp.agents) || [];
      renderSecretsTable();
      populateGrantAgentSelect();
      await loadSelectedAgentGrants();
    } catch (err) {
      ui.toast(`Failed to load secrets: ${err.message}`, "error");
    }
  }

  function renderSecretsTable() {
    const ui = bridge();
    const wrap = $("secrets-table");
    const count = $("secrets-count");
    if (!ui || !wrap) return;
    const list = secretsState.secrets;
    if (count) {
      count.textContent = `${list.length} secret bundle(s)`;
    }
    ui.clearNode(wrap);
    if (list.length === 0) {
      wrap.appendChild(
        ui.el("div", {
          class: "empty-state",
          text: "No encrypted deployment secrets configured yet. Use the write-only form below to add one.",
        })
      );
      return;
    }

    const tbody = ui.el(
      "tbody",
      {},
      ...list.map((sec) => {
        const keyPills = ui.el(
          "div",
          { class: "badge-row" },
          ...(sec.env_keys || []).map((k) => ui.pill(k, "accent"))
        );
        const grantedNames =
          sec.granted_agents && sec.granted_agents.length > 0
            ? sec.granted_agents
            : sec.granted_agent_ids || [];
        const grantedEl =
          grantedNames.length > 0
            ? ui.el(
                "div",
                { class: "badge-row" },
                ...grantedNames.map((n) => ui.pill(n, "success"))
              )
            : ui.el("span", { class: "muted text-sm", text: "none" });
        const rotateBtn = ui.el("button", {
          class: "btn btn-secondary btn-sm",
          type: "button",
          text: "Rotate",
          onClick: () => {
            const nameInput = $("secret-name");
            const linesInput = $("secret-env-lines");
            if (nameInput) {
              nameInput.value = sec.name;
              nameInput.focus();
            }
            if (linesInput) {
              linesInput.value = "";
            }
          },
        });
        const delBtn = ui.el("button", {
          class: "btn btn-danger btn-sm",
          type: "button",
          text: "Delete",
          onClick: () => deleteSecret(sec.name),
        });
        return ui.el(
          "tr",
          {},
          ui.el("td", { class: "mono", text: sec.name }),
          ui.el("td", {}, keyPills),
          ui.el("td", { class: "mono text-sm", text: ui.fmtTime(sec.updated_at_ms) }),
          ui.el("td", {}, grantedEl),
          ui.el("td", {}, ui.el("div", { class: "toolbar-group" }, rotateBtn, delBtn))
        );
      })
    );

    const tbl = ui.el(
      "table",
      {},
      ui.el(
        "thead",
        {},
        ui.el(
          "tr",
          {},
          ui.el("th", { text: "Secret Name" }),
          ui.el("th", { text: "Env Keys" }),
          ui.el("th", { text: "Updated" }),
          ui.el("th", { text: "Granted Agents" }),
          ui.el("th", { text: "Actions" })
        )
      ),
      tbody
    );
    wrap.appendChild(tbl);
  }

  async function deleteSecret(name) {
    const ui = bridge();
    if (!ui) return;
    if (
      !window.confirm(
        `Delete secret "${name}"? This also revokes it from all granted agents.`
      )
    ) {
      return;
    }
    try {
      await ui.api(`/v1/secrets/${encodeURIComponent(name)}`, { method: "DELETE" });
      ui.toast(`Deleted secret "${name}"`, "success");
      await loadSecretsTab();
    } catch (err) {
      ui.toast(`Delete failed: ${err.message}`, "error");
    }
  }

  function populateGrantAgentSelect() {
    const ui = bridge();
    const select = $("secret-grant-agent");
    if (!ui || !select) return;
    const prev = secretsState.selectedGrantAgent || select.value;
    ui.clearNode(select);
    const active = secretsState.agents.filter((a) => a.state !== "archived");
    const archived = secretsState.agents.filter((a) => a.state === "archived");
    const ordered = active.concat(archived);
    if (ordered.length === 0) {
      select.appendChild(ui.el("option", { value: "", text: "(no agents)" }));
      secretsState.selectedGrantAgent = "";
      return;
    }
    for (const a of ordered) {
      const label = `${a.name} (${a.harness}${a.state === "archived" ? ", archived" : ""})`;
      select.appendChild(ui.el("option", { value: a.name, text: label }));
    }
    const found = ordered.find((a) => a.name === prev);
    secretsState.selectedGrantAgent = found ? found.name : ordered[0].name;
    select.value = secretsState.selectedGrantAgent;
  }

  async function loadSelectedAgentGrants() {
    const ui = bridge();
    const agentName = secretsState.selectedGrantAgent;
    if (!ui) return;
    if (!agentName) {
      secretsState.selectedGrantSecrets = new Set();
      renderGrantCheckboxes();
      return;
    }
    try {
      const resp = await ui.api(
        `/v1/agents/${encodeURIComponent(agentName)}/secrets`
      );
      secretsState.selectedGrantSecrets = new Set((resp && resp.secrets) || []);
    } catch (err) {
      secretsState.selectedGrantSecrets = new Set();
      ui.toast(`Failed to load grants for "${agentName}": ${err.message}`, "error");
    }
    renderGrantCheckboxes();
  }

  function renderGrantCheckboxes() {
    const ui = bridge();
    const wrap = $("secret-grant-checkboxes");
    if (!ui || !wrap) return;
    ui.clearNode(wrap);
    if (!secretsState.selectedGrantAgent) {
      wrap.appendChild(
        ui.el("div", { class: "muted text-sm", text: "Select an agent to edit grants." })
      );
      return;
    }
    if (secretsState.secrets.length === 0) {
      wrap.appendChild(
        ui.el("div", {
          class: "muted text-sm",
          text: "Create at least one deployment secret first.",
        })
      );
      return;
    }
    for (const sec of secretsState.secrets) {
      const checked = secretsState.selectedGrantSecrets.has(sec.name);
      const cb = ui.el("input", {
        type: "checkbox",
        value: sec.name,
        checked,
        onChange: (evt) => {
          if (evt.target.checked) {
            secretsState.selectedGrantSecrets.add(sec.name);
          } else {
            secretsState.selectedGrantSecrets.delete(sec.name);
          }
        },
      });
      const label = ui.el(
        "label",
        { class: "checkbox-label secret-grant-item" },
        cb,
        ui.el("span", { class: "mono", text: sec.name }),
        ui.el("span", {
          class: "muted text-sm",
          text: `(${(sec.env_keys || []).join(", ")})`,
        })
      );
      wrap.appendChild(label);
    }
  }

  function renderInspectorSecrets(agent) {
    const ui = bridge();
    const container = $("insp-secrets-list");
    if (!ui || !container) return;
    ui.clearNode(container);
    const grants = (agent && agent.secrets) || [];
    if (grants.length === 0) {
      container.appendChild(
        ui.el("span", { class: "muted text-sm", text: "No secrets granted." })
      );
    } else {
      for (const name of grants) {
        container.appendChild(ui.pill(name, "accent"));
      }
    }
    const editBtn = $("btn-manage-agent-secrets");
    if (editBtn && agent) {
      editBtn.dataset.agentName = agent.name;
    }
  }

  function initSecretsUI() {
    const refreshBtn = $("secrets-refresh-btn");
    if (refreshBtn) {
      refreshBtn.addEventListener("click", loadSecretsTab);
    }

    const maskToggle = $("secret-mask-toggle");
    const envTextarea = $("secret-env-lines");
    if (maskToggle && envTextarea) {
      maskToggle.addEventListener("change", () => {
        envTextarea.classList.toggle("secret-writeonly", maskToggle.checked);
      });
    }

    const upsertForm = $("form-secret-upsert");
    if (upsertForm) {
      upsertForm.addEventListener("submit", async (evt) => {
        evt.preventDefault();
        const ui = bridge();
        if (!ui) return;
        const nameInput = $("secret-name");
        const linesInput = $("secret-env-lines");
        const name = nameInput ? nameInput.value.trim() : "";
        const rawLines = linesInput ? linesInput.value : "";
        let env;
        try {
          if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(name)) {
            throw new Error(
              "Secret name must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$"
            );
          }
          env = parseKeyValueLines(rawLines);
        } catch (err) {
          ui.toast(err.message, "error");
          return;
        }
        // Write-only: clear plaintext input immediately before awaiting network response.
        if (linesInput) {
          linesInput.value = "";
        }
        try {
          await ui.api(`/v1/secrets/${encodeURIComponent(name)}`, {
            method: "PUT",
            body: { env },
          });
          if (nameInput) {
            nameInput.value = "";
          }
          ui.toast(`Encrypted & saved secret "${name}"`, "success");
          await loadSecretsTab();
        } catch (err) {
          ui.toast(`Save secret failed: ${err.message}`, "error");
        }
      });
    }

    const agentSelect = $("secret-grant-agent");
    if (agentSelect) {
      agentSelect.addEventListener("change", async () => {
        secretsState.selectedGrantAgent = agentSelect.value;
        await loadSelectedAgentGrants();
      });
    }

    const grantForm = $("form-agent-secrets");
    if (grantForm) {
      grantForm.addEventListener("submit", async (evt) => {
        evt.preventDefault();
        const ui = bridge();
        const agentName = secretsState.selectedGrantAgent;
        if (!ui || !agentName) return;
        const secrets = Array.from(secretsState.selectedGrantSecrets).sort();
        try {
          await ui.api(`/v1/agents/${encodeURIComponent(agentName)}/secrets`, {
            method: "PUT",
            body: { secrets },
          });
          ui.toast(`Updated secret grants for "${agentName}"`, "success");
          await loadSecretsTab();
        } catch (err) {
          ui.toast(`Update grants failed: ${err.message}`, "error");
        }
      });
    }

    const manageBtn = $("btn-manage-agent-secrets");
    if (manageBtn) {
      manageBtn.addEventListener("click", () => {
        const ui = bridge();
        if (!ui) return;
        const target = manageBtn.dataset.agentName || "";
        if (target) {
          secretsState.selectedGrantAgent = target;
        }
        ui.switchTab("secrets");
      });
    }

    window.addEventListener("anyagents:tab", (evt) => {
      if (evt.detail === "secrets") {
        loadSecretsTab();
      }
    });

    window.addEventListener("anyagents:agent", (evt) => {
      renderInspectorSecrets(evt.detail);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSecretsUI);
  } else {
    initSecretsUI();
  }
})();
