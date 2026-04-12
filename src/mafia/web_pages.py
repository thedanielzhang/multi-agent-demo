from __future__ import annotations

from textwrap import dedent


def chat_page_html() -> str:
    return dedent(
        """\
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Mafia Chatroom</title>
          <style>
            :root {
              --bg: #f4efe6;
              --panel: #fff9f0;
              --ink: #1f1c18;
              --muted: #6f675f;
              --accent: #b44c2b;
              --line: #d8cfc2;
            }
            body { margin: 0; font-family: Georgia, "Times New Roman", serif; background: linear-gradient(180deg, #f7f0e4, #efe7d8); color: var(--ink); }
            .shell { max-width: 1100px; margin: 0 auto; padding: 24px; display: grid; gap: 16px; }
            .card { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 12px 32px rgba(70, 53, 32, 0.08); }
            .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; }
            h1, h2 { margin: 0 0 8px; }
            .subtle { color: var(--muted); }
            .grid { display: grid; grid-template-columns: 1fr 320px; gap: 16px; }
            .feed { height: 480px; overflow-y: auto; display: grid; gap: 10px; padding-right: 8px; }
            .debug-feed { height: 420px; overflow-y: auto; display: grid; gap: 10px; padding-right: 8px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.88rem; }
            .message { border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; background: #fffdf9; }
            .message[data-kind="agent"] { border-left: 5px solid #c17c44; }
            .message[data-kind="human"] { border-left: 5px solid #4f7a5d; }
            .message[data-kind="system"] { border-left: 5px solid #7c7c7c; }
            .debug-item { border: 1px solid var(--line); border-radius: 12px; padding: 8px 10px; background: #fffdf9; }
            .debug-item strong { color: var(--accent); }
            .debug-header { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }
            .debug-meta { color: var(--muted); font-size: 0.84rem; margin-top: 3px; }
            .debug-summary { margin-top: 6px; line-height: 1.45; }
            .debug-keyvals { margin-top: 6px; display: grid; gap: 4px; }
            .debug-keyval { display: flex; gap: 8px; flex-wrap: wrap; }
            .debug-label { color: var(--muted); min-width: 74px; }
            .debug-value { font-weight: 600; }
            .debug-details { margin-top: 8px; }
            .debug-details summary { cursor: pointer; color: var(--accent); }
            .debug-json { margin-top: 6px; white-space: pre-wrap; word-break: break-word; background: #f7f1e8; border: 1px solid var(--line); border-radius: 10px; padding: 8px; font-size: 0.8rem; line-height: 1.4; }
            .message-head { display: flex; justify-content: space-between; gap: 12px; font-size: 0.92rem; color: var(--muted); }
            .message-text { margin-top: 6px; white-space: pre-wrap; line-height: 1.45; }
            .controls, .join-form, .compose-form { display: grid; gap: 10px; }
            .controls-row { display: flex; flex-wrap: wrap; gap: 8px; }
            input, textarea, select, button { font: inherit; }
            input, textarea, select { width: 100%; border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; background: white; box-sizing: border-box; }
            textarea { min-height: 84px; resize: vertical; }
            button { border: 0; border-radius: 999px; padding: 10px 16px; background: var(--accent); color: white; cursor: pointer; }
            button.secondary { background: #5a5a5a; }
            button.ghost { background: transparent; color: var(--accent); border: 1px solid var(--accent); }
            .badge { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #efe3d4; color: #5c3a22; font-size: 0.9rem; }
            .mini { font-size: 0.9rem; }
            a { color: var(--accent); }
            @media (max-width: 900px) {
              .grid { grid-template-columns: 1fr; }
              .feed { height: 360px; }
            }
          </style>
        </head>
        <body>
          <div class="shell">
            <div class="topbar card">
              <div>
                <h1>Mafia Chatroom</h1>
                <div class="subtle">Mixed human and agent chat for local user testing.</div>
              </div>
              <div><a href="/config">Configure Chatroom</a></div>
            </div>

            <div class="grid">
              <div class="card">
                <div class="topbar" style="margin-bottom: 12px;">
                  <h2>Live Room</h2>
                  <span id="run-badge" class="badge">idle</span>
                </div>
                <div id="feed" class="feed"></div>
              </div>

              <div class="controls">
                <div class="card">
                  <h2>Room Controls</h2>
                  <div class="controls-row">
                    <button id="start-btn">Start</button>
                    <button id="pause-btn" class="secondary">Pause</button>
                    <button id="resume-btn" class="secondary">Resume</button>
                    <button id="stop-btn" class="ghost">Stop</button>
                  </div>
                  <div id="status-text" class="subtle mini" style="margin-top: 10px;"></div>
                </div>

                <div class="card">
                  <h2>Join</h2>
                  <form id="join-form" class="join-form">
                    <input id="display-name" placeholder="Display name" value="Human" />
                    <input id="participant-id" placeholder="Participant id (optional)" />
                    <button type="submit">Join Chat</button>
                  </form>
                  <div id="join-status" class="subtle mini" style="margin-top: 8px;"></div>
                </div>

                <div class="card">
                  <h2>Send Message</h2>
                  <form id="compose-form" class="compose-form">
                    <textarea id="message-text" placeholder="Say something to the room..."></textarea>
                    <button type="submit">Send</button>
                  </form>
                </div>

                <div class="card">
                  <div class="topbar" style="margin-bottom: 12px;">
                    <h2>Agent Debug</h2>
                    <button id="clear-debug-btn" type="button" class="ghost">Clear</button>
                  </div>
                  <div id="debug-feed" class="debug-feed"></div>
                </div>
              </div>
            </div>
          </div>

          <script>
            const feed = document.getElementById("feed");
            const debugFeed = document.getElementById("debug-feed");
            const runBadge = document.getElementById("run-badge");
            const statusText = document.getElementById("status-text");
            const joinStatus = document.getElementById("join-status");
            const state = { ws: null, joined: false, participantId: null, displayName: null, seenMessageIds: new Set(), seenDebugKeys: new Set() };

            function wsUrl() {
              const protocol = window.location.protocol === "https:" ? "wss" : "ws";
              return `${protocol}://${window.location.host}/ws`;
            }

            function appendMessage(message) {
              if (state.seenMessageIds.has(message.message_id)) {
                return;
              }
              state.seenMessageIds.add(message.message_id);
              const item = document.createElement("div");
              item.className = "message";
              item.dataset.kind = message.kind;
              item.innerHTML = `
                <div class="message-head">
                  <strong>${escapeHtml(message.display_name)} <span class="subtle">(${escapeHtml(message.kind)})</span></strong>
                  <span>${new Date(message.created_at).toLocaleTimeString()}</span>
                </div>
                <div class="message-text">${escapeHtml(message.text)}</div>
              `;
              feed.appendChild(item);
              feed.scrollTop = feed.scrollHeight;
            }

            function appendDebugEvent(entry) {
              const key = `${entry.subject}:${entry.timestamp}:${entry.event?.agent_id || ""}:${entry.event?.worker_kind || ""}:${entry.event?.command_subject || ""}`;
              if (state.seenDebugKeys.has(key)) {
                return;
              }
              state.seenDebugKeys.add(key);
              const data = entry.event || {};
              const item = document.createElement("div");
              item.className = "debug-item";
              item.innerHTML = renderDebugEvent(entry);
              debugFeed.appendChild(item);
              debugFeed.scrollTop = debugFeed.scrollHeight;
            }

            function renderDebugEvent(entry) {
              const data = entry.event || {};
              const decision = data.output_summary?.decision;
              const reason = data.output_summary?.reason;
              const decisionBlock = decision || reason ? `
                <div class="debug-keyvals">
                  ${decision ? `<div class="debug-keyval"><span class="debug-label">decision</span><span class="debug-value">${escapeHtml(decision)}</span></div>` : ""}
                  ${reason ? `<div class="debug-keyval"><span class="debug-label">reason</span><span class="debug-value">${escapeHtml(reason)}</span></div>` : ""}
                </div>
              ` : "";
              const detailsBlock = renderDebugDetails(data);
              return `
                <div class="debug-header">
                  <div><strong>${escapeHtml(data.agent_id || "?")}</strong> ${escapeHtml(data.worker_kind || "worker")}</div>
                  <div class="subtle">${escapeHtml(entry.subject || "")}</div>
                </div>
                <div class="debug-meta">${escapeHtml(new Date(entry.timestamp).toLocaleTimeString())} · ${escapeHtml(data.command_subject || "")}</div>
                ${decisionBlock}
                <div class="debug-summary">${escapeHtml(formatDebugEntry(entry))}</div>
                ${detailsBlock}
              `;
            }

            function renderDebugDetails(data) {
              const sections = [];
              if (data.input_summary) {
                sections.push(`
                  <details class="debug-details">
                    <summary>Input Summary</summary>
                    <pre class="debug-json">${escapeHtml(prettyJson(data.input_summary))}</pre>
                  </details>
                `);
              }
              if (data.output_summary) {
                sections.push(`
                  <details class="debug-details">
                    <summary>Output Summary</summary>
                    <pre class="debug-json">${escapeHtml(prettyJson(data.output_summary))}</pre>
                  </details>
                `);
              }
              if (data.error) {
                sections.push(`
                  <details class="debug-details">
                    <summary>Error</summary>
                    <pre class="debug-json">${escapeHtml(String(data.error))}</pre>
                  </details>
                `);
              }
              return sections.join("");
            }

            function formatDebugEntry(entry) {
              const data = entry.event || {};
              if (entry.subject === "debug.event.agent.call.started") {
                return `start · ${summarizeObject(data.input_summary, 320)}`;
              }
              if (entry.subject === "debug.event.agent.call.completed") {
                return `${Number(data.duration_ms || 0).toFixed(1)}ms · ${summarizeObject(data.output_summary, 320)}`;
              }
              if (entry.subject === "debug.event.agent.call.failed") {
                return `failed after ${Number(data.duration_ms || 0).toFixed(1)}ms · ${data.error || "unknown error"}`;
              }
              return summarizeObject(data, 320);
            }

            function summarizeObject(value, limit = 220) {
              if (!value) return "";
              try {
                const text = JSON.stringify(value);
                return text.length > limit ? text.slice(0, limit - 3) + "..." : text;
              } catch (_error) {
                return String(value);
              }
            }

            function prettyJson(value) {
              try {
                return JSON.stringify(value, null, 2);
              } catch (_error) {
                return String(value);
              }
            }

            function escapeHtml(value) {
              return String(value ?? "").replace(/[&<>\"']/g, (char) => ({
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#39;'
              }[char]));
            }

            async function refreshStatus() {
              const response = await fetch("/status");
              const payload = await response.json();
              runBadge.textContent = payload.run_state;
              statusText.textContent = `Scenario: ${payload.scenario} | Mode: ${payload.mode} | Messages: ${payload.message_count}`;
            }

            async function refreshDebug() {
              const response = await fetch("/api/debug");
              const payload = await response.json();
              debugFeed.innerHTML = "";
              state.seenDebugKeys.clear();
              payload.forEach((entry) => appendDebugEvent(entry));
            }

            async function refreshMessages() {
              const response = await fetch("/api/messages");
              const payload = await response.json();
              feed.innerHTML = "";
              state.seenMessageIds.clear();
              payload.forEach((message) => appendMessage(message));
            }

            function connectSocket() {
              const ws = new WebSocket(wsUrl());
              state.ws = ws;
              ws.onmessage = (event) => {
                const payload = JSON.parse(event.data);
                if (payload.type === "join") {
                  state.joined = true;
                  joinStatus.textContent = `Joined as ${payload.participant.display_name}`;
                  runBadge.textContent = payload.run_state;
                  return;
                }
                if (payload.type === "message_committed") {
                  appendMessage(payload.message);
                  refreshStatus().catch(console.error);
                  return;
                }
                if (payload.type === "run_state_changed") {
                  runBadge.textContent = payload.state;
                  refreshStatus().catch(console.error);
                  return;
                }
                if (payload.type === "debug_event") {
                  appendDebugEvent(payload);
                  return;
                }
                if (payload.type === "error") {
                  joinStatus.textContent = payload.message;
                }
              };
              ws.onclose = () => {
                state.joined = false;
                joinStatus.textContent = "Socket disconnected. Reload to reconnect.";
              };
            }

            document.getElementById("join-form").addEventListener("submit", (event) => {
              event.preventDefault();
              if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
                joinStatus.textContent = "Socket not connected yet.";
                return;
              }
              const displayName = document.getElementById("display-name").value || "Human";
              const participantId = document.getElementById("participant-id").value || undefined;
              state.displayName = displayName;
              state.participantId = participantId;
              state.ws.send(JSON.stringify({
                type: "join",
                display_name: displayName,
                participant_id: participantId
              }));
            });

            document.getElementById("compose-form").addEventListener("submit", (event) => {
              event.preventDefault();
              if (!state.joined || !state.ws || state.ws.readyState !== WebSocket.OPEN) {
                joinStatus.textContent = "Join the chat before sending.";
                return;
              }
              const textarea = document.getElementById("message-text");
              const text = textarea.value.trim();
              if (!text) return;
              state.ws.send(JSON.stringify({
                type: "send_message",
                text,
                client_message_id: `browser-${Date.now()}`
              }));
              textarea.value = "";
            });

            document.getElementById("start-btn").addEventListener("click", async () => {
              await fetch("/start", { method: "POST" });
              await refreshStatus();
            });
            document.getElementById("pause-btn").addEventListener("click", async () => {
              await fetch("/pause", { method: "POST" });
              await refreshStatus();
            });
            document.getElementById("resume-btn").addEventListener("click", async () => {
              await fetch("/resume", { method: "POST" });
              await refreshStatus();
            });
            document.getElementById("stop-btn").addEventListener("click", async () => {
              await fetch("/stop", { method: "POST" });
              await refreshStatus();
            });
            document.getElementById("clear-debug-btn").addEventListener("click", () => {
              debugFeed.innerHTML = "";
              state.seenDebugKeys.clear();
            });

            connectSocket();
            refreshStatus().catch(console.error);
            refreshMessages().catch(console.error);
            refreshDebug().catch(console.error);
          </script>
        </body>
        </html>
        """
    )


def config_page_html() -> str:
    return dedent(
        """\
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Chatroom Config</title>
          <style>
            :root {
              --bg: #f4efe6;
              --panel: #fff9f0;
              --ink: #1f1c18;
              --muted: #6f675f;
              --accent: #b44c2b;
              --line: #d8cfc2;
            }
            body { margin: 0; font-family: Georgia, "Times New Roman", serif; background: linear-gradient(180deg, #f7f0e4, #efe7d8); color: var(--ink); }
            .shell { max-width: 1100px; margin: 0 auto; padding: 24px; display: grid; gap: 16px; }
            .card { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 12px 32px rgba(70, 53, 32, 0.08); }
            .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; }
            .form-grid { display: grid; gap: 12px; grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .full { grid-column: 1 / -1; }
            .agents { display: grid; gap: 14px; }
            .agent-card { border: 1px solid var(--line); border-radius: 14px; padding: 12px; background: white; display: grid; gap: 10px; }
            .agent-grid { display: grid; gap: 10px; grid-template-columns: repeat(2, minmax(0, 1fr)); }
            input, textarea, select, button { font: inherit; }
            input, textarea, select { width: 100%; border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; background: white; box-sizing: border-box; }
            textarea { min-height: 84px; resize: vertical; }
            button { border: 0; border-radius: 999px; padding: 10px 16px; background: var(--accent); color: white; cursor: pointer; }
            button.secondary { background: #5a5a5a; }
            button.ghost { background: transparent; color: var(--accent); border: 1px solid var(--accent); }
            .controls { display: flex; flex-wrap: wrap; gap: 8px; }
            .subtle { color: var(--muted); }
            label { display: grid; gap: 6px; font-size: 0.95rem; }
            @media (max-width: 900px) {
              .form-grid, .agent-grid { grid-template-columns: 1fr; }
            }
          </style>
        </head>
        <body>
          <div class="shell">
            <div class="topbar card">
              <div>
                <h1 style="margin: 0 0 8px;">Chatroom Config</h1>
                <div class="subtle">Edit the next room run. Changes apply on the next start.</div>
              </div>
              <div><a href="/">Back to Chat</a></div>
            </div>

            <div class="card">
              <div id="save-status" class="subtle">Loading config…</div>
            </div>

            <form id="config-form" class="card" style="display: grid; gap: 20px;">
              <section>
                <h2 style="margin-top: 0;">Room</h2>
                <div class="form-grid">
                  <label class="full">Scenario
                    <textarea id="scenario"></textarea>
                  </label>
                  <label>Mode
                    <select id="mode"></select>
                  </label>
                  <label>Runtime Provider
                    <select id="provider"></select>
                  </label>
                  <label>Model
                    <input id="model" />
                  </label>
                  <label>Max Duration (seconds, blank = unlimited)
                    <input id="max-duration" type="number" step="0.1" min="1" />
                  </label>
                  <label>Max Messages (blank = unlimited)
                    <input id="max-messages" type="number" min="1" />
                  </label>
                  <label>Typing Words / Second
                    <input id="typing-speed" type="number" step="0.1" min="0.1" />
                  </label>
                </div>
              </section>

              <section>
                <div class="topbar" style="margin-bottom: 12px;">
                  <h2 style="margin: 0;">Agents</h2>
                  <button type="button" id="add-agent" class="secondary">Add Agent</button>
                </div>
                <div id="agents" class="agents"></div>
              </section>

              <div class="controls">
                <button type="submit">Save Config</button>
                <button type="button" id="save-start" class="secondary">Save And Start</button>
              </div>
            </form>
          </div>

          <script>
            const saveStatus = document.getElementById("save-status");
            const agentsContainer = document.getElementById("agents");
            let schema = null;
            let config = null;

            function makeAgentCard(agent = null) {
              const wrapper = document.createElement("div");
              wrapper.className = "agent-card";
              const data = agent || {
                id: "",
                display_name: "",
                goals: [],
                style_prompt: "Speak naturally.",
                max_words: 12,
                personality: {
                  talkativeness: 0.5,
                  confidence: 0.5,
                  reactivity: 0.5,
                  topic_loyalty: 0.5
                },
                scheduler: { tick_rate_seconds: 1.0 },
                generation: {
                  tick_rate_seconds: 0.5,
                  buffer_size: 5,
                  staleness_window_seconds: 30.0
                },
                context: { memory_decay: 0.8 }
              };
              wrapper.innerHTML = `
                <div class="topbar">
                  <strong>${data.display_name || "New agent"}</strong>
                  <button type="button" class="ghost remove-agent">Remove</button>
                </div>
                <div class="agent-grid">
                  <label>Agent Id <input data-field="id" value="${escapeAttr(data.id)}" /></label>
                  <label>Display Name <input data-field="display_name" value="${escapeAttr(data.display_name)}" /></label>
                  <label class="full">Goals (comma separated)
                    <input data-field="goals" value="${escapeAttr((data.goals || []).join(", "))}" />
                  </label>
                  <label class="full">Style Prompt
                    <textarea data-field="style_prompt">${escapeText(data.style_prompt || "")}</textarea>
                  </label>
                  <label>Max Words <input data-field="max_words" type="number" min="1" value="${Number(data.max_words || 12)}" /></label>
                  <label>Scheduler Tick <input data-field="scheduler_tick" type="number" step="0.05" min="0.05" value="${Number(data.scheduler?.tick_rate_seconds || 1.0)}" /></label>
                  <label>Generation Tick <input data-field="generation_tick" type="number" step="0.05" min="0.05" value="${Number(data.generation?.tick_rate_seconds || 0.5)}" /></label>
                  <label>Buffer Size <input data-field="buffer_size" type="number" min="1" value="${Number(data.generation?.buffer_size || 5)}" /></label>
                  <label>Staleness Window <input data-field="staleness_window" type="number" step="0.5" min="1" value="${Number(data.generation?.staleness_window_seconds || 30)}" /></label>
                  <label>Talkativeness <input data-field="talkativeness" type="number" step="0.05" min="0" max="1" value="${Number(data.personality?.talkativeness || 0.5)}" /></label>
                  <label>Confidence <input data-field="confidence" type="number" step="0.05" min="0" max="1" value="${Number(data.personality?.confidence || 0.5)}" /></label>
                  <label>Reactivity <input data-field="reactivity" type="number" step="0.05" min="0" max="1" value="${Number(data.personality?.reactivity || 0.5)}" /></label>
                  <label>Topic Loyalty <input data-field="topic_loyalty" type="number" step="0.05" min="0" max="1" value="${Number(data.personality?.topic_loyalty || 0.5)}" /></label>
                  <label>Memory Decay <input data-field="memory_decay" type="number" step="0.05" min="0" max="1" value="${Number(data.context?.memory_decay || 0.8)}" /></label>
                </div>
              `;
              wrapper.querySelector(".remove-agent").addEventListener("click", () => wrapper.remove());
              return wrapper;
            }

            function escapeAttr(value) {
              return String(value ?? "").replace(/["&<>]/g, (char) => ({
                '"': "&quot;", "&": "&amp;", "<": "&lt;", ">": "&gt;"
              }[char]));
            }

            function escapeText(value) {
              return String(value ?? "").replace(/[&<>]/g, (char) => ({
                "&": "&amp;", "<": "&lt;", ">": "&gt;"
              }[char]));
            }

            function setSelectOptions(select, values) {
              select.innerHTML = "";
              values.forEach((value) => {
                const option = document.createElement("option");
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
              });
            }

            function renderConfig() {
              document.getElementById("scenario").value = config.chat.scenario;
              document.getElementById("mode").value = config.mode;
              document.getElementById("provider").value = config.runtime.provider;
              document.getElementById("model").value = config.runtime.model;
              document.getElementById("max-duration").value = config.chat.max_duration_seconds ?? "";
              document.getElementById("max-messages").value = config.chat.max_messages ?? "";
              document.getElementById("typing-speed").value = config.chat.typing_words_per_second;
              agentsContainer.innerHTML = "";
              (config.agents || []).forEach((agent) => agentsContainer.appendChild(makeAgentCard(agent)));
            }

            function optionalNumberValue(id) {
              const raw = document.getElementById(id).value.trim();
              if (!raw) return null;
              return Number(raw);
            }

            function collectConfig() {
              const agents = [...agentsContainer.children].map((card) => {
                const field = (name) => card.querySelector(`[data-field="${name}"]`).value;
                return {
                  id: field("id"),
                  display_name: field("display_name"),
                  goals: field("goals").split(",").map((item) => item.trim()).filter(Boolean),
                  style_prompt: field("style_prompt"),
                  max_words: Number(field("max_words")),
                  personality: {
                    talkativeness: Number(field("talkativeness")),
                    confidence: Number(field("confidence")),
                    reactivity: Number(field("reactivity")),
                    topic_loyalty: Number(field("topic_loyalty"))
                  },
                  scheduler: {
                    tick_rate_seconds: Number(field("scheduler_tick"))
                  },
                  generation: {
                    tick_rate_seconds: Number(field("generation_tick")),
                    buffer_size: Number(field("buffer_size")),
                    staleness_window_seconds: Number(field("staleness_window"))
                  },
                  context: {
                    memory_decay: Number(field("memory_decay"))
                  }
                };
              });
              return {
                ...config,
                mode: document.getElementById("mode").value,
                runtime: {
                  ...config.runtime,
                  provider: document.getElementById("provider").value,
                  model: document.getElementById("model").value
                },
                chat: {
                  ...config.chat,
                  scenario: document.getElementById("scenario").value,
                  max_duration_seconds: optionalNumberValue("max-duration"),
                  max_messages: optionalNumberValue("max-messages"),
                  typing_words_per_second: Number(document.getElementById("typing-speed").value)
                },
                agents
              };
            }

            async function saveConfig(startAfter = false) {
              const payload = collectConfig();
              const response = await fetch("/api/config", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
              });
              const body = await response.json();
              if (!response.ok) {
                saveStatus.textContent = body.detail || "Failed to save config.";
                return;
              }
              config = body.config;
              saveStatus.textContent = "Draft config saved.";
              renderConfig();
              if (startAfter) {
                const start = await fetch("/start", { method: "POST" });
                const startBody = await start.json();
                if (!start.ok) {
                  saveStatus.textContent = startBody.detail || "Failed to start room.";
                  return;
                }
                saveStatus.textContent = `Saved and started: ${startBody.run_state}`;
              }
            }

            async function boot() {
              schema = await (await fetch("/api/config/schema")).json();
              config = await (await fetch("/api/config")).json();
              setSelectOptions(document.getElementById("mode"), schema.modes);
              setSelectOptions(document.getElementById("provider"), schema.runtime_providers);
              renderConfig();
              saveStatus.textContent = "Draft config loaded.";
            }

            document.getElementById("add-agent").addEventListener("click", () => {
              agentsContainer.appendChild(makeAgentCard());
            });
            document.getElementById("config-form").addEventListener("submit", async (event) => {
              event.preventDefault();
              await saveConfig(false);
            });
            document.getElementById("save-start").addEventListener("click", async () => {
              await saveConfig(true);
            });

            boot().catch((error) => {
              console.error(error);
              saveStatus.textContent = "Failed to load config.";
            });
          </script>
        </body>
        </html>
        """
    )


__all__ = ["chat_page_html", "config_page_html"]
