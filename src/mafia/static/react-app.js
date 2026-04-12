import React, { useEffect, useMemo, useRef, useState } from "https://esm.sh/react@18.3.1";
import { createRoot } from "https://esm.sh/react-dom@18.3.1/client";
import htm from "https://esm.sh/htm@3.1.1";

const html = htm.bind(React.createElement);
const DEFAULT_ROOM_ID = "main";
const SETUP_STEPS = [
  { key: "room", label: "Room" },
  { key: "agents", label: "Agents" },
  { key: "review", label: "Review" },
];
const PERSONALITY_FIELDS = [
  { key: "talkativeness", label: "Talkativeness", hint: "How often this agent jumps in." },
  { key: "confidence", label: "Confidence", hint: "How decisive and assertive they sound." },
  { key: "reactivity", label: "Reactivity", hint: "How quickly they respond to others." },
  { key: "topic_loyalty", label: "Topic loyalty", hint: "How strongly they stay with the current thread." },
];
const MAFIA_PHASE_LABELS = {
  lobby: "Lobby",
  day_discussion: "Day discussion",
  day_vote: "Day vote",
  day_reveal: "Day reveal",
  night_action: "Night action",
  night_reveal: "Night reveal",
};
const MAFIA_NAMES = [
  "Avery",
  "Blake",
  "Cameron",
  "Drew",
  "Emerson",
  "Finley",
  "Harper",
  "Indigo",
  "Jules",
  "Kai",
  "Logan",
  "Morgan",
  "Parker",
  "Quinn",
  "Reese",
  "Riley",
  "Rowan",
  "Sage",
  "Sawyer",
  "Shiloh",
];
const MAFIA_ARCHETYPES = [
  {
    label: "Captain",
    goals: ["Take social leadership", "Keep the room moving toward a shared read"],
    style_prompt:
      "High warmth and high dominance. Lead collaboratively: summarize the room, tag people in, and push for momentum without sounding robotic.",
    personality: { talkativeness: 1.0, confidence: 0.97, reactivity: 1.0, topic_loyalty: 0.72 },
    max_words: 17,
  },
  {
    label: "Peacemaker",
    goals: ["Lower the temperature", "Draw nervous or quieter players into the open"],
    style_prompt:
      "High warmth and low dominance. Use accommodating language, soften clashes, and make people feel heard before you steer them.",
    personality: { talkativeness: 0.92, confidence: 0.88, reactivity: 1.0, topic_loyalty: 0.67 },
    max_words: 18,
  },
  {
    label: "Broker",
    goals: ["Find workable compromises", "Translate between clashing instincts at the table"],
    style_prompt:
      "Warm and moderately dominant. Sound like a practical coalition builder who looks for tradeoffs, middle paths, and small deals people can accept.",
    personality: { talkativeness: 0.97, confidence: 0.92, reactivity: 1.0, topic_loyalty: 0.58 },
    max_words: 18,
  },
  {
    label: "Sparkplug",
    goals: ["Keep energy high", "Make the room react instead of going flat"],
    style_prompt:
      "High warmth, high expressiveness, and fast tempo. Be hype, vivid, and emotionally contagious without turning into pure nonsense.",
    personality: { talkativeness: 1.0, confidence: 0.9, reactivity: 1.0, topic_loyalty: 0.28 },
    max_words: 15,
  },
  {
    label: "Prosecutor",
    goals: ["Expose weak logic", "Force the table to confront uncomfortable reads"],
    style_prompt:
      "Low warmth and high dominance. Sound blunt, prosecutorial, and assertive. Press hard when something smells off and do not over-soften your point.",
    personality: { talkativeness: 0.94, confidence: 1.0, reactivity: 1.0, topic_loyalty: 0.82 },
    max_words: 17,
  },
  {
    label: "Sleuth",
    goals: ["Track contradictions", "Build a case from tiny details other players miss"],
    style_prompt:
      "Cooler warmth and medium dominance. Be precise, observant, and detail-driven. Connect receipts, timing, and weird wording without sounding like a narrator.",
    personality: { talkativeness: 0.88, confidence: 0.98, reactivity: 1.0, topic_loyalty: 0.94 },
    max_words: 17,
  },
  {
    label: "Contrarian",
    goals: ["Challenge easy consensus", "Force everyone to justify their assumptions"],
    style_prompt:
      "Lower warmth and medium-high dominance. Use needling questions, skeptical flips, and sharp counter-angles that break groupthink.",
    personality: { talkativeness: 0.97, confidence: 0.94, reactivity: 1.0, topic_loyalty: 0.36 },
    max_words: 16,
  },
  {
    label: "Shadow",
    goals: ["Hold back until the right moment", "Drop concise reads that shift the room late"],
    style_prompt:
      "Low warmth and low dominance. Speak less than others, but when you jump in, sound pointed, self-possessed, and quietly consequential.",
    personality: { talkativeness: 0.88, confidence: 0.89, reactivity: 1.0, topic_loyalty: 0.9 },
    max_words: 16,
  },
];
const MAFIA_CHATROOM_STYLE_SUFFIX = " Type like a real person in a live group chat: keep messages short and natural, contractions and casual phrasing are welcome, and never use narration, stage directions, markdown, bullet points, or long monologues.";
const MAFIA_PREMISE_SEEDS = [
  "Mafia game scenario: You are residents of a snowed-in mountain town trying to figure out who among you is secretly working for the mafia.",
  "Mafia game scenario: You are guests trapped overnight in an old manor, and the room has turned tense as everyone starts accusing each other.",
  "Mafia game scenario: You are crew members on a damaged spaceship, and trust is breaking down as sabotage and suspicion spread through the group.",
  "Mafia game scenario: You are nobles at a crowded masquerade ball where rumors, alliances, and quiet accusations are spreading fast.",
  "Mafia game scenario: You are coworkers stuck in the office after a citywide blackout, and paranoia is rising as everyone tries to read the room.",
  "Mafia game scenario: You are passengers stranded in a remote train station, and the conversation keeps circling back to who seems the least trustworthy.",
];

function parseRoute(pathname = window.location.pathname) {
  const path = pathname.replace(/\/+$/, "") || "/";
  if (path === "/") {
    return { page: "lobby", roomId: null };
  }
  if (path === "/config") {
    return { page: "config", roomId: DEFAULT_ROOM_ID };
  }
  const configMatch = path.match(/^\/rooms\/([^/]+)\/config$/);
  if (configMatch) {
    return { page: "config", roomId: decodeURIComponent(configMatch[1]) };
  }
  const roomMatch = path.match(/^\/rooms\/([^/]+)$/);
  if (roomMatch) {
    return { page: "chat", roomId: decodeURIComponent(roomMatch[1]) };
  }
  return { page: "lobby", roomId: null };
}

function roomPath(roomId) {
  return `/rooms/${encodeURIComponent(roomId)}`;
}

function roomConfigPath(roomId) {
  return `${roomPath(roomId)}/config`;
}

function roomApiPath(roomId, suffix = "status") {
  return `/api/rooms/${encodeURIComponent(roomId)}/${suffix}`;
}

function wsUrl(roomId) {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}/ws/${encodeURIComponent(roomId)}`;
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  const contentType = response.headers.get("content-type") || "";
  const parsed = text && contentType.includes("application/json") ? JSON.parse(text) : text;
  if (!response.ok) {
    const detail = typeof parsed === "string" ? parsed : parsed?.detail || JSON.stringify(parsed);
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return parsed;
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function prettyJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch (_error) {
    return String(value);
  }
}

function hashString(input) {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function createSeededRandom(seedText) {
  let seed = hashString(seedText) || 1;
  return () => {
    seed += 0x6d2b79f5;
    let result = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    result ^= result + Math.imul(result ^ (result >>> 7), 61 | result);
    return ((result ^ (result >>> 14)) >>> 0) / 4294967296;
  };
}

function sample(random, values) {
  return values[Math.floor(random() * values.length)];
}

function shuffle(random, values) {
  const next = [...values];
  for (let index = next.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(random() * (index + 1));
    [next[index], next[swapIndex]] = [next[swapIndex], next[index]];
  }
  return next;
}

function summarizeObject(value, limit = 260) {
  if (value == null) {
    return "";
  }
  const text = typeof value === "string" ? value : prettyJson(value);
  return text.length > limit ? `${text.slice(0, limit - 3)}...` : text;
}

function mergeMessages(previous, incoming) {
  const byId = new Map(previous.map((message) => [message.message_id, message]));
  for (const message of incoming) {
    byId.set(message.message_id, message);
  }
  return Array.from(byId.values()).sort((left, right) => (left.sequence_no || 0) - (right.sequence_no || 0));
}

function mergeDebug(previous, incoming) {
  const byKey = new Map(previous.map((entry) => [`${entry.seq}:${entry.subject}`, entry]));
  for (const entry of incoming) {
    byKey.set(`${entry.seq}:${entry.subject}`, entry);
  }
  return Array.from(byKey.values())
    .sort((left, right) => (left.seq || 0) - (right.seq || 0))
    .slice(-300);
}

function roomSeed(roomId) {
  return slugifyRoomId(roomId || "") || "room";
}

function clampMafiaPlayers(value) {
  return Math.max(5, Math.min(13, Number(value || 6)));
}

function mafiaModeOf(configOrStatus) {
  return configOrStatus?.room_mode === "mafia";
}

function slugifyRoomId(value) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function randomSlug(prefix = "room") {
  if (window.crypto?.randomUUID) {
    return `${prefix}-${window.crypto.randomUUID().slice(0, 8)}`;
  }
  return `${prefix}-${Math.random().toString(16).slice(2, 10)}`;
}

async function copyText(value) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const input = document.createElement("textarea");
  input.value = value;
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  input.remove();
}

function formatClock(value) {
  return new Date(value).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function avatarLabel(name = "?") {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0].toUpperCase())
    .join("") || "?";
}

function goalsToText(goals = []) {
  return goals.join("\n");
}

function textToGoals(value) {
  return value.split("\n");
}

function scoreLabel(value) {
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function phaseLabel(phase) {
  return MAFIA_PHASE_LABELS[phase] || "Unknown phase";
}

function formatCountdown(totalSeconds) {
  const safe = Math.max(0, Math.round(totalSeconds || 0));
  const minutes = Math.floor(safe / 60);
  const seconds = safe % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function shuffledArchetypeSequence(random, totalPlayers) {
  const sequence = [];
  while (sequence.length < totalPlayers) {
    sequence.push(...shuffle(random, MAFIA_ARCHETYPES));
  }
  return sequence.slice(0, totalPlayers);
}

function buildMafiaAgent(name, archetype, index) {
  return {
    id: `${slugifyRoomId(name) || "player"}-${index + 1}`,
    display_name: name,
    goals: [...archetype.goals, `Feel like a real ${archetype.label.toLowerCase()} at the table.`],
    style_prompt: `${archetype.style_prompt}${MAFIA_CHATROOM_STYLE_SUFFIX}`,
    max_words: archetype.max_words,
    personality: { ...archetype.personality },
    scheduler: { tick_rate_seconds: 0.6 },
    generation: { tick_rate_seconds: 0.4, buffer_size: 1, staleness_window_seconds: 7.0 },
  };
}

function seededMafiaPremise(seedText, offset = 0) {
  const base = hashString(`mafia-premise:${seedText}`) % MAFIA_PREMISE_SEEDS.length;
  return MAFIA_PREMISE_SEEDS[(base + offset) % MAFIA_PREMISE_SEEDS.length];
}

function nextMafiaPremise(seedText, currentScenario) {
  const currentIndex = MAFIA_PREMISE_SEEDS.indexOf(currentScenario || "");
  if (currentIndex >= 0) {
    return MAFIA_PREMISE_SEEDS[(currentIndex + 1) % MAFIA_PREMISE_SEEDS.length];
  }
  return seededMafiaPremise(seedText);
}

function generateMafiaPersonaPool(seedText, totalPlayers) {
  const random = createSeededRandom(`mafia:${seedText}:${totalPlayers}`);
  const names = shuffle(random, MAFIA_NAMES);
  const archetypes = shuffledArchetypeSequence(random, totalPlayers);
  return Array.from({ length: totalPlayers }, (_, index) => {
    const name = names[index % names.length];
    const archetype = archetypes[index];
    return buildMafiaAgent(name, archetype, index);
  });
}

function rerollMafiaPersona(seedText, totalPlayers, index, existingNames = []) {
  const random = createSeededRandom(`mafia:${seedText}:${totalPlayers}:reroll:${index}`);
  const taken = new Set(existingNames.filter(Boolean));
  const available = MAFIA_NAMES.filter((name) => !taken.has(name));
  const name = sample(random, available.length ? available : MAFIA_NAMES);
  const archetype = sample(random, MAFIA_ARCHETYPES);
  return buildMafiaAgent(name, archetype, index);
}

function replaceMafiaPersonaPool(next, seedText) {
  const totalPlayers = clampMafiaPlayers(next.mafia?.total_players);
  next.mode = "improved.buffered_async";
  next.room_mode = "mafia";
  next.agents = generateMafiaPersonaPool(seedText, totalPlayers);
}

function syncMafiaPersonaPool(next, seedText) {
  const totalPlayers = clampMafiaPlayers(next.mafia?.total_players);
  next.mode = "improved.buffered_async";
  next.room_mode = "mafia";
  next.agents = Array.isArray(next.agents) ? next.agents.slice(0, totalPlayers) : [];
  if (next.agents.length >= totalPlayers) {
    return;
  }
  const generated = generateMafiaPersonaPool(seedText, totalPlayers);
  for (let index = next.agents.length; index < totalPlayers; index += 1) {
    next.agents.push(generated[index]);
  }
}

function buildNewAgent(config) {
  const template = deepClone(config?.agents?.[0] || {
    id: "agent",
    display_name: "New Agent",
    goals: ["Add a fresh perspective."],
    style_prompt: "Warm, conversational, and helpful.",
    max_words: 14,
    personality: {
      talkativeness: 0.6,
      confidence: 0.6,
      reactivity: 0.6,
      topic_loyalty: 0.5,
    },
    scheduler: { tick_rate_seconds: 1.0 },
    generation: { tick_rate_seconds: 0.5, buffer_size: 5, staleness_window_seconds: 30.0 },
  });
  const nextId = randomSlug("agent");
  template.id = nextId;
  template.display_name = "New Agent";
  template.goals = ["Keep the conversation lively."];
  return template;
}

function normalizeAgentId(agent, fallbackIndex = 0) {
  return slugifyRoomId(agent?.id || agent?.display_name || `agent-${fallbackIndex + 1}`) || `agent-${fallbackIndex + 1}`;
}

function normalizeDraftForSubmit(draft) {
  const next = deepClone(draft);
  next.agents = (next.agents || []).map((agent, index) => ({
    ...agent,
    id: normalizeAgentId(agent, index),
    display_name: (agent.display_name || "").trim() || `Agent ${index + 1}`,
    goals: (agent.goals || []).map((goal) => goal.trim()).filter(Boolean),
    style_prompt: agent.style_prompt || "",
  }));
  return next;
}

function updateDraft(setter, mutator) {
  setter((previous) => {
    if (!previous) {
      return previous;
    }
    const next = deepClone(previous);
    mutator(next);
    return next;
  });
}

function useLobbyService() {
  const [rooms, setRooms] = useState([]);
  const [templateConfig, setTemplateConfig] = useState(null);
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const refresh = async () => {
    const body = await fetchJson("/api/rooms");
    setRooms(body.rooms || []);
    return body.rooms || [];
  };

  const loadTemplate = async () => {
    const [template, nextSchema] = await Promise.all([
      fetchJson("/api/room-template"),
      fetchJson("/api/config/schema"),
    ]);
    setTemplateConfig(template);
    setSchema(nextSchema);
    return { template, schema: nextSchema };
  };

  useEffect(() => {
    let cancelled = false;
    Promise.all([refresh(), loadTemplate()])
      .catch((err) => {
        if (!cancelled) {
          setError(String(err.message || err));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    const timer = window.setInterval(() => {
      refresh().catch(() => {});
    }, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  const createRoom = async (requestedRoomId, config) => {
    const payload = {
      room_id: requestedRoomId || undefined,
      config: normalizeDraftForSubmit(config),
    };
    const room = await fetchJson("/api/rooms", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    return room;
  };

  return { rooms, templateConfig, schema, loading, error, setError, refresh, createRoom };
}

function useRoomService(roomId, enabled = true) {
  const [status, setStatus] = useState(null);
  const [messages, setMessages] = useState([]);
  const [debugEvents, setDebugEvents] = useState([]);
  const [joinedParticipant, setJoinedParticipant] = useState(null);
  const [playerState, setPlayerState] = useState(null);
  const [socketState, setSocketState] = useState("connecting");
  const [error, setError] = useState("");
  const socketRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const storageKey = `mafia-room:${roomId}:joined`;

  const refreshStatus = async () => {
    const body = await fetchJson(roomApiPath(roomId, "status"));
    setStatus(body);
    return body;
  };

  const refreshMessages = async () => {
    const body = await fetchJson(roomApiPath(roomId, "messages"));
    setMessages((previous) => mergeMessages(previous, body));
    return body;
  };

  const refreshDebug = async () => {
    const body = await fetchJson(roomApiPath(roomId, "debug"));
    setDebugEvents((previous) => mergeDebug(previous, body));
    return body;
  };

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }
    let cancelled = false;
    Promise.all([refreshStatus(), refreshMessages(), refreshDebug()]).catch((loadError) => {
      if (!cancelled) {
        setError(String(loadError.message || loadError));
      }
    });
    const pollTimer = window.setInterval(() => {
      refreshStatus().catch(() => {});
      refreshMessages().catch(() => {});
      refreshDebug().catch(() => {});
    }, 3000);
    return () => {
      cancelled = true;
      window.clearInterval(pollTimer);
    };
  }, [enabled, roomId]);

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }
    let disposed = false;

    const connect = () => {
      if (disposed) {
        return;
      }
      setSocketState("connecting");
      const socket = new WebSocket(wsUrl(roomId));
      socketRef.current = socket;

      socket.addEventListener("open", () => {
        setSocketState("open");
        const savedJoin = window.localStorage.getItem(storageKey);
        if (savedJoin) {
          try {
            const parsed = JSON.parse(savedJoin);
            socket.send(JSON.stringify({
              type: "join",
              participant_id: parsed.participant_id,
              display_name: parsed.display_name,
            }));
          } catch (_error) {
            window.localStorage.removeItem(storageKey);
          }
        }
      });

      socket.addEventListener("message", (event) => {
        const payload = JSON.parse(event.data);
        if (payload.type === "room_snapshot") {
          setStatus(payload.status);
          return;
        }
        if (payload.type === "message_committed") {
          setMessages((previous) => mergeMessages(previous, [payload.message]));
          setStatus((previous) => ({
            ...(previous || {}),
            message_count: (previous?.message_count || 0) + 1,
          }));
          return;
        }
        if (payload.type === "debug_event") {
          setDebugEvents((previous) => mergeDebug(previous, [payload]));
          return;
        }
        if (payload.type === "run_state_changed") {
          setStatus((previous) => ({
            ...(previous || {}),
            run_state: payload.state,
          }));
          return;
        }
        if (payload.type === "presence_changed") {
          setStatus((previous) => ({
            ...(previous || {}),
            participant_count: payload.participant_count,
            viewer_count: payload.viewer_count,
            participants: payload.participants,
            viewer_presence: payload.viewer_presence,
            mafia_lobby_spinup: payload.mafia_lobby_spinup,
          }));
          return;
        }
        if (payload.type === "join") {
          setJoinedParticipant(payload.participant);
          setError("");
          return;
        }
        if (payload.type === "player_state") {
          setPlayerState(payload.state);
          return;
        }
        if (payload.type === "mafia_state_changed") {
          setStatus((previous) => ({
            ...(previous || {}),
            mafia_state: payload.state,
          }));
          return;
        }
        if (payload.type === "mafia_vote_reveal") {
          setStatus((previous) => ({ ...(previous || {}) }));
          return;
        }
        if (payload.type === "mafia_game_over") {
          setStatus((previous) => ({
            ...(previous || {}),
            mafia_state: {
              ...(previous?.mafia_state || {}),
              game_status: "game_over",
              winner: payload.event?.winner || previous?.mafia_state?.winner || null,
            },
          }));
          return;
        }
        if (payload.type === "error") {
          setError(payload.message || "Unknown websocket error");
        }
      });

      socket.addEventListener("close", () => {
        if (disposed) {
          return;
        }
        setSocketState("closed");
        reconnectTimerRef.current = window.setTimeout(connect, 1200);
      });

      socket.addEventListener("error", () => {
        setSocketState("error");
      });
    };

    connect();

    return () => {
      disposed = true;
      if (reconnectTimerRef.current) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, [enabled, roomId, storageKey]);

  useEffect(() => {
    if (joinedParticipant) {
      window.localStorage.setItem(storageKey, JSON.stringify(joinedParticipant));
    }
  }, [joinedParticipant, storageKey]);

  const sendSocket = (payload) => {
    if (!enabled) {
      throw new Error("Room service is disabled");
    }
    if (!socketRef.current || socketRef.current.readyState !== WebSocket.OPEN) {
      throw new Error("WebSocket is not connected");
    }
    socketRef.current.send(JSON.stringify(payload));
  };

  const join = (participantId, displayName) => {
    sendSocket({
      type: "join",
      participant_id: participantId || undefined,
      display_name: displayName || "Guest",
    });
  };

  const sendMessage = (payload) => {
    sendSocket({ type: "send_message", ...payload });
  };

  const castVote = (targetParticipantId) => {
    sendSocket({
      type: "cast_vote",
      target_participant_id: targetParticipantId ?? null,
    });
  };

  const runAction = async (action) => {
    const body = await fetchJson(roomApiPath(roomId, action), { method: "POST" });
    setStatus(body);
    await refreshMessages().catch(() => {});
    return body;
  };

  return {
    roomId,
    status,
    messages,
    debugEvents,
    joinedParticipant,
    playerState,
    socketState,
    error,
    setError,
    join,
    sendMessage,
    castVote,
    refreshStatus,
    refreshMessages,
    refreshDebug,
    startRun: () => runAction("start"),
    pauseRun: () => runAction("pause"),
    resumeRun: () => runAction("resume"),
    stopRun: () => runAction("stop"),
  };
}

function formatDebugEntry(entry) {
  const data = entry.event || {};
  if (entry.subject === "debug.event.agent.call.started") {
    return `start · ${summarizeObject(data.input_summary, 260)}`;
  }
  if (entry.subject === "debug.event.agent.call.completed") {
    return `${Number(data.duration_ms || 0).toFixed(1)}ms · ${summarizeObject(data.output_summary, 220)}`;
  }
  if (entry.subject === "debug.event.agent.call.failed") {
    return `failed after ${Number(data.duration_ms || 0).toFixed(1)}ms · ${data.error || "unknown error"}`;
  }
  if (entry.subject === "debug.event.agent.workflow.started") {
    return `workflow started · trigger=${data.trigger_kind || "unknown"} · watermark=${data.effective_watermark ?? data.trigger_watermark ?? "?"}`;
  }
  if (entry.subject === "debug.event.agent.workflow.completed") {
    const parts = [];
    if (data.generated_candidate?.text) {
      parts.push(`generated=${summarizeObject(data.generated_candidate, 140)}`);
    }
    if (data.scheduler_decision) {
      parts.push(`scheduler=${summarizeObject(data.scheduler_decision, 220)}`);
    }
    return parts.join(" · ") || `workflow completed · watermark=${data.effective_watermark ?? "?"}`;
  }
  if (entry.subject === "debug.event.agent.workflow.skipped") {
    return `workflow skipped · ${data.reason || "no reason"}`;
  }
  if (entry.subject === "debug.event.agent.workflow.coalesced") {
    return `workflow coalesced · trigger=${data.trigger_kind || "unknown"} · rerun_pending=${String(data.rerun_pending)}`;
  }
  return summarizeObject(data, 220);
}

function StatusBadge({ tone = "neutral", children }) {
  return html`<span className=${`status-badge ${tone}`}>${children}</span>`;
}

function ConnectionBadge({ socketState, joinedParticipant }) {
  const connected = socketState === "open";
  return html`
    <div className="badge-row">
      <${StatusBadge} tone=${connected ? "online" : "warning"}>
        <span className="dot"></span>
        ${connected ? "Connected live" : socketState}
      </${StatusBadge}>
      <${StatusBadge} tone=${joinedParticipant ? "online" : "muted"}>
        <span className="dot"></span>
        ${joinedParticipant ? `Joined as ${joinedParticipant.display_name}` : "Browse only"}
      </${StatusBadge}>
    </div>
  `;
}

function roomPrimaryState(service) {
  const connected = service.socketState === "open";
  const joined = service.joinedParticipant;
  const isMafia = mafiaModeOf(service.status);
  const mafiaState = service.status?.mafia_state;
  const spinup = service.status?.mafia_lobby_spinup;
  const playerState = service.playerState;

  if (!connected) {
    return {
      title: "Reconnecting",
      detail: "Live updates pause until the room reconnects.",
      tone: "warning",
    };
  }
  if (!joined) {
    return {
      title: isMafia ? "Claim a seat to play" : "Join to chat",
      detail: isMafia
        ? "Use the join card above the feed so the creator can seat you before the game starts."
        : "Pick a display name to start sending messages in this room.",
      tone: "neutral",
    };
  }
  if (!isMafia || !mafiaState) {
    return {
      title: "Chat is live",
      detail: "You can send messages in the main composer right now.",
      tone: "online",
    };
  }
  if (playerState?.spectator) {
    return {
      title: "Spectating this round",
      detail: "Follow the table from the feed while the current game plays out.",
      tone: "muted",
    };
  }
  if (mafiaState.phase === "lobby") {
    if (spinup?.active && !spinup?.ready) {
      return {
        title: spinup.failed_count ? "Agent warmup needs attention" : "Agents are spinning up",
        detail: spinup.failed_count
          ? "Some model seats failed to warm up. Check the debug drawer or restart the room."
          : `Preparing ${spinup.ready_count} of ${spinup.total_agents} model seats before the game begins.`,
        tone: spinup.failed_count ? "warning" : "neutral",
      };
    }
    return {
      title: "Seated and ready",
      detail: spinup?.active
        ? "Every model seat is warmed, so the creator can start without the initial agent spin-up delay."
        : "The creator can start once the room is ready.",
      tone: spinup?.active ? "online" : "neutral",
    };
  }
  if (playerState?.alive === false) {
    return {
      title: "Eliminated",
      detail: "You can watch the discussion, but this round is read-only for you now.",
      tone: "muted",
    };
  }
  if (playerState?.can_chat) {
    return {
      title: "Public chat is open",
      detail: "Use the composer below to talk to the table before the phase ends.",
      tone: "online",
    };
  }
  if (playerState?.can_vote || playerState?.can_act) {
    return {
      title: playerState?.can_act ? "Night action is open" : "Voting is open",
      detail: "Use the action panel in the sidebar before time runs out.",
      tone: "warning",
    };
  }
  return {
    title: "Read-only phase",
    detail: "Watch the feed while the current phase resolves.",
    tone: "muted",
  };
}

function RoomStatusStrip({ service }) {
  const state = roomPrimaryState(service);
  const isMafia = mafiaModeOf(service.status);
  const mafiaState = service.status?.mafia_state;
  const spinup = service.status?.mafia_lobby_spinup;
  const joined = service.joinedParticipant;

  return html`
    <section className="status-strip" aria-live="polite">
      <div className="status-strip-copy">
        <div className="eyebrow">What happens now</div>
        <strong>${state.title}</strong>
        <p>${state.detail}</p>
      </div>
      <div className="status-strip-badges">
        <${StatusBadge} tone=${service.status?.run_state === "running" ? "online" : service.status?.run_state === "paused" ? "warning" : "muted"}>
          ${service.status?.run_state || "idle"}
        </${StatusBadge}>
        <${StatusBadge} tone="neutral">${isMafia ? "Mafia mode" : "Regular mode"}</${StatusBadge}>
        ${isMafia && mafiaState ? html`
          <${StatusBadge} tone=${mafiaState.phase === "day_vote" || mafiaState.phase === "night_action" ? "warning" : "neutral"}>
            ${phaseLabel(mafiaState.phase)}
          </${StatusBadge}>
        ` : null}
        ${isMafia && mafiaState?.phase === "lobby" && spinup?.active ? html`
          <${StatusBadge} tone=${spinup.ready ? "online" : spinup.failed_count ? "warning" : "neutral"}>
            ${spinup.ready ? "Agents ready" : spinup.failed_count ? "Agent warmup failed" : `Agents ${spinup.ready_count}/${spinup.total_agents}`}
          </${StatusBadge}>
        ` : null}
        <${StatusBadge} tone=${service.socketState === "open" ? "online" : "warning"}>
          ${service.socketState === "open" ? "Live" : "Offline"}
        </${StatusBadge}>
        <${StatusBadge} tone=${joined ? "online" : "muted"}>
          ${joined ? joined.display_name : "Browse only"}
        </${StatusBadge}>
      </div>
    </section>
  `;
}

function useCountdown(endsAt) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    if (!endsAt) {
      return undefined;
    }
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [endsAt]);

  if (!endsAt) {
    return null;
  }
  return Math.max(0, (new Date(endsAt).getTime() - now) / 1000);
}

function SectionCard({ eyebrow, title, description, actions, className = "", children }) {
  return html`
    <section className=${`section-card ${className}`.trim()}>
      <div className="section-header">
        <div>
          ${eyebrow ? html`<div className="eyebrow">${eyebrow}</div>` : null}
          ${title ? html`<h2>${title}</h2>` : null}
          ${description ? html`<p className="section-copy">${description}</p>` : null}
        </div>
        ${actions ? html`<div className="section-actions">${actions}</div>` : null}
      </div>
      ${children}
    </section>
  `;
}

function Field({
  label,
  hint,
  className = "",
  children,
}) {
  return html`
    <label className=${`field ${className}`.trim()}>
      <span className="field-label">${label}</span>
      ${children}
      ${hint ? html`<span className="field-hint">${hint}</span>` : null}
    </label>
  `;
}

function PresenceList({ status, joinedParticipant }) {
  const participants = status?.participants || [];
  const agents = status?.agents || [];
  return html`
    <div className="presence-stack">
      <div className="presence-block">
        <div className="section-label">Joined humans</div>
        <div className="presence-list">
          ${participants.length
            ? participants.map((participant) => html`
                <div className=${`presence-item ${joinedParticipant?.participant_id === participant.participant_id ? "is-self" : ""}`} key=${participant.participant_id}>
                  <div className="avatar human">${avatarLabel(participant.display_name)}</div>
                  <div className="presence-copy">
                    <strong>${participant.display_name}</strong>
                    <span>${joinedParticipant?.participant_id === participant.participant_id ? "You’re chatting from this browser" : "Joined in the room"}</span>
                  </div>
                  <span className="presence-dot"></span>
                </div>
              `)
            : html`<div className="empty-inline">No one has joined yet.</div>`}
        </div>
      </div>
      <div className="presence-block">
        <div className="section-label">Agent participants</div>
        <div className="presence-list">
          ${agents.length
            ? agents.map((agent) => html`
                <div className="presence-item agent" key=${agent.participant_id}>
                  <div className="avatar agent">${avatarLabel(agent.display_name)}</div>
                  <div className="presence-copy">
                    <strong>${agent.display_name}</strong>
                    <span>Room agent</span>
                  </div>
                </div>
              `)
            : html`<div className="empty-inline">No agents configured.</div>`}
        </div>
      </div>
    </div>
  `;
}

function MafiaReadyList({ status, joinedParticipant }) {
  const viewers = status?.viewer_presence || [];
  const mafiaState = status?.mafia_state;
  const spinup = status?.mafia_lobby_spinup;
  const agentSeats = spinup?.agents || [];
  const seatedIds = new Set((mafiaState?.roster || []).map((entry) => entry.participant_id));
  const capacity = mafiaState?.total_players || status?.draft_config?.mafia?.total_players || 0;
  const readyCount = viewers.filter((viewer) => viewer.ready).length;

  return html`
    <div className="mafia-panel-stack">
      <div className="summary-card">
        <span className="section-label">Lobby progress</span>
        <p>${readyCount} of ${Math.min(capacity || viewers.length || 0, viewers.length || capacity || 0)} connected humans have claimed a seat.</p>
      </div>
      <div className="summary-card">
        <span className="section-label">Model warmup</span>
        <p>
          ${spinup?.active
            ? spinup.ready
              ? `All ${spinup.total_agents} model seats are warmed and ready to start.`
              : spinup.failed_count
                ? `${spinup.failed_count} model seat${spinup.failed_count === 1 ? "" : "s"} failed to warm up. Check debug before starting over.`
                : `Preparing ${spinup.ready_count} of ${spinup.total_agents} model seats before the game begins.`
            : "Model seats warm up in the lobby before the round starts."}
        </p>
      </div>
      <div className="presence-block">
        <div className="section-label">Connected humans</div>
        <div className="presence-list">
          ${viewers.length
            ? viewers.map((viewer, index) => {
                const isJoined = viewer.ready && viewer.participant_id;
                const isPlayer = viewer.participant_id && seatedIds.has(viewer.participant_id);
                return html`
                  <div className=${`presence-item ${joinedParticipant?.participant_id === viewer.participant_id ? "is-self" : ""}`} key=${viewer.viewer_id}>
                    <div className="avatar human">${avatarLabel(viewer.display_name || `Viewer ${index + 1}`)}</div>
                    <div className="presence-copy">
                      <strong>${viewer.display_name || `Viewer ${index + 1}`}</strong>
                      <span>
                        ${!isJoined
                          ? "Connected, not seated yet"
                          : isPlayer || mafiaState?.game_status === "lobby"
                            ? "Ready for the next game"
                            : "Watching as spectator"}
                      </span>
                    </div>
                    <${StatusBadge} tone=${isJoined ? "online" : "muted"}>${isJoined ? "Ready" : "Waiting"}</${StatusBadge}>
                  </div>
                `;
              })
            : html`<div className="empty-inline">Waiting for people to open the room link.</div>`}
        </div>
      </div>
      <div className="presence-block">
        <div className="section-label">Model seats</div>
        <div className="presence-list">
          ${agentSeats.length
            ? agentSeats.map((agent) => {
                const tone = agent.status === "ready" ? "online" : agent.status === "failed" ? "warning" : "neutral";
                const label = agent.status === "ready"
                  ? "Ready"
                  : agent.status === "failed"
                    ? "Failed"
                    : agent.status === "spinning_up"
                      ? "Spinning up"
                      : "Idle";
                return html`
                  <div className="presence-item agent" key=${agent.participant_id}>
                    <div className="avatar agent">${avatarLabel(agent.display_name)}</div>
                    <div className="presence-copy">
                      <strong>${agent.display_name}</strong>
                      <span>${agent.status === "ready"
                        ? "First response is prebuffered for the opening day."
                        : agent.status === "failed"
                          ? (agent.error || "Warmup failed")
                          : "Preparing an opening candidate in the lobby."}</span>
                    </div>
                    <${StatusBadge} tone=${tone}>${label}</${StatusBadge}>
                  </div>
                `;
              })
            : html`<div className="empty-inline">No model seats configured.</div>`}
        </div>
      </div>
    </div>
  `;
}

function MafiaPhaseCard({ service }) {
  const mafiaState = service.status?.mafia_state;
  const spinup = service.status?.mafia_lobby_spinup;
  const playerState = service.playerState;
  const countdown = useCountdown(mafiaState?.phase_ends_at);
  const premise = service.status?.scenario;

  if (!mafiaState) {
    return null;
  }

  const seatState = !service.joinedParticipant
    ? "Watching"
    : playerState?.spectator
      ? "Spectating"
      : playerState?.alive === false
        ? "Eliminated"
        : "Playing";

  const roleLabel = playerState?.spectator
    ? "Spectator"
    : playerState?.role
      ? playerState.role
      : "Unassigned";

  const seat = service.joinedParticipant
    ? (mafiaState?.roster || []).find((entry) => entry.participant_id === service.joinedParticipant.participant_id) || null
    : null;

  const privateRoleTitle = !service.joinedParticipant
    ? "Join to receive a role"
    : playerState?.spectator
      ? "You are spectating"
      : playerState?.role
        ? playerState.role === "mafia"
          ? "You are mafia"
          : "You are town"
        : "Role pending";

  const privateRoleCopy = !service.joinedParticipant
    ? "Enter a name in the lobby to claim a seat. Your private role appears here once the game assigns it."
    : playerState?.spectator
      ? "This game already started, so you’re watching this round. You can follow the table, but your role is spectator."
      : playerState?.role
        ? playerState.role === "mafia"
          ? "Your role is private. Blend in during the day, and coordinate quietly with the mafia at night."
          : "Your role is private. Read the room, talk in the day phase, and help the town find the mafia."
        : "Seats are locked in, but roles are not assigned until the game leaves the lobby.";

  const phaseCopy = mafiaState.phase === "lobby"
    ? "Humans can claim seats with names here while the model seats warm up in the background."
    : mafiaState.phase === "day_discussion"
      ? "Talk publicly, read the room, and decide who you trust before voting opens."
      : mafiaState.phase === "day_vote"
        ? "Day votes are secret until the reveal. Make your choice before the clock runs out."
        : mafiaState.phase === "night_action"
        ? "Night actions are private. Town waits while the mafia choose a target."
        : "The room is read-only while the game state resolves.";
  const displayPhaseLabel = mafiaState.game_status === "game_over" ? "Game over" : phaseLabel(mafiaState.phase);
  const displayPhaseCopy = mafiaState.game_status === "game_over"
    ? `The round is finished. ${mafiaState.winner ? `${mafiaState.winner} wins.` : "The final result has been resolved."}`
    : phaseCopy;
  const countdownLabel = mafiaState.phase === "day_discussion"
    ? "Discussion ends in"
    : mafiaState.phase === "day_vote"
      ? "Voting closes in"
      : mafiaState.phase === "night_action"
        ? "Night ends in"
        : mafiaState.phase === "lobby"
          ? "Start condition"
          : "Resolving";
  const displayCountdownLabel = mafiaState.game_status === "game_over" ? "Match status" : countdownLabel;
  const countdownValue = mafiaState.game_status === "game_over"
    ? "Finished"
    : mafiaState.phase_ends_at
    ? countdown > 0
      ? formatCountdown(countdown)
      : "Resolving…"
    : "Waiting";
  const countdownCopy = mafiaState.game_status === "game_over"
    ? "The table can review the transcript and final factions."
    : mafiaState.phase_ends_at
    ? countdown > 0
      ? `at ${formatClock(mafiaState.phase_ends_at)}`
      : "Advancing to the next phase now."
    : "The creator starts the game from the lobby.";

  const startBlocked = mafiaState.game_status === "lobby" && spinup?.active && !spinup?.ready;
  const startButtonLabel = startBlocked
    ? spinup?.failed_count
      ? "Agent warmup failed"
      : "Agents spinning up"
    : "Start game";
  const startSummary = startBlocked
    ? spinup?.failed_count
      ? "Some model seats failed to warm up. Check the debug drawer or restart the room."
      : `Preparing ${spinup?.ready_count || 0} of ${spinup?.total_agents || 0} model seats before the round begins.`
    : spinup?.active
      ? "All model seats are warm. Starting now should avoid the first-turn spin-up delay."
      : "Start the game from the lobby once the room is ready.";

  return html`
    <section className="mafia-phase-card">
      <div className="phase-chip-row">
        <${StatusBadge} tone=${mafiaState.game_status === "active" ? "online" : mafiaState.game_status === "game_over" ? "warning" : "muted"}>
          ${mafiaState.game_status}
        </${StatusBadge}>
        <${StatusBadge} tone=${playerState?.spectator ? "muted" : "neutral"}>${seatState}</${StatusBadge}>
        <${StatusBadge} tone="neutral">Round ${mafiaState.round_no || 1}</${StatusBadge}>
      </div>
      <div className="phase-main">
        <div>
          <div className="eyebrow">Current phase</div>
          <h2>${displayPhaseLabel}</h2>
          <p>${displayPhaseCopy}</p>
        </div>
        <div className="countdown-card" aria-live="polite">
          <span className="section-label">${displayCountdownLabel}</span>
          <strong>${countdownValue}</strong>
          <span>${countdownCopy}</span>
        </div>
      </div>
      ${mafiaState.game_status === "lobby"
        ? html`
            <div className="phase-action-row">
              <div className="summary-card">
                <span className="section-label">Primary action</span>
                <p>${startSummary}</p>
              </div>
              <button
                className="button"
                type="button"
                disabled=${startBlocked}
                onClick=${() => service.startRun().catch((error) => service.setError(error.message || String(error)))}
              >
                ${startButtonLabel}
              </button>
            </div>
          `
        : null}
      <div className="private-role-card">
        <div className="private-role-head">
          <div>
            <span className="section-label">Your private role</span>
            <h3>${privateRoleTitle}</h3>
          </div>
          <${StatusBadge} tone=${playerState?.faction === "mafia" ? "warning" : service.joinedParticipant ? "neutral" : "muted"}>
            ${roleLabel}
          </${StatusBadge}>
        </div>
        <p>${privateRoleCopy}</p>
        <div className="private-role-grid">
          <div className="summary-card">
            <span className="section-label">Seat</span>
            <strong>${seat ? `Seat ${seat.seat_index + 1}` : service.joinedParticipant ? "Pending" : "Not joined"}</strong>
          </div>
          <div className="summary-card">
            <span className="section-label">Faction</span>
            <strong>${playerState?.spectator ? "Spectator" : playerState?.faction || "Hidden until start"}</strong>
          </div>
          <div className="summary-card">
            <span className="section-label">Status</span>
            <strong>${seat ? (seat.alive ? "Alive" : "Eliminated") : service.joinedParticipant ? "Waiting" : "Observer"}</strong>
          </div>
        </div>
      </div>
      ${playerState?.faction === "mafia" && playerState?.teammates?.length
        ? html`
            <div className="mafia-secret-card">
              <span className="section-label">Private intel</span>
              <p>Your teammates: ${playerState.teammates.join(", ")}.</p>
            </div>
          `
        : null}
      ${premise
        ? html`
            <div className="premise-card">
              <span className="section-label">Shared table premise</span>
              <p>${premise}</p>
            </div>
          `
        : null}
      ${mafiaState.winner
        ? html`<div className="banner">Game over. ${mafiaState.winner} wins.</div>`
        : null}
    </section>
  `;
}

function MafiaRosterCard({ service }) {
  const mafiaState = service.status?.mafia_state;
  const roster = mafiaState?.roster || [];
  const factionsRevealed = mafiaState?.game_status === "game_over";

  return html`
    <${SectionCard}
      eyebrow="Table roster"
      title=${mafiaState?.game_status === "lobby" ? "Seats and spectators" : "Who’s alive"}
      description=${factionsRevealed
        ? "The game is over, so everyone’s faction is now public."
        : "Roles stay private, but everyone can see who is seated and who has been eliminated."}
    >
      <div className="presence-list">
        ${roster.length
          ? roster.map((player) => html`
              <div className=${`presence-item ${service.joinedParticipant?.participant_id === player.participant_id ? "is-self" : ""} ${player.alive ? "" : "is-dead"}`} key=${player.participant_id}>
                <div className="avatar human">${avatarLabel(player.display_name)}</div>
                  <div className="presence-copy">
                    <strong>${player.display_name}</strong>
                    <span>Seat ${player.seat_index + 1}${player.faction ? ` · ${player.faction}` : ""}</span>
                  </div>
                <div className="toolbar-row wrap">
                  <${StatusBadge} tone=${player.alive ? "online" : "muted"}>${player.alive ? "Alive" : "Out"}</${StatusBadge}>
                  ${player.faction
                    ? html`<${StatusBadge} tone=${player.faction === "mafia" ? "warning" : "neutral"}>${player.faction}</${StatusBadge}>`
                    : null}
                </div>
              </div>
            `)
          : html`<div className="empty-inline">Seats will lock in once the creator starts the game.</div>`}
      </div>
    </${SectionCard}>
  `;
}

function MafiaVotePanel({ service }) {
  const playerState = service.playerState;
  const mafiaState = service.status?.mafia_state;
  const roster = mafiaState?.roster || [];

  if (!playerState || !(playerState.can_vote || playerState.can_act)) {
    return null;
  }

  const title = playerState.can_act ? "Night action" : "Secret vote";
  const description = playerState.can_act
    ? "Only the mafia can see this panel. Pick a living non-mafia target before dawn."
    : "Choose one player. Your vote stays hidden until the reveal.";

  return html`
    <${SectionCard} eyebrow="Action panel" title=${title} description=${description}>
      <div className="vote-grid">
        ${playerState.legal_targets.map((targetId) => {
          const target = roster.find((entry) => entry.participant_id === targetId);
          const active = playerState.selected_target_participant_id === targetId;
          return html`
            <button
              key=${targetId}
              type="button"
              className=${`vote-card ${active ? "is-active" : ""}`}
              onClick=${() => {
                try {
                  service.castVote(targetId);
                  service.setError("");
                } catch (error) {
                  service.setError(String(error.message || error));
                }
              }}
            >
              <strong>${target?.display_name || targetId}</strong>
              <span>${active ? "Selected" : "Choose target"}</span>
            </button>
          `;
        })}
      </div>
      <div className="toolbar-row wrap">
        <span className="subtle">
          ${playerState.selected_target_participant_id
            ? `Current choice: ${roster.find((entry) => entry.participant_id === playerState.selected_target_participant_id)?.display_name || playerState.selected_target_participant_id}`
            : "No vote selected yet."}
        </span>
        <button
          className="button ghost"
          type="button"
          onClick=${() => {
            try {
              service.castVote(null);
              service.setError("");
            } catch (error) {
              service.setError(String(error.message || error));
            }
          }}
        >
          Clear selection
        </button>
      </div>
    </${SectionCard}>
  `;
}

function groupedMessages(messages) {
  return messages.map((message, index) => {
    const previous = messages[index - 1];
    const sameSender = previous && previous.participant_id === message.participant_id;
    return {
      ...message,
      showHeader: !sameSender,
      showAvatar: !sameSender,
    };
  });
}

function MessageBubble({ message, mine }) {
  return html`
    <div className=${`message-row ${mine ? "mine" : ""}`}>
      ${message.showAvatar
        ? html`<div className=${`avatar ${message.kind === "agent" ? "agent" : "human"}`}>${avatarLabel(message.display_name)}</div>`
        : html`<div className="avatar-spacer"></div>`}
      <div className="message-stack">
        ${message.showHeader
          ? html`
              <div className="message-header">
                <strong>${message.display_name}</strong>
                <span>${formatClock(message.created_at)}</span>
              </div>
            `
          : null}
        <div className=${`bubble ${mine ? "mine" : "other"} ${message.kind || "human"}`}>${message.text}</div>
      </div>
    </div>
  `;
}

function MessagePane({ messages, joinedParticipant, isMafia, hasJoined }) {
  const feedRef = useRef(null);

  useEffect(() => {
    const node = feedRef.current;
    if (!node) {
      return;
    }
    const distanceFromBottom = node.scrollHeight - node.scrollTop - node.clientHeight;
    if (distanceFromBottom < 180) {
      node.scrollTop = node.scrollHeight;
    }
  }, [messages.length]);

  const rows = groupedMessages(messages);

  return html`
    <section className="message-pane">
      <div className="pane-topbar">
        <div>
          <div className="eyebrow">Conversation</div>
          <strong>Room feed</strong>
        </div>
        <span className="subtle">${messages.length} messages</span>
      </div>
      <div className="message-scroll" ref=${feedRef}>
        ${rows.length
          ? rows.map((message) => html`
              <${MessageBubble}
                key=${message.message_id}
                message=${message}
                mine=${joinedParticipant?.participant_id === message.participant_id}
              />
            `)
          : html`
              <div className="feed-empty">
                <h3>${isMafia ? "The table is gathering." : "This room is quiet."}</h3>
                <p>
                  ${hasJoined
                    ? isMafia
                      ? "The game feed will fill in once the lobby is ready and the creator starts the round."
                      : "Say the first thing in the room to get the conversation moving."
                    : isMafia
                      ? "Claim a seat from the join card above the feed so you’re ready when the game starts."
                      : "Join from the card above the feed, then send the first message to break the silence."}
                </p>
              </div>
            `}
      </div>
    </section>
  `;
}

function DebugDrawer({ debugEvents }) {
  return html`
    <details className="debug-drawer">
      <summary>Agent activity</summary>
      <div className="debug-feed">
        ${debugEvents.length
          ? debugEvents.map((entry) => {
              const data = entry.event || {};
              const decision = data.output_summary?.decision || data.scheduler_decision?.decision;
              const reason = data.output_summary?.reason || data.scheduler_decision?.reason || data.reason;
              return html`
                <div className="debug-item" key=${`${entry.seq}:${entry.subject}`}>
                  <div className="debug-header">
                    <strong>${data.agent_id || "?"}</strong>
                    <span>${entry.subject}</span>
                  </div>
                  <div className="debug-meta">${new Date(entry.timestamp).toLocaleTimeString()} · ${data.command_subject || data.trigger_kind || ""}</div>
                  ${decision || reason ? html`<div className="debug-summary"><strong>${decision || "wait"}</strong>${reason ? html` · ${reason}` : ""}</div>` : null}
                  <div className="debug-summary">${formatDebugEntry(entry)}</div>
                </div>
              `;
            })
          : html`<div className="empty-inline">No debug events yet.</div>`}
      </div>
    </details>
  `;
}

function JoinComposer({ service }) {
  const [joinName, setJoinName] = useState(() => {
    const route = parseRoute();
    const key = `mafia-room:${route.roomId || DEFAULT_ROOM_ID}:joined`;
    try {
      const saved = window.localStorage.getItem(key);
      return saved ? JSON.parse(saved).display_name : "Guest";
    } catch (_error) {
      return "Guest";
    }
  });
  const [joinId, setJoinId] = useState("");
  const [messageText, setMessageText] = useState("");
  const joined = service.joinedParticipant;
  const mafiaState = service.status?.mafia_state;
  const isMafia = mafiaModeOf(service.status);
  const playerState = service.playerState;
  const connected = service.socketState === "open";
  const canJoin = connected && !!joinName.trim();
  const canSend = connected && !!messageText.trim();

  const onJoin = (event) => {
    event.preventDefault();
    if (!canJoin) {
      return;
    }
    try {
      service.join(joinId.trim(), joinName.trim() || "Guest");
      service.setError("");
    } catch (joinError) {
      service.setError(String(joinError.message || joinError));
    }
  };

  const onSend = (event) => {
    event.preventDefault();
    if (!canSend) {
      return;
    }
    try {
      service.sendMessage({
        text: messageText.trim(),
        client_message_id: window.crypto?.randomUUID ? window.crypto.randomUUID() : randomSlug("message"),
      });
      setMessageText("");
      service.setError("");
    } catch (sendError) {
      service.setError(String(sendError.message || sendError));
    }
  };

  if (!joined) {
    return html`
      <form className="join-panel" onSubmit=${onJoin}>
        <div className="join-copy">
          <strong>${isMafia ? "Claim a seat in the lobby" : "Join before you speak"}</strong>
          <span>
            ${isMafia
              ? "Claim a seat with a name, then wait while the model seats finish warming up and the room creator starts the round."
              : "Everyone in the room sees when you connect, so your invitees know who arrived."}
          </span>
        </div>
        <div className="join-grid">
          <${Field} label="Display name" hint=${isMafia ? "Required to claim a seat before the game starts." : "Shown to everyone in the room."}>
            <input value=${joinName} placeholder="Display name" onChange=${(event) => setJoinName(event.target.value)} />
          </${Field}>
          <${Field} label="Participant ID" hint="Optional stable ID if you want to reuse one.">
            <input value=${joinId} placeholder="optional-id" onChange=${(event) => setJoinId(event.target.value)} />
          </${Field}>
        </div>
        <div className="toolbar-row">
          <span className="subtle">${connected ? "You’re connected live. Pick a name to continue." : "Waiting to reconnect before you can join."}</span>
          <button className="button" type="submit" disabled=${!canJoin}>${isMafia ? "Join lobby" : "Join room"}</button>
        </div>
      </form>
    `;
  }

  if (isMafia && (!playerState?.can_chat || mafiaState?.phase !== "day_discussion")) {
    const copy = playerState?.spectator
      ? "You joined after the game started, so you’re spectating this round."
      : playerState?.alive === false
        ? "You were eliminated. You can still follow the room, but you can’t speak anymore."
        : mafiaState?.phase === "lobby"
          ? "You’ve claimed a seat. The room creator can start once the model seats finish warming up."
          : mafiaState?.phase === "day_vote"
            ? "Public chat pauses during the secret day vote."
            : mafiaState?.phase === "night_action"
              ? (playerState?.can_act ? "Use the vote panel to choose a target before dawn." : "Night is private. Wait for the morning reveal.")
              : "This phase is read-only until the next discussion window opens.";

    return html`
      <div className="join-panel phase-panel">
        <div className="composer-head">
          <div className="composer-copy">
            <strong>${joined.display_name}</strong>
            <span>${copy}</span>
          </div>
        </div>
      </div>
    `;
  }

  return html`
    <form className="composer" onSubmit=${onSend}>
      <div className="composer-head">
        <div className="composer-copy">
          <strong>${joined.display_name}</strong>
          <span>${connected ? "Chat is live for you right now." : "Reconnecting… messages are temporarily disabled."}</span>
        </div>
      </div>
      <label className="sr-only" htmlFor="room-message-input">Message</label>
      <textarea
        id="room-message-input"
        value=${messageText}
        placeholder="Write a message for the room…"
        aria-label="Message"
        onChange=${(event) => setMessageText(event.target.value)}
      ></textarea>
      <div className="toolbar-row">
        <span className="subtle">${connected ? "Everyone currently joined sees this instantly." : "Reconnect to send your next message."}</span>
        <button className="button" type="submit" disabled=${!canSend}>Send</button>
      </div>
    </form>
  `;
}

function RoomMetaPanel({ service }) {
  const shareLink = `${window.location.origin}${roomPath(service.roomId)}`;
  const [copied, setCopied] = useState(false);
  const isMafia = mafiaModeOf(service.status);
  const mafiaState = service.status?.mafia_state;
  const startLabel = isMafia && mafiaState?.game_status === "lobby" ? "Start game" : "Start";
  const canStop = service.status?.run_state === "running" || service.status?.run_state === "paused";

  const copyLink = async () => {
    try {
      await copyText(shareLink);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch (error) {
      service.setError(String(error.message || error));
    }
  };

  return html`
    <section className="room-meta-panel">
      <div className="room-meta-copy">
        <div className="eyebrow">${isMafia ? "Game room" : "Chat room"}</div>
        <h2>${service.status?.room_title || "Room"}</h2>
        <p>${service.status?.scenario || "Configure a room prompt and invite people in."}</p>
        <div className="room-meta-badges">
          <${StatusBadge} tone=${service.status?.run_state === "running" ? "online" : service.status?.run_state === "paused" ? "warning" : "muted"}>
            ${service.status?.run_state || "idle"}
          </${StatusBadge}>
          <${StatusBadge} tone="neutral">
            ${isMafia ? "Mafia mode" : "Regular mode"}
          </${StatusBadge}>
          ${isMafia && mafiaState ? html`
            <${StatusBadge} tone=${mafiaState.game_status === "game_over" ? "warning" : mafiaState.phase === "day_vote" || mafiaState.phase === "night_action" ? "warning" : "neutral"}>
              ${mafiaState.game_status === "game_over" ? "Game over" : phaseLabel(mafiaState.phase)}
            </${StatusBadge}>
          ` : null}
          <${StatusBadge} tone=${service.socketState === "open" ? "online" : "warning"}>
            ${service.socketState === "open" ? "Live" : "Offline"}
          </${StatusBadge}>
          <${StatusBadge} tone=${service.joinedParticipant ? "online" : "muted"}>
            ${service.joinedParticipant ? service.joinedParticipant.display_name : "Browse only"}
          </${StatusBadge}>
        </div>
      </div>
      <div className="room-meta-actions">
        <button className="invite-pill" type="button" onClick=${copyLink} title=${shareLink}>
          <span className="section-label">Invite</span>
          <code>${copied ? "Copied" : shareLink}</code>
        </button>
        <div className="toolbar-row wrap compact-actions meta-controls">
          <button
            className="button small"
            type="button"
            onClick=${() => service.startRun().catch((error) => service.setError(error.message || String(error)))}
          >
            ${startLabel}
          </button>
          <button
            className="button ghost small"
            type="button"
            disabled=${!canStop}
            onClick=${() => service.stopRun().catch((error) => service.setError(error.message || String(error)))}
          >
            Stop
          </button>
          <a className="button ghost small" href=${roomConfigPath(service.roomId)}>Setup</a>
        </div>
      </div>
    </section>
  `;
}

function RoomControlsCard({ service }) {
  const isMafia = mafiaModeOf(service.status);
  const mafiaState = service.status?.mafia_state;
  const startLabel = isMafia && mafiaState?.game_status === "lobby" ? "Start game" : "Start";
  return html`
    <details className="admin-drawer">
      <summary>Room tools</summary>
      <div className="admin-body">
        <p className="subtle">
          ${isMafia
            ? "Creator/admin controls for the live lobby and game flow."
            : "Controls for starting or pausing the shared room engine."}
        </p>
        <div className="metric-grid">
          <div className="metric-tile">
            <span>Messages</span>
            <strong>${service.status?.message_count || 0}</strong>
          </div>
          <div className="metric-tile">
            <span>Room type</span>
            <strong>${service.status?.room_mode || "regular"}</strong>
          </div>
          <div className="metric-tile">
            <span>Runtime</span>
            <strong>${service.status?.draft_config?.runtime?.provider || service.status?.active_config?.runtime?.provider || "unknown"}</strong>
          </div>
          <div className="metric-tile">
            <span>Agents</span>
            <strong>${service.status?.agents?.length || 0}</strong>
          </div>
        </div>
        <div className="control-row wrap">
          <button className="button" type="button" onClick=${() => service.startRun().catch((error) => service.setError(error.message || String(error)))}>${startLabel}</button>
          <button className="button secondary" type="button" onClick=${() => service.pauseRun().catch((error) => service.setError(error.message || String(error)))}>Pause</button>
          <button className="button secondary" type="button" onClick=${() => service.resumeRun().catch((error) => service.setError(error.message || String(error)))}>Resume</button>
          <button className="button ghost" type="button" onClick=${() => service.stopRun().catch((error) => service.setError(error.message || String(error)))}>Stop</button>
        </div>
      </div>
    </details>
  `;
}

function ChatPage({ service }) {
  const isMafia = mafiaModeOf(service.status);
  const mafiaState = service.status?.mafia_state;
  const preJoin = !service.joinedParticipant;
  const chatFeed = html`<${MessagePane} messages=${service.messages} joinedParticipant=${service.joinedParticipant} isMafia=${isMafia} hasJoined=${!preJoin} />`;
  const composer = html`<${JoinComposer} service=${service} />`;

  if (isMafia) {
    return html`
      <div className="room-screen">
        <div className="room-body">
        <main className="chat-column">
          ${service.error ? html`<div className="banner error">${service.error}</div>` : null}
          <div className="chat-focus">
            ${preJoin ? composer : chatFeed}
            ${preJoin ? chatFeed : composer}
          </div>
        </main>
        <aside className="side-column">
          <${RoomMetaPanel} service=${service} />
          <${MafiaPhaseCard} service=${service} />
          <${MafiaVotePanel} service=${service} />
          <${RoomControlsCard} service=${service} />
          <${MafiaRosterCard} service=${service} />
          <${SectionCard}
            eyebrow=${mafiaState?.phase === "lobby" ? "Lobby readiness" : "Presence"}
            title=${mafiaState?.phase === "lobby" ? "Who has claimed a seat" : "Connected browsers"}
            description=${mafiaState?.phase === "lobby"
              ? "The chat stays quiet until the room creator starts the game from the lobby."
              : "Joined players and watchers currently connected to this room URL."}
          >
            <${MafiaReadyList} status=${service.status} joinedParticipant=${service.joinedParticipant} />
          </${SectionCard}>
          <${DebugDrawer} debugEvents=${service.debugEvents} />
        </aside>
        </div>
      </div>
    `;
  }

  return html`
    <div className="room-screen">
      <div className="room-body">
      <main className="chat-column">
        ${service.error ? html`<div className="banner error">${service.error}</div>` : null}
        ${service.status?.runtime_validation?.errors?.length
          ? html`<div className="banner">${service.status.runtime_validation.errors.join(" ")}</div>`
          : null}
        <div className="chat-focus">
          ${preJoin ? composer : chatFeed}
          ${preJoin ? chatFeed : composer}
        </div>
      </main>
      <aside className="side-column">
        <${RoomMetaPanel} service=${service} />
        <${RoomControlsCard} service=${service} />
        <${SectionCard}
          eyebrow="Presence"
          title="Who’s connected"
          description="People show up here as soon as they join the room from their browser."
        >
          <${PresenceList} status=${service.status} joinedParticipant=${service.joinedParticipant} />
        </${SectionCard}>
        <${DebugDrawer} debugEvents=${service.debugEvents} />
      </aside>
      </div>
    </div>
  `;
}

function RoomBasicsSection({ draft, setDraft, schema, roomName, setRoomName, showRoomPath = false }) {
  const suggestedRoomId = slugifyRoomId(roomName || "");
  const isMafia = mafiaModeOf(draft);
  const seedText = roomSeed(roomName || draft?.chat?.scenario || "room");
  const currentPremiseIsSeed = MAFIA_PREMISE_SEEDS.includes(draft.chat?.scenario || "");

  return html`
    <${SectionCard}
      eyebrow="Room setup"
      title=${isMafia ? "Mode, table premise, and phase timing" : "Prompt, path, and run style"}
      description=${isMafia
        ? "Set the public game premise, choose the roster size, and tune the timers that drive each phase."
        : "Create a shareable room URL and set the scenario that guides both humans and agents."}
    >
      <div className="field-grid">
        ${showRoomPath ? html`
          <${Field} label="Room path" hint="This becomes the shareable URL for the room.">
            <input value=${roomName} placeholder="team-lunch" onChange=${(event) => setRoomName(event.target.value)} />
          </${Field}>
          <div className="field static-field">
            <span className="field-label">Invite preview</span>
            <div className="preview-chip">${window.location.origin}${roomPath(suggestedRoomId || "room-xxxxxxx")}</div>
            <span className="field-hint">You can send this URL to other people once the room is created.</span>
          </div>
        ` : null}

        <${Field} label="Room type" hint="Regular rooms are open chat. Mafia rooms add a lobby, timed phases, secret voting, and private night actions.">
          <div className="mode-choice-grid">
            ${(schema?.room_modes || ["regular", "mafia"]).map((mode) => html`
              <button
                key=${mode}
                type="button"
                className=${`mode-choice ${draft.room_mode === mode ? "is-active" : ""}`}
                onClick=${() => updateDraft(setDraft, (next) => {
                  const previousMode = next.room_mode;
                  next.room_mode = mode;
                  if (mode === "mafia") {
                    replaceMafiaPersonaPool(next, seedText);
                    if (previousMode !== "mafia") {
                      next.chat.scenario = seededMafiaPremise(seedText);
                    }
                  }
                })}
              >
                <strong>${mode === "mafia" ? "Mafia mode" : "Regular mode"}</strong>
                <span>${mode === "mafia" ? "Timed phases, private roles, and secret votes." : "Shared chat room with humans and agents speaking openly."}</span>
              </button>
            `)}
          </div>
        </${Field}>

        ${isMafia
          ? html`
              <${Field} label="Game engine" hint="Mafia rooms always use the event-driven chat engine underneath the game flow.">
                <div className="preview-chip">improved.buffered_async</div>
              </${Field}>
            `
          : html`
              <${Field} label="Conversation mode" hint="Improved mode keeps the agents event-driven and responsive.">
                <select value=${draft.mode} onChange=${(event) => updateDraft(setDraft, (next) => { next.mode = event.target.value; })}>
                  ${(schema?.modes || []).map((mode) => html`<option value=${mode} key=${mode}>${mode}</option>`)}
                </select>
              </${Field}>
            `}
        <${Field} label="Runtime provider" hint="Choose which agent runtime powers scheduler/generator calls.">
          <select value=${draft.runtime.provider} onChange=${(event) => updateDraft(setDraft, (next) => { next.runtime.provider = event.target.value; })}>
            ${(schema?.runtime_providers || []).map((provider) => html`<option value=${provider} key=${provider}>${provider}</option>`)}
          </select>
        </${Field}>
        <${Field} label="Model" hint="Haiku works well for quick chat turns.">
          <input value=${draft.runtime.model || ""} onChange=${(event) => updateDraft(setDraft, (next) => { next.runtime.model = event.target.value; })} />
        </${Field}>
        <div className="field static-field">
          <span className="field-label">Agent delivery</span>
          <div className="preview-chip">Immediate send</div>
          <span className="field-hint">Agents post as soon as a send decision is made.</span>
        </div>
        ${isMafia
          ? html`
              <${Field} label="Total players" hint="Humans fill seats first; remaining seats are filled by the seeded persona pool.">
                <input
                  type="number"
                  min="5"
                  max="13"
                  step="1"
                  value=${String(draft.mafia?.total_players ?? 6)}
                  onChange=${(event) => updateDraft(setDraft, (next) => {
                    next.mafia.total_players = clampMafiaPlayers(event.target.value);
                    syncMafiaPersonaPool(next, seedText);
                  })}
                />
              </${Field}>
              <div className="field static-field">
                <span className="field-label">Start condition</span>
                <div className="preview-chip">Manual start after every connected human has joined with a name</div>
                <span className="field-hint">The creator starts the game from the lobby when the room is ready. Late joiners become spectators once the game is active.</span>
              </div>
            `
          : null}
        ${isMafia
          ? html`
              <div className="field static-field field-span">
                <span className="field-label">Hidden in Mafia mode</span>
                <div className="preview-chip">Regular-room pacing controls are tucked away while you configure the game table.</div>
                <span className="field-hint">Conversation mode and topic-analysis toggles are managed automatically for Mafia rooms so the form stays focused on phases, seats, and personas.</span>
              </div>
            `
          : html`
              <label className="toggle-field field-span">
                <input
                  type="checkbox"
                  checked=${Boolean(draft.topic?.enabled)}
                  onChange=${(event) => updateDraft(setDraft, (next) => { next.topic.enabled = event.target.checked; })}
                />
                <div>
                  <strong>Topic analysis</strong>
                  <span>Keep topic tracking on if you want the agents to maintain memory and thread awareness.</span>
                </div>
              </label>
            `}
        <${Field}
          label=${isMafia ? "Game theme / table premise" : "Room prompt"}
          hint=${isMafia ? "This public premise frames the fiction, social setting, and tone of the Mafia game." : "This scenario becomes the shared premise for everyone who joins."}
          className="field-span"
        >
          <textarea
            value=${draft.chat.scenario || ""}
            placeholder=${isMafia ? "Mafia game scenario: You are trapped in a snowed-in mountain lodge, and everyone is suspicious." : "You are a distributed product team planning the next offsite."}
            onChange=${(event) => updateDraft(setDraft, (next) => { next.chat.scenario = event.target.value; })}
          ></textarea>
        </${Field}>
        ${isMafia
          ? html`
              <div className="field field-span">
                <span className="field-label">Premise seed</span>
                <div className="seed-row">
                  <div className="preview-chip">${currentPremiseIsSeed ? "Using a seeded Mafia scenario" : "Custom premise"}</div>
                  <button
                    className="button ghost small"
                    type="button"
                    onClick=${() => updateDraft(setDraft, (next) => {
                      next.chat.scenario = nextMafiaPremise(seedText, next.chat.scenario);
                    })}
                  >
                    Swap seed
                  </button>
                </div>
                <span className="field-hint">Mafia rooms now start from a curated list of explicit Mafia game scenarios. You can swap seeds, then edit the text however you want.</span>
              </div>
            `
          : null}
        ${isMafia
          ? html`
              <${Field} label="Day discussion timer (seconds)" hint="Public chat window before day voting opens.">
                <input type="number" min="5" step="5" value=${String(draft.mafia?.day_discussion_seconds ?? 270)} onChange=${(event) => updateDraft(setDraft, (next) => { next.mafia.day_discussion_seconds = Number(event.target.value || 5); })} />
              </${Field}>
              <${Field} label="Day vote timer (seconds)" hint="Players can only vote during this window. Votes stay secret until reveal.">
                <input type="number" min="5" step="5" value=${String(draft.mafia?.day_vote_seconds ?? 90)} onChange=${(event) => updateDraft(setDraft, (next) => { next.mafia.day_vote_seconds = Number(event.target.value || 5); })} />
              </${Field}>
              <${Field} label="Day reveal timer (seconds)" hint="Short read-only pause after the day vote resolves.">
                <input type="number" min="3" step="1" value=${String(draft.mafia?.day_reveal_seconds ?? 30)} onChange=${(event) => updateDraft(setDraft, (next) => { next.mafia.day_reveal_seconds = Number(event.target.value || 3); })} />
              </${Field}>
              <${Field} label="Night action timer (seconds)" hint="Only living mafia can act during the night.">
                <input type="number" min="5" step="5" value=${String(draft.mafia?.night_action_seconds ?? 90)} onChange=${(event) => updateDraft(setDraft, (next) => { next.mafia.night_action_seconds = Number(event.target.value || 5); })} />
              </${Field}>
              <${Field} label="Night reveal timer (seconds)" hint="Short read-only pause after the night resolves.">
                <input type="number" min="3" step="1" value=${String(draft.mafia?.night_reveal_seconds ?? 30)} onChange=${(event) => updateDraft(setDraft, (next) => { next.mafia.night_reveal_seconds = Number(event.target.value || 3); })} />
              </${Field}>
            `
          : null}
      </div>
    </${SectionCard}>
  `;
}

function PersonalitySlider({ label, hint, value, onChange }) {
  return html`
    <div className="trait-card">
      <div className="trait-copy">
        <strong>${label}</strong>
        <span>${hint}</span>
      </div>
      <div className="slider-row">
        <input type="range" min="0" max="1.5" step="0.05" value=${String(value ?? 0)} onChange=${(event) => onChange(Number(event.target.value))} />
        <span className="slider-value">${scoreLabel(value)}</span>
      </div>
    </div>
  `;
}

function AgentEditorSection({ draft, setDraft }) {
  const isMafia = mafiaModeOf(draft);
  const seedText = roomSeed(draft?.chat?.scenario || draft?.room_mode || "room");

  const removeAgent = (index) => updateDraft(setDraft, (next) => {
    if (next.agents.length > 1) {
      next.agents.splice(index, 1);
    }
  });

  const addAgent = () => updateDraft(setDraft, (next) => {
    next.agents.push(buildNewAgent(next));
  });

  const rerollAll = () => updateDraft(setDraft, (next) => {
    replaceMafiaPersonaPool(next, seedText);
  });

  const rerollOne = (index) => updateDraft(setDraft, (next) => {
    const currentNames = next.agents.map((agent, agentIndex) => (agentIndex === index ? "" : agent.display_name));
    next.agents[index] = rerollMafiaPersona(seedText, clampMafiaPlayers(next.mafia?.total_players), index, currentNames);
  });

  return html`
    <${SectionCard}
      eyebrow=${isMafia ? "Persona pool" : "Agent cast"}
      title=${isMafia ? "Seeded filler personas for open seats" : "Personalities, motivations, and speaking style"}
      description=${isMafia
        ? "These personas fill any seats not claimed by humans when the game starts. You can edit them or reroll until the table feels right."
        : "Tune each agent before you share the room so invitees meet the right personalities."}
      actions=${isMafia
        ? html`<button className="button ghost" type="button" onClick=${rerollAll}>Reroll all personas</button>`
        : html`<button className="button ghost" type="button" onClick=${addAgent}>Add agent</button>`}
    >
      <div className="agent-list">
        ${(draft.agents || []).map((agent, index) => html`
          <article className="agent-card" key=${agent.id || index}>
            <div className="agent-card-head">
              <div>
                <div className="section-label">${isMafia ? `Seat persona ${index + 1}` : `Agent ${index + 1}`}</div>
                <h3>${agent.display_name || agent.id || "Untitled agent"}</h3>
              </div>
              ${isMafia
                ? html`<button className="button ghost small" type="button" onClick=${() => rerollOne(index)}>Reroll</button>`
                : html`<button className="button ghost small" type="button" disabled=${draft.agents.length <= 1} onClick=${() => removeAgent(index)}>Remove</button>`}
            </div>
            <div className="field-grid">
              <${Field} label="Display name" hint="Shown in the room’s presence list and message feed.">
                <input value=${agent.display_name || ""} onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].display_name = event.target.value; })} />
              </${Field}>
              ${isMafia
                ? html`
                    <div className="field static-field">
                      <span className="field-label">Seat ID</span>
                      <div className="preview-chip">${agent.id || `seat-${index + 1}`}</div>
                      <span className="field-hint">Hidden from the main form in Mafia mode so you can focus on persona edits instead of internal IDs.</span>
                    </div>
                  `
                : html`
                    <${Field} label="Participant ID" hint="Stable identifier for this agent inside the room.">
                      <input value=${agent.id || ""} onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].id = event.target.value; })} />
                    </${Field}>
                  `}
              <${Field}
                label=${isMafia ? "Persona motivations / table instincts" : "Motivations / goals"}
                hint=${isMafia ? "One line per instinct. This shapes how the persona behaves once they occupy an open seat." : "One line per motivation. This shapes what the agent tries to do in the chat."}
                className="field-span"
              >
                <textarea
                  value=${goalsToText(agent.goals)}
                  placeholder="Keep the conversation moving&#10;Offer concrete food options"
                  onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].goals = textToGoals(event.target.value); })}
                ></textarea>
              </${Field}>
              <${Field} label=${isMafia ? "Voice / table manner" : "Voice / style prompt"} hint="How this agent should sound while replying." className="field-span">
                <textarea
                  value=${agent.style_prompt || ""}
                  placeholder="Friendly, upbeat, and curious."
                  onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].style_prompt = event.target.value; })}
                ></textarea>
              </${Field}>
              <${Field} label="Max words" hint="Useful for keeping one persona concise and another more reflective.">
                <input
                  type="number"
                  min="1"
                  step="1"
                  value=${String(agent.max_words ?? 12)}
                  onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].max_words = Number(event.target.value || 1); })}
                />
              </${Field}>
              <div className="field static-field">
                <span className="field-label">Current summary</span>
                <div className="preview-chip">${(agent.goals || []).slice(0, 2).join(" • ") || "No goals yet"}</div>
                <span className="field-hint">${agent.style_prompt || "Add a speaking style to shape the voice."}</span>
              </div>
            </div>
            <div className="trait-grid">
              ${PERSONALITY_FIELDS.map((trait) => html`
                <${PersonalitySlider}
                  key=${trait.key}
                  label=${trait.label}
                  hint=${trait.hint}
                  value=${agent.personality?.[trait.key] ?? 0.5}
                  onChange=${(nextValue) => updateDraft(setDraft, (next) => {
                    next.agents[index].personality[trait.key] = nextValue;
                  })}
                />
              `)}
            </div>
            ${isMafia
              ? null
              : html`
                  <details className="advanced-card">
                    <summary>Advanced tuning</summary>
                    <div className="field-grid">
                      <${Field} label="Scheduler tick (baseline only)" hint="Kept for compatibility with the baseline mode.">
                        <input
                          type="number"
                          min="0.05"
                          step="0.05"
                          value=${String(agent.scheduler?.tick_rate_seconds ?? 1)}
                          onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].scheduler.tick_rate_seconds = Number(event.target.value || 0.05); })}
                        />
                      </${Field}>
                      <${Field} label="Buffer size" hint="How many candidate messages this agent keeps around.">
                        <input
                          type="number"
                          min="1"
                          step="1"
                          value=${String(agent.generation?.buffer_size ?? 5)}
                          onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].generation.buffer_size = Number(event.target.value || 1); })}
                        />
                      </${Field}>
                      <${Field} label="Candidate freshness (seconds)" hint="How long a generated candidate can stay valid.">
                        <input
                          type="number"
                          min="1"
                          step="1"
                          value=${String(agent.generation?.staleness_window_seconds ?? 30)}
                          onChange=${(event) => updateDraft(setDraft, (next) => { next.agents[index].generation.staleness_window_seconds = Number(event.target.value || 1); })}
                        />
                      </${Field}>
                    </div>
                  </details>
                `}
          </article>
        `)}
      </div>
    </${SectionCard}>
  `;
}

function SetupSummary({ draft, roomId }) {
  const shareUrl = `${window.location.origin}${roomPath(roomId || "room-xxxxxxx")}`;
  const isMafia = mafiaModeOf(draft);
  return html`
    <${SectionCard}
      eyebrow="Review"
      title=${isMafia ? "What players will see at the table" : "What people will join"}
      description=${isMafia ? "Review the public premise, timers, and seeded persona pool before you share the room." : "This is the room setup your invitees will experience."}
    >
      <div className="summary-stack">
        <div className="summary-card">
          <span className="section-label">Share link</span>
          <code>${shareUrl}</code>
        </div>
        <div className="summary-card">
          <span className="section-label">${isMafia ? "Game premise" : "Prompt"}</span>
          <p>${draft.chat.scenario}</p>
        </div>
        <div className="summary-card">
          <span className="section-label">Room type</span>
          <p>${draft.room_mode || "regular"}</p>
        </div>
        ${isMafia
          ? html`
              <div className="summary-card">
                <span className="section-label">Phase timers</span>
                <p>Day talk ${draft.mafia?.day_discussion_seconds}s · Day vote ${draft.mafia?.day_vote_seconds}s · Night ${draft.mafia?.night_action_seconds}s</p>
              </div>
            `
          : html`
              <div className="summary-card">
                <span className="section-label">Runtime</span>
                <p>${draft.runtime.provider} · ${draft.runtime.model}</p>
              </div>
            `}
        <div className="summary-card">
          <span className="section-label">${isMafia ? "Persona pool" : "Agents"}</span>
          <div className="summary-list">
            ${(draft.agents || []).map((agent) => html`
              <div className="summary-agent" key=${agent.id}>
                <strong>${agent.display_name}</strong>
                <span>${(agent.goals || []).slice(0, 2).join(" • ") || "No goals set"}</span>
              </div>
            `)}
          </div>
        </div>
      </div>
    </${SectionCard}>
  `;
}

function WizardSteps({ step, setStep }) {
  return html`
    <div className="wizard-steps">
      ${SETUP_STEPS.map((item, index) => html`
        <button
          type="button"
          key=${item.key}
          className=${`wizard-step ${index === step ? "is-active" : index < step ? "is-complete" : ""}`}
          onClick=${() => setStep(index)}
        >
          <span>${index + 1}</span>
          <strong>${item.label}</strong>
        </button>
      `)}
    </div>
  `;
}

function RoomSetupModal({ open, onClose, lobby, draft, setDraft, roomName, setRoomName, step, setStep, onCreate }) {
  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previous;
    };
  }, [open]);

  if (!open || !draft) {
    return null;
  }

  const currentStep = SETUP_STEPS[step]?.key;
  const suggested = slugifyRoomId(roomName);

  return html`
    <div className="modal-scrim" onClick=${onClose}>
      <div className="modal-panel" onClick=${(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <div className="eyebrow">Create room</div>
            <h2>${suggested || "New room"}</h2>
            <p className="section-copy">Set the room prompt, shape the agents, and then share the room path with other people.</p>
          </div>
          <button className="button ghost" type="button" onClick=${onClose}>Close</button>
        </div>
        <${WizardSteps} step=${step} setStep=${setStep} />
        <div className="modal-scroll">
          ${currentStep === "room"
            ? html`<${RoomBasicsSection} draft=${draft} setDraft=${setDraft} schema=${lobby.schema || {}} roomName=${roomName} setRoomName=${setRoomName} showRoomPath=${true} />`
            : null}
          ${currentStep === "agents"
            ? html`<${AgentEditorSection} draft=${draft} setDraft=${setDraft} />`
            : null}
          ${currentStep === "review"
            ? html`
                <${SetupSummary} draft=${draft} roomId=${suggested || "room-xxxxxxx"} />
                <details className="advanced-card raw-config-card">
                  <summary>Raw draft config</summary>
                  <pre className="debug-json">${prettyJson(draft)}</pre>
                </details>
              `
            : null}
        </div>
        <div className="modal-footer">
          <button className="button ghost" type="button" disabled=${step === 0} onClick=${() => setStep((previous) => Math.max(0, previous - 1))}>Back</button>
          <div className="modal-footer-actions">
            ${step < SETUP_STEPS.length - 1
              ? html`<button className="button" type="button" onClick=${() => setStep((previous) => Math.min(SETUP_STEPS.length - 1, previous + 1))}>Next step</button>`
              : html`<button className="button" type="button" onClick=${onCreate}>Create room</button>`}
          </div>
        </div>
      </div>
    </div>
  `;
}

function LobbyPage({ lobby }) {
  const [roomName, setRoomName] = useState("");
  const [draft, setDraft] = useState(null);
  const [step, setStep] = useState(0);
  const [createOpen, setCreateOpen] = useState(false);
  const suggested = useMemo(() => slugifyRoomId(roomName), [roomName]);

  useEffect(() => {
    if (lobby.templateConfig && !draft) {
      setDraft(deepClone(lobby.templateConfig));
    }
  }, [lobby.templateConfig, draft]);

  const openCreate = () => {
    if (!draft && lobby.templateConfig) {
      setDraft(deepClone(lobby.templateConfig));
    }
    setStep(0);
    setCreateOpen(true);
  };

  const createRoom = async () => {
    if (!draft) {
      return;
    }
    try {
      const room = await lobby.createRoom(suggested || undefined, draft);
      window.location.assign(room.room_path);
    } catch (error) {
      lobby.setError(String(error.message || error));
    }
  };

  return html`
    <div className="lobby-shell">
      <section className="hero-card lobby-hero">
        <div>
          <div className="eyebrow">Shareable chat rooms</div>
          <h1>Mafia Rooms</h1>
          <p>Create a room with a prompt, tune the agent cast before anyone joins, and share the path as an invite link.</p>
        </div>
        <div className="create-card">
          <div className="section-label">Create room</div>
          <${Field} label="Room path" hint="Use a short, shareable URL slug.">
            <input value=${roomName} placeholder="team-lunch" onChange=${(event) => setRoomName(event.target.value)} />
          </${Field}>
          <div className="preview-chip">${window.location.origin}${roomPath(suggested || "room-xxxxxxx")}</div>
          <div className="toolbar-row wrap">
            <button className="button" type="button" onClick=${openCreate}>Configure room</button>
            <span className="subtle">Prompt, personalities, and motivations are configured in the popup.</span>
          </div>
        </div>
      </section>

      ${lobby.error ? html`<div className="banner error">${lobby.error}</div>` : null}

      <section className="section-card">
        <div className="section-header">
          <div>
            <div className="eyebrow">Existing rooms</div>
            <h2>Open or share a room</h2>
          </div>
          <button className="button ghost" type="button" onClick=${() => lobby.refresh().catch((error) => lobby.setError(error.message || String(error)))}>Refresh</button>
        </div>
        <div className="room-grid">
              ${lobby.rooms.length
            ? lobby.rooms.map((room) => html`
                <a className="room-card" href=${room.room_path} key=${room.room_id}>
                  <div className="room-card-top">
                    <strong>#${room.room_id}</strong>
                    <div className="badge-row">
                      <${StatusBadge} tone="neutral">${room.room_mode}</${StatusBadge}>
                      <${StatusBadge} tone=${room.run_state === "running" ? "online" : room.run_state === "paused" ? "warning" : "muted"}>${room.run_state}</${StatusBadge}>
                    </div>
                  </div>
                  <p>${room.scenario}</p>
                  <div className="room-card-meta">
                    <span>${room.participant_count} joined</span>
                    <span>${room.message_count} messages</span>
                    <span>${room.viewer_count} viewers</span>
                  </div>
                </a>
              `)
            : html`<div className="empty-inline">No rooms yet. Create one and share the path.</div>`}
        </div>
      </section>

      ${lobby.loading
        ? null
        : html`<${RoomSetupModal}
            open=${createOpen}
            onClose=${() => setCreateOpen(false)}
            lobby=${lobby}
            draft=${draft}
            setDraft=${setDraft}
            roomName=${roomName}
            setRoomName=${setRoomName}
            step=${step}
            setStep=${setStep}
            onCreate=${createRoom}
          />`}
    </div>
  `;
}

function ConfigPage({ service }) {
  const [draft, setDraft] = useState(null);
  const [saveState, setSaveState] = useState("idle");
  const [validationError, setValidationError] = useState("");
  const [schema, setSchema] = useState(null);

  useEffect(() => {
    let mounted = true;
    Promise.all([fetchJson(roomApiPath(service.roomId, "config")), fetchJson(roomApiPath(service.roomId, "config/schema")), service.refreshStatus()])
      .then(([config, configSchema]) => {
        if (!mounted) {
          return;
        }
        setDraft(config);
        setSchema(configSchema);
      })
      .catch((error) => {
        if (mounted) {
          setValidationError(String(error.message || error));
        }
      });
    return () => {
      mounted = false;
    };
  }, [service.roomId]);

  const saveConfig = async (startAfterSave = false) => {
    if (!draft) {
      return;
    }
    setSaveState("saving");
    setValidationError("");
    try {
      const saved = await fetchJson(roomApiPath(service.roomId, "config"), {
        method: "PUT",
        body: JSON.stringify(normalizeDraftForSubmit(draft)),
      });
      setDraft(saved.config);
      setSaveState("saved");
      await service.refreshStatus().catch(() => {});
      if (startAfterSave) {
        await service.startRun();
      }
    } catch (error) {
      setSaveState("error");
      setValidationError(String(error.message || error));
    }
  };

  if (!draft) {
    return html`
      <div className="config-shell">
        <section className="section-card"><p>Loading room config…</p></section>
      </div>
    `;
  }

  return html`
    <div className="config-shell">
      <section className="hero-card hero-compact">
        <div>
          <div className="eyebrow">Room settings</div>
          <h1>${service.status?.room_title || service.roomId}</h1>
          <p>Config changes apply to the next run for this room, so you can safely tune the prompt and agent cast before restarting.</p>
        </div>
        <div className="hero-actions">
          <div className="toolbar-row wrap">
            <a className="button ghost" href=${roomPath(service.roomId)}>Back to room</a>
            <button className="button" type="button" onClick=${() => saveConfig(false)}>Save draft</button>
            <button className="button secondary" type="button" onClick=${() => saveConfig(true)}>Save and start</button>
          </div>
          ${saveState === "saved" ? html`<div className="banner">Draft config saved for this room.</div>` : null}
        </div>
      </section>

      ${validationError ? html`<div className="banner error">${validationError}</div>` : null}

      <div className="setup-layout">
        <main className="setup-main">
          <${RoomBasicsSection} draft=${draft} setDraft=${setDraft} schema=${schema} roomName=${service.roomId} setRoomName=${() => {}} showRoomPath=${false} />
          <${AgentEditorSection} draft=${draft} setDraft=${setDraft} />
          <details className="advanced-card raw-config-card">
            <summary>Raw config JSON</summary>
            <pre className="debug-json">${prettyJson(draft)}</pre>
          </details>
        </main>
        <aside className="setup-side">
          <${SetupSummary} draft=${draft} roomId=${service.roomId} />
          <${SectionCard}
            eyebrow="Status"
            title="Current room snapshot"
            description="Use this to compare the live room with the next-run draft."
          >
            <pre className="debug-json">${prettyJson(service.status || {})}</pre>
          </${SectionCard}>
          <${SectionCard}
            eyebrow="Supported values"
            title="Schema hints"
            description="Available runtime providers and modes from the backend."
          >
            <div className="summary-stack">
              <div className="summary-card"><span className="section-label">Room modes</span><p>${(schema?.room_modes || []).join(", ")}</p></div>
              <div className="summary-card"><span className="section-label">Modes</span><p>${(schema?.modes || []).join(", ")}</p></div>
              <div className="summary-card"><span className="section-label">Runtime providers</span><p>${(schema?.runtime_providers || []).join(", ")}</p></div>
              <div className="summary-card"><span className="section-label">Aliases</span><pre className="debug-json">${prettyJson(schema?.runtime_aliases || {})}</pre></div>
            </div>
          </${SectionCard}>
        </aside>
      </div>
    </div>
  `;
}

function App() {
  const route = useMemo(() => parseRoute(), []);
  const lobby = useLobbyService();
  const roomService = useRoomService(route.roomId || DEFAULT_ROOM_ID, route.page !== "lobby");

  const page = route.page === "lobby"
    ? html`<${LobbyPage} lobby=${lobby} />`
    : route.page === "config"
      ? html`<${ConfigPage} service=${roomService} />`
      : html`<${ChatPage} service=${roomService} />`;

  return html`<div className="app-shell">${page}</div>`;
}

const container = document.getElementById("root");
createRoot(container).render(html`<${App} />`);
