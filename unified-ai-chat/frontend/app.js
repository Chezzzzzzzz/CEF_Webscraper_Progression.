/**
 * Unified AI Chat — Frontend
 *
 * Architecture notes:
 *  - Single AppState object is source of truth
 *  - All API calls go through the api module
 *  - SSE streaming updates the DOM incrementally
 *  - Switching providers mid-conversation is instant — the backend
 *    already holds the full normalized history
 */

const API = "/api";

// ── State ────────────────────────────────────────────────────────────────────
const state = {
  provider: "openai",       // "openai" | "claude"
  model: null,              // null = use backend default
  conversationId: null,
  isStreaming: false,
  models: { openai: [], claude: [] },
};

// ── DOM refs ─────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const els = {
  messages:      $("messages"),
  chatContainer: $("chat-container"),
  input:         $("message-input"),
  sendBtn:       $("send-btn"),
  modelSelect:   $("model-select"),
  contextBadge:  $("context-badge"),
  convList:      $("conversation-list"),
  emptyState:    $("empty-state"),
  systemInput:   $("system-prompt-input"),
  sysPanel:      $("system-prompt-panel"),
};

// ── Init ─────────────────────────────────────────────────────────────────────
async function init() {
  await loadModels();
  await loadConversationList();
  setupEventListeners();
  updateProviderUI();
}

async function loadModels() {
  try {
    const data = await apiFetch("/models");
    state.models = data;
  } catch {
    state.models = {
      openai: { models: ["gpt-4o", "gpt-3.5-turbo"], default: "gpt-4o" },
      claude: { models: ["claude-sonnet-4-6", "claude-haiku-4-5-20251001"], default: "claude-sonnet-4-6" },
    };
  }
  rebuildModelSelect();
}

function rebuildModelSelect() {
  const pData = state.models[state.provider];
  els.modelSelect.innerHTML = "";
  (pData?.models || []).forEach(m => {
    const opt = document.createElement("option");
    opt.value = m;
    opt.textContent = m;
    if (m === pData?.default) opt.selected = true;
    els.modelSelect.appendChild(opt);
  });
  state.model = els.modelSelect.value || null;
}

// ── Event wiring ──────────────────────────────────────────────────────────────
function setupEventListeners() {
  // Provider toggle
  document.querySelectorAll(".provider-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      state.provider = btn.dataset.provider;
      updateProviderUI();
      rebuildModelSelect();
    });
  });

  // Model select
  els.modelSelect.addEventListener("change", () => {
    state.model = els.modelSelect.value;
  });

  // Send button
  els.sendBtn.addEventListener("click", sendMessage);

  // Enter to send (Shift+Enter = newline)
  els.input.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  // Auto-resize textarea
  els.input.addEventListener("input", () => {
    els.input.style.height = "auto";
    els.input.style.height = Math.min(els.input.scrollHeight, 200) + "px";
  });

  // New chat
  $("new-chat-btn").addEventListener("click", () => {
    state.conversationId = null;
    clearMessages();
    showEmptyState(true);
    highlightActiveConv(null);
  });

  // System prompt toggle
  $("system-prompt-toggle").addEventListener("click", () => {
    els.sysPanel.classList.toggle("open");
  });
}

function updateProviderUI() {
  document.querySelectorAll(".provider-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.provider === state.provider);
  });
}

// ── Send a message ───────────────────────────────────────────────────────────
async function sendMessage() {
  const text = els.input.value.trim();
  if (!text || state.isStreaming) return;

  showEmptyState(false);

  // Render user bubble immediately
  appendMessage({ role: "user", content: text });
  els.input.value = "";
  els.input.style.height = "auto";

  // Create a streaming assistant bubble
  const bubbleId = "streaming-" + Date.now();
  const bubble = appendMessage({
    role: "assistant",
    content: "",
    provider: state.provider,
    model: state.model || state.models[state.provider]?.default,
    id: bubbleId,
    streaming: true,
  });

  setStreaming(true);

  try {
    await streamChat({
      conversation_id: state.conversationId,
      message: text,
      provider: state.provider,
      model: state.model || undefined,
      system_prompt: els.systemInput.value.trim() || undefined,
      stream: true,
    }, bubble);
  } catch (err) {
    updateBubbleContent(bubble, `⚠ Error: ${err.message}`);
  } finally {
    setStreaming(false);
  }
}

async function streamChat(payload, bubble) {
  const resp = await fetch(`${API}/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(errText);
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let fullContent = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop(); // last potentially incomplete line

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      const raw = line.slice(6).trim();
      if (!raw) continue;

      let evt;
      try { evt = JSON.parse(raw); } catch { continue; }

      if (evt.type === "meta") {
        state.conversationId = evt.conversation_id;
        updateBubbleMeta(bubble, evt.model);
        updateContextBadge();
        await loadConversationList();
        highlightActiveConv(state.conversationId);
      } else if (evt.type === "chunk") {
        fullContent += evt.content;
        updateBubbleContent(bubble, fullContent);
        scrollToBottom();
      } else if (evt.type === "done") {
        finalizeBubble(bubble);
      } else if (evt.type === "error") {
        throw new Error(evt.detail);
      }
    }
  }
}

// ── DOM helpers ───────────────────────────────────────────────────────────────
function appendMessage({ role, content, provider, model, id, streaming }) {
  const div = document.createElement("div");
  div.className = `message ${role}`;
  if (id) div.id = id;

  const meta = document.createElement("div");
  meta.className = "message-meta";

  const badge = document.createElement("span");
  badge.className = `role-badge ${role === "user" ? "user" : (provider || "")}`;
  badge.textContent = role === "user" ? "You" : (provider === "openai" ? "ChatGPT" : "Claude");
  meta.appendChild(badge);

  if (model) {
    const modelSpan = document.createElement("span");
    modelSpan.className = "model-tag";
    modelSpan.textContent = model;
    modelSpan.style.cssText = "font-size:11px;color:var(--text-muted)";
    meta.appendChild(modelSpan);
    div.dataset.model = model;
  }

  const body = document.createElement("div");
  body.className = "message-body" + (streaming ? " streaming-cursor" : "");
  body.textContent = content;

  div.appendChild(meta);
  div.appendChild(body);
  els.messages.appendChild(div);
  scrollToBottom();
  return div;
}

function updateBubbleContent(bubble, content) {
  const body = bubble.querySelector(".message-body");
  if (body) body.textContent = content;
}

function updateBubbleMeta(bubble, model) {
  const modelTag = bubble.querySelector(".model-tag");
  if (modelTag) modelTag.textContent = model;
  if (!modelTag && model) {
    const meta = bubble.querySelector(".message-meta");
    const span = document.createElement("span");
    span.className = "model-tag";
    span.style.cssText = "font-size:11px;color:var(--text-muted)";
    span.textContent = model;
    meta?.appendChild(span);
  }
}

function finalizeBubble(bubble) {
  const body = bubble.querySelector(".message-body");
  if (body) body.classList.remove("streaming-cursor");
}

function clearMessages() {
  els.messages.innerHTML = "";
}

function scrollToBottom() {
  els.chatContainer.scrollTop = els.chatContainer.scrollHeight;
}

function setStreaming(val) {
  state.isStreaming = val;
  els.sendBtn.disabled = val;
  els.input.disabled = val;
}

function showEmptyState(show) {
  els.emptyState.style.display = show ? "flex" : "none";
  els.chatContainer.style.display = show ? "none" : "flex";
}

function updateContextBadge() {
  const conv = state.conversationId;
  els.contextBadge.textContent = conv ? `Thread: ${conv.slice(0, 8)}…` : "No thread";
}

// ── Conversation list ─────────────────────────────────────────────────────────
async function loadConversationList() {
  try {
    const convs = await apiFetch("/conversations");
    renderConvList(convs);
  } catch { /* ignore */ }
}

function renderConvList(convs) {
  els.convList.innerHTML = "";
  convs.forEach(c => {
    const item = document.createElement("div");
    item.className = "conv-item" + (c.id === state.conversationId ? " active" : "");
    item.dataset.id = c.id;

    const title = document.createElement("span");
    title.style.cssText = "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0";
    title.textContent = c.title || "New conversation";

    const del = document.createElement("button");
    del.className = "del-btn";
    del.title = "Delete";
    del.textContent = "✕";
    del.addEventListener("click", async e => {
      e.stopPropagation();
      await deleteConversation(c.id);
    });

    item.appendChild(title);
    item.appendChild(del);
    item.addEventListener("click", () => loadConversation(c.id));
    els.convList.appendChild(item);
  });
}

function highlightActiveConv(id) {
  document.querySelectorAll(".conv-item").forEach(el => {
    el.classList.toggle("active", el.dataset.id === id);
  });
}

async function loadConversation(id) {
  try {
    const conv = await apiFetch(`/conversations/${id}`);
    state.conversationId = id;
    clearMessages();
    showEmptyState(false);
    conv.messages.forEach(m => {
      if (m.role === "system") return;
      appendMessage({
        role: m.role,
        content: m.content,
        provider: m.provider,
        model: m.model,
      });
    });
    scrollToBottom();
    updateContextBadge();
    highlightActiveConv(id);
  } catch (err) {
    console.error("Failed to load conversation", err);
  }
}

async function deleteConversation(id) {
  try {
    await apiFetch(`/conversations/${id}`, { method: "DELETE" });
    if (state.conversationId === id) {
      state.conversationId = null;
      clearMessages();
      showEmptyState(true);
      updateContextBadge();
    }
    await loadConversationList();
  } catch (err) {
    console.error("Failed to delete", err);
  }
}

// ── API helper ────────────────────────────────────────────────────────────────
async function apiFetch(path, options = {}) {
  const resp = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
    body: options.body ? (typeof options.body === "string" ? options.body : JSON.stringify(options.body)) : undefined,
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  if (resp.status === 204) return null;
  return resp.json();
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", init);
