/**
 * Unified AI Chat — Renderer Process
 *
 * Manages:
 *  - Tab switching (ChatGPT ↔ Claude)
 *  - Split-view toggle
 *  - Context bridge: grab the last AI reply from one webview,
 *    let the user edit it, then inject it into the other webview's input
 *  - Loading indicators per tab
 */

// ── Elements ─────────────────────────────────────────────────────────────────
const wvGPT    = document.getElementById("wv-chatgpt");
const wvClaude = document.getElementById("wv-claude");
const tabGPT   = document.getElementById("tab-chatgpt");
const tabClaude= document.getElementById("tab-claude");
const statusGPT   = document.getElementById("status-chatgpt");
const statusClaude= document.getElementById("status-claude");

const btnBridge  = document.getElementById("btn-bridge");
const btnSplit   = document.getElementById("btn-split");
const btnReload  = document.getElementById("btn-reload");

const bridgePanel  = document.getElementById("bridge-panel");
const bridgeText   = document.getElementById("bridge-text");
const bridgeSource = document.getElementById("bridge-source");
const bridgeDest   = document.getElementById("bridge-dest");
const btnSendBridge    = document.getElementById("btn-send-bridge");
const btnDismissBridge = document.getElementById("btn-dismiss-bridge");

const loadingOverlay = document.getElementById("loading-overlay");
const loadingLabel   = document.getElementById("loading-label");

// ── State ─────────────────────────────────────────────────────────────────────
let activeTab = "chatgpt"; // "chatgpt" | "claude"
let splitMode = false;

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(tab) {
  if (splitMode) return; // in split mode both are visible

  activeTab = tab;

  // Webview visibility
  wvGPT.classList.toggle("hidden", tab !== "chatgpt");
  wvClaude.classList.toggle("hidden", tab !== "claude");

  // Tab button active state
  tabGPT.classList.toggle("active", tab === "chatgpt");
  tabClaude.classList.toggle("active", tab === "claude");
}

tabGPT.addEventListener("click",    () => switchTab("chatgpt"));
tabClaude.addEventListener("click", () => switchTab("claude"));

// Keyboard shortcuts from menu (via preload bridge)
window.electronAPI?.onMenuSwitchTab(tab => switchTab(tab));
window.electronAPI?.onMenuReload(() => activeWebview().reload());

// ── Split view ────────────────────────────────────────────────────────────────
btnSplit.addEventListener("click", () => {
  splitMode = !splitMode;
  document.body.toggleAttribute("data-split", splitMode);
  btnSplit.textContent = splitMode ? "⊟ Single" : "⊞ Split";

  if (splitMode) {
    // Show both, remove hidden
    wvGPT.classList.remove("hidden");
    wvClaude.classList.remove("hidden");
    tabGPT.classList.add("active");
    tabClaude.classList.add("active");
  } else {
    switchTab(activeTab);
  }
});

// ── Reload ────────────────────────────────────────────────────────────────────
btnReload.addEventListener("click", () => activeWebview().reload());

function activeWebview() {
  return activeTab === "chatgpt" ? wvGPT : wvClaude;
}

// ── Loading indicators ────────────────────────────────────────────────────────
function attachLoadingEvents(wv, statusEl, label) {
  wv.addEventListener("did-start-loading", () => {
    statusEl.className = "tab-status loading";
    if (!splitMode && wv === activeWebview()) {
      loadingOverlay.classList.remove("hidden");
      loadingLabel.textContent = `Loading ${label}…`;
    }
  });

  wv.addEventListener("did-stop-loading", () => {
    statusEl.className = "tab-status ready";
    loadingOverlay.classList.add("hidden");
  });

  wv.addEventListener("did-fail-load", (e) => {
    if (e.errorCode === -3) return; // aborted (navigation) — ignore
    statusEl.className = "tab-status";
  });
}

attachLoadingEvents(wvGPT,    statusGPT,    "ChatGPT");
attachLoadingEvents(wvClaude, statusClaude, "Claude");

// ── Context bridge ────────────────────────────────────────────────────────────
/**
 * Extracts the last assistant reply from a webview using JS injection.
 * Works by querying DOM elements specific to each site.
 */
async function captureLastReply(wv, site) {
  let js;
  if (site === "chatgpt") {
    // ChatGPT: last [data-message-author-role="assistant"] container
    js = `
      (function() {
        const msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
        if (!msgs.length) return "";
        const last = msgs[msgs.length - 1];
        return (last.innerText || last.textContent || "").trim();
      })()
    `;
  } else {
    // Claude: last .font-claude-message block (main turn content)
    js = `
      (function() {
        // Claude renders responses inside a div with data-is-streaming=false
        const blocks = document.querySelectorAll('[data-is-streaming="false"]');
        if (blocks.length) {
          const last = blocks[blocks.length - 1];
          return (last.innerText || last.textContent || "").trim();
        }
        // Fallback: any assistant message prose
        const prose = document.querySelectorAll('.font-claude-message');
        if (!prose.length) return "";
        return (prose[prose.length - 1].innerText || "").trim();
      })()
    `;
  }

  try {
    const result = await wv.executeJavaScript(js);
    return (result || "").trim();
  } catch (err) {
    console.error("captureLastReply error:", err);
    return "";
  }
}

/**
 * Injects text into the active input of a webview.
 * Both ChatGPT and Claude use a ProseMirror or contenteditable textarea.
 */
async function injectTextIntoInput(wv, site, text) {
  const escaped = JSON.stringify(text);
  let js;

  if (site === "chatgpt") {
    js = `
      (function() {
        const el = document.getElementById('prompt-textarea')
                || document.querySelector('[data-id="root"] textarea')
                || document.querySelector('textarea[placeholder]');
        if (!el) return false;
        el.focus();
        const nativeInput = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
        nativeInput.set.call(el, ${escaped});
        el.dispatchEvent(new Event('input', { bubbles: true }));
        return true;
      })()
    `;
  } else {
    // Claude uses a ProseMirror contenteditable div
    js = `
      (function() {
        const editor = document.querySelector('.ProseMirror[contenteditable="true"]')
                    || document.querySelector('[contenteditable="true"][data-placeholder]');
        if (!editor) return false;
        editor.focus();
        // Clear existing content and insert new text
        document.execCommand('selectAll', false, null);
        document.execCommand('insertText', false, ${escaped});
        editor.dispatchEvent(new InputEvent('input', { bubbles: true }));
        return true;
      })()
    `;
  }

  try {
    const ok = await wv.executeJavaScript(js);
    return !!ok;
  } catch (err) {
    console.error("injectText error:", err);
    return false;
  }
}

// Bridge button — capture from the INACTIVE webview, show panel
async function openBridge() {
  const sourceTab = activeTab === "chatgpt" ? "claude" : "chatgpt";
  const sourceWv  = sourceTab === "chatgpt" ? wvGPT : wvClaude;
  const destTab   = activeTab;

  const sourceLabel = sourceTab === "chatgpt" ? "ChatGPT" : "Claude";
  const destLabel   = destTab   === "chatgpt" ? "ChatGPT" : "Claude";

  const reply = await captureLastReply(sourceWv, sourceTab);

  bridgeSource.textContent = sourceLabel;
  bridgeDest.textContent   = destLabel;
  bridgeText.value = reply || "(No reply found — scroll to a message in the other tab first)";

  // Open panel
  bridgePanel.classList.remove("hidden");
  document.body.setAttribute("data-bridge-open", "");
  // Measure panel height for CSS variable
  requestAnimationFrame(() => {
    document.body.style.setProperty("--bridge-h", bridgePanel.offsetHeight + "px");
  });
  bridgeText.focus();
}

btnBridge.addEventListener("click", openBridge);
window.electronAPI?.onMenuBridge(openBridge);

// Send bridge text to dest webview input
btnSendBridge.addEventListener("click", async () => {
  const text = bridgeText.value.trim();
  if (!text) return;

  const destTab = activeTab;
  const destWv  = destTab === "chatgpt" ? wvGPT : wvClaude;

  const ok = await injectTextIntoInput(destWv, destTab, text);

  // Close panel
  closeBridgePanel();

  if (!ok) {
    alert("Could not find the input box. Make sure you're on a new conversation page.");
  } else {
    // Focus the webview so the user can review + press Send
    destWv.focus();
  }
});

btnDismissBridge.addEventListener("click", closeBridgePanel);

function closeBridgePanel() {
  bridgePanel.classList.add("hidden");
  document.body.removeAttribute("data-bridge-open");
}

// ── Initial tab state ─────────────────────────────────────────────────────────
switchTab("chatgpt");
