# Unified AI Chat

One desktop window. Two real AI interfaces. Zero context loss.

Log in to **ChatGPT** and **Claude** once — then switch between them
instantly without leaving the app. Use the **Context Bridge** to grab
the last reply from one AI and send it directly into the other's input box.

---

## How it works

This is an **Electron app** that embeds the real `chatgpt.com` and
`claude.ai` web interfaces inside two persistent WebViews. Each site
gets its own isolated browser session (cookies, storage, logins) so
you stay logged in to both independently.

```
┌─────────────────────────────────────────────────┐
│  [ChatGPT ●]  [Claude]   ⌘1/⌘2  🔀 Bridge  ⊞ Split  │  ← Chrome bar
├─────────────────────────────────────────────────┤
│                                                 │
│         chatgpt.com  (real web UI)              │  ← WebView A
│         claude.ai    (hidden)                   │  ← WebView B
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Features

| Feature | How to use |
|---|---|
| **Switch AI** | Click the tab or press **⌘1** (ChatGPT) / **⌘2** (Claude) |
| **Context Bridge** | Press **🔀 Bridge context** (or **⌘⇧C**) to grab the last reply from the *other* AI, edit it, then inject it into the current AI's input box |
| **Split view** | Click **⊞ Split** to see both side-by-side |
| **Reload tab** | Click **↺** or press **⌘R** |
| **Persistent logins** | Each webview uses its own `persist:` session partition — logins survive restarts |

---

## Quick start

### Prerequisites
- [Node.js](https://nodejs.org) 18+

### Install & run
```bash
cd unified-ai-chat
npm install
npm start
```

### Build distributable
```bash
npm run build:linux   # → AppImage
npm run build:win     # → NSIS installer
npm run build:mac     # → DMG
```

---

## File structure

```
unified-ai-chat/
├── package.json
└── src/
    ├── main.js          # Electron main process — window, menu, webContents policy
    ├── preload.js       # contextBridge exposing IPC to renderer
    └── renderer/
        ├── index.html   # Chrome bar + <webview> tags
        ├── style.css    # Dark theme
        ├── renderer.js  # Tab switching, bridge logic, loading indicators
        └── icons/
            ├── chatgpt.svg
            └── claude.svg
```

---

## Context bridge — detailed flow

1. You're chatting with **ChatGPT**. Claude gave you useful background earlier.
2. Press **⌘⇧C** (or click **🔀 Bridge context**).
3. Electron runs `executeJavaScript` in the **Claude WebView** to extract the
   last assistant reply from the DOM.
4. The reply appears in an editable panel — you can trim or annotate it.
5. Click **Send to ChatGPT ➤** — Electron injects the text into ChatGPT's
   input field using `executeJavaScript`. You just press Enter to send.

---

## Notes

- No API keys required — you use your own accounts on each site.
- External links (OAuth flows, etc.) open in your system browser.
- The bridge DOM selectors may need updating if ChatGPT or Claude
  significantly change their frontend markup.
