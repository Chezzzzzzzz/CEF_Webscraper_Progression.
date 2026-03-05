/**
 * Unified AI Chat — Electron Main Process
 *
 * Creates one BrowserWindow that hosts the renderer (the chrome bar).
 * Two <webview> tags inside the renderer each carry their own persistent
 * session partition so cookies / logins are kept separate:
 *   partition: "persist:chatgpt"
 *   partition: "persist:claude"
 *
 * IPC surface (renderer ↔ main):
 *   "get-active"          → returns current active tab ("chatgpt" | "claude")
 *   "switch-tab"          → renderer asks to switch tab
 *   "webview-title"       → renderer forwards title change from a webview
 *   "inject-text"         → main tells active webview to paste text into its input
 *   "capture-last-reply"  → renderer asks main to extract last assistant reply
 *                           from the currently INACTIVE webview
 */

const { app, BrowserWindow, ipcMain, Menu, shell } = require("electron");
const path = require("path");

let mainWindow;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 860,
    minWidth: 900,
    minHeight: 600,
    backgroundColor: "#0f0f13",
    titleBarStyle: process.platform === "darwin" ? "hiddenInset" : "default",
    title: "Unified AI Chat",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      // Allow the renderer to host <webview> tags
      webviewTag: true,
    },
  });

  mainWindow.loadFile(path.join(__dirname, "renderer", "index.html"));

  // Forward console output from renderer in dev
  if (process.env.NODE_ENV === "development") {
    mainWindow.webContents.openDevTools({ mode: "detach" });
  }

  buildMenu();
}

// ── Application menu ──────────────────────────────────────────────────────────
function buildMenu() {
  const template = [
    {
      label: "Chat",
      submenu: [
        {
          label: "Switch to ChatGPT",
          accelerator: "CmdOrCtrl+1",
          click: () => mainWindow.webContents.send("menu-switch-tab", "chatgpt"),
        },
        {
          label: "Switch to Claude",
          accelerator: "CmdOrCtrl+2",
          click: () => mainWindow.webContents.send("menu-switch-tab", "claude"),
        },
        { type: "separator" },
        {
          label: "Send Last Reply to Other AI",
          accelerator: "CmdOrCtrl+Shift+C",
          click: () => mainWindow.webContents.send("menu-bridge"),
        },
        { type: "separator" },
        {
          label: "Reload Active Tab",
          accelerator: "CmdOrCtrl+R",
          click: () => mainWindow.webContents.send("menu-reload"),
        },
      ],
    },
    {
      label: "Edit",
      submenu: [
        { role: "undo" }, { role: "redo" }, { type: "separator" },
        { role: "cut" }, { role: "copy" }, { role: "paste" },
        { role: "selectAll" },
      ],
    },
    {
      label: "View",
      submenu: [
        {
          label: "Toggle DevTools (main)",
          accelerator: "CmdOrCtrl+Option+I",
          click: () => mainWindow.webContents.toggleDevTools(),
        },
        { type: "separator" },
        { role: "resetZoom" }, { role: "zoomIn" }, { role: "zoomOut" },
        { type: "separator" },
        { role: "togglefullscreen" },
      ],
    },
    {
      label: "Window",
      submenu: [{ role: "minimize" }, { role: "zoom" }, { role: "close" }],
    },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

// ── IPC handlers ─────────────────────────────────────────────────────────────

// Renderer can ask for the currently saved active tab
ipcMain.handle("get-active", () => "chatgpt");

// Open external links in the system browser instead of a new Electron window
app.on("web-contents-created", (_, contents) => {
  contents.setWindowOpenHandler(({ url }) => {
    // Allow popups from the AI sites (OAuth, etc.) inside a new Electron window
    const allowed = ["accounts.google.com", "auth0.com", "login.microsoftonline.com"];
    if (allowed.some(d => url.includes(d))) {
      return { action: "allow" };
    }
    return { action: "deny" };
  });

  // Prevent navigation away from the two known domains inside the webviews
  contents.on("will-navigate", (event, url) => {
    const allowed = [
      "chatgpt.com", "chat.openai.com", "openai.com",
      "claude.ai", "anthropic.com",
      "accounts.google.com", "auth0.com", "login.microsoftonline.com",
      "appleid.apple.com",
    ];
    const isAllowed = allowed.some(d => url.includes(d));
    if (!isAllowed && !url.startsWith("file://")) {
      event.preventDefault();
      shell.openExternal(url);
    }
  });
});

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
