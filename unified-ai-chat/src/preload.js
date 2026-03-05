/**
 * Preload — runs in the renderer (BrowserWindow) context.
 * Exposes a safe `window.electronAPI` bridge so renderer JS can
 * communicate with the main process without nodeIntegration.
 */

const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("electronAPI", {
  // Receive menu commands from main → renderer
  onMenuSwitchTab: (cb) => ipcRenderer.on("menu-switch-tab", (_, tab) => cb(tab)),
  onMenuBridge:    (cb) => ipcRenderer.on("menu-bridge", () => cb()),
  onMenuReload:    (cb) => ipcRenderer.on("menu-reload", () => cb()),
});
