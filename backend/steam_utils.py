"""Steam-related utilities used across LuaTools backend modules."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from typing import Any, Dict, Optional

import Millennium  # type: ignore

from logger import logger

_STEAM_INSTALL_PATH: Optional[str] = None

if sys.platform.startswith("win"):
    try:
        import winreg  # type: ignore
    except Exception:  # pragma: no cover
        winreg = None  # type: ignore
else:
    winreg = None  # type: ignore


def detect_steam_install_path() -> str:
    """Return the cached Steam installation path or discover it.

    Resolution order:
    1. In-memory cache (fastest)
    2. Windows registry HKCU\\Software\\Valve\\Steam (most reliable on Windows)
    3. Millennium.steam_path() (Millennium host fallback)
    """
    global _STEAM_INSTALL_PATH
    if _STEAM_INSTALL_PATH:
        return _STEAM_INSTALL_PATH

    path: Optional[str] = None

    if sys.platform.startswith("win") and winreg is not None:
        for hive, key_path, value in [
            (winreg.HKEY_CURRENT_USER, r"Software\Valve\Steam", "SteamPath"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Valve\Steam", "InstallPath"),
        ]:
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    candidate, _ = winreg.QueryValueEx(key, value)
                    if candidate and os.path.exists(str(candidate)):
                        path = str(candidate)
                        break
            except Exception:
                continue

    if not path:
        try:
            candidate = Millennium.steam_path()
            if candidate and os.path.exists(str(candidate)):
                path = str(candidate)
        except Exception:
            pass

    _STEAM_INSTALL_PATH = path
    logger.log(f"LuaTools: Steam install path set to {_STEAM_INSTALL_PATH}")
    return _STEAM_INSTALL_PATH or ""


# Alias kept for internal callers that used the old private function name.
_find_steam_path = detect_steam_install_path


def _parse_vdf_simple(content: str) -> Dict[str, Any]:
    """Simple VDF parser for libraryfolders.vdf and appmanifest files."""
    result: Dict[str, Any] = {}
    stack = [result]
    current_key: Optional[str] = None

    tokens = []
    for line in content.split("\n"):
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        tokens.extend(re.findall(r'"[^"]*"|\{|\}', line))

    i = 0
    while i < len(tokens):
        raw = tokens[i]
        token = raw.strip('"')

        if raw == "{":
            if current_key:
                new_dict: Dict[str, Any] = {}
                stack[-1][current_key] = new_dict
                stack.append(new_dict)
                current_key = None
        elif raw == "}":
            if len(stack) > 1:
                stack.pop()
        elif current_key is None:
            current_key = token
        else:
            stack[-1][current_key] = token
            current_key = None
        i += 1

    return result


# ── Steam Library Refresh ────────────────────────────────────────────────────
#
# Problem: after writing a new .lua file to stplug-in/, Steam's internal games
# library does not refresh automatically.  Users must restart Steam or toggle
# offline/online to force a rescan.
#
# Solution (senior-level, no Steam restart required):
#
# 1. PRIMARY — SteamAPI IPC via named pipe / steam:// protocol
#    Steam listens on a local named pipe (Windows: \\.\pipe\SteamServicePipe
#    or via the steam:// protocol handler).  We fire  `steam://reload/<appid>`
#    which instructs the running Steam client to rescan that specific appid.
#
# 2. SECONDARY — ACF manifest touch trick
#    Steam watches appmanifest_*.acf files for changes. If we set the mtime of
#    the relevant .acf file to `now`, the file-watcher thread in Steam wakes up
#    and rescans the library entry for that appid — making the game appear as
#    "owned" immediately.
#
# 3. TERTIARY — stplug-in sentinel file touch
#    Writing a temp file inside stplug-in/ (then deleting it) can also trigger
#    the Steamworks plugin watcher used by Millennium itself.
#
# We attempt all three in order, silently ignoring failures.

def _touch_file(path: str) -> None:
    """Update mtime of an existing file to now without changing content."""
    try:
        now = time.time()
        os.utime(path, (now, now))
    except Exception:
        pass


def trigger_steam_library_refresh(appid: int) -> None:
    """Force Steam to refresh its library view for *appid* without a restart.

    Attempts three strategies in order of reliability:
    1. steam:// URI protocol handler  →  fastest, tells Steam directly
    2. ACF manifest mtime touch       →  file-watcher based, very reliable
    3. stplug-in sentinel touch       →  Millennium plugin watcher fallback
    """
    appid_int = int(appid)

    # ── Strategy 1: steam:// URI ──────────────────────────────────────────
    # `steam://nav/games/details/<appid>` forces the Steam React UI to navigate 
    # to the game. This triggers `GetAppDetails`, which reads our hooked ownership
    # status and immediately caches the app, making it appear in the sidebar
    # without requiring a restart!
    try:
        if sys.platform.startswith("win"):
            subprocess.Popen(
                ["cmd", "/c", f"start steam://nav/games/details/{appid_int}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            logger.log(f"LuaTools: Sent steam://nav/games/details/{appid_int}")
        else:
            subprocess.Popen(
                ["steam", f"steam://nav/games/details/{appid_int}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception as exc:
        logger.warn(f"LuaTools: steam:// reload failed: {exc}")

    # ── Strategy 2: Touch the ACF manifest ───────────────────────────────
    # Steam's library thread polls appmanifest mtime. Touching refreshes the
    # entry without touching game files.
    try:
        steam_path = detect_steam_install_path()
        if steam_path:
            library_vdf = os.path.join(steam_path, "config", "libraryfolders.vdf")
            if os.path.exists(library_vdf):
                with open(library_vdf, "r", encoding="utf-8") as fh:
                    vdf_content = fh.read()
                library_data = _parse_vdf_simple(vdf_content)
                app_str = str(appid_int)

                for folder_data in library_data.get("libraryfolders", {}).values():
                    if not isinstance(folder_data, dict):
                        continue
                    folder_path = folder_data.get("path", "").replace("\\\\", "\\")
                    if not folder_path:
                        continue
                    apps = folder_data.get("apps", {})
                    if isinstance(apps, dict) and app_str in apps:
                        acf = os.path.join(folder_path, "steamapps", f"appmanifest_{appid_int}.acf")
                        if os.path.exists(acf):
                            _touch_file(acf)
                            logger.log(f"LuaTools: Touched ACF {acf} for library refresh")
                        break
    except Exception as exc:
        logger.warn(f"LuaTools: ACF touch failed: {exc}")

    # ── Strategy 3: stplug-in sentinel file ──────────────────────────────
    # Writing + deleting a dummy file triggers Millennium's file-watcher.
    try:
        steam_path = detect_steam_install_path()
        if steam_path:
            stplug = os.path.join(steam_path, "config", "stplug-in")
            if os.path.isdir(stplug):
                sentinel = os.path.join(stplug, f".lt_refresh_{appid_int}")
                with open(sentinel, "w") as fh:
                    fh.write("")
                time.sleep(0.05)
                try:
                    os.remove(sentinel)
                except Exception:
                    pass
    except Exception:
        pass


def has_lua_for_app(appid: int) -> bool:
    try:
        base_path = detect_steam_install_path() or Millennium.steam_path()
        if not base_path:
            return False
        stplug_path = os.path.join(base_path, "config", "stplug-in")
        lua_file = os.path.join(stplug_path, f"{appid}.lua")
        disabled_file = os.path.join(stplug_path, f"{appid}.lua.disabled")
        return os.path.exists(lua_file) or os.path.exists(disabled_file)
    except Exception as exc:
        logger.error(f"LuaTools (steam_utils): Error checking Lua scripts for app {appid}: {exc}")
        return False


def get_game_install_path_response(appid: int) -> Dict[str, Any]:
    """Find the game installation path. Returns dict mirroring previous JSON output."""
    try:
        appid = int(appid)
    except Exception:
        return {"success": False, "error": "Invalid appid"}

    steam_path = detect_steam_install_path()
    if not steam_path:
        return {"success": False, "error": "Could not find Steam installation path"}

    library_vdf_path = os.path.join(steam_path, "config", "libraryfolders.vdf")
    if not os.path.exists(library_vdf_path):
        logger.warn(f"LuaTools: libraryfolders.vdf not found at {library_vdf_path}")
        return {"success": False, "error": "Could not find libraryfolders.vdf"}

    try:
        with open(library_vdf_path, "r", encoding="utf-8") as handle:
            vdf_content = handle.read()
        library_data = _parse_vdf_simple(vdf_content)
    except Exception as exc:
        logger.warn(f"LuaTools: Failed to parse libraryfolders.vdf: {exc}")
        return {"success": False, "error": "Failed to parse libraryfolders.vdf"}

    library_folders = library_data.get("libraryfolders", {})
    library_path: Optional[str] = None
    appid_str = str(appid)
    all_library_paths = []

    for folder_data in library_folders.values():
        if not isinstance(folder_data, dict):
            continue
        folder_path = folder_data.get("path", "").replace("\\\\", "\\")
        if folder_path:
            all_library_paths.append(folder_path)
        apps = folder_data.get("apps", {})
        if isinstance(apps, dict) and appid_str in apps:
            library_path = folder_path
            break

    appmanifest_path: Optional[str] = None
    if not library_path:
        logger.log(
            f"LuaTools: appid {appid} not in libraryfolders.vdf, searching all libraries for appmanifest"
        )
        for lib_path in all_library_paths:
            candidate_path = os.path.join(lib_path, "steamapps", f"appmanifest_{appid}.acf")
            if os.path.exists(candidate_path):
                library_path = lib_path
                appmanifest_path = candidate_path
                logger.log(f"LuaTools: Found appmanifest at {appmanifest_path}")
                break
    else:
        appmanifest_path = os.path.join(library_path, "steamapps", f"appmanifest_{appid}.acf")

    if not library_path or not appmanifest_path or not os.path.exists(appmanifest_path):
        logger.log(f"LuaTools: appmanifest not found for {appid} in any library")
        return {"success": False, "error": "menu.error.notInstalled"}

    try:
        with open(appmanifest_path, "r", encoding="utf-8") as handle:
            manifest_content = handle.read()
        manifest_data = _parse_vdf_simple(manifest_content)
    except Exception as exc:
        logger.warn(f"LuaTools: Failed to parse appmanifest: {exc}")
        return {"success": False, "error": "Failed to parse appmanifest"}

    app_state = manifest_data.get("AppState", {})
    install_dir = app_state.get("installdir", "")
    if not install_dir:
        logger.warn(f"LuaTools: installdir not found in appmanifest for {appid}")
        return {"success": False, "error": "Install directory not found"}

    full_install_path = os.path.join(library_path, "steamapps", "common", install_dir)
    if not os.path.exists(full_install_path):
        logger.warn(f"LuaTools: Game install path does not exist: {full_install_path}")
        return {"success": False, "error": "Game directory not found"}

    logger.log(f"LuaTools: Game install path for {appid}: {full_install_path}")
    return {
        "success": True,
        "installPath": full_install_path,
        "installDir": install_dir,
        "libraryPath": library_path,
        "path": full_install_path,
    }


def open_game_folder(path: str) -> bool:
    """Open the game folder using the platform default file explorer."""
    try:
        if not path or not os.path.exists(path):
            return False
        if sys.platform.startswith("win"):
            subprocess.Popen(["explorer", os.path.normpath(path)])
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
        return True
    except Exception as exc:
        logger.warn(f"LuaTools: Failed to open game folder: {exc}")
        return False


__all__ = [
    "detect_steam_install_path",
    "get_game_install_path_response",
    "has_lua_for_app",
    "open_game_folder",
    "trigger_steam_library_refresh",
]
