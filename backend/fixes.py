"""Game fix lookup, application, and removal logic."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
import zipfile
import urllib.request
import urllib.error
from datetime import datetime
from typing import Any, Dict, Optional, Set

from downloads import fetch_app_name
from http_client import ensure_http_client
from logger import logger
from utils import ensure_temp_download_dir
from steam_utils import get_game_install_path_response

# ── Per-appid state stores (thread-safe via dedicated locks) ─────────────
FIX_DOWNLOAD_STATE: Dict[int, Dict[str, Any]] = {}
FIX_DOWNLOAD_LOCK = threading.Lock()
UNFIX_STATE: Dict[int, Dict[str, Any]] = {}
UNFIX_LOCK = threading.Lock()

# ── HuggingFace fixes index cache ────────────────────────────────────────
# Built once via the HF Tree API with RFC-5988 pagination, cached for 1 h.
# Falls back to per-file HEAD requests only when the tree API fails.
HF_REPO_ID = "RaiSantos/fix"
HF_GENERIC_PATH = "GameBypasses"
HF_ONLINE_PATH = "OnlineFix1"

_fixes_index_lock = threading.Lock()
_fixes_index_cache: Optional[Dict[str, Set[int]]] = None
_fixes_index_fetched_at: float = 0.0
_FIXES_INDEX_TTL = 3600  # seconds — refresh every hour

# Browser-like headers to bypass HuggingFace WAF (HTTP 429/403)
_HF_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0"
    ),
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "DNT": "1",
}

# Browser-like headers for direct file downloads from HuggingFace CDN
_HF_DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Opera GX";v="129"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "DNT": "1",
    "Priority": "u=0, i",
    "Connection": "keep-alive",
}


def _fetch_hf_tree(repo_id: str, path: str) -> Set[int]:
    """Fetch all .zip filenames under `path` in `repo_id` via HF Tree API.

    Uses standard urllib (no custom httpx client) to avoid WAF fingerprinting.
    Handles RFC-5988 Link header pagination automatically.

    Returns a set of integer appids corresponding to the zip files found.
    """
    base_url = f"https://huggingface.co/api/datasets/{repo_id}/tree/main/{path}"
    url: Optional[str] = base_url
    appids: Set[int] = set()

    while url:
        try:
            req = urllib.request.Request(url, headers=_HF_REQUEST_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status != 200:
                    logger.warn(f"LuaTools: HF Tree API returned {response.status} for {path}")
                    break

                data = json.loads(response.read().decode("utf-8"))
                for item in data:
                    if isinstance(item, dict) and item.get("type") == "file":
                        filename = item.get("path", "").split("/")[-1]
                        if filename.endswith(".zip"):
                            id_str = filename[:-4]
                            try:
                                appids.add(int(id_str))
                            except ValueError:
                                pass

                # Handle RFC 5988 "Link: <url>; rel='next'" pagination
                link_header = response.headers.get("link", "")
                url = None
                if link_header:
                    for entry in link_header.split(","):
                        if 'rel="next"' in entry and "<" in entry and ">" in entry:
                            url = entry[entry.find("<") + 1 : entry.find(">")]
                            break

        except urllib.error.HTTPError as exc:
            logger.warn(f"LuaTools: HF Tree HTTP error on '{path}': {exc.code} {exc.reason}")
            break
        except Exception as exc:
            logger.warn(f"LuaTools: HF Tree pagination failed on '{path}': {exc}")
            break

    return appids


def _fetch_fixes_index() -> Optional[Dict[str, Set[int]]]:
    """Return the in-memory HF fixes index, rebuilding it when stale.

    Pattern: double-checked locking — first check without lock (fast path),
    then check again inside lock before writing (correct path).

    Returns None only when both HF tree fetch AND cache are unavailable.
    """
    global _fixes_index_cache, _fixes_index_fetched_at
    now = time.time()

    # Fast path: cache is valid — no lock needed for read
    cache = _fixes_index_cache
    if cache is not None and (now - _fixes_index_fetched_at) < _FIXES_INDEX_TTL:
        return cache

    # Fetch outside the write lock to avoid blocking other threads
    try:
        generic_set = _fetch_hf_tree(HF_REPO_ID, HF_GENERIC_PATH)
        online_set = _fetch_hf_tree(HF_REPO_ID, HF_ONLINE_PATH)
        new_index: Dict[str, Set[int]] = {"generic": generic_set, "online": online_set}

        # Only persist if we got at least something useful
        if generic_set or online_set:
            with _fixes_index_lock:
                _fixes_index_cache = new_index
                _fixes_index_fetched_at = time.time()
            logger.log(
                f"LuaTools: HF fixes index built — "
                f"{len(generic_set)} generic, {len(online_set)} online"
            )
            return new_index

    except Exception as exc:
        logger.warn(f"LuaTools: Failed to build HF fixes index: {exc}")

    # Return stale cache rather than none when possible
    with _fixes_index_lock:
        return _fixes_index_cache


def init_fixes_index() -> None:
    """Pre-warm the HF fixes index at plugin startup (non-blocking call)."""
    try:
        _fetch_fixes_index()
    except Exception as exc:
        logger.warn(f"LuaTools: init_fixes_index failed: {exc}")


# ── Path traversal protection ─────────────────────────────────────────────

def _is_safe_path(base_path: str, target_path: str) -> bool:
    """Return True only when target_path remains inside base_path."""
    abs_base = os.path.abspath(base_path)
    abs_target = os.path.abspath(os.path.join(base_path, target_path))
    return abs_target.startswith(abs_base + os.sep) or abs_target == abs_base


# ── State helpers ─────────────────────────────────────────────────────────

def _set_fix_download_state(appid: int, update: dict) -> None:
    with FIX_DOWNLOAD_LOCK:
        state = FIX_DOWNLOAD_STATE.get(appid) or {}
        state.update(update)
        FIX_DOWNLOAD_STATE[appid] = state


def _get_fix_download_state(appid: int) -> dict:
    with FIX_DOWNLOAD_LOCK:
        return FIX_DOWNLOAD_STATE.get(appid, {}).copy()


def _set_unfix_state(appid: int, update: dict) -> None:
    with UNFIX_LOCK:
        state = UNFIX_STATE.get(appid) or {}
        state.update(update)
        UNFIX_STATE[appid] = state


def _get_unfix_state(appid: int) -> dict:
    with UNFIX_LOCK:
        return UNFIX_STATE.get(appid, {}).copy()


# ── Public: check availability ────────────────────────────────────────────

def check_for_fixes(appid: int) -> str:
    """Check both HuggingFace fix buckets for the given appid.

    Priority:
    1. Fast path — in-memory HF tree index (built once at startup, cached 1 h)
    2. Fallback — live HEAD requests when index is unavailable
    """
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    result: Dict[str, Any] = {
        "success": True,
        "appid": appid,
        "gameName": "",
        "genericFix": {"status": 0, "available": False},
        "onlineFix": {"status": 0, "available": False},
    }

    try:
        result["gameName"] = fetch_app_name(appid) or f"Unknown Game ({appid})"
    except Exception as exc:
        logger.warn(f"LuaTools: Failed to fetch game name for {appid}: {exc}")
        result["gameName"] = f"Unknown Game ({appid})"

    generic_url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{HF_GENERIC_PATH}/{appid}.zip"
    online_url = f"https://huggingface.co/datasets/{HF_REPO_ID}/resolve/main/{HF_ONLINE_PATH}/{appid}.zip"

    index = _fetch_fixes_index()
    if index is not None:
        has_generic = appid in index["generic"]
        has_online = appid in index["online"]

        result["genericFix"]["status"] = 200 if has_generic else 404
        result["genericFix"]["available"] = has_generic
        if has_generic:
            result["genericFix"]["url"] = generic_url

        result["onlineFix"]["status"] = 200 if has_online else 404
        result["onlineFix"]["available"] = has_online
        if has_online:
            result["onlineFix"]["url"] = online_url

        logger.log(
            f"LuaTools: HF fix check (index) for {appid}: "
            f"generic={has_generic}, online={has_online}"
        )
    else:
        # Fallback: individual HEAD requests (slow but reliable)
        logger.warn(f"LuaTools: HF index unavailable, falling back to HEAD for {appid}")
        client = ensure_http_client("LuaTools: CheckForFixes")
        for key, url_check in [("genericFix", generic_url), ("onlineFix", online_url)]:
            try:
                resp = client.head(
                    url_check,
                    follow_redirects=True,
                    timeout=10,
                    headers={"User-Agent": _HF_DOWNLOAD_HEADERS["User-Agent"]},
                )
                result[key]["status"] = resp.status_code
                result[key]["available"] = resp.status_code == 200
                if resp.status_code == 200:
                    result[key]["url"] = url_check
            except Exception as exc:
                logger.warn(f"LuaTools: HEAD fallback error on {url_check}: {exc}")

    return json.dumps(result)


# ── Extraction utilities ───────────────────────────────────────────────────

def _get_extractor_binary() -> tuple:
    """Locate 7-Zip or WinRAR on the system. Returns (type, exe_path)."""
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\7-Zip") as key:
            path = winreg.QueryValueEx(key, "Path")[0]
            exe = os.path.join(path, "7z.exe")
            if os.path.exists(exe):
                return ("7z", exe)
    except Exception:
        pass

    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WinRAR") as key:
            exe = winreg.QueryValueEx(key, "exe64")[0]
            if os.path.exists(exe):
                return ("winrar", exe)
    except Exception:
        pass

    # Common installation paths
    for path in [
        r"C:\Program Files\7-Zip\7z.exe",
        r"C:\Program Files (x86)\7-Zip\7z.exe",
    ]:
        if os.path.exists(path):
            return ("7z", path)

    for path in [
        r"C:\Program Files\WinRAR\WinRAR.exe",
        r"C:\Program Files (x86)\WinRAR\WinRAR.exe",
    ]:
        if os.path.exists(path):
            return ("winrar", path)

    return (None, None)


def _extract_archive_robust(archive_path: str, dest_dir: str, pwd: str, appid: int) -> list:
    """Extract ZIP, RAR, or 7Z archives to dest_dir.

    - ZIP: native Python zipfile (no external dependency)
    - RAR/7Z: requires WinRAR or 7-Zip to be installed
    - Auto-strips single redundant parent folder
    - Returns list of relative paths of extracted files
    """
    with open(archive_path, "rb") as fh:
        sig = fh.read(6)

    is_zip = sig[:4] in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
    is_rar = sig[:6] == b"Rar!\x1a\x07" or sig[:6] == b"Rar!\x1a\x00"
    is_7z = sig[:6] == b"7z\xbc\xaf\x27\x1c"

    if not (is_zip or is_rar or is_7z):
        raise RuntimeError(
            f"Corrupted or unsupported archive format (magic: {sig[:4].hex()})"
        )

    extracted_files: list = []
    extract_temp = os.path.join(
        ensure_temp_download_dir(), f"xtr_{appid}_{int(time.time() * 1000)}"
    )
    os.makedirs(extract_temp, exist_ok=True)

    try:
        if is_zip:
            logger.log("LuaTools: Extracting standard ZIP natively…")
            with zipfile.ZipFile(archive_path, "r") as archive:
                for member in archive.namelist():
                    if member.endswith("/") or ".." in member:
                        continue
                    try:
                        archive.extract(member, extract_temp)
                    except RuntimeError as exc:
                        err_lower = str(exc).lower()
                        if any(k in err_lower for k in ("pwd", "password", "encrypt", "bad password")):
                            archive.extract(member, extract_temp, pwd=pwd.encode("utf-8"))
                        else:
                            raise
        else:
            ext_type, exe_path = _get_extractor_binary()
            if not exe_path:
                raise RuntimeError(
                    "This fix uses RAR/7Z format. "
                    "Please install WinRAR or 7-Zip to extract it."
                )

            logger.log(f"LuaTools: Extracting {ext_type.upper()} archive with native tools…")
            startupinfo = None
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = 0  # Hidden window

            if ext_type == "7z":
                cmd = [exe_path, "x", archive_path, f"-p{pwd}", "-y", f"-o{extract_temp}"]
            else:  # winrar
                cmd = [exe_path, "x", f"-p{pwd}", "-y", archive_path, extract_temp + "\\"]

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                startupinfo=startupinfo,
            )
            if proc.returncode != 0:
                logger.warn(f"LuaTools: {ext_type} extraction stderr: {proc.stderr[:500]}")
                raise RuntimeError(
                    f"Extraction via {ext_type} failed (exit {proc.returncode}). "
                    "Wrong password or damaged archive."
                )

        # Auto-strip single redundant parent folder (e.g., GameName/files → files)
        top_items = os.listdir(extract_temp)
        if len(top_items) == 1 and os.path.isdir(os.path.join(extract_temp, top_items[0])):
            base_src = os.path.join(extract_temp, top_items[0])
            logger.log(f"LuaTools: Stripping redundant parent folder '{top_items[0]}'")
        else:
            base_src = extract_temp

        # Move files to game directory
        for root, _dirs, files in os.walk(base_src):
            for filename in files:
                src = os.path.join(root, filename)
                rel = os.path.relpath(src, base_src)
                dst = os.path.join(dest_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.move(src, dst)
                extracted_files.append(rel.replace("\\", "/"))

    finally:
        try:
            shutil.rmtree(extract_temp, ignore_errors=True)
        except Exception:
            pass

    return extracted_files


# ── Fix download worker ───────────────────────────────────────────────────

def _download_and_extract_fix(
    appid: int,
    download_url: str,
    install_path: str,
    fix_type: str,
    game_name: str = "",
) -> None:
    """Background worker: download archive from URL and extract to install_path.

    Handles:
    - Chunked streaming download with cancel checks
    - HuggingFace LFS pointer detection & automatic CDN redirect
    - Robust extraction (ZIP/RAR/7Z)
    - unsteam.ini appid patching for Online Fix (Unsteam) type
    - Atomic log append for unfix tracking
    """
    dest_zip = ""  # Initialize before try to avoid NameError in except block
    client = ensure_http_client("LuaTools: fix download")

    try:
        dest_root = ensure_temp_download_dir()
        dest_zip = os.path.join(dest_root, f"fix_{appid}.zip")
        _set_fix_download_state(appid, {
            "status": "downloading",
            "bytesRead": 0,
            "totalBytes": 0,
            "error": None,
        })

        logger.log(f"LuaTools: Downloading {fix_type} fix for {appid} from {download_url}")

        with client.stream(
            "GET",
            download_url,
            follow_redirects=True,
            timeout=60,
            headers=_HF_DOWNLOAD_HEADERS,
        ) as resp:
            logger.log(f"LuaTools: Fix download response for {appid}: {resp.status_code}")
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", "0") or "0")
            _set_fix_download_state(appid, {"totalBytes": total})

            with open(dest_zip, "wb") as output:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    if not chunk:
                        continue
                    if _get_fix_download_state(appid).get("status") == "cancelled":
                        logger.log(f"LuaTools: Fix download cancelled mid-stream for {appid}")
                        raise RuntimeError("cancelled")
                    output.write(chunk)
                    read = int(_get_fix_download_state(appid).get("bytesRead", 0)) + len(chunk)
                    _set_fix_download_state(appid, {"bytesRead": read})

        # ── HuggingFace LFS pointer detection ─────────────────────────────
        # HF occasionally serves an LFS pointer (text file ~130 bytes) instead of
        # the actual binary when the resolve URL is accessed without proper headers.
        # Detect by file size + content pattern and recover via the LFS CDN.
        file_size = os.path.getsize(dest_zip)
        if file_size < 1000:
            with open(dest_zip, "rb") as fh:
                header = fh.read(500)
            if b"git-lfs" in header:
                logger.warn(f"LuaTools: Received HF LFS pointer for {appid}. Resolving CDN URL…")
                ptr_text = header.decode("utf-8", errors="ignore")
                oid_match = re.search(r"oid sha256:([a-f0-9]{64})", ptr_text)
                if oid_match:
                    oid = oid_match.group(1)
                    # Direct HuggingFace LFS CDN URL
                    lfs_url = (
                        f"https://cdn-lfs.huggingface.co/repos/"
                        f"{HF_REPO_ID.replace('/', '/')}/objects/{oid[:2]}/{oid[2:4]}/{oid}"
                    )
                    logger.log(f"LuaTools: Retrying via LFS CDN: {lfs_url}")
                    os.remove(dest_zip)
                    return _download_and_extract_fix(
                        appid, lfs_url, install_path, fix_type, game_name
                    )
                else:
                    raise RuntimeError("Received HF LFS pointer but could not parse OID.")

        if file_size < 1024:
            raise RuntimeError(
                f"Downloaded file is too small ({file_size} bytes). "
                "Network or host issue."
            )

        # ── Check for cancellation before CPU-intensive extraction ──────────
        if _get_fix_download_state(appid).get("status") == "cancelled":
            raise RuntimeError("cancelled")

        logger.log(f"LuaTools: Download complete ({file_size:,} bytes), extracting…")
        _set_fix_download_state(appid, {"status": "extracting"})

        try:
            extracted_files = _extract_archive_robust(
                dest_zip, install_path, "online-fix.me", appid
            )
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

        if _get_fix_download_state(appid).get("status") == "cancelled":
            logger.log(f"LuaTools: Fix cancelled after extraction for {appid}")
            raise RuntimeError("cancelled")

        # ── unsteam.ini appid patching ─────────────────────────────────────
        if fix_type.lower() == "online fix (unsteam)":
            ini_path = None
            for rel_path in extracted_files:
                if rel_path.replace("\\", "/").lower().endswith("unsteam.ini"):
                    ini_path = os.path.join(install_path, rel_path.replace("/", os.sep))
                    break

            if ini_path and os.path.exists(ini_path):
                try:
                    with open(ini_path, "r", encoding="utf-8", errors="ignore") as fh:
                        contents = fh.read()
                    updated = contents.replace("<appid>", str(appid))
                    if updated != contents:
                        with open(ini_path, "w", encoding="utf-8") as fh:
                            fh.write(updated)
                        logger.log(f"LuaTools: Patched unsteam.ini with appid={appid}")
                    else:
                        logger.log("LuaTools: unsteam.ini had no <appid> placeholder")
                except Exception as exc:
                    logger.warn(f"LuaTools: Failed to patch unsteam.ini: {exc}")
            elif ini_path:
                logger.warn(f"LuaTools: Expected unsteam.ini at {ini_path} but not found")
            else:
                logger.warn("LuaTools: No unsteam.ini found for Online Fix (Unsteam)")

        # ── Append to fix log (for unfix tracking) ────────────────────────
        log_file_path = os.path.join(install_path, f"luatools-fix-log-{appid}.log")
        try:
            existing_content = ""
            if os.path.exists(log_file_path):
                try:
                    with open(log_file_path, "r", encoding="utf-8") as fh:
                        existing_content = fh.read()
                except Exception:
                    pass

            with open(log_file_path, "w", encoding="utf-8") as fh:
                if existing_content:
                    fh.write(existing_content)
                    if not existing_content.endswith("\n"):
                        fh.write("\n")
                    fh.write("\n---\n\n")

                fh.write("[FIX]\n")
                fh.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                fh.write(f"Game: {game_name or f'Unknown Game ({appid})'}\n")
                fh.write(f"Fix Type: {fix_type}\n")
                fh.write(f"Download URL: {download_url}\n")
                fh.write("Files:\n")
                for fp in extracted_files:
                    fh.write(f"{fp}\n")
                fh.write("[/FIX]\n")

            logger.log(
                f"LuaTools: Fix log updated — {len(extracted_files)} files at {log_file_path}"
            )
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to write fix log: {exc}")

        logger.log(f"LuaTools: {fix_type} applied successfully to {install_path}")
        _set_fix_download_state(appid, {"status": "done", "success": True})

    except Exception as exc:
        if str(exc) == "cancelled":
            _set_fix_download_state(
                appid, {"status": "cancelled", "success": False, "error": "Cancelled by user"}
            )
        else:
            logger.warn(f"LuaTools: Failed to apply fix for {appid}: {exc}")
            _set_fix_download_state(appid, {"status": "failed", "error": str(exc)})
    finally:
        if dest_zip:
            try:
                if os.path.exists(dest_zip):
                    os.remove(dest_zip)
            except Exception:
                pass


# ── Public: apply / status / cancel ──────────────────────────────────────

def apply_game_fix(
    appid: int,
    download_url: str,
    install_path: str,
    fix_type: str = "",
    game_name: str = "",
) -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    if not download_url or not install_path:
        return json.dumps({"success": False, "error": "Missing download URL or install path"})

    if not os.path.exists(install_path):
        return json.dumps({"success": False, "error": "Install path does not exist"})

    # Millennium IPC sometimes delivers translated strings with Latin-1→UTF-8
    # mojibake (e.g. "CorreÃ§Ã£o" instead of "Correção"). Sanitize here.
    def _decode_ipc(s: str) -> str:
        try:
            return s.encode("latin-1").decode("utf-8")
        except Exception:
            return s

    fix_type = _decode_ipc(fix_type) if fix_type else fix_type
    game_name = _decode_ipc(game_name) if game_name else game_name

    logger.log(f"LuaTools: ApplyGameFix appid={appid}, fixType={fix_type}")


    _set_fix_download_state(appid, {
        "status": "queued",
        "bytesRead": 0,
        "totalBytes": 0,
        "error": None,
    })
    threading.Thread(
        target=_download_and_extract_fix,
        args=(appid, download_url, install_path, fix_type, game_name),
        daemon=True,
    ).start()

    return json.dumps({"success": True})


def get_apply_fix_status(appid: int) -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    state = _get_fix_download_state(appid)
    return json.dumps({"success": True, "state": state})


def cancel_apply_fix(appid: int) -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    state = _get_fix_download_state(appid)
    if not state or state.get("status") in {"done", "failed", "cancelled"}:
        return json.dumps({"success": True, "message": "Nothing to cancel"})

    _set_fix_download_state(appid, {
        "status": "cancelled",
        "success": False,
        "error": "Cancelled by user",
    })
    logger.log(f"LuaTools: CancelApplyFix requested for appid={appid}")
    return json.dumps({"success": True})


# ── Public: unfix ─────────────────────────────────────────────────────────

def _unfix_game_worker(appid: int, install_path: str, fix_date: Optional[str] = None) -> None:
    """Background worker that reverses a previously applied fix."""
    try:
        logger.log(f"LuaTools: Starting un-fix for appid={appid}, fix_date={fix_date}")
        log_file_path = os.path.join(install_path, f"luatools-fix-log-{appid}.log")

        if not os.path.exists(log_file_path):
            _set_unfix_state(appid, {"status": "failed", "error": "No fix log found."})
            return

        _set_unfix_state(appid, {"status": "removing", "progress": "Reading log file…"})

        files_to_delete: Set[str] = set()
        remaining_fixes: list = []

        try:
            with open(log_file_path, "r", encoding="utf-8") as fh:
                log_content = fh.read()

            if "[FIX]" in log_content:
                for block in log_content.split("[FIX]"):
                    if not block.strip():
                        continue

                    block_date: Optional[str] = None
                    block_lines: list = []
                    in_files = False

                    for line in block.split("\n"):
                        stripped = line.strip()
                        if stripped in ("[/FIX]", "---"):
                            break
                        if stripped.startswith("Date:"):
                            block_date = stripped.replace("Date:", "").strip()
                        block_lines.append(line)

                        if stripped == "Files:":
                            in_files = True
                        elif in_files and stripped:
                            if fix_date is None or block_date == fix_date:
                                files_to_delete.add(stripped)

                    if fix_date is not None and block_date and block_date != fix_date:
                        remaining_fixes.append("[FIX]\n" + "\n".join(block_lines) + "\n[/FIX]")
            else:
                # Legacy format (no markers)
                in_files = False
                for line in log_content.split("\n"):
                    line = line.strip()
                    if line == "Files:":
                        in_files = True
                    elif in_files and line:
                        files_to_delete.add(line)

            logger.log(f"LuaTools: {len(files_to_delete)} unique files to remove")
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to read log file: {exc}")
            _set_unfix_state(appid, {"status": "failed", "error": f"Log read error: {exc}"})
            return

        _set_unfix_state(appid, {
            "status": "removing",
            "progress": f"Removing {len(files_to_delete)} files…",
        })
        deleted = 0
        for rel_path in files_to_delete:
            full = os.path.join(install_path, rel_path)
            try:
                if os.path.exists(full):
                    os.remove(full)
                    deleted += 1
                    logger.log(f"LuaTools: Deleted {rel_path}")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to delete {rel_path}: {exc}")

        logger.log(f"LuaTools: Deleted {deleted}/{len(files_to_delete)} files")

        if remaining_fixes:
            try:
                with open(log_file_path, "w", encoding="utf-8") as fh:
                    fh.write("\n\n---\n\n".join(remaining_fixes))
                logger.log(f"LuaTools: Log updated — {len(remaining_fixes)} fix(es) remaining")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to update log file: {exc}")
        else:
            try:
                os.remove(log_file_path)
                logger.log(f"LuaTools: Deleted log file {log_file_path}")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to delete log file: {exc}")

        _set_unfix_state(appid, {"status": "done", "success": True, "filesRemoved": deleted})

    except Exception as exc:
        logger.warn(f"LuaTools: Un-fix failed: {exc}")
        _set_unfix_state(appid, {"status": "failed", "error": str(exc)})


def unfix_game(appid: int, install_path: str = "", fix_date: str = "") -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    resolved_path = install_path
    if not resolved_path:
        try:
            res = get_game_install_path_response(appid)
            if not res.get("success") or not res.get("installPath"):
                return json.dumps({"success": False, "error": "Could not find game install path"})
            resolved_path = res["installPath"]
        except Exception as exc:
            return json.dumps({"success": False, "error": f"Failed to get install path: {exc}"})

    if not os.path.exists(resolved_path):
        return json.dumps({"success": False, "error": "Install path does not exist"})

    logger.log(f"LuaTools: UnFixGame appid={appid}, path={resolved_path}, fix_date={fix_date}")

    _set_unfix_state(appid, {"status": "queued", "progress": "", "error": None})
    threading.Thread(
        target=_unfix_game_worker,
        args=(appid, resolved_path, fix_date or None),
        daemon=True,
    ).start()

    return json.dumps({"success": True})


def get_unfix_status(appid: int) -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    return json.dumps({"success": True, "state": _get_unfix_state(appid)})


# ── Public: list installed fixes ──────────────────────────────────────────

def get_installed_fixes() -> str:
    """Scan all Steam library paths for games that have a luatools fix log."""
    try:
        from steam_utils import _find_steam_path, _parse_vdf_simple

        steam_path = _find_steam_path()
        if not steam_path:
            return json.dumps({"success": False, "error": "Could not find Steam installation path"})

        library_vdf = os.path.join(steam_path, "config", "libraryfolders.vdf")
        if not os.path.exists(library_vdf):
            return json.dumps({"success": False, "error": "libraryfolders.vdf not found"})

        try:
            with open(library_vdf, "r", encoding="utf-8") as fh:
                vdf_content = fh.read()
            library_data = _parse_vdf_simple(vdf_content)
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to parse libraryfolders.vdf: {exc}")
            return json.dumps({"success": False, "error": "Failed to parse libraryfolders.vdf"})

        library_paths: list = []
        for folder_data in library_data.get("libraryfolders", {}).values():
            if isinstance(folder_data, dict):
                path = folder_data.get("path", "").replace("\\\\", "\\")
                if path:
                    library_paths.append(path)

        installed_fixes: list = []

        for lib_path in library_paths:
            steamapps = os.path.join(lib_path, "steamapps")
            if not os.path.exists(steamapps):
                continue

            try:
                for filename in os.listdir(steamapps):
                    if not filename.startswith("appmanifest_") or not filename.endswith(".acf"):
                        continue

                    try:
                        fappid = int(filename.replace("appmanifest_", "").replace(".acf", ""))
                    except Exception:
                        continue

                    try:
                        with open(os.path.join(steamapps, filename), "r", encoding="utf-8") as fh:
                            manifest_data = _parse_vdf_simple(fh.read())
                        app_state = manifest_data.get("AppState", {})
                        install_dir = app_state.get("installdir", "")
                        game_name = app_state.get("name", f"Unknown Game ({fappid})")

                        if not install_dir:
                            continue

                        full_path = os.path.join(steamapps, "common", install_dir)
                        if not os.path.exists(full_path):
                            continue

                        log_path = os.path.join(full_path, f"luatools-fix-log-{fappid}.log")
                        if not os.path.exists(log_path):
                            continue

                        try:
                            with open(log_path, "r", encoding="utf-8") as fh:
                                log_content = fh.read()

                            fixes_in_log: list = []

                            if "[FIX]" in log_content:
                                for block in log_content.split("[FIX]"):
                                    if not block.strip():
                                        continue
                                    fix_data: Dict[str, Any] = {
                                        "appid": fappid,
                                        "gameName": game_name,
                                        "installPath": full_path,
                                        "date": "",
                                        "fixType": "",
                                        "downloadUrl": "",
                                        "filesCount": 0,
                                        "files": [],
                                    }
                                    in_files = False
                                    for line in block.split("\n"):
                                        line = line.strip()
                                        if line in ("[/FIX]", "---"):
                                            break
                                        if line.startswith("Date:"):
                                            fix_data["date"] = line.replace("Date:", "").strip()
                                        elif line.startswith("Game:"):
                                            val = line.replace("Game:", "").strip()
                                            if val and val != f"Unknown Game ({fappid})":
                                                fix_data["gameName"] = val
                                        elif line.startswith("Fix Type:"):
                                            fix_data["fixType"] = line.replace("Fix Type:", "").strip()
                                        elif line.startswith("Download URL:"):
                                            fix_data["downloadUrl"] = line.replace("Download URL:", "").strip()
                                        elif line == "Files:":
                                            in_files = True
                                        elif in_files and line:
                                            fix_data["files"].append(line)
                                    fix_data["filesCount"] = len(fix_data["files"])
                                    if fix_data["date"]:
                                        fixes_in_log.append(fix_data)
                            else:
                                # Legacy single-fix format
                                fix_data = {
                                    "appid": fappid,
                                    "gameName": game_name,
                                    "installPath": full_path,
                                    "date": "",
                                    "fixType": "",
                                    "downloadUrl": "",
                                    "filesCount": 0,
                                    "files": [],
                                }
                                in_files = False
                                for line in log_content.split("\n"):
                                    line = line.strip()
                                    if line.startswith("Date:"):
                                        fix_data["date"] = line.replace("Date:", "").strip()
                                    elif line.startswith("Game:"):
                                        val = line.replace("Game:", "").strip()
                                        if val and val != f"Unknown Game ({fappid})":
                                            fix_data["gameName"] = val
                                    elif line.startswith("Fix Type:"):
                                        fix_data["fixType"] = line.replace("Fix Type:", "").strip()
                                    elif line.startswith("Download URL:"):
                                        fix_data["downloadUrl"] = line.replace("Download URL:", "").strip()
                                    elif line == "Files:":
                                        in_files = True
                                    elif in_files and line:
                                        fix_data["files"].append(line)
                                fix_data["filesCount"] = len(fix_data["files"])
                                if fix_data["date"]:
                                    fixes_in_log.append(fix_data)

                            installed_fixes.extend(fixes_in_log)

                        except Exception as exc:
                            logger.warn(f"LuaTools: Failed to parse fix log for {fappid}: {exc}")

                    except Exception as exc:
                        logger.warn(f"LuaTools: Failed to process manifest {filename}: {exc}")

            except Exception as exc:
                logger.warn(f"LuaTools: Failed to scan library {lib_path}: {exc}")

        return json.dumps({"success": True, "fixes": installed_fixes})

    except Exception as exc:
        logger.warn(f"LuaTools: get_installed_fixes failed: {exc}")
        return json.dumps({"success": False, "error": str(exc)})


__all__ = [
    "apply_game_fix",
    "cancel_apply_fix",
    "check_for_fixes",
    "get_apply_fix_status",
    "get_installed_fixes",
    "get_unfix_status",
    "init_fixes_index",
    "unfix_game",
]
