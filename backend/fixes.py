"""Game fix lookup, application, and removal logic."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from typing import Any, Dict, Optional, Set

from downloads import fetch_app_name
from http_client import ensure_http_client
from logger import logger
from utils import ensure_temp_download_dir
from steam_utils import get_game_install_path_response

FIX_DOWNLOAD_STATE: Dict[int, Dict[str, Any]] = {}
FIX_DOWNLOAD_LOCK = threading.Lock()
UNFIX_STATE: Dict[int, Dict[str, Any]] = {}
UNFIX_LOCK = threading.Lock()

# ── Fixes index cache (fetched once at startup, cached for session) ──────────
# Uses the centralized luatools index endpoint for instant availability checks,
# then resolves actual download URLs to HuggingFace CDN.
FIXES_INDEX_URL = "https://index.luatools.work/fixes-index.json"
_fixes_index_lock = threading.Lock()
_fixes_index_cache: Optional[Dict] = None


def _fetch_hf_tree(repo_id: str, path: str) -> set:
    """Build the set of available appids by walking the HuggingFace dataset tree.

    Uses urllib (stdlib) instead of httpx to avoid sharing the plugin's HTTP
    client state and to use a dedicated User-Agent that bypasses HF's WAF
    without triggering the 429 rate-limiter that hits scraper-like clients.

    Handles RFC 5988 Link-header pagination so datasets with >1000 files are
    fully enumerated.
    """
    base_url = f"https://huggingface.co/api/datasets/{repo_id}/tree/main/{path}"
    url: Optional[str] = base_url
    appids: set = set()

    # Disguised Windows browser UA — required to avoid HF Cloudflare WAF 403/429
    req_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0"
        ),
        "Accept": "application/json",
    }

    while url:
        try:
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status != 200:
                    logger.warn(f"LuaTools: HF tree walk got status {response.status} on {path}")
                    break

                data = json.loads(response.read().decode("utf-8"))
                for item in data:
                    if isinstance(item, dict) and item.get("type") == "file":
                        filename = item.get("path", "").split("/")[-1]
                        if filename.endswith(".zip"):
                            try:
                                appids.add(int(filename[:-4]))
                            except ValueError:
                                pass

                # Follow RFC 5988 Link: <url>; rel="next" pagination
                link_header = response.headers.get("link", "")
                url = None
                if link_header:
                    for link_entry in link_header.split(","):
                        if 'rel="next"' in link_entry and "<" in link_entry and ">" in link_entry:
                            url = link_entry[link_entry.find("<") + 1 : link_entry.find(">")]
                            break
        except urllib.error.HTTPError as exc:
            logger.warn(f"LuaTools: HF tree walk HTTP {exc.code} on {path}: {exc.reason}")
            break
        except Exception as exc:
            logger.warn(f"LuaTools: HF tree walk failed on {path}: {exc}")
            break

    return appids


def _fetch_fixes_index() -> Optional[Dict]:
    """Fetch and cache the fixes availability index.

    Three-tier strategy — most reliable to most expensive:
      1. Centralized JSON index (index.luatools.work) — single HTTP GET, O(1) parse.
      2. HuggingFace dataset tree walk — enumerates all ZIPs on the HF repo;
         used when the centralized index is unavailable (server down / cold cache).
      3. None — callers fall back to per-appid HEAD requests.

    Result is cached for the entire Steam session; a plugin reload always refetches.
    """
    global _fixes_index_cache

    with _fixes_index_lock:
        if _fixes_index_cache is not None:
            return _fixes_index_cache

    # ── Tier 1: Centralized index (fastest, preferred) ───────────────────────
    try:
        client = ensure_http_client("LuaTools: FixesIndex")
        resp = client.get(FIXES_INDEX_URL, follow_redirects=True, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            generic_set = set(data.get("genericFixes", []))
            online_set  = set(data.get("onlineFixes",  []))
            index = {"generic": generic_set, "online": online_set}
            with _fixes_index_lock:
                _fixes_index_cache = index
            logger.log(
                f"LuaTools: [Tier-1] Fixes index loaded — "
                f"{len(generic_set)} generic, {len(online_set)} online"
            )
            return index
        else:
            logger.warn(
                f"LuaTools: [Tier-1] Centralized index returned {resp.status_code}, "
                "falling back to HF tree walk..."
            )
    except Exception as exc:
        logger.warn(f"LuaTools: [Tier-1] Centralized index unreachable: {exc}, falling back to HF tree walk...")

    # ── Tier 2: HuggingFace dataset tree walk (authoritative fallback) ────────
    # This is slower (multiple paginated API calls) but is the ground truth —
    # it reads directly from the source repo and is immune to index server outages.
    try:
        logger.log("LuaTools: [Tier-2] Walking HuggingFace dataset tree for RaiSantos/fix...")
        generic_set = _fetch_hf_tree("RaiSantos/fix", "GameBypasses")
        online_set  = _fetch_hf_tree("RaiSantos/fix", "OnlineFix1")

        if generic_set or online_set:
            index = {"generic": generic_set, "online": online_set}
            with _fixes_index_lock:
                _fixes_index_cache = index
            logger.log(
                f"LuaTools: [Tier-2] HF index built — "
                f"{len(generic_set)} generic, {len(online_set)} online"
            )
            return index
        else:
            logger.warn("LuaTools: [Tier-2] HF tree walk returned empty sets")
    except Exception as exc:
        logger.warn(f"LuaTools: [Tier-2] HF tree walk failed: {exc}")

    # ── Tier 3: None — check_for_fixes will do per-appid HEAD requests ────────
    logger.warn("LuaTools: [Tier-3] Both index sources failed — falling back to per-appid HEAD checks")
    return None


def init_fixes_index() -> None:
    """Pre-fetch the fixes index at startup in a background thread.

    Called once during Plugin._load(). Runs asynchronously to avoid blocking
    the Steam UI while the index is being fetched (especially on Tier-2 path).
    """
    def _worker():
        try:
            _fetch_fixes_index()
        except Exception as exc:
            logger.warn(f"LuaTools: init_fixes_index background worker failed: {exc}")

    threading.Thread(target=_worker, daemon=True, name="LuaTools-FixesIndex").start()


def _is_safe_path(base_path: str, target_path: str) -> bool:
    """Return True only if target_path resolves within base_path (prevents path traversal)."""
    abs_base = os.path.abspath(base_path)
    abs_target = os.path.abspath(os.path.join(base_path, target_path))
    return abs_target.startswith(abs_base + os.sep) or abs_target == abs_base


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


def check_for_fixes(appid: int) -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    result = {
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

    # Primary path: use cached index for O(1) availability check
    index = _fetch_fixes_index()
    if index is not None:
        has_generic = appid in index["generic"]
        has_online = appid in index["online"]

        result["genericFix"]["status"] = 200 if has_generic else 404
        result["genericFix"]["available"] = has_generic
        if has_generic:
            result["genericFix"]["url"] = (
                f"https://huggingface.co/datasets/RaiSantos/fix/resolve/main/GameBypasses/{appid}.zip"
            )

        result["onlineFix"]["status"] = 200 if has_online else 404
        result["onlineFix"]["available"] = has_online
        if has_online:
            result["onlineFix"]["url"] = (
                f"https://huggingface.co/datasets/RaiSantos/fix/resolve/main/OnlineFix1/{appid}.zip"
            )

        logger.log(
            f"LuaTools: Fix check for {appid} via index: "
            f"generic={has_generic}, online={has_online}"
        )
    else:
        # Fallback: synchronous HEAD requests when index is unavailable
        logger.warn(f"LuaTools: Fixes index unavailable, falling back to HEAD requests for {appid}")
        client = ensure_http_client("LuaTools: CheckForFixes")
        paths = [
            ("genericFix", f"https://huggingface.co/datasets/RaiSantos/fix/resolve/main/GameBypasses/{appid}.zip"),
            ("onlineFix",  f"https://huggingface.co/datasets/RaiSantos/fix/resolve/main/OnlineFix1/{appid}.zip"),
        ]
        for key, url_check in paths:
            try:
                resp = client.head(url_check, follow_redirects=True, timeout=10)
                result[key]["status"] = resp.status_code
                result[key]["available"] = resp.status_code == 200
                if resp.status_code == 200:
                    result[key]["url"] = url_check
            except Exception as exc:
                logger.warn(f"LuaTools: HF HEAD check failed for {url_check}: {exc}")

    return json.dumps(result)


# ── Archive extraction engine ─────────────────────────────────────────────────

def _get_extractor_binary() -> tuple:
    """Locate 7-Zip or WinRAR on the system via registry or common paths.

    Returns (type_str, exe_path) or (None, None) if neither is found.
    Required for RAR and 7z archives that Python's zipfile cannot handle.
    """
    try:
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\7-Zip") as key:
                path = winreg.QueryValueEx(key, "Path")[0]
                exe = os.path.join(path, "7z.exe")
                if os.path.exists(exe):
                    return ("7z", exe)
        except Exception:
            pass

        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WinRAR") as key:
                exe = winreg.QueryValueEx(key, "exe64")[0]
                if os.path.exists(exe):
                    return ("winrar", exe)
        except Exception:
            pass
    except ImportError:
        pass  # Non-Windows platform

    # Common installation paths as last resort
    if os.path.exists(r"C:\Program Files\7-Zip\7z.exe"):
        return ("7z", r"C:\Program Files\7-Zip\7z.exe")
    if os.path.exists(r"C:\Program Files\WinRAR\WinRAR.exe"):
        return ("winrar", r"C:\Program Files\WinRAR\WinRAR.exe")

    return (None, None)


def _extract_archive_robust(archive_path: str, dest_dir: str, pwd: str, appid: int) -> list:
    """Extract ZIP, RAR, or 7z archive to dest_dir.

    Detects format via magic bytes — never trusts the file extension.
    For ZIP: uses Python's zipfile with password fallback.
    For RAR/7z: delegates to native binary (7-Zip or WinRAR) in stealth mode.
    Strips single redundant top-level folder if present (common in HF ZIPs).

    Returns list of extracted file relative paths (forward-slash separated).
    """
    with open(archive_path, "rb") as fh:
        sig = fh.read(6)

    is_zip = sig.startswith(b"PK\x03\x04")
    is_rar = sig.startswith(b"Rar!\x1a\x07") or sig.startswith(b"Rar!\x1a\x00")
    is_7z  = sig.startswith(b"7z\xbc\xaf'\x1c")

    if not (is_zip or is_rar or is_7z):
        raise RuntimeError(f"Corrupted or unknown file format (magic={sig[:4].hex()})")

    extracted_files: list = []
    extract_temp = os.path.join(ensure_temp_download_dir(), f"xtr_{appid}_{int(time.time())}")
    os.makedirs(extract_temp, exist_ok=True)

    try:
        if is_zip:
            logger.log("LuaTools: Extracting ZIP natively...")
            with zipfile.ZipFile(archive_path, "r") as archive:
                for member in archive.namelist():
                    if member.endswith("/"):
                        continue
                    if ".." in member:
                        # Path traversal guard
                        logger.warn(f"LuaTools: Skipping unsafe zip member: {member}")
                        continue
                    try:
                        archive.extract(member, extract_temp)
                    except RuntimeError as exc:
                        err = str(exc).lower()
                        if any(k in err for k in ("pwd", "password", "encrypted", "bad password")):
                            archive.extract(member, extract_temp, pwd=pwd.encode("utf-8"))
                        else:
                            raise
        else:
            ext_type, exe_path = _get_extractor_binary()
            if not exe_path:
                raise RuntimeError(
                    "This fix is packed as RAR/7Z. Install WinRAR or 7-Zip to extract it."
                )
            logger.log(f"LuaTools: Extracting {ext_type.upper()} archive using native binary...")
            startupinfo = None
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = 0  # Hidden console window

            if ext_type == "7z":
                cmd = [exe_path, "x", archive_path, f"-p{pwd}", "-y", f"-o{extract_temp}"]
            else:
                cmd = [exe_path, "x", f"-p{pwd}", "-y", archive_path, extract_temp + "\\"]

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                startupinfo=startupinfo,
            )
            if proc.returncode != 0:
                logger.warn(f"LuaTools: Native extraction error: {proc.stderr}")
                raise RuntimeError(f"Extraction via {ext_type} failed — invalid password or damaged archive.")

        # Strip single redundant parent folder (common HF packaging pattern)
        items = os.listdir(extract_temp)
        if len(items) == 1 and os.path.isdir(os.path.join(extract_temp, items[0])):
            base_src = os.path.join(extract_temp, items[0])
            logger.log(f"LuaTools: Bypassing redundant root folder '{items[0]}'")
        else:
            base_src = extract_temp

        for root, _dirs, files in os.walk(base_src):
            for file in files:
                src = os.path.join(root, file)
                rel = os.path.relpath(src, base_src)
                dst = os.path.join(dest_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.move(src, dst)
                extracted_files.append(rel.replace("\\", "/"))

    finally:
        try:
            shutil.rmtree(extract_temp)
        except Exception:
            pass

    return extracted_files


# ── Download and install engine ───────────────────────────────────────────────

def _download_and_extract_fix(
    appid: int,
    download_url: str,
    install_path: str,
    fix_type: str,
    game_name: str = "",
) -> None:
    """Download a game fix archive from the given URL and extract it to install_path.

    Handles HuggingFace WAF bypass via disguised browser headers.
    Detects and transparently resolves Git-LFS pointer responses (common on HF free tier)
    by fetching the real object directly from the HF LFS CDN.
    Supports ZIP, RAR, and 7z archives via _extract_archive_robust.
    """
    client = ensure_http_client("LuaTools: fix download")
    dest_root = ensure_temp_download_dir()
    dest_zip = os.path.join(dest_root, f"fix_{appid}.zip")

    try:
        _set_fix_download_state(appid, {"status": "downloading", "bytesRead": 0, "totalBytes": 0, "error": None})

        # Disguised browser headers bypass HuggingFace's WAF (Cloudflare) and
        # instruct the CDN to serve the real file instead of an HTML gate page.
        hf_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0"
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

        logger.log(f"LuaTools: Downloading {fix_type} fix for {appid} from {download_url}")

        with client.stream("GET", download_url, follow_redirects=True, timeout=30, headers=hf_headers) as resp:
            logger.log(f"LuaTools: Fix download response for {appid}: status={resp.status_code}")
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", "0") or "0")
            _set_fix_download_state(appid, {"totalBytes": total})

            with open(dest_zip, "wb") as output:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    if not chunk:
                        continue
                    state = _get_fix_download_state(appid)
                    if state.get("status") == "cancelled":
                        logger.log(f"LuaTools: Fix download cancelled mid-stream for {appid}")
                        raise RuntimeError("cancelled")
                    output.write(chunk)
                    read = int(state.get("bytesRead", 0)) + len(chunk)
                    _set_fix_download_state(appid, {"bytesRead": read})

        # LFS Pointer Detection:
        # HuggingFace free tier may serve a small Git-LFS pointer text file instead of the
        # actual binary when the client does not negotiate LFS correctly. The pointer is
        # ~150 bytes and contains "git-lfs" and an "oid sha256:<hash>" line.
        # We detect this, extract the OID, and fetch directly from the HF LFS CDN.
        file_size = os.path.getsize(dest_zip)
        if file_size < 1000:
            with open(dest_zip, "rb") as fh:
                header = fh.read(500)
            if b"git-lfs" in header:
                logger.warn(f"LuaTools: HF LFS pointer received for {appid}, resolving real object...")
                ptr_text = header.decode("utf-8", errors="ignore")
                oid_match = re.search(r"oid sha256:([a-f0-9]{64})", ptr_text)
                if oid_match:
                    oid = oid_match.group(1)
                    # Direct HF LFS CDN URL — bypasses the LFS negotiation entirely
                    lfs_url = (
                        f"https://cdn-lfs.huggingface.co/repos/RaiSantos/fix"
                        f"/objects/{oid[:2]}/{oid[2:4]}/{oid}"
                    )
                    logger.log(f"LuaTools: Fetching from LFS CDN: {lfs_url}")
                    try:
                        os.remove(dest_zip)
                    except Exception:
                        pass
                    # Recursive call with resolved CDN URL — at most one level of recursion
                    return _download_and_extract_fix(appid, lfs_url, install_path, fix_type, game_name)
                raise RuntimeError(f"HF LFS pointer received but OID not found ({file_size} bytes)")

        if file_size < 1024:
            raise RuntimeError(
                f"Downloaded file is too small ({file_size} bytes) — network or hoster issue."
            )

        logger.log(f"LuaTools: Download complete ({file_size} bytes), extracting to {install_path}")
        _set_fix_download_state(appid, {"status": "extracting"})

        try:
            extracted_files = _extract_archive_robust(dest_zip, install_path, "online-fix.me", appid)
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

        if _get_fix_download_state(appid).get("status") == "cancelled":
            logger.log(f"LuaTools: Fix cancelled after extraction for {appid}")
            raise RuntimeError("cancelled")

        # Post-extraction: patch unsteam.ini appid placeholder
        ini_relative_path = None
        for rel_path in extracted_files:
            if rel_path.replace("\\", "/").lower().endswith("unsteam.ini"):
                ini_relative_path = rel_path
                break

        if fix_type.lower() == "online fix (unsteam)":
            try:
                if ini_relative_path:
                    ini_full_path = os.path.join(install_path, ini_relative_path.replace("/", os.sep))
                    if os.path.exists(ini_full_path):
                        with open(ini_full_path, "r", encoding="utf-8", errors="ignore") as fh:
                            contents = fh.read()
                        updated = contents.replace("<appid>", str(appid))
                        if updated != contents:
                            with open(ini_full_path, "w", encoding="utf-8") as fh:
                                fh.write(updated)
                            logger.log(f"LuaTools: Patched unsteam.ini with appid={appid}")
                        else:
                            logger.log("LuaTools: unsteam.ini had no <appid> placeholder or was already patched")
                    else:
                        logger.warn(f"LuaTools: Expected unsteam.ini not found at {ini_full_path}")
                else:
                    logger.warn("LuaTools: unsteam.ini not found in extracted files for Online Fix (Unsteam)")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to patch unsteam.ini: {exc}")

        # Write fix log (append-safe multi-fix format)
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
                fh.write(f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                fh.write(f'Game: {game_name or f"Unknown Game ({appid})"}\n')
                fh.write(f"Fix Type: {fix_type}\n")
                fh.write(f"Download URL: {download_url}\n")
                fh.write("Files:\n")
                for fp in extracted_files:
                    fh.write(f"{fp}\n")
                fh.write("[/FIX]\n")

            logger.log(f"LuaTools: Fix log written at {log_file_path} ({len(extracted_files)} files)")
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to write fix log: {exc}")

        logger.log(f"LuaTools: {fix_type} applied successfully to {install_path}")
        _set_fix_download_state(appid, {"status": "done", "success": True})

        try:
            os.remove(dest_zip)
        except Exception:
            pass

    except Exception as exc:
        if str(exc) == "cancelled":
            try:
                if os.path.exists(dest_zip):
                    os.remove(dest_zip)
            except Exception:
                pass
            _set_fix_download_state(appid, {"status": "cancelled", "success": False, "error": "Cancelled by user"})
            return
        logger.warn(f"LuaTools: Failed to apply fix for {appid}: {exc}")
        _set_fix_download_state(appid, {"status": "failed", "error": str(exc)})


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

    logger.log(f"LuaTools: ApplyGameFix appid={appid}, fixType={fix_type}")

    _set_fix_download_state(appid, {"status": "queued", "bytesRead": 0, "totalBytes": 0, "error": None})
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
    if not state or state.get("status") in {"done", "failed"}:
        return json.dumps({"success": True, "message": "Nothing to cancel"})

    _set_fix_download_state(appid, {"status": "cancelled", "success": False, "error": "Cancelled by user"})
    logger.log(f"LuaTools: CancelApplyFix requested for appid={appid}")
    return json.dumps({"success": True})


def _unfix_game_worker(appid: int, install_path: str, fix_date: Optional[str] = None) -> None:
    try:
        logger.log(f"LuaTools: Starting un-fix for appid={appid}, fix_date={fix_date}")
        log_file_path = os.path.join(install_path, f"luatools-fix-log-{appid}.log")

        if not os.path.exists(log_file_path):
            _set_unfix_state(appid, {"status": "failed", "error": "No fix log found. Cannot un-fix."})
            return

        _set_unfix_state(appid, {"status": "removing", "progress": "Reading log file..."})

        files_to_delete: Set[str] = set()
        remaining_fixes: list = []

        try:
            with open(log_file_path, "r", encoding="utf-8") as fh:
                log_content = fh.read()

            if "[FIX]" in log_content:
                for block in log_content.split("[FIX]"):
                    if not block.strip():
                        continue
                    lines = block.split("\n")
                    in_files = False
                    block_date = None
                    block_lines: list = []

                    for line in lines:
                        stripped = line.strip()
                        if stripped in ("[/FIX]", "---"):
                            break
                        if stripped.startswith("Date:"):
                            block_date = stripped.replace("Date:", "").strip()
                        block_lines.append(line)
                        if stripped == "Files:":
                            in_files = True
                        elif in_files and stripped:
                            if fix_date is None or (block_date and block_date == fix_date):
                                files_to_delete.add(stripped)

                    if fix_date is not None and block_date and block_date != fix_date:
                        remaining_fixes.append("[FIX]\n" + "\n".join(block_lines) + "\n[/FIX]")
            else:
                # Legacy single-fix format (no [FIX] markers)
                in_files = False
                for line in log_content.split("\n"):
                    line = line.strip()
                    if line == "Files:":
                        in_files = True
                    elif in_files and line:
                        files_to_delete.add(line)

            logger.log(f"LuaTools: {len(files_to_delete)} unique files queued for removal")
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to parse fix log: {exc}")
            _set_unfix_state(appid, {"status": "failed", "error": f"Failed to read log: {exc}"})
            return

        _set_unfix_state(appid, {"status": "removing", "progress": f"Removing {len(files_to_delete)} files..."})
        deleted = 0
        for fp in files_to_delete:
            try:
                full = os.path.join(install_path, fp)
                if os.path.exists(full):
                    os.remove(full)
                    deleted += 1
                    logger.log(f"LuaTools: Deleted {fp}")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to delete {fp}: {exc}")

        logger.log(f"LuaTools: Deleted {deleted}/{len(files_to_delete)} files")

        if remaining_fixes:
            try:
                with open(log_file_path, "w", encoding="utf-8") as fh:
                    fh.write("\n\n---\n\n".join(remaining_fixes))
                logger.log(f"LuaTools: Log updated, {len(remaining_fixes)} fixes remaining")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to update fix log: {exc}")
        else:
            try:
                os.remove(log_file_path)
                logger.log(f"LuaTools: Deleted fix log {log_file_path}")
            except Exception as exc:
                logger.warn(f"LuaTools: Failed to delete fix log: {exc}")

        _set_unfix_state(appid, {"status": "done", "success": True, "filesRemoved": deleted})

    except Exception as exc:
        logger.warn(f"LuaTools: Un-fix failed for {appid}: {exc}")
        _set_unfix_state(appid, {"status": "failed", "error": str(exc)})


def unfix_game(appid: int, install_path: str = "", fix_date: str = "") -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({"success": False, "error": "Invalid appid"})

    resolved_path = install_path
    if not resolved_path:
        try:
            result = get_game_install_path_response(appid)
            if not result.get("success") or not result.get("installPath"):
                return json.dumps({"success": False, "error": "Could not find game install path"})
            resolved_path = result["installPath"]
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
    state = _get_unfix_state(appid)
    return json.dumps({"success": True, "state": state})


def get_installed_fixes() -> str:
    """Scan all Steam library folders for games with luatools fix logs."""
    try:
        from steam_utils import _find_steam_path, _parse_vdf_simple

        steam_path = _find_steam_path()
        if not steam_path:
            return json.dumps({"success": False, "error": "Could not find Steam installation path"})

        library_vdf_path = os.path.join(steam_path, "config", "libraryfolders.vdf")
        if not os.path.exists(library_vdf_path):
            return json.dumps({"success": False, "error": "Could not find libraryfolders.vdf"})

        try:
            with open(library_vdf_path, "r", encoding="utf-8") as fh:
                vdf_content = fh.read()
            library_data = _parse_vdf_simple(vdf_content)
        except Exception as exc:
            logger.warn(f"LuaTools: Failed to parse libraryfolders.vdf: {exc}")
            return json.dumps({"success": False, "error": "Failed to parse libraryfolders.vdf"})

        all_library_paths = []
        for folder_data in library_data.get("libraryfolders", {}).values():
            if isinstance(folder_data, dict):
                fp = folder_data.get("path", "")
                if fp:
                    all_library_paths.append(fp.replace("\\\\", "\\"))

        installed_fixes = []

        for lib_path in all_library_paths:
            steamapps_path = os.path.join(lib_path, "steamapps")
            if not os.path.exists(steamapps_path):
                continue
            try:
                for filename in os.listdir(steamapps_path):
                    if not filename.startswith("appmanifest_") or not filename.endswith(".acf"):
                        continue
                    try:
                        appid = int(filename.replace("appmanifest_", "").replace(".acf", ""))
                    except Exception:
                        continue

                    manifest_path = os.path.join(steamapps_path, filename)
                    try:
                        with open(manifest_path, "r", encoding="utf-8") as fh:
                            manifest_content = fh.read()
                        manifest_data = _parse_vdf_simple(manifest_content)
                        app_state = manifest_data.get("AppState", {})
                        install_dir = app_state.get("installdir", "")
                        game_name = app_state.get("name", f"Unknown Game ({appid})")

                        if not install_dir:
                            continue
                        full_install_path = os.path.join(lib_path, "steamapps", "common", install_dir)
                        if not os.path.exists(full_install_path):
                            continue

                        log_file_path = os.path.join(full_install_path, f"luatools-fix-log-{appid}.log")
                        if not os.path.exists(log_file_path):
                            continue

                        try:
                            with open(log_file_path, "r", encoding="utf-8") as fh:
                                log_content = fh.read()

                            fixes_in_log = []

                            if "[FIX]" in log_content:
                                for block in log_content.split("[FIX]"):
                                    if not block.strip():
                                        continue
                                    fix_data: Dict[str, Any] = {
                                        "appid": appid,
                                        "gameName": game_name,
                                        "installPath": full_install_path,
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
                                            n = line.replace("Game:", "").strip()
                                            if n and n != f"Unknown Game ({appid})":
                                                fix_data["gameName"] = n
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
                                # Legacy format
                                fix_data = {
                                    "appid": appid,
                                    "gameName": game_name,
                                    "installPath": full_install_path,
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
                                        n = line.replace("Game:", "").strip()
                                        if n and n != f"Unknown Game ({appid})":
                                            fix_data["gameName"] = n
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
                            logger.warn(f"LuaTools: Failed to parse fix log for {appid}: {exc}")

                    except Exception as exc:
                        logger.warn(f"LuaTools: Failed to process manifest {filename}: {exc}")
                        continue

            except Exception as exc:
                logger.warn(f"LuaTools: Failed to scan library {lib_path}: {exc}")
                continue

        return json.dumps({"success": True, "fixes": installed_fixes})

    except Exception as exc:
        logger.warn(f"LuaTools: Failed to get installed fixes: {exc}")
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
