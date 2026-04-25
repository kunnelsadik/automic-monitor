import os
import logging
import requests
import subprocess
import sys
from requests.auth import HTTPBasicAuth

BASE_URL = os.getenv("AUTOMIC_BASE_URL")
CLIENT = os.getenv("AUTOMIC_CLIENT", "3000")
USERNAME = os.getenv("AUTOMIC_USERNAME")
PASSWORD = os.getenv("AUTOMIC_PASSWORD")

logger = logging.getLogger(__name__)

class AutomicClient:
    def __init__(self):
        logger.info("Initializing AutomicClient")
        self.session = requests.Session()
        self.session.trust_env = True
        self.session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
        self.session.verify = False
        self.session.headers.update({"Accept": "application/json"})

        proxies = self._get_proxies()
        if proxies:
            logger.info(f"Applying proxy settings: {proxies}")
            self.session.proxies.update(proxies)
        else:
            logger.info("No proxy configuration detected for requests session")

    def _parse_proxy_server(self, proxy_server: str) -> dict[str, str]:
        proxies = {}
        if "=" in proxy_server or ";" in proxy_server:
            for pair in proxy_server.split(";"):
                if not pair:
                    continue
                if "=" in pair:
                    scheme, addr = pair.split("=", 1)
                    proxies[scheme.lower()] = addr
                else:
                    proxies["http"] = proxies["https"] = pair
        else:
            proxies["http"] = proxies["https"] = proxy_server
        return proxies

    def _get_windows_proxy(self) -> dict[str, str]:
        if os.name != "nt":
            return {}

        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                 r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as reg:
                proxy_enable = winreg.QueryValueEx(reg, "ProxyEnable")[0]
                if proxy_enable:
                    proxy_server = winreg.QueryValueEx(reg, "ProxyServer")[0]
                    return self._parse_proxy_server(proxy_server)
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.debug(f"Windows proxy detection failed: {exc}")

        try:
            output = subprocess.check_output(["netsh", "winhttp", "show", "proxy"], text=True, stderr=subprocess.DEVNULL)
            for line in output.splitlines():
                if "Proxy Server" in line:
                    proxy_addr = line.split(":", 1)[1].strip()
                    if proxy_addr and "Direct access" not in proxy_addr:
                        return self._parse_proxy_server(proxy_addr)
        except Exception as exc:
            logger.debug(f"WinHTTP proxy detection failed: {exc}")

        return {}

    def _get_proxies(self) -> dict[str, str]:
        proxies = requests.utils.get_environ_proxies(BASE_URL or "")
        proxies = {k: v for k, v in proxies.items() if v}
        if proxies:
            return proxies

        return self._get_windows_proxy()

    def get_latest_executions(self, object_name: str):
        """
        Returns latest execution(s) for a JOBS or JOBP
        """
        logger.info(f"Fetching latest executions for object: {object_name}")
        try:
            url = f"{BASE_URL}/{CLIENT}/executions"
            params = {"name": object_name}

            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            logger.info(f"Retrieved {len(data)} executions for {object_name}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch executions for {object_name}: {e}")
            raise

    def get_children(self, run_id: int):
        """
        Used ONLY for workflows (JOBP)
        """
        logger.info(f"Fetching children for run_id: {run_id}")
        try:
            url = f"{BASE_URL}/{CLIENT}/executions/{run_id}/children"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            logger.info(f"Retrieved {len(data)} children for run_id {run_id}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch children for run_id {run_id}: {e}")
            raise