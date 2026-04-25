"""Automic API HTTP client."""
import logging
import os
import subprocess
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from src.config import AutomicConfig

logger = logging.getLogger(__name__)


class AutomicClient:
    def __init__(self, config: AutomicConfig) -> None:
        logger.info("Initializing AutomicClient")
        self.config = config
        self.session = requests.Session()
        self.session.trust_env = True
        self.session.auth = HTTPBasicAuth(
            config.username, config.password.get_secret_value()
        )
        self.session.verify = config.ssl_verify
        self.session.headers.update({"Accept": "application/json"})

        proxies = self._get_proxies()
        if proxies:
            logger.info(f"Applying proxy settings: {proxies}")
            self.session.proxies.update(proxies)

    def _parse_proxy_server(self, proxy_server: str) -> dict[str, str]:
        proxies: dict[str, str] = {}
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

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Internet Settings",
            ) as reg:
                if winreg.QueryValueEx(reg, "ProxyEnable")[0]:
                    proxy_server = winreg.QueryValueEx(reg, "ProxyServer")[0]
                    return self._parse_proxy_server(proxy_server)
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.debug(f"Windows Registry proxy detection failed: {exc}")

        try:
            output = subprocess.check_output(
                ["netsh", "winhttp", "show", "proxy"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            for line in output.splitlines():
                if "Proxy Server" in line:
                    proxy_addr = line.split(":", 1)[1].strip()
                    if proxy_addr and "Direct access" not in proxy_addr:
                        return self._parse_proxy_server(proxy_addr)
        except Exception as exc:
            logger.debug(f"WinHTTP proxy detection failed: {exc}")

        return {}

    def _get_proxies(self) -> dict[str, str]:
        proxies = {
            k: v
            for k, v in requests.utils.get_environ_proxies(
                self.config.base_url or ""
            ).items()
            if v
        }
        if proxies:
            return proxies
        if self.config.proxy_server:
            return self._parse_proxy_server(self.config.proxy_server)
        return self._get_windows_proxy()

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self.session.request(
                method, url, timeout=self.config.timeout, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP {method} {url} failed: {e}")
            raise

    def get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = f"{self.config.base_url}{endpoint}"
        logger.debug(f"GET {url} params={params}")
        return self._request("GET", url, params=params)

    def post(
        self, endpoint: str, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = f"{self.config.base_url}{endpoint}"
        logger.debug(f"POST {url}")
        return self._request("POST", url, json=json)

    def get_latest_executions(self, object_name: str) -> list:
        logger.info(f"Fetching executions for: {object_name}")
        data = self.get(
            f"/{self.config.client_id}/executions", params={"name": object_name}
        ).get("data", [])
        logger.info(f"Retrieved {len(data)} executions for {object_name}")
        return data

    def get_execution_details(self, run_id: str) -> dict[str, Any]:
        logger.info(f"Fetching execution details for run_id={run_id}")
        return self.get(f"/{self.config.client_id}/executions/{run_id}").get("data", {})

    def get_children(self, run_id: str | int) -> list:
        logger.info(f"Fetching children for run_id={run_id}")
        return self.get(
            f"/{self.config.client_id}/executions/{run_id}/children"
        ).get("data", [])

    def search(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.post(f"/{self.config.client_id}/search", json=payload)

    def get_job_logs(self, run_id: str, report_type: str = "REP") -> str:
        logger.info(f"Fetching logs for run_id={run_id} type={report_type}")
        return self.get(
            f"/{self.config.client_id}/executions/{run_id}/reports/{report_type}"
        ).get("content", "")

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
