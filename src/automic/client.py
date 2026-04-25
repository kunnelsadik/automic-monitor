"""Automic API client for HTTP communication."""
import logging
import subprocess
from typing import Any, Dict, Optional

import requests
from requests.auth import HTTPBasicAuth

from src.config import AutomicConfig

logger = logging.getLogger(__name__)


class AutomicClient:
    """HTTP client for Automic API communication.
    
    Handles authentication, proxy configuration, and HTTP requests to the
    Automic API endpoint.
    
    Attributes:
        config: AutomicConfig instance with API credentials and settings
        session: Persistent requests.Session for connection pooling
    
    Example:
        from src.config import Config
        from src.automic.client import AutomicClient
        
        config = Config()
        client = AutomicClient(config=config.automic)
        executions = client.get_latest_executions("JOBP.WORKFLOW_NAME")
    """

    def __init__(self, config: AutomicConfig):
        """Initialize the Automic API client.
        
        Args:
            config: AutomicConfig instance with credentials and API URL
        """
        logger.info("Initializing AutomicClient")
        self.config = config
        self.session = requests.Session()
        self.session.trust_env = True

        # Set up authentication
        self.session.auth = HTTPBasicAuth(config.username, config.password.get_secret_value())
        self.session.verify = config.ssl_verify
        self.session.headers.update({"Accept": "application/json"})

        # Configure proxies
        proxies = self._get_proxies()
        if proxies:
            logger.info(f"Applying proxy settings: {proxies}")
            self.session.proxies.update(proxies)
        else:
            logger.info("No proxy configuration detected")

    def _parse_proxy_server(self, proxy_server: str) -> Dict[str, str]:
        """Parse proxy server string into protocol dict.
        
        Args:
            proxy_server: Proxy server string (e.g., "http.example.com:8080")
            
        Returns:
            Dict mapping protocol to proxy URL
        """
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

    def _get_windows_proxy(self) -> Dict[str, str]:
        """Get proxy settings from Windows Registry.
        
        Returns:
            Dict of proxy settings or empty dict if not found
        """
        import os

        if os.name != "nt":
            return {}

        # Try Windows Registry
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Internet Settings",
            ) as reg:
                proxy_enable = winreg.QueryValueEx(reg, "ProxyEnable")[0]
                if proxy_enable:
                    proxy_server = winreg.QueryValueEx(reg, "ProxyServer")[0]
                    return self._parse_proxy_server(proxy_server)
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.debug(f"Windows Registry proxy detection failed: {exc}")

        # Try WinHTTP
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

    def _get_proxies(self) -> Dict[str, str]:
        """Get proxy configuration from environment or system.
        
        Returns:
            Dict of proxy settings or empty dict
        """
        # Try environment variables first
        proxies = requests.utils.get_environ_proxies(self.config.base_url or "")
        proxies = {k: v for k, v in proxies.items() if v}
        if proxies:
            return proxies

        # Fall back to custom proxy server if configured
        if self.config.proxy_server:
            return self._parse_proxy_server(self.config.proxy_server)

        # Try Windows system proxy
        return self._get_windows_proxy()

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Make an HTTP request to Automic API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL for the request
            **kwargs: Additional arguments for requests.Session.request()
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.exceptions.RequestException: On HTTP error
        """
        try:
            response = self.session.request(
                method,
                url,
                timeout=self.config.timeout,
                **kwargs,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request.
        
        Args:
            endpoint: API endpoint path (e.g., "/3000/executions")
            params: Query parameters
            
        Returns:
            JSON response
        """
        url = f"{self.config.base_url}{endpoint}"
        logger.debug(f"GET {url} with params {params}")
        return self.request("GET", url, params=params)

    def post(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a POST request.
        
        Args:
            endpoint: API endpoint path
            json: JSON payload
            
        Returns:
            JSON response
        """
        url = f"{self.config.base_url}{endpoint}"
        logger.debug(f"POST {url} with payload {json}")
        return self.request("POST", url, json=json)

    def get_latest_executions(self, object_name: str) -> list:
        """Get latest executions for a workflow or job.
        
        Args:
            object_name: Automic object name (e.g., "JOBS.JOB_NAME", "JOBP.WORKFLOW")
            
        Returns:
            List of execution records
            
        Example:
            executions = client.get_latest_executions("JOBP.DAILY_WORKFLOW")
        """
        logger.info(f"Fetching latest executions for: {object_name}")
        try:
            endpoint = f"/{self.config.client_id}/executions"
            response = self.get(endpoint, params={"name": object_name})
            data = response.get("data", [])
            logger.info(f"Retrieved {len(data)} executions for {object_name}")
            return data
        except Exception as e:
            logger.error(f"Failed to get executions for {object_name}: {e}")
            raise

    def get_execution_details(self, run_id: str) -> Dict[str, Any]:
        """Get details for a specific execution.
        
        Args:
            run_id: Execution run ID
            
        Returns:
            Execution details dictionary
        """
        logger.info(f"Fetching execution details for run_id: {run_id}")
        try:
            endpoint = f"/{self.config.client_id}/executions/{run_id}"
            response = self.get(endpoint)
            return response.get("data", {})
        except Exception as e:
            logger.error(f"Failed to get execution details for {run_id}: {e}")
            raise

    def get_children(self, run_id: str) -> list:
        """Get child executions for a JOBP.
        
        Args:
            run_id: Parent execution run ID
            
        Returns:
            List of child execution records
        """
        logger.info(f"Fetching children for run_id: {run_id}")
        try:
            endpoint = f"/{self.config.client_id}/executions/{run_id}/children"
            response = self.get(endpoint)
            return response.get("data", [])
        except Exception as e:
            logger.error(f"Failed to get children for {run_id}: {e}")
            raise

    def search(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a search query on Automic API.
        
        Args:
            payload: Search payload with filters
            
        Returns:
            Search results
        """
        logger.debug(f"Executing search with payload: {payload}")
        try:
            endpoint = f"/{self.config.client_id}/search"
            return self.post(endpoint, json=payload)
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise

    def get_job_logs(self, run_id: str, report_type: str = "REP") -> str:
        """Get job execution logs.
        
        Args:
            run_id: Execution run ID
            report_type: Report type (REP, LOG, etc.)
            
        Returns:
            Log content as string
        """
        logger.info(f"Fetching logs for run_id: {run_id}, type: {report_type}")
        try:
            endpoint = f"/{self.config.client_id}/executions/{run_id}/reports/{report_type}"
            response = self.get(endpoint)
            return response.get("content", "")
        except Exception as e:
            logger.error(f"Failed to get logs for {run_id}: {e}")
            raise

    def close(self) -> None:
        """Close the session and clean up resources."""
        logger.info("Closing Automic API session")
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
