"""Automic API HTTP client."""
import logging
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
        self.session.auth = HTTPBasicAuth(
            config.username, config.password.get_secret_value()
        )
        self.session.verify = config.ssl_verify
        self.session.headers.update({"Accept": "application/json"})

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

    def get_available_reports(self, run_id: str) -> list[dict]:
        logger.info(f"Fetching available reports for run_id={run_id}")
        result = self.get(f"/{self.config.client_id}/executions/{run_id}/reports")
        if isinstance(result, list):
            return result
        return result.get("data", [])

    def get_job_logs(self, run_id: str, report_type: str = "REP") -> str:
        logger.info(f"Fetching logs for run_id={run_id} type={report_type}")
        resp = self.get(
            f"/{self.config.client_id}/executions/{run_id}/reports/{report_type}"
        )
        pages = resp.get("data", [])
        return "\n".join(page.get("content", "") for page in pages)

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
