import os
import requests
from requests.auth import HTTPBasicAuth

BASE_URL = os.getenv("AUTOMIC_BASE_URL")
CLIENT = os.getenv("AUTOMIC_CLIENT", "3000")
USERNAME = os.getenv("AUTOMIC_USERNAME")
PASSWORD = os.getenv("AUTOMIC_PASSWORD")

class AutomicClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
        self.session.verify = False
        self.session.headers.update({"Accept": "application/json"})

    def get_latest_executions(self, object_name: str):
        """
        Returns latest execution(s) for a JOBS or JOBP
        """
        url = f"{BASE_URL}/{CLIENT}/executions"
        params = {"name": object_name}

        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_children(self, run_id: int):
        """
        Used ONLY for workflows (JOBP)
        """
        url = f"{BASE_URL}/{CLIENT}/executions/{run_id}/children"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("data", [])