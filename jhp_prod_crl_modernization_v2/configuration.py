from typing import Annotated
from pydantic import ConfigDict, SecretStr,SkipValidation
from pydantic.dataclasses import dataclass
import re

from requests.auth import HTTPBasicAuth

@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class AutomicConfig:
    username: str
    password: SecretStr  # Special type that masks the value
    endpoint: str
    client_id: int
    base_url: str
    timeout: int = 30
    auth: Annotated[HTTPBasicAuth, SkipValidation] = None

    def __post_init__(self):
        """
        This runs automatically after the object is created.
        It generates the Auth object using the secret password.
        """
        self.auth = HTTPBasicAuth(
            self.username, 
            self.password.get_secret_value()
        )