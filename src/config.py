"""Application configuration loaded from environment variables."""
from functools import lru_cache
from typing import Optional

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AutomicConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUTOMIC_", case_sensitive=False)

    base_url: str = Field(...)
    client_id: int = Field(default=3000)
    username: str = Field(...)
    password: SecretStr = Field(...)
    timeout: int = Field(default=30)
    ssl_verify: bool = Field(default=False)

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("timeout must be positive")
        return v


class DatabaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", case_sensitive=False)

    connection_string: Optional[str] = Field(default=None)
    driver: str = Field(default="Microsoft Access Driver (*.mdb, *.accdb)")
    file_path: Optional[str] = Field(default=None)

    def get_connection_string(self) -> str:
        if self.connection_string:
            return self.connection_string
        if self.file_path:
            return f"DRIVER={{{self.driver}}};DBQ={self.file_path};"
        raise ValueError("Either DB_CONNECTION_STRING or DB_FILE_PATH must be set")


class FileProcessingConfig(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    shared_drive_path: str = Field(
        default=r"\\hpfs\SharedSecure$\Operations\IS\ProductionControl"
    )
    landing_zone_path: str = Field(default="C:\\LandingZone")


class ApplicationConfig(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    log_level: str = Field(default="INFO")
    polling_interval_seconds: int = Field(default=60)
    worker_threads: int = Field(default=4)
    queue_size: int = Field(default=1000)
    enable_file_processing: bool = Field(default=True)
    enable_log_parsing: bool = Field(default=True)
    enable_database_logging: bool = Field(default=True)

    @field_validator("polling_interval_seconds", "worker_threads", "queue_size")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Value must be positive")
        return v


class Config(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    automic: AutomicConfig = Field(default_factory=AutomicConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    file_processing: FileProcessingConfig = Field(default_factory=FileProcessingConfig)
    app: ApplicationConfig = Field(default_factory=ApplicationConfig)


@lru_cache(maxsize=1)
def get_config() -> Config:
    return Config()
