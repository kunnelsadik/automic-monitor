"""Application configuration management using Pydantic."""
import logging
from typing import Optional

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class AutomicConfig(BaseSettings):
    """Automic API Configuration.
    
    This class handles all Automic-specific settings loaded from environment variables.
    Sensitive values like passwords are masked by Pydantic.
    
    Example:
        config = AutomicConfig()
        client = AutomicClient(config=config)
    """

    base_url: str = Field(
        ...,
        description="Base URL for Automic API",
        examples=["https://hpappworx01:8488/ae/api/v2"],
    )
    client_id: int = Field(default=3000, description="Automic client ID")
    username: str = Field(..., description="Automic API username")
    password: SecretStr = Field(..., description="Automic API password (masked)")
    timeout: int = Field(default=30, description="HTTP request timeout in seconds")
    ssl_verify: bool = Field(
        default=False, description="Verify SSL certificates for HTTPS connections"
    )
    proxy_server: Optional[str] = Field(default=None, description="Proxy server URL")

    class Config:
        env_prefix = "AUTOMIC_"
        case_sensitive = False

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Ensure timeout is positive."""
        if v <= 0:
            raise ValueError("timeout must be positive")
        return v


class DatabaseConfig(BaseSettings):
    """Database Configuration.
    
    Handles both SQL Server and MS Access database connections.
    
    Example:
        config = DatabaseConfig()
        engine = create_engine(config.connection_string)
    """

    connection_string: Optional[str] = Field(
        default=None, description="Full database connection string"
    )
    driver: str = Field(
        default="Microsoft Access Driver (*.mdb, *.accdb)",
        description="ODBC driver name",
    )
    file_path: Optional[str] = Field(
        default=None, description="Path to Access database file"
    )

    class Config:
        env_prefix = "DB_"
        case_sensitive = False

    def get_connection_string(self) -> str:
        """Get the appropriate connection string."""
        if self.connection_string:
            return self.connection_string
        if self.file_path:
            return f"DRIVER={{{self.driver}}};DBQ={self.file_path};"
        raise ValueError(
            "Either CONNECTION_STRING or FILE_PATH must be configured"
        )


class FileProcessingConfig(BaseSettings):
    """File Processing Configuration.
    
    Configuration for file processing and shared drives.
    """

    shared_drive_path: str = Field(
        default=r"\\hpfs\SharedSecure$\Operations\IS\ProductionControl",
        description="Path to shared drive for file operations",
    )
    landing_zone_path: str = Field(
        default="C:\\LandingZone", description="Local landing zone directory"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False


class ApplicationConfig(BaseSettings):
    """Application Configuration.
    
    General application settings including logging and threading.
    
    Example:
        config = ApplicationConfig()
        logging.basicConfig(level=config.log_level)
    """

    log_level: str = Field(default="INFO", description="Logging level")
    polling_interval_seconds: int = Field(
        default=60, description="Polling interval in seconds"
    )
    worker_threads: int = Field(default=4, description="Number of worker threads")
    queue_size: int = Field(default=1000, description="Job queue size")

    # Feature flags
    enable_file_processing: bool = Field(
        default=True, description="Enable file processing"
    )
    enable_log_parsing: bool = Field(default=True, description="Enable log parsing")
    enable_database_logging: bool = Field(
        default=True, description="Enable database logging"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False

    @field_validator("polling_interval_seconds", "worker_threads", "queue_size")
    @classmethod
    def validate_positive(cls, v: int) -> int:
        """Ensure positive values."""
        if v <= 0:
            raise ValueError("Value must be positive")
        return v


class Config(BaseSettings):
    """Main Application Configuration.
    
    Aggregates all sub-configurations into a single point of access.
    
    Usage:
        from src.config import Config
        config = Config()
        
        # Access sub-configurations
        automic_url = config.automic.base_url
        db_conn = config.database.get_connection_string()
        log_level = config.app.log_level
    """

    automic: AutomicConfig = Field(default_factory=AutomicConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    file_processing: FileProcessingConfig = Field(
        default_factory=FileProcessingConfig
    )
    app: ApplicationConfig = Field(default_factory=ApplicationConfig)

    class Config:
        case_sensitive = False

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        logger.info("Loading configuration from environment variables")
        return cls()


def get_config() -> Config:
    """Get the application configuration.
    
    This is a singleton pattern to avoid reloading config multiple times.
    
    Returns:
        Config: Application configuration instance
        
    Example:
        config = get_config()
        automic_client = AutomicClient(config=config.automic)
    """
    return Config.from_env()
