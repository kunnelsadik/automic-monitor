"""Logging configuration and setup."""
import logging
import logging.config
import os
from pathlib import Path

import yaml


def setup_logging(
    config_file: str = "config/logging.yaml", log_level: str = "INFO"
) -> None:
    """Set up logging from YAML configuration file.
    
    Args:
        config_file: Path to logging configuration YAML file
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    config_path = Path(config_file)

    if config_path.exists():
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            logging.config.dictConfig(config)
    else:
        # Fallback basic configuration
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized with level: {log_level}")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        from src.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Application started")
    """
    return logging.getLogger(name)
