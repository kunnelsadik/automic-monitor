# Project Structure and Organization

## Phase 1 Implementation Summary

This document describes the unified project structure implemented in Phase 1.

## Directory Organization

### Root Level Files
- `main.py` - Application entry point
- `pyproject.toml` - Project metadata and build configuration
- `requirements.txt` - Python dependencies
- `.env.template` - Environment variables template
- `.env` - Actual environment variables (not in version control)
- `.gitignore` - Git ignore rules
- `README.md` - Project documentation

### Source Code (`src/`)

#### `src/automic/`
Contains Automic API integration:
- `client.py` - Main AutomicClient class for API communication
- `apis.py` - Specific API endpoint functions
  - Job execution functions
  - Workflow retrieval functions
  - Status query functions

#### `src/processors/`
Contains business logic for processing:
- `job_processor.py` - Job execution processing
- `workflow_processor.py` - Workflow-level processing
- Related utilities for data transformation

#### `src/database/`
Contains database layer:
- `config.py` - Database connection configuration
- `models.py` - SQLAlchemy ORM models
- `operations.py` - CRUD operations and queries

#### `src/utils/`
Contains shared utilities:
- `csv_utils.py` - CSV file read/write operations
- `log_parser.py` - Job log parsing functionality
- `validators.py` - Business rules validation
- `helpers.py` - General helper functions

#### `src/`
Root source level:
- `config.py` - Pydantic application configuration class
- `logger.py` - Centralized logging setup
- `__init__.py` - Package initialization

### Data Directory (`data/`)
CSV files for configuration and tracking:
- `config_workflows.csv` - List of workflows to monitor
- `processed_runs.csv` - Tracking of processed executions
- `workflow_results.csv` - Results of workflow executions
- `business_rules.csv` - Business rules definitions
- `job_details.csv` - Job metadata

### Configuration (`config/`)
- `logging.yaml` - Logging configuration
- Application-specific config files

### Logs (`logs/`)
- `automic_monitor.log` - Main application log
- Dated log files for archival

### Tests (`tests/`)
- `test_automic_client.py` - API client tests
- `test_processors.py` - Processor logic tests
- `test_database.py` - Database operation tests
- `test_integration.py` - Integration tests

### Documentation (`docs/`)
- `setup.md` - Detailed setup instructions
- `architecture.md` - System architecture documentation
- `api.md` - API documentation
- `database.md` - Database schema documentation

## Naming Conventions

### Files
- Module files: `snake_case.py`
- Configuration: `config.py`
- Tests: `test_*.py`

### Classes
- Regular classes: `PascalCase`
- Configuration dataclass: `Config`
- Examples: `AutomicClient`, `JobProcessor`, `DatabaseManager`

### Functions
- Regular functions: `snake_case`
- Examples: `get_latest_executions()`, `process_job()`

### Constants
- Constants: `UPPER_SNAKE_CASE`
- Examples: `MAX_WORKERS`, `POLLING_INTERVAL`

### Modules
- Package names: `snake_case`
- Examples: `automic`, `processors`, `database`, `utils`

## Import Structure

### Relative Imports (Within Package)
```python
# From same package
from .client import AutomicClient

# From sibling package
from ..utils.validators import validate_job
```

### Absolute Imports (From External)
```python
# Standard library
import logging
from pathlib import Path
from datetime import datetime

# Third-party
import pandas as pd
import requests
from pydantic import BaseSettings

# Local
from src.automic.client import AutomicClient
from src.config import Config
```

## Module Dependencies Flow

```
main.py (Entry Point)
    ├── src.config (Configuration)
    ├── src.logger (Logging setup)
    ├── src.automic.client (API communication)
    ├── src.processors (Job/Workflow processing)
    │   └── src.database (DB operations)
    │   └── src.utils (Validation, parsing)
    └── src.utils (CSV operations, helpers)
```

## Configuration Hierarchy

1. **Environment Variables (.env file)**
   - Lowest level: hardcoded in the system
   
2. **Pydantic Config Class (src/config.py)**
   - Validates and type-checks environment variables
   - Provides defaults for missing values
   
3. **Application Runtime**
   - Config instance used throughout application
   - Configuration available via dependency injection

## Code Style Standards

### Black Formatting
- Line length: 100 characters
- String quotes: double quotes preferred
- Trailing commas in multi-line structures

### isort Import Sorting
```python
# 1. Standard library
import os
import sys
from pathlib import Path

# 2. Third-party
import pandas as pd
from pydantic import BaseSettings

# 3. Local
from src.config import Config
from src.logger import get_logger
```

### Type Hints
```python
# Function signatures
def process_job(job_data: dict, parent_run_id: Optional[int] = None) -> bool:
    """Process a single job."""
    pass

# Class attributes
class JobProcessor:
    name: str
    run_id: int
    status: str
```

## Error Handling

### Exception Hierarchy
```
Exception
├── AutomicClientError (API errors)
├── DatabaseError (Database operations)
├── ProcessingError (Job processing)
└── ValidationError (Business rules)
```

### Logging Exceptions
```python
try:
    executions = client.get_latest_executions(workflow_name)
except AutomicClientError as e:
    logger.error(f"Failed to get executions for {workflow_name}: {e}")
```

## Testing Structure

- Unit tests: `tests/test_*.py`
- Integration tests: `tests/integration/`
- Fixtures: `tests/conftest.py`
- Mock data: `tests/fixtures/`

## Dependencies Organization

### Core Dependencies
- `requests` - HTTP client
- `pandas` - Data manipulation
- `python-dotenv` - Environment variables

### Data & Validation
- `pydantic` - Data validation
- `sqlalchemy-access` - Database ORM

### File Processing
- `pyx12` - EDI parsing
- `edi-835-parser` - Claim parsing
- `openpyxl` - Excel support

### Development (Optional)
- `pytest` - Testing framework
- `black` - Code formatting
- `isort` - Import sorting
- `flake8` - Linting
- `mypy` - Type checking

## Phase 1 Completion Checklist

✅ Created consolidated `pyproject.toml`
✅ Updated `requirements.txt` with all dependencies
✅ Created `.env.template` with all configuration variables
✅ Created `.gitignore` for version control
✅ Organized directory structure:
  - ✅ src/automic/ (API client)
  - ✅ src/processors/ (Job/Workflow processing)
  - ✅ src/database/ (Database utilities)
  - ✅ src/utils/ (Shared utilities)
  - ✅ config/ (Configuration files)
  - ✅ logs/ (Log output)
  - ✅ tests/ (Test suite)
  - ✅ docs/ (Documentation)
✅ Created package `__init__.py` files
✅ Updated README.md with comprehensive documentation
✅ Created this structure documentation

## Next Steps (Phase 2)

In Phase 2, we will:
1. Merge API client code from v2 into `src/automic/`
2. Consolidate processor logic into `src/processors/`
3. Integrate database utilities into `src/database/`
4. Organize utility functions into `src/utils/`

---

**Date**: April 25, 2026
**Phase**: 1 - Complete
