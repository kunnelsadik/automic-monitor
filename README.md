# Automic Monitor

A comprehensive monitoring and processing system for Automic workflows and jobs.

## Overview

Automic Monitor is a robust system designed to:
- Poll Automic workflows for execution status
- Process job executions and file transfers
- Track workflow execution history in a database
- Parse and analyze job logs
- Apply business rules validation
- Maintain audit trails and metrics

## Project Structure

```
automic_monitor/
├── src/
│   ├── __init__.py
│   ├── automic/              # Automic API client and integration
│   │   ├── __init__.py
│   │   ├── client.py         # AutomicClient class
│   │   └── apis.py           # API endpoint functions
│   ├── processors/           # Job and workflow processing logic
│   │   ├── __init__.py
│   │   ├── job_processor.py  # Job processing
│   │   └── workflow_processor.py  # Workflow processing
│   ├── database/             # Database utilities
│   │   ├── __init__.py
│   │   ├── config.py         # Database configuration
│   │   ├── models.py         # SQLAlchemy models
│   │   └── operations.py     # DB operations
│   ├── utils/                # Utility functions
│   │   ├── __init__.py
│   │   ├── csv_utils.py      # CSV file operations
│   │   ├── log_parser.py     # Log parsing utilities
│   │   └── validators.py     # Data validation rules
│   ├── config.py             # Application configuration (Pydantic)
│   └── logger.py             # Logging setup
│
├── config/                   # Configuration files
│   └── logging.yaml          # Logging configuration
│
├── data/                     # Data files
│   ├── config_workflows.csv  # Workflow configuration
│   ├── processed_runs.csv    # Processed execution tracking
│   └── workflow_results.csv  # Workflow execution results
│
├── logs/                     # Application logs
│
├── tests/                    # Unit and integration tests
│   ├── __init__.py
│   ├── test_automic_client.py
│   ├── test_processors.py
│   └── test_database.py
│
├── docs/                     # Documentation
│   ├── setup.md              # Setup instructions
│   ├── architecture.md       # Architecture documentation
│   └── api.md                # API documentation
│
├── main.py                   # Application entry point
├── pyproject.toml            # Project configuration
├── requirements.txt          # Python dependencies
├── .env.template             # Environment variables template
└── .gitignore                # Git ignore rules
```

## Installation

### Prerequisites
- Python 3.10 or higher
- pip or conda
- Virtual environment (recommended)

### Setup Steps

1. **Clone or download the project**
```bash
cd automic_monitor
```

2. **Create a virtual environment**
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Create environment configuration**
```bash
# Copy the template
cp .env.template .env

# Edit .env with your configuration
notepad .env  # Windows
nano .env     # Linux/Mac
```

5. **Configure required environment variables**
```
# Automic API
AUTOMIC_BASE_URL=https://hpappworx01:8488/ae/api/v2
AUTOMIC_CLIENT=3000
AUTOMIC_USERNAME=<your_username>
AUTOMIC_PASSWORD=<your_password>

# Database
DB_CONNECTION_STRING=<your_db_connection>

# Application
LOG_LEVEL=INFO
POLLING_INTERVAL_SECONDS=60
WORKER_THREADS=4
```

## Configuration

Configuration is managed through:

1. **Environment Variables (.env file)**
   - API credentials
   - Database connection strings
   - Application settings

2. **Pydantic Configuration Class (src/config.py)**
   - Type validation
   - Default values
   - Runtime configuration

## Usage

### Running the Application

```bash
python main.py
```

### Command Line Options

```bash
# Verbose logging
python main.py --log-level DEBUG

# Single poll run (no continuous loop)
python main.py --once

# Custom polling interval (in seconds)
python main.py --interval 30
```

### Core Components

#### 1. Automic Client
```python
from src.automic.client import AutomicClient

client = AutomicClient()
executions = client.get_latest_executions("JOBP.WORKFLOW_NAME")
```

#### 2. Job Processing
```python
from src.processors.job_processor import process_job

process_job(job_data, parent_run_id=123)
```

#### 3. Database Operations
```python
from src.database.operations import get_job_stats, update_workflow_status

stats = get_job_stats(job_name)
update_workflow_status(run_id, "COMPLETED")
```

## Development

### Code Style

- **Formatting**: black (line-length: 100)
- **Import sorting**: isort
- **Linting**: flake8
- **Type checking**: mypy

### Running Tests

```bash
# Install dev dependencies
pip install -r requirements.txt -e ".[dev]"

# Run tests
pytest

# With coverage
pytest --cov=src --cov-report=html
```

### Code Quality Checks

```bash
# Format code
black src/

# Sort imports
isort src/

# Lint
flake8 src/

# Type check
mypy src/
```

## Architecture

### Polling & Processing Flow

```
┌─────────────┐
│   Poller    │  (Runs every 60s by default)
│             │  - Loads active workflows
│             │  - Fetches latest executions
│             │  - Adds new runs to queue
└──────┬──────┘
       │
       ├─→ Job Queue
       │
       ▼
┌─────────────┐
│   Worker    │  (4 threads by default)
│ Threads     │  - Processes queue items
│             │  - Handles JOBS and JOBP types
│             │  - Persists results
└─────────────┘
```

### Key Classes

- **AutomicClient**: HTTP client for Automic API
- **JobProcessor**: Processes individual job executions
- **WorkflowProcessor**: Manages workflow-level logic
- **DatabaseManager**: Handles all database operations
- **RuleEngine**: Applies business rules validation

## Monitoring & Logging

Logs are written to:
- Console (INFO level)
- `logs/automic_monitor.log` (DEBUG level)

Log format: `timestamp - level - message`

## Database Schema

The system uses MS SQL Server / Access with tables:
- `job_stats`: Job execution statistics
- `workflow_status`: Workflow run history
- `job_details`: Job configuration and metadata

## Troubleshooting

### Connection Issues
```
Error: Failed to get executions for WORKFLOW_NAME
- Check AUTOMIC_BASE_URL is correct
- Verify AUTOMIC_USERNAME and AUTOMIC_PASSWORD
- Ensure network connectivity to Automic server
```

### Database Errors
```
Error: Database connection failed
- Verify DB_CONNECTION_STRING is correct
- Check database driver is installed (pyodbc)
- Ensure database file/server is accessible
```

### File Processing Issues
```
Error: File not found
- Verify SHARED_DRIVE_PATH and file paths are correct
- Check file permissions
```

## Contributing

Please follow these guidelines:
1. Create a feature branch
2. Follow code style standards (black, isort, flake8)
3. Add tests for new functionality
4. Update documentation
5. Submit pull request

## License

MIT License - See LICENSE file for details

## Support

For issues or questions:
- Check the docs/ folder for detailed documentation
- Review logs in the logs/ folder
- Contact the development team

---

**Last Updated**: April 25, 2026
**Version**: 0.2.0
