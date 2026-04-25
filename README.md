# Automic Monitor

Polls Automic (job scheduling platform) for workflow and job execution status, processes results, parses logs, applies business rules, and persists everything to CSV/database.

## Project Structure

```
automic-monitor/
├── src/
│   ├── automic/
│   │   ├── client.py         # AutomicClient — HTTP session, proxy detection, API calls
│   │   └── apis.py           # normalize_automic_log() — cleans raw API log text
│   ├── processors/
│   │   └── __init__.py       # normalize_status(), process_job()
│   ├── database/
│   │   └── __init__.py       # (Phase 2 — DB layer placeholder)
│   ├── utils/
│   │   ├── csv_utils.py      # read_csv, write_csv, append_csv, now(), loader helpers
│   │   ├── log_parser.py     # parse_job_log(), extract_file_counts(), get_error_summary()
│   │   └── rule_engine.py    # validate_files() — file-count business rules
│   ├── config.py             # Pydantic-settings config classes + get_config()
│   └── logger.py             # setup_logging() from config/logging.yaml
│
├── config/
│   └── logging.yaml          # Rotating file + console handlers
│
├── data/                     # Runtime CSV files (created on first run)
│   ├── config_workflows.csv  # Workflows to monitor
│   ├── processed_runs.csv    # Dedup tracking for seen run IDs
│   ├── workflow_results.csv  # Per-run status records
│   ├── job_details.csv       # Per-job detail records
│   └── business_rules.csv    # File-count rules
│
├── tests/
├── main.py                   # Entry point: poller thread + N worker threads
├── pyproject.toml
├── requirements.txt
└── .env.template
```

## Setup

### Prerequisites

- Python 3.10+
- ODBC driver for MS Access (`Microsoft Access Driver (*.mdb, *.accdb)`) if using Access DB

### Install

```bash
# Using uv (recommended — uv.lock is committed)
uv sync

# Or plain pip
pip install -r requirements.txt
```

### Environment Variables

Copy `.env.template` to `.env` and fill in values:

```bash
cp .env.template .env
```

Required variables:

| Variable | Description |
|---|---|
| `AUTOMIC_BASE_URL` | e.g. `https://hpappworx01:8488/ae/api/v2` |
| `AUTOMIC_USERNAME` | API username |
| `AUTOMIC_PASSWORD` | API password |
| `AUTOMIC_CLIENT_ID` | Client number (default `3000`) |

Optional:

| Variable | Default | Description |
|---|---|---|
| `AUTOMIC_TIMEOUT` | `30` | HTTP timeout in seconds |
| `AUTOMIC_SSL_VERIFY` | `false` | Verify TLS certificates |
| `AUTOMIC_PROXY_SERVER` | — | Override proxy (auto-detected from system if blank) |
| `DB_CONNECTION_STRING` | — | Full ODBC connection string |
| `DB_FILE_PATH` | — | Path to `.accdb` file (alternative to connection string) |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `POLLING_INTERVAL_SECONDS` | `60` | How often to poll Automic |
| `WORKER_THREADS` | `4` | Concurrent job processor threads |
| `QUEUE_SIZE` | `1000` | Max items in the processing queue |

## Running

```bash
python main.py
```

On startup the app:
1. Loads config from `.env`
2. Starts `WORKER_THREADS` background threads
3. Enters a polling loop — every `POLLING_INTERVAL_SECONDS` it fetches new executions for each active workflow in `data/config_workflows.csv` and queues anything not already in `data/processed_runs.csv`
4. Workers pick up queue items, normalize status, append to CSV, and process job logs

## Architecture

```
Poller (main thread, every N seconds)
  │  reads config_workflows.csv
  │  reads processed_runs.csv  →  dedup set
  │  calls AutomicClient.get_latest_executions()
  └──► Queue
           │
    Worker threads (N)
           │  normalize_status()
           │  append workflow_results.csv
           ├── JOBS  → process_job()  → append job_details.csv
           └── JOBP  → get_children() → process_job() per child
```

## Key Modules

### `src/config.py`

Four `BaseSettings` subclasses (`AutomicConfig`, `DatabaseConfig`, `FileProcessingConfig`, `ApplicationConfig`) aggregated under `Config`. Call `get_config()` anywhere — it's cached via `@lru_cache`.

```python
from src.config import get_config
cfg = get_config()
print(cfg.automic.base_url)
print(cfg.app.worker_threads)
```

### `src/automic/client.py`

```python
from src.automic.client import AutomicClient
from src.config import get_config

client = AutomicClient(config=get_config().automic)
executions = client.get_latest_executions("JOBP.DAILY_WORKFLOW")
children   = client.get_children(run_id=86042036)
logs       = client.get_job_logs(run_id="86041014", report_type="REP")
```

Proxy is resolved in order: environment variables → `AUTOMIC_PROXY_SERVER` env var → Windows Registry / WinHTTP.

### `src/processors/__init__.py`

```python
from src.processors import normalize_status, process_job

status = normalize_status(1900)          # "ENDED_OK"
status = normalize_status(1800)          # "ENDED_NOT_OK"
process_job(job_dict, parent_run_id=123) # appends to data/job_details.csv
```

Status codes: `1900` ENDED_OK · `1800` ENDED_NOT_OK · `1801` ABORTED · `1700` WAITING · `1560` BLOCKED · `1550` ACTIVE

### `src/utils/log_parser.py`

```python
from src.utils.log_parser import parse_job_log

result = parse_job_log(log_text)
# result["return_code"]       int or None
# result["errors"]            list of {type, severity, retryable, line}
# result["transfer_details"]  {command, source, destination, extension, count, transfer_ok, has_failure}
# result["external_logs"]     list of external log paths found in the log
```

## Development

```bash
# Format
black src/ main.py

# Sort imports
isort src/ main.py

# Lint
flake8 src/ main.py

# Type check
mypy src/

# Tests
pytest
pytest --cov=src --cov-report=html
```

## Logs

- Console: `INFO` and above
- `logs/automic_monitor.log`: `DEBUG` and above, rotating 10 MB × 5
- `logs/automic_monitor_error.log`: `ERROR` and above, rotating 5 MB × 3

## Troubleshooting

**`Failed to fetch executions`** — check `AUTOMIC_BASE_URL`, credentials, and network connectivity to the Automic server. SSL errors on internal certs are expected with `AUTOMIC_SSL_VERIFY=false`.

**Proxy issues** — the client auto-detects Windows system proxy. Set `AUTOMIC_PROXY_SERVER=http://proxy:8080` to override, or set standard `HTTP_PROXY` / `HTTPS_PROXY` env vars.

**Queue fills up** — increase `WORKER_THREADS` or `QUEUE_SIZE`, or reduce `POLLING_INTERVAL_SECONDS`.

**MS Access connection** — ensure the 32-bit or 64-bit Access ODBC driver matches your Python bitness. Set either `DB_CONNECTION_STRING` (full string) or `DB_FILE_PATH` (path to `.accdb`).
