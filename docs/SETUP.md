# Setup Instructions

## Prerequisites

- Windows 10/11 or Linux/Mac
- Python 3.10 or higher
- pip package manager
- Git (for version control)
- Access to Automic API endpoint
- MS SQL Server or Access database connection

## Step-by-Step Installation

### 1. Clone/Download Project

```bash
# Using Git
git clone <repository_url>
cd automic_monitor

# Or download and extract zip file
cd automic_monitor
```

### 2. Create Virtual Environment

**Windows:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Upgrade pip

```bash
pip install --upgrade pip setuptools wheel
```

### 4. Install Dependencies

```bash
# Install from requirements.txt
pip install -r requirements.txt

# OR install from pyproject.toml (development)
pip install -e .

# OR install with development tools
pip install -e ".[dev]"
```

### 5. Create Environment Configuration

**Step 5a: Copy template**
```bash
# Windows
copy .env.template .env

# Linux/Mac
cp .env.template .env
```

**Step 5b: Edit .env file**

Open `.env` in a text editor and fill in required values:

```ini
# ===== AUTOMIC API CONFIGURATION =====
AUTOMIC_BASE_URL=https://hpappworx01:8488/ae/api/v2
AUTOMIC_CLIENT=3000
AUTOMIC_USERNAME=<your_username>
AUTOMIC_PASSWORD=<your_password>

# ===== DATABASE CONFIGURATION =====
# For SQL Server
DB_CONNECTION_STRING=DRIVER={ODBC Driver 17 for SQL Server};SERVER=server_name;DATABASE=db_name;UID=user;PWD=password

# For MS Access
DB_DRIVER=Microsoft Access Driver (*.mdb, *.accdb)
DB_FILE_PATH=\\path\to\database.accdb

# ===== FILE PROCESSING =====
SHARED_DRIVE_PATH=\\hpfs\SharedSecure$\Operations\IS\ProductionControl
LANDING_ZONE_PATH=C:\LandingZone

# ===== APPLICATION SETTINGS =====
LOG_LEVEL=INFO
POLLING_INTERVAL_SECONDS=60
WORKER_THREADS=4
QUEUE_SIZE=1000

# ===== SSL/TLS SETTINGS =====
SSL_VERIFY=false
PROXY_SERVER=

# ===== FEATURE FLAGS =====
ENABLE_FILE_PROCESSING=true
ENABLE_LOG_PARSING=true
ENABLE_DATABASE_LOGGING=true
```

### 6. Verify Installation

```bash
# Test Python environment
python --version
# Should show Python 3.10+

# Test imports
python -c "import pandas, pydantic, requests; print('All imports OK')"
```

### 7. Test Automic Connection

```bash
# Create a test script or run:
python -c "
from src.automic.client import AutomicClient
client = AutomicClient()
print('Automic connection successful')
"
```

## Configuration Details

### Automic API Configuration

**AUTOMIC_BASE_URL**
- URL to your Automic API endpoint
- Default: `https://hpappworx01:8488/ae/api/v2`
- Format: `https://hostname:port/ae/api/vX`

**AUTOMIC_CLIENT**
- Client ID in Automic system
- Default: `3000`
- Obtain from Automic administration

**AUTOMIC_USERNAME**
- Username for API authentication
- Must have permissions to query workflows and jobs

**AUTOMIC_PASSWORD**
- Password for API authentication
- Never commit to version control

### Database Configuration

**For MS SQL Server:**
```
DB_CONNECTION_STRING=DRIVER={ODBC Driver 17 for SQL Server};SERVER=hostname;DATABASE=dbname;UID=username;PWD=password
```

**For MS Access:**
```
DB_DRIVER=Microsoft Access Driver (*.mdb, *.accdb)
DB_FILE_PATH=C:\path\to\database.accdb
```

### Application Settings

**LOG_LEVEL**
- Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
- DEBUG: Most verbose logging
- INFO: Standard logging (recommended)

**POLLING_INTERVAL_SECONDS**
- How often to poll Automic for new executions
- Minimum: 30 seconds
- Default: 60 seconds
- Increase for large systems to reduce API load

**WORKER_THREADS**
- Number of concurrent job processing threads
- Default: 4
- Increase for high-throughput systems
- Monitor system resources

### SSL/TLS Settings

**SSL_VERIFY**
- Whether to verify SSL certificates
- Set to `false` for self-signed certificates
- Set to `true` for production

**PROXY_SERVER**
- Corporate proxy server (if needed)
- Format: `http://proxy_host:port`
- Leave empty if not needed

## Running the Application

### Basic Start

```bash
python main.py
```

### With Command Line Options

```bash
# Debug logging
python main.py --log-level DEBUG

# Single poll execution (no loop)
python main.py --once

# Custom polling interval
python main.py --interval 30

# Combine options
python main.py --log-level DEBUG --interval 30
```

## Troubleshooting

### Issue: "Module not found" errors

**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# Check if src package is recognized
python -c "import src; print(src.__file__)"
```

### Issue: Connection refused to Automic

**Solution:**
1. Verify AUTOMIC_BASE_URL is correct
2. Check credentials in .env file
3. Test network connectivity: `ping hpappworx01`
4. Check if proxy is needed: `echo %HTTP_PROXY%`
5. Disable SSL verification if using self-signed cert

### Issue: Database connection failed

**Solution:**
1. Verify database file/server is accessible
2. Check ODBC driver installed: `python -m pyodbc`
3. Test connection string separately
4. Check file permissions for Access database

### Issue: Environment variables not loading

**Solution:**
```bash
# Verify .env file exists in correct location
ls .env

# Check file format (no spaces around =)
# Should be: KEY=value
# Not: KEY = value

# Reload environment
# Close and reopen terminal
```

### Issue: Permission denied errors

**Solution:**
1. Check file/folder permissions
2. Run as administrator (Windows)
3. Check network drive access
4. Verify service account has proper permissions

## Verification Checklist

After installation, verify:

- [ ] Python 3.10+ installed: `python --version`
- [ ] Virtual environment activated
- [ ] Dependencies installed: `pip list | grep pandas`
- [ ] .env file created and populated
- [ ] Automic connection works
- [ ] Database connection works
- [ ] Can read CSV files from data/ folder
- [ ] Logs directory is writable
- [ ] No import errors: `python main.py --help`

## Daily Operations

### Starting the Application

**Windows (PowerShell):**
```powershell
.\.venv\Scripts\Activate.ps1
python main.py
```

**Linux/Mac:**
```bash
source venv/bin/activate
python main.py
```

### Stopping the Application

- Press `Ctrl+C` in terminal to gracefully shutdown
- Wait for current operations to complete

### Viewing Logs

```bash
# Current session logs
tail -f logs/automic_monitor.log

# Windows
Get-Content logs/automic_monitor.log -Tail 50 -Wait
```

### Checking Application Status

```bash
# View process
ps aux | grep main.py

# Windows
Get-Process python | Where-Object {$_.CommandLine -like "*main.py*"}
```

## Updating Dependencies

```bash
# Check for updates
pip list --outdated

# Update specific package
pip install --upgrade pandas

# Update all packages
pip install --upgrade -r requirements.txt
```

## Development Setup

If contributing to the project:

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Setup pre-commit hooks
pre-commit install

# Run tests
pytest

# Run code quality checks
black src/
isort src/
flake8 src/
mypy src/
```

## Performance Tuning

### For High-Volume Workflows

```ini
WORKER_THREADS=8
QUEUE_SIZE=2000
POLLING_INTERVAL_SECONDS=30
```

### For Low-Volume Workflows

```ini
WORKER_THREADS=2
QUEUE_SIZE=100
POLLING_INTERVAL_SECONDS=120
```

### For Development/Testing

```ini
LOG_LEVEL=DEBUG
WORKER_THREADS=1
QUEUE_SIZE=50
POLLING_INTERVAL_SECONDS=300
```

## Security Best Practices

1. **Never commit .env file to version control**
   - Always use .env.template
   - Add .env to .gitignore (already done)

2. **Rotate credentials regularly**
   - Change AUTOMIC_PASSWORD quarterly
   - Update service account passwords

3. **Restrict file permissions**
   - Database files: 600 permissions
   - Configuration files: 640 permissions

4. **Use HTTPS for Automic API**
   - Always verify SSL certificates in production
   - Use corporate SSL certificates

5. **Monitor logs for errors**
   - Check logs daily for authentication failures
   - Alert on repeated connection errors

## Getting Help

1. **Check logs** in `logs/automic_monitor.log`
2. **Review documentation** in `docs/` folder
3. **Check troubleshooting** section above
4. **Contact development team** with:
   - Error message
   - Application logs
   - Environment configuration (sanitized)
   - Steps to reproduce

---

**Last Updated**: April 25, 2026
**Version**: 0.2.0
