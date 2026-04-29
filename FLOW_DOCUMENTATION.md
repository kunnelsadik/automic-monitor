# AUTOMIC MONITOR - COMPLETE APPLICATION FLOW

## 1. APPLICATION STARTUP

```
main.py execution begins
    ↓
Load .env configuration
    ↓
Initialize components:
  - config = pydantic settings from .env
  - logging = setup_logging(log_level from config)
  - AutomicClient = HTTP client to Automic API with proxy detection
  - OpenAIClient = Optional GPT-4o-mini for log summarization
  - job_queue = Thread-safe queue (max 1000 items)
    ↓
Create ThreadPoolExecutor:
  - Spawn cfg.app.worker_threads threads (default: 4)
  - Each thread runs worker() function (infinite loop)
    ↓
Enter polling loop:
  - poller() called every cfg.app.polling_interval_seconds (default: 60)
```

---

## 2. POLLING CYCLE (runs every 60 seconds)

### 2A. Load Configuration & Track Processed Runs

```
poller() starts
    ↓
Load data/config_workflows.csv:
  - Read list of workflows to monitor
  - Columns: workflow_name, object_type (JOBS|JOBP), is_active, last_polled_at
    ↓
Load data/processed_runs.csv:
  - Read all previously processed run_ids
  - Convert to set: seen_run_ids = {run_id, run_id, ...}
  - Purpose: Prevent reprocessing same workflow execution
```

### 2B. Iterate Through Active Workflows

```
FOR EACH workflow in config_workflows.csv:
  IF workflow.is_active == False: SKIP
  
  TRY:
    executions = AutomicClient.get_latest_executions(workflow_name)
    
    FOR EACH execution in executions:
      run_id = execution["run_id"]
      
      IF run_id IN seen_run_ids:
        SKIP (already processed in this session or previous session)
      
      # For standalone JOBS: only process when terminal
      IF workflow.object_type == "JOBS":
        status_code = execution["status"]  (1900=OK, 1800=NOT_OK, 1801=ABORTED, etc.)
        IF status_code NOT IN (1900, 1800, 1801):
          SKIP (job still running, wait until terminal)
      
      # Queue for worker thread processing
      seen_run_ids.ADD(run_id)
      job_queue.PUT((workflow, execution))
      
      # Track in processed_runs.csv
      append_csv("data/processed_runs.csv", {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "processed_timestamp": now()
      })
      
      # If JOBP: track for re-polling children
      IF workflow.object_type == "JOBP":
        add_active_jobp(run_id, workflow_name)
        → Appends to data/active_jobps.csv
```

### 2C. Re-Poll All Active JOBPs (Parent Jobs)

```
Load data/active_jobps.csv:
  - Get all parent jobs still being tracked (JOBP type)

FOR EACH active JOBP in data/active_jobps.csv:
  run_id = active["run_id"]
  
  TRY:
    exec_details = AutomicClient.get_execution_details(run_id)
    parent_status = normalize_status(exec_details["status"])
    
    # Append updated status to workflow_results
    append_csv("data/workflow_results.csv", {
      "run_id": run_id,
      "workflow_name": wf_name,
      "status": parent_status,
      "start_time": exec_details["start_time"],
      "end_time": exec_details["end_time"]
    })
    
    IF parent_status IN TERMINAL_STATUSES (ENDED_OK, ENDED_NOT_OK, ABORTED):
      remove_active_jobp(run_id)  # Remove from tracking
```

**⚠️ ISSUE: Same JOBP written multiple times!**
- Polling cycle 1: Worker appends JOBP to workflow_results.csv
- Polling cycle 2: _repoll_active_jobp() appends SAME JOBP again
- Polling cycle 3: _repoll_active_jobp() appends SAME JOBP again
- ... continues until JOBP is terminal

---

## 3. WORKER THREAD PROCESSING (4 threads processing queue items)

```
worker() thread (infinite loop):
  WHILE True:
    (workflow, execution) = job_queue.GET()  // Blocks until item available
    run_id = execution["run_id"]
    object_type = execution.get("type") or execution.get("object_type")
    
    TRY:
      status = normalize_status(execution["status"])
      
      # APPEND to workflow_results (first write for this execution)
      append_csv("data/workflow_results.csv", {
        "run_id": run_id,
        "workflow_name": execution["name"],
        "status": status,
        "start_time": execution.get("start_time"),
        "end_time": execution.get("end_time")
      })
      
      IF object_type == "JOBS":
        # Single standalone job
        process_job(
          {"details": execution, "reports": {}},
          parent_run_id=execution.get("parent"),
          ai_client=ai_client
        )
      
      ELIF object_type == "JOBP":
        # Parent job with children - fetch and process each child
        children = AutomicClient.get_children(run_id)
        
        FOR EACH child in children:
          child_run_id = child["run_id"]
          
          # Check if child already processed in previous session
          processed_df = read_csv("data/processed_runs.csv")
          IF child_run_id IN processed_df["run_id"]:
            CONTINUE (already processed)
          
          # Fetch logs for this child
          reports = {}
          available_reports = AutomicClient.get_available_reports(child_run_id)
          
          FOR EACH report_type in available_reports:
            content = AutomicClient.get_job_logs(child_run_id, report_type)
            IF content:
              reports[report_type] = content
          
          # Process child job
          process_job(
            {"details": child, "reports": reports},
            parent_run_id=run_id,
            ai_client=ai_client
          )
          
          # Track child as processed
          append_csv("data/processed_runs.csv", {
            "run_id": child_run_id,
            "workflow_name": f"{execution['name']} (child)",
            "processed_timestamp": now()
          })
    
    FINALLY:
      job_queue.TASK_DONE()
```

---

## 4. PROCESS JOB (normalize and persist job details)

```
process_job(job, parent_run_id, ai_client):
  details = job["details"]
  reports = job["reports"]
  status_code = details["status"]
  status_text = STATUS_MAP[status_code]
  job_run_id = details["run_id"]
  
  # Generate AI summary if job ended with logs
  log_summary = None
  IF status_text IN ("ENDED_OK", "ENDED_NOT_OK") AND ai_client AND reports:
    combined_logs = "\n\n".join(f"=== {rtype} ===\n{content}" for each report)
    
    TRY:
      log_summary = OpenAIClient.summarize_logs(combined_logs)
      logger.info(f"Generated AI summary for run_id={job_run_id}")
    EXCEPT:
      logger.error(f"AI summarization failed")
  
  # APPEND job details
  append_csv("data/job_details.csv", {
    "job_run_id": job_run_id,
    "parent_run_id": parent_run_id,
    "status": status_text,
    "runtime": details["runtime"],
    "ai_log_summary": log_summary
  })
  
  # APPEND all logs for this job
  FOR EACH report_type, log_content in reports:
    IF log_content:
      append_csv("data/automic_logs.csv", {
        "job_run_id": job_run_id,
        "parent_run_id": parent_run_id,
        "report_type": report_type,
        "log_content": log_content
      })
```

---

## 5. CSV DATA FILES

### 5A. config_workflows.csv (INPUT - user configured)
```
workflow_name | object_type | is_active | last_polled_at
JobA          | JOBS        | 1         | 2026-04-29 10:00:00
WorkflowB     | JOBP        | 1         | 2026-04-29 10:00:00
TestWorkflow  | JOBP        | 0         | (blank)

Purpose: Define which workflows to monitor
Manually edited by user to add/remove workflows
```

### 5B. processed_runs.csv (OUTPUT - tracking)
```
run_id     | workflow_name        | processed_timestamp
86112815   | WorkflowB            | 2026-04-29 10:00:15
86113625   | WorkflowB (child)    | 2026-04-29 10:00:16
86113624   | WorkflowB (child)    | 2026-04-29 10:00:16
86112816   | WorkflowB            | 2026-04-29 10:05:20

Purpose: 
- Prevent reprocessing same execution in same session
- Prevent reprocessing on app restart
- Tracks: parent JOBP + all child jobs
```

### 5C. active_jobps.csv (OUTPUT - tracking in-progress parents)
```
run_id     | workflow_name | first_seen_at
86112815   | WorkflowB     | 2026-04-29 10:00:15
86112816   | WorkflowB     | 2026-04-29 10:05:20

Purpose: 
- Track parent jobs (JOBP) that are still running
- Removed when parent reaches terminal status
- Used to trigger re-polling every cycle to capture updated children
```

### 5D. workflow_results.csv (OUTPUT - status history)
```
run_id | workflow_name | status    | start_time            | end_time
86112815 | WorkflowB   | ACTIVE    | 2026-04-29 10:00:10   | (null)
86112815 | WorkflowB   | WAITING   | 2026-04-29 10:00:10   | (null)
86112815 | WorkflowB   | ACTIVE    | 2026-04-29 10:00:10   | (null)    ← Duplicate from _repoll_active_jobp()
86112815 | WorkflowB   | ENDED_OK  | 2026-04-29 10:00:10   | 2026-04-29 10:01:00

⚠️ PROBLEM: Same run_id written MULTIPLE times
  - First write: worker() thread when queued
  - Subsequent writes: _repoll_active_jobp() on EACH polling cycle
  - Result: Duplicates with same/updated status
```

### 5E. job_details.csv (OUTPUT - job details + AI summary)
```
job_run_id | parent_run_id | status    | runtime | ai_log_summary
86113625   | 86112815      | ENDED_OK  | 0.0     | {JSON summary}
86113624   | 86112815      | ENDED_OK  | 253.0   | {JSON summary}
86113623   | 86112815      | ACTIVE    | (null)  | (null)
86113623   | 86112815      | ENDED_OK  | 15.0    | {JSON summary}  ← Status changed

Purpose: Detailed job information with AI-generated summaries
Format:
  - Appended on every job status report
  - Latest status = most recent append for that job_run_id
  - Can have multiple entries per job_run_id (status progression)
```

### 5F. automic_logs.csv (OUTPUT - raw logs)
```
job_run_id | parent_run_id | report_type | log_content
86113625   | 86112815      | ACT         | [Activity log text]
86113625   | 86112815      | POST        | [Post execution log]
86113625   | 86112815      | REP         | [Report text]
86113624   | 86112815      | ACT         | [Activity log text]

Purpose: Store raw Automic logs for each job
Report types: ACT, POST, REP, OREP, LOG, PRCO
One row per report type per job
```

---

## 6. DUPLICATION PROBLEM IDENTIFIED

### Current Flow Problem:

```
Polling Cycle 1 (t=0):
  ├─ poller(): 
  │   └─ Fetch WorkflowB (JOBP), run_id=86112815, status=ACTIVE
  │   └─ Queue it → worker thread
  │   └─ add_active_jobp(86112815)
  │
  └─ worker(): 
      └─ append_csv("workflow_results.csv", {run_id=86112815, status=ACTIVE}) ✓

Polling Cycle 2 (t=60):
  ├─ poller():
  │   └─ Fetch WorkflowB again, run_id=86112815 (ALREADY IN seen_run_ids)
  │   └─ SKIP: don't queue again
  │   └─ But active_jobps still contains 86112815
  │   └─ _repoll_active_jobp():
  │       └─ fetch current status = WAITING
  │       └─ append_csv("workflow_results.csv", {run_id=86112815, status=WAITING})  ✗ DUPLICATE!

Polling Cycle 3 (t=120):
  └─ Same as Cycle 2:
      └─ _repoll_active_jobp() appends AGAIN: status=ACTIVE ✗ DUPLICATE!

... continues until ENDED_OK ...
```

### Why Duplicates Happen:

1. **Initial append**: When execution first queued and processed by worker
2. **Re-polling appends**: On every subsequent polling cycle, _repoll_active_jobp() appends SAME run_id
3. **Append mode**: New code uses append_csv() instead of upsert_csv()
4. **No deduplication**: Append mode means EVERY call writes to CSV

### Result:

```
One execution may appear 5-10 times in workflow_results.csv:
- 1 time from worker() thread (when queued)
- 4-9 times from _repoll_active_jobp() (one per polling cycle)

Total rows: 59 unique run_ids × ~7 appends each = 400+ rows
```

---

## 7. INTENDED BEHAVIOR (DESIGN INTENT)

The original design with `upsert_csv()` was:
```
- IF run_id not in CSV: INSERT new row
- IF run_id in CSV AND status changed: UPDATE that row
- IF run_id in CSV AND status unchanged: SKIP (no write)
```

This prevented duplicates because:
- Updating same run_id overwrote previous entry
- No status change = no write = no duplicates

---

## 8. CURRENT ISSUES WITH APPEND MODE

1. **Loss of deduplication logic**: Switched from upsert to append, but didn't provide alternative dedup
2. **Re-polling adds duplicates**: _repoll_active_jobp() should update parent status, not append
3. **No tracking of what's written**: Can't distinguish between "status changed" and "just polled again"

---

## 9. SOLUTION OPTIONS

### Option A: Keep Append + Add Dedup Check
```
Before appending to workflow_results:
  - Check if last record for this run_id has same status
  - IF same status: SKIP append
  - IF different status: APPEND
  
Issue: Requires reading entire CSV, filtering, checking timestamp
Performance: Slow with large CSVs
```

### Option B: Revert to Upsert (Original Design)
```
Use upsert_csv() for workflow_results.csv:
  - Only updates when status changes
  - No duplicates
  - Maintains simple logic

Pros: Simple, efficient, proven to work
Cons: Less audit trail (overwrites old status)
```

### Option C: Separate Tables
```
- workflow_results.csv: Only latest status per run_id (upsert)
- workflow_history.csv: All status changes (append)

Pros: Latest fast lookup + audit trail
Cons: More complex schema
```

### Option D: Add Timestamp + Check Before Append
```
Modify append_csv to:
  1. Check if last row for this run_id is within 5 minutes
  2. If same status within 5 min: SKIP
  3. If different status OR > 5 min: APPEND
  
Pros: Prevents rapid duplicates while allowing history
Cons: Arbitrary 5-min threshold
```

---

## SUMMARY

```
                    ┌─────────────────────────┐
                    │   POLLING LOOP (60s)    │
                    └────────────┬────────────┘
                                 ↓
                ┌────────────────────────────────────┐
                │  1. Load workflows & processed_ids │
                │  2. Fetch new executions           │
                │  3. Queue non-processed runs       │
                │  4. Re-poll active JOBP parents    │ ← CAUSES DUPLICATES HERE
                └────────────────────────────────────┘
                                 ↓
                    ┌────────────────────────┐
                    │  WORKER THREADS (4)    │
                    └────────────┬───────────┘
                                 ↓
                    ┌────────────────────────┐
                    │  1. Append to results  │
                    │  2. Fetch logs         │
                    │  3. Summarize with AI  │
                    │  4. Append to details  │
                    └────────────────────────┘
```
