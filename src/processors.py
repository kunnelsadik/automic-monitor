from src.utils import append_csv
from src.utils import now

# STATUS_MAP = {
#     1800: "ENDED_NOT_OK",
#     1900: "ENDED_OK",
#     1700: "WAITING",
#     1560: "BLOCKED",
#     1801: "ABORTED"
# }

STATUS_MAP = {
    1900: "ENDED_OK",
    1800: "ENDED_NOT_OK",
    1801: "ABORTED",
    1700: "WAITING",
    1560: "BLOCKED",
    1550: "ACTIVE"
}

def normalize_status(status_code, status_text=None):
    return STATUS_MAP.get(status_code, status_text or "UNKNOWN")

def process_job(job, parent_run_id, ai_client=None):
    details = job.get("details", {})
    status = details.get("status")
    status_text = STATUS_MAP.get(status, "UNKNOWN")

    runtime = details.get("runtime")
    log_summary = None

    if status_text in ("ENDED_OK", "ENDED_NOT_OK") and ai_client:
        logs = job.get("reports", {}).get("ACT", "")
        log_summary = ai_client.summarize(logs)

    append_csv("job_details.csv", {
        "job_run_id": details.get("run_id"),
        "parent_run_id": parent_run_id,
        "status": status_text,
        "runtime": runtime,
        "ai_log_summary": log_summary
    })