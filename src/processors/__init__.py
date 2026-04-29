"""Job and workflow processors."""
import logging

from src.utils import append_csv

logger = logging.getLogger(__name__)

STATUS_MAP: dict[int, str] = {
    1900: "ENDED_OK",
    1800: "ENDED_NOT_OK",
    1801: "ABORTED",
    1700: "WAITING",
    1560: "BLOCKED",
    1550: "ACTIVE",
}

TERMINAL_STATUSES = {"ENDED_OK", "ENDED_NOT_OK", "ABORTED"}


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


def normalize_status(status_code: int, status_text: str | None = None) -> str:
    status = STATUS_MAP.get(status_code, status_text or "UNKNOWN")
    logger.debug(f"Normalized status {status_code} -> {status}")
    return status


def process_job(job: dict, parent_run_id: int | str, ai_client=None) -> None:
    """Process and persist job details. All records are appended to CSV.
    
    Queries will use ORDER BY or GROUP BY to get the latest status/details for each job_run_id.
    This provides an audit trail of all status changes and a simple, reliable append-only model.
    """
    details = job.get("details", {})
    reports = job.get("reports", {})
    status_code = details.get("status")
    status_text = STATUS_MAP.get(status_code, "UNKNOWN")
    job_run_id = details.get("run_id")

    log_summary = None
    if status_text in ("ENDED_OK", "ENDED_NOT_OK") and ai_client and reports:
        combined = "\n\n".join(
            f"=== {rtype} ===\n{content}"
            for rtype, content in reports.items()
            if content
        )
        if combined:
            try:
                log_summary = ai_client.summarize_logs(combined)
                logger.info(f"Generated AI summary for run_id={job_run_id}")
            except Exception as e:
                logger.error(f"AI summarization failed for run_id={job_run_id}: {e}")

    append_csv("data/job_details.csv", {
        "job_run_id": job_run_id,
        "parent_run_id": parent_run_id,
        "status": status_text,
        "runtime": details.get("runtime"),
        "ai_log_summary": log_summary,
    })

    for report_type, log_content in reports.items():
        if log_content:
            append_csv("data/automic_logs.csv", {
                "job_run_id": job_run_id,
                "parent_run_id": parent_run_id,
                "report_type": report_type,
                "log_content": log_content,
            })

    logger.info(f"Processed job run_id={job_run_id} status={status_text}")
