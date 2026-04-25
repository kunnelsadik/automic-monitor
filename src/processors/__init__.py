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


def normalize_status(status_code: int, status_text: str | None = None) -> str:
    status = STATUS_MAP.get(status_code, status_text or "UNKNOWN")
    logger.debug(f"Normalized status {status_code} -> {status}")
    return status


def process_job(job: dict, parent_run_id: int | str, ai_client=None) -> None:
    details = job.get("details", {})
    status_code = details.get("status")
    status_text = STATUS_MAP.get(status_code, "UNKNOWN")
    log_summary = None

    if status_text in ("ENDED_OK", "ENDED_NOT_OK") and ai_client:
        logs = job.get("reports", {}).get("ACT", "")
        log_summary = ai_client.summarize(logs)

    append_csv("data/job_details.csv", {
        "job_run_id": details.get("run_id"),
        "parent_run_id": parent_run_id,
        "status": status_text,
        "runtime": details.get("runtime"),
        "ai_log_summary": log_summary,
    })
    logger.info(f"Processed job run_id={details.get('run_id')} status={status_text}")
