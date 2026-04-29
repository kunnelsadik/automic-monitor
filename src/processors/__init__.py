"""Job and workflow processors."""
import logging

from src.notifications import maybe_send_alert
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


def normalize_status(status_code: int, status_text: str | None = None) -> str:
    status = STATUS_MAP.get(status_code, status_text or "UNKNOWN")
    logger.debug(f"Normalized status {status_code} -> {status}")
    return status


def process_job(
    job: dict,
    parent_run_id: int | str,
    combined_log: str = "",
    ai_client=None,
) -> None:
    details = job.get("details", {})
    run_id = str(details.get("run_id", ""))
    job_name = details.get("name", run_id)
    status_code = details.get("status")
    status_text = STATUS_MAP.get(status_code, "UNKNOWN")
    ai_summary = None

    if status_text in TERMINAL_STATUSES and ai_client and combined_log.strip():
        ai_summary = ai_client.summarize(combined_log)
        logger.info(f"AI summary for run_id={run_id}: {ai_summary[:80]}...")
        maybe_send_alert(job_name, run_id, status_text, ai_summary)
    elif status_text == "ENDED_NOT_OK":
        maybe_send_alert(job_name, run_id, status_text, ai_summary or "")

    append_csv("data/job_details.csv", {
        "job_run_id": run_id,
        "parent_run_id": parent_run_id,
        "status": status_text,
        "runtime": details.get("runtime"),
        "ai_log_summary": ai_summary,
    })
    logger.info(f"Processed job run_id={run_id} status={status_text}")
