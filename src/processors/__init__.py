"""Job and workflow processors."""
import logging

from src.notifications import maybe_send_alert
from src.utils import append_csv, parse_job_log, read_csv

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


def _fallback_summary(status_text: str, combined_log: str) -> str:
    """Build a deterministic summary when AI is unavailable."""
    if not combined_log.strip():
        return f"{status_text}: No log content extracted from Automic reports."

    parsed = parse_job_log(combined_log)
    return_code = parsed.get("return_code")
    errors = parsed.get("errors", [])
    transfer = parsed.get("transfer_details", {})

    parts = [f"{status_text}: terminal execution processed."]
    if return_code is not None:
        parts.append(f"RET={return_code}.")
    if transfer.get("count") is not None:
        parts.append(
            f"Transfer {transfer.get('command', 'operation')} count={transfer['count']}."
        )
    if errors:
        parts.append(f"Detected {len(errors)} error signal(s) in logs.")
    else:
        parts.append("No known error patterns detected in extracted logs.")

    return " ".join(parts)


def _build_combined_log_from_csv(run_id: str) -> str:
    """Build combined log for a run using persisted automic_logs.csv rows."""
    logs_df = read_csv(
        "data/automic_logs.csv",
        columns=["job_run_id", "report_type", "log_content"],
        dtypes={"job_run_id": str, "report_type": str, "log_content": str},
    )
    if logs_df.empty:
        return ""

    job_logs = logs_df[logs_df["job_run_id"].astype(str) == run_id]
    if job_logs.empty:
        return ""

    parts: list[str] = []
    for _, row in job_logs.iterrows():
        report_type = str(row.get("report_type", "") or "").strip()
        log_content = str(row.get("log_content", "") or "").strip()
        if not log_content:
            continue
        if report_type:
            parts.append(f"=== {report_type} ===\n{log_content}")
        else:
            parts.append(log_content)
    return "\n\n".join(parts)


def process_job(
    job: dict,
    parent_run_id: int | str,
    combined_log: str = "",
    report_logs: list[dict[str, str]] | None = None,
    ai_client=None,
) -> None:
    details = job.get("details", {})
    run_id = str(details.get("run_id", ""))
    job_name = details.get("name", run_id)
    raw_status = details.get("status")
    try:
        status_code = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status_code = None
    status_text = normalize_status(status_code, details.get("status_text"))
    ai_summary = None

    if status_text in TERMINAL_STATUSES:
        for report in report_logs or []:
            append_csv("data/automic_logs.csv", {
                "job_run_id": run_id,
                "parent_run_id": parent_run_id,
                "report_type": report.get("report_type", ""),
                "log_content": report.get("log_content", ""),
            })

    combined_log_for_ai = ""
    if status_text in TERMINAL_STATUSES:
        combined_log_for_ai = _build_combined_log_from_csv(run_id)

    if status_text in TERMINAL_STATUSES and ai_client and combined_log_for_ai.strip():
        ai_summary = ai_client.summarize(combined_log_for_ai)
        logger.info(f"AI summary for run_id={run_id}: {ai_summary[:80]}...")
        maybe_send_alert(job_name, run_id, status_text, ai_summary)
    elif status_text in TERMINAL_STATUSES:
        ai_summary = _fallback_summary(status_text, combined_log_for_ai)
        logger.info(
            "Skipping AI summary for run_id=%s (ai_client=%s, has_log=%s)",
            run_id,
            bool(ai_client),
            bool(combined_log_for_ai.strip()),
        )
        if status_text == "ENDED_NOT_OK":
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
