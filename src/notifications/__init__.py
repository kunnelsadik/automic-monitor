"""Email alert notifications via SMTP."""
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

_SMTP_HOST = os.getenv("ALERT_SMTP_HOST", "mail.healthpartners.org")
_SMTP_PORT = int(os.getenv("ALERT_SMTP_PORT", "25"))
_FROM = os.getenv("ALERT_FROM_EMAIL", "")
_TO = os.getenv("ALERT_TO_EMAIL", "")

_FAILURE_KEYWORDS = {"fail", "error", "not ok", "unsuccessful", "exception", "abort", "terminated", "critical"}


def _summary_indicates_failure(summary: str) -> bool:
    lower = summary.lower()
    return any(kw in lower for kw in _FAILURE_KEYWORDS)


def send_failure_alert(job_name: str, run_id: str, status: str, ai_summary: str) -> None:
    if not _FROM or not _TO:
        logger.warning(
            "Alert email skipped for run_id=%s: ALERT_FROM_EMAIL or ALERT_TO_EMAIL not configured",
            run_id,
        )
        return

    body = (
        f"Automic Job Execution Alert\n"
        f"{'=' * 40}\n"
        f"Job Name : {job_name}\n"
        f"Run ID   : {run_id}\n"
        f"Status   : {status}\n\n"
        f"AI Summary:\n{ai_summary}\n"
    )
    msg = MIMEMultipart()
    msg["From"] = _FROM
    msg["To"] = _TO
    msg["Subject"] = f"[Automic Alert] {job_name} — {status}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT) as server:
            server.sendmail(_FROM, _TO, msg.as_string())
        logger.info(f"Alert email sent for job={job_name} run_id={run_id}")
    except Exception as e:
        logger.error(f"Failed to send alert email for run_id={run_id}: {e}")


def maybe_send_alert(job_name: str, run_id: str, status: str, ai_summary: str) -> None:
    """Send an alert email for terminal outcomes and failure-like summaries."""
    if status in {"ENDED_OK", "ENDED_NOT_OK"} or _summary_indicates_failure(ai_summary or ""):
        send_failure_alert(job_name, run_id, status, ai_summary or "")
