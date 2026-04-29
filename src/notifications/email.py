"""SMTP email notifier for Automic job alerts."""
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.config import EmailConfig

logger = logging.getLogger(__name__)


class EmailNotifier:
    def __init__(self, config: EmailConfig) -> None:
        self.config = config

    def send_job_alert(
        self,
        job_run_id: str,
        status: str,
        ai_log_summary: str,
        recipient: str,
        workflow_name: str = "",
    ) -> None:
        subject = f"Automic Job Alert — {workflow_name or job_run_id} [{status}]"
        body = self._build_body(job_run_id, status, workflow_name, ai_log_summary)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.config.smtp_from
        msg["To"] = recipient
        msg.attach(MIMEText(body, "plain"))

        logger.info(f"Sending alert for job_run_id={job_run_id} to {recipient}")
        with smtplib.SMTP(
            host=self.config.smtp_host,
            port=self.config.smtp_port,
            timeout=self.config.smtp_timeout,
        ) as server:
            server.sendmail(self.config.smtp_from, recipient, msg.as_string())
        logger.info(f"Alert sent for job_run_id={job_run_id}")

    @staticmethod
    def _build_body(
        job_run_id: str, status: str, workflow_name: str, ai_log_summary: str
    ) -> str:
        lines = [
            "Automic Job Execution Alert",
            "=" * 40,
            f"Job Run ID  : {job_run_id}",
            f"Workflow    : {workflow_name or 'N/A'}",
            f"Status      : {status}",
            "",
            "--- AI Log Summary ---",
            ai_log_summary or "(no summary available)",
        ]
        return "\n".join(lines)
