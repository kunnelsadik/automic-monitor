"""Manual script to send Automic job alert emails.

Usage:
    # Send alerts for all jobs that have an ai_log_summary
    python send_alert.py

    # Send alert for a specific job run ID
    python send_alert.py --run-id 86108744

    # Override the recipient
    python send_alert.py --recipient someone@example.com
"""
import argparse
import logging
import sys

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from src.config import EmailConfig
from src.logger import setup_logging
from src.notifications import EmailNotifier

setup_logging()
logger = logging.getLogger(__name__)


def load_jobs() -> pd.DataFrame:
    jobs = pd.read_csv("data/job_details.csv")
    # Try to join workflow name from workflow_results.csv
    try:
        wf = pd.read_csv("data/workflow_results.csv")[["run_id", "workflow_name"]]
        wf["run_id"] = wf["run_id"].astype(str)
        jobs["job_run_id_str"] = jobs["job_run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        jobs = jobs.merge(wf, left_on="job_run_id_str", right_on="run_id", how="left")
    except Exception:
        jobs["workflow_name"] = ""
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Automic job alert emails")
    parser.add_argument("--run-id", help="Send alert for a specific job_run_id only")
    parser.add_argument("--recipient", help="Override the default email recipient")
    args = parser.parse_args()

    email_cfg = EmailConfig()
    notifier = EmailNotifier(config=email_cfg)
    recipient = args.recipient or email_cfg.default_recipient

    jobs = load_jobs()
    has_summary = jobs["ai_log_summary"].notna() & (jobs["ai_log_summary"].astype(str).str.strip() != "")

    if args.run_id:
        targets = jobs[jobs["job_run_id"].astype(str).str.replace(r"\.0$", "", regex=True) == str(args.run_id)]
        if targets.empty:
            logger.error(f"No job found with run_id={args.run_id}")
            sys.exit(1)
        targets = targets[has_summary]
        if targets.empty:
            logger.error(f"Job {args.run_id} has no ai_log_summary yet")
            sys.exit(1)
    else:
        targets = jobs[has_summary]
        if targets.empty:
            logger.info("No jobs with ai_log_summary found. Run ai_summary.py first.")
            return

    logger.info(f"Sending alerts for {len(targets)} job(s) to {recipient}")
    sent, failed = 0, 0

    for _, row in targets.iterrows():
        job_run_id = str(int(row["job_run_id"]))
        try:
            notifier.send_job_alert(
                job_run_id=job_run_id,
                status=row.get("status", ""),
                ai_log_summary=str(row["ai_log_summary"]),
                recipient=recipient,
                workflow_name=str(row.get("workflow_name", "") or ""),
            )
            sent += 1
        except Exception as e:
            logger.error(f"Failed to send alert for job_run_id={job_run_id}: {e}")
            failed += 1

    logger.info(f"Done. Sent={sent}, Failed={failed}")


if __name__ == "__main__":
    main()
