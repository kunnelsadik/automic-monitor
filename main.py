import logging
import time
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

load_dotenv()

from src.ai import OpenAIClient
from src.automic.client import AutomicClient
from src.config import get_config
from src.logger import setup_logging
from src.processors import TERMINAL_STATUSES, normalize_status, process_job
from src.utils import append_csv, now
from src.utils.csv_utils import read_csv

cfg = get_config()
setup_logging(log_level=cfg.app.log_level)
logger = logging.getLogger(__name__)

client = AutomicClient(config=cfg.automic)
job_queue: Queue = Queue(maxsize=cfg.app.queue_size)

try:
    ai_client = OpenAIClient()
    logger.info("OpenAI client initialized")
except Exception as e:
    ai_client = None
    logger.warning(f"OpenAI client unavailable — AI summaries disabled: {e}")


def _fetch_combined_logs(run_id: str) -> str:
    """Fetch all available report types for a run_id and combine into one string."""
    try:
        reports = client.get_available_reports(run_id)
        parts = []
        seen_types = set()
        for report in reports:
            rtype = (
                report.get("type")
                or report.get("report_type")
                or report.get("name")
                or report.get("id")
                or ""
            )
            if not rtype:
                continue
            rtype = str(rtype).strip().upper()
            if rtype in seen_types:
                continue
            seen_types.add(rtype)
            content = client.get_job_logs(run_id, rtype)
            if content:
                parts.append(f"=== {rtype} ===\n{content}")

        # Some Automic environments do not list report types reliably.
        # Try known report types as a fallback so terminal jobs still capture logs.
        if not parts:
            for fallback_type in ("REP", "ACT", "PLOG", "LOG"):
                if fallback_type in seen_types:
                    continue
                content = client.get_job_logs(run_id, fallback_type)
                if content:
                    parts.append(f"=== {fallback_type} ===\n{content}")
        return "\n\n".join(parts)
    except Exception as e:
        logger.error(f"Failed to fetch logs for run_id={run_id}: {e}")
        return ""


def poller() -> None:
    logger.info("Starting polling cycle")
    workflows = read_csv(
        "data/config_workflows.csv",
        ["workflow_name", "object_type", "is_active", "last_polled_at"],
    )
    processed = read_csv(
        "data/processed_runs.csv",
        ["run_id", "workflow_name", "processed_timestamp"],
    )
    seen_run_ids = set(processed["run_id"].astype(str))
    logger.info(f"Loaded {len(workflows)} workflows, {len(seen_run_ids)} processed run IDs")

    for _, wf in workflows.iterrows():
        if not wf["is_active"]:
            continue

        try:
            executions = client.get_latest_executions(wf["workflow_name"])
        except Exception as e:
            logger.error(f"Failed to fetch executions for {wf['workflow_name']}: {e}")
            continue

        for exec_row in executions:
            run_id = str(exec_row["run_id"])
            if run_id in seen_run_ids:
                continue

            seen_run_ids.add(run_id)
            job_queue.put((wf, exec_row))
            append_csv("data/processed_runs.csv", {
                "run_id": run_id,
                "workflow_name": wf["workflow_name"],
                "processed_timestamp": now(),
            })


def worker() -> None:
    logger.info("Worker thread started")
    while True:
        _, exec_row = job_queue.get()
        run_id = exec_row["run_id"]
        object_type = exec_row.get("type") or exec_row.get("object_type")

        try:
            status = normalize_status(exec_row["status"], exec_row.get("status_text"))
            append_csv("data/workflow_results.csv", {
                "run_id": run_id,
                "workflow_name": exec_row["name"],
                "status": status,
                "start_time": exec_row.get("start_time"),
                "end_time": exec_row.get("end_time"),
            })

            if object_type == "JOBS":
                try:
                    combined_log = ""
                    if status in TERMINAL_STATUSES:
                        combined_log = _fetch_combined_logs(str(run_id))
                    process_job(
                        {"details": exec_row, "reports": {}},
                        parent_run_id=exec_row.get("parent"),
                        combined_log=combined_log,
                        ai_client=ai_client,
                    )
                except Exception as e:
                    logger.error(f"Failed to process job {exec_row.get('name')}: {e}")

            elif object_type == "JOBP":
                try:
                    children = client.get_children(run_id)
                except Exception as e:
                    logger.error(f"Failed to fetch children for run_id={run_id}: {e}")
                else:
                    for child in children:
                        try:
                            child_run_id = str(child.get("run_id", ""))
                            child_status_code = child.get("status")
                            child_status = normalize_status(child_status_code, child.get("status_text"))
                            combined_log = ""
                            if child_status in TERMINAL_STATUSES and child_run_id:
                                combined_log = _fetch_combined_logs(child_run_id)
                            process_job(
                                {"details": child, "reports": {}},
                                parent_run_id=run_id,
                                combined_log=combined_log,
                                ai_client=ai_client,
                            )
                        except Exception as e:
                            name = child.get("name", "?")
                            logger.error(f"Failed to process child job {name}: {e}")

        finally:
            job_queue.task_done()
            logger.info(f"Completed run_id={run_id}")


if __name__ == "__main__":
    logger.info("Starting Automic Monitor")
    with ThreadPoolExecutor(max_workers=cfg.app.worker_threads) as executor:
        for _ in range(cfg.app.worker_threads):
            executor.submit(worker)
        logger.info(f"Started {cfg.app.worker_threads} worker threads, entering poll loop")
        while True:
            poller()
            logger.info(
                f"Polling cycle complete, sleeping {cfg.app.polling_interval_seconds}s"
            )
            time.sleep(cfg.app.polling_interval_seconds)
