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
from src.processors import is_terminal, normalize_status, process_job
from src.utils import add_active_jobp, append_csv, load_active_jobps, now, remove_active_jobp
from src.utils.csv_utils import read_csv

cfg = get_config()
setup_logging(log_level=cfg.app.log_level)
logger = logging.getLogger(__name__)

client = AutomicClient(config=cfg.automic)
try:
    ai_client = OpenAIClient(config=cfg.openai)
    logger.info("OpenAI client initialized")
except Exception:
    ai_client = None
    logger.warning("OpenAI client not initialized — OPENAI_API_KEY missing or invalid")
job_queue: Queue = Queue(maxsize=cfg.app.queue_size)


def _repoll_active_jobp(run_id: str, wf_name: str) -> None:
    """Re-fetch parent status for a tracked JOBP; remove when fully terminal.
    
    NOTE: Children are processed by the worker thread with full reports.
    This function only updates the parent workflow_results to track progress.
    """
    try:
        exec_details = client.get_execution_details(run_id)
    except Exception as e:
        logger.error(f"Failed to re-poll JOBP run_id={run_id}: {e}")
        return

    parent_status = normalize_status(exec_details.get("status"), exec_details.get("status_text"))
    append_csv("data/workflow_results.csv", {
        "run_id": run_id,
        "workflow_name": wf_name,
        "status": parent_status,
        "start_time": exec_details.get("start_time"),
        "end_time": exec_details.get("end_time"),
    })

    if is_terminal(parent_status):
        remove_active_jobp(run_id)
        logger.info(f"JOBP run_id={run_id} terminal, removed from active tracking")


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

            # For standalone jobs, wait until terminal before queueing — children are handled via active JOBP re-poll
            if wf["object_type"] == "JOBS" and exec_row.get("status") not in (1900, 1800, 1801):
                continue

            seen_run_ids.add(run_id)
            job_queue.put((wf, exec_row))
            append_csv("data/processed_runs.csv", {
                "run_id": run_id,
                "workflow_name": wf["workflow_name"],
                "processed_timestamp": now(),
            })
            # Track JOBP runs for child re-polling each cycle until fully terminal
            if wf["object_type"] == "JOBP":
                add_active_jobp(run_id, wf["workflow_name"])

    # Re-poll every known active JOBP to capture new/updated children each cycle
    active_jobps = load_active_jobps()
    if not active_jobps.empty:
        logger.info(f"Re-polling {len(active_jobps)} active JOBPs")
        for _, active in active_jobps.iterrows():
            _repoll_active_jobp(str(active["run_id"]), str(active["workflow_name"]))


def worker() -> None:
    logger.info("Worker thread started")
    while True:
        wf, exec_row = job_queue.get()
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
                    process_job(
                        {"details": exec_row, "reports": {}},
                        parent_run_id=exec_row.get("parent"),
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
                        child_run_id = child.get("run_id")
                        
                        # Skip if child already processed in a previous session
                        processed_df = read_csv("data/processed_runs.csv", columns=["run_id", "workflow_name", "processed_timestamp"])
                        if not processed_df.empty and str(child_run_id) in processed_df["run_id"].astype(str).values:
                            logger.debug(f"Skipping child run_id={child_run_id}: already processed")
                            continue
                        
                        reports = {}
                        try:
                            available = client.get_available_reports(child_run_id)
                            for r in available:
                                report_type = r["type"]
                                try:
                                    content = client.get_job_logs(child_run_id, report_type)
                                    if content:
                                        reports[report_type] = content
                                except Exception as e:
                                    logger.warning(f"Failed to fetch {report_type} log for run_id={child_run_id}: {e}")
                        except Exception as e:
                            logger.warning(f"Failed to list reports for run_id={child_run_id}: {e}")
                        try:
                            process_job({"details": child, "reports": reports}, parent_run_id=run_id, ai_client=ai_client)
                            # Track child job as processed to prevent re-processing on next app start
                            append_csv("data/processed_runs.csv", {
                                "run_id": str(child_run_id),
                                "workflow_name": f"{exec_row['name']} (child)",
                                "processed_timestamp": now(),
                            })
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
