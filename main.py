import time
import logging
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

from src.api_clients import AutomicClient
from src.processors import normalize_status, process_job
from src.utils import read_csv, append_csv, now

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

client = AutomicClient()
job_queue = Queue()


def poller():
    logger.info("Starting polling cycle")
    workflows = read_csv(
        "config_workflows.csv",
        ["workflow_name", "object_type", "is_active", "last_polled_at"]
    )

    processed = read_csv(
        "processed_runs.csv",
        ["run_id", "workflow_name", "processed_timestamp"]
    )

    seen_run_ids = set(processed["run_id"].astype(str))
    logger.info(f"Loaded {len(workflows)} workflows and {len(seen_run_ids)} processed run IDs")

    for _, wf in workflows.iterrows():
        if not wf["is_active"]:
            continue

        logger.info(f"Polling workflow: {wf['workflow_name']}")
        executions = client.get_latest_executions(wf["workflow_name"])

        for exec_row in executions:
            run_id = str(exec_row["run_id"])

            if run_id in seen_run_ids:
                continue

            logger.info(f"Adding new execution to queue: run_id={run_id}, workflow={wf['workflow_name']}")
            job_queue.put((wf, exec_row))

            append_csv("processed_runs.csv", {
                "run_id": run_id,
                "workflow_name": wf["workflow_name"],
                "processed_timestamp": now()
            })


def worker():
    logger.info("Worker thread started")
    while True:
        wf, exec_row = job_queue.get()
        run_id = exec_row["run_id"]
        logger.info(f"Processing execution: run_id={run_id}, workflow={exec_row['name']}")
        object_type = exec_row["type"]   # JOBS or JOBP

        status = normalize_status(
            exec_row["status"],
            exec_row.get("status_text")
        )

        # Persist workflow/job execution
        append_csv("workflow_results.csv", {
            "run_id": run_id,
            "workflow_name": exec_row["name"],
            "status": status,
            "start_time": exec_row.get("start_time"),
            "end_time": exec_row.get("end_time")
        })

        # JOBS → process directly
        if object_type == "JOBS":
            logger.info(f"Processing JOB: {exec_row['name']}")
            process_job(
                {"details": exec_row, "reports": {}},
                parent_run_id=exec_row.get("parent")
            )

        # JOBP → fetch children
        elif object_type == "JOBP":
            logger.info(f"Processing JOBP: {exec_row['name']}, fetching children")
            children = client.get_children(run_id)
            for child in children:
                logger.info(f"Processing child job: {child['details']['name']}")
                process_job(child, parent_run_id=run_id)

        job_queue.task_done()
        logger.info(f"Completed processing run_id={run_id}")

if __name__ == "__main__":
    logger.info("Starting Automic Monitor application")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(worker)
        logger.info("Submitted worker thread, starting polling loop")
        while True:
            poller()
            logger.info("Polling cycle completed, sleeping for 60 seconds")
            time.sleep(60)
