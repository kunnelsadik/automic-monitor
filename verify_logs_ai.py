#!/usr/bin/env python
"""Verify that logging and AI summarization are working."""
import pandas as pd
from pathlib import Path

print("=" * 80)
print("AUTOMIC MONITOR - LOGGING & AI SUMMARIZATION VERIFICATION")
print("=" * 80)

# Check automic_logs.csv
logs_file = Path("data/automic_logs.csv")
if logs_file.exists():
    df_logs = pd.read_csv(logs_file)
    print(f"\n✅ automic_logs.csv")
    print(f"   Total rows: {len(df_logs)}")
    print(f"   Unique job_run_ids: {df_logs['job_run_id'].nunique()}")
    print(f"   Report types: {df_logs['report_type'].unique().tolist()}")
    print(f"\n   Sample entries:")
    for report_type in df_logs['report_type'].unique()[:3]:
        count = len(df_logs[df_logs['report_type'] == report_type])
        print(f"   - {report_type}: {count} records")
else:
    print(f"\n❌ automic_logs.csv not found")

# Check job_details.csv for AI summaries
details_file = Path("data/job_details.csv")
if details_file.exists():
    df_details = pd.read_csv(details_file)
    print(f"\n✅ job_details.csv")
    print(f"   Total rows: {len(df_details)}")
    print(f"   Unique job_run_ids: {df_details['job_run_id'].nunique()}")
    
    # Check for AI summaries
    ai_summaries = df_details[df_details['ai_log_summary'].notna()]
    if len(ai_summaries) > 0:
        print(f"\n   ✅ AI Log Summaries Found: {len(ai_summaries)} records")
        print(f"\n   Sample AI summary:")
        sample = ai_summaries.iloc[0]['ai_log_summary']
        if isinstance(sample, str) and len(sample) > 200:
            print(f"   {sample[:200]}...")
        else:
            print(f"   {sample}")
    else:
        print(f"\n   ⚠️  No AI summaries yet (may not have ENDED_OK/ENDED_NOT_OK jobs with logs)")
    
    # Check status distribution
    print(f"\n   Status distribution:")
    for status in df_details['status'].value_counts().index:
        count = len(df_details[df_details['status'] == status])
        print(f"   - {status}: {count} records")
else:
    print(f"\n❌ job_details.csv not found")

# Check if workflow_results.csv is growing (append mode)
results_file = Path("data/workflow_results.csv")
if results_file.exists():
    df_results = pd.read_csv(results_file)
    print(f"\n✅ workflow_results.csv")
    print(f"   Total rows: {len(df_results)}")
    print(f"   Unique run_ids: {df_results['run_id'].nunique()}")
    
    # Show how many duplicate run_ids exist (showing append mode is working)
    dupes = df_results.groupby('run_id').size()
    dupe_count = (dupes > 1).sum()
    if dupe_count > 0:
        print(f"   ✅ Append mode working: {dupe_count} run_ids have multiple status entries")
        multi_status = dupes[dupes > 1].value_counts()
        for count, freq in multi_status.items():
            print(f"      {freq} run_ids appear {count} times")
else:
    print(f"\n❌ workflow_results.csv not found")

print("\n" + "=" * 80)
print("CONFIGURATION SUMMARY")
print("=" * 80)

# Check if OpenAI is configured
try:
    from src.config import get_config
    cfg = get_config()
    print(f"\n✅ OpenAI Configuration:")
    print(f"   Model: {cfg.openai.model}")
    print(f"   API Key: {'✅ Configured' if cfg.openai.api_key else '❌ Missing'}")
    print(f"\n✅ Polling Configuration:")
    print(f"   Polling Interval: {cfg.app.polling_interval_seconds} seconds")
    print(f"   Worker Threads: {cfg.app.worker_threads}")
    print(f"   Queue Size: {cfg.app.queue_size}")
except Exception as e:
    print(f"❌ Error loading config: {e}")

print("\n" + "=" * 80)
print("HOW IT WORKS NOW (APPEND MODE)")
print("=" * 80)
print("""
1. DATA COLLECTION:
   - Logs: Every job execution fetches logs and appends to automic_logs.csv
   - Status: Every workflow execution appends status to workflow_results.csv
   - AI Summary: When job ends (OK/NOT_OK) with logs, OpenAI generates summary

2. LATEST STATUS:
   - Queries should use: GROUP BY run_id ORDER BY timestamp DESC
   - Or: SELECT DISTINCT ON (run_id) * FROM ... ORDER BY run_id, timestamp DESC
   - This shows the most recent status for each job

3. AUDIT TRAIL:
   - All historical status changes are preserved
   - Can track when/how status changed over time
   - No data loss or overwrites

4. DUPLICATE PROTECTION:
   - processed_runs.csv tracks run_ids processed in current session
   - Child jobs tracked to prevent reprocessing on app restart
   - Append mode + dedup tracking = no duplicates
""")

print("=" * 80)
