"""
migration_workflow.py
A 4-Hour Data Migration State Machine Workflow.

Models a real-world enterprise database migration:
- 1,000,000 customer records migrating from Legacy MySQL to Distributed Vector DB / Data Lake.
- Each stage updates the shared state.
- If a server crashes or process is killed mid-flight, state is retrieved from SQLite WAL checkpoint.
- CLI-enabled: Custom volumes, partition batch sizes, dry-run mode, and live status inspection.
"""

import os
import sys
import time
import argparse
from typing import Dict, Any
from core.engine import DurableWorkflow, RetryPolicy, CheckpointStore

# A control file used to trigger a simulated crash on the first run
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CRASH_TRIGGER_FILE = os.path.join(BASE_DIR, "simulate_crash_at_batch4.flag")

# Global failure toggle for transient network errors (recovers on retry)
transient_network_failure_count = 0


# ==========================================
# STEP 1: Validate Prerequisites
# ==========================================
def step_validate_prerequisites(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 1] Validating source and target databases...")
    time.sleep(0.3)
    
    source_db = state.get("source_db", "mysql://prod-cluster:3306/customers")
    target_db = state.get("target_db", "postgres://distributed-lake:5432/customers_v2")
    
    print(f"  -> Connected to Source: {source_db}")
    print(f"  -> Connected to Target: {target_db}")
    print("  -> Schema verification: PASSED (Types match, partitions ready)")
    
    return {
        "status": "VALIDATED",
        "validation_timestamp": time.time()
    }


# ==========================================
# STEP 2: Partition & Plan Work
# ==========================================
def step_partition_and_plan(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 2] Calculating data volume and partitioning into chunks...")
    time.sleep(0.3)
    
    total_records = state.get("total_records", 1_000_000)
    batch_size = state.get("batch_size", 100_000)
    num_batches = max(1, total_records // batch_size)
    
    batches = [
        {
            "batch_id": i + 1,
            "start_id": i * batch_size + 1,
            "end_id": min(total_records, (i + 1) * batch_size),
            "status": "QUEUED"
        }
        for i in range(num_batches)
    ]
    
    print(f"  -> Total records to migrate: {total_records:,}")
    print(f"  -> Created {num_batches} deterministic batches of up to {batch_size:,} records each.")
    
    return {
        "total_batches": num_batches,
        "batches": batches,
        "processed_records": 0,
        "completed_batch_ids": []
    }


# ==========================================
# STEP 3: Process Batches Phase 1 (First Third)
# ==========================================
def step_migrate_phase_1(state: Dict[str, Any]) -> Dict[str, Any]:
    batches = state["batches"]
    split_1 = max(1, len(batches) // 3)
    phase_batches = batches[:split_1]
    
    print(f"\n[Step 3] Processing Phase 1 ({len(phase_batches)} batches: Batches 1 to {split_1})...")
    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in phase_batches:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
        
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.2)
        completed.append(b_id)
        processed += (b["end_id"] - b["start_id"] + 1)
        print(f"     [Batch {b_id} Committed] Cumulative: {processed:,} records.")

    return {
        "completed_batch_ids": completed,
        "processed_records": processed,
        "phase_1_done": True
    }


# ==========================================
# STEP 4: Process Batches Phase 2 (Mid-Flight: Retries & Crash Simulation)
# ==========================================
def step_migrate_phase_2(state: Dict[str, Any]) -> Dict[str, Any]:
    global transient_network_failure_count
    batches = state["batches"]
    split_1 = max(1, len(batches) // 3)
    split_2 = max(split_1 + 1, (2 * len(batches)) // 3)
    phase_batches = batches[split_1:split_2]
    
    print(f"\n[Step 4] Processing Phase 2 ({len(phase_batches)} batches: Batches {split_1 + 1} to {split_2})...")
    
    # 1. Simulate a transient network error that triggers Exponential Retry
    if transient_network_failure_count < 2:
        transient_network_failure_count += 1
        print(f"  💥 [Simulated Transient Network Drop] Target DB connection reset! (Simulated error #{transient_network_failure_count})")
        raise ConnectionResetError("Connection dropped by target host during bulk COPY.")

    # 2. Simulate a catastrophic server crash (e.g. OOM Kill or Spot Instance Termination)
    if os.path.exists(CRASH_TRIGGER_FILE):
        print("\n💀 [SIMULATED DISASTER] Process crashed / killed mid-migration during Phase 2!")
        print("   In an un-checkpointed `while True` loop, ALL progress would be LOST!")
        os.remove(CRASH_TRIGGER_FILE)
        sys.stdout.flush()
        os._exit(137)  # 137 = SIGKILL / OOM Killed

    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in phase_batches:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
            
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.2)
        completed.append(b_id)
        processed += (b["end_id"] - b["start_id"] + 1)
        print(f"     [Batch {b_id} Committed] Cumulative: {processed:,} records.")

    return {
        "completed_batch_ids": completed,
        "processed_records": processed,
        "phase_2_done": True
    }


# ==========================================
# STEP 5: Process Batches Phase 3 (Final Third)
# ==========================================
def step_migrate_phase_3(state: Dict[str, Any]) -> Dict[str, Any]:
    batches = state["batches"]
    split_1 = max(1, len(batches) // 3)
    split_2 = max(split_1 + 1, (2 * len(batches)) // 3)
    phase_batches = batches[split_2:]
    
    print(f"\n[Step 5] Processing Phase 3 ({len(phase_batches)} batches: Batches {split_2 + 1} to {len(batches)})...")
    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in phase_batches:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
            
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.2)
        completed.append(b_id)
        processed += (b["end_id"] - b["start_id"] + 1)
        print(f"     [Batch {b_id} Committed] Cumulative: {processed:,} records.")

    return {
        "completed_batch_ids": completed,
        "processed_records": processed,
        "phase_3_done": True
    }


# ==========================================
# STEP 6: Reconcile & Verify Data Parity
# ==========================================
def step_reconcile_and_verify(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 6] Running Data Parity & Checksum Verification...")
    time.sleep(0.3)
    
    expected = state["total_records"]
    actual = state["processed_records"]
    
    if expected != actual:
        raise ValueError(f"Data Mismatch! Expected {expected:,} rows, but migrated {actual:,}.")
        
    print(f"  -> Source Row Count: {expected:,}")
    print(f"  -> Target Row Count: {actual:,}")
    print("  -> SHA-256 Hash Parity: MATCHED (Zero data loss)")
    
    return {
        "verified": True,
        "discrepancies": 0
    }


# ==========================================
# STEP 7: Cutover & Notify
# ==========================================
def step_cutover_and_notify(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 7] Finalizing Cutover and Sending Migration Telemetry...")
    time.sleep(0.2)
    
    print("  -> DNS / Traffic router updated to Point to Target DB.")
    print("  -> Migration locks released.")
    print("  -> Slack/PagerDuty notification sent: Migration Successful 🚀")
    
    return {
        "migration_completed": True,
        "completed_at": time.time()
    }


def build_migration_workflow(db_path: str = "migration_checkpoints.db", dry_run: bool = False) -> DurableWorkflow:
    """Builds the 4-hour migration DAG with checkpointing, DLQ, and retry policies."""
    checkpointer = CheckpointStore(db_path=db_path)
    wf = DurableWorkflow(name="4HourDataMigration", checkpointer=checkpointer)
    
    # 1. Validation (Quick retry)
    wf.add_step("validate_prerequisites", step_validate_prerequisites, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    # 2. Partitioning (Deterministic)
    wf.add_step("partition_and_plan", step_partition_and_plan, RetryPolicy(max_attempts=2, initial_interval=0.5))
    
    if dry_run:
        def step_dry_run_summary(state: Dict[str, Any]) -> Dict[str, Any]:
            print("\n🔍 [Dry-Run Complete] Databases validated and partition batches mapped. Zero records mutated.")
            return {"dry_run_verified": True}
        wf.add_step("dry_run_summary", step_dry_run_summary)
        return wf
        
    # 3. Phase 1 (Initial batches)
    wf.add_step("migrate_phase_1", step_migrate_phase_1, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    # 4. Phase 2 (Mid-flight with exponential backoff & jitter)
    wf.add_step("migrate_phase_2", step_migrate_phase_2, RetryPolicy(max_attempts=5, initial_interval=0.5, backoff_factor=2.0, jitter=True))
    
    # 5. Phase 3 (Final batches)
    wf.add_step("migrate_phase_3", step_migrate_phase_3, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    # 6. Reconciliation
    wf.add_step("reconcile_and_verify", step_reconcile_and_verify, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    # 7. Cutover
    wf.add_step("cutover_and_notify", step_cutover_and_notify, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    return wf


def print_workflow_status(db_path: str, workflow_id: str):
    """Prints a formatted terminal inspection dashboard of the workflow's state and DLQ."""
    checkpointer = CheckpointStore(db_path=db_path)
    latest_step, persisted_state, completed_steps = checkpointer.load_latest_state(workflow_id)
    dlq_records = checkpointer.get_dlq_records(workflow_id)
    
    print("=" * 65)
    print(f"📊 WORKFLOW EXECUTION STATUS: {workflow_id}")
    print("=" * 65)
    print(f"Checkpoint DB : {db_path}")
    print(f"Latest Step   : {latest_step or 'None (Not started or in-progress)'}")
    print(f"Steps Done    : {len(completed_steps)} completed")
    for idx, s in enumerate(completed_steps, 1):
        print(f"  [{idx}] {s} (COMPLETED)")
        
    print("\nState Snapshot:")
    if persisted_state:
        total = persisted_state.get("total_records", "N/A")
        processed = persisted_state.get("processed_records", 0)
        pct = (processed / total * 100) if isinstance(total, int) and total > 0 else 0.0
        print(f"  - Total Records     : {total:,}" if isinstance(total, int) else f"  - Total Records     : {total}")
        print(f"  - Processed Records : {processed:,} ({pct:.1f}%)" if isinstance(total, int) else f"  - Processed Records : {processed}")
        print(f"  - Batches Finished  : {len(persisted_state.get('completed_batch_ids', []))}")
    else:
        print("  - No active state committed.")
        
    print(f"\nDead Letter Queue (DLQ): {len(dlq_records)} quarantined records")
    for r in dlq_records:
        print(f"  🚨 Record #{r['id']} | Step: {r['step_name']} | Attempts: {r['attempt_count']}")
        print(f"     Error: {r['error_message']}")
        print(f"     Timestamp: {r['quarantined_at']}")
    print("=" * 65)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Durable 4-Hour Data Migration State Machine CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default="MIGRATION-RUN-2026-PROD-01",
        help="Unique correlation ID for the migration run (resumes if already exists)"
    )
    parser.add_argument(
        "--records",
        type=int,
        default=1_000_000,
        help="Total number of records to migrate"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Partition chunk size for batch processing"
    )
    parser.add_argument(
        "--source-db",
        type=str,
        default="mysql://prod-cluster:3306/customers",
        help="Source database connection URI"
    )
    parser.add_argument(
        "--target-db",
        type=str,
        default="postgres://distributed-lake:5432/customers_v2",
        help="Target database connection URI"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="migration_checkpoints.db",
        help="SQLite checkpoint database file path"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate databases and plan partitions without executing data writes"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset existing checkpoints and DLQ records for this workflow ID before starting"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Inspect current execution and DLQ status for the workflow ID and exit"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    checkpointer = CheckpointStore(db_path=args.db_path)
    
    if args.reset:
        print(f"🧹 Clearing checkpoints and DLQ records for workflow '{args.workflow_id}'...")
        checkpointer.clear(args.workflow_id)
        print("   Done.")

    if args.status:
        print_workflow_status(args.db_path, args.workflow_id)
        sys.exit(0)
        
    wf = build_migration_workflow(db_path=args.db_path, dry_run=args.dry_run)
    
    initial_state = {
        "workflow_id": args.workflow_id,
        "source_db": args.source_db,
        "target_db": args.target_db,
        "total_records": args.records,
        "batch_size": args.batch_size,
        "dry_run": args.dry_run
    }
    
    result = wf.execute(args.workflow_id, initial_state)
    if result.get("_graceful_shutdown"):
        print(f"\n[Shutdown] Workflow '{args.workflow_id}' safely paused at step '{result.get('_shutdown_at_step')}'. Exiting gracefully (0).")
        sys.exit(0)
    success = result.get("migration_completed") or result.get("dry_run_verified")
    print(f"\nExecution Finished: {'SUCCESS' if success else 'FAILED'}")
