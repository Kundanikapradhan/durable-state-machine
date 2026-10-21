"""
migration_workflow.py
A 4-Hour Data Migration State Machine Workflow.

Models a real-world enterprise database migration:
- 1,000,000 customer records migrating from Legacy MySQL to Distributed Vector DB / Data Lake.
- Each stage updates the shared state.
- If a server crashes or process is killed mid-flight, state is retrieved from SQLite WAL checkpoint.
"""

import os
import sys
import time
import random
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
    time.sleep(0.5)  # Simulate latency
    
    # Check connection health and target table readiness
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
    time.sleep(0.5)
    
    total_records = state.get("total_records", 1_000_000)
    batch_size = 100_000
    num_batches = total_records // batch_size
    
    batches = [
        {
            "batch_id": i + 1,
            "start_id": i * batch_size + 1,
            "end_id": (i + 1) * batch_size,
            "status": "QUEUED"
        }
        for i in range(num_batches)
    ]
    
    print(f"  -> Total records to migrate: {total_records:,}")
    print(f"  -> Created {num_batches} deterministic batches of {batch_size:,} records each.")
    
    return {
        "total_batches": num_batches,
        "batches": batches,
        "processed_records": 0,
        "completed_batch_ids": []
    }


# ==========================================
# STEP 3: Process Batches 1 to 3 (Initial Phase)
# ==========================================
def step_migrate_phase_1(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 3] Processing Batches 1 to 3 (Records 1 - 300,000)...")
    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in state["batches"][:3]:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
        
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.4)
        completed.append(b_id)
        processed += (b["end_id"] - b["start_id"] + 1)
        print(f"     [Batch {b_id} Committed] Cumulative: {processed:,} records.")

    return {
        "completed_batch_ids": completed,
        "processed_records": processed,
        "phase_1_done": True
    }


# ==========================================
# STEP 4: Process Batches 4 to 6 (Mid-Flight Phase with Network Glitch & Crash Simulation)
# ==========================================
def step_migrate_phase_2(state: Dict[str, Any]) -> Dict[str, Any]:
    global transient_network_failure_count
    print("\n[Step 4] Processing Batches 4 to 6 (Records 300,001 - 600,000)...")
    
    # 1. Simulate a transient network error that triggers Exponential Retry
    if transient_network_failure_count < 2:
        transient_network_failure_count += 1
        print(f"  💥 [Simulated Transient Network Drop] Target DB connection reset! (Simulated error #{transient_network_failure_count})")
        raise ConnectionResetError("Connection dropped by target host during bulk COPY.")

    # 2. Simulate a catastrophic server crash (e.g. OOM Kill or Spot Instance Termination)
    if os.path.exists(CRASH_TRIGGER_FILE):
        print("\n💀 [SIMULATED DISASTER] Process crashed / killed mid-migration during Phase 2!")
        print("   In an un-checkpointed `while True` loop, ALL progress would be LOST!")
        # Remove trigger so on reboot it doesn't loop crashing
        os.remove(CRASH_TRIGGER_FILE)
        # Flush stdout and simulate immediate process termination
        sys.stdout.flush()
        os._exit(137)  # 137 = SIGKILL / OOM Killed

    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in state["batches"][3:6]:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
            
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.4)
        completed.append(b_id)
        processed += (b["end_id"] - b["start_id"] + 1)
        print(f"     [Batch {b_id} Committed] Cumulative: {processed:,} records.")

    return {
        "completed_batch_ids": completed,
        "processed_records": processed,
        "phase_2_done": True
    }


# ==========================================
# STEP 5: Process Batches 7 to 10 (Final Phase)
# ==========================================
def step_migrate_phase_3(state: Dict[str, Any]) -> Dict[str, Any]:
    print("\n[Step 5] Processing Batches 7 to 10 (Records 600,001 - 1,000,000)...")
    completed = list(state.get("completed_batch_ids", []))
    processed = state.get("processed_records", 0)
    
    for b in state["batches"][6:]:
        b_id = b["batch_id"]
        if b_id in completed:
            print(f"  -> Batch {b_id} already migrated. Skipping.")
            continue
            
        print(f"  -> Migrating Batch {b_id} (IDs {b['start_id']:,} - {b['end_id']:,})...")
        time.sleep(0.4)
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
    time.sleep(0.5)
    
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
    time.sleep(0.3)
    
    print("  -> DNS / Traffic router updated to Point to Target DB.")
    print("  -> Migration locks released.")
    print("  -> Slack/PagerDuty notification sent: Migration Successful 🚀")
    
    return {
        "migration_completed": True,
        "completed_at": time.time()
    }


def build_migration_workflow(db_path: str = "migration_checkpoints.db") -> DurableWorkflow:
    """Builds the 4-hour migration DAG with checkpointing and retry policies."""
    checkpointer = CheckpointStore(db_path=db_path)
    wf = DurableWorkflow(name="4HourDataMigration", checkpointer=checkpointer)
    
    # 1. Validation (Quick retry)
    wf.add_step("validate_prerequisites", step_validate_prerequisites, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    # 2. Partitioning (Deterministic)
    wf.add_step("partition_and_plan", step_partition_and_plan, RetryPolicy(max_attempts=2, initial_interval=0.5))
    
    # 3. Phase 1 (Batches 1-3)
    wf.add_step("migrate_phase_1", step_migrate_phase_1, RetryPolicy(max_attempts=3, initial_interval=1.0))
    
    # 4. Phase 2 (Batches 4-6) - Exponential backoff with jitter
    wf.add_step("migrate_phase_2", step_migrate_phase_2, RetryPolicy(max_attempts=5, initial_interval=0.5, backoff_factor=2.0, jitter=True))
    
    # 5. Phase 3 (Batches 7-10)
    wf.add_step("migrate_phase_3", step_migrate_phase_3, RetryPolicy(max_attempts=3, initial_interval=1.0))
    
    # 6. Reconciliation
    wf.add_step("reconcile_and_verify", step_reconcile_and_verify, RetryPolicy(max_attempts=3, initial_interval=1.0))
    
    # 7. Cutover
    wf.add_step("cutover_and_notify", step_cutover_and_notify, RetryPolicy(max_attempts=3, initial_interval=0.5))
    
    return wf


if __name__ == "__main__":
    workflow_id = "MIGRATION-RUN-2026-PROD-01"
    wf = build_migration_workflow()
    
    initial_state = {
        "source_db": "mysql://prod-cluster:3306/customers",
        "target_db": "postgres://distributed-lake:5432/customers_v2",
        "total_records": 1_000_000
    }
    
    result = wf.execute(workflow_id, initial_state)
    print("\nResult summary:", result.get("migration_completed"))
