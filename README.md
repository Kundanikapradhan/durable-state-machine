# Durable State Machine Workflow 

[![CI](https://github.com/Kundanikapradhan/durable-state-machine/actions/workflows/ci.yml/badge.svg)](https://github.com/Kundanikapradhan/durable-state-machine/actions)
[![Python](https://img.shields.io/badge/Python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Architecture: DAG](https://img.shields.io/badge/Architecture-State%20Machine%20DAG-orange.svg)](#architecture-overview)
[![Fault Tolerance: ACID WAL](https://img.shields.io/badge/Checkpoints-SQLite%20WAL-success.svg)](#-key-highlights)
[![Reliability: DLQ](https://img.shields.io/badge/Fault%20Tolerance-Dead%20Letter%20Queue-red.svg)](#%EF%B8%8F-dead-letter-queue-dlq--poison-pill-isolation)

A production-grade, fault-tolerant workflow orchestrator designed for multi-hour data migration pipelines.

Built to solve the critical flaw of naive `while True` scripts: **unhandled process termination (Kubernetes OOMKills, Spot instance terminations, rolling deploys, transient network drops) destroying in-memory state and resetting long-running jobs back to zero.**

---

## Architecture Overview

```mermaid
flowchart TD
    Start([Start Migration Run: 1,000,000 Rows]) --> Step1[Step 1: Validate Prerequisites]
    Step1 --> CP1[(Checkpoint 1: DB WAL)]
    CP1 --> Step2[Step 2: Partition 1M Rows into Chunks]
    Step2 --> CP2[(Checkpoint 2: DB WAL)]
    CP2 --> Step3[Step 3: Migrate Batches 1 to 3]
    Step3 --> CP3[(Checkpoint 3: DB WAL)]
    
    subgraph DangerZone [Simulated 2-Hour Point: Process Crash, Retries & Poison Pills]
        CP3 --> Step4[Step 4: Migrate Batches 4 to 6]
        Step4 -.->|Transient Drop| Backoff[Exponential Backoff with Full Jitter]
        Backoff -.-> Step4
        Step4 -.->|Unrecoverable Poison Pill / Max Retries Exceeded| DLQ[(🚨 Dead Letter Queue Table)]
        Step4 -.->|Process Killed SIGKILL 137| Crash([☠️ Process Terminated])
    end

    Crash --> Reboot([Reboot / Worker Restart])
    Reboot --> Inspect{Inspect Checkpoint Store}
    Inspect -->|Skip Completed Batches 1-3| Step4
    
    Step4 --> CP4[(Checkpoint 4: DB WAL)]
    CP4 --> Step5[Step 5: Migrate Batches 7 to 10]
    Step5 --> CP5[(Checkpoint 5: DB WAL)]
    CP5 --> Step6[Step 6: Data Parity & SHA-256 Checksum]
    Step6 --> CP6[(Checkpoint 6: DB WAL)]
    CP6 --> Step7[Step 7: Cutover & Notification]
    Step7 --> Done([Migration Complete 🚀])

    style DangerZone fill:#fff3cd,stroke:#ffeeba
    style Crash fill:#f8d7da,stroke:#f5c6cb
    style DLQ fill:#f8d7da,stroke:#dc3545,stroke-width:2px
    style Done fill:#d4edda,stroke:#c3e6cb
```

---

## 💡 Key Highlights

- **Durable Checkpointing**: Atomically snapshots state after every DAG node using SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`).
- **Crash Recovery**: If the OS process dies (`os._exit(137)`), restarting the workflow skips all committed steps and resumes the unfinished task from disk.
- **Dead Letter Queue (DLQ)**: Isolates unrecoverable "poison pill" records after max retry attempts, preserving full payloads and stack traces in a dedicated SQLite table for telemetry and audit.
- **Non-Fatal Pipeline Bypasses**: Allows optional enrichment steps to quarantine to DLQ without halting critical downstream migration tasks.
- **Exponential Backoff with Full Jitter**: Protects failing downstream databases from the *Thundering Herd* problem during retries:
  $$\text{Delay} = \text{Uniform}(0, \min(\text{MaxInterval}, \text{Initial} \times \text{Factor}^{\text{Attempt}}))$$
- **Step Idempotency**: Each chunk execution is keyed with deterministic partition tokens so re-running after network failure never produces duplicate rows.
- **Zero Dependencies for Core Engine**: Standard library Python (`sqlite3`, `json`, `dataclasses`, `time`).
- **Production Framework Adapters**: Includes reference implementations for both **LangGraph** (`StateGraph`, `SqliteSaver`) and **Temporal** (`@workflow.defn`, `@activity.defn`, Heartbeats).

---

## Dead Letter Queue (DLQ) & Poison Pill Isolation

In production data pipelines, **retries alone are not enough**. If record `#9421` contains invalid UTF-8 or a corrupted foreign key, retrying 5 times will simply fail 5 times, blocking the entire 4-hour migration forever.

This engine introduces an enterprise **Dead Letter Queue (DLQ)** pattern:

### 1. SQLite DLQ Schema
```sql
CREATE TABLE dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    failed_payload_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Inspecting Quarantined Tasks
Engineers can query the DLQ directly in Python or via SQL to inspect why a task failed:
```python
from core.engine import CheckpointStore

store = CheckpointStore("migration_checkpoints.db")
poison_records = store.get_dlq_records(workflow_id="MIGRATION-RUN-01")

for item in poison_records:
    print(f"Quarantined Step: {item['step_name']}")
    print(f"Error: {item['error_message']}")
    print(f"Failed Payload: {item['failed_payload']}")
    print(f"Traceback:\n{item['stack_trace']}")
```

### 3. Non-Fatal Step Bypass
For optional pipeline stages (e.g. non-critical customer demographic enrichment), steps can be flagged as `dlq_non_fatal=True`. The bad record is safely recorded in the DLQ, and downstream stages continue unimpeded!

---

## Quickstart

Clone the repo and run the automated crash-and-recovery test:

```bash
git clone https://github.com/Kundanikapradhan/durable-state-machine.git
cd durable-state-machine

# Run the crash and recovery demonstration
python3 demo_crash_and_recover.py
```

### Live Test Output
```text
======================================================================
 🚀 DURABLE STATE MACHINE WORKFLOW DEMO: CRASH & RECOVERY TEST
======================================================================

[PHASE A] Starting fresh workflow with simulated fatal crash trigger...
[Step 1] Validating source and target databases...
  -> Schema verification: PASSED
[Step 2] Calculating data volume and partitioning into chunks...
  -> Total records to migrate: 1,000,000
[Step 3] Processing Batches 1 to 3 (Records 1 - 300,000)...
     [Batch 1 Committed] Cumulative: 100,000 records.
     [Batch 2 Committed] Cumulative: 200,000 records.
     [Batch 3 Committed] Cumulative: 300,000 records.
[Step 4] Processing Batches 4 to 6 (Records 300,001 - 600,000)...
  💥 [Simulated Transient Network Drop] Target DB connection reset!
  ⏳ Backing off for 0.17s before retry #2...
💀 [SIMULATED DISASTER] Process crashed / killed mid-migration during Phase 2!
🛑 Process 1 Exit Code: 137 (SIGKILL / OOM)

======================================================================
 🔄 [PHASE B] SIMULATING SERVER REBOOT & WORKER RESUMPTION
======================================================================
🔁 [Crash Recovery] Found existing run! Resuming from state after 'migrate_phase_1'.
⏩ Skipping 3 already completed steps: ['partition_and_plan', 'validate_prerequisites', 'migrate_phase_1']
▶️  [Executing Step] --> 'migrate_phase_2'
  -> Migrating Batch 4 (IDs 300,001 - 400,000)...
  -> Migrating Batch 5 (IDs 400,001 - 500,000)...
  -> Migrating Batch 6 (IDs 500,001 - 600,000)...
...
[Step 6] Running Data Parity & Checksum Verification...
  -> SHA-256 Hash Parity: MATCHED (Zero data loss)
======================================================================
🎯 TEST SUCCESSFUL! Complete crash recovery achieved!
======================================================================
```

---

## Project Structure

```text
durable-state-machine/
├── .github/workflows/
│   └── ci.yml                 # Automated CI running tests across Python 3.9 - 3.12
├── core/
│   └── engine.py              # Zero-dependency durable workflow engine (ACID Checkpointing, Backoff, DLQ)
├── migration_workflow.py      # 4-Hour data migration DAG (1M records, 10 partitions)
├── demo_crash_and_recover.py  # Interactive crash & recovery simulation script
├── langgraph_workflow.py      # Production LangGraph implementation (StateGraph, SqliteSaver)
├── temporal_workflow.py       # Production Temporal implementation (Activities, Heartbeats)
├── tests/
│   └── test_engine.py         # Unit test suite verifying backoff, jitter, recovery & DLQ
├── requirements.txt           # Optional dependencies for LangGraph/Temporal adapters
├── LICENSE                    # MIT License
└── README.md                  # Documentation, Architecture & DLQ Guide
```

---

## Running Unit Tests

Run the full suite covering checkpoint serialization, jitter bounds, crash-skipping, and DLQ quarantine:

```bash
python3 -m unittest discover tests
```

---

## Framework Comparison: Temporal vs. LangGraph

| Feature | Temporal | LangGraph |
| :--- | :--- | :--- |
| **Primary Domain** | Distributed Microservices & Critical Infrastructure | LLM Agents, Multi-turn RAG, Tool Use Chains |
| **Durable Execution** | Event Sourcing & History Replay engine | Checkpointer snapshots (Postgres / SQLite) |
| **Crash Recovery** | Automatic worker reassignment + Replay | Thread-ID checkpoint resumption |
| **Failure Detection** | Activity Heartbeats with sub-minute timeouts | Node-level retry policies & exception handlers |
| **Poison Pill Handling** | ApplicationError with non-retryable flags | Exception routing with checkpointer branches |
| **Hosting** | Temporal Cloud or Self-hosted Temporal Server (Go) | Embedded Python library or LangGraph Cloud |

---

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
