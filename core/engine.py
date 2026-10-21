"""
core/engine.py
A zero-dependency, pure-Python Durable State Machine Engine.

Built to mirror Temporal and LangGraph's core primitives:
1. Deterministic State Transitions (DAG Nodes & Edges)
2. Durable Checkpointing (SQLite WAL mode)
3. Exponential Backoff with Full Jitter
4. Crash Recovery (Resumes exact state from disk without re-executing finished tasks)
5. Idempotent Step Execution
6. Dead Letter Queue (DLQ) for Poison Pill Isolation
"""

import sqlite3
import json
import time
import random
import logging
import traceback
import signal
import sys
from typing import Callable, Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DurableEngine")


class DeadLetterQueueException(RuntimeError):
    """Raised when a task exceeds maximum retry attempts and is quarantined to the DLQ."""
    def __init__(self, workflow_id: str, step_name: str, attempts: int, original_error: str, dlq_id: Optional[int] = None):
        super().__init__(
            f"Workflow '{workflow_id}' step '{step_name}' exceeded {attempts} attempts and was quarantined to DLQ "
            f"(Record #{dlq_id}). Root cause: {original_error}"
        )
        self.workflow_id = workflow_id
        self.step_name = step_name
        self.attempts = attempts
        self.original_error = original_error
        self.dlq_id = dlq_id


@dataclass
class RetryPolicy:
    """Configures retry behavior for transient errors and poison pill quarantine."""
    max_attempts: int = 5
    initial_interval: float = 1.0  # seconds
    backoff_factor: float = 2.0
    max_interval: float = 30.0
    jitter: bool = True
    send_to_dlq: bool = True

    def compute_delay(self, attempt: int) -> float:
        """
        Calculates exponential backoff with full jitter.
        Formula: sleep = min(max_interval, initial_interval * (factor ** attempt))
        Full jitter randomly distributes retries between 0 and calculated sleep
        to prevent 'Thundering Herd' spikes on failing databases.
        """
        raw_delay = self.initial_interval * (self.backoff_factor ** (attempt - 1))
        capped_delay = min(self.max_interval, raw_delay)
        if self.jitter:
            return random.uniform(0, capped_delay)
        return capped_delay


class CheckpointStore:
    """
    Durable persistence layer backed by SQLite with Write-Ahead Logging (WAL).
    Guarantees ACID transactions for state commits and DLQ quarantine.
    """
    def __init__(self, db_path: str = "workflow_checkpoints.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        # Enable WAL mode for high concurrency and crash resilience
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    workflow_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    status TEXT NOT NULL, -- PENDING, COMPLETED, FAILED, QUARANTINED
                    attempt_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at_ts REAL DEFAULT 0.0,
                    PRIMARY KEY (workflow_id, step_name)
                );
            """)
            try:
                conn.execute("ALTER TABLE checkpoints ADD COLUMN updated_at_ts REAL DEFAULT 0.0;")
            except sqlite3.OperationalError:
                pass
            conn.execute("""
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    failed_payload_json TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL,
                    error_message TEXT NOT NULL,
                    stack_trace TEXT,
                    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

    def save_checkpoint(
        self,
        workflow_id: str,
        step_name: str,
        state: Dict[str, Any],
        status: str,
        attempt_count: int = 0,
        error_message: Optional[str] = None
    ):
        now_ts = time.time()
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO checkpoints (workflow_id, step_name, state_json, status, attempt_count, error_message, updated_at, updated_at_ts)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(workflow_id, step_name) DO UPDATE SET
                    state_json = excluded.state_json,
                    status = excluded.status,
                    attempt_count = excluded.attempt_count,
                    error_message = excluded.error_message,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_at_ts = excluded.updated_at_ts;
            """, (
                workflow_id,
                step_name,
                json.dumps(state),
                status,
                attempt_count,
                error_message,
                now_ts
            ))
            conn.execute("""
                INSERT INTO event_log (workflow_id, step_name, event_type, payload_json)
                VALUES (?, ?, ?, ?);
            """, (workflow_id, step_name, f"STEP_{status}", json.dumps({"attempts": attempt_count, "error": error_message})))
            conn.commit()

    def send_to_dlq(
        self,
        workflow_id: str,
        step_name: str,
        state: Dict[str, Any],
        attempt_count: int,
        error_message: str,
        stack_trace: Optional[str] = None
    ) -> int:
        """Stores a poisoned payload and stack trace in the Dead Letter Queue."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dead_letter_queue (workflow_id, step_name, failed_payload_json, attempt_count, error_message, stack_trace)
                VALUES (?, ?, ?, ?, ?, ?);
            """, (
                workflow_id,
                step_name,
                json.dumps(state),
                attempt_count,
                error_message,
                stack_trace
            ))
            conn.commit()
            return cursor.lastrowid

    def get_dlq_records(self, workflow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieves quarantined tasks from the Dead Letter Queue for inspection."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if workflow_id:
                cursor.execute("""
                    SELECT id, workflow_id, step_name, failed_payload_json, attempt_count, error_message, stack_trace, quarantined_at
                    FROM dead_letter_queue
                    WHERE workflow_id = ?
                    ORDER BY id DESC;
                """, (workflow_id,))
            else:
                cursor.execute("""
                    SELECT id, workflow_id, step_name, failed_payload_json, attempt_count, error_message, stack_trace, quarantined_at
                    FROM dead_letter_queue
                    ORDER BY id DESC;
                """)
            rows = cursor.fetchall()
            return [
                {
                    "id": r[0],
                    "workflow_id": r[1],
                    "step_name": r[2],
                    "failed_payload": json.loads(r[3]),
                    "attempt_count": r[4],
                    "error_message": r[5],
                    "stack_trace": r[6],
                    "quarantined_at": r[7]
                }
                for r in rows
            ]

    def load_latest_state(self, workflow_id: str, step_order: Optional[List[str]] = None) -> Tuple[Optional[str], Dict[str, Any], List[str]]:
        """
        Retrieves the latest completed state and list of finished steps for crash recovery.
        If step_order is provided, selects the latest state according to the DAG's topological sequence.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT step_name, state_json, status FROM checkpoints
                WHERE workflow_id = ? AND status = 'COMPLETED'
                ORDER BY updated_at_ts ASC, updated_at ASC;
            """, (workflow_id,))
            rows = cursor.fetchall()

            if not rows:
                return None, {}, []

            completed_steps = [r[0] for r in rows]
            step_to_state = {r[0]: json.loads(r[1]) for r in rows}

            if step_order:
                latest_step = None
                for s in step_order:
                    if s in step_to_state:
                        latest_step = s
                latest_state = step_to_state[latest_step] if latest_step else json.loads(rows[-1][1])
            else:
                latest_step = rows[-1][0]
                latest_state = json.loads(rows[-1][1])

            return latest_step, latest_state, completed_steps

    def clear(self, workflow_id: str):
        """Cleans up checkpoint history (e.g. for testing)."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM checkpoints WHERE workflow_id = ?;", (workflow_id,))
            conn.execute("DELETE FROM event_log WHERE workflow_id = ?;", (workflow_id,))
            conn.execute("DELETE FROM dead_letter_queue WHERE workflow_id = ?;", (workflow_id,))
            conn.commit()


@dataclass
class WorkflowStep:
    name: str
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    dlq_non_fatal: bool = False  # If True, quarantine to DLQ and allow downstream steps to continue


class DurableWorkflow:
    """
    A DAG runner with checkpoints, crash recovery, and Dead Letter Queue.
    Modeled on Temporal Activity Execution and LangGraph StateGraph.
    """
    def __init__(self, name: str, checkpointer: Optional[CheckpointStore] = None):
        self.name = name
        self.steps: Dict[str, WorkflowStep] = {}
        self.step_order: List[str] = []
        self.checkpointer = checkpointer or CheckpointStore()
        self._shutdown_requested = False
        self._register_signal_handlers()

    def _register_signal_handlers(self):
        """Registers OS signal listeners for Kubernetes SIGTERM (graceful pod termination) and SIGINT."""
        def _handle_signal(signum, frame):
            sig_name = "SIGTERM" if signum == signal.SIGTERM else ("SIGINT" if signum == signal.SIGINT else str(signum))
            logger.warning(
                f"🛑 [OS Signal {sig_name} Received] Container termination initiated by orchestrator. "
                "Allowing active step to commit checkpoint before graceful exit..."
            )
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGTERM, _handle_signal)
            signal.signal(signal.SIGINT, _handle_signal)
        except (ValueError, AttributeError):
            # Non-main thread or unsupported environment
            pass

    def add_step(
        self,
        name: str,
        handler: Callable[[Dict[str, Any]], Dict[str, Any]],
        retry_policy: Optional[RetryPolicy] = None,
        dlq_non_fatal: bool = False
    ):
        if name in self.steps:
            raise ValueError(f"Step '{name}' is already registered.")
        self.steps[name] = WorkflowStep(
            name=name,
            handler=handler,
            retry_policy=retry_policy or RetryPolicy(),
            dlq_non_fatal=dlq_non_fatal
        )
        self.step_order.append(name)

    def execute(self, workflow_id: str, initial_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the workflow deterministically.
        If interrupted, resuming with the same workflow_id continues from the last checkpoint.
        """
        logger.info(f"=== Starting / Resuming Workflow '{self.name}' (ID: {workflow_id}) ===")

        # 1. Crash Recovery check
        latest_step, persisted_state, completed_steps = self.checkpointer.load_latest_state(
            workflow_id, step_order=self.step_order
        )

        if completed_steps:
            logger.info(f"🔁 [Crash Recovery] Found existing run! Resuming from state after '{latest_step}'.")
            logger.info(f"⏩ Skipping {len(completed_steps)} already completed steps: {completed_steps}")
            state = persisted_state
        else:
            logger.info("✨ Fresh run detected. Initializing state.")
            state = initial_state

        # 2. Iterate through DAG steps
        for step_name in self.step_order:
            if self._shutdown_requested:
                logger.warning(
                    f"🛑 [SIGTERM Graceful Exit] Aborting start of step '{step_name}'. "
                    f"Workflow '{workflow_id}' paused cleanly for container restart."
                )
                state["_graceful_shutdown"] = True
                return state

            if step_name in completed_steps:
                logger.debug(f"Step '{step_name}' already completed. Skipping.")
                continue

            step = self.steps[step_name]
            policy = step.retry_policy
            attempt = 0
            step_succeeded = False

            logger.info(f"▶️  [Executing Step] --> '{step_name}'")

            while not step_succeeded:
                attempt += 1
                try:
                    # Execute node handler with current state
                    output_diff = step.handler(state)
                    if not isinstance(output_diff, dict):
                        raise TypeError(f"Step '{step_name}' handler must return a dict of state updates.")

                    # Merge state changes (Deterministic State Progression)
                    state.update(output_diff)

                    # Atomically persist checkpoint before continuing
                    self.checkpointer.save_checkpoint(
                        workflow_id=workflow_id,
                        step_name=step_name,
                        state=state,
                        status="COMPLETED",
                        attempt_count=attempt
                    )
                    step_succeeded = True
                    logger.info(f"✅ [Step Completed & Checkpointed] '{step_name}' (Attempt {attempt})")

                    if self._shutdown_requested:
                        logger.warning(
                            f"🛑 [SIGTERM Graceful Exit] Step '{step_name}' committed to SQLite WAL. "
                            f"Workflow '{workflow_id}' paused cleanly for container restart."
                        )
                        state["_graceful_shutdown"] = True
                        state["_shutdown_at_step"] = step_name
                        return state

                except Exception as ex:
                    logger.warning(f"⚠️  [Error in Step '{step_name}'] Attempt {attempt}/{policy.max_attempts}: {str(ex)}")

                    if attempt >= policy.max_attempts:
                        tb = traceback.format_exc()
                        dlq_id = None
                        status = "FAILED"

                        if policy.send_to_dlq:
                            dlq_id = self.checkpointer.send_to_dlq(
                                workflow_id=workflow_id,
                                step_name=step_name,
                                state=state,
                                attempt_count=attempt,
                                error_message=str(ex),
                                stack_trace=tb
                            )
                            status = "QUARANTINED"
                            logger.critical(
                                f"🚨 [Poison Pill Quarantined to DLQ #{dlq_id}] Step '{step_name}' "
                                f"failed permanently after {attempt} attempts."
                            )
                        else:
                            logger.critical(f"❌ [Max Retries Exceeded] Step '{step_name}' failed permanently.")

                        self.checkpointer.save_checkpoint(
                            workflow_id=workflow_id,
                            step_name=step_name,
                            state=state,
                            status=status,
                            attempt_count=attempt,
                            error_message=str(ex)
                        )

                        if step.dlq_non_fatal:
                            logger.warning(
                                f"⚠️ Step '{step_name}' marked non-fatal. Bypassing and continuing downstream workflow..."
                            )
                            state[f"{step_name}_quarantined"] = True
                            state[f"{step_name}_dlq_id"] = dlq_id
                            break
                        else:
                            raise DeadLetterQueueException(
                                workflow_id=workflow_id,
                                step_name=step_name,
                                attempts=attempt,
                                original_error=str(ex),
                                dlq_id=dlq_id
                            ) from ex

                    delay = policy.compute_delay(attempt)
                    logger.info(f"⏳ Backing off for {delay:.2f}s before retry #{attempt + 1}...")
                    time.sleep(delay)

        logger.info(f"🎉 === Workflow '{self.name}' (ID: {workflow_id}) COMPLETED SUCCESSFULLY ===")
        return state
