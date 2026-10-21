"""
core/engine.py
A zero-dependency, pure-Python Durable State Machine Engine.

Built to mirror Temporal and LangGraph's core primitives:
1. Deterministic State Transitions (DAG Nodes & Edges)
2. Durable Checkpointing (SQLite WAL mode)
3. Exponential Backoff with Full Jitter
4. Crash Recovery (Resumes exact state from disk without re-executing finished tasks)
5. Idempotent Step Execution
"""

import sqlite3
import json
import time
import random
import logging
from typing import Callable, Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DurableEngine")


@dataclass
class RetryPolicy:
    """Configures retry behavior for transient errors."""
    max_attempts: int = 5
    initial_interval: float = 1.0  # seconds
    backoff_factor: float = 2.0
    max_interval: float = 30.0
    jitter: bool = True

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
    Guarantees ACID transactions for state commits.
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
                    status TEXT NOT NULL, -- PENDING, COMPLETED, FAILED
                    attempt_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (workflow_id, step_name)
                );
            """)
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
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO checkpoints (workflow_id, step_name, state_json, status, attempt_count, error_message, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(workflow_id, step_name) DO UPDATE SET
                    state_json = excluded.state_json,
                    status = excluded.status,
                    attempt_count = excluded.attempt_count,
                    error_message = excluded.error_message,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                workflow_id,
                step_name,
                json.dumps(state),
                status,
                attempt_count,
                error_message
            ))
            conn.execute("""
                INSERT INTO event_log (workflow_id, step_name, event_type, payload_json)
                VALUES (?, ?, ?, ?);
            """, (workflow_id, step_name, f"STEP_{status}", json.dumps({"attempts": attempt_count, "error": error_message})))
            conn.commit()

    def load_latest_state(self, workflow_id: str) -> Tuple[Optional[str], Dict[str, Any], List[str]]:
        """
        Retrieves the latest completed state and list of finished steps for crash recovery.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT step_name, state_json, status FROM checkpoints
                WHERE workflow_id = ? AND status = 'COMPLETED'
                ORDER BY updated_at ASC;
            """, (workflow_id,))
            rows = cursor.fetchall()

            if not rows:
                return None, {}, []

            completed_steps = [r[0] for r in rows]
            latest_state = json.loads(rows[-1][1])
            latest_step = rows[-1][0]
            return latest_step, latest_state, completed_steps

    def clear(self, workflow_id: str):
        """Cleans up checkpoint history (e.g. for testing)."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM checkpoints WHERE workflow_id = ?;", (workflow_id,))
            conn.execute("DELETE FROM event_log WHERE workflow_id = ?;", (workflow_id,))
            conn.commit()


@dataclass
class WorkflowStep:
    name: str
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


class DurableWorkflow:
    """
    A DAG runner with checkpoints and crash recovery.
    Modeled on Temporal Activity Execution and LangGraph StateGraph.
    """
    def __init__(self, name: str, checkpointer: Optional[CheckpointStore] = None):
        self.name = name
        self.steps: Dict[str, WorkflowStep] = {}
        self.step_order: List[str] = []
        self.checkpointer = checkpointer or CheckpointStore()

    def add_step(
        self,
        name: str,
        handler: Callable[[Dict[str, Any]], Dict[str, Any]],
        retry_policy: Optional[RetryPolicy] = None
    ):
        if name in self.steps:
            raise ValueError(f"Step '{name}' is already registered.")
        self.steps[name] = WorkflowStep(
            name=name,
            handler=handler,
            retry_policy=retry_policy or RetryPolicy()
        )
        self.step_order.append(name)

    def execute(self, workflow_id: str, initial_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the workflow deterministically.
        If interrupted, resuming with the same workflow_id continues from the last checkpoint.
        """
        logger.info(f"=== Starting / Resuming Workflow '{self.name}' (ID: {workflow_id}) ===")

        # 1. Crash Recovery check
        latest_step, persisted_state, completed_steps = self.checkpointer.load_latest_state(workflow_id)

        if completed_steps:
            logger.info(f"🔁 [Crash Recovery] Found existing run! Resuming from state after '{latest_step}'.")
            logger.info(f"⏩ Skipping {len(completed_steps)} already completed steps: {completed_steps}")
            state = persisted_state
        else:
            logger.info("✨ Fresh run detected. Initializing state.")
            state = initial_state

        # 2. Iterate through DAG steps
        for step_name in self.step_order:
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

                except Exception as ex:
                    logger.warning(f"⚠️  [Error in Step '{step_name}'] Attempt {attempt}/{policy.max_attempts}: {str(ex)}")

                    if attempt >= policy.max_attempts:
                        self.checkpointer.save_checkpoint(
                            workflow_id=workflow_id,
                            step_name=step_name,
                            state=state,
                            status="FAILED",
                            attempt_count=attempt,
                            error_message=str(ex)
                        )
                        logger.critical(f"❌ [Max Retries Exceeded] Step '{step_name}' failed permanently.")
                        raise RuntimeError(f"Workflow '{workflow_id}' failed at step '{step_name}': {ex}") from ex

                    delay = policy.compute_delay(attempt)
                    logger.info(f"⏳ Backing off for {delay:.2f}s before retry #{attempt + 1}...")
                    time.sleep(delay)

        logger.info(f"🎉 === Workflow '{self.name}' (ID: {workflow_id}) COMPLETED SUCCESSFULLY ===")
        return state
