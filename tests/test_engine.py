"""
tests/test_engine.py
Unit tests verifying the core mechanics of the durable state machine:
- Checkpoint persistence & retrieval
- Exponential backoff calculation & jitter bounds
- Skip completed steps on resume
- Dead Letter Queue (DLQ) quarantine for poison pills
- Non-fatal DLQ bypass for optional pipeline stages
"""

import unittest
import os
import tempfile
from core.engine import (
    CheckpointStore,
    RetryPolicy,
    DurableWorkflow,
    DeadLetterQueueException
)


class TestDurableEngine(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.store = CheckpointStore(db_path=self.temp_db.name)

    def tearDown(self):
        if os.path.exists(self.temp_db.name):
            os.remove(self.temp_db.name)

    def test_retry_policy_delay_computation(self):
        policy = RetryPolicy(
            max_attempts=4,
            initial_interval=1.0,
            backoff_factor=2.0,
            max_interval=10.0,
            jitter=False
        )
        self.assertEqual(policy.compute_delay(1), 1.0)
        self.assertEqual(policy.compute_delay(2), 2.0)
        self.assertEqual(policy.compute_delay(3), 4.0)
        self.assertEqual(policy.compute_delay(4), 8.0)
        self.assertEqual(policy.compute_delay(5), 10.0)  # Capped at max_interval

    def test_jitter_stays_within_bounds(self):
        policy = RetryPolicy(
            initial_interval=2.0,
            backoff_factor=2.0,
            jitter=True
        )
        for attempt in range(1, 5):
            delay = policy.compute_delay(attempt)
            max_expected = 2.0 * (2.0 ** (attempt - 1))
            self.assertGreaterEqual(delay, 0.0)
            self.assertLessEqual(delay, max_expected)

    def test_checkpoint_store_save_and_load(self):
        workflow_id = "wf-test-123"
        state = {"counter": 42, "items": ["a", "b"]}
        self.store.save_checkpoint(workflow_id, "step_1", state, status="COMPLETED", attempt_count=1)

        latest_step, loaded_state, completed_steps = self.store.load_latest_state(workflow_id)
        self.assertEqual(latest_step, "step_1")
        self.assertEqual(loaded_state["counter"], 42)
        self.assertEqual(loaded_state["items"], ["a", "b"])
        self.assertIn("step_1", completed_steps)

    def test_workflow_skips_completed_steps(self):
        workflow_id = "wf-skip-test"
        execution_count = {"step_a": 0, "step_b": 0}

        def handler_a(state):
            execution_count["step_a"] += 1
            return {"a_done": True}

        def handler_b(state):
            execution_count["step_b"] += 1
            return {"b_done": True}

        wf = DurableWorkflow("TestWorkflow", checkpointer=self.store)
        wf.add_step("step_a", handler_a)
        wf.add_step("step_b", handler_b)

        # Run first time -> Both execute
        wf.execute(workflow_id, {})
        self.assertEqual(execution_count["step_a"], 1)
        self.assertEqual(execution_count["step_b"], 1)

        # Run second time -> Zero executions because both are completed in checkpoint
        wf.execute(workflow_id, {})
        self.assertEqual(execution_count["step_a"], 1)
        self.assertEqual(execution_count["step_b"], 1)

    def test_dlq_quarantine_on_poison_pill(self):
        """Verifies that an unrecoverable failure is shunted to the Dead Letter Queue."""
        workflow_id = "wf-poison-test"

        def poison_handler(state):
            raise ValueError("Corrupt character encoding in record #9421")

        wf = DurableWorkflow("DLQWorkflow", checkpointer=self.store)
        wf.add_step(
            "bad_step",
            poison_handler,
            retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.01, jitter=False)
        )

        with self.assertRaises(DeadLetterQueueException) as ctx:
            wf.execute(workflow_id, {"records": [1, 2, 3]})

        self.assertIn("Corrupt character encoding", str(ctx.exception))

        # Verify DLQ table in SQLite
        records = self.store.get_dlq_records(workflow_id)
        self.assertEqual(len(records), 1)
        dlq_entry = records[0]
        self.assertEqual(dlq_entry["step_name"], "bad_step")
        self.assertEqual(dlq_entry["attempt_count"], 3)
        self.assertIn("Corrupt character encoding", dlq_entry["error_message"])
        self.assertIn("records", dlq_entry["failed_payload"])
        self.assertIsNotNone(dlq_entry["stack_trace"])

    def test_dlq_non_fatal_bypass(self):
        """Verifies that optional pipeline steps can be quarantined without halting downstream work."""
        workflow_id = "wf-non-fatal-test"

        def optional_flaky_enrichment(state):
            raise TimeoutError("Third-party enrichment API unreachable")

        def final_step(state):
            return {"pipeline_finished": True}

        wf = DurableWorkflow("NonFatalDLQWorkflow", checkpointer=self.store)
        wf.add_step(
            "optional_enrichment",
            optional_flaky_enrichment,
            retry_policy=RetryPolicy(max_attempts=2, initial_interval=0.01, jitter=False),
            dlq_non_fatal=True
        )
        wf.add_step("final_step", final_step)

        result = wf.execute(workflow_id, {"initial_data": "user_42"})

        # Step was bypassed and recorded as quarantined
        self.assertTrue(result.get("optional_enrichment_quarantined"))
        self.assertTrue(result.get("pipeline_finished"))

        # DLQ still caught the failed payload and stack trace
        records = self.store.get_dlq_records(workflow_id)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["step_name"], "optional_enrichment")


if __name__ == "__main__":
    unittest.main()
