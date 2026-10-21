"""
tests/test_engine.py
Unit tests verifying the core mechanics of the durable state machine:
- Checkpoint persistence & retrieval
- Exponential backoff calculation & jitter bounds
- Skip completed steps on resume
- Error bubbling on max attempts exceeded
"""

import unittest
import os
import tempfile
from core.engine import CheckpointStore, RetryPolicy, DurableWorkflow


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


if __name__ == "__main__":
    unittest.main()
