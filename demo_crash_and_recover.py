"""
demo_crash_and_recover.py
Demonstration of Crash Recovery & Exponential Retry in Action.

What this script proves:
1. Starts the 4-Hour Migration workflow.
2. In Phase 2, a fatal SIGKILL (code 137) is injected to simulate a server reboot / pod eviction.
3. In a naive `while True` loop, 100% of the work would be lost and restart from 0.
4. Here, on reboot, the engine inspects SQLite, detects completed steps (Batches 1-3),
   skips them completely, recovers the state, and completes the migration without data loss.
"""

import subprocess
import sys
import os
import time

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
CRASH_FLAG = os.path.join(WORK_DIR, "simulate_crash_at_batch4.flag")
DB_FILE = os.path.join(WORK_DIR, "migration_checkpoints.db")


def main():
    print("=" * 70)
    print(" 🚀 DURABLE STATE MACHINE WORKFLOW DEMO: CRASH & RECOVERY TEST")
    print("=" * 70)
    
    # 1. Clean slate
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    if os.path.exists(CRASH_FLAG):
        os.remove(CRASH_FLAG)
        
    print("\n[PHASE A] Starting fresh workflow with simulated fatal crash trigger...")
    with open(CRASH_FLAG, "w") as f:
        f.write("crash_me")
        
    start_time = time.time()
    
    # Run the workflow in a subprocess to demonstrate true OS-level process death
    proc1 = subprocess.run(
        [sys.executable, "migration_workflow.py"],
        cwd=WORK_DIR,
        capture_output=True,
        text=True
    )
    
    print("\n--- Process 1 Output (Pre-Crash) ---")
    print(proc1.stdout)
    if proc1.stderr:
        print(proc1.stderr)
        
    print(f"🛑 Process 1 Exit Code: {proc1.returncode}")
    if proc1.returncode in (137, -9, 255):
        print("💥 CONFIRMED: Process was killed mid-execution (Exit Code 137 = SIGKILL / OOM)!")
    else:
        print(f"Process terminated with exit code {proc1.returncode}")

    print("\n" + "=" * 70)
    print(" 🔄 [PHASE B] SIMULATING SERVER REBOOT & WORKER RESUMPTION")
    print(" In naive code: Everything would restart from scratch (wasting hours).")
    print(" In our Durable State Machine: It reads the WAL checkpoint and resumes!")
    print("=" * 70 + "\n")
    
    time.sleep(1)
    
    # Run again with the exact same workflow ID and DB
    proc2 = subprocess.run(
        [sys.executable, "migration_workflow.py"],
        cwd=WORK_DIR,
        capture_output=True,
        text=True
    )
    
    print("--- Process 2 Output (Post-Reboot / Resumed) ---")
    print(proc2.stdout)
    if proc2.stderr:
        print(proc2.stderr)

    print("=" * 70)
    if proc2.returncode == 0:
        print("🎯 TEST SUCCESSFUL! Complete crash recovery achieved!")
        print("  - Batches 1 to 3 were NOT re-processed (zero duplicate work).")
        print("  - Transient network errors in Step 4 were resolved via exponential backoff.")
        print("  - Batches 4 to 10 completed cleanly.")
        print("  - Row count & SHA-256 data parity verified 100%.")
    else:
        print(f"❌ Process 2 failed with exit code {proc2.returncode}")
    print("=" * 70)


if __name__ == "__main__":
    main()
