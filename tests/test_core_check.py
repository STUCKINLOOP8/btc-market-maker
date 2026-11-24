import subprocess
import sys
import unittest
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


class CoreCheckTests(unittest.TestCase):
    def test_command_runs_core_tests_and_prints_the_same_trace_twice(self):
        command = [sys.executable, "-B", "-m", "market_maker.core_check"]
        first, second = (
            subprocess.run(command, cwd=PROJECT, check=True, capture_output=True)
            for _ in range(2)
        )

        self.assertEqual(first.stdout, second.stdout)
        self.assertIn(b"[6500 ms] MARKOUT_5S", first.stdout)
        self.assertIn(b"fees_usdt=5.13375 net_pnl_usdt=-4.93", first.stdout)
        for module in (b"test_replay.", b"test_wallet.", b"test_execution.", b"test_queue_model.",
                       b"test_risk.", b"test_replay_session."):
            self.assertIn(module, first.stderr)
        self.assertIn(b"OK", first.stderr)
