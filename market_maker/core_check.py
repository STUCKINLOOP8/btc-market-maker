"""Run the Milestone 1 tests and print the deterministic replay trace."""

import sys
import unittest
from pathlib import Path

from . import replay


TEST_DIRECTORY = Path(__file__).resolve().parents[1] / "tests"
CORE_TEST_FILES = (
    "test_replay.py",
    "test_wallet.py",
    "test_execution.py",
    "test_queue_model.py",
    "test_risk.py",
    "test_replay_session.py",
)


def main() -> int:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite(
        loader.discover(str(TEST_DIRECTORY), pattern=filename) for filename in CORE_TEST_FILES
    )
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    return replay.main([])


if __name__ == "__main__":
    raise SystemExit(main())
