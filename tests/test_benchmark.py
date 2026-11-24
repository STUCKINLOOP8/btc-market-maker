import json
import subprocess
import sys
import unittest
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT = Path(__file__).resolve().parents[1]
RECORDING = PROJECT / "fixtures" / "order_timeline_recording.jsonl"
FILTERS = PROJECT / "fixtures" / "binance_btcusdt_filters.json"


class BenchmarkTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.manifest = self.root / "manifest.json"
        self.data = {
            "schema_version": 1,
            "dataset_id": "fixture-dataset",
            "status": "complete",
            "symbol": "BTCUSDT",
            "recording_format_version": 1,
            "capture_plan": [
                {"id": "development", "split": "development"},
                {"id": "evaluation", "split": "held_out"},
            ],
            "sessions": [
                {"id": "development", "split": "development", "recording": {"path": "unused"}},
                {
                    "id": "evaluation",
                    "split": "held_out",
                    "recording": self.reference(RECORDING),
                    "market_filters": self.reference(FILTERS),
                },
            ],
        }
        self.save_manifest()

    @staticmethod
    def reference(path):
        payload = path.read_bytes()
        return {"path": path.as_posix(), "bytes": len(payload), "sha256": sha256(payload).hexdigest()}

    def save_manifest(self):
        self.manifest.write_text(json.dumps(self.data), encoding="utf-8")

    def command(self, *args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "market_maker.benchmark", *args],
            cwd=PROJECT, capture_output=True, text=True, timeout=20,
        )

    def test_cli_runs_only_held_out_and_reproduces_all_files_from_saved_config(self):
        first = self.root / "first"
        second = self.root / "second"
        execution = self.command("--manifest", str(self.manifest), "--output", str(first))
        self.assertEqual(execution.returncode, 0, execution.stderr)
        repeated = self.command("--config", str(first / "config.json"), "--output", str(second))
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        self.assertEqual(
            {file.relative_to(first): file.read_bytes() for file in first.rglob("*") if file.is_file()},
            {file.relative_to(second): file.read_bytes() for file in second.rglob("*") if file.is_file()},
        )

        rows = json.loads((first / "summary.json").read_text(encoding="utf-8"))["runs"]
        self.assertEqual([row["session_id"] for row in rows], ["evaluation"] * 3)
        self.assertEqual([row["strategy"] for row in rows],
                         ["fixed-spread", "avellaneda-stoikov", "avellaneda-stoikov-imbalance"])
        configs = []
        for row in rows:
            run = first / row["directory"]
            audit = (run / "audit.jsonl").read_bytes()
            self.assertEqual(sha256(audit).hexdigest(), row["audit_sha256"])
            events = [json.loads(line) for line in audit.splitlines()]
            self.assertEqual(len(events), row["audit_events"])
            first_order = next(entry for entry in events if entry["kind"] == "ORDER_SUBMITTED")
            self.assertIsInstance(first_order["details"]["price"], str)
            configs.append(json.loads((run / "config.json").read_text(encoding="utf-8")))
        self.assertTrue(all(config["settings"] == configs[0]["settings"] for config in configs))
        self.assertIn("strategy_source_sha256", configs[0])
        self.assertEqual(configs[2]["strategy_parameters"]["imbalance_limit_ticks"], 2)

    def test_cli_rejects_an_unverified_held_out_file_before_publishing_results(self):
        self.data["sessions"][1]["recording"]["sha256"] = "0" * 64
        self.save_manifest()
        output = self.root / "output"
        result = self.command("--manifest", str(self.manifest), "--output", str(output))
        self.assertEqual(result.returncode, 2)
        self.assertIn("Dataset file does not match its manifest", result.stderr)
        self.assertFalse(output.exists())
