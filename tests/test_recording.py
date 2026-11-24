import json
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from market_maker.recording import RecordedMessage, read_recording, write_recording


class RecordingTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "sample.jsonl"

    def test_round_trip_preserves_receive_order_with_tied_times(self):
        messages = [
            RecordedMessage(
                stream="btcusdt@trade",
                sequence=1,
                received_elapsed_ms=25,
                exchange_timestamp_ms=1700000000102,
                raw_payload='{"e":"trade","t":101,"p":"68448.01000000","q":"0.25000000","m":true}',
            ),
            RecordedMessage(
                stream="btcusdt@depth@100ms",
                sequence=2,
                received_elapsed_ms=25,
                exchange_timestamp_ms=1700000000101,
                raw_payload='{"e":"depthUpdate","U":11,"u":12,"b":[["68448.01000000","0.15000000"]],"a":[]}',
            ),
            RecordedMessage(
                stream="rest_depth",
                sequence=3,
                received_elapsed_ms=30,
                raw_payload='{"lastUpdateId":12,"bids":[["68448.01000000","0.15000000"]],"asks":[]}',
            ),
        ]

        write_recording(self.path, iter(messages))
        loaded = list(read_recording(self.path))

        self.assertEqual(loaded, messages)
        self.assertEqual([message.sequence for message in loaded], [1, 2, 3])
        lines = self.path.read_text(encoding="utf-8").splitlines()
        self.assertEqual([json.loads(line)["sequence"] for line in lines], [1, 2, 3])
        self.assertIsNone(json.loads(lines[2])["exchange_timestamp_ms"])

    def test_round_trip_preserves_raw_text_and_exact_decimals(self):
        price = "68448.01000000"
        quantity = "0.12345678901234567890"
        payload = f'{{\n  "p": "{price}",\n  "q": "{quantity}"\n}}'
        message = RecordedMessage("btcusdt@trade", 1, 25, payload, 1700000000102)

        write_recording(self.path, [message])
        [loaded] = read_recording(self.path)

        self.assertEqual(len(self.path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(loaded.raw_payload, payload)
        decoded = json.loads(loaded.raw_payload)
        self.assertEqual(decoded, {"p": price, "q": quantity})
        self.assertEqual(Decimal(decoded["p"]).as_tuple(), Decimal(price).as_tuple())
        self.assertEqual(Decimal(decoded["q"]).as_tuple(), Decimal(quantity).as_tuple())

    def test_reports_line_number_for_unsupported_version(self):
        write_recording(self.path, [RecordedMessage("btcusdt@trade", 1, 25, "{}")])
        with self.path.open("a", encoding="utf-8") as recording:
            recording.write('{"version":2}\n')

        with self.assertRaisesRegex(ValueError, "sample[.]jsonl:2: unsupported recording version 2"):
            list(read_recording(self.path))
