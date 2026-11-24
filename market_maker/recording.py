"""Read and write local JSONL recordings in capture order."""

import json
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TextIO


RECORDING_VERSION = 1


@dataclass(frozen=True, slots=True)
class RecordedMessage:
    """One source message with its receive order and clocks.

    Receive times are monotonic milliseconds since the session started. The
    sequence number preserves order when multiple messages share a millisecond.
    The optional exchange timestamp is in Unix milliseconds. The raw payload
    is retained as text, including all price and quantity strings.
    """

    stream: str
    sequence: int
    received_elapsed_ms: int
    raw_payload: str
    exchange_timestamp_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.stream, str) or not self.stream:
            raise ValueError("stream must be a non-empty string")
        if not isinstance(self.raw_payload, str) or not self.raw_payload:
            raise ValueError("raw_payload must be a non-empty string")
        _require_nonnegative_integer(self.sequence, "sequence")
        _require_nonnegative_integer(self.received_elapsed_ms, "received_elapsed_ms")
        if self.exchange_timestamp_ms is not None:
            _require_nonnegative_integer(self.exchange_timestamp_ms, "exchange_timestamp_ms")


def write_recording(path: str | Path, messages: Iterable[RecordedMessage]) -> None:
    """Write one versioned record per line in the supplied capture order."""
    with Path(path).open("w", encoding="utf-8", newline="\n") as recording:
        for message in messages:
            write_message(recording, message)


def write_message(recording: TextIO, message: RecordedMessage) -> None:
    """Append one record to an open recording."""
    record = {"version": RECORDING_VERSION, **asdict(message)}
    recording.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def read_recording(path: str | Path) -> Iterator[RecordedMessage]:
    """Yield records in file order, with file and line context for invalid records."""
    with Path(path).open(encoding="utf-8") as recording:
        for line_number, line in enumerate(recording, start=1):
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be a JSON object")
                version = record.pop("version", None)
                if type(version) is not int or version != RECORDING_VERSION:
                    raise ValueError(f"unsupported recording version {version!r}")
                message = RecordedMessage(**record)
            except (TypeError, ValueError) as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
            yield message


def _require_nonnegative_integer(value: object, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
