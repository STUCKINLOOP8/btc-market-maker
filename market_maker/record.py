"""Record Binance BTC/USDT spot market data to a local JSONL file."""

import argparse
import asyncio
import json
import math
import sys
from collections import Counter
from collections.abc import Callable
from contextlib import AsyncExitStack, ExitStack
from decimal import InvalidOperation
from itertools import count
from pathlib import Path
from typing import TextIO
from urllib.request import urlopen

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .binance_book import (
    DEPTH_STREAM,
    FEED_STATUS_STREAM,
    REST_DEPTH_STREAM,
    TRADE_STREAM,
    BinanceOrderBook,
    DepthGapError,
)
from .binance_replay import trade_event
from .models import BookSnapshot, DepthUpdate, FeedUnavailable, Trade
from .recording import RecordedMessage, write_message


STREAMS = (DEPTH_STREAM, TRADE_STREAM)
BINANCE_STREAM_URL = "wss://data-stream.binance.vision/stream?streams=" + "/".join(STREAMS)
BINANCE_SNAPSHOT_URL = "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=5000"
DEFAULT_DEPTH_TIMEOUT_SECONDS = 0.5
RECONNECT_DELAY_SECONDS = 2
LiveMarketEvent = BookSnapshot | DepthUpdate | FeedUnavailable | Trade
LiveSink = Callable[[LiveMarketEvent], None]


class _FeedInterruption(ValueError):
    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


class _Capture:
    def __init__(self, file: TextIO | None, *, started_at: float | None = None) -> None:
        self._file = file
        self._loop = asyncio.get_running_loop()
        self.started_at = self._loop.time() if started_at is None else started_at
        self._sequence = count(1)
        self.counts: Counter[str] = Counter()

    def save(self, raw_payload: str, stream: str | None = None) -> RecordedMessage:
        elapsed_ms = int((self._loop.time() - self.started_at) * 1000)
        sequence = next(self._sequence)
        if stream is None:
            message = _decode_frame(raw_payload, sequence, elapsed_ms)
        else:
            message = RecordedMessage(stream, sequence, elapsed_ms, raw_payload)
        if self._file is not None:
            write_message(self._file, message)
            self.counts[message.stream] += 1
        return message

    def status(self, state: str, **details: str | int) -> RecordedMessage:
        return self.save(json.dumps({"state": state, **details}), FEED_STATUS_STREAM)


async def record_binance(
    path: str | Path,
    *,
    duration_seconds: float,
    order_book: BinanceOrderBook | None = None,
    depth_timeout_seconds: float = DEFAULT_DEPTH_TIMEOUT_SECONDS,
) -> Counter[str]:
    """Record public Binance frames, recovering interrupted synchronized captures."""
    if not math.isfinite(duration_seconds) or duration_seconds <= 0:
        raise ValueError("duration must be a positive, finite number of seconds")
    if not math.isfinite(depth_timeout_seconds) or depth_timeout_seconds <= 0:
        raise ValueError("depth timeout must be a positive, finite number of seconds")

    capture: _Capture | None = None
    capture_timer = asyncio.timeout(None)
    try:
        async with capture_timer:
            with ExitStack() as files:
                while True:
                    async with AsyncExitStack() as transport:
                        try:
                            try:
                                websocket = await transport.enter_async_context(
                                    connect(BINANCE_STREAM_URL, open_timeout=10, close_timeout=2)
                                )
                            except (OSError, WebSocketException) as error:
                                if capture is None or order_book is None:
                                    raise
                                raise _FeedInterruption("disconnected", str(error)) from error
                            if capture is None:
                                # Keep one file and receive sequence across reconnects.
                                file = files.enter_context(
                                    Path(path).open("x", encoding="utf-8", newline="\n", buffering=1)
                                )
                                capture = _Capture(file)
                                capture_timer.reschedule(capture.started_at + duration_seconds)
                                if order_book is not None:
                                    order_book.reset()
                                    capture.status("syncing", reason="awaiting_snapshot")
                            await _capture_connection(
                                websocket,
                                capture,
                                order_book,
                                depth_timeout_seconds,
                                capture.started_at + duration_seconds,
                            )
                            capture_timer.reschedule(None)
                            break
                        except _FeedInterruption as error:
                            assert capture is not None and order_book is not None
                            order_book.reset()
                            capture.status("unavailable", reason=error.reason, detail=str(error))
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
    except TimeoutError:
        if not capture_timer.expired():
            raise

    assert capture is not None
    missing = [stream for stream in STREAMS if not capture.counts[stream]]
    if missing:
        raise ValueError(f"Capture ended without messages from: {', '.join(missing)}")
    if order_book is not None and order_book.top is None:
        raise ValueError("Capture ended before the Binance book was synchronized")
    return capture.counts


async def stream_binance(
    on_event: LiveSink, *, started_at: float, testnet: bool = False,
) -> None:
    """Stream synchronized public market events until canceled, reconnecting on gaps."""
    book = BinanceOrderBook()
    capture = _Capture(None, started_at=started_at)
    stream_url = BINANCE_STREAM_URL
    snapshot_fetcher = None
    if testnet:
        stream_url = "wss://stream.testnet.binance.vision/stream?streams=" + "/".join(STREAMS)

        def snapshot_fetcher() -> str:
            return _fetch_snapshot("https://testnet.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=5000")

    on_event(FeedUnavailable(0, 0, "awaiting_snapshot"))
    while True:
        async with AsyncExitStack() as transport:
            try:
                websocket = await transport.enter_async_context(
                    connect(stream_url, open_timeout=10, close_timeout=2)
                )
                await _capture_connection(
                    websocket, capture, book, DEFAULT_DEPTH_TIMEOUT_SECONDS, float("inf"),
                    on_event=on_event, snapshot_fetcher=snapshot_fetcher,
                )
            except (OSError, WebSocketException, ValueError, KeyError, InvalidOperation, TypeError) as error:
                if isinstance(error, _FeedInterruption):
                    reason = error.reason
                elif isinstance(error, (OSError, WebSocketException)):
                    reason = "disconnected"
                else:
                    reason = "invalid_market_data"
                book.reset()
                message = capture.status("unavailable", reason=reason)
                on_event(FeedUnavailable(message.received_elapsed_ms, message.sequence, reason, str(error)))
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


async def _capture_connection(
    websocket: ClientConnection,
    capture: _Capture,
    book: BinanceOrderBook | None,
    depth_timeout_seconds: float,
    capture_deadline: float,
    *,
    on_event: LiveSink | None = None,
    snapshot_fetcher: Callable[[], str] | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    watchdog = asyncio.timeout(depth_timeout_seconds if book is not None else None)
    snapshot_task: asyncio.Task[None] | None = None
    timeout_detail = f"No advancing Binance depth update for {depth_timeout_seconds:g} seconds"

    def check_depth() -> None:
        deadline = watchdog.when()
        if deadline is not None and loop.time() >= deadline:
            raise _FeedInterruption("depth_timeout", timeout_detail)

    try:
        async with watchdog:
            while loop.time() < capture_deadline:
                check_depth()
                if snapshot_task is not None and snapshot_task.done():
                    snapshot_task.result()
                try:
                    raw_payload = await websocket.recv(decode=True)
                except (OSError, WebSocketException) as error:
                    if book is None:
                        raise
                    raise _FeedInterruption("disconnected", str(error)) from error
                if loop.time() >= capture_deadline:
                    break
                check_depth()
                message = capture.save(raw_payload)
                if book is None:
                    continue
                if message.stream == "!serverShutdown":
                    raise _FeedInterruption("disconnected", "Binance announced a server shutdown")
                if message.stream != DEPTH_STREAM:
                    if on_event is not None and message.stream == TRADE_STREAM and book.top is not None:
                        on_event(trade_event(message))
                    continue
                synchronized = on_event is not None and book.top is not None
                try:
                    update = book.apply_depth_event(message)
                except DepthGapError as error:
                    raise _FeedInterruption("depth_gap", str(error)) from error
                if update is not None:
                    watchdog.reschedule(loop.time() + depth_timeout_seconds)
                    if synchronized:
                        assert on_event is not None
                        on_event(update)
                if snapshot_task is None:
                    snapshot_task = asyncio.create_task(
                        _synchronize_book(book, capture, check_depth, on_event, snapshot_fetcher or _fetch_snapshot)
                    )
    except TimeoutError as error:
        if not watchdog.expired():
            raise
        raise _FeedInterruption("depth_timeout", timeout_detail) from error
    finally:
        if snapshot_task is not None:
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)


def _decode_frame(raw_payload: str, sequence: int, received_elapsed_ms: int) -> RecordedMessage:
    envelope = json.loads(raw_payload)
    if not isinstance(envelope, dict) or not isinstance(envelope.get("data"), dict):
        raise ValueError("Expected a Binance combined stream message with a data object")

    return RecordedMessage(
        stream=envelope.get("stream"),
        sequence=sequence,
        received_elapsed_ms=received_elapsed_ms,
        raw_payload=raw_payload,
        exchange_timestamp_ms=envelope["data"].get("E"),
    )


async def _synchronize_book(
    book: BinanceOrderBook, capture: _Capture, check_depth: Callable[[], None], on_event: LiveSink | None,
    fetch_snapshot: Callable[[], str],
) -> None:
    attempts = 3
    for attempt in range(attempts):
        try:
            raw_payload = await asyncio.to_thread(fetch_snapshot)
        except OSError as error:
            raise _FeedInterruption("snapshot_unavailable", str(error)) from error
        check_depth()
        if book.apply_snapshot(capture.save(raw_payload, REST_DEPTH_STREAM)):
            top = book.top
            assert top is not None
            ready = capture.status("ready", last_update_id=top.last_update_id)
            if on_event is not None:
                on_event(book.snapshot(ready.received_elapsed_ms, ready.sequence))
            return
        if attempt + 1 < attempts:
            await asyncio.sleep(1)
    raise _FeedInterruption("snapshot_unavailable", "Binance REST snapshots did not bridge buffered updates")


def _fetch_snapshot(url: str = BINANCE_SNAPSHOT_URL) -> str:
    with urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record public Binance BTC/USDT market data.")
    parser.add_argument("output", type=Path, help="path for the new JSONL recording")
    parser.add_argument("--duration", type=float, default=60, help="capture duration in seconds")
    parser.add_argument("--sync-book", action="store_true", help="record a REST snapshot and synchronize the book")
    parser.add_argument("--depth-timeout", type=float, default=DEFAULT_DEPTH_TIMEOUT_SECONDS,
                        help="seconds without an advancing depth update before reconnecting in synchronized mode")
    args = parser.parse_args(argv)

    book = BinanceOrderBook() if args.sync_book else None
    try:
        counts = asyncio.run(record_binance(
            args.output,
            duration_seconds=args.duration,
            order_book=book,
            depth_timeout_seconds=args.depth_timeout,
        ))
    except (InvalidOperation, KeyError, OSError, ValueError, WebSocketException) as error:
        print(f"Capture failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Capture stopped by user", file=sys.stderr)
        return 130

    print(f"Saved {counts.total()} messages to {args.output}")
    for stream in STREAMS:
        print(f"{stream}: {counts[stream]}")
    if book is not None and (top := book.top) is not None:
        print(f"{REST_DEPTH_STREAM}: {counts[REST_DEPTH_STREAM]}")
        print(f"{FEED_STATUS_STREAM}: {counts[FEED_STATUS_STREAM]}")
        print(f"Book at update {top.last_update_id}: bid={top.best_bid} ask={top.best_ask} mid={top.midpoint}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
