"""Translate Binance JSONL recordings into timestamped paper replay events."""

import json
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

from .binance_book import (
    DEPTH_STREAM,
    FEED_STATUS_STREAM,
    REST_DEPTH_STREAM,
    TRADE_STREAM,
    BinanceOrderBook,
    DepthGapError,
)
from .models import CancelOrder, FeedUnavailable, RecordingEnd, ReplayEvent, SubmitOrder, Trade, order_events
from .parsing import read_decimal, read_integer
from .recording import RecordedMessage, read_recording


def load_binance_replay(
    path: str | Path, scripted_orders: Iterable[ReplayEvent] = ()
) -> list[ReplayEvent]:
    """Load using receive time, applying recorded market events before scripted ties."""
    book = BinanceOrderBook()
    events: list[ReplayEvent] = [FeedUnavailable(0, 0, "awaiting_snapshot")]
    unavailable_reason: str | None = "awaiting_snapshot"
    expects_ready_marker = False
    saw_ready = False
    last_sequence = None
    last_timestamp_ms = 0

    def pause(message: RecordedMessage, reason: str, detail: str | None = None) -> None:
        nonlocal unavailable_reason
        if unavailable_reason != reason:
            events.append(FeedUnavailable(message.received_elapsed_ms, message.sequence, reason, detail))
        unavailable_reason = reason

    def resume(message: RecordedMessage) -> None:
        nonlocal unavailable_reason, saw_ready
        events.append(book.snapshot(message.received_elapsed_ms, message.sequence))
        unavailable_reason = None
        saw_ready = True

    for message in read_recording(path):
        if last_sequence is not None and message.sequence != last_sequence + 1:
            raise ValueError(f"Recording sequence gap: expected {last_sequence + 1}, got {message.sequence}")
        if message.received_elapsed_ms < last_timestamp_ms:
            raise ValueError(f"Receive time went backwards at recording sequence {message.sequence}")
        last_sequence = message.sequence
        last_timestamp_ms = message.received_elapsed_ms

        if message.stream == FEED_STATUS_STREAM:
            expects_ready_marker = True
            status = json.loads(message.raw_payload)
            if not isinstance(status, dict):
                raise ValueError(f"Invalid feed status at recording sequence {message.sequence}")
            state = status["state"]
            if state == "ready":
                top = book.top
                update_id = read_integer(status["last_update_id"], "last_update_id")
                if top is None or top.last_update_id != update_id:
                    raise ValueError(f"Ready marker at sequence {message.sequence} has no matching synchronized book")
                if unavailable_reason is not None:
                    resume(message)
            elif state in ("syncing", "unavailable"):
                reason, detail = status.get("reason", state), status.get("detail")
                if not isinstance(reason, str) or not reason or (detail is not None and not isinstance(detail, str)):
                    raise ValueError(f"Invalid feed status reason or detail at recording sequence {message.sequence}")
                book.reset()
                pause(message, reason, detail)
            else:
                raise ValueError(f"Unknown feed state {state!r} at recording sequence {message.sequence}")
        elif message.stream == DEPTH_STREAM:
            try:
                update = book.apply_depth_event(message)
            except DepthGapError as error:
                pause(message, "depth_gap", str(error))
            else:
                if update is not None and unavailable_reason is None:
                    events.append(update)
        elif message.stream == REST_DEPTH_STREAM:
            if unavailable_reason is None:
                raise ValueError(f"Unexpected REST snapshot while feed is ready at sequence {message.sequence}")
            if book.apply_snapshot(message) and not expects_ready_marker:
                # Older synchronized recordings have a snapshot but no status rows.
                resume(message)
        elif message.stream == TRADE_STREAM:
            events.append(trade_event(message))
        elif message.stream == "!serverShutdown":
            book.reset()
            pause(message, "disconnected", "Binance announced a server shutdown")

    if not saw_ready or last_sequence is None:
        raise ValueError("Recording has no synchronized Binance book. Capture it with --sync-book.")

    for order in order_events(scripted_orders):
        if not isinstance(order, (SubmitOrder, CancelOrder)):
            raise ValueError("Scripted events must be order submissions or cancellations")
        if not 0 <= order.timestamp_ms <= last_timestamp_ms:
            raise ValueError(f"Scripted order {order.order_id!r} is outside the recorded time range")
        last_sequence += 1
        events.append(replace(order, sequence=last_sequence))
    events.append(RecordingEnd(last_timestamp_ms, last_sequence + 1))
    return order_events(events)


def trade_event(message: RecordedMessage) -> Trade:
    data = json.loads(message.raw_payload)["data"]
    buyer_is_maker = data["m"]
    if not isinstance(buyer_is_maker, bool):
        raise ValueError("buyer_is_maker must be a JSON boolean")
    return Trade(
        timestamp_ms=message.received_elapsed_ms,
        sequence=message.sequence,
        trade_id=read_integer(data["t"], "trade_id"),
        price=read_decimal(data["p"], "price"),
        quantity=read_decimal(data["q"], "quantity"),
        buyer_is_maker=buyer_is_maker,
    )
