import json
from dataclasses import dataclass
from decimal import Decimal

from .models import BookSnapshot, DepthUpdate
from .order_book import OrderBook
from .parsing import read_integer, read_levels
from .recording import RecordedMessage


DEPTH_STREAM = "btcusdt@depth@100ms"
TRADE_STREAM = "btcusdt@trade"
REST_DEPTH_STREAM = "rest_depth"
FEED_STATUS_STREAM = "feed_status"


class DepthGapError(ValueError):
    """The next depth update does not continue the local book."""


@dataclass(frozen=True, slots=True)
class BookTop:
    last_update_id: int
    best_bid: Decimal | None
    best_ask: Decimal | None
    midpoint: Decimal | None


@dataclass(frozen=True, slots=True)
class _DepthChange:
    first_id: int
    last_id: int
    event: DepthUpdate


class BinanceOrderBook:
    """Bridge an initial REST snapshot to the received Binance depth stream."""

    def __init__(self) -> None:
        self._book = OrderBook()
        self._last_update_id: int | None = None
        self._buffer: list[_DepthChange] = []

    @property
    def top(self) -> BookTop | None:
        if self._last_update_id is None:
            return None
        return BookTop(self._last_update_id, self._book.best_bid, self._book.best_ask, self._book.mid_price)

    def reset(self) -> None:
        """Discard the old book and buffered updates before opening a new stream."""
        self._book = OrderBook()
        self._last_update_id = None
        self._buffer.clear()

    def apply_depth(self, message: RecordedMessage) -> bool:
        """Return whether the update advanced the book or its bootstrap buffer."""
        return self.apply_depth_event(message) is not None

    def apply_depth_event(self, message: RecordedMessage) -> DepthUpdate | None:
        """Apply or buffer a depth frame, returning the normalized accepted update."""
        envelope = json.loads(message.raw_payload)
        data = envelope["data"]
        first_id = read_integer(data["U"], "first update ID")
        last_id = read_integer(data["u"], "last update ID")
        if first_id < 0 or last_id < first_id:
            raise ValueError("Invalid Binance depth update ID range")
        change = _DepthChange(
            first_id,
            last_id,
            DepthUpdate(message.received_elapsed_ms, message.sequence, read_levels(data["b"]), read_levels(data["a"])),
        )
        if self._last_update_id is None:
            if self._buffer and last_id <= self._buffer[-1].last_id:
                return None
            self._buffer.append(change)
            return change.event

        previous_id = self._last_update_id
        try:
            self._last_update_id = self._apply_change(self._book, previous_id, change)
        except DepthGapError:
            self.reset()
            self._buffer.append(change)
            raise
        return change.event if self._last_update_id != previous_id else None

    def snapshot(self, timestamp_ms: int, sequence: int) -> BookSnapshot:
        """Export the synchronized book, including all applied buffered updates."""
        return self._book.snapshot(timestamp_ms, sequence)

    def apply_snapshot(self, message: RecordedMessage) -> bool:
        """Install a snapshot and buffered updates, or request a newer snapshot."""
        data = json.loads(message.raw_payload)
        update_id = read_integer(data["lastUpdateId"], "lastUpdateId")
        if update_id < 0:
            raise ValueError("lastUpdateId must be non-negative")
        if self._buffer and update_id < self._buffer[0].first_id:
            return False

        book = OrderBook()
        book.apply(
            BookSnapshot(
                message.received_elapsed_ms,
                message.sequence,
                read_levels(data["bids"]),
                read_levels(data["asks"]),
            )
        )
        try:
            for change in self._buffer:
                update_id = self._apply_change(book, update_id, change)
        except DepthGapError:
            return False

        self._book = book
        self._last_update_id = update_id
        self._buffer.clear()
        return True

    @staticmethod
    def _apply_change(book: OrderBook, update_id: int, change: _DepthChange) -> int:
        if change.last_id <= update_id:
            return update_id
        if change.first_id > update_id + 1:
            raise DepthGapError(f"Binance depth gap: expected update {update_id + 1}, received {change.first_id}")
        book.apply(change.event)
        return change.last_id
