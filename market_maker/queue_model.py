"""Receive-time reconciliation of public trades and displayed depth reductions."""

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


QueueModel = Literal["trade-only", "proportional-cancellation"]
QUEUE_MODELS = ("trade-only", "proportional-cancellation")
CANCELLATION_WINDOW_MS = 150
ZERO = Decimal("0")


@dataclass(slots=True)
class DepthDrop:
    timestamp_ms: int
    previous_depth: Decimal
    removed: Decimal
    trades: Decimal
    ambiguous: str | None = None


@dataclass(frozen=True, slots=True)
class CancellationEstimate:
    removed: Decimal = ZERO
    queue_advance: Decimal = ZERO
    reason: str | None = None


class ProportionalQueue:
    """Reconcile one working order's price level independently of other orders."""

    def __init__(self, observed_from_ms: int | None) -> None:
        self._observed_from_ms = observed_from_ms
        self._drop: DepthDrop | None = None
        self._trades: deque[tuple[int, Decimal]] = deque()
        self._late_volume = ZERO
        self._late_advance = ZERO

    def _prune_trades(self, timestamp_ms: int) -> None:
        while self._trades and self._trades[0][0] < timestamp_ms - CANCELLATION_WINDOW_MS:
            self._trades.popleft()

    def depth_changed(self, timestamp_ms: int, previous: Decimal, current: Decimal) -> DepthDrop | None:
        if previous == current:
            return None
        self._late_volume = self._late_advance = ZERO
        self._prune_trades(timestamp_ms)
        trades = sum((quantity for _, quantity in self._trades), ZERO)
        self._trades.clear()
        if self._drop is not None:
            self._drop.ambiguous = self._drop.ambiguous or "multiple depth changes inside the reconciliation window"
            return None
        if current > previous:
            return None
        self._drop = DepthDrop(timestamp_ms, previous, previous - current, trades)
        if current == 0:
            self._drop.ambiguous = "price level disappeared"
        elif self._observed_from_ms is not None and timestamp_ms - CANCELLATION_WINDOW_MS < self._observed_from_ms:
            self._drop.ambiguous = "trade window crosses activation or a fresh snapshot"
        return self._drop

    def late_restoration(self, quantity: Decimal) -> Decimal:
        """Return the queue credit to undo before matching an eligible late print."""
        if not self._late_volume:
            return ZERO
        if quantity >= self._late_volume:
            return self._late_advance
        return self._late_advance * quantity / self._late_volume

    def trade(self, timestamp_ms: int, quantity: Decimal, restored: Decimal) -> None:
        # Attribute a print to the last closed drop first, until a new absolute
        # change at this price gives us another depth boundary.
        overlap = min(quantity, self._late_volume)
        self._late_volume -= overlap
        self._late_advance -= restored
        quantity -= overlap
        if not quantity:
            return
        if self._drop is not None:
            self._drop.trades += quantity
        else:
            self._prune_trades(timestamp_ms)
            self._trades.append((timestamp_ms, quantity))

    def close(self, drop: DepthDrop, queue_ahead: Decimal) -> CancellationEstimate | None:
        if self._drop is not drop:
            return None
        self._drop = None
        if drop.ambiguous:
            return CancellationEstimate(reason=drop.ambiguous)
        if drop.trades >= drop.removed:
            reason = "trades exceed the net depth drop" if drop.trades > drop.removed else "depth drop explained by trades"
            return CancellationEstimate(reason=reason)
        canceled = drop.removed - drop.trades
        remaining_depth = drop.previous_depth - drop.trades
        fraction_ahead = min(Decimal("1"), queue_ahead / remaining_depth)
        advance = min(queue_ahead, canceled * fraction_ahead)
        self._late_volume = canceled
        self._late_advance = advance
        return CancellationEstimate(canceled, advance)
