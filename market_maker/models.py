from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TypeVar


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING_NEW = "PENDING_NEW"
    ACTIVE = "ACTIVE"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class Order:
    order_id: str
    side: Side
    limit_price: Decimal
    original_quantity: Decimal
    filled_quantity: Decimal = Decimal("0")
    status: OrderStatus = OrderStatus.PENDING_NEW


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class Event:
    timestamp_ms: int
    sequence: int


@dataclass(frozen=True, slots=True)
class BookSnapshot(Event):
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class DepthUpdate(Event):
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class Trade(Event):
    trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_is_maker: bool


@dataclass(frozen=True, slots=True)
class SubmitOrder(Event):
    order_id: str
    side: Side
    limit_price: Decimal
    quantity: Decimal
    post_only: bool = False


@dataclass(frozen=True, slots=True)
class CancelOrder(Event):
    order_id: str


@dataclass(frozen=True, slots=True)
class FeedUnavailable(Event):
    reason: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class RecordingEnd(Event):
    """Advance the replay clock through the last recorded message."""


@dataclass(frozen=True, slots=True)
class ClockAdvance(Event):
    """Run a scheduled execution action between or after recorded inputs."""


ReplayEvent = (
    BookSnapshot | DepthUpdate | Trade | SubmitOrder | CancelOrder | FeedUnavailable | RecordingEnd | ClockAdvance
)
EventType = TypeVar("EventType", bound=Event)


def order_events(events: Iterable[EventType]) -> list[EventType]:
    """Sort by timestamp and sequence, preserving input order for identical keys."""
    return sorted(events, key=lambda event: (event.timestamp_ms, event.sequence))
