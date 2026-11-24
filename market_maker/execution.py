from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal
from functools import partial
from heapq import heappop, heappush
from itertools import count

from .models import (
    BookSnapshot,
    CancelOrder,
    DepthUpdate,
    FeedUnavailable,
    Order,
    OrderStatus,
    ReplayEvent,
    Side,
    SubmitOrder,
    Trade,
)
from .order_book import OrderBook
from .queue_model import CANCELLATION_WINDOW_MS, QUEUE_MODELS, DepthDrop, ProportionalQueue, QueueModel
from .risk import InventoryLimit, InventoryLimitError
from .wallet import InsufficientFundsError, Wallet, validate_order_fields


ZERO = Decimal("0")
DEFAULT_LATENCY_MS = 15
UNRESOLVED_STATUSES = frozenset({
    OrderStatus.PENDING_NEW, OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCEL_PENDING,
})
AuditValue = str | int | Decimal | None


@dataclass(frozen=True, slots=True)
class AuditEvent:
    timestamp_ms: int
    kind: str
    details: tuple[tuple[str, AuditValue], ...]


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    trade_id: int
    timestamp_ms: int
    side: Side
    price: Decimal
    quantity: Decimal
    fee_usdt: Decimal


@dataclass(slots=True)
class _TrackedOrder:
    order: Order
    queue_ahead: Decimal | None = None
    post_only: bool = False
    cancellation_queue: ProportionalQueue | None = None


class _ActivationUnavailable(ValueError):
    """The order cannot start working against the current book or side."""


class FillEngine:
    """Match trades against one working paper order per side.

    Feed timestamp- and sequence-ordered events through process_event for timed execution.
    """

    def __init__(
        self,
        wallet: Wallet,
        *,
        latency_ms: int = DEFAULT_LATENCY_MS,
        target_btc: Decimal | None = None,
        audit_log: list[AuditEvent] | None = None,
        queue_model: QueueModel = "trade-only",
    ) -> None:
        if type(latency_ms) is not int or latency_ms < 0:
            raise ValueError("latency_ms must be a non-negative integer")
        if queue_model not in QUEUE_MODELS:
            raise ValueError(f"Unknown queue model: {queue_model!r}")
        self._wallet = wallet
        self._inventory_limit = InventoryLimit(wallet.balances, target_btc=target_btc)
        self._latency_ms = latency_ms
        self._audit_log = audit_log
        self._queue_model = queue_model
        self._clock_ms: int | None = None
        self._scheduled: list[tuple[int, int, Callable[[], None]]] = []
        self._action_sequence = count()
        self._book = OrderBook()
        self._feed_available = True
        self._halt_requested = False
        self._orders: dict[str, _TrackedOrder] = {}
        self._working: dict[Side, _TrackedOrder] = {}
        self._processed_trade_ids: set[int] = set()

    def process_event(self, event: ReplayEvent) -> Fill | None:
        """Process an ordered event after applying actions due at its timestamp."""
        self.advance_to(event.timestamp_ms)
        match event:
            case FeedUnavailable():
                self._pause_for_feed(event)
            case BookSnapshot() | DepthUpdate():
                if not self._feed_available and isinstance(event, DepthUpdate):
                    return None
                self.update_book(event)
                if not self._feed_available:
                    self._feed_available = True
                    self._record("FEED_READY", mid_price=self.mid_price)
                self._record("BOOK", mid_price=self.mid_price)
            case SubmitOrder():
                if self._halt_requested:
                    self._record(
                        "ORDER_SKIPPED", order_id=event.order_id, side=event.side.value,
                        price=event.limit_price, quantity=event.quantity, reason="halt requested",
                    )
                    return None
                if not self._feed_available:
                    self._record("ORDER_REJECTED", order_id=event.order_id, reason="feed unavailable")
                    return None
                try:
                    self.submit(event)
                except (InventoryLimitError, InsufficientFundsError) as error:
                    self._record("ORDER_REJECTED", order_id=event.order_id, reason=str(error))
                    raise
                self._record_wallet(
                    "ORDER_SUBMITTED",
                    order_id=event.order_id,
                    side=event.side.value,
                    price=event.limit_price,
                    quantity=event.quantity,
                )
                self._schedule(partial(self._activate_scheduled_order, event.order_id))
            case CancelOrder():
                if event.order_id not in self._orders:
                    self._record("CANCEL_IGNORED", order_id=event.order_id, reason="unknown order")
                else:
                    self.schedule_cancel(event.order_id)
            case Trade():
                if not self._feed_available:
                    self._record("TRADE_IGNORED", trade_id=event.trade_id, reason="feed unavailable")
                    return None
                return self.process_trade(event)
        return None

    @property
    def halt_requested(self) -> bool:
        return self._halt_requested

    @property
    def clock_ms(self) -> int | None:
        return self._clock_ms

    @property
    def next_action_ms(self) -> int | None:
        return self._scheduled[0][0] if self._scheduled else None

    @property
    def unresolved_order_count(self) -> int:
        return sum(tracked.order.status in UNRESOLVED_STATUSES for tracked in self._orders.values())

    def halt(self) -> None:
        """Block submissions and request cancellation of orders already working."""
        if self._halt_requested:
            return
        self._halt_requested = True
        if self._clock_ms is None:
            self._clock_ms = 0
        self._record("HALT_REQUESTED")
        for order_id, tracked in self._orders.items():
            if tracked.order.status in (OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED):
                self.schedule_cancel(order_id, reason="halt requested")

    def _pause_for_feed(self, event: FeedUnavailable) -> None:
        self._feed_available = False
        self._book = OrderBook()
        self._scheduled.clear()
        self._record("FEED_UNAVAILABLE", reason=event.reason, detail=event.detail)

        # A missing market interval breaks queue continuity for every open order.
        for order_id, tracked in self._orders.items():
            if self.request_cancel(order_id) or tracked.order.status is OrderStatus.CANCEL_PENDING:
                self.confirm_cancel(order_id)

    def advance_to(self, timestamp_ms: int) -> None:
        """Advance the simulated clock and apply due actions in scheduling order."""
        if self._clock_ms is not None and timestamp_ms < self._clock_ms:
            raise ValueError("Cannot move the execution clock backwards")

        while self._scheduled and self._scheduled[0][0] <= timestamp_ms:
            due_ms, _, action = self._scheduled[0]
            self._clock_ms = due_ms
            action()
            heappop(self._scheduled)
        self._clock_ms = timestamp_ms

    def _schedule(self, action: Callable[[], None], *, delay_ms: int | None = None) -> int:
        assert self._clock_ms is not None
        delay = self._latency_ms if delay_ms is None else delay_ms
        due_ms = self._clock_ms + delay
        heappush(self._scheduled, (due_ms, next(self._action_sequence), action))
        if delay == 0:
            self.advance_to(due_ms)
        return due_ms

    def update_book(self, event: BookSnapshot | DepthUpdate) -> None:
        """Apply displayed depth for future activations."""
        preceding_depth = {
            side: self._book.quantity_at(side, tracked.order.limit_price)
            for side, tracked in self._working.items() if tracked.cancellation_queue is not None
        }
        self._book.apply(event)
        self._reconcile_depth(event, preceding_depth)
        mid_price = self.mid_price
        if mid_price is not None:
            self._inventory_limit.update_mark(mid_price)

    def _reconcile_depth(self, event: BookSnapshot | DepthUpdate, preceding_depth: dict[Side, Decimal]) -> None:
        for side, previous in preceding_depth.items():
            tracked = self._working[side]
            model = tracked.cancellation_queue
            assert model is not None
            if isinstance(event, BookSnapshot):
                tracked.cancellation_queue = ProportionalQueue(event.timestamp_ms)
                continue
            current = self._book.quantity_at(side, tracked.order.limit_price)
            drop = model.depth_changed(event.timestamp_ms, previous, current)
            if drop is not None:
                # Close after both streams have had the full inclusive matching window.
                self._schedule(
                    partial(self._close_depth_drop, tracked, model, drop), delay_ms=CANCELLATION_WINDOW_MS + 1,
                )

    def _close_depth_drop(self, tracked: _TrackedOrder, model: ProportionalQueue, drop: DepthDrop) -> None:
        order = tracked.order
        if self._working.get(order.side) is not tracked or tracked.cancellation_queue is not model:
            return
        ahead = tracked.queue_ahead
        assert ahead is not None
        estimate = model.close(drop, ahead)
        if estimate is None:
            return
        tracked.queue_ahead = ahead - estimate.queue_advance
        self._record(
            "QUEUE_CANCELLATION_SKIPPED" if estimate.reason else "QUEUE_CANCELLATION",
            order_id=order.order_id, depth_timestamp_ms=drop.timestamp_ms, depth_removed=drop.removed,
            trade_quantity=drop.trades, cancellation_quantity=estimate.removed,
            queue_before=ahead, queue_after=tracked.queue_ahead, reason=estimate.reason,
        )

    @property
    def mid_price(self) -> Decimal | None:
        """Return the current two-sided book midpoint, when available."""
        return self._book.mid_price

    def book_snapshot(self, timestamp_ms: int, sequence: int, *, limit: int | None = None) -> BookSnapshot | None:
        """Copy the displayed depth for the current replay position."""
        if not self._book.initialized:
            return None
        return self._book.snapshot(timestamp_ms, sequence, limit=limit)

    def submit(self, request: SubmitOrder) -> None:
        """Check inventory, reserve funds, and register the pending submission."""
        if self._halt_requested:
            raise ValueError("Replay has halted new order submissions")
        if request.order_id in self._orders:
            raise ValueError(f"Order {request.order_id!r} already exists")
        validate_order_fields(request)
        mark_price = self._inventory_limit.check_submission(
            request,
            held_btc=self._wallet.balances.btc,
            outstanding_quantity=self._wallet.reserved_quantity(request.side),
        )
        order = Order(request.order_id, request.side, request.limit_price, request.quantity)
        self._wallet.reserve(request)
        self._inventory_limit.update_mark(mark_price)
        self._orders[order.order_id] = _TrackedOrder(order, post_only=request.post_only)

    def activate(self, order_id: str) -> None:
        """Join the back of the currently displayed queue at the order price."""
        tracked = self._orders[order_id]
        order = tracked.order
        if tracked.queue_ahead is not None or order.status not in (
            OrderStatus.PENDING_NEW,
            OrderStatus.CANCEL_PENDING,
        ):
            raise ValueError(f"Order {order_id!r} is not pending activation")
        if not self._book.initialized:
            raise _ActivationUnavailable("A book snapshot is required before activating orders")
        if order.side in self._working:
            raise _ActivationUnavailable(f"A {order.side.value} order is already working")
        if tracked.post_only:
            self._check_post_only(order)

        tracked.queue_ahead = self._book.quantity_at(order.side, order.limit_price)
        if self._queue_model == "proportional-cancellation":
            tracked.cancellation_queue = ProportionalQueue(self._clock_ms)
        if order.status is OrderStatus.PENDING_NEW:
            order.status = OrderStatus.ACTIVE
        self._working[order.side] = tracked
        self._record("ORDER_ACTIVATED", order_id=order_id, queue_ahead=tracked.queue_ahead)

    def _check_post_only(self, order: Order) -> None:
        bid, ask = self._book.best_bid, self._book.best_ask
        if bid is None or ask is None or bid >= ask:
            raise _ActivationUnavailable("Post-only activation requires a valid two-sided book")
        if order.side is Side.BUY and order.limit_price >= ask:
            raise _ActivationUnavailable(f"Post-only buy would match best ask {ask:f} at activation")
        if order.side is Side.SELL and order.limit_price <= bid:
            raise _ActivationUnavailable(f"Post-only sell would match best bid {bid:f} at activation")

    def _activate_scheduled_order(self, order_id: str) -> None:
        tracked = self._orders[order_id]
        order = tracked.order
        if tracked.queue_ahead is not None or order.status is OrderStatus.CANCELED:
            return
        try:
            self.activate(order_id)
        except _ActivationUnavailable as error:
            self._wallet.release(order_id)
            order.status = OrderStatus.REJECTED
            self._record_wallet("ORDER_REJECTED", order_id=order_id, reason=str(error))
        else:
            if self._halt_requested:
                self.schedule_cancel(order_id, reason="halt requested")

    def schedule_cancel(self, order_id: str, *, reason: str | None = None) -> int | None:
        """Request a cancellation and return its scheduled confirmation time."""
        if self.request_cancel(order_id):
            details = {} if reason is None else {"reason": reason}
            self._record("CANCEL_REQUESTED", order_id=order_id, **details)
            return self._schedule(partial(self.confirm_cancel, order_id))
        return None

    def request_cancel(self, order_id: str) -> bool:
        """Mark the first cancellation request while retaining any working order."""
        order = self._orders[order_id].order
        if order.status not in (OrderStatus.PENDING_NEW, OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED):
            return False
        order.status = OrderStatus.CANCEL_PENDING
        return True

    def confirm_cancel(self, order_id: str) -> None:
        """Remove the working order and release its unfilled reservation."""
        tracked = self._orders[order_id]
        order = tracked.order
        if order.status is not OrderStatus.CANCEL_PENDING:
            return
        self._wallet.release(order_id)
        if self._working.get(order.side) is tracked:
            del self._working[order.side]
        order.status = OrderStatus.CANCELED
        tracked.cancellation_queue = None
        self._record_wallet("ORDER_CANCELED", order_id=order_id, filled_quantity=order.filled_quantity)

    def get_order(self, order_id: str) -> Order:
        """Return a snapshot of the order's current state."""
        return replace(self._orders[order_id].order)

    @property
    def orders(self) -> tuple[Order, ...]:
        """Return snapshots of all tracked orders in submission order."""
        return tuple(replace(tracked.order) for tracked in self._orders.values())

    def queue_ahead(self, order_id: str) -> Decimal | None:
        """Return volume ahead once the order has activated."""
        return self._orders[order_id].queue_ahead

    def process_trade(self, trade: Trade) -> Fill | None:
        """Process each public trade ID once, settling any fill before recording it."""
        if trade.trade_id in self._processed_trade_ids:
            self._record("TRADE_IGNORED", trade_id=trade.trade_id, reason="duplicate trade ID")
            return None
        if not isinstance(trade.buyer_is_maker, bool):
            raise ValueError("buyer_is_maker must be a boolean")
        if not trade.quantity.is_finite() or trade.quantity <= ZERO:
            raise ValueError("Trade quantity must be positive and finite")

        fill = self._match_trade(trade)
        self._processed_trade_ids.add(trade.trade_id)
        return fill

    def _match_trade(self, trade: Trade) -> Fill | None:
        side = Side.BUY if trade.buyer_is_maker else Side.SELL
        tracked = self._working.get(side)
        if tracked is None:
            self._record("TRADE_IGNORED", trade_id=trade.trade_id, reason=f"no working {side.value} order")
            return None
        if trade.price != tracked.order.limit_price:
            self._record(
                "TRADE_IGNORED", trade_id=trade.trade_id, order_id=tracked.order.order_id,
                reason="trade price differs from order price",
            )
            return None

        original_queue = tracked.queue_ahead
        assert original_queue is not None
        model = tracked.cancellation_queue
        restored = model.late_restoration(trade.quantity) if model is not None else ZERO
        queue_ahead = original_queue + restored if restored else original_queue
        overflow = trade.quantity - queue_ahead
        if overflow <= ZERO:
            self._reconcile_trade(tracked, trade, restored)
            tracked.queue_ahead = -overflow
            self._record(
                "QUEUE_ADVANCED",
                order_id=tracked.order.order_id,
                trade_id=trade.trade_id,
                trade_quantity=trade.quantity,
                queue_before=queue_ahead,
                queue_after=tracked.queue_ahead,
                fill_quantity=ZERO,
            )
            return None

        order = tracked.order
        fill_quantity = min(overflow, order.original_quantity - order.filled_quantity)
        filled_quantity = order.filled_quantity + fill_quantity
        fee = self._wallet.settle_fill(order.order_id, price=trade.price, quantity=fill_quantity)
        self._reconcile_trade(tracked, trade, restored)

        tracked.queue_ahead = ZERO
        order.filled_quantity = filled_quantity
        if filled_quantity == order.original_quantity:
            order.status = OrderStatus.FILLED
            tracked.cancellation_queue = None
            del self._working[side]
        elif order.status is not OrderStatus.CANCEL_PENDING:
            order.status = OrderStatus.PARTIALLY_FILLED

        self._record_wallet(
            "FILL",
            order_id=order.order_id,
            trade_id=trade.trade_id,
            side=side.value,
            price=trade.price,
            trade_quantity=trade.quantity,
            queue_before=queue_ahead,
            queue_after=tracked.queue_ahead,
            fill_quantity=fill_quantity,
            fee_usdt=fee,
            status=order.status.value,
        )
        return Fill(
            order_id=order.order_id,
            trade_id=trade.trade_id,
            timestamp_ms=trade.timestamp_ms,
            side=side,
            price=trade.price,
            quantity=fill_quantity,
            fee_usdt=fee,
        )

    def _reconcile_trade(self, tracked: _TrackedOrder, trade: Trade, restored: Decimal) -> None:
        model = tracked.cancellation_queue
        if model is None:
            return
        model.trade(trade.timestamp_ms, trade.quantity, restored)
        if restored:
            ahead = tracked.queue_ahead
            assert ahead is not None
            self._record(
                "QUEUE_CANCELLATION_REVERSED", order_id=tracked.order.order_id, trade_id=trade.trade_id,
                queue_before=ahead, queue_after=ahead + restored,
            )

    def _record(self, kind: str, **details: AuditValue) -> None:
        if self._audit_log is not None and self._clock_ms is not None:
            self._audit_log.append(AuditEvent(self._clock_ms, kind, tuple(details.items())))

    def _record_wallet(self, kind: str, **details: AuditValue) -> None:
        if self._audit_log is None:
            return
        balances = self._wallet.balances
        self._record(
            kind,
            **details,
            btc=balances.btc,
            usdt=balances.usdt,
            reserved_btc=balances.reserved_btc,
            reserved_usdt=balances.reserved_usdt,
        )
