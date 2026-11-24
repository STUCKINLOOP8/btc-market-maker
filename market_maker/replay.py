import argparse
import json
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from itertools import count
from pathlib import Path

from .binance_replay import load_binance_replay
from .execution import DEFAULT_LATENCY_MS, UNRESOLVED_STATUSES, AuditEvent, AuditValue, Fill, FillEngine
from .models import (
    BookSnapshot,
    CancelOrder,
    ClockAdvance,
    DepthUpdate,
    FeedUnavailable,
    Order,
    ReplayEvent,
    Side,
    SubmitOrder,
    Trade,
    order_events,
)
from .parsing import read_decimal as _read_decimal
from .parsing import read_integer as _read_integer
from .parsing import read_levels as _levels
from .quote_filters import DEFAULT_FILTERS, BinanceQuoteFilters, QuoteFilterError, load_binance_quote_filters
from .queue_model import QUEUE_MODELS, QueueModel
from .risk import InventoryLimitError
from .strategy import (
    AS_HORIZON_SECONDS,
    AS_MODEL_LOT_BTC,
    DEFAULT_QUOTE_SIZE_BTC,
    DEFAULT_SPREAD_USDT,
    IMBALANCE_DEPTH_LEVELS,
    MAX_IMBALANCE_TICKS,
    STRATEGY_TYPES,
    AvellanedaStoikovImbalanceStrategy,
    AvellanedaStoikovStrategy,
    Quote,
    QuoteContext,
    Strategy,
    create_strategy,
)
from .volatility import VOLATILITY_WINDOW_SECONDS, MidpointReturnVariance
from .wallet import DEFAULT_MAKER_FEE_RATE, InsufficientFundsError, Wallet, WalletBalances


MARKOUT_DELAY_MS = 5000
DEFAULT_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "milestone_1_replay.json"
DEFAULT_STARTING_BTC = Decimal("0.50")
DEFAULT_STARTING_USDT = Decimal("34225")


@dataclass(frozen=True, slots=True)
class ReplayResult:
    audit_log: tuple[AuditEvent, ...]
    balances: WalletBalances
    fees_usdt: Decimal
    net_pnl_usdt: Decimal | None
    excess_pnl_usdt: Decimal | None
    pending_markouts: int


@dataclass(frozen=True, slots=True)
class Portfolio:
    mid_price: Decimal | None
    equity_usdt: Decimal | None
    hold_equity_usdt: Decimal | None
    net_pnl_usdt: Decimal | None
    excess_pnl_usdt: Decimal | None
    fees_usdt: Decimal
    inventory_delta_btc: Decimal
    filled_volume_btc: Decimal


@dataclass(frozen=True, slots=True)
class ReplayStep:
    """One processed input and all audit entries added by that step."""

    event: ReplayEvent
    audit_log: tuple[AuditEvent, ...]


class ReplaySession:
    """Replay ordered inputs at their recorded times through one paper engine."""

    def __init__(
        self,
        events: Iterable[ReplayEvent],
        wallet: Wallet,
        *,
        latency_ms: int = DEFAULT_LATENCY_MS,
        strategy: Strategy | None = None,
        quote_filters: BinanceQuoteFilters | None = None,
        fee_guard: bool | None = None,
        target_btc: Decimal | None = None,
        queue_model: QueueModel = "trade-only",
        live: bool = False,
    ) -> None:
        self._audit_log: list[AuditEvent] = []
        self._wallet = wallet
        self._starting_balances = wallet.balances
        self._target_btc = self._starting_balances.btc if target_btc is None else target_btc
        self._engine = FillEngine(
            wallet, latency_ms=latency_ms, target_btc=self._target_btc, audit_log=self._audit_log,
            queue_model=queue_model,
        )
        self._events = deque(order_events(events))
        self._live = live
        if strategy is not None and any(isinstance(event, (SubmitOrder, CancelOrder)) for event in self._events):
            raise ValueError("Strategy replay requires market events without scripted orders")
        if strategy is None and (quote_filters is not None or fee_guard):
            raise ValueError("Quote filters and an enabled fee guard require strategy replay")
        self._strategy = strategy
        self._midpoint_variance = MidpointReturnVariance() if strategy is not None else None
        self._quote_filters = (quote_filters or load_binance_quote_filters()) if strategy is not None else None
        self._fee_guard = strategy is not None if fee_guard is None else fee_guard
        self._quote_due = False
        self._refresh_after: dict[Side, int] = {}
        self._order_ids = count(1)
        self._last_event: ReplayEvent | None = None
        self._opening_mid: Decimal | None = None
        self._fees = Decimal("0")
        self._filled_volume_btc = Decimal("0")
        self._feed_epoch = 0
        self._pending: deque[tuple[Fill, Decimal | None, int]] = deque()

    @property
    def book(self) -> BookSnapshot | None:
        """Return the displayed depth when a snapshot is available."""
        return self.book_snapshot()

    def book_snapshot(self, *, limit: int | None = None) -> BookSnapshot | None:
        """Copy displayed depth, optionally limited to the best levels on each side."""
        if self._last_event is None:
            return None
        return self._engine.book_snapshot(self._last_event.timestamp_ms, self._last_event.sequence, limit=limit)

    @property
    def next_timestamp_ms(self) -> int | None:
        recorded_time = self._events[0].timestamp_ms if self._events else None
        if not self._engine.halt_requested:
            if recorded_time is None or not self._refresh_after:
                return recorded_time
            return min(recorded_time, min(self._refresh_after.values()))
        if not self.unresolved_order_count:
            return None
        action_time = self._engine.next_action_ms
        assert action_time is not None, "An unresolved order must have a scheduled action during halt"
        return action_time if recorded_time is None else min(action_time, recorded_time)

    @property
    def unresolved_order_count(self) -> int:
        return self._engine.unresolved_order_count

    def halt(self) -> int:
        """Begin settling outstanding orders at the current replay time."""
        self._engine.halt()
        timestamp_ms = self._engine.clock_ms
        assert timestamp_ms is not None
        return timestamp_ms

    @property
    def balances(self) -> WalletBalances:
        return self._wallet.balances

    @property
    def starting_balances(self) -> WalletBalances:
        return self._starting_balances

    @property
    def orders(self) -> tuple[Order, ...]:
        return self._engine.orders

    def queue_ahead(self, order_id: str) -> Decimal | None:
        return self._engine.queue_ahead(order_id)

    @property
    def audit_count(self) -> int:
        return len(self._audit_log)

    def audit_since(self, offset: int, limit: int) -> tuple[AuditEvent, ...]:
        """Copy one page of audit entries beginning at a zero-based offset."""
        return tuple(self._audit_log[offset:offset + limit])

    def append_live_events(self, events: Iterable[BookSnapshot | DepthUpdate | FeedUnavailable | Trade | ClockAdvance]) -> None:
        """Queue new market inputs in receive order before stepping the live clock."""
        if not self._live:
            raise ValueError("Only a live paper session accepts new inputs")
        previous = self._events[-1] if self._events else self._last_event
        for event in events:
            if previous is not None and (event.timestamp_ms, event.sequence) < (previous.timestamp_ms, previous.sequence):
                raise ValueError("Live market inputs must be in receive order")
            self._events.append(event)
            previous = event

    def step(self) -> ReplayStep | None:
        """Apply one input or halt settlement action and return its audit entries."""
        timestamp_ms = self.next_timestamp_ms
        if timestamp_ms is None:
            return None

        has_input = bool(self._events and self._events[0].timestamp_ms == timestamp_ms)
        if has_input:
            event = self._events.popleft()
        else:
            sequence = self._last_event.sequence if self._last_event is not None else 0
            event = ClockAdvance(timestamp_ms, sequence)
        audit_start = len(self._audit_log)
        # Settlement can advance beyond the tape, where there is no market data for markouts.
        within_recording = bool(self._events) or not isinstance(event, ClockAdvance) or (self._live and has_input)
        if within_recording:
            self._emit_due_markouts(event.timestamp_ms - 1)
        if isinstance(event, FeedUnavailable):
            self._feed_epoch += 1
            self._refresh_after.clear()
            if self._midpoint_variance is not None:
                self._midpoint_variance.reset()
        self._process_event(event)
        for side, deadline in tuple(self._refresh_after.items()):
            if deadline <= event.timestamp_ms:
                del self._refresh_after[side]
                self._quote_due = True
        if self._strategy is not None and isinstance(event, (BookSnapshot, DepthUpdate)):
            self._quote_due = True

        # A markout at this exact millisecond must see all inputs at that time.
        if within_recording and self.next_timestamp_ms != event.timestamp_ms:
            self._emit_due_markouts(event.timestamp_ms)
            if self._quote_due:
                self._quote_due = False
                self._submit_strategy_quotes(event)
        self._last_event = event
        return ReplayStep(event, tuple(self._audit_log[audit_start:]))

    def _process_event(self, event: ReplayEvent) -> None:
        try:
            fill = self._engine.process_event(event)
        except (InventoryLimitError, InsufficientFundsError):
            # The engine has already audited the expected pre-trade rejection.
            return
        if self._opening_mid is None:
            self._opening_mid = self._engine.mid_price
        if fill is not None:
            self._fees += fill.fee_usdt
            self._filled_volume_btc += fill.quantity
            self._pending.append((fill, self._engine.mid_price, self._feed_epoch))

    def _submit_strategy_quotes(self, event: ReplayEvent) -> None:
        assert self._strategy is not None
        assert self._quote_filters is not None
        assert self._midpoint_variance is not None
        if self._engine.halt_requested:
            return
        mid = self._engine.mid_price
        variance = self._midpoint_variance.observe(event.timestamp_ms, mid)
        if mid is None:
            return
        book = self._engine.book_snapshot(event.timestamp_ms, event.sequence, limit=IMBALANCE_DEPTH_LEVELS)
        assert book is not None
        outstanding = {order.side: order for order in self.orders if order.status in UNRESOLVED_STATUSES}
        context = QuoteContext(
            book, mid, self.balances, tuple(outstanding.values()), self._target_btc, variance,
            self._quote_filters.tick_size,
        )
        seen_sides = set(self._refresh_after)
        finalized: dict[Side, tuple[Quote, Quote]] = {}
        for proposed in self._strategy.quote(context):
            if proposed.side in seen_sides:
                continue
            seen_sides.add(proposed.side)
            quote = proposed
            try:
                quote = self._quote_filters.round_quote(proposed)
                self._quote_filters.check(quote)
            except QuoteFilterError as error:
                order_id = self._record_quote_proposal(event.timestamp_ms, mid, proposed, quote)
                self._record_quote_skip(event.timestamp_ms, order_id, quote, str(error))
                continue
            current = outstanding.get(quote.side)
            if current is not None and (quote.limit_price, quote.quantity) == (
                current.limit_price, current.original_quantity,
            ):
                continue
            finalized[quote.side] = (proposed, quote)
        if not finalized:
            return

        proposed_prices = {side: order.limit_price for side, order in outstanding.items()}
        proposed_prices.update({side: quote.limit_price for side, (_, quote) in finalized.items()})
        replacement_reason = self._fee_guard_reason(proposed_prices) if self._fee_guard else None
        for side, (proposed, quote) in finalized.items():
            order = outstanding.get(side)
            if order is None:
                continue
            if replacement_reason is not None:
                order_id = self._record_quote_proposal(event.timestamp_ms, mid, proposed, quote)
                self._record_quote_skip(event.timestamp_ms, order_id, quote, replacement_reason)
                continue
            deadline = self._engine.schedule_cancel(order.order_id, reason="quote price or size changed")
            if deadline == event.timestamp_ms:
                del outstanding[side]
            elif deadline is not None:
                self._refresh_after[side] = deadline

        ready = {side: quotes for side, quotes in finalized.items() if side not in outstanding}
        prices = {side: order.limit_price for side, order in outstanding.items()}
        prices.update({side: quote.limit_price for side, (_, quote) in ready.items()})
        fee_reason = self._fee_guard_reason(prices) if self._fee_guard and ready else None
        for proposed, quote in ready.values():
            order_id = self._record_quote_proposal(event.timestamp_ms, mid, proposed, quote)
            if fee_reason is not None:
                self._record_quote_skip(event.timestamp_ms, order_id, quote, fee_reason)
                continue
            self._process_event(
                SubmitOrder(
                    event.timestamp_ms, event.sequence, order_id, quote.side, quote.limit_price, quote.quantity,
                    post_only=True,
                )
            )

    def _record_quote_proposal(self, timestamp_ms: int, mid: Decimal, proposed: Quote, rounded: Quote) -> str:
        order_id = f"strategy-{next(self._order_ids)}"
        self._audit_log.append(AuditEvent(timestamp_ms, "QUOTE_PROPOSED", (
            ("order_id", order_id),
            ("side", proposed.side.value),
            ("mid_price", mid),
            ("limit_price", proposed.limit_price),
            ("quantity", proposed.quantity),
        )))
        if rounded != proposed:
            self._audit_log.append(AuditEvent(timestamp_ms, "QUOTE_ROUNDED", (
                ("order_id", order_id),
                ("side", rounded.side.value),
                ("limit_price", rounded.limit_price),
                ("quantity", rounded.quantity),
            )))
        return order_id

    def _fee_guard_reason(self, prices: dict[Side, Decimal]) -> str | None:
        bid, ask = prices.get(Side.BUY), prices.get(Side.SELL)
        if bid is None or ask is None:
            return "Maker-fee guard requires a valid bid and ask"
        spread = ask - bid
        fees = (bid + ask) * self._wallet.maker_fee_rate
        if spread < fees:
            return f"Maker-fee guard: spread {spread:f} is below round-trip fees {fees:f} USDT per BTC"
        return None

    def _record_quote_skip(self, timestamp_ms: int, order_id: str, quote: Quote, reason: str) -> None:
        self._audit_log.append(AuditEvent(timestamp_ms, "ORDER_SKIPPED", (
            ("order_id", order_id),
            ("side", quote.side.value),
            ("price", quote.limit_price),
            ("quantity", quote.quantity),
            ("reason", reason),
        )))

    @property
    def portfolio(self) -> Portfolio:
        """Mark the current and starting wallets at the replay's current midpoint."""
        return self._value_portfolio(self.balances)

    def _value_portfolio(self, balances: WalletBalances) -> Portfolio:
        mid = self._engine.mid_price
        starting = self._starting_balances
        equity = hold = net_pnl = excess_pnl = None
        if mid is not None:
            equity = balances.usdt + balances.btc * mid
            hold = starting.usdt + starting.btc * mid
            if self._opening_mid is not None:
                net_pnl = equity - (starting.usdt + starting.btc * self._opening_mid)
                excess_pnl = equity - hold
        return Portfolio(mid, equity, hold, net_pnl, excess_pnl, self._fees,
                         balances.btc - self._target_btc, self._filled_volume_btc)

    def result(self) -> ReplayResult:
        """Report the audit, balances, and P&L at the current replay position."""
        balances = self.balances
        portfolio = self._value_portfolio(balances)
        return ReplayResult(
            tuple(self._audit_log), balances, portfolio.fees_usdt, portfolio.net_pnl_usdt,
            portfolio.excess_pnl_usdt, len(self._pending),
        )

    def _emit_due_markouts(self, through_ms: int) -> None:
        while self._pending and self._pending[0][0].timestamp_ms + MARKOUT_DELAY_MS <= through_ms:
            fill, mid_at_fill, fill_epoch = self._pending.popleft()
            due_ms = fill.timestamp_ms + MARKOUT_DELAY_MS
            self._engine.advance_to(due_ms)
            mid_at_horizon = self._engine.mid_price if fill_epoch == self._feed_epoch else None
            markout = None
            if mid_at_fill is not None and mid_at_horizon is not None:
                markout = mid_at_horizon - mid_at_fill
                if fill.side is Side.SELL:
                    markout = -markout
            self._audit_log.append(
                AuditEvent(
                    due_ms,
                    "MARKOUT_5S",
                    (
                        ("order_id", fill.order_id),
                        ("trade_id", fill.trade_id),
                        ("mid_at_fill", mid_at_fill),
                        ("mid_at_horizon", mid_at_horizon),
                        ("markout_usdt_per_btc", markout),
                    ),
                )
            )

def load_fixture(path: str | Path) -> list[ReplayEvent]:
    """Read events from a JSON fixture and return them in replay order."""
    with Path(path).open(encoding="utf-8") as fixture:
        records = json.load(fixture)
    return order_events(_decode_event(record) for record in records)


def run_replay(
    events: Iterable[ReplayEvent],
    wallet: Wallet,
    *,
    latency_ms: int = DEFAULT_LATENCY_MS,
    strategy: Strategy | None = None,
    quote_filters: BinanceQuoteFilters | None = None,
    fee_guard: bool | None = None,
    target_btc: Decimal | None = None,
    queue_model: QueueModel = "trade-only",
) -> ReplayResult:
    """Run through the last input timestamp and report fills, markouts, and P&L."""
    session = ReplaySession(
        events, wallet, latency_ms=latency_ms, strategy=strategy, quote_filters=quote_filters,
        fee_guard=fee_guard, target_btc=target_btc, queue_model=queue_model,
    )
    while session.step() is not None:
        pass
    return session.result()


def _decode_event(record: dict) -> ReplayEvent:
    timestamp_ms = _read_integer(record["timestamp_ms"], "timestamp_ms")
    sequence = _read_integer(record["sequence"], "sequence")
    event_type = record["type"]

    match event_type:
        case "book_snapshot":
            return BookSnapshot(timestamp_ms, sequence, _levels(record["bids"]), _levels(record["asks"]))
        case "depth_update":
            return DepthUpdate(timestamp_ms, sequence, _levels(record["bids"]), _levels(record["asks"]))
        case "trade":
            buyer_is_maker = record["buyer_is_maker"]
            if not isinstance(buyer_is_maker, bool):
                raise ValueError("buyer_is_maker must be a JSON boolean")
            return Trade(
                timestamp_ms=timestamp_ms,
                sequence=sequence,
                trade_id=_read_integer(record["trade_id"], "trade_id"),
                price=_read_decimal(record["price"], "price"),
                quantity=_read_decimal(record["quantity"], "quantity"),
                buyer_is_maker=buyer_is_maker,
            )
        case "submit_order":
            post_only = record.get("post_only", False)
            if not isinstance(post_only, bool):
                raise ValueError("post_only must be a JSON boolean")
            return SubmitOrder(
                timestamp_ms=timestamp_ms,
                sequence=sequence,
                order_id=record["order_id"],
                side=Side(record["side"]),
                limit_price=_read_decimal(record["limit_price"], "limit_price"),
                quantity=_read_decimal(record["quantity"], "quantity"),
                post_only=post_only,
            )
        case "cancel_order":
            return CancelOrder(timestamp_ms, sequence, record["order_id"])

    raise ValueError(f"Unknown event type: {event_type!r}")


def _display(value: AuditValue) -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, Decimal):
        formatted = format(value, "f")
        return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted
    if isinstance(value, str) and " " in value:
        return repr(value)
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay a BTC/USDT fixture or a Binance JSONL recording.")
    parser.add_argument("fixture", nargs="?", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--orders", type=Path, help="JSON file of scripted orders for a Binance recording")
    parser.add_argument("--btc", type=Decimal, default=DEFAULT_STARTING_BTC, help="starting BTC balance")
    parser.add_argument("--target-btc", type=Decimal, help="inventory target in BTC (defaults to starting BTC)")
    parser.add_argument("--usdt", type=Decimal, default=DEFAULT_STARTING_USDT, help="starting USDT balance")
    parser.add_argument("--maker-fee", type=Decimal, default=DEFAULT_MAKER_FEE_RATE, help="decimal fee rate")
    parser.add_argument("--latency-ms", type=int, default=DEFAULT_LATENCY_MS)
    parser.add_argument("--queue-model", choices=QUEUE_MODELS, default="trade-only")
    parser.add_argument(
        "--strategy", choices=STRATEGY_TYPES,
        help="generate quotes on unscripted market data",
    )
    parser.add_argument("--spread-usdt", type=Decimal, help="full fixed spread in USDT per BTC (default 100)")
    parser.add_argument("--quote-size-btc", type=Decimal, help="order quantity in BTC per side (default 0.001)")
    parser.add_argument("--filters", type=Path, help="saved BTCUSDT Binance exchangeInfo for strategy quotes")
    parser.add_argument("--fee-guard", choices=("on", "off"), help="round-trip maker-fee guard (on for strategy runs)")
    parser.add_argument("--risk-aversion", type=Decimal, help="Avellaneda–Stoikov gamma in inverse USDT (default 0.01)")

    try:
        args = parser.parse_args(argv)
        strategy = None
        quote_filters = None
        if args.strategy is not None:
            if args.orders is not None:
                raise ValueError("--strategy and --orders are mutually exclusive")
            spread = DEFAULT_SPREAD_USDT if args.spread_usdt is None else args.spread_usdt
            quantity = DEFAULT_QUOTE_SIZE_BTC if args.quote_size_btc is None else args.quote_size_btc
            strategy = create_strategy(args.strategy, spread, quantity, risk_aversion=args.risk_aversion)
            quote_filters = load_binance_quote_filters(args.filters or DEFAULT_FILTERS)
        elif args.risk_aversion is not None:
            raise ValueError("--risk-aversion requires an Avellaneda–Stoikov strategy")
        elif args.spread_usdt is not None or args.quote_size_btc is not None:
            raise ValueError("--spread-usdt and --quote-size-btc require --strategy")
        elif args.filters is not None:
            raise ValueError("--filters requires --strategy")
        wallet = Wallet(btc=args.btc, usdt=args.usdt, maker_fee_rate=args.maker_fee)
        if args.fixture.suffix.lower() == ".jsonl":
            orders = load_fixture(args.orders) if args.orders is not None else ()
            events = load_binance_replay(args.fixture, orders)
        else:
            if args.orders is not None:
                raise ValueError("--orders requires a Binance JSONL recording")
            events = load_fixture(args.fixture)
        result = run_replay(
            events, wallet, latency_ms=args.latency_ms, strategy=strategy, quote_filters=quote_filters,
            fee_guard=None if args.fee_guard is None else args.fee_guard == "on", target_btc=args.target_btc,
            queue_model=args.queue_model,
        )
    except (InvalidOperation, OSError, ValueError, KeyError) as error:
        parser.error(str(error))

    strategy_config = ""
    if strategy is not None:
        strategy_config = (
            f" strategy={args.strategy} spread_usdt={strategy.spread_usdt} quote_size_btc={strategy.quote_size_btc}"
            f" filters={(args.filters or DEFAULT_FILTERS).name} fee_guard={args.fee_guard or 'on'}"
        )
    elif args.fee_guard is not None:
        strategy_config = f" fee_guard={args.fee_guard}"
    if isinstance(strategy, AvellanedaStoikovStrategy):
        strategy_config += (
            f" risk_aversion={strategy.risk_aversion} horizon_seconds={AS_HORIZON_SECONDS}"
            f" model_lot_btc={AS_MODEL_LOT_BTC} volatility_window_seconds={VOLATILITY_WINDOW_SECONDS}"
        )
    if isinstance(strategy, AvellanedaStoikovImbalanceStrategy):
        strategy_config += f" imbalance_depth_levels={IMBALANCE_DEPTH_LEVELS} imbalance_limit_ticks={MAX_IMBALANCE_TICKS}"
    if args.target_btc is not None or isinstance(strategy, AvellanedaStoikovStrategy):
        strategy_config += f" target_btc={args.btc if args.target_btc is None else args.target_btc}"
    if args.queue_model != "trade-only":
        strategy_config += f" queue_model={args.queue_model}"
    print(
        f"REPLAY {args.fixture.name} starting_btc={args.btc} starting_usdt={args.usdt} "
        f"maker_fee_rate={args.maker_fee} latency_ms={args.latency_ms}{strategy_config}"
    )
    for event in result.audit_log:
        details = " ".join(f"{key}={_display(value)}" for key, value in event.details)
        print(f"[{event.timestamp_ms} ms] {event.kind} {details}")
    balances = result.balances
    net = "unavailable" if result.net_pnl_usdt is None else f"{result.net_pnl_usdt:.2f}"
    excess = "unavailable" if result.excess_pnl_usdt is None else f"{result.excess_pnl_usdt:.2f}"
    print(
        f"SUMMARY btc={_display(balances.btc)} usdt={_display(balances.usdt)} "
        f"reserved_btc={_display(balances.reserved_btc)} reserved_usdt={_display(balances.reserved_usdt)} "
        f"fees_usdt={_display(result.fees_usdt)} net_pnl_usdt={net} "
        f"excess_pnl_usdt={excess} pending_markouts={result.pending_markouts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
