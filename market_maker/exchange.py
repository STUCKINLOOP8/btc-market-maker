"""Run the quote strategy against exchange-confirmed BTC/USDT orders."""

import asyncio
import sqlite3
from collections.abc import Callable
from decimal import Decimal, DecimalException
from pathlib import Path
from uuid import uuid4

from .binance_spot import AsyncOrderJournal, BinanceError, BinanceSpot
from .execution import AuditEvent, AuditValue, UNRESOLVED_STATUSES
from .models import BookSnapshot, DepthUpdate, FeedUnavailable, Order, OrderStatus, Side, SubmitOrder
from .order_book import OrderBook
from .parsing import read_decimal
from .quote_filters import BinanceQuoteFilters, QuoteFilterError, parse_binance_quote_filters
from .record import DEFAULT_DEPTH_TIMEOUT_SECONDS, LiveMarketEvent, stream_binance
from .replay import Portfolio
from .risk import InventoryLimit, InventoryLimitError
from .strategy import IMBALANCE_DEPTH_LEVELS, QuoteContext, Strategy
from .volatility import MidpointReturnVariance
from .wallet import WalletBalances, ZERO


SYMBOL = "BTCUSDT"
POLL_SECONDS = 1
ORDER_PREFIX = "pmm-"
_ORDER_STATUS = {
    "PENDING_NEW": OrderStatus.PENDING_NEW,
    "NEW": OrderStatus.ACTIVE,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "PENDING_CANCEL": OrderStatus.CANCEL_PENDING,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "EXPIRED": OrderStatus.CANCELED,
    "EXPIRED_IN_MATCH": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
}


def _balances(account: dict) -> WalletBalances:
    funds = {
        entry["asset"]: (
            read_decimal(entry["free"], "free balance", allow_zero=True),
            read_decimal(entry["locked"], "locked balance", allow_zero=True),
        )
        for entry in account["balances"]
    }
    btc, locked_btc = funds.get("BTC", (ZERO, ZERO))
    usdt, locked_usdt = funds.get("USDT", (ZERO, ZERO))
    return WalletBalances(btc + locked_btc, usdt + locked_usdt, locked_btc, locked_usdt)


def _maker_rate(data: dict) -> Decimal:
    return sum(
        (
            read_decimal(rates["maker"], "maker commission", allow_zero=True)
            + max(read_decimal(rates[side], side + " commission", allow_zero=True) for side in ("buyer", "seller"))
            for rates in (data[key] for key in ("standardCommission", "specialCommission", "taxCommission"))
        ),
        start=ZERO,
    )


class ExchangeSession:
    """Poll private exchange state separately from the public market feed."""

    @classmethod
    async def start(
        cls,
        root: Path,
        client: BinanceSpot,
        strategy: Strategy,
        *,
        max_order_usdt: Decimal,
        max_drawdown_usdt: Decimal,
        publish: Callable[[], None],
    ) -> "ExchangeSession":
        journal = AsyncOrderJournal()
        try:
            pending = await journal.open(root, client.environment)
            return cls(
                client, strategy, journal, pending, max_order_usdt=max_order_usdt,
                max_drawdown_usdt=max_drawdown_usdt, publish=publish,
            )
        except BaseException:
            await journal.close()
            raise

    def __init__(
        self,
        client: BinanceSpot,
        strategy: Strategy,
        journal: AsyncOrderJournal,
        pending: list[tuple[str, str, int | None]],
        *,
        max_order_usdt: Decimal,
        max_drawdown_usdt: Decimal,
        publish: Callable[[], None],
    ) -> None:
        self.client = client
        self.strategy = strategy
        self.max_order_usdt = max_order_usdt
        self.max_drawdown_usdt = max_drawdown_usdt
        self.publish = publish
        self.run_id = uuid4().hex
        self.status = "starting"
        self.error: str | None = None
        self.started_at = asyncio.get_running_loop().time()
        self.book = OrderBook()
        self.depth: BookSnapshot | None = None
        self.last_depth_ms: int | None = None
        self.feed_reason = "awaiting_snapshot"
        self.position = 0
        self.orders: dict[str, Order] = {}
        self.balances: WalletBalances | None = None
        self.starting_balances: WalletBalances | None = None
        self.audit: list[AuditEvent] = []
        self.equity_history: list[tuple[int, Decimal | None, Decimal | None]] = []
        self.last_fill: dict | None = None
        self.volume = ZERO
        self.fees_usdt = ZERO
        self.peak_equity_usdt: Decimal | None = None
        self.opening_equity_usdt: Decimal | None = None
        self.variance = MidpointReturnVariance()
        self._fills_seen: dict[int, set[int]] = {}
        self._fills_quantity: dict[int, Decimal] = {}
        self._awaiting_close: set[int] = set()
        self._feed_lost = False
        self._urgent = asyncio.Event()
        self._filters: BinanceQuoteFilters | None = None
        self._risk: InventoryLimit | None = None
        self._rate: Decimal | None = None
        self._account: str | None = None
        self._sync_clock = True
        self._journal = journal
        self._owned = {client_id: (account, exchange_id) for client_id, account, exchange_id in pending}
        if self._owned:
            self.halt("Recovering outstanding orders from a previous process")
        self.task = asyncio.create_task(self._run())

    @property
    def elapsed_ms(self) -> int:
        return int((asyncio.get_running_loop().time() - self.started_at) * 1000)

    @property
    def can_replace(self) -> bool:
        return self.status in ("halted", "failed")

    @property
    def unresolved_order_count(self) -> int:
        return len(self._owned) + len(self._awaiting_close)

    @property
    def portfolio(self) -> Portfolio:
        mid, balances, opening = self.book.mid_price, self.balances, self.starting_balances
        equity = balances.usdt + balances.btc * mid if balances is not None and mid is not None else None
        hold = opening.usdt + opening.btc * mid if opening is not None and mid is not None else None
        opening_value = self.opening_equity_usdt
        return Portfolio(
            mid, equity, hold,
            equity - opening_value if equity is not None and opening_value is not None else None,
            equity - hold if equity is not None and hold is not None else None,
            self.fees_usdt,
            balances.btc - opening.btc if balances is not None and opening is not None else ZERO,
            self.volume,
        )

    def halt(self, reason: str | None = None) -> None:
        if not self.can_replace and self.status != "halting":
            self.status = "halting"
            self._urgent.set()
            if reason:
                self.error = reason
            self._record("HALT_REQUESTED", reason=reason)
            self.publish()

    async def close(self) -> None:
        self.halt()
        try:
            await asyncio.wait_for(asyncio.shield(self.task), timeout=15)
        except TimeoutError:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def _record(self, kind: str, **details: AuditValue) -> None:
        self.audit.append(AuditEvent(self.elapsed_ms, kind, tuple(details.items())))

    def _on_market(self, event: LiveMarketEvent) -> None:
        self.position += 1
        match event:
            case FeedUnavailable():
                self.book = OrderBook()
                self.depth = None
                self.feed_reason = event.reason
                self.variance.reset()
                if event.reason != "awaiting_snapshot":
                    self._feed_lost = True
                    self._urgent.set()
                    self._record("FEED_UNAVAILABLE", reason=event.reason)
            case BookSnapshot() | DepthUpdate():
                self.book.apply(event)
                self.depth = self.book.snapshot(event.timestamp_ms, event.sequence, limit=20)
                self.last_depth_ms = event.timestamp_ms
                self.feed_reason = ""
                self.variance.observe(event.timestamp_ms, self.book.mid_price)

    @property
    def feed_ready(self) -> bool:
        return (
            self.last_depth_ms is not None
            and self.elapsed_ms - self.last_depth_ms <= DEFAULT_DEPTH_TIMEOUT_SECONDS * 1000
            and not self.feed_reason
            and self.book.mid_price is not None
        )

    async def _run(self) -> None:
        feed = None
        final_status = None
        try:
            while final_status is None:
                try:
                    if self._sync_clock:
                        await self.client.sync_time()
                        self._sync_clock = False
                    await self._poll()
                    if self.status == "starting":
                        await self._prepare()
                        if self.status == "starting":
                            self.status = "live"
                            feed = asyncio.create_task(stream_binance(
                                self._on_market, started_at=self.started_at, testnet=True,
                            ))
                    elif self.status == "live":
                        if feed is not None and feed.done():
                            self.halt("The market feed stopped")
                        else:
                            await self._maintain_quotes()
                    if self.status == "halting" and self.unresolved_order_count == 0:
                        final_status = "halted"
                except (BinanceError, sqlite3.Error, ValueError, LookupError, TypeError, AttributeError,
                        DecimalException, OverflowError) as error:
                    if isinstance(error, BinanceError) and error.code == -1021:
                        self._sync_clock = True
                    # Losing private exchange state disables new orders. Retain ownership until reconciliation succeeds.
                    if self.unresolved_order_count or self.status == "halting":
                        self.halt(str(error))
                        self.error = str(error)
                    else:
                        final_status = "failed"
                        self.error = str(error)
                if self.status == "halting" and self._account is not None:
                    try:
                        await self._cancel_working()
                    except (BinanceError, sqlite3.Error, ValueError, KeyError, TypeError, DecimalException) as error:
                        if isinstance(error, BinanceError) and error.code == -1021:
                            self._sync_clock = True
                        self.error = str(error)
                portfolio = self.portfolio
                values = (portfolio.equity_usdt, portfolio.hold_equity_usdt)
                if self.equity_history:
                    if self.equity_history[-1][1:] != values:
                        self.equity_history.append((self.elapsed_ms, *values))
                elif values[0] is not None:
                    self.equity_history.append((self.elapsed_ms, *values))
                self.publish()
                if final_status is None:
                    try:
                        await asyncio.wait_for(self._urgent.wait(), timeout=POLL_SECONDS)
                    except TimeoutError:
                        pass
                    self._urgent.clear()
        finally:
            if feed is not None:
                feed.cancel()
            await self._journal.close()
            if feed is not None:
                await asyncio.gather(feed, return_exceptions=True)
            if final_status is not None:
                self.status = final_status
                if final_status == "halted":
                    self._record("HALTED")
                self.publish()

    async def _prepare(self) -> None:
        info = await self.client.request("GET", "/api/v3/exchangeInfo", signed=False, symbol=SYMBOL)
        symbol = next((item for item in info["symbols"] if item["symbol"] == SYMBOL), None)
        if symbol is None or symbol["status"] != "TRADING" or "LIMIT_MAKER" not in symbol["orderTypes"]:
            raise ValueError("BTCUSDT post-only orders are unavailable on this exchange")
        self._filters = parse_binance_quote_filters(info)
        self._rate = _maker_rate(await self.client.request("GET", "/api/v3/account/commission", symbol=SYMBOL))
        assert self.balances is not None
        self.starting_balances = self.balances
        self._risk = InventoryLimit(self.balances)

    async def _poll(self) -> None:
        account = await self.client.request("GET", "/api/v3/account")
        uid = account["uid"]
        if type(uid) not in (int, str) or not str(uid):
            raise ValueError("Binance returned an invalid account identifier")
        account_id = str(uid)
        if any(owner != account_id for owner, _ in self._owned.values()):
            self._account = None
            raise ValueError("Pending bot orders belong to a different Binance account")
        if not account["canTrade"] and self.status != "halting":
            raise ValueError("Trading is disabled for the Binance account")
        self._account = account_id

        open_orders = await self.client.request("GET", "/api/v3/openOrders", symbol=SYMBOL)
        by_id = {row["orderId"]: row for row in open_orders}
        by_client = {row["clientOrderId"]: row for row in open_orders}
        self._awaiting_close.intersection_update(by_id)
        other_orders = set(by_id) - {order_id for _, order_id in self._owned.values()}
        other_orders -= {by_client[key]["orderId"] for key in self._owned if key in by_client}
        other_orders -= self._awaiting_close
        if other_orders and self.status in ("starting", "live"):
            self.halt("Found BTCUSDT orders outside this bot. Clear them before starting exchange trading.")

        tracked = tuple(self._owned.items())
        failure = None
        for key, (owner, order_id) in tracked:
            try:
                row = by_id.get(order_id) or by_client.get(key)
                if row is None:
                    identity = {"orderId": order_id} if order_id is not None else {"origClientOrderId": key}
                    row = await self.client.request("GET", "/api/v3/order", symbol=SYMBOL, **identity)
                await self._apply_order(key, owner, row)
            except (BinanceError, ValueError, KeyError, TypeError, DecimalException) as error:
                failure = error
        if failure is not None:
            raise failure
        if tracked:
            account = await self.client.request("GET", "/api/v3/account")
        self.balances = _balances(account)

    async def _apply_order(self, key: str, owner: str, row: dict) -> None:
        exchange_id = row["orderId"]
        known_id = self._owned[key][1]
        if type(exchange_id) is not int or row["symbol"] != SYMBOL or (
            exchange_id != known_id if known_id is not None else row["clientOrderId"] != key
        ):
            raise ValueError("Binance response did not identify the expected bot order")
        if known_id is None:
            await self._journal.identify(key, exchange_id)
            self._owned[key] = (owner, exchange_id)
        price = read_decimal(row["price"], "price")
        quantity = read_decimal(row["origQty"], "original quantity")
        filled = read_decimal(row["executedQty"], "filled quantity", allow_zero=True)
        order = Order(key, Side(row["side"].lower()), price, quantity, filled, _ORDER_STATUS[row["status"]])
        prior = self.orders.get(key)
        self.orders[key] = order
        if prior is None or order.status != prior.status:
            kind = {
                OrderStatus.ACTIVE: "ORDER_ACTIVATED",
                OrderStatus.CANCELED: "ORDER_CANCELED",
                OrderStatus.REJECTED: "ORDER_REJECTED",
            }.get(order.status, "EXCHANGE_ORDER_UPDATED")
            self._record(kind, order_id=key, side=order.side.value, price=price, quantity=quantity,
                         filled_quantity=filled, status=order.status.value)
        if filled > self._fills_quantity.get(exchange_id, ZERO):
            await self._sync_fills(order, exchange_id)
        if order.status not in UNRESOLVED_STATUSES:
            await self._journal.remove(key)
            del self._owned[key]
            self._awaiting_close.add(exchange_id)

    async def _sync_fills(self, order: Order, exchange_id: int) -> None:
        seen = self._fills_seen.setdefault(exchange_id, set())
        total = self._fills_quantity.get(exchange_id, ZERO)
        while total < order.filled_quantity:
            since = {"fromId": max(seen) + 1} if seen else {}
            trades = await self.client.request(
                "GET", "/api/v3/myTrades", symbol=SYMBOL, orderId=exchange_id, limit=1000, **since,
            )
            new = sorted((row for row in trades if row["id"] not in seen), key=lambda row: row["id"])
            if not new:
                raise ValueError("Binance has not returned all fills for a bot order")
            for row in new:
                if row["orderId"] != exchange_id or row["symbol"] != SYMBOL:
                    raise ValueError("Binance returned a fill for an unexpected order")
                quantity = read_decimal(row["qty"], "fill quantity")
                fee = read_decimal(row["commission"], "commission", allow_zero=True)
                price = read_decimal(row["price"], "fill price")
                asset = row["commissionAsset"]
                total += quantity
                self._fills_quantity[exchange_id] = total
                seen.add(row["id"])
                self.volume += quantity
                if asset == "USDT":
                    self.fees_usdt += fee
                details = {
                    "order_id": order.order_id, "side": order.side.value, "trade_id": row["id"],
                    "price": price, "trade_quantity": quantity, "fill_quantity": quantity,
                    "commission": fee, "commission_asset": asset,
                    "status": OrderStatus.FILLED.value if total == order.original_quantity else OrderStatus.PARTIALLY_FILLED.value,
                }
                self._record("FILL", **details)
                self.last_fill = {"timestamp_ms": self.elapsed_ms, **details}

    async def _cancel_working(self, orders: tuple[Order, ...] | None = None) -> None:
        failure = None
        for order in orders if orders is not None else tuple(self.orders.values()):
            key = order.order_id
            if key not in self._owned or order.status not in UNRESOLVED_STATUSES:
                continue
            owner, exchange_id = self._owned[key]
            self._record("CANCEL_REQUESTED", order_id=key)
            try:
                row = await self.client.request("DELETE", "/api/v3/order", symbol=SYMBOL, orderId=exchange_id)
            except BinanceError as error:
                if error.code != -2011:
                    failure = error
                # The order may have filled while cancellation was in flight. Query on the next poll.
            else:
                try:
                    await self._apply_order(key, owner, row)
                except (BinanceError, ValueError, KeyError, TypeError, DecimalException) as error:
                    failure = error
        if failure is not None:
            raise failure

    async def _maintain_quotes(self) -> None:
        if self._feed_lost or not self.feed_ready:
            await self._cancel_working()
            self._feed_lost = bool(self.unresolved_order_count)
            return
        if self._awaiting_close:
            return
        assert self.balances is not None and self.starting_balances is not None
        assert self._filters is not None and self._risk is not None and self._rate is not None
        mid = self.book.mid_price
        assert mid is not None
        equity = self.balances.usdt + self.balances.btc * mid
        if self.opening_equity_usdt is None:
            self.opening_equity_usdt = self.peak_equity_usdt = equity
        assert self.peak_equity_usdt is not None
        self.peak_equity_usdt = max(equity, self.peak_equity_usdt)
        if self.peak_equity_usdt - equity >= self.max_drawdown_usdt:
            self.halt("Exchange session drawdown limit reached")
            return
        self._risk.update_mark(mid)
        working = {order.side: order for key, order in self.orders.items() if key in self._owned}
        for order in working.values():
            remaining = order.original_quantity - order.filled_quantity
            if remaining <= ZERO:
                continue
            try:
                self._risk.check_submission(
                    SubmitOrder(self.elapsed_ms, 0, order.order_id, order.side, order.limit_price, remaining),
                    held_btc=self.balances.btc, outstanding_quantity=ZERO,
                )
            except InventoryLimitError as error:
                self.halt(str(error))
                return
        context = QuoteContext(
            self.book.snapshot(self.elapsed_ms, 0, limit=IMBALANCE_DEPTH_LEVELS), mid, self.balances,
            tuple(working.values()), self.starting_balances.btc,
            self.variance.observe(self.elapsed_ms, mid), self._filters.tick_size,
        )
        quotes = {quote.side: self._filters.round_quote(quote) for quote in self.strategy.quote(context)}
        bid, ask = quotes.get(Side.BUY), quotes.get(Side.SELL)
        if bid is None or ask is None or ask.limit_price - bid.limit_price < (ask.limit_price + bid.limit_price) * self._rate:
            await self._cancel_working()
            return

        obsolete = tuple(order for side, order in working.items() if (
            order.limit_price, order.original_quantity
        ) != (quotes[side].limit_price, quotes[side].quantity))
        if obsolete:
            await self._cancel_working(obsolete)
            return
        for side, quote in quotes.items():
            if self.status != "live" or self._feed_lost or not self.feed_ready:
                return
            if side in working:
                continue
            notional = quote.limit_price * quote.quantity
            assert self.book.best_bid is not None and self.book.best_ask is not None
            if (
                notional > self.max_order_usdt
                or (side is Side.BUY and (quote.limit_price >= self.book.best_ask or notional * (1 + self._rate) > self.balances.available_usdt))
                or (side is Side.SELL and (quote.limit_price <= self.book.best_bid or quote.quantity > self.balances.available_btc))
            ):
                continue
            key = ORDER_PREFIX + uuid4().hex
            event = SubmitOrder(self.elapsed_ms, 0, key, side, quote.limit_price, quote.quantity, post_only=True)
            try:
                self._risk.check_submission(event, held_btc=self.balances.btc, outstanding_quantity=ZERO)
                self._filters.check(quote)
            except (InventoryLimitError, QuoteFilterError):
                continue
            await self._submit(event)

    async def _submit(self, event: SubmitOrder) -> None:
        assert self._account is not None
        key = event.order_id
        await self._journal.add(key, self._account)
        self._owned[key] = (self._account, None)
        if self.status != "live" or self._feed_lost or not self.feed_ready:
            await self._journal.remove(key)
            del self._owned[key]
            return
        self._record("ORDER_SUBMITTED", order_id=key, side=event.side.value,
                     price=event.limit_price, quantity=event.quantity)
        try:
            row = await self.client.request(
                "POST", "/api/v3/order", symbol=SYMBOL, side=event.side.value.upper(), type="LIMIT_MAKER",
                price=format(event.limit_price, "f"), quantity=format(event.quantity, "f"),
                newClientOrderId=key, newOrderRespType="RESULT",
            )
        except BinanceError as error:
            if not error.definitive_rejection:
                raise
            await self._journal.remove(key)
            del self._owned[key]
            self._record("ORDER_REJECTED", order_id=key, reason=str(error))
            if error.code in (-1021, -1022):
                self._sync_clock = error.code == -1021
                self.halt(str(error))
        else:
            await self._apply_order(key, self._account, row)
