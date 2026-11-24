import asyncio
import hashlib
import hmac
import io
import json
import os
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

from market_maker.binance_spot import BinanceError, BinanceSpot, OrderJournal
from market_maker.exchange import ExchangeSession
from market_maker.models import BookLevel, BookSnapshot, FeedUnavailable
from market_maker.playback import DEFAULT_PORT, create_app, main
from market_maker.quote_filters import DEFAULT_FILTERS
from market_maker.strategy import FixedSpreadStrategy


TESTNET_START = {
    "environment": "testnet", "confirmation": "TESTNET",
    "max_order_usdt": "100", "max_drawdown_usdt": "10",
}


async def request(app, method, path, payload=None, *, origin=None):
    headers = [(b"host", f"127.0.0.1:{DEFAULT_PORT}".encode()), (b"content-type", b"application/json")]
    if origin is not None:
        headers.append((b"origin", origin.encode()))
    messages = []

    async def receive():
        return {"type": "http.request", "body": json.dumps(payload).encode(), "more_body": False}

    async def send(message):
        messages.append(message)

    await app({
        "type": "http", "method": method, "path": path, "root_path": "", "scheme": "http",
        "http_version": "1.1", "headers": headers, "client": ("127.0.0.1", 57000),
        "server": ("127.0.0.1", DEFAULT_PORT), "query_string": b"",
    }, receive, send)
    return messages[0]["status"], json.loads(messages[-1]["body"])


class TerminalLaunchTests(unittest.TestCase):
    def test_launcher_binds_the_blueprint_port_on_loopback(self):
        with patch("uvicorn.run") as run:
            main()
        self.assertEqual(run.call_args.kwargs, {"host": "127.0.0.1", "port": 8080})


class FakeBinance:
    environment = "testnet"

    def __init__(self):
        self.orders = {}
        self.posts = []
        self.cancels = []
        self.trades = []
        self.hidden_ids = set()
        self.stale_open = []
        self.hide_second_submit = False
        self.btc = Decimal("0.01")
        self.usdt = Decimal("1500")
        self.exchange_info = json.loads(DEFAULT_FILTERS.read_bytes())
        self.exchange_info["symbols"][0].update(status="TRADING", orderTypes=["LIMIT_MAKER"])

    def working(self):
        return [row for number, row in self.orders.items()
                if number not in self.hidden_ids and row["status"] in ("NEW", "PARTIALLY_FILLED")]

    async def sync_time(self):
        pass

    async def request(self, method, path, **parameters):
        if path == "/api/v3/account":
            locked_btc = locked_usdt = Decimal("0")
            for order in self.working():
                remaining = Decimal(order["origQty"]) - Decimal(order["executedQty"])
                if order["side"] == "BUY":
                    locked_usdt += remaining * Decimal(order["price"])
                else:
                    locked_btc += remaining
            return {
                "uid": 42, "canTrade": True, "balances": [
                    {"asset": "BTC", "free": str(self.btc - locked_btc), "locked": str(locked_btc)},
                    {"asset": "USDT", "free": str(self.usdt - locked_usdt), "locked": str(locked_usdt)},
                ],
            }
        if path == "/api/v3/exchangeInfo":
            return self.exchange_info
        if path == "/api/v3/account/commission":
            return {category: {"maker": "0.00025", "buyer": "0", "seller": "0"} for category in (
                "standardCommission", "specialCommission", "taxCommission",
            )}
        if path == "/api/v3/openOrders":
            return [row.copy() for row in self.working() + self.stale_open]
        if path == "/api/v3/myTrades":
            return [row.copy() for row in self.trades
                    if row["orderId"] == parameters["orderId"] and row["id"] >= parameters.get("fromId", 0)]
        if method == "POST" and path == "/api/v3/order":
            self.posts.append(parameters)
            number = len(self.orders) + 1
            row = {
                "symbol": "BTCUSDT", "orderId": number, "clientOrderId": parameters["newClientOrderId"],
                "price": parameters["price"], "origQty": parameters["quantity"], "executedQty": "0",
                "status": "NEW", "side": parameters["side"],
            }
            self.orders[number] = row
            if number == 2 and self.hide_second_submit:
                self.hidden_ids.add(number)
                raise BinanceError(method, path, status=504, code=-1007)
            return row.copy()
        if path == "/api/v3/order":
            number = parameters.get("orderId")
            if number is None:
                number = next(
                    (idx for idx, row in self.orders.items() if row["clientOrderId"] == parameters["origClientOrderId"]),
                    None,
                )
            if number not in self.orders or number in self.hidden_ids:
                raise BinanceError(method, path, status=400, code=-2013)
            row = self.orders[number]
            if method == "DELETE":
                self.cancels.append(number)
                row.update(status="CANCELED", clientOrderId="binance-cancel-id")
            return row.copy()
        raise AssertionError((method, path))


class ExchangeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.client = FakeBinance()
        self.feed = None

    async def market(self, sink, *, started_at, testnet):
        self.assertTrue(testnet)
        self.feed = sink
        self.snapshot(0)
        await asyncio.Future()

    def snapshot(self, timestamp):
        self.feed(BookSnapshot(
            timestamp, 2, (BookLevel(Decimal("59999"), Decimal("1")),),
            (BookLevel(Decimal("60001"), Decimal("1")),),
        ))

    async def wait_for(self, predicate):
        async with asyncio.timeout(3):
            while not predicate():
                await asyncio.sleep(0.005)

    async def start(self, *, max_order_usdt=Decimal("100")):
        session = await ExchangeSession.start(
            self.root, self.client, FixedSpreadStrategy(Decimal("250"), Decimal("0.001")),
            max_order_usdt=max_order_usdt, max_drawdown_usdt=Decimal("10"), publish=lambda: None,
        )
        self.addAsyncCleanup(session.close)
        return session

    async def test_exchange_reports_supply_fills_and_halt_cancels_owned_orders(self):
        with patch("market_maker.exchange.stream_binance", self.market), patch("market_maker.exchange.POLL_SECONDS", 0.02):
            session = await self.start()
            await self.wait_for(lambda: len(self.client.posts) == 2)
            self.assertEqual([post["type"] for post in self.client.posts], ["LIMIT_MAKER", "LIMIT_MAKER"])
            buy = self.client.orders[1]
            buy.update(status="PARTIALLY_FILLED", executedQty="0.0004")
            self.client.trades.append({"id": 74, "symbol": "BTCUSDT", "orderId": 1, "qty": "0.0004",
                                       "price": buy["price"], "commission": "0.0000003", "commissionAsset": "BTC"})
            self.client.btc += Decimal("0.0003997")
            self.client.usdt -= Decimal("0.0004") * Decimal(buy["price"])
            await self.wait_for(lambda: session.last_fill is not None)
            self.assertEqual(session.last_fill["commission_asset"], "BTC")
            self.assertEqual(session.last_fill["fill_quantity"], Decimal("0.0004"))
            await self.wait_for(lambda: session.balances.btc == self.client.btc)

            self.feed(FeedUnavailable(session.elapsed_ms, 3, "depth_gap"))
            await self.wait_for(lambda: len(self.client.cancels) == 2)
            await asyncio.sleep(0.04)
            self.assertEqual(len(self.client.posts), 2)
            self.assertFalse(session.feed_ready)
            self.snapshot(session.elapsed_ms)
            await self.wait_for(lambda: len(self.client.posts) == 4)
            session.halt()
            self.assertFalse(session.can_replace)
            await self.wait_for(lambda: session.status == "halted")
            self.assertEqual(session.unresolved_order_count, 0)
            self.assertEqual(self.client.cancels, [1, 2, 3, 4])
            self.assertEqual((session.balances.reserved_btc, session.balances.reserved_usdt), (0, 0))
            self.assertEqual(session.balances.btc, Decimal("0.0103997"))
            self.assertEqual([event.kind for event in session.audit].count("FILL"), 1)

    async def test_unknown_submit_cancels_confirmed_order_and_blocks_switch_until_reconciled(self):
        self.client.hide_second_submit = True
        with patch("market_maker.exchange.stream_binance", self.market), patch("market_maker.exchange.POLL_SECONDS", 0.02):
            session = await self.start()
            await self.wait_for(lambda: (
                session.status == "halting" and self.client.cancels == [1] and session.unresolved_order_count == 1
            ))
            self.assertFalse(session.can_replace)
            self.assertEqual(session.unresolved_order_count, 1)
            self.assertEqual(len(self.client.posts), 2)
            self.client.hidden_ids.clear()
            await self.wait_for(lambda: session.status == "halted")
            self.assertEqual(self.client.cancels, [1, 2])
            self.assertEqual(len(self.client.posts), 2)

    async def test_halt_waits_for_open_order_query_after_cancel_acknowledgment(self):
        with patch("market_maker.exchange.stream_binance", self.market), patch("market_maker.exchange.POLL_SECONDS", 0.02):
            session = await self.start()
            await self.wait_for(lambda: len(self.client.posts) == 2)
            self.client.stale_open = [self.client.orders[1].copy()]
            session.halt()
            await self.wait_for(lambda: self.client.cancels == [1, 2] and session.unresolved_order_count == 1)
            self.assertEqual(session.status, "halting")
            self.assertFalse(session.can_replace)
            self.client.stale_open.clear()
            await self.wait_for(lambda: session.status == "halted")
            self.assertEqual(session.unresolved_order_count, 0)

    async def test_order_notional_and_drawdown_limits_are_checked_before_submitting(self):
        with patch("market_maker.exchange.stream_binance", self.market), patch("market_maker.exchange.POLL_SECONDS", 0.02):
            session = await self.start(max_order_usdt=Decimal("59"))
            await self.wait_for(lambda: session.opening_equity_usdt is not None)
            self.assertEqual(self.client.posts, [])
            self.client.usdt -= Decimal("11")
            await self.wait_for(lambda: session.status == "halted")
            self.assertIn("drawdown limit", session.error)
            self.assertEqual(self.client.posts, [])

    async def test_restart_reconciles_owned_orders_without_submitting_new_quotes(self):
        order = await self.client.request(
            "POST", "/api/v3/order", newClientOrderId="pmm-recovery", side="BUY", price="59875", quantity="0.001",
        )
        def seed_pending_order():
            journal = OrderJournal(self.root, "testnet")
            try:
                journal.add(order["clientOrderId"], "42")
                with self.assertRaisesRegex(ValueError, "already using"):
                    OrderJournal(self.root, "testnet")
            finally:
                journal.close()

        await asyncio.to_thread(seed_pending_order)
        with patch("market_maker.exchange.POLL_SECONDS", 0.02):
            session = await self.start()
            await self.wait_for(lambda: session.status == "halted")
            self.assertEqual(len(self.client.posts), 1)
            self.assertEqual(self.client.cancels, [order["orderId"]])
            self.assertIsNone(self.feed)

    async def test_api_requires_opt_in_and_switches_to_paper_only_after_halt(self):
        app = create_app(self.root)

        async def idle_paper_feed(_sink, *, started_at):
            await asyncio.Future()

        payload = TESTNET_START
        with patch.dict(os.environ, {"BINANCE_TRADING_ENV": ""}):
            self.assertEqual((await request(app, "POST", "/api/exchange/start", payload))[0], 400)
        with (
            patch("market_maker.playback.BinanceSpot.configured", return_value=self.client),
            patch("market_maker.exchange.stream_binance", self.market),
            patch("market_maker.exchange.POLL_SECONDS", 0.02),
            patch("market_maker.playback.stream_binance", idle_paper_feed),
        ):
            async with app.router.lifespan_context(app):
                self.assertEqual((await request(app, "POST", "/api/exchange/start", payload, origin="https://example.com"))[0], 403)
                wrong_confirmation = {**payload, "confirmation": "WRONG"}
                self.assertEqual((await request(app, "POST", "/api/exchange/start", wrong_confirmation))[0], 400)
                code, state = await request(app, "POST", "/api/exchange/start", payload)
                self.assertEqual(code, 200)
                self.assertEqual(state["mode"], "exchange")
                self.assertNotIn("confirmation", state["source"])
                self.assertEqual((await request(app, "POST", "/api/live/start", {}))[0], 409)
                await self.wait_for(lambda: len(self.client.posts) == 2)
                self.assertEqual((await request(app, "POST", "/api/replay/halt"))[0], 200)
                async with asyncio.timeout(3):
                    while True:
                        _, state = await request(app, "GET", "/api/replay/state")
                        if state["status"] == "halted":
                            break
                        await asyncio.sleep(0.005)
                self.assertEqual(state["unresolved_order_count"], 0)
                code, state = await request(app, "POST", "/api/live/start", {})
                self.assertEqual((code, state["mode"]), (200, "live"))

    async def test_mainnet_is_rejected_by_configuration_api_and_signed_client(self):
        app = create_app(self.root)
        with patch.dict(os.environ, {
            "BINANCE_TRADING_ENV": "mainnet", "BINANCE_API_KEY": "fake", "BINANCE_API_SECRET": "fake",
        }):
            self.assertEqual(await request(app, "GET", "/api/exchange/config"), (200, {"environment": None}))
            self.assertEqual((await request(app, "POST", "/api/exchange/start", TESTNET_START))[0], 400)
        payload = {**TESTNET_START, "environment": "mainnet", "confirmation": "MAINNET"}
        with patch("market_maker.playback.BinanceSpot.configured") as configured:
            self.assertEqual((await request(app, "POST", "/api/exchange/start", payload))[0], 422)
            configured.assert_not_called()
        with self.assertRaisesRegex(ValueError, "Only Binance Spot Testnet"):
            BinanceSpot("mainnet", "fake", "fake")
        with self.assertRaisesRegex(ValueError, "Spot Testnet API path"):
            client = BinanceSpot("testnet", "fake", "fake")
            await client.request("POST", "https://api.binance.com/api/v3/order")

    async def test_slow_journal_write_keeps_loop_responsive_and_halt_prevents_submit(self):
        loop_thread = threading.get_ident()
        writing = threading.Event()
        release = threading.Event()
        worker_threads = []
        add = OrderJournal.add

        def slow_add(journal, *args):
            worker_threads.append(threading.get_ident())
            writing.set()
            if not release.wait(2):
                raise TimeoutError("The event loop did not release the journal")
            return add(journal, *args)

        with (
            patch("market_maker.exchange.stream_binance", self.market),
            patch("market_maker.exchange.POLL_SECONDS", 0.02),
            patch.object(OrderJournal, "add", slow_add),
        ):
            session = await self.start()
            try:
                await self.wait_for(writing.is_set)
                self.assertNotIn(loop_thread, worker_threads)
                self.assertEqual(self.client.posts, [])
                session.halt()
            finally:
                release.set()
            await self.wait_for(lambda: session.status == "halted")
            self.assertEqual(session.unresolved_order_count, 0)
            self.assertEqual(self.client.posts, [])

    async def test_signed_post_uses_exact_body_and_credentials_stay_in_header(self):
        client = BinanceSpot("testnet", "fake-key", "fake-secret")
        with patch("market_maker.binance_spot.build_opener") as build:
            send = build.return_value.open
            send.return_value = io.BytesIO(b'{}')
            await client.request("POST", "/api/v3/order", symbol="BTCUSDT", quantity="0.00100000")
        request = send.call_args.args[0]
        signed_body, signature = request.data.rsplit(b"&signature=", 1)
        expected_signature = hmac.new(b"fake-secret", signed_body, hashlib.sha256).hexdigest().encode()
        self.assertEqual(signature, expected_signature)
        self.assertEqual(parse_qs(signed_body.decode())["quantity"], ["0.00100000"])
        self.assertEqual(request.get_header("X-mbx-apikey"), "fake-key")
        self.assertEqual(request.full_url, "https://testnet.binance.vision/api/v3/order")
