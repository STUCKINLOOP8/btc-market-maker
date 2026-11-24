import subprocess
import sys
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from market_maker.models import (
    BookLevel, BookSnapshot, ClockAdvance, DepthUpdate, FeedUnavailable, OrderStatus, Side, SubmitOrder, Trade,
)
from market_maker.quote_filters import load_binance_quote_filters
from market_maker.replay import ReplaySession, run_replay
from market_maker.strategy import (
    AvellanedaStoikovImbalanceStrategy,
    AvellanedaStoikovStrategy,
    FixedSpreadStrategy,
    Quote,
    QuoteContext,
)
from market_maker.volatility import MidpointReturnVariance
from market_maker.wallet import Wallet


PROJECT = Path(__file__).resolve().parents[1]
BID = Decimal("68448")
ASK = Decimal("68452")
SIZE = Decimal("0.05")


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.book = BookSnapshot(100, 1, (BookLevel(BID, Decimal("0.04")),), (BookLevel(ASK, SIZE),))

    def wallet(self):
        return Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))

    def test_fixed_spread_prices_are_symmetric_and_exact(self):
        mid = Decimal("68450.005")
        context = QuoteContext(self.book, mid, self.wallet().balances, ())
        strategy = FixedSpreadStrategy(Decimal("0.03"), Decimal("0.0015"))
        self.assertEqual(strategy.quote(context), (
            Quote(Side.BUY, Decimal("68449.990"), Decimal("0.0015")),
            Quote(Side.SELL, Decimal("68450.020"), Decimal("0.0015")),
        ))
        self.assertEqual(FixedSpreadStrategy(mid * 2).quote(context), ())

    def test_fixed_spread_rejects_invalid_configuration(self):
        for field in ("spread_usdt", "quote_size_btc"):
            for value in (Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity"), 1):
                with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, field):
                    FixedSpreadStrategy(**{field: value})

    def test_replay_quotes_after_timestamp_batch_and_orders_use_existing_fill_engine(self):
        new_ask = ASK + 2
        events = [
            self.book,
            DepthUpdate(100, 2, (), (BookLevel(ASK, Decimal("0")), BookLevel(new_ask, SIZE))),
            Trade(100, 3, 101, BID, Decimal("0.10"), True),
            DepthUpdate(110, 4, (), ()),
            Trade(120, 5, 102, BID, Decimal("0.06"), True),
            DepthUpdate(121, 6, (), ()),
        ]
        strategy = FixedSpreadStrategy(Decimal("6"), SIZE)
        first = run_replay(events, self.wallet(), strategy=strategy, fee_guard=False)
        second = run_replay(reversed(events), self.wallet(), strategy=strategy, fee_guard=False)
        self.assertEqual(first, second)

        decision = [event for event in first.audit_log if event.kind == "QUOTE_PROPOSED"]
        self.assertEqual([(event.timestamp_ms, dict(event.details)) for event in decision], [
            (100, {"order_id": "strategy-1", "side": "buy", "mid_price": Decimal("68451"),
                   "limit_price": BID, "quantity": SIZE}),
            (100, {"order_id": "strategy-2", "side": "sell", "mid_price": Decimal("68451"),
                   "limit_price": new_ask, "quantity": SIZE}),
        ])
        self.assertEqual(first.audit_log[2].kind, "TRADE_IGNORED")
        self.assertEqual(
            [event.timestamp_ms for event in first.audit_log if event.kind == "ORDER_ACTIVATED"], [115, 115]
        )
        [fill] = [dict(event.details) for event in first.audit_log if event.kind == "FILL"]
        self.assertEqual(fill["trade_id"], 102)
        self.assertEqual(fill["fill_quantity"], Decimal("0.02"))
        self.assertEqual(first.fees_usdt, Decimal("1.02672"))
        self.assertEqual(first.balances.btc, Decimal("0.52"))

    def test_replay_waits_for_two_sided_book_and_quotes_again_after_recovery(self):
        events = [
            BookSnapshot(100, 1, self.book.bids, ()),
            DepthUpdate(110, 2, (), self.book.asks),
            FeedUnavailable(140, 3, "depth_gap"),
            DepthUpdate(150, 4, self.book.bids, self.book.asks),
            BookSnapshot(160, 5, self.book.bids, self.book.asks),
        ]
        result = run_replay(events, self.wallet(), strategy=FixedSpreadStrategy(Decimal("110")))
        self.assertEqual(
            [event.timestamp_ms for event in result.audit_log if event.kind == "QUOTE_PROPOSED"],
            [110, 110, 160, 160],
        )
        self.assertEqual(
            [event.timestamp_ms for event in result.audit_log if event.kind == "ORDER_CANCELED"], [140, 140]
        )

    def test_strategy_mode_refuses_embedded_scripted_orders(self):
        submit = SubmitOrder(100, 2, "script-bid", Side.BUY, BID, SIZE)
        with self.assertRaisesRegex(ValueError, "without scripted orders"):
            run_replay([self.book, submit], self.wallet(), strategy=FixedSpreadStrategy())

    def test_unchanged_quote_keeps_order_id_and_consumed_queue(self):
        events = [
            self.book,
            Trade(120, 2, 101, BID, Decimal("0.015"), True),
            DepthUpdate(121, 3, (BookLevel(BID, Decimal("0.50")),), ()),
        ]
        session = ReplaySession(
            events, self.wallet(), strategy=FixedSpreadStrategy(Decimal("4"), SIZE), fee_guard=False,
        )
        while session.step() is not None:
            pass
        bid = next(order for order in session.orders if order.side is Side.BUY)
        self.assertEqual(bid.order_id, "strategy-1")
        self.assertIs(bid.status, OrderStatus.ACTIVE)
        self.assertEqual(session.queue_ahead(bid.order_id), Decimal("0.025"))
        kinds = [entry.kind for entry in session.result().audit_log]
        self.assertEqual(kinds.count("ORDER_SUBMITTED"), 2)
        self.assertNotIn("CANCEL_REQUESTED", kinds)

    def test_refresh_waits_for_confirmation_and_uses_latest_prices(self):
        update = BookSnapshot(130, 2, (BookLevel(BID + 1, SIZE),), (BookLevel(ASK + 1, SIZE),))
        latest = BookSnapshot(135, 3, (BookLevel(BID + 2, SIZE),), (BookLevel(ASK + 2, SIZE),))
        events = [
            self.book, update, latest,
            Trade(140, 4, 101, BID, Decimal("0.05"), True),
            DepthUpdate(161, 5, (), ()),
        ]
        session = ReplaySession(
            events, self.wallet(), strategy=FixedSpreadStrategy(Decimal("4"), SIZE), fee_guard=False,
        )
        session.step()
        cancel = session.step()
        self.assertEqual([entry.kind for entry in cancel.audit_log][-2:], ["CANCEL_REQUESTED"] * 2)
        self.assertTrue(all(order.status is OrderStatus.CANCEL_PENDING for order in session.orders))
        session.step()
        fill = session.step()
        self.assertEqual([entry.kind for entry in fill.audit_log], ["FILL"])
        self.assertEqual(session.orders[0].filled_quantity, Decimal("0.01"))
        self.assertEqual(len(session.orders), 2)

        replacement = session.step()
        self.assertIsInstance(replacement.event, ClockAdvance)
        self.assertEqual(replacement.event.timestamp_ms, 145)
        self.assertEqual([entry.kind for entry in replacement.audit_log][:2], ["ORDER_CANCELED"] * 2)
        new_orders = [order for order in session.orders if order.status is OrderStatus.PENDING_NEW]
        self.assertEqual([order.limit_price for order in new_orders], [BID + 2, ASK + 2])
        self.assertEqual([order.order_id for order in new_orders], ["strategy-3", "strategy-4"])
        session.step()
        self.assertTrue(all(order.status is OrderStatus.ACTIVE for order in session.orders[-2:]))

    def test_cli_can_select_and_configure_fixed_spread(self):
        command = [
            sys.executable, "-B", "-m", "market_maker.replay", "fixtures/order_timeline_recording.jsonl",
            "--strategy", "fixed-spread", "--spread-usdt", "4", "--quote-size-btc", "0.05", "--fee-guard", "off",
        ]
        strategy_result = subprocess.run(command, cwd=PROJECT, check=True, capture_output=True, text=True)
        self.assertIn("strategy=fixed-spread spread_usdt=4 quote_size_btc=0.05", strategy_result.stdout)
        self.assertIn("QUOTE_PROPOSED order_id=strategy-1 side=buy mid_price=68450 limit_price=68448 quantity=0.05",
                      strategy_result.stdout)
        self.assertIn("fill_quantity=0.02 fee_usdt=1.02672", strategy_result.stdout)

        mixed = subprocess.run([*command, "--orders", "fixtures/order_timeline_orders.json"],
                               cwd=PROJECT, capture_output=True, text=True)
        self.assertEqual(mixed.returncode, 2)
        self.assertIn("--strategy and --orders are mutually exclusive", mixed.stderr)


class InventorySkewTests(unittest.TestCase):
    def test_model_lot_conversion_moves_both_quotes_and_preserves_the_spread(self):
        strategy = AvellanedaStoikovStrategy(Decimal("100"), Decimal("0.001"))
        wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))
        mid = Decimal("68000")
        book = BookSnapshot(2000, 1, (), ())
        context = QuoteContext(book, mid, wallet.balances, (), Decimal("0.50"), Decimal("0.00000001"))
        baseline = FixedSpreadStrategy().quote(context)
        self.assertEqual(strategy.quote(context), baseline)

        for held, bid, ask in (("0.55", "67945.8384", "68045.8384"), ("0.45", "67954.1616", "68054.1616")):
            with self.subTest(held=held):
                balances = replace(context.balances, btc=Decimal(held))
                shifted = strategy.quote(replace(context, balances=balances))
                self.assertEqual(shifted, (
                    Quote(Side.BUY, Decimal(bid), Decimal("0.001")),
                    Quote(Side.SELL, Decimal(ask), Decimal("0.001")),
                ))

    def test_replay_off_target_warms_up_and_moves_both_rounded_quotes(self):
        events = []
        for second, price in enumerate(("68000", "68005", "68000")):
            mid = Decimal(price)
            events.append(BookSnapshot(second * 1000, second, (
                BookLevel(mid - 1, SIZE),
            ), (
                BookLevel(mid + 1, SIZE),
            )))

        def submissions(target):
            wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))
            result = run_replay(
                events, wallet, strategy=AvellanedaStoikovStrategy(Decimal("120")), target_btc=Decimal(target),
            )
            return [(event.timestamp_ms, dict(event.details)["price"])
                    for event in result.audit_log if event.kind == "ORDER_SUBMITTED"]

        neutral, excess, shortfall = submissions("0.50"), submissions("0.45"), submissions("0.55")
        self.assertEqual(neutral[:2], [(0, Decimal("67940")), (0, Decimal("68060"))])
        self.assertEqual([timestamp for timestamp, _ in excess], [2000, 2000])
        self.assertEqual([timestamp for timestamp, _ in shortfall], [2000, 2000])
        for (_, lower), (_, center), (_, higher) in zip(excess, neutral[:2], shortfall, strict=True):
            self.assertLess(lower, center)
            self.assertLess(center, higher)

    def test_replay_restarts_off_target_warmup_at_feed_recovery(self):
        bids, asks = (BookLevel(BID, SIZE),), (BookLevel(ASK, SIZE),)
        events = [
            BookSnapshot(0, 1, bids, asks),
            BookSnapshot(1000, 2, bids, asks),
            FeedUnavailable(1500, 3, "depth_gap"),
            BookSnapshot(2000, 4, bids, asks),
            BookSnapshot(3000, 5, bids, asks),
            BookSnapshot(4000, 6, bids, asks),
        ]
        wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))
        result = run_replay(
            events, wallet, strategy=AvellanedaStoikovStrategy(Decimal("120")), target_btc=Decimal("0.45"),
        )
        self.assertEqual([event.timestamp_ms for event in result.audit_log if event.kind == "ORDER_SUBMITTED"],
                         [4000, 4000])


class ImbalanceStrategyTests(unittest.TestCase):
    def setUp(self):
        self.plain = AvellanedaStoikovStrategy(Decimal("120"))
        self.imbalance = AvellanedaStoikovImbalanceStrategy(Decimal("120"))
        self.wallet = Wallet(btc=Decimal("0.55"), usdt=Decimal("34225"))

    def context(self, bid_sizes, ask_sizes):
        mid = Decimal("68000")
        book = BookSnapshot(
            2000, 1,
            tuple(BookLevel(mid - rank, Decimal(size)) for rank, size in enumerate(bid_sizes, 1)),
            tuple(BookLevel(mid + rank, Decimal(size)) for rank, size in enumerate(ask_sizes, 1)),
        )
        return QuoteContext(
            book, mid, self.wallet.balances, (), Decimal("0.50"), Decimal("0.00000001"), Decimal("0.05"),
        )

    def test_balanced_top_five_matches_plain_as_even_with_unequal_deeper_liquidity(self):
        context = self.context([1, 2, 3, 4, 5, 1], [3, 3, 3, 3, 3, 1000])
        self.assertEqual(self.imbalance.quote(context), self.plain.quote(context))

    def test_extreme_imbalance_shifts_both_quotes_within_two_ticks(self):
        for bids, asks, direction in (([1000000] * 5, [1] * 5, 1), ([1] * 5, [1000000] * 5, -1)):
            with self.subTest(direction=direction):
                context = self.context(bids, asks)
                plain = self.plain.quote(context)
                shifted = self.imbalance.quote(context)
                shifts = [new.limit_price - old.limit_price for old, new in zip(plain, shifted, strict=True)]
                self.assertEqual(shifts[0], shifts[1])
                self.assertGreater(shifts[0] * direction, context.tick_size)
                self.assertLessEqual(abs(shifts[0]), 2 * context.tick_size)
                self.assertEqual([quote.quantity for quote in shifted], [quote.quantity for quote in plain])

    def test_empty_depth_or_disabled_tick_leaves_plain_quotes(self):
        for context in (self.context([], []), replace(self.context([100], [1]), tick_size=Decimal("0"))):
            self.assertEqual(self.imbalance.quote(context), self.plain.quote(context))

    def test_replay_uses_best_five_levels_and_the_saved_filter_tick(self):
        context = self.context([3, 3, 3, 3, 3], [1, 1, 1, 1, 1, 1000])
        book = replace(context.book, asks=tuple(reversed(context.book.asks)))
        filters = replace(load_binance_quote_filters(), tick_size=Decimal("0.05"))
        wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))
        result = run_replay([book], wallet, strategy=self.imbalance, quote_filters=filters)
        submitted = [dict(event.details)["price"] for event in result.audit_log if event.kind == "ORDER_SUBMITTED"]
        self.assertEqual(submitted, [Decimal("67940.05"), Decimal("68060.05")])


class MidpointVarianceTests(unittest.TestCase):
    def test_uses_only_midpoints_known_at_each_whole_second(self):
        estimator = MidpointReturnVariance()
        for timestamp, mid in ((0, "100"), (999, "101"), (1000, "102"), (1500, "103")):
            self.assertIsNone(estimator.observe(timestamp, Decimal(mid)))
        variance = estimator.observe(2100, Decimal("104"))
        first = (Decimal("102") / Decimal("100")).ln()
        second = (Decimal("103") / Decimal("102")).ln()
        self.assertAlmostEqual(variance, (first - second) ** 2 / 2, places=25)

    def test_drops_returns_older_than_the_window_and_resets_without_a_midpoint(self):
        estimator = MidpointReturnVariance()
        estimator.observe(0, Decimal("100"))
        estimator.observe(1000, Decimal("101"))
        self.assertGreater(estimator.observe(2000, Decimal("102")), 0)
        self.assertEqual(estimator.observe(63001, Decimal("102")), 0)
        self.assertIsNone(estimator.observe(64000, None))
        self.assertIsNone(estimator.observe(65000, Decimal("105")))
        self.assertIsNone(estimator.observe(66000, Decimal("107")))
        self.assertGreater(estimator.observe(67000, Decimal("108")), 0)
