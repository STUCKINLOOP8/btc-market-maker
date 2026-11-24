import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from market_maker.execution import Fill, FillEngine
from market_maker.models import (
    BookLevel,
    BookSnapshot,
    CancelOrder,
    DepthUpdate,
    OrderStatus,
    Side,
    SubmitOrder,
    Trade,
)
from market_maker.replay import load_fixture
from market_maker.wallet import Wallet, WalletBalances


FIXTURE = Path(__file__).parent / "fixtures" / "milestone_1_events.json"
PRICE = Decimal("70000")
ORDER_QUANTITY = Decimal("0.05")


class FillEngineTests(unittest.TestCase):
    def setUp(self):
        self.wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("5000"))
        self.engine = FillEngine(self.wallet)

    def start_order(self, queue_ahead: str, side: Side = Side.BUY) -> None:
        levels = (BookLevel(PRICE, Decimal(queue_ahead)),)
        bids, asks = (levels, ()) if side is Side.BUY else ((), levels)
        self.engine.update_book(BookSnapshot(1000, 1, bids, asks))
        self.engine.submit(SubmitOrder(1001, 2, "order", side, PRICE, ORDER_QUANTITY))
        self.engine.activate("order")

    def test_replay_trade_clears_queue_and_settles_bid_once(self):
        snapshot, submission, depth_update, first_trade, _ = load_fixture(FIXTURE)
        self.engine.update_book(snapshot)
        self.engine.submit(submission)
        self.engine.activate(submission.order_id)
        self.assertEqual(self.engine.queue_ahead(submission.order_id), Decimal("0.40"))

        before_trade = self.wallet.balances
        self.engine.update_book(depth_update)
        self.assertEqual(self.engine.queue_ahead(submission.order_id), Decimal("0.40"))
        self.assertIsNone(self.engine.process_trade(first_trade))
        self.assertIsNone(self.engine.process_trade(first_trade))
        self.assertEqual(self.engine.queue_ahead(submission.order_id), Decimal("0.15"))
        self.assertEqual(self.wallet.balances, before_trade)

        second_trade = replace(
            first_trade, timestamp_ms=1015, sequence=6, trade_id=102, quantity=Decimal("0.30")
        )
        fill = self.engine.process_trade(second_trade)

        self.assertEqual(
            fill,
            Fill(
                "bid-1", 102, 1015, Side.BUY, Decimal("68448.01"), ORDER_QUANTITY,
                Decimal("2.566800375"),
            ),
        )
        self.assertEqual(self.engine.get_order("bid-1").status, OrderStatus.FILLED)
        self.assertEqual(self.engine.get_order("bid-1").filled_quantity, ORDER_QUANTITY)
        self.assertEqual(self.engine.queue_ahead("bid-1"), Decimal("0"))
        self.assertIsNone(self.engine.process_trade(second_trade))
        self.assertEqual(
            self.wallet.balances,
            WalletBalances(Decimal("0.55"), Decimal("1575.032699625"), Decimal("0"), Decimal("0")),
        )

    def test_wrong_side_and_wrong_price_trades_leave_queue_unchanged(self):
        self.start_order("0.40")
        before_trades = self.wallet.balances
        wrong_side = Trade(1002, 3, 101, PRICE, Decimal("0.50"), False)
        wrong_price = replace(wrong_side, trade_id=102, price=Decimal("69999"), buyer_is_maker=True)

        self.assertIsNone(self.engine.process_trade(wrong_side))
        self.assertIsNone(self.engine.process_trade(wrong_price))

        self.assertEqual(self.engine.queue_ahead("order"), Decimal("0.40"))
        self.assertEqual(self.engine.get_order("order").status, OrderStatus.ACTIVE)
        self.assertEqual(self.wallet.balances, before_trades)

    def test_partial_fill_reduces_reservation_and_duplicate_has_no_effect(self):
        self.start_order("0.02")
        trade = Trade(1002, 3, 101, PRICE, Decimal("0.04"), True)

        fill = self.engine.process_trade(trade)

        self.assertEqual(fill, Fill("order", 101, 1002, Side.BUY, PRICE, Decimal("0.02"), Decimal("1.05")))
        self.assertEqual(self.engine.get_order("order").status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.engine.get_order("order").filled_quantity, Decimal("0.02"))
        self.assertEqual(self.engine.queue_ahead("order"), Decimal("0"))
        self.assertIsNone(self.engine.process_trade(trade))
        self.assertEqual(
            self.wallet.balances,
            WalletBalances(Decimal("0.52"), Decimal("3598.95"), Decimal("0"), Decimal("2101.575")),
        )

    def test_pending_order_joins_latest_displayed_queue_on_activation(self):
        self.engine.update_book(BookSnapshot(1000, 1, (BookLevel(PRICE, Decimal("0.40")),), ()))
        self.engine.submit(SubmitOrder(1001, 2, "bid", Side.BUY, PRICE, ORDER_QUANTITY))
        earlier_trade = Trade(1002, 3, 101, PRICE, Decimal("0.25"), True)
        self.assertIsNone(self.engine.queue_ahead("bid"))
        self.assertIsNone(self.engine.process_trade(earlier_trade))
        self.engine.update_book(DepthUpdate(1003, 4, (BookLevel(PRICE, Decimal("0.15")),), ()))

        self.engine.activate("bid")

        self.assertIsNone(self.engine.process_trade(earlier_trade))
        self.assertEqual(self.engine.queue_ahead("bid"), Decimal("0.15"))
        self.assertEqual(self.engine.get_order("bid").filled_quantity, Decimal("0"))

    def test_taker_buy_fills_ask_and_credits_net_usdt(self):
        self.start_order("0.10", Side.SELL)
        trade = Trade(1002, 3, 101, PRICE, Decimal("0.15"), False)

        self.assertEqual(
            self.engine.process_trade(trade),
            Fill("order", 101, 1002, Side.SELL, PRICE, ORDER_QUANTITY, Decimal("2.625")),
        )
        self.assertEqual(self.engine.get_order("order").status, OrderStatus.FILLED)
        self.assertEqual(
            self.wallet.balances,
            WalletBalances(Decimal("0.45"), Decimal("8497.375"), Decimal("0"), Decimal("0")),
        )

    def test_failed_settlement_preserves_queue_and_can_be_retried(self):
        self.start_order("0.02")
        trade = Trade(1002, 3, 101, PRICE, Decimal("0.04"), True)
        before_trade = self.wallet.balances

        with patch.object(self.wallet, "settle_fill", side_effect=ValueError("settlement failed")):
            with self.assertRaisesRegex(ValueError, "settlement failed"):
                self.engine.process_trade(trade)

        self.assertEqual(self.engine.queue_ahead("order"), Decimal("0.02"))
        self.assertEqual(self.engine.get_order("order").filled_quantity, Decimal("0"))
        self.assertEqual(self.wallet.balances, before_trade)
        self.assertEqual(
            self.engine.process_trade(trade),
            Fill("order", 101, 1002, Side.BUY, PRICE, Decimal("0.02"), Decimal("1.05")),
        )


class TimedFillEngineTests(unittest.TestCase):
    def setUp(self):
        self.wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("5000"))
        self.engine = FillEngine(self.wallet)
        self.engine.process_event(BookSnapshot(1000, 0, (), ()))

    def submit_bid(self) -> None:
        self.engine.process_event(SubmitOrder(1000, 1, "bid", Side.BUY, PRICE, ORDER_QUANTITY))

    def trade(self, timestamp_ms: int, trade_id: int, quantity: str) -> Trade:
        return Trade(timestamp_ms, trade_id, trade_id, PRICE, Decimal(quantity), True)

    def test_activation_uses_displayed_depth_at_deadline(self):
        self.engine.process_event(DepthUpdate(1000, 1, (BookLevel(PRICE, Decimal("0.40")),), ()))
        self.submit_bid()
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("3502.625"))

        self.assertIsNone(self.engine.process_event(self.trade(1014, 101, "0.50")))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.PENDING_NEW)
        self.assertIsNone(self.engine.queue_ahead("bid"))
        self.engine.process_event(DepthUpdate(1014, 102, (BookLevel(PRICE, Decimal("0.02")),), ()))

        fill = self.engine.process_event(self.trade(1015, 103, "0.03"))

        self.assertEqual(fill, Fill("bid", 103, 1015, Side.BUY, PRICE, Decimal("0.01"), Decimal("0.525")))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.engine.queue_ahead("bid"), Decimal("0"))

    def test_partial_fill_then_cancel_releases_only_unfilled_reservation(self):
        self.submit_bid()
        self.engine.process_event(self.trade(1015, 101, "0.02"))
        self.engine.process_event(CancelOrder(1020, 102, "bid"))
        self.engine.advance_to(1034)
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("2101.575"))

        self.assertIsNone(self.engine.process_event(self.trade(1035, 103, "0.03")))

        order = self.engine.get_order("bid")
        self.assertEqual(order.status, OrderStatus.CANCELED)
        self.assertEqual(order.filled_quantity, Decimal("0.02"))
        self.assertEqual(
            self.wallet.balances,
            WalletBalances(Decimal("0.52"), Decimal("3598.95"), Decimal("0"), Decimal("0")),
        )

    def test_partial_fill_while_cancel_pending_preserves_deadline_and_reservation(self):
        self.submit_bid()
        self.engine.process_event(CancelOrder(1020, 101, "bid"))
        self.engine.process_event(CancelOrder(1025, 102, "bid"))

        fill = self.engine.process_event(self.trade(1034, 103, "0.02"))

        self.assertEqual(fill, Fill("bid", 103, 1034, Side.BUY, PRICE, Decimal("0.02"), Decimal("1.05")))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("2101.575"))
        self.engine.advance_to(1035)
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCELED)
        self.assertEqual(self.engine.get_order("bid").filled_quantity, Decimal("0.02"))
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("0"))

    def test_complete_fill_while_cancel_pending_survives_confirmation(self):
        self.submit_bid()
        self.engine.process_event(CancelOrder(1020, 101, "bid"))

        fill = self.engine.process_event(self.trade(1034, 102, "0.05"))
        self.assertEqual(fill, Fill("bid", 102, 1034, Side.BUY, PRICE, ORDER_QUANTITY, Decimal("2.625")))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.FILLED)
        self.engine.advance_to(1035)

        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.FILLED)
        self.assertEqual(
            self.wallet.balances,
            WalletBalances(Decimal("0.55"), Decimal("1497.375"), Decimal("0"), Decimal("0")),
        )

    def test_cancel_requested_before_activation_allows_fill_only_after_activation(self):
        self.submit_bid()
        self.engine.process_event(CancelOrder(1005, 101, "bid"))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCEL_PENDING)
        self.assertIsNone(self.engine.queue_ahead("bid"))
        self.assertIsNone(self.engine.process_event(self.trade(1014, 102, "0.02")))

        fill = self.engine.process_event(self.trade(1015, 103, "0.02"))

        self.assertEqual(fill, Fill("bid", 103, 1015, Side.BUY, PRICE, Decimal("0.02"), Decimal("1.05")))
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCEL_PENDING)
        self.engine.advance_to(1020)
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCELED)
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("0"))

    def test_overlapping_activation_rejects_second_bid_and_releases_its_funds(self):
        quantity = Decimal("0.02")
        self.engine.process_event(SubmitOrder(1000, 1, "first", Side.BUY, PRICE, quantity))
        self.engine.process_event(SubmitOrder(1000, 2, "second", Side.BUY, PRICE, quantity))
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("2802.10"))

        self.engine.advance_to(1015)

        self.assertEqual(self.engine.get_order("first").status, OrderStatus.ACTIVE)
        self.assertEqual(self.engine.get_order("second").status, OrderStatus.REJECTED)
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("1401.05"))

    def test_zero_configured_latency_applies_actions_immediately(self):
        self.engine = FillEngine(self.wallet, latency_ms=0)
        self.engine.process_event(BookSnapshot(1000, 0, (), ()))
        self.submit_bid()
        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.ACTIVE)

        self.engine.process_event(CancelOrder(1000, 2, "bid"))

        self.assertEqual(self.engine.get_order("bid").status, OrderStatus.CANCELED)
        self.assertIsNone(self.engine.process_event(self.trade(1000, 101, "0.05")))
        self.assertEqual(self.wallet.balances.reserved_usdt, Decimal("0"))
