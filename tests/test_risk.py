import unittest
from decimal import Decimal

from market_maker.execution import FillEngine
from market_maker.models import BookLevel, BookSnapshot, CancelOrder, OrderStatus, Side, SubmitOrder, Trade
from market_maker.risk import InventoryLimitError
from market_maker.wallet import Wallet


PRICE = Decimal("10000")


class InventoryLimitTests(unittest.TestCase):
    def setUp(self):
        self.wallet = Wallet(btc=Decimal("1"), usdt=Decimal("20000"))
        self.engine = FillEngine(self.wallet)
        self.set_mark(1000, PRICE)

    def set_mark(self, timestamp_ms: int, mark: Decimal) -> None:
        bids = (BookLevel(mark - 1, Decimal("1")),)
        asks = (BookLevel(mark + 1, Decimal("1")),)
        self.engine.process_event(BookSnapshot(timestamp_ms, 0, bids, asks))

    def submit(
        self,
        order_id: str,
        quantity: str,
        *,
        side: Side = Side.BUY,
        timestamp_ms: int = 1001,
        price: Decimal = PRICE,
    ) -> None:
        self.engine.process_event(SubmitOrder(timestamp_ms, 1, order_id, side, price, Decimal(quantity)))

    def test_rejects_affordable_buy_when_pending_bid_pushes_it_over_cap(self):
        self.submit("pending", "0.40")
        before = self.wallet.balances
        self.assertGreater(before.available_usdt, Decimal("2501.875"))

        with self.assertRaisesRegex(InventoryLimitError, r"projected deviation 6500\.00 USDT exceeds 6000\.00"):
            self.submit("too-large", "0.25")

        self.assertEqual(self.wallet.balances, before)
        self.assertEqual(self.wallet.reserved_quantity(Side.BUY), Decimal("0.40"))
        self.assertEqual(self.engine.get_order("pending").status, OrderStatus.PENDING_NEW)

    def test_projection_includes_both_active_and_pending_orders(self):
        self.submit("active", "0.40")
        self.submit("pending", "0.10", timestamp_ms=1020)
        self.assertEqual(self.engine.get_order("active").status, OrderStatus.ACTIVE)
        self.assertEqual(self.engine.get_order("pending").status, OrderStatus.PENDING_NEW)

        with self.assertRaisesRegex(InventoryLimitError, "breaches the 20% inventory cap"):
            self.submit("too-large", "0.12", timestamp_ms=1021)

    def test_cancel_pending_order_counts_until_cancellation_takes_effect(self):
        self.submit("first", "0.40")
        self.engine.process_event(CancelOrder(1020, 1, "first"))
        self.assertEqual(self.engine.get_order("first").status, OrderStatus.CANCEL_PENDING)

        with self.assertRaises(InventoryLimitError):
            self.submit("too-early", "0.25", timestamp_ms=1034)
        self.submit("replacement", "0.25", timestamp_ms=1035)

        self.assertEqual(self.engine.get_order("first").status, OrderStatus.CANCELED)
        self.assertEqual(self.engine.get_order("replacement").status, OrderStatus.PENDING_NEW)
        self.assertEqual(self.wallet.reserved_quantity(Side.BUY), Decimal("0.25"))

    def test_partial_fill_counts_remaining_quantity_and_exact_cap_is_allowed(self):
        self.submit("first", "0.40")
        self.engine.process_event(Trade(1016, 1, 101, PRICE, Decimal("0.10"), True))
        self.assertEqual(self.engine.get_order("first").status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.wallet.balances.btc, Decimal("1.10"))

        self.submit("at-cap", "0.20", timestamp_ms=1017)

        self.assertEqual(self.wallet.reserved_quantity(Side.BUY), Decimal("0.50"))
        with self.assertRaises(InventoryLimitError):
            self.submit("over-cap", "0.00001", timestamp_ms=1018)

    def test_buy_orders_do_not_offset_worst_case_sell_exposure(self):
        self.submit("bid", "0.60")
        self.submit("ask", "0.40", side=Side.SELL)
        before = self.wallet.balances
        self.assertEqual(before.available_btc, Decimal("0.60"))

        with self.assertRaises(InventoryLimitError):
            self.submit("too-large-ask", "0.25", side=Side.SELL)

        self.assertEqual(self.wallet.balances, before)
        self.assertEqual(self.wallet.reserved_quantity(Side.SELL), Decimal("0.40"))

    def test_latest_midpoint_values_exposure_with_fixed_opening_cap(self):
        self.set_mark(1010, Decimal("20000"))

        with self.assertRaisesRegex(InventoryLimitError, r"projected deviation 7000\.00 USDT exceeds 6000\.00"):
            self.submit("too-large", "0.35", timestamp_ms=1011, price=Decimal("20000"))

    def test_configured_target_is_used_to_measure_deviation(self):
        self.engine = FillEngine(self.wallet, target_btc=Decimal("0.80"))
        self.set_mark(1000, PRICE)

        with self.assertRaises(InventoryLimitError):
            self.submit("too-large", "0.41")
        self.submit("at-cap", "0.40")

        self.assertEqual(self.wallet.reserved_quantity(Side.BUY), Decimal("0.40"))
