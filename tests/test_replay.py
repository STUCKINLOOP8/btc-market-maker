import json
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from market_maker.models import (
    BookSnapshot,
    CancelOrder,
    DepthUpdate,
    Order,
    OrderStatus,
    Side,
    SubmitOrder,
    Trade,
)
from market_maker.replay import load_fixture, order_events


FIXTURE = Path(__file__).parent / "fixtures" / "milestone_1_events.json"


class ReplayTests(unittest.TestCase):
    def test_orders_by_timestamp_then_sequence(self):
        events = load_fixture(FIXTURE)

        self.assertEqual(
            [(event.timestamp_ms, event.sequence) for event in events],
            [(1000, 1), (1002, 2), (1005, 3), (1005, 4), (1010, 5)],
        )
        self.assertEqual(
            [type(event) for event in events],
            [BookSnapshot, SubmitOrder, DepthUpdate, Trade, CancelOrder],
        )
        earlier = CancelOrder(timestamp_ms=999, sequence=20, order_id="bid-0")
        self.assertEqual(order_events([events[0], earlier]), [earlier, events[0]])

    def test_loading_and_ordering_are_repeatable(self):
        first = load_fixture(FIXTURE)

        self.assertEqual(load_fixture(FIXTURE), first)
        self.assertEqual(order_events(reversed(first)), first)

    def test_fixture_parses_decimals_exactly(self):
        snapshot, submission, update, trade, _ = load_fixture(FIXTURE)

        self.assertEqual(snapshot.bids[0].price, Decimal("68448.01"))
        self.assertEqual(snapshot.asks[0].quantity, Decimal("0.50000000"))
        self.assertEqual(submission.limit_price, Decimal("68448.01"))
        self.assertEqual(submission.quantity, Decimal("0.05000000"))
        self.assertEqual(submission.side, Side.BUY)
        self.assertEqual(update.bids[0].quantity, Decimal("0.15000000"))
        self.assertEqual(trade.trade_id, 101)
        self.assertIs(trade.buyer_is_maker, True)
        self.assertEqual(trade.price, Decimal("68448.01"))
        self.assertEqual(trade.quantity.as_tuple(), Decimal("0.25000000").as_tuple())

    def test_order_fill_quantity_survives_status_change(self):
        order = Order("bid-1", Side.BUY, Decimal("68448.01"), Decimal("0.05"))
        self.assertEqual(order.status, OrderStatus.PENDING_NEW)
        order.filled_quantity = Decimal("0.02")
        order.status = OrderStatus.PARTIALLY_FILLED

        order.status = OrderStatus.CANCEL_PENDING

        self.assertEqual(order.filled_quantity, Decimal("0.02"))
        self.assertEqual(order.original_quantity, Decimal("0.05"))


class ReplayDecimalValidationTests(unittest.TestCase):
    def setUp(self):
        records = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.records = {record["type"]: record for record in records}
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture_path = Path(directory.name) / "events.json"

    def load_event(self, event_type, **changes):
        record = {**self.records[event_type], **changes}
        self.fixture_path.write_text(json.dumps([record]), encoding="utf-8")
        return load_fixture(self.fixture_path)[0]

    def test_rejects_negative_price(self):
        with self.assertRaisesRegex(ValueError, "^price must be positive$"):
            self.load_event("trade", price="-100")

    def test_rejects_negative_quantity_for_trade_and_book(self):
        with self.assertRaisesRegex(ValueError, "^quantity must be positive$"):
            self.load_event("trade", quantity="-1")
        with self.assertRaisesRegex(ValueError, "^level quantity must be non-negative$"):
            self.load_event("depth_update", bids=[["68448.01", "-1"]])

    def test_rejects_non_finite_trade_and_book_values(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "^price must be finite$"):
                    self.load_event("trade", price=value)
                with self.assertRaisesRegex(ValueError, "^level quantity must be finite$"):
                    self.load_event("depth_update", bids=[["68448.01", value]])

    def test_rejects_zero_trade_and_order_quantities(self):
        for event_type in ("trade", "submit_order"):
            with self.subTest(event_type=event_type):
                with self.assertRaisesRegex(ValueError, "^quantity must be positive$"):
                    self.load_event(event_type, quantity="0")

    def test_rejects_zero_prices(self):
        cases = (
            ("trade", "price", {"price": "0"}),
            ("submit_order", "limit_price", {"limit_price": "0"}),
            ("book_snapshot", "level price", {"bids": [["0", "0.4"]]}),
            ("depth_update", "level price", {"bids": [["0", "0.4"]]}),
        )
        for event_type, field, changes in cases:
            with self.subTest(event_type=event_type):
                with self.assertRaisesRegex(ValueError, f"^{field} must be positive$"):
                    self.load_event(event_type, **changes)

    def test_accepts_zero_book_level_quantities(self):
        for event_type in ("book_snapshot", "depth_update"):
            with self.subTest(event_type=event_type):
                event = self.load_event(event_type, bids=[["68448.01", "0"]])
                self.assertEqual(event.bids[0].quantity, Decimal("0"))
