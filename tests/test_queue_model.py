import unittest
from decimal import Decimal

from market_maker.execution import FillEngine
from market_maker.models import BookLevel, BookSnapshot, DepthUpdate, Side, SubmitOrder, Trade
from market_maker.wallet import Wallet


D = Decimal
PRICE = D("68448")


class ProportionalQueueTests(unittest.TestCase):
    def start_order(self, model="proportional-cancellation", *, inflated_depth="0.60"):
        self.audit = []
        self.wallet = Wallet(btc=D("0.50"), usdt=D("34225"))
        self.engine = FillEngine(self.wallet, latency_ms=0, queue_model=model, audit_log=self.audit)
        self.engine.process_event(BookSnapshot(0, 1, (BookLevel(PRICE, D("0.40")),), ()))
        self.engine.process_event(SubmitOrder(0, 2, "bid", Side.BUY, PRICE, D("0.05")))
        self.depth(10, inflated_depth)

    def trade(self, time, quantity, *, trade_id=1, maker=True):
        return self.engine.process_event(Trade(time, trade_id, trade_id, PRICE, D(quantity), maker))

    def depth(self, time, quantity):
        self.engine.process_event(DepthUpdate(time, time, (BookLevel(PRICE, D(quantity)),), ()))

    def reconciliations(self, kind="QUEUE_CANCELLATION"):
        return [dict(event.details) for event in self.audit if event.kind == kind]

    def check_reconciled_drop(self):
        [change] = self.reconciliations()
        self.assertEqual(change["trade_quantity"], D("0.10"))
        self.assertEqual(change["cancellation_quantity"], D("0.20"))
        self.assertEqual(change["queue_before"], D("0.30"))
        self.assertEqual(change["queue_after"], D("0.18"))
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.18"))
        self.assertEqual(self.engine.get_order("bid").filled_quantity, 0)

    def test_trade_before_depth_reduces_queue_only_once(self):
        self.start_order()
        self.trade(230, "0.10")
        self.depth(250, "0.30")
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.30"))
        self.engine.advance_to(401)
        self.check_reconciled_drop()

    def test_depth_before_trade_waits_through_last_millisecond_and_deduplicates_trade(self):
        self.start_order()
        self.depth(250, "0.30")
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.40"))
        self.trade(400, "0.10")
        self.trade(400, "0.10")
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.30"))
        self.engine.advance_to(401)
        self.check_reconciled_drop()

    def test_cancellation_only_reduces_proportional_queue_without_filling(self):
        for model, expected_queue in (("trade-only", D("0.40")), ("proportional-cancellation", D("0.30"))):
            with self.subTest(model=model):
                self.start_order(model, inflated_depth="0.80")
                before = self.wallet.balances
                self.depth(250, "0.60")
                self.assertEqual(self.engine.queue_ahead("bid"), D("0.40"))
                self.engine.advance_to(401)
                self.assertEqual(self.engine.queue_ahead("bid"), expected_queue)
                self.assertEqual(self.engine.get_order("bid").filled_quantity, D("0"))
                self.assertEqual(self.wallet.balances, before)
                changes = self.reconciliations()
                if model == "proportional-cancellation":
                    [change] = changes
                    self.assertEqual(change["trade_quantity"], D("0"))
                    self.assertEqual(change["cancellation_quantity"], D("0.20"))
                else:
                    self.assertEqual(changes, [])

    def test_ambiguous_drop_with_replenishment_or_disappearance_creates_no_credit(self):
        for changed in ("0.40", "0"):
            with self.subTest(depth=changed):
                self.start_order()
                self.depth(250, changed)
                self.trade(260, "0.10")
                if changed != "0":
                    self.depth(275, "0.45")
                self.engine.advance_to(401)
                [skipped] = self.reconciliations("QUEUE_CANCELLATION_SKIPPED")
                self.assertEqual(skipped["queue_before"], skipped["queue_after"])
                self.assertEqual(self.engine.queue_ahead("bid"), D("0.30"))
                self.assertEqual(self.engine.get_order("bid").filled_quantity, 0)

    def test_drop_inside_activation_window_creates_no_credit(self):
        self.start_order()
        self.depth(100, "0.30")
        self.engine.advance_to(251)
        [skipped] = self.reconciliations("QUEUE_CANCELLATION_SKIPPED")
        self.assertIn("activation", skipped["reason"])
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.40"))

    def test_late_print_reverses_its_credit_before_ordinary_trade_matching(self):
        self.start_order(inflated_depth="0.80")
        self.depth(250, "0.60")
        self.engine.advance_to(401)
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.30"))
        self.assertIsNone(self.trade(420, "0.20"))
        [reversed_credit] = self.reconciliations("QUEUE_CANCELLATION_REVERSED")
        self.assertEqual(reversed_credit["queue_after"], D("0.40"))
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.20"))

    def test_trade_only_mode_stays_unaffected_by_the_same_depth_drop(self):
        self.start_order("trade-only")
        self.depth(250, "0.30")
        self.trade(260, "0.10")
        self.engine.advance_to(401)
        self.assertEqual(self.engine.queue_ahead("bid"), D("0.30"))
        self.assertEqual(self.reconciliations(), [])
