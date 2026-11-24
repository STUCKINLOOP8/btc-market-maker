import subprocess
import sys
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from market_maker.models import FeedUnavailable, OrderStatus, Side, SubmitOrder
from market_maker.replay import DEFAULT_FIXTURE, ReplaySession, load_fixture, run_replay
from market_maker.wallet import Wallet, WalletBalances


PROJECT = Path(__file__).resolve().parents[1]


class ReplaySessionTests(unittest.TestCase):
    def setUp(self):
        self.wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))

    def test_step_in_timestamp_and_sequence_order_exposes_current_state(self):
        book, submit, depth, *_ = load_fixture(DEFAULT_FIXTURE)
        book = replace(book, timestamp_ms=submit.timestamp_ms)
        session = ReplaySession([depth, submit, book], self.wallet)

        self.assertIsNone(session.book)
        self.assertEqual(session.orders, ())
        self.assertEqual(session.step().event, book)
        self.assertEqual(session.book, book)
        self.assertEqual(session.balances.reserved_usdt, Decimal("0"))

        submission = session.step()
        [pending] = session.orders
        self.assertEqual(submission.event, submit)
        self.assertEqual([entry.kind for entry in submission.audit_log], ["ORDER_SUBMITTED"])
        self.assertIs(pending.status, OrderStatus.PENDING_NEW)
        self.assertIsNone(session.queue_ahead(pending.order_id))
        self.assertEqual(session.balances.reserved_usdt, Decimal("3424.9668"))

        update = session.step()
        self.assertEqual(update.event, depth)
        self.assertEqual([(entry.kind, entry.timestamp_ms) for entry in update.audit_log], [
            ("ORDER_ACTIVATED", 1016), ("BOOK", 1029),
        ])
        self.assertEqual(session.book.bids, depth.bids)
        self.assertEqual(session.queue_ahead(pending.order_id), Decimal("0.40"))
        self.assertIs(session.orders[0].status, OrderStatus.ACTIVE)
        self.assertIs(pending.status, OrderStatus.PENDING_NEW)

    def test_step_audit_and_wallet_match_run_to_completion(self):
        events = load_fixture(DEFAULT_FIXTURE)
        session = ReplaySession(reversed(events), self.wallet)
        stepped_audit = []
        for event in events:
            step = session.step()
            self.assertEqual(step.event, event)
            stepped_audit.extend(step.audit_log)

        whole_run = run_replay(events, Wallet(btc=Decimal("0.50"), usdt=Decimal("34225")))
        self.assertEqual(tuple(stepped_audit), whole_run.audit_log)
        self.assertEqual(session.result(), whole_run)
        self.assertIsNone(session.step())
        self.assertEqual(session.result(), whole_run)

    def test_exact_markout_waits_for_last_input_at_same_timestamp(self):
        events = load_fixture(DEFAULT_FIXTURE)
        horizon_book = events[-2]
        final_book = replace(events[-1], timestamp_ms=horizon_book.timestamp_ms)
        session = ReplaySession([*events[:-1], final_book], self.wallet)
        for _ in events[:-2]:
            session.step()

        first = session.step()
        self.assertEqual(first.event, horizon_book)
        self.assertEqual([entry.kind for entry in first.audit_log], ["BOOK"])
        self.assertEqual(session.result().pending_markouts, 2)

        last = session.step()
        self.assertEqual(last.event, final_book)
        book_entry, markout = last.audit_log
        self.assertEqual(book_entry.kind, "BOOK")
        self.assertEqual(markout.kind, "MARKOUT_5S")
        self.assertEqual(dict(markout.details)["mid_at_horizon"], Decimal("68450"))
        self.assertEqual(session.result().pending_markouts, 1)

    def test_step_clears_book_on_feed_loss_and_restores_it_on_snapshot(self):
        book, submit, *_ = load_fixture(DEFAULT_FIXTURE)
        outage = FeedUnavailable(submit.timestamp_ms + 1, 3, "depth_gap")
        recovery = replace(book, timestamp_ms=outage.timestamp_ms + 1, sequence=4)
        session = ReplaySession([book, submit, outage, recovery], self.wallet)
        session.step()
        session.step()

        self.assertEqual(session.step().event, outage)
        self.assertIsNone(session.book)
        self.assertIs(session.orders[0].status, OrderStatus.CANCELED)
        self.assertEqual(session.balances.reserved_usdt, Decimal("0"))
        self.assertEqual(session.step().event, recovery)
        self.assertEqual(session.book, recovery)

    def test_end_of_input_does_not_advance_pending_placement(self):
        book, submit, *_ = load_fixture(DEFAULT_FIXTURE)
        session = ReplaySession([book, submit], self.wallet)
        session.step()
        session.step()
        final = session.result()

        self.assertIsNone(session.step())
        self.assertEqual(session.result(), final)
        self.assertIs(session.orders[0].status, OrderStatus.PENDING_NEW)
        self.assertEqual(session.balances.reserved_usdt, Decimal("3424.9668"))

    def test_round_trip_fees_and_wallet_reconcile(self):
        result = run_replay(load_fixture(DEFAULT_FIXTURE), self.wallet, fee_guard=False)
        fills = [dict(event.details) for event in result.audit_log if event.kind == "FILL"]
        queue_step = next(event for event in result.audit_log if event.kind == "QUEUE_ADVANCED")

        self.assertEqual(dict(queue_step.details)["queue_before"], Decimal("0.40"))
        self.assertEqual(dict(queue_step.details)["queue_after"], Decimal("0.15"))
        self.assertEqual(dict(queue_step.details)["fill_quantity"], Decimal("0"))
        self.assertEqual([fill["fill_quantity"] for fill in fills], [Decimal("0.05"), Decimal("0.05")])
        self.assertEqual([fill["fee_usdt"] for fill in fills], [Decimal("2.5668"), Decimal("2.56695")])
        self.assertEqual(result.fees_usdt, Decimal("5.13375"))
        self.assertEqual(result.net_pnl_usdt, Decimal("-4.93375"))
        self.assertEqual(result.excess_pnl_usdt, Decimal("-4.93375"))
        self.assertEqual(
            result.balances,
            WalletBalances(Decimal("0.50"), Decimal("34220.06625"), Decimal("0"), Decimal("0")),
        )

    def test_markouts_use_book_at_five_second_horizon_and_are_signed_by_side(self):
        result = run_replay(load_fixture(DEFAULT_FIXTURE), self.wallet)
        markouts = [event for event in result.audit_log if event.kind == "MARKOUT_5S"]

        self.assertEqual([event.timestamp_ms for event in markouts], [6500, 8000])
        self.assertEqual(
            [
                (details["order_id"], details["mid_at_fill"], details["mid_at_horizon"],
                 details["markout_usdt_per_btc"])
                for details in map(lambda event: dict(event.details), markouts)
            ],
            [
                ("buy-1", Decimal("68450"), Decimal("68451.50"), Decimal("1.50")),
                ("sell-1", Decimal("68450"), Decimal("68451.50"), Decimal("-1.50")),
            ],
        )
        self.assertEqual(result.pending_markouts, 0)

    def test_replay_ending_before_horizon_reports_pending_markouts(self):
        events = load_fixture(DEFAULT_FIXTURE)
        through_6499 = [event for event in events if event.timestamp_ms < 6500]
        first_markout_book = next(event for event in events if event.timestamp_ms == 6500)
        through_6499.append(replace(first_markout_book, timestamp_ms=6499))

        result = run_replay(through_6499, self.wallet)

        self.assertFalse(any(event.kind == "MARKOUT_5S" for event in result.audit_log))
        self.assertEqual(result.pending_markouts, 2)

    def test_audit_records_rejection_and_cancellation_at_effective_time(self):
        events = load_fixture(PROJECT / "tests" / "fixtures" / "milestone_1_events.json")
        events.append(SubmitOrder(1018, 6, "too-large", Side.BUY, Decimal("68448.01"), Decimal("0.30")))
        events.append(replace(events[0], timestamp_ms=1030, sequence=7))

        result = run_replay(events, self.wallet)

        self.assertEqual([event.timestamp_ms for event in result.audit_log], [
            1000, 1002, 1005, 1005, 1010, 1017, 1018, 1025, 1030,
        ])
        rejection = next(event for event in result.audit_log if event.kind == "ORDER_REJECTED")
        self.assertIn("breaches the 20% inventory cap", dict(rejection.details)["reason"])
        cancellation = next(event for event in result.audit_log if event.kind == "ORDER_CANCELED")
        self.assertEqual(cancellation.timestamp_ms, 1025)
        self.assertEqual(dict(cancellation.details)["reserved_usdt"], Decimal("0"))

    def test_cli_replays_same_fixture_with_identical_output(self):
        command = [sys.executable, "-B", "-m", "market_maker.replay"]
        first = subprocess.run(command, cwd=PROJECT, check=True, capture_output=True, text=True)
        second = subprocess.run(command, cwd=PROJECT, check=True, capture_output=True, text=True)

        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(first.stderr, "")
        self.assertIn("[6500 ms] MARKOUT_5S order_id=buy-1", first.stdout)
        self.assertIn("markout_usdt_per_btc=1.5", first.stdout)
        self.assertIn("fees_usdt=5.13375 net_pnl_usdt=-4.93", first.stdout)
