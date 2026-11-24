import json
import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from market_maker.binance_book import DEPTH_STREAM, FEED_STATUS_STREAM, REST_DEPTH_STREAM, TRADE_STREAM
from market_maker.binance_replay import load_binance_replay
from market_maker.models import CancelOrder, Side, SubmitOrder, Trade
from market_maker.recording import RecordedMessage, write_recording
from market_maker.replay import run_replay
from market_maker.wallet import Wallet, WalletBalances


PROJECT = Path(__file__).resolve().parents[1]
BID = Decimal("68448")


def status(when: int, state: str, **details):
    return when, FEED_STATUS_STREAM, {"state": state, **details}


def depth(when: int, first_id: int, last_id: int, quantity: str):
    data = {"U": first_id, "u": last_id, "b": [["68448", quantity]], "a": []}
    return when, DEPTH_STREAM, {"stream": DEPTH_STREAM, "data": data}


def snapshot(when: int, update_id: int):
    return when, REST_DEPTH_STREAM, {
        "lastUpdateId": update_id,
        "bids": [["68447", "1"]],
        "asks": [["68452", "0.50"]],
    }


def trade(when: int, trade_id: int, quantity: str):
    # Earlier exchange timestamps for later arrivals exercise receive-time replay.
    data = {"t": trade_id, "p": "68448", "q": quantity, "m": True, "E": 1700000001000 - trade_id}
    return when, TRADE_STREAM, {"stream": TRADE_STREAM, "data": data}


def opening_rows(queue_ahead: str = "0.40"):
    return [
        status(0, "syncing", reason="awaiting_snapshot"),
        depth(5, 100, 101, queue_ahead),
        snapshot(15, 100),
        trade(19, 90, "1"),
        status(20, "ready", last_update_id=101),
    ]


class BinanceReplayTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "captured.jsonl"
        self.wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("34225"))

    def record(self, rows):
        write_recording(self.path, (
            RecordedMessage(stream, sequence, timestamp_ms, json.dumps(payload), payload.get("data", {}).get("E"))
            for sequence, (timestamp_ms, stream, payload) in enumerate(rows, 1)
        ))

    def test_scripted_order_uses_received_time_buffered_queue_and_existing_wallet(self):
        self.record([
            *opening_rows(),
            trade(34, 91, "1"),
            depth(36, 102, 102, "0.15"),
            trade(37, 92, "0.25"),
            trade(38, 93, "0.30"),
        ])
        order = SubmitOrder(20, 1, "paper-bid", Side.BUY, BID, Decimal("0.05"))
        events = load_binance_replay(self.path, [order])

        result = run_replay(events, self.wallet)

        self.assertEqual([event.timestamp_ms for event in events if isinstance(event, Trade)], [19, 34, 37, 38])
        activation = next(event for event in result.audit_log if event.kind == "ORDER_ACTIVATED")
        self.assertEqual(activation.timestamp_ms, 35)
        self.assertEqual(dict(activation.details)["queue_ahead"], Decimal("0.40"))
        queue = next(event for event in result.audit_log if event.kind == "QUEUE_ADVANCED")
        self.assertEqual(dict(queue.details)["queue_after"], Decimal("0.15"))
        [fill] = [event for event in result.audit_log if event.kind == "FILL"]
        self.assertEqual((fill.timestamp_ms, dict(fill.details)["trade_id"]), (38, 93))
        self.assertEqual(result.fees_usdt, Decimal("2.5668"))
        self.assertEqual(
            result.balances,
            WalletBalances(Decimal("0.55"), Decimal("30800.0332"), Decimal("0"), Decimal("0")),
        )

    def test_gap_cancels_old_orders_and_recovery_waits_for_ready_marker(self):
        self.record([
            *opening_rows("0.02"),
            trade(40, 91, "0.04"),
            depth(45, 104, 104, "0"),
            status(45, "unavailable", reason="depth_gap"),
            trade(46, 92, "0.50"),
            depth(50, 200, 201, "0.10"),
            snapshot(55, 200),
            trade(57, 93, "0.50"),
            status(60, "ready", last_update_id=201),
            trade(80, 94, "0.13"),
            depth(5500, 202, 202, "0.10"),
        ])
        orders = [
            SubmitOrder(20, 1, "old-bid", Side.BUY, BID, Decimal("0.05")),
            SubmitOrder(42, 2, "pending-ask", Side.SELL, Decimal("68452"), Decimal("0.05")),
            SubmitOrder(46, 3, "paused-bid", Side.BUY, BID, Decimal("0.01")),
            CancelOrder(47, 4, "paused-bid"),
            SubmitOrder(60, 5, "fresh-bid", Side.BUY, BID, Decimal("0.03")),
        ]

        result = run_replay(load_binance_replay(self.path, orders), self.wallet)

        self.assertEqual([event.timestamp_ms for event in result.audit_log if event.kind == "FEED_READY"], [20, 60])
        cancellations = [event for event in result.audit_log if event.kind == "ORDER_CANCELED"]
        self.assertEqual([(event.timestamp_ms, dict(event.details)["order_id"]) for event in cancellations], [
            (45, "old-bid"), (45, "pending-ask"),
        ])
        self.assertEqual(dict(cancellations[-1].details)["reserved_btc"], Decimal("0"))
        self.assertEqual(dict(cancellations[-1].details)["reserved_usdt"], Decimal("0"))
        [rejected] = [dict(event.details) for event in result.audit_log if event.kind == "ORDER_REJECTED"]
        self.assertEqual(rejected, {"order_id": "paused-bid", "reason": "feed unavailable"})
        fills = [dict(event.details) for event in result.audit_log if event.kind == "FILL"]
        self.assertEqual([(fill["order_id"], fill["trade_id"]) for fill in fills], [("old-bid", 91), ("fresh-bid", 94)])
        self.assertEqual(fills[-1]["queue_before"], Decimal("0.10"))
        markouts = [dict(event.details) for event in result.audit_log if event.kind == "MARKOUT_5S"]
        self.assertIsNone(markouts[0]["markout_usdt_per_btc"])
        self.assertEqual(markouts[1]["markout_usdt_per_btc"], Decimal("0"))
        self.assertEqual(
            result.balances,
            WalletBalances(Decimal("0.55"), Decimal("30800.0332"), Decimal("0"), Decimal("0")),
        )

    def test_disconnect_marker_closes_order_even_without_a_depth_gap(self):
        self.record([
            *opening_rows(),
            status(40, "unavailable", reason="disconnected"),
            trade(41, 91, "1"),
        ])
        order = SubmitOrder(20, 1, "paper-bid", Side.BUY, BID, Decimal("0.05"))

        result = run_replay(load_binance_replay(self.path, [order]), self.wallet)

        [canceled] = [event for event in result.audit_log if event.kind == "ORDER_CANCELED"]
        self.assertEqual(canceled.timestamp_ms, 40)
        self.assertFalse(any(event.kind == "FILL" for event in result.audit_log))
        self.assertEqual(result.balances.reserved_usdt, Decimal("0"))
        self.assertIsNone(result.net_pnl_usdt)

    def test_raw_only_capture_requires_a_synchronized_snapshot(self):
        self.record([depth(5, 100, 101, "0.40"), trade(6, 90, "1")])

        with self.assertRaisesRegex(ValueError, "Capture it with --sync-book"):
            load_binance_replay(self.path)

    def test_cli_accepts_legacy_synchronized_recording_and_scripted_orders(self):
        self.record([depth(5, 100, 101, "0.02"), snapshot(15, 100), trade(31, 90, "0.07")])
        orders = self.path.with_name("orders.json")
        orders.write_text(json.dumps([{
            "type": "submit_order", "timestamp_ms": 15, "sequence": 1, "order_id": "paper-bid",
            "side": "buy", "limit_price": "68448", "quantity": "0.05",
        }]), encoding="utf-8")
        command = [sys.executable, "-B", "-m", "market_maker.replay", str(self.path), "--orders", str(orders)]

        first = subprocess.run(command, cwd=PROJECT, check=True, capture_output=True, text=True)
        second = subprocess.run(command, cwd=PROJECT, check=True, capture_output=True, text=True)

        self.assertEqual(first.stdout, second.stdout)
        self.assertIn("[15 ms] FEED_READY", first.stdout)
        self.assertIn("[31 ms] FILL order_id=paper-bid", first.stdout)
        self.assertIn("SUMMARY btc=0.55 usdt=30800.0332", first.stdout)
