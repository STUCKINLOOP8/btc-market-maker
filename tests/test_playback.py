import asyncio
import http.client
import json
import socket
import threading
import time
import unittest
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from queue import Empty, SimpleQueue
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import uvicorn

from market_maker.binance_book import DEPTH_STREAM, FEED_STATUS_STREAM, REST_DEPTH_STREAM, TRADE_STREAM
from market_maker.binance_replay import load_binance_replay
from market_maker.playback import BOOK_LEVELS, create_app
from market_maker.recording import RecordedMessage, write_recording
from market_maker.replay import DEFAULT_STARTING_BTC, DEFAULT_STARTING_USDT, load_fixture, run_replay
from market_maker.wallet import Wallet


API = "/api/replay"
TIMELINE_RECORDING = "order_timeline_recording.jsonl"
TIMELINE_ORDERS = "order_timeline_orders.json"


class PlaybackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.root = Path(directory.name)
        fixtures = Path(__file__).resolve().parents[1] / "fixtures"
        for file in (TIMELINE_RECORDING, TIMELINE_ORDERS):
            (cls.root / file).write_bytes((fixtures / file).read_bytes())
        cls.recording = cls.root / "session.jsonl"
        cls.orders = cls.root / "orders.json"
        rows = [
            (0, DEPTH_STREAM, {"data": {"U": 100, "u": 101, "b": [["68448", "0.02"]], "a": []}}),
            (0, REST_DEPTH_STREAM, {"lastUpdateId": 100,
                                    "bids": [[str(price), "1"] for price in range(68420, 68448)],
                                    "asks": [["68452", "0.5"]]}),
            (20000, TRADE_STREAM, {"data": {"t": 1, "p": "68448", "q": "0.07", "m": True}}),
            (25000, DEPTH_STREAM, {"data": {"U": 102, "u": 102, "b": [["68449", "0.01"]], "a": []}}),
        ]
        cls.write_session(cls.recording, rows)
        cls.panel_recording = cls.root / "panel-session.jsonl"
        cls.write_session(cls.panel_recording, [
            *rows[:2],
            (100, TRADE_STREAM, {"data": {"t": 2, "p": "68448", "q": "0.01", "m": True}}),
            *rows[2:],
            (26000, FEED_STATUS_STREAM, {"state": "unavailable", "reason": "depth_gap"}),
            (27000, DEPTH_STREAM, {"data": {"U": 200, "u": 201, "b": [["68450", "0.04"]], "a": []}}),
            (27500, REST_DEPTH_STREAM, {"lastUpdateId": 200, "bids": [["68449", "1"]],
                                        "asks": [["68452", "0.5"]]}),
            (28000, FEED_STATUS_STREAM, {"state": "ready", "last_update_id": 201}),
        ])
        cls.orders.write_text(json.dumps([{
            "type": "submit_order", "timestamp_ms": 0, "sequence": 1, "order_id": "paper-bid",
            "side": "buy", "limit_price": "68448", "quantity": "0.05",
        }]), encoding="utf-8")

        listener = socket.socket()
        cls.addClassCleanup(listener.close)
        listener.bind(("127.0.0.1", 0))
        cls.port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(
            create_app(cls.root), log_level="error", timeout_graceful_shutdown=2,
        ))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()

        def stop():
            server.should_exit = True
            thread.join(timeout=5)

        cls.addClassCleanup(stop)
        deadline = time.monotonic() + 5
        while not server.started:
            if time.monotonic() >= deadline or not thread.is_alive():
                raise RuntimeError("Playback test server failed to start")
            time.sleep(0.01)

    @staticmethod
    def write_session(path, rows):
        write_recording(path, (
            RecordedMessage(stream, sequence, timestamp, json.dumps(payload), None)
            for sequence, (timestamp, stream, payload) in enumerate(rows, 1)
        ))

    def request(self, method, path, payload=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            body = json.dumps(payload) if payload is not None else None
            connection.request(method, path, body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            data = response.read()
            if response.headers.get("Content-Type") == "application/json":
                data = json.loads(data)
            return response.status, data
        finally:
            connection.close()

    def action(self, name, payload=None):
        status, data = self.request("POST", f"{API}/{name}", payload)
        self.assertEqual(status, 200, data)
        return data

    def load(self, *, orders=True):
        self.action("speed", {"speed": 1})
        return self.action("load", {"recording": self.recording.name, "orders": self.orders.name if orders else None})

    def state(self):
        status, data = self.request("GET", f"{API}/state")
        self.assertEqual(status, 200)
        return data

    @contextmanager
    def stream(self, last_event_id=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            headers = {"Last-Event-ID": last_event_id} if last_event_id is not None else {}
            connection.request("GET", f"{API}/stream", headers=headers)
            with connection.getresponse() as response:
                self.assertEqual(response.status, 200)
                self.assertIn("text/event-stream", response.headers["Content-Type"])
                yield response
        finally:
            connection.close()

    def next_update(self, response):
        fields = {}
        while True:
            line = response.readline()
            self.assertTrue(line, "The event stream closed")
            if not line.strip() and fields:
                self.assertEqual(fields["event"], "replay")
                return fields.get("id"), json.loads(fields["data"])
            if line.strip() and not line.startswith(b":"):
                key, value = line.decode().rstrip("\r\n").split(":", 1)
                fields[key] = value.lstrip()

    def expected_result(self):
        return run_replay(
            load_binance_replay(self.recording, load_fixture(self.orders)),
            Wallet(btc=DEFAULT_STARTING_BTC, usdt=DEFAULT_STARTING_USDT),
        )

    def test_load_controls_and_file_validation(self):
        status, page = self.request("GET", "/")
        self.assertEqual(status, 200)
        for label in (b"Load recording", b">Play<", b">Pause<", b">Step<", b">Halt<", b"Remaining BTC",
                      b"Depth ladder", b"Portfolio equity",
                      b"Buy-and-hold", b"Paper wallet", b"Queue ahead", b"Order timeline", b"Wallet changes"):
            self.assertIn(label, page)
        for asset in ("/assets/playback.css", "/assets/playback.js", "/assets/order-timeline.js", "/assets/about.css"):
            self.assertEqual(self.request("GET", asset)[0], 200)

        about_status, about_page = self.request("GET", "/about")
        self.assertEqual(about_status, 200)
        for label in (b"Pixel Market Maker", b"Watch the market", b"Post a bid and ask",
                      b"Simulate fills and fees", b"Inspect every order",
                      b"Fixed spread", b"Avellaneda-Stoikov",
                      b"All funds are simulated, results are not predictions"):
            self.assertIn(label, about_page)

        loaded = self.load(orders=False)
        self.assertEqual(loaded["mode"], "replay")
        self.assertEqual(loaded["status"], "paused")
        self.assertIsNone(loaded["source"]["orders"])
        for payload in ({"recording": "../elsewhere.jsonl"}, {"recording": "missing.jsonl"},
                        {"recording": self.recording.name, "orders": "missing.json"}):
            with self.subTest(payload=payload):
                status, error = self.request("POST", f"{API}/load", payload)
                self.assertEqual(status, 400, error)
                self.assertEqual(self.state()["run_id"], loaded["run_id"])
        status, _ = self.request("POST", f"{API}/speed", {"speed": 0})
        self.assertEqual(status, 422)

    def test_live_paper_quotes_pause_on_gap_resume_after_snapshot_and_halt_clears_reserves(self):
        frames = [SimpleQueue(), SimpleQueue()]
        connections = [MagicMock(), MagicMock()]
        for connection, incoming in zip(connections, frames):
            async def recv(*, decode, queue=incoming):
                while True:
                    try:
                        return queue.get_nowait()
                    except Empty:
                        await asyncio.sleep(0.005)
            connection.__aenter__.return_value.recv = recv

        def depth(update_id, bids=()):
            return json.dumps({"stream": DEPTH_STREAM, "data": {"U": update_id, "u": update_id, "b": bids, "a": []}})

        def trade(trade_id, price):
            return json.dumps({"stream": TRADE_STREAM, "data": {"t": trade_id, "p": price, "q": "0.0004", "m": True}})

        snapshot_requested = threading.Event()
        allow_resync = threading.Event()
        snapshots = iter([(10, "68448", "68452"), (20, "68449", "68453")])

        def fetch_snapshot():
            update_id, bid, ask = next(snapshots)
            if update_id == 20:
                snapshot_requested.set()
                if not allow_resync.wait(3):
                    raise TimeoutError("The test did not release the resync snapshot")
            return json.dumps({"lastUpdateId": update_id, "bids": [[bid, "1"]], "asks": [[ask, "1"]]})

        def wait_for(predicate):
            deadline = time.monotonic() + 3
            while True:
                state = self.state()
                if predicate(state):
                    return state
                if time.monotonic() >= deadline:
                    self.fail(f"Live paper state did not reach the expected condition: {state}")
                time.sleep(0.01)

        def assert_no_reserves(state):
            self.assertEqual(state["unresolved_order_count"], 0)
            for asset in ("btc", "usdt"):
                self.assertEqual(Decimal(state["balances"][f"reserved_{asset}"]), 0)

        with (
            patch("market_maker.record.connect", side_effect=connections) as connect,
            patch("market_maker.record._fetch_snapshot", side_effect=fetch_snapshot),
            patch("market_maker.record.RECONNECT_DELAY_SECONDS", 0.02),
            patch("market_maker.record.DEFAULT_DEPTH_TIMEOUT_SECONDS", 1.5),
        ):
            status, state = self.request("POST", "/api/live/start", {"spread_usdt": "120"})
            self.assertEqual(status, 200, state)
            self.assertEqual((state["mode"], state["status"], state["feed"]["health"]), ("live", "live", "syncing"))
            self.assertEqual(self.request("POST", f"{API}/step")[0], 409)
            self.assertEqual(self.request("POST", f"{API}/load", {"recording": self.recording.name})[0], 409)
            try:
                frames[0].put(depth(10))
                ready = wait_for(lambda state: sum(order["status"] == "ACTIVE" for order in state["orders"]) == 2)
                self.assertEqual(ready["feed"]["health"], "ready")
                self.assertGreaterEqual(ready["feed"]["age_ms"], 0)
                working_ids = {order["order_id"] for order in ready["orders"]}
                prices = {order["side"]: Decimal(order["limit_price"]) for order in ready["orders"]}
                self.assertEqual(prices, {"buy": Decimal("68390"), "sell": Decimal("68510")})

                frames[0].put(trade(1, "68390"))
                filled = wait_for(lambda state: state["last_fill"] is not None)
                self.assertEqual(Decimal(filled["last_fill"]["fill_quantity"]), Decimal("0.0004"))
                self.assertEqual(Decimal(filled["balances"]["btc"]), Decimal("0.5004"))
                self.assertGreaterEqual(filled["feed"]["age_ms"], ready["feed"]["age_ms"])

                frames[0].put(depth(12))
                paused = wait_for(lambda state: state["feed"]["health"] == "unavailable")
                self.assertEqual(paused["feed"]["reason"], "depth_gap")
                self.assertIsNone(paused["book"])
                assert_no_reserves(paused)

                frames[1].put(depth(20))
                self.assertTrue(snapshot_requested.wait(2))
                frames[1].put(trade(2, "68390"))
                resyncing = self.state()
                self.assertEqual(resyncing["feed"]["health"], "unavailable")
                self.assertEqual({order["order_id"] for order in resyncing["orders"]}, working_ids)
                assert_no_reserves(resyncing)
                allow_resync.set()
                resumed = wait_for(lambda state: sum(order["status"] == "ACTIVE" for order in state["orders"]) == 2)
                self.assertEqual(resumed["feed"]["health"], "ready")
                quotes = {order["side"]: Decimal(order["limit_price"]) for order in resumed["orders"]
                          if order["status"] == "ACTIVE"}
                self.assertEqual(quotes, {"buy": Decimal("68391"), "sell": Decimal("68511")})
                self.assertEqual(Decimal(resumed["portfolio"]["filled_volume_btc"]), Decimal("0.0004"))
                self.assertGreater(Decimal(resumed["balances"]["reserved_btc"]), 0)
                self.assertGreater(Decimal(resumed["balances"]["reserved_usdt"]), 0)

                self.action("halt")
                halted = wait_for(lambda state: state["status"] == "halted")
                assert_no_reserves(halted)
                self.assertEqual(halted["feed"]["health"], "stopped")
                self.assertEqual(Decimal(halted["balances"]["btc"]), Decimal("0.5004"))
                self.assertEqual(connect.call_count, 2)

                with self.stream() as response:
                    _, update = self.next_update(response)
                kinds = [entry["kind"] for entry in update["audit_log"]]
                self.assertEqual(kinds.count("FILL"), 1)
                gap = kinds.index("FEED_UNAVAILABLE", kinds.index("FEED_READY"))
                resync = kinds.index("FEED_READY", gap)
                self.assertNotIn("ORDER_SUBMITTED", kinds[gap:resync])
                self.assertIn("ORDER_SUBMITTED", kinds[resync:kinds.index("HALT_REQUESTED")])
                self.assertNotIn("ORDER_SUBMITTED", kinds[kinds.index("HALT_REQUESTED"):])
            finally:
                allow_resync.set()
                self.action("halt")
                wait_for(lambda state: state["status"] == "halted")
                self.action("load", {"recording": self.recording.name})

    def test_terminal_loads_all_three_strategies_through_replay(self):
        for name in ("fixed-spread", "avellaneda-stoikov", "avellaneda-stoikov-imbalance"):
            with self.subTest(strategy=name):
                payload = {"recording": TIMELINE_RECORDING, "strategy": name, "spread_usdt": "120"}
                loaded = self.action("load", payload)
                self.assertEqual(loaded["source"]["strategy"], name)
                self.assertIsNone(loaded["source"]["orders"])
                with self.stream() as response:
                    self.next_update(response)
                    self.action("step")
                    self.next_update(response)
                    stepped = self.action("step")
                    _, frame = self.next_update(response)
                self.assertEqual([order["side"] for order in stepped["orders"]], ["buy", "sell"])
                self.assertEqual([order["status"] for order in stepped["orders"]], ["PENDING_NEW"] * 2)
                self.assertEqual(sum(entry["kind"] == "QUOTE_PROPOSED" for entry in frame["audit_log"]), 2)
                if name.endswith("-imbalance"):
                    self.assertTrue(any(entry["kind"] == "QUOTE_ROUNDED" for entry in frame["audit_log"]))

        status, error = self.request("POST", f"{API}/load", {**payload, "orders": TIMELINE_ORDERS})
        self.assertEqual(status, 400)
        self.assertIn("Choose a strategy or an order script", error["detail"])
        self.assertEqual(self.state()["run_id"], loaded["run_id"])

    def test_step_streams_audit_and_finishes_with_cli_balances(self):
        self.load()
        audit_log = []
        with self.stream() as response:
            _, initial = self.next_update(response)
            self.assertEqual(initial["state"]["position"], 0)
            state = initial["state"]
            while state["status"] != "finished":
                stepped = self.action("step")
                _, update = self.next_update(response)
                self.assertEqual(update["state"], stepped)
                self.assertEqual(stepped["position"], state["position"] + 1)
                state = stepped
                audit_log.extend(update["audit_log"])

        expected = self.expected_result()
        self.assertEqual(audit_log, [
            {
                "timestamp_ms": entry.timestamp_ms,
                "kind": entry.kind,
                "details": {key: str(value) if isinstance(value, Decimal) else value for key, value in entry.details},
            }
            for entry in expected.audit_log
        ])
        for field in ("btc", "usdt", "reserved_btc", "reserved_usdt"):
            self.assertEqual(state["balances"][field], str(getattr(expected.balances, field)))
        self.assertEqual(state["orders"][0]["filled_quantity"], "0.05")
        self.assertEqual(len(state["book"]["bids"]), BOOK_LEVELS)
        self.assertEqual(state["book"]["bids"][0], {"price": "68449", "quantity": "0.01"})
        fill = next(event for event in audit_log if event["kind"] == "FILL")
        self.assertEqual(fill["details"]["fee_usdt"], "2.5668000")
        status, _ = self.request("POST", f"{API}/step")
        self.assertEqual(status, 409)

    def test_pause_during_gap_and_change_speed_while_playing(self):
        self.load()
        self.action("speed", {"speed": 25})
        with self.stream() as response:
            self.next_update(response)
            self.action("play")
            while True:
                _, update = self.next_update(response)
                if update["state"]["position"] >= 3:
                    break
            self.assertEqual(update["state"]["position"], 3)
            status, _ = self.request("POST", f"{API}/step")
            self.assertEqual(status, 409)
            paused = self.action("pause")
            self.assertEqual(paused["timestamp_ms"], 0)
            self.assertEqual(paused["status"], "paused")
            time.sleep(0.9)
            self.assertEqual(self.state(), paused)
            self.action("speed", {"speed": 1})
            self.action("play")
            self.assertEqual(self.action("speed", {"speed": 100})["speed"], 100)
            while True:
                _, update = self.next_update(response)
                if update["state"]["status"] == "finished":
                    break

        expected = self.expected_result()
        self.assertEqual(update["state"]["timestamp_ms"], 25000)
        self.assertEqual(update["state"]["audit_count"], len(expected.audit_log))
        self.assertEqual(update["state"]["balances"]["usdt"], str(expected.balances.usdt))

    def test_stream_reconnect_delivers_only_missed_audit_entries_and_resets_on_load(self):
        first_run = self.load()
        self.action("step")
        with self.stream() as response:
            cursor, first = self.next_update(response)
        self.assertEqual([entry["kind"] for entry in first["audit_log"]], ["FEED_UNAVAILABLE"])
        self.action("step")
        with self.stream(cursor) as response:
            _, resumed = self.next_update(response)
        self.assertEqual([entry["kind"] for entry in resumed["audit_log"]], ["FEED_READY", "BOOK"])

        second_run = self.load()
        self.assertNotEqual(first_run["run_id"], second_run["run_id"])
        self.action("step")
        with self.stream(cursor) as response:
            _, reset = self.next_update(response)
        self.assertEqual(reset["state"]["run_id"], second_run["run_id"])
        self.assertEqual([entry["kind"] for entry in reset["audit_log"]], ["FEED_UNAVAILABLE"])

    def test_order_timeline_uses_streamed_partial_fill_and_cancel_audit(self):
        self.action("load", {"recording": TIMELINE_RECORDING, "orders": TIMELINE_ORDERS})
        with self.stream() as response:
            _, frame = self.next_update(response)
            self.assertEqual(frame["state"]["starting_balances"],
                             {"btc": "0.50", "usdt": "34225", "reserved_btc": "0", "reserved_usdt": "0"})
            audit = []
            while frame["state"]["status"] != "finished":
                self.action("step")
                _, frame = self.next_update(response)
                audit.extend(frame["audit_log"])
                timestamp_ms = frame["state"]["timestamp_ms"]
                if timestamp_ms == 130:
                    self.assertEqual(frame["state"]["orders"][0]["status"], "PARTIALLY_FILLED")
                elif timestamp_ms == 140:
                    self.assertEqual(frame["state"]["orders"][0]["status"], "CANCEL_PENDING")
                    self.assertEqual(Decimal(frame["state"]["balances"]["reserved_usdt"]), Decimal("2054.98008"))

        entries = [entry for entry in audit if entry["details"].get("order_id") == "partial-bid"]
        self.assertEqual([(entry["timestamp_ms"], entry["kind"]) for entry in entries], [
            (100, "ORDER_SUBMITTED"), (115, "ORDER_ACTIVATED"), (120, "QUEUE_ADVANCED"),
            (130, "FILL"), (140, "CANCEL_REQUESTED"), (155, "ORDER_CANCELED"),
        ])
        submitted, activation, queue, fill, _, canceled = [entry["details"] for entry in entries]
        self.assertEqual(Decimal(submitted["reserved_usdt"]), Decimal("3424.9668"))
        self.assertEqual(Decimal(activation["queue_ahead"]), Decimal("0.04"))
        self.assertEqual((queue["trade_id"], Decimal(queue["queue_before"]), Decimal(queue["queue_after"])),
                         (101, Decimal("0.04"), Decimal("0.015")))
        self.assertEqual(fill["trade_id"], 102)
        self.assertEqual(Decimal(fill["trade_quantity"]), Decimal("0.035"))
        self.assertEqual(Decimal(fill["fill_quantity"]), Decimal("0.02"))
        self.assertEqual(Decimal(fill["fee_usdt"]), Decimal("1.02672"))
        self.assertEqual(Decimal(fill["reserved_usdt"]), Decimal("2054.98008"))
        self.assertEqual(Decimal(canceled["reserved_usdt"]), 0)
        self.assertEqual(Decimal(canceled["filled_quantity"]), Decimal("0.02"))
        self.assertEqual((canceled["btc"], canceled["usdt"]), (fill["btc"], fill["usdt"]))
        self.assertEqual((Decimal(fill["btc"]), Decimal(fill["usdt"])), (Decimal("0.52"), Decimal("32855.01328")))
        [rejection] = [entry for entry in audit if entry["kind"] == "ORDER_REJECTED"]
        self.assertEqual(rejection["details"]["order_id"], "over-cap-bid")
        self.assertIn("breaches the 20% inventory cap", rejection["details"]["reason"])

    def test_halt_during_placement_blocks_submissions_and_waits_for_cancel_confirmation(self):
        orders = json.loads((self.root / TIMELINE_ORDERS).read_text(encoding="utf-8"))
        orders.append(dict(orders[0], timestamp_ms=110, sequence=4, order_id="blocked-bid"))
        script = self.root / "halt_orders.json"
        script.write_text(json.dumps(orders), encoding="utf-8")
        payload = {"recording": TIMELINE_RECORDING, "orders": script.name}
        self.action("load", payload)
        self.action("speed", {"speed": 0.1})
        self.action("step")
        self.action("step")
        pending = self.action("step")
        self.assertEqual(pending["timestamp_ms"], 100)
        self.assertEqual(pending["orders"][0]["status"], "PENDING_NEW")
        self.assertEqual(Decimal(pending["balances"]["reserved_usdt"]), Decimal("3424.9668"))

        with self.stream() as response:
            self.next_update(response)
            halting = self.action("halt")
            self.assertEqual(halting["status"], "halting")
            self.assertEqual(halting["unresolved_order_count"], 1)
            self.assertEqual(halting["balances"], pending["balances"])
            for action, data in (("play", None), ("step", None), ("load", payload)):
                self.assertEqual(self.request("POST", f"{API}/{action}", data)[0], 409)
            self.action("halt")
            audit = []
            while True:
                _, frame = self.next_update(response)
                audit.extend(frame["audit_log"])
                state = frame["state"]
                if state["status"] == "halted":
                    break
                self.assertEqual(state["unresolved_order_count"], 1)
                self.assertEqual(Decimal(state["balances"]["reserved_usdt"]), Decimal("3424.9668"))

        self.assertEqual(state["timestamp_ms"], 130)
        self.assertEqual(state["unresolved_order_count"], 0)
        self.assertIsNone(state["next_timestamp_ms"])
        self.assertEqual([order["order_id"] for order in state["orders"]], ["partial-bid"])
        self.assertEqual(state["orders"][0]["status"], "CANCELED")
        self.assertEqual(state["balances"]["btc"], "0.50")
        self.assertEqual(Decimal(state["balances"]["reserved_btc"]), 0)
        self.assertEqual(Decimal(state["balances"]["reserved_usdt"]), 0)
        self.assertEqual(self.action("halt"), state)

        lifecycle = {entry["kind"]: entry for entry in audit}
        self.assertEqual(sum(entry["kind"] == "HALT_REQUESTED" for entry in audit), 1)
        self.assertEqual(lifecycle["ORDER_SKIPPED"]["details"]["order_id"], "blocked-bid")
        self.assertEqual(lifecycle["ORDER_SKIPPED"]["details"]["reason"], "halt requested")
        self.assertEqual(lifecycle["ORDER_ACTIVATED"]["timestamp_ms"], 115)
        self.assertEqual(lifecycle["CANCEL_REQUESTED"]["timestamp_ms"], 115)
        self.assertEqual(lifecycle["ORDER_CANCELED"]["timestamp_ms"], 130)
        self.assertNotIn("FILL", lifecycle)

    def test_halt_after_recording_end_clears_reserves_and_keeps_residual_btc(self):
        orders = json.loads((self.root / TIMELINE_ORDERS).read_text(encoding="utf-8"))
        script = self.root / "hold_partial.json"
        script.write_text(json.dumps(orders[:1]), encoding="utf-8")
        tape = self.root / "hold_partial.jsonl"
        lines = (self.root / TIMELINE_RECORDING).read_text(encoding="utf-8").splitlines()
        last = dict(json.loads(lines[-1]), received_elapsed_ms=5120)
        tape.write_text("\n".join([*lines[:-1], json.dumps(last)]) + "\n", encoding="utf-8")
        self.action("load", {"recording": tape.name, "orders": script.name})
        self.action("speed", {"speed": 0.1})
        while True:
            finished = self.action("step")
            if finished["status"] == "finished":
                break
        self.assertEqual(finished["timestamp_ms"], 5120)
        self.assertEqual(finished["orders"][0]["status"], "PARTIALLY_FILLED")
        self.assertEqual(Decimal(finished["balances"]["reserved_usdt"]), Decimal("2054.98008"))

        with self.stream() as response:
            self.next_update(response)
            requested = self.action("halt")
            self.assertEqual(requested["status"], "halting")
            self.assertEqual(requested["orders"][0]["status"], "CANCEL_PENDING")
            while True:
                _, frame = self.next_update(response)
                self.assertFalse(any(entry["kind"] == "MARKOUT_5S" for entry in frame["audit_log"]))
                state = frame["state"]
                if state["status"] == "halted":
                    break
        self.assertEqual(state["position"], finished["position"])
        self.assertEqual(state["timestamp_ms"], 5135)
        self.assertEqual(state["unresolved_order_count"], 0)
        self.assertEqual(Decimal(state["balances"]["btc"]), Decimal("0.52"))
        self.assertEqual(Decimal(state["portfolio"]["inventory_delta_btc"]), Decimal("0.02"))
        self.assertEqual(state["balances"]["reserved_btc"], "0")
        self.assertEqual(state["balances"]["reserved_usdt"], "0")
        self.assertEqual(state["orders"][0]["status"], "CANCELED")
        self.assertEqual(Decimal(state["orders"][0]["filled_quantity"]), Decimal("0.02"))

    def test_portfolio_depth_queue_and_wallet_match_replay(self):
        self.action("load", {"recording": self.panel_recording.name, "orders": self.orders.name})
        self.action("step")
        opening = self.action("step")
        self.assertEqual(opening["book"]["bids"][0], {"price": "68448", "quantity": "0.02"})
        self.assertEqual(opening["book"]["asks"][0], {"price": "68452", "quantity": "0.5"})
        self.assertEqual(Decimal(opening["portfolio"]["equity_usdt"]), Decimal("68450"))

        submitted = self.action("step")
        self.assertEqual(Decimal(submitted["balances"]["reserved_usdt"]), Decimal("3424.9668"))
        self.assertEqual(Decimal(submitted["balances"]["available_usdt"]), Decimal("30800.0332"))
        self.assertEqual(submitted["portfolio"], opening["portfolio"])

        active = self.action("step")
        [order] = active["orders"]
        self.assertEqual(order["status"], "ACTIVE")
        self.assertEqual(order["queue_ahead"], "0.01")
        filled = self.action("step")
        self.assertEqual(filled["orders"][0]["status"], "FILLED")
        self.assertEqual(filled["balances"]["btc"], "0.55")
        self.assertEqual(Decimal(filled["balances"]["reserved_usdt"]), 0)
        self.assertEqual(Decimal(filled["portfolio"]["equity_usdt"]), Decimal("68447.5332"))
        self.assertEqual(Decimal(filled["portfolio"]["hold_equity_usdt"]), Decimal("68450"))
        self.assertEqual(filled["portfolio"]["inventory_delta_btc"], "0.05")
        self.assertEqual(filled["portfolio"]["filled_volume_btc"], "0.05")

        final = filled
        while final["status"] != "finished":
            final = self.action("step")
        expected = run_replay(
            load_binance_replay(self.panel_recording, load_fixture(self.orders)),
            Wallet(btc=DEFAULT_STARTING_BTC, usdt=DEFAULT_STARTING_USDT),
        )
        for field in ("fees_usdt", "net_pnl_usdt", "excess_pnl_usdt"):
            self.assertEqual(Decimal(final["portfolio"][field]), getattr(expected, field))

    def test_equity_history_catches_up_after_disconnect_and_marks_feed_gap(self):
        payload = {"recording": self.panel_recording.name, "orders": self.orders.name}
        self.action("load", payload)
        self.action("step")
        self.action("step")
        with self.stream() as response:
            cursor, opening = self.next_update(response)
        [first_point] = opening["equity_points"]
        self.assertEqual(first_point["timestamp_ms"], 0)

        while True:
            state = self.action("step")
            if state["timestamp_ms"] == 26000:
                self.assertIsNone(state["book"])
                self.assertIsNone(state["portfolio"]["equity_usdt"])
                self.assertIsNone(state["portfolio"]["hold_equity_usdt"])
            if state["status"] == "finished":
                break
        with self.stream(cursor) as response:
            final_cursor, caught_up = self.next_update(response)
        points = caught_up["equity_points"]
        self.assertEqual([point["timestamp_ms"] for point in points], [20000, 25000, 26000, 28000])
        self.assertIsNone(points[2]["equity_usdt"])
        self.assertIsNone(points[2]["hold_equity_usdt"])
        self.assertEqual(points[-1]["equity_usdt"], state["portfolio"]["equity_usdt"])
        self.assertEqual(points[-1]["hold_equity_usdt"], state["portfolio"]["hold_equity_usdt"])
        self.assertEqual(state["equity_point_count"], 5)

        self.action("load", payload)
        with self.stream(final_cursor) as response:
            _, reset = self.next_update(response)
        self.assertEqual(reset["equity_points"], [])
        self.assertEqual(reset["state"]["equity_point_count"], 0)
