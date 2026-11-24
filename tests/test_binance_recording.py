import asyncio
import json
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from websockets.exceptions import ConnectionClosed

from market_maker.binance_book import REST_DEPTH_STREAM, BinanceOrderBook, BookTop
from market_maker.record import DEPTH_STREAM, FEED_STATUS_STREAM, TRADE_STREAM, record_binance
from market_maker.recording import read_recording


TRADE = (
    '{"stream":"btcusdt@trade","data":{"e":"trade","E":1700000000102,"T":1700000000100,'
    '"t":101,"p":"68448.01000000","q":"0.12345678901234567890","m":true}}'
)
DEPTH = (
    '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000101,'
    '"U":11,"u":12,"b":[["68448.01000000","0.15000000"]],"a":[]}}'
)


class BinanceRecordingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "binance.jsonl"

    async def capture(self, frames: list[str]):
        connection = MagicMock()
        websocket = connection.__aenter__.return_value
        messages = iter(frames)

        async def recv(*, decode: bool) -> str:
            self.assertTrue(decode)
            frame = next(messages, None)
            if frame is not None:
                return frame
            await asyncio.Future()
            raise AssertionError("The capture deadline should cancel this read")

        websocket.recv = recv
        with patch("market_maker.record.connect", return_value=connection) as client:
            counts = await record_binance(self.path, duration_seconds=0.1)
        client.assert_called_once_with(
            "wss://data-stream.binance.vision/stream?streams=btcusdt@depth@100ms/btcusdt@trade",
            open_timeout=10,
            close_timeout=2,
        )
        connection.__aexit__.assert_awaited_once()
        return counts

    async def test_records_both_streams_in_receive_order_with_exact_values(self):
        counts = await self.capture([TRADE, DEPTH])
        messages = list(read_recording(self.path))

        self.assertEqual(counts, {TRADE_STREAM: 1, DEPTH_STREAM: 1})
        self.assertEqual([message.stream for message in messages], [TRADE_STREAM, DEPTH_STREAM])
        self.assertEqual([message.sequence for message in messages], [1, 2])
        self.assertEqual([message.exchange_timestamp_ms for message in messages], [1700000000102, 1700000000101])
        self.assertLessEqual(messages[0].received_elapsed_ms, messages[1].received_elapsed_ms)
        self.assertEqual([message.raw_payload for message in messages], [TRADE, DEPTH])
        trade = json.loads(messages[0].raw_payload)["data"]
        self.assertEqual(trade["t"], 101)
        self.assertIs(trade["m"], True)
        self.assertEqual(Decimal(trade["p"]).as_tuple(), Decimal("68448.01000000").as_tuple())
        self.assertEqual(Decimal(trade["q"]).as_tuple(), Decimal("0.12345678901234567890").as_tuple())

    async def test_reports_missing_stream_and_keeps_messages_received(self):
        with self.assertRaisesRegex(ValueError, "Capture ended without messages from: btcusdt@depth@100ms"):
            await self.capture([TRADE])

        self.assertEqual([message.raw_payload for message in read_recording(self.path)], [TRADE])

    async def test_capture_preserves_existing_file(self):
        self.path.write_text("existing recording\n", encoding="utf-8")

        with self.assertRaises(FileExistsError):
            await self.capture([TRADE, DEPTH])

        self.assertEqual(self.path.read_text(encoding="utf-8"), "existing recording\n")

    async def test_synchronized_capture_buffers_depth_while_fetching_snapshot(self):
        book = BinanceOrderBook()
        snapshot_started = asyncio.Event()
        frames_recorded = asyncio.Event()
        snapshot = '{"lastUpdateId":11,"bids":[["68447.01","1"]],"asks":[["68453.01","1"]]}'
        second_depth = '{"stream":"btcusdt@depth@100ms","data":{"U":13,"u":13,"b":[],"a":[["68452.01","1"]]}}'
        frames = iter([TRADE, second_depth])
        connection = MagicMock()
        websocket = connection.__aenter__.return_value
        first_received = False

        async def recv(*, decode: bool) -> str:
            nonlocal first_received
            if not first_received:
                first_received = True
                return DEPTH
            await snapshot_started.wait()
            frame = next(frames, None)
            if frame is not None:
                return frame
            frames_recorded.set()
            await asyncio.Future()
            raise AssertionError("The capture deadline should cancel this read")

        async def fetch_in_thread(_fetcher) -> str:
            snapshot_started.set()
            await frames_recorded.wait()
            return snapshot

        websocket.recv = recv
        with (
            patch("market_maker.record.connect", return_value=connection),
            patch("market_maker.record.asyncio.to_thread", side_effect=fetch_in_thread),
        ):
            counts = await record_binance(self.path, duration_seconds=0.25, order_book=book)

        messages = list(read_recording(self.path))
        self.assertEqual([message.stream for message in messages], [
            FEED_STATUS_STREAM, DEPTH_STREAM, TRADE_STREAM, DEPTH_STREAM, REST_DEPTH_STREAM, FEED_STATUS_STREAM,
        ])
        self.assertEqual([message.sequence for message in messages], list(range(1, 7)))
        self.assertEqual(messages[-2].raw_payload, snapshot)
        self.assertEqual(counts[REST_DEPTH_STREAM], 1)
        self.assertEqual(book.top, BookTop(13, Decimal("68448.01"), Decimal("68452.01"), Decimal("68450.01")))

    async def capture_recovery(self, interruption, *, depth_timeout_seconds: float = 0.5):
        book = BinanceOrderBook()
        first_ready, second_ready = asyncio.Event(), asyncio.Event()
        first_frames = iter([DEPTH, TRADE])
        fresh_depth = (
            '{"stream":"btcusdt@depth@100ms","data":{"U":20,"u":21,'
            '"b":[["68444","1"]],"a":[["68446","1"]]}}'
        )
        second_frames = iter([fresh_depth, TRADE])
        next_update_id = 22
        first_connection, second_connection = MagicMock(), MagicMock()

        async def first_recv(*, decode: bool) -> str:
            frame = next(first_frames, None)
            if frame is not None:
                return frame
            await first_ready.wait()
            return await interruption()

        async def second_recv(*, decode: bool) -> str:
            nonlocal next_update_id
            frame = next(second_frames, None)
            if frame is not None:
                if frame == fresh_depth:
                    self.assertIsNone(book.top)
                return frame
            await second_ready.wait()
            await asyncio.sleep(0.01)
            update_id = next_update_id
            next_update_id += 1
            return json.dumps({"stream": DEPTH_STREAM, "data": {"U": update_id, "u": update_id, "b": [], "a": []}})

        snapshots = iter([
            (first_ready, '{"lastUpdateId":11,"bids":[["68447.01","1"]],"asks":[["68453.01","1"]]}'),
            (second_ready, '{"lastUpdateId":20,"bids":[["68443","1"]],"asks":[["68447","1"]]}'),
        ])

        async def fetch_in_thread(_fetcher) -> str:
            ready, snapshot = next(snapshots)
            ready.set()
            return snapshot

        first_connection.__aenter__.return_value.recv = first_recv
        second_connection.__aenter__.return_value.recv = second_recv
        with (
            patch("market_maker.record.connect", side_effect=[first_connection, second_connection]) as connect,
            patch("market_maker.record.asyncio.to_thread", side_effect=fetch_in_thread),
            patch("market_maker.record.RECONNECT_DELAY_SECONDS", 0),
        ):
            counts = await record_binance(
                self.path,
                duration_seconds=0.3,
                order_book=book,
                depth_timeout_seconds=depth_timeout_seconds,
            )

        self.assertEqual(connect.call_count, 2)
        self.assertEqual(counts[REST_DEPTH_STREAM], 2)
        self.assertEqual((book.top.best_bid, book.top.best_ask, book.top.midpoint), (
            Decimal("68444"), Decimal("68446"), Decimal("68445"),
        ))
        messages = list(read_recording(self.path))
        self.assertEqual([message.sequence for message in messages], list(range(1, len(messages) + 1)))
        statuses = [json.loads(message.raw_payload) for message in messages if message.stream == FEED_STATUS_STREAM]
        self.assertEqual([status["state"] for status in statuses], ["syncing", "ready", "unavailable", "ready"])
        self.assertEqual(statuses[1]["last_update_id"], 12)
        self.assertEqual(statuses[3]["last_update_id"], 21)
        return statuses[2], counts

    async def test_missing_update_reconnects_and_rebuilds_from_fresh_snapshot(self):
        async def gap() -> str:
            return '{"stream":"btcusdt@depth@100ms","data":{"U":14,"u":14,"b":[],"a":[]}}'

        status, _ = await self.capture_recovery(gap)

        self.assertEqual(status["reason"], "depth_gap")
        self.assertIn("expected update 13, received 14", status["detail"])

    async def test_depth_timeout_recovers_even_when_trades_and_duplicates_keep_arriving(self):
        frames_sent = 0

        async def stalled_depth() -> str:
            nonlocal frames_sent
            await asyncio.sleep(0.01)
            frames_sent += 1
            return TRADE if frames_sent % 2 else DEPTH

        status, counts = await self.capture_recovery(stalled_depth, depth_timeout_seconds=0.08)

        self.assertEqual(status["reason"], "depth_timeout")
        self.assertGreater(counts[TRADE_STREAM], 2)

    async def test_disconnect_reconnects_and_rebuilds_from_fresh_snapshot(self):
        async def disconnected() -> str:
            raise ConnectionClosed(None, None)

        status, _ = await self.capture_recovery(disconnected)

        self.assertEqual(status["reason"], "disconnected")
