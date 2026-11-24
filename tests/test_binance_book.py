import json
import unittest
from decimal import Decimal

from market_maker.binance_book import (
    DEPTH_STREAM,
    REST_DEPTH_STREAM,
    BinanceOrderBook,
    BookTop,
    DepthGapError,
)
from market_maker.recording import RecordedMessage


def snapshot(update_id: int) -> RecordedMessage:
    payload = {"lastUpdateId": update_id, "bids": [["100.00", "1"]], "asks": [["102.00", "1"], ["104.00", "2"]]}
    return RecordedMessage(REST_DEPTH_STREAM, 0, 0, json.dumps(payload))


def depth(first_id: int, last_id: int, *, bids=(), asks=()) -> RecordedMessage:
    envelope = {"stream": DEPTH_STREAM, "data": {"U": first_id, "u": last_id, "b": bids, "a": asks}}
    return RecordedMessage(DEPTH_STREAM, 0, 0, json.dumps(envelope))


class BinanceOrderBookTests(unittest.TestCase):
    def setUp(self):
        self.book = BinanceOrderBook()

    def test_snapshot_joins_buffered_updates_and_computes_top(self):
        self.book.apply_depth(depth(95, 99, bids=[["101.75", "7"]]))
        self.book.apply_depth(depth(100, 103, bids=[["101", "2"]], asks=[["102", "0"]]))
        self.book.apply_depth(depth(104, 104, asks=[["103", "1"]]))
        self.assertIsNone(self.book.top)

        self.assertTrue(self.book.apply_snapshot(snapshot(100)))

        self.assertEqual(self.book.top, BookTop(104, Decimal("101"), Decimal("103"), Decimal("102")))

    def test_stale_snapshot_can_be_retried_with_buffer_intact(self):
        self.book.apply_depth(depth(110, 112, bids=[["100.50", "1"]]))

        self.assertFalse(self.book.apply_snapshot(snapshot(100)))
        self.assertIsNone(self.book.top)
        self.assertTrue(self.book.apply_snapshot(snapshot(110)))

        self.assertEqual(self.book.top, BookTop(112, Decimal("100.50"), Decimal("102"), Decimal("101.25")))

    def test_active_book_ignores_duplicate_and_applies_next_update(self):
        self.book.apply_depth(depth(100, 103, bids=[["101", "2"]]))
        self.assertTrue(self.book.apply_snapshot(snapshot(100)))

        self.book.apply_depth(depth(100, 103, bids=[["101.75", "7"]]))
        self.assertEqual(self.book.top.best_bid, Decimal("101"))
        self.book.apply_depth(depth(104, 104, bids=[["101", "0"]], asks=[["101.50", "1"]]))

        self.assertEqual(self.book.top, BookTop(104, Decimal("100"), Decimal("101.50"), Decimal("100.75")))

    def test_gap_during_bootstrap_requires_newer_snapshot(self):
        self.book.apply_depth(depth(100, 102, bids=[["101", "2"]]))
        self.book.apply_depth(depth(105, 106, asks=[["101.50", "1"]]))

        self.assertFalse(self.book.apply_snapshot(snapshot(100)))
        self.assertIsNone(self.book.top)
        self.assertTrue(self.book.apply_snapshot(snapshot(104)))

        self.assertEqual(self.book.top, BookTop(106, Decimal("100"), Decimal("101.50"), Decimal("100.75")))

    def test_gap_invalidates_the_synchronized_book(self):
        self.book.apply_depth(depth(100, 103))
        self.assertTrue(self.book.apply_snapshot(snapshot(100)))

        with self.assertRaisesRegex(DepthGapError, "expected update 104, received 105"):
            self.book.apply_depth(depth(105, 106, bids=[["101", "1"]]))

        self.assertIsNone(self.book.top)
