"""
Edge-case checks to run against BOTH codebases (AI 1 version).

Step 1: edit ONLY the ADAPTER section below for the codebase you are testing.
Step 2: run:  python -m unittest test_compare_edge_cases -v
Compare how many tests pass in each codebase.

Some tests encode a judgment call (marked JUDGMENT). Stricter input handling is
treated as better, but read the failure message before penalising either side.
"""
import json
import random
import unittest
from decimal import Decimal

# =====================================================================
# ADAPTER for AI 1 (market_maker) -- already wired, nothing to edit
# =====================================================================
import os
import tempfile
from market_maker.replay import load_fixture, order_events


def build_trade(ts, seq, price, qty, trade_id):
    return {
        "type": "trade",
        "timestamp_ms": ts,
        "sequence": seq,
        "trade_id": trade_id,
        "price": price,
        "quantity": qty,
        "buyer_is_maker": False,
    }


def parse(raw_events):
    # AI 1 loads from a file path, so write the events to a temp JSON file.
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "fixture.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_events, f)
        return load_fixture(path)


def order(events):
    return order_events(events)


def trade_id_of(event):
    return event.trade_id


def price_of(event):
    return event.price
# =====================================================================


class EdgeCaseChecks(unittest.TestCase):

    def test_full_tie_keeps_input_order(self):
        """Same timestamp AND sequence: input order must be preserved."""
        raw = [build_trade(1000, 1, "100.0", "1", tid) for tid in (11, 22, 33)]
        out = order(parse(raw))
        self.assertEqual([trade_id_of(e) for e in out], [11, 22, 33])

    def test_full_tie_stable_after_reverse_input(self):
        raw = [build_trade(1000, 1, "100.0", "1", tid) for tid in (33, 22, 11)]
        out = order(parse(raw))
        self.assertEqual([trade_id_of(e) for e in out], [33, 22, 11])

    def test_timestamp_beats_sequence(self):
        """Earlier timestamp wins even with a larger sequence number."""
        raw = [build_trade(2000, 1, "1", "1", 1), build_trade(1000, 9, "1", "1", 2)]
        out = order(parse(raw))
        self.assertEqual([trade_id_of(e) for e in out], [2, 1])

    def test_shuffle_gives_same_order_many_times(self):
        base = [build_trade(1000 + (i // 3), i % 3, "1", "1", i) for i in range(30)]
        expected = [trade_id_of(e) for e in order(parse(base))]
        rng = random.Random(0)
        for _ in range(50):
            shuffled = base[:]
            rng.shuffle(shuffled)
            got = [trade_id_of(e) for e in order(parse(shuffled))]
            self.assertEqual(got, expected)

    def test_price_is_decimal_and_exact(self):
        e = parse([build_trade(1, 1, "0.10000000000000000001", "1", 1)])[0]
        self.assertIsInstance(price_of(e), Decimal)
        self.assertEqual(price_of(e), Decimal("0.10000000000000000001"))

    def test_float_price_rejected(self):
        """JUDGMENT: a JSON float loses precision, so it should be rejected."""
        with self.assertRaises(Exception):
            parse([build_trade(1, 1, 100.1, "1", 1)])

    def test_float_quantity_rejected(self):
        """JUDGMENT: same reasoning as price."""
        with self.assertRaises(Exception):
            parse([build_trade(1, 1, "100.1", 0.1, 1)])

    def test_negative_quantity_rejected(self):
        """JUDGMENT: a trade with negative quantity is invalid."""
        with self.assertRaises(Exception):
            parse([build_trade(1, 1, "100", "-1", 1)])

    def test_negative_price_rejected(self):
        with self.assertRaises(Exception):
            parse([build_trade(1, 1, "-100", "1", 1)])

    def test_missing_field_raises(self):
        bad = build_trade(1, 1, "100", "1", 1)
        del bad["price"]
        with self.assertRaises(Exception):
            parse([bad])

    def test_unknown_event_type_raises(self):
        bad = build_trade(1, 1, "100", "1", 1)
        bad["type"] = "banana"
        with self.assertRaises(Exception):
            parse([bad])

    def test_garbage_price_string_raises(self):
        with self.assertRaises(Exception):
            parse([build_trade(1, 1, "abc", "1", 1)])

    def test_nan_and_infinity_rejected(self):
        """JUDGMENT: Decimal('NaN') parses fine, but is never a valid price."""
        for bad in ("NaN", "Infinity"):
            with self.subTest(bad=bad):
                with self.assertRaises(Exception):
                    parse([build_trade(1, 1, bad, "1", 1)])

    def test_non_integer_timestamp_rejected(self):
        """JUDGMENT: timestamps are integer milliseconds."""
        with self.assertRaises(Exception):
            parse([build_trade(1000.5, 1, "100", "1", 1)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
