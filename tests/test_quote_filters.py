import json
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from market_maker.models import BookLevel, BookSnapshot, Side, Trade
from market_maker.quote_filters import DEFAULT_FILTERS, QuoteFilterError, load_binance_quote_filters
from market_maker.replay import run_replay
from market_maker.strategy import FixedSpreadStrategy, Quote
from market_maker.wallet import Wallet


D = Decimal


def book(timestamp_ms, sequence, bid, ask):
    return BookSnapshot(timestamp_ms, sequence, (BookLevel(D(bid), D("0.02")),), (BookLevel(D(ask), D("0.03")),))


class QuoteFilterTests(unittest.TestCase):
    def setUp(self):
        self.filters = load_binance_quote_filters()

    def test_rounds_prices_outward_and_quantity_down(self):
        bid = Quote(Side.BUY, D("61124.045"), D("0.001009"))
        ask = Quote(Side.SELL, D("61224.045"), D("0.001009"))
        self.assertEqual(self.filters.round_quote(bid), Quote(Side.BUY, D("61124.04"), D("0.001")))
        self.assertEqual(self.filters.round_quote(ask), Quote(Side.SELL, D("61224.05"), D("0.001")))
        with self.assertRaisesRegex(QuoteFilterError, "off tick"):
            self.filters.check(bid)
        with self.assertRaisesRegex(QuoteFilterError, "off step"):
            self.filters.check(replace(bid, limit_price=D("61124.04")))

    def test_checks_price_quantity_and_notional_after_rounding(self):
        self.filters.check(Quote(Side.BUY, D("500000"), D("0.00001")))  # Exactly 5 USDT.
        invalid = [
            (Quote(Side.BUY, D("0.009"), D("1")), "PRICE_FILTER"),
            (Quote(Side.SELL, D("1000000.001"), D("1")), "PRICE_FILTER"),
            (Quote(Side.BUY, D("68448"), D("0.000009")), "LOT_SIZE"),
            (Quote(Side.SELL, D("1"), D("9000.00002")), "LOT_SIZE"),
            (Quote(Side.BUY, D("499.999"), D("0.010001")), "below minimum"),
            (Quote(Side.SELL, D("1000000"), D("9.00001")), "exceeds maximum"),
        ]
        for quote, error in invalid:
            with self.subTest(quote=quote), self.assertRaisesRegex(QuoteFilterError, error):
                self.filters.check(self.filters.round_quote(quote))

    def test_supports_exchange_info_with_min_notional_only(self):
        data = json.loads(DEFAULT_FILTERS.read_text(encoding="utf-8"))
        data["symbols"][0]["filters"][-1] = {"filterType": "MIN_NOTIONAL", "minNotional": "6.5"}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "exchange_info.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            filters = load_binance_quote_filters(path)
        self.assertEqual(filters.min_notional, D("6.5"))
        self.assertIsNone(filters.max_notional)


class StrategyQuoteValidationTests(unittest.TestCase):
    def test_fee_guard_uses_rounded_pair_and_configured_maker_rate(self):
        events = [book(0, 1, "99.50", "100.50")]
        strategy = FixedSpreadStrategy(D("0.998"), D("0.100009"))
        covers_fees = run_replay(
            events, Wallet(btc=D("0.50"), usdt=D("100"), maker_fee_rate=D("0.005")), strategy=strategy,
        )
        submits = [dict(entry.details) for entry in covers_fees.audit_log if entry.kind == "ORDER_SUBMITTED"]
        self.assertEqual([(entry["price"], entry["quantity"]) for entry in submits],
                         [(D("99.50"), D("0.1")), (D("100.50"), D("0.1"))])

        too_narrow = run_replay(
            events, Wallet(btc=D("0.50"), usdt=D("100"), maker_fee_rate=D("0.0051")), strategy=strategy,
        )
        skipped = [dict(entry.details) for entry in too_narrow.audit_log if entry.kind == "ORDER_SKIPPED"]
        self.assertEqual([entry["price"] for entry in skipped], [D("99.50"), D("100.50")])
        self.assertTrue(all("below round-trip fees" in entry["reason"] for entry in skipped))
        self.assertFalse(any(entry.kind == "ORDER_SUBMITTED" for entry in too_narrow.audit_log))
        self.assertEqual((too_narrow.balances.reserved_usdt, too_narrow.balances.reserved_btc), (0, 0))

    def test_filter_failure_is_audited_before_any_order_is_reserved(self):
        result = run_replay(
            [book(0, 1, "99.50", "100.50")], Wallet(btc=D("0.50"), usdt=D("100")),
            strategy=FixedSpreadStrategy(D("1"), D("0.001")), fee_guard=False,
        )
        skips = [dict(entry.details)["reason"] for entry in result.audit_log if entry.kind == "ORDER_SKIPPED"]
        self.assertEqual(len(skips), 2)
        self.assertTrue(all("below minimum" in reason for reason in skips))
        self.assertEqual((result.balances.reserved_usdt, result.balances.reserved_btc), (0, 0))

    def test_fee_guard_checks_against_the_price_of_the_resting_opposite_order(self):
        result = run_replay(
            [
                book(0, 1, "99.50", "100.50"),
                Trade(15, 2, 1, D("99"), D("0.1"), True),
                book(20, 3, "100.90", "102.10"),
            ],
            Wallet(btc=D("0.50"), usdt=D("100"), maker_fee_rate=D("0.005")),
            strategy=FixedSpreadStrategy(D("2"), D("0.1")),
        )
        [skipped] = [dict(entry.details) for entry in result.audit_log if entry.kind == "ORDER_SKIPPED"]
        self.assertEqual(skipped["side"], "buy")
        self.assertEqual(skipped["price"], D("100.50"))
        self.assertIn("below round-trip fees", skipped["reason"])
        self.assertEqual(result.balances.reserved_usdt, 0)
        self.assertEqual(result.balances.reserved_btc, D("0.1"))

    def test_post_only_rechecks_the_book_at_activation_and_releases_reserve(self):
        for side, bid, ask, price, reserve in (
            (Side.BUY, "99", "99.50", "99.50", "reserved_usdt"),
            (Side.SELL, "100.50", "101", "100.50", "reserved_btc"),
        ):
            with self.subTest(side=side):
                events = [
                    book(0, 1, "99.50", "100.50"),
                    book(14, 2, bid, ask),
                    Trade(15, 3, 1, D(price), D("0.5"), side is Side.BUY),
                ]
                result = run_replay(
                    events, Wallet(btc=D("0.50"), usdt=D("100")),
                    strategy=FixedSpreadStrategy(D("1"), D("0.1")), fee_guard=False,
                )
                [rejected] = [entry for entry in result.audit_log if entry.kind == "ORDER_REJECTED"]
                self.assertEqual(rejected.timestamp_ms, 15)
                self.assertIn(f"Post-only {side.value} would match", dict(rejected.details)["reason"])
                self.assertEqual(getattr(result.balances, reserve), 0)
                self.assertEqual(dict(rejected.details)[reserve], 0)
                self.assertEqual(sum(entry.kind == "ORDER_ACTIVATED" for entry in result.audit_log), 1)
                self.assertFalse(any(entry.kind == "FILL" for entry in result.audit_log))
