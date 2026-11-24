"""Shared Step 2 wallet scenarios, using the Codex project's public API."""

import unittest
from decimal import Decimal

from market_maker.models import Side, SubmitOrder
from market_maker.wallet import InsufficientFundsError, Wallet


D = Decimal


def order(order_id="A", side=Side.BUY, price="100", quantity="1"):
    return SubmitOrder(
        timestamp_ms=0, sequence=0, order_id=order_id, side=side,
        limit_price=D(price), quantity=D(quantity),
    )


class WalletChecks(unittest.TestCase):
    def check_balances(self, wallet, *, btc, usdt, reserved_btc, reserved_usdt):
        b = wallet.balances
        self.assertEqual(b.btc, D(btc))
        self.assertEqual(b.usdt, D(usdt))
        self.assertEqual(b.reserved_btc, D(reserved_btc))
        self.assertEqual(b.reserved_usdt, D(reserved_usdt))
        self.assertEqual(b.available_btc, D(btc) - D(reserved_btc))
        self.assertEqual(b.available_usdt, D(usdt) - D(reserved_usdt))

    def test_two_pending_bids_compete_for_funds(self):
        w = Wallet(btc=D("0"), usdt=D("5000"))
        w.reserve(order(price="70000", quantity="0.05"))
        with self.assertRaises(InsufficientFundsError):
            w.reserve(order("B", price="70000", quantity="0.05"))
        self.check_balances(w, btc="0", usdt="5000", reserved_btc="0", reserved_usdt="3502.625")

    def test_exact_cash_boundary_and_just_below(self):
        for cash, affordable in [("100.075", True), ("100.07499999", False)]:
            with self.subTest(cash=cash):
                w = Wallet(btc=D("0"), usdt=D(cash))
                if affordable:
                    w.reserve(order())
                    self.assertEqual(w.balances.available_usdt, D("0"))
                else:
                    with self.assertRaises(InsufficientFundsError):
                        w.reserve(order())
                    self.assertEqual(w.balances.available_usdt, D(cash))

    def test_partial_buy_price_improvement_then_cancel(self):
        w = Wallet(btc=D("0"), usdt=D("10000"))
        w.reserve(order(price="1000"))
        fee = w.settle_fill("A", price=D("990"), quantity=D("0.4"))
        self.assertEqual(fee, D("0.297"))
        self.check_balances(w, btc="0.4", usdt="9603.703", reserved_btc="0", reserved_usdt="600.45")
        w.release("A")
        self.check_balances(w, btc="0.4", usdt="9603.703", reserved_btc="0", reserved_usdt="0")

    def test_partial_sell_price_improvement_then_cancel(self):
        w = Wallet(btc=D("1"), usdt=D("0"))
        w.reserve(order(side=Side.SELL))
        self.check_balances(w, btc="1", usdt="0", reserved_btc="1", reserved_usdt="0")
        fee = w.settle_fill("A", price=D("110"), quantity=D("0.4"))
        self.assertEqual(fee, D("0.033"))
        self.check_balances(w, btc="0.6", usdt="43.967", reserved_btc="0.6", reserved_usdt="0")
        w.release("A")
        self.check_balances(w, btc="0.6", usdt="43.967", reserved_btc="0", reserved_usdt="0")

    def test_pending_asks_cannot_double_reserve_btc(self):
        w = Wallet(btc=D("1"), usdt=D("5000"))
        w.reserve(order(side=Side.SELL, quantity="0.75"))
        with self.assertRaises(InsufficientFundsError):
            w.reserve(order("B", Side.SELL, quantity="0.25000001"))
        w.reserve(order("C", Side.SELL, quantity="0.25"))
        self.assertEqual(w.balances.available_btc, D("0"))

    def test_duplicate_open_id_leaves_original_reservation_intact(self):
        w = Wallet(btc=D("1"), usdt=D("5000"))
        w.reserve(order())
        with self.assertRaises(ValueError):
            w.reserve(order(side=Side.SELL))
        self.check_balances(w, btc="1", usdt="5000", reserved_btc="0", reserved_usdt="100.075")

    def test_overfill_uses_remaining_quantity_and_is_atomic(self):
        w = Wallet(btc=D("0"), usdt=D("1000"))
        w.reserve(order())
        w.settle_fill("A", price=D("100"), quantity=D("0.4"))
        before = w.balances
        with self.assertRaises(ValueError):
            w.settle_fill("A", price=D("100"), quantity=D("0.7"))
        self.assertEqual(w.balances, before)
        w.settle_fill("A", price=D("100"), quantity=D("0.6"))
        self.check_balances(w, btc="1", usdt="899.925", reserved_btc="0", reserved_usdt="0")

    def test_fill_outside_limit_is_atomic_on_both_sides(self):
        for side, price in [(Side.BUY, "100.01"), (Side.SELL, "99.99")]:
            with self.subTest(side=side):
                w = Wallet(btc=D("1"), usdt=D("1000"))
                w.reserve(order(side=side))
                before = w.balances
                with self.assertRaises(ValueError):
                    w.settle_fill("A", price=D(price), quantity=D("1"))
                self.assertEqual(w.balances, before)

    def test_fills_on_unknown_filled_and_released_orders_are_rejected(self):
        w = Wallet(btc=D("0"), usdt=D("1000"))
        w.reserve(order("filled"))
        w.settle_fill("filled", price=D("100"), quantity=D("1"))
        w.reserve(order("released"))
        w.release("released")
        for key in ("missing", "filled", "released"):
            with self.subTest(order_id=key):
                before = w.balances
                with self.assertRaises(ValueError):
                    w.settle_fill(key, price=D("100"), quantity=D("1"))
                self.assertEqual(w.balances, before)

    def test_round_trip_costs_two_maker_fees(self):
        w = Wallet(btc=D("0"), usdt=D("200"))
        w.reserve(order())
        w.settle_fill("A", price=D("100"), quantity=D("1"))
        w.reserve(order("B", Side.SELL))
        w.settle_fill("B", price=D("100"), quantity=D("1"))
        self.check_balances(w, btc="0", usdt="199.85", reserved_btc="0", reserved_usdt="0")

    def test_configured_fee_applies_to_reservation_and_fill(self):
        for rate, reserve, fee in [("0", "100", "0"), ("0.002", "100.2", "0.2")]:
            with self.subTest(rate=rate):
                w = Wallet(btc=D("0"), usdt=D("200"), maker_fee_rate=D(rate))
                w.reserve(order())
                self.assertEqual(w.balances.reserved_usdt, D(reserve))
                self.assertEqual(w.settle_fill("A", price=D("100"), quantity=D("1")), D(fee))
                self.assertEqual(w.balances.usdt, D("200") - D(reserve))

    def test_invalid_fill_amounts_leave_balances_unchanged(self):
        w = Wallet(btc=D("0"), usdt=D("200"))
        w.reserve(order())
        before = w.balances
        for bad in [D("0"), D("-1"), D("NaN"), D("Infinity"), 1.5]:
            for field in ("price", "quantity"):
                with self.subTest(field=field, value=bad):
                    args = {"price": D("100"), "quantity": D("1")}
                    args[field] = bad
                    with self.assertRaises(ValueError):
                        w.settle_fill("A", **args)
                    self.assertEqual(w.balances, before)

    def test_invalid_order_amounts_leave_balances_unchanged(self):
        w = Wallet(btc=D("1"), usdt=D("1000"))
        before = w.balances
        for field in ("price", "quantity"):
            for bad in ("0", "-1", "NaN", "Infinity"):
                with self.subTest(field=field, value=bad):
                    with self.assertRaises(ValueError):
                        w.reserve(order(**{field: bad}))
                    self.assertEqual(w.balances, before)

    def test_releasing_one_order_preserves_other_reservations(self):
        w = Wallet(btc=D("1"), usdt=D("1000"))
        w.reserve(order())
        w.reserve(order("B", Side.SELL, quantity="0.5"))
        w.release("A")
        w.release("A")
        self.check_balances(w, btc="1", usdt="1000", reserved_btc="0.5", reserved_usdt="0")


if __name__ == "__main__":
    unittest.main()
