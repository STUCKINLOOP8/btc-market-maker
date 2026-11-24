import unittest
from decimal import Decimal

from market_maker.models import Side, SubmitOrder
from market_maker.wallet import InsufficientFundsError, Wallet, WalletBalances


def submit_order(order_id: str, side: Side = Side.BUY) -> SubmitOrder:
    return SubmitOrder(
        timestamp_ms=1000,
        sequence=1,
        order_id=order_id,
        side=side,
        limit_price=Decimal("70000"),
        quantity=Decimal("0.05"),
    )


class WalletTests(unittest.TestCase):
    def test_pending_bid_prevents_double_submission(self):
        wallet = Wallet(btc=Decimal("0"), usdt=Decimal("5000"), maker_fee_rate=Decimal("0"))
        wallet.reserve(submit_order("first"))
        before_rejection = wallet.balances

        with self.assertRaisesRegex(InsufficientFundsError, "Insufficient USDT"):
            wallet.reserve(submit_order("second"))

        self.assertEqual(wallet.balances, before_rejection)
        self.assertEqual(wallet.balances.reserved_usdt, Decimal("3500"))
        self.assertEqual(wallet.balances.available_usdt, Decimal("1500"))
        wallet.release("first")
        wallet.reserve(submit_order("second"))
        self.assertEqual(wallet.balances.available_usdt, Decimal("1500"))

    def test_bid_must_cover_maker_fee(self):
        wallet = Wallet(btc=Decimal("0"), usdt=Decimal("3500"))

        with self.assertRaisesRegex(InsufficientFundsError, "Insufficient USDT"):
            wallet.reserve(submit_order("bid"))

        self.assertEqual(wallet.balances.reserved_usdt, Decimal("0"))

    def test_partial_bid_fill_settles_wallet_and_keeps_unfilled_reservation(self):
        wallet = Wallet(btc=Decimal("0.50"), usdt=Decimal("5000"))
        wallet.reserve(submit_order("bid"))
        self.assertEqual(wallet.balances.reserved_usdt, Decimal("3502.625"))

        fee = wallet.settle_fill("bid", price=Decimal("70000"), quantity=Decimal("0.02"))

        self.assertEqual(fee, Decimal("1.05"))
        self.assertEqual(
            wallet.balances,
            WalletBalances(Decimal("0.52"), Decimal("3598.95"), Decimal("0"), Decimal("2101.575")),
        )
        self.assertEqual(wallet.balances.available_usdt, Decimal("1497.375"))

        wallet.release("bid")
        after_release = wallet.balances
        wallet.release("bid")

        self.assertEqual(wallet.balances, after_release)
        self.assertEqual(wallet.balances.available_usdt, Decimal("3598.95"))
        self.assertEqual(wallet.balances.available_btc, Decimal("0.52"))

    def test_full_bid_fill_uses_actual_price_and_clears_reservation(self):
        wallet = Wallet(btc=Decimal("0"), usdt=Decimal("5000"))
        wallet.reserve(submit_order("bid"))

        fee = wallet.settle_fill("bid", price=Decimal("68000"), quantity=Decimal("0.05"))

        self.assertEqual(fee, Decimal("2.55"))
        self.assertEqual(wallet.balances.available_usdt, Decimal("1597.45"))
        self.assertEqual(wallet.balances.available_btc, Decimal("0.05"))
        with self.assertRaisesRegex(ValueError, "has no reservation"):
            wallet.settle_fill("bid", price=Decimal("68000"), quantity=Decimal("0.05"))

    def test_pending_ask_reserves_btc_and_fills_credit_net_usdt(self):
        wallet = Wallet(btc=Decimal("0.05"), usdt=Decimal("0"))
        wallet.reserve(submit_order("ask", Side.SELL))
        self.assertEqual(wallet.balances.available_btc, Decimal("0"))
        self.assertEqual(wallet.balances.reserved_btc, Decimal("0.05"))
        with self.assertRaisesRegex(InsufficientFundsError, "Insufficient BTC"):
            wallet.reserve(submit_order("second", Side.SELL))

        fee = wallet.settle_fill("ask", price=Decimal("70000"), quantity=Decimal("0.02"))

        self.assertEqual(fee, Decimal("1.05"))
        self.assertEqual(wallet.balances.btc, Decimal("0.03"))
        self.assertEqual(wallet.balances.reserved_btc, Decimal("0.03"))
        self.assertEqual(wallet.balances.usdt, Decimal("1398.95"))

        wallet.settle_fill("ask", price=Decimal("70000"), quantity=Decimal("0.03"))
        self.assertEqual(
            wallet.balances,
            WalletBalances(Decimal("0"), Decimal("3497.375"), Decimal("0"), Decimal("0")),
        )

    def test_rejected_reservation_or_fill_leaves_wallet_intact(self):
        wallet = Wallet(btc=Decimal("0.05"), usdt=Decimal("5000"))
        wallet.reserve(submit_order("bid"))
        wallet.reserve(submit_order("ask", Side.SELL))
        before_rejection = wallet.balances

        with self.assertRaisesRegex(ValueError, "already has a reservation"):
            wallet.reserve(submit_order("bid"))
        cases = (
            ("bid", "70000", "0.06", "exceeds the remaining quantity"),
            ("bid", "70001", "0.05", "exceeds the buy limit"),
            ("ask", "69999", "0.05", "below the sell limit"),
            ("ask", "NaN", "0.05", "price must be a finite Decimal"),
        )
        for order_id, price, quantity, message in cases:
            with self.subTest(order_id=order_id, price=price, quantity=quantity):
                with self.assertRaisesRegex(ValueError, message):
                    wallet.settle_fill(order_id, price=Decimal(price), quantity=Decimal(quantity))
                self.assertEqual(wallet.balances, before_rejection)
