from decimal import Decimal

from .models import Side, SubmitOrder
from .wallet import WalletBalances


INVENTORY_CAP_FRACTION = Decimal("0.20")


class InventoryLimitError(ValueError):
    """The proposed order would exceed the worst-case inventory cap."""


class InventoryLimit:
    """Value projected BTC deviation at the current mark against a fixed opening cap."""

    def __init__(self, starting_balances: WalletBalances, *, target_btc: Decimal | None = None) -> None:
        self._target_btc = starting_balances.btc if target_btc is None else target_btc
        if (
            not isinstance(self._target_btc, Decimal)
            or not self._target_btc.is_finite()
            or self._target_btc < 0
        ):
            raise ValueError("target_btc must be a finite, non-negative Decimal")

        self._starting_balances = starting_balances
        self._mark_price: Decimal | None = None
        self._cap_usdt: Decimal | None = None

    def update_mark(self, price: Decimal) -> None:
        if self._cap_usdt is None:
            self._cap_usdt = self._opening_equity(price) * INVENTORY_CAP_FRACTION
        self._mark_price = price

    def check_submission(
        self, order: SubmitOrder, *, held_btc: Decimal, outstanding_quantity: Decimal
    ) -> Decimal:
        """Check the projection and return the mark used for the calculation."""
        # Synthetic books can start without a midpoint. The first accepted quote
        # provides an opening mark in that case.
        mark_price = order.limit_price if self._mark_price is None else self._mark_price
        cap_usdt = self._cap_usdt
        if cap_usdt is None:
            cap_usdt = self._opening_equity(mark_price) * INVENTORY_CAP_FRACTION

        quantity = outstanding_quantity + order.quantity
        projected_btc = held_btc + quantity if order.side is Side.BUY else held_btc - quantity
        deviation_usdt = abs(projected_btc - self._target_btc) * mark_price
        if deviation_usdt > cap_usdt:
            raise InventoryLimitError(
                f"Order {order.order_id!r} breaches the 20% inventory cap: "
                f"projected deviation {deviation_usdt:f} USDT exceeds {cap_usdt:f} USDT"
            )
        return mark_price

    def _opening_equity(self, mark_price: Decimal) -> Decimal:
        return self._starting_balances.usdt + self._starting_balances.btc * mark_price
