from dataclasses import dataclass
from decimal import Decimal

from .models import Side, SubmitOrder


ZERO = Decimal("0")
DEFAULT_MAKER_FEE_RATE = Decimal("0.00075")


class InsufficientFundsError(ValueError):
    """An order requires more than the available balance in its funding currency."""


@dataclass(frozen=True, slots=True)
class WalletBalances:
    btc: Decimal
    usdt: Decimal
    reserved_btc: Decimal
    reserved_usdt: Decimal

    @property
    def available_btc(self) -> Decimal:
        return self.btc - self.reserved_btc

    @property
    def available_usdt(self) -> Decimal:
        return self.usdt - self.reserved_usdt


@dataclass(slots=True)
class _Reservation:
    order: SubmitOrder
    remaining_quantity: Decimal


class Wallet:
    """BTC/USDT paper wallet that settles maker fees in USDT."""

    def __init__(
        self,
        *,
        btc: Decimal,
        usdt: Decimal,
        maker_fee_rate: Decimal = DEFAULT_MAKER_FEE_RATE,
    ):
        _require_amount(btc, "btc", allow_zero=True)
        _require_amount(usdt, "usdt", allow_zero=True)
        _require_amount(maker_fee_rate, "maker_fee_rate", allow_zero=True)
        if maker_fee_rate > 1:
            raise ValueError("maker_fee_rate must be at most 1")

        self._btc = btc
        self._usdt = usdt
        self._maker_fee_rate = maker_fee_rate
        self._reservations: dict[str, _Reservation] = {}

    @property
    def maker_fee_rate(self) -> Decimal:
        return self._maker_fee_rate

    @property
    def balances(self) -> WalletBalances:
        reserved_btc = ZERO
        reserved_usdt = ZERO
        for reservation in self._reservations.values():
            if reservation.order.side is Side.BUY:
                notional = reservation.order.limit_price * reservation.remaining_quantity
                reserved_usdt += notional + self._maker_fee(notional)
            else:
                reserved_btc += reservation.remaining_quantity
        return WalletBalances(self._btc, self._usdt, reserved_btc, reserved_usdt)

    def reserved_quantity(self, side: Side) -> Decimal:
        """Return unfilled BTC quantity committed by orders on this side."""
        return sum(
            (
                reservation.remaining_quantity
                for reservation in self._reservations.values()
                if reservation.order.side is side
            ),
            ZERO,
        )

    def reserve(self, order: SubmitOrder) -> None:
        """Reserve the full amount before accepting a pending submission."""
        if order.order_id in self._reservations:
            raise ValueError(f"Order {order.order_id!r} already has a reservation")
        validate_order_fields(order)

        balances = self.balances
        if order.side is Side.BUY:
            notional = order.limit_price * order.quantity
            required = notional + self._maker_fee(notional)
            available = balances.available_usdt
            currency = "USDT"
        else:
            required = order.quantity
            available = balances.available_btc
            currency = "BTC"

        if required > available:
            raise InsufficientFundsError(
                f"Insufficient {currency} for order {order.order_id!r}: "
                f"requires {required}, available {available}"
            )
        self._reservations[order.order_id] = _Reservation(order, order.quantity)

    def settle_fill(self, order_id: str, *, price: Decimal, quantity: Decimal) -> Decimal:
        """Settle a fill against its reservation and return the maker fee in USDT."""
        _require_amount(price, "price")
        _require_amount(quantity, "quantity")
        reservation = self._reservations.get(order_id)
        if reservation is None:
            raise ValueError(f"Order {order_id!r} has no reservation")
        if quantity > reservation.remaining_quantity:
            raise ValueError(f"Fill quantity exceeds the remaining quantity for order {order_id!r}")

        remaining_quantity = reservation.remaining_quantity - quantity
        notional = price * quantity
        fee = self._maker_fee(notional)
        if reservation.order.side is Side.BUY:
            if price > reservation.order.limit_price:
                raise ValueError(f"Fill price exceeds the buy limit for order {order_id!r}")
            btc = self._btc + quantity
            usdt = self._usdt - (notional + fee)
        else:
            if price < reservation.order.limit_price:
                raise ValueError(f"Fill price is below the sell limit for order {order_id!r}")
            btc = self._btc - quantity
            usdt = self._usdt + (notional - fee)

        self._btc = btc
        self._usdt = usdt
        if remaining_quantity == ZERO:
            del self._reservations[order_id]
        else:
            reservation.remaining_quantity = remaining_quantity
        return fee

    def release(self, order_id: str) -> None:
        """Release unfilled funds after confirmed cancellation or rejection."""
        self._reservations.pop(order_id, None)

    def _maker_fee(self, notional: Decimal) -> Decimal:
        return notional * self._maker_fee_rate


def validate_order_fields(order: SubmitOrder) -> None:
    if not isinstance(order.side, Side):
        raise ValueError("side must be a Side")
    _require_amount(order.limit_price, "limit_price")
    _require_amount(order.quantity, "quantity")


def _require_amount(value: Decimal, field: str, *, allow_zero: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field} must be a finite Decimal")
    if value < ZERO or (value == ZERO and not allow_zero):
        requirement = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {requirement}")
