"""Quote decisions for the paper replay engine."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Protocol

from .models import BookSnapshot, Order, Side
from .wallet import WalletBalances


DEFAULT_SPREAD_USDT = Decimal("100")
DEFAULT_QUOTE_SIZE_BTC = Decimal("0.001")
DEFAULT_RISK_AVERSION = Decimal("0.01")
AS_HORIZON_SECONDS = 180
AS_MODEL_LOT_BTC = Decimal("0.05")
IMBALANCE_DEPTH_LEVELS = 5
MAX_IMBALANCE_TICKS = 2
StrategyName = Literal["fixed-spread", "avellaneda-stoikov", "avellaneda-stoikov-imbalance"]


@dataclass(frozen=True, slots=True)
class QuoteContext:
    """Market inputs at a decision tick, with each book side in best-price order."""

    book: BookSnapshot
    mid_price: Decimal
    balances: WalletBalances
    outstanding_orders: tuple[Order, ...]
    target_btc: Decimal | None = None
    midpoint_return_variance: Decimal | None = None
    tick_size: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class Quote:
    side: Side
    limit_price: Decimal
    quantity: Decimal


class Strategy(Protocol):
    def quote(self, context: QuoteContext) -> tuple[Quote, ...]:
        """Return up to one desired quote per side for the current book."""
        ...


@dataclass(frozen=True, slots=True)
class FixedSpreadStrategy:
    spread_usdt: Decimal = DEFAULT_SPREAD_USDT
    quote_size_btc: Decimal = DEFAULT_QUOTE_SIZE_BTC

    def __post_init__(self) -> None:
        for field, value in (("spread_usdt", self.spread_usdt), ("quote_size_btc", self.quote_size_btc)):
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{field} must be a positive, finite Decimal")

    def quote(self, context: QuoteContext) -> tuple[Quote, ...]:
        return self._quotes_around(context.mid_price)

    def _quotes_around(self, center: Decimal) -> tuple[Quote, ...]:
        half_spread = self.spread_usdt / 2
        bid = center - half_spread
        if bid <= 0:
            return ()
        return (
            Quote(Side.BUY, bid, self.quote_size_btc),
            Quote(Side.SELL, center + half_spread, self.quote_size_btc),
        )


@dataclass(frozen=True, slots=True)
class AvellanedaStoikovStrategy(FixedSpreadStrategy):
    """Use the reservation-price adjustment with the shared fixed spread."""

    risk_aversion: Decimal = DEFAULT_RISK_AVERSION

    def __post_init__(self) -> None:
        FixedSpreadStrategy.__post_init__(self)
        gamma = self.risk_aversion
        if not isinstance(gamma, Decimal) or not gamma.is_finite() or gamma <= 0:
            raise ValueError("risk_aversion must be a positive, finite Decimal")

    def quote(self, context: QuoteContext) -> tuple[Quote, ...]:
        center = self._quote_center(context)
        return () if center is None else self._quotes_around(center)

    def _quote_center(self, context: QuoteContext) -> Decimal | None:
        if context.target_btc is None:
            raise ValueError("Avellaneda–Stoikov requires a BTC inventory target")
        inventory_lots = (context.balances.btc - context.target_btc) / AS_MODEL_LOT_BTC
        variance = context.midpoint_return_variance
        if not inventory_lots:
            return context.mid_price
        if variance is None:
            return None
        # Convert relative one-second variance to squared USDT per model lot per second.
        lot_mid = context.mid_price * AS_MODEL_LOT_BTC
        lot_price_variance = variance * lot_mid ** 2
        lot_shift = inventory_lots * self.risk_aversion * lot_price_variance * AS_HORIZON_SECONDS
        return context.mid_price - lot_shift / AS_MODEL_LOT_BTC


@dataclass(frozen=True, slots=True)
class AvellanedaStoikovImbalanceStrategy(AvellanedaStoikovStrategy):
    """Shift the reservation price toward the heavier side of the top five levels."""

    def _quote_center(self, context: QuoteContext) -> Decimal | None:
        center = AvellanedaStoikovStrategy._quote_center(self, context)
        if center is None or not context.tick_size:
            return center
        bid_depth = sum(level.quantity for level in context.book.bids[:IMBALANCE_DEPTH_LEVELS])
        ask_depth = sum(level.quantity for level in context.book.asks[:IMBALANCE_DEPTH_LEVELS])
        total_depth = bid_depth + ask_depth
        if not total_depth:
            return center
        imbalance = (bid_depth - ask_depth) / total_depth
        return center + imbalance * MAX_IMBALANCE_TICKS * context.tick_size


STRATEGY_TYPES = {
    "fixed-spread": FixedSpreadStrategy,
    "avellaneda-stoikov": AvellanedaStoikovStrategy,
    "avellaneda-stoikov-imbalance": AvellanedaStoikovImbalanceStrategy,
}


def create_strategy(
    name: StrategyName,
    spread_usdt: Decimal = DEFAULT_SPREAD_USDT,
    quote_size_btc: Decimal = DEFAULT_QUOTE_SIZE_BTC,
    *,
    risk_aversion: Decimal | None = None,
) -> FixedSpreadStrategy:
    strategy_type = STRATEGY_TYPES[name]
    if issubclass(strategy_type, AvellanedaStoikovStrategy):
        gamma = DEFAULT_RISK_AVERSION if risk_aversion is None else risk_aversion
        return strategy_type(spread_usdt, quote_size_btc, gamma)
    if risk_aversion is not None:
        raise ValueError("--risk-aversion requires an Avellaneda–Stoikov strategy")
    return strategy_type(spread_usdt, quote_size_btc)
