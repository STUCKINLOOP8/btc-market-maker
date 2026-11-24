"""Offline Binance Spot price, quantity, and notional rules for BTCUSDT quotes."""

import json
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .models import Side
from .parsing import read_decimal
from .strategy import Quote


DEFAULT_FILTERS = Path(__file__).resolve().parents[1] / "fixtures" / "binance_btcusdt_filters.json"


class QuoteFilterError(ValueError):
    """A proposed quote falls outside a modeled Binance filter after rounding."""


@dataclass(frozen=True, slots=True)
class BinanceQuoteFilters:
    tick_size: Decimal
    min_price: Decimal
    max_price: Decimal
    step_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal
    max_notional: Decimal | None = None

    def round_quote(self, quote: Quote) -> Quote:
        price = _round_increment(quote.limit_price, self.tick_size, round_up=quote.side is Side.SELL)
        quantity = _round_increment(quote.quantity, self.step_size)
        return replace(quote, limit_price=price, quantity=quantity)

    def check(self, quote: Quote) -> None:
        price, quantity = quote.limit_price, quote.quantity
        if not _positive_decimal(price) or price < self.min_price or (self.max_price and price > self.max_price):
            raise QuoteFilterError("PRICE_FILTER: rounded price is outside the allowed range")
        if self.tick_size and price % self.tick_size:
            raise QuoteFilterError("PRICE_FILTER: price is off tick")
        if not _positive_decimal(quantity) or not self.min_quantity <= quantity <= self.max_quantity:
            raise QuoteFilterError("LOT_SIZE: rounded quantity is outside the allowed range")
        if quantity % self.step_size:
            raise QuoteFilterError("LOT_SIZE: quantity is off step")
        notional = price * quantity
        if notional < self.min_notional:
            raise QuoteFilterError(f"Notional {notional:f} is below minimum {self.min_notional:f} USDT")
        if self.max_notional is not None and notional > self.max_notional:
            raise QuoteFilterError(f"Notional {notional:f} exceeds maximum {self.max_notional:f} USDT")


def _round_increment(value: Decimal, increment: Decimal, *, round_up: bool = False) -> Decimal:
    if not _positive_decimal(value):
        raise QuoteFilterError("Quote price and quantity must be positive and finite")
    if increment.is_zero():
        return value
    try:
        remainder = value % increment
    except InvalidOperation as error:
        raise QuoteFilterError("Quote magnitude is too large for the configured increment") from error
    rounded = value - remainder
    return rounded + increment if round_up and remainder else rounded


def _positive_decimal(value: Decimal) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def load_binance_quote_filters(path: str | Path = DEFAULT_FILTERS) -> BinanceQuoteFilters:
    """Read the modeled rules from a saved Binance exchangeInfo response."""
    with Path(path).open(encoding="utf-8") as file:
        return parse_binance_quote_filters(json.load(file))


def parse_binance_quote_filters(data: dict) -> BinanceQuoteFilters:
    """Read the price, size and notional rules from exchangeInfo."""
    try:
        symbol = next(item for item in data["symbols"] if item["symbol"] == "BTCUSDT")
        filters = {item["filterType"]: item for item in symbol["filters"]}

        def decimal(filter_type: str, field: str, *, allow_zero: bool = False) -> Decimal:
            return read_decimal(filters[filter_type][field], f"{filter_type}.{field}", allow_zero=allow_zero)

        min_notionals = [
            decimal(name, "minNotional", allow_zero=True)
            for name in ("MIN_NOTIONAL", "NOTIONAL")
            if name in filters
        ]
        if not min_notionals:
            raise ValueError("Expected a MIN_NOTIONAL or NOTIONAL filter")
        rules = BinanceQuoteFilters(
            tick_size=decimal("PRICE_FILTER", "tickSize", allow_zero=True),
            min_price=decimal("PRICE_FILTER", "minPrice", allow_zero=True),
            max_price=decimal("PRICE_FILTER", "maxPrice", allow_zero=True),
            step_size=decimal("LOT_SIZE", "stepSize"),
            min_quantity=decimal("LOT_SIZE", "minQty", allow_zero=True),
            max_quantity=decimal("LOT_SIZE", "maxQty"),
            min_notional=max(min_notionals),
            max_notional=decimal("NOTIONAL", "maxNotional") if "NOTIONAL" in filters else None,
        )
    except (KeyError, StopIteration, TypeError) as error:
        raise ValueError("Expected BTCUSDT price, lot size, and notional filters in Binance exchangeInfo") from error
    if (
        (rules.max_price and rules.min_price > rules.max_price)
        or rules.min_quantity > rules.max_quantity
        or (rules.max_notional is not None and rules.min_notional > rules.max_notional)
    ):
        raise ValueError("A Binance filter minimum exceeds its maximum")
    return rules
