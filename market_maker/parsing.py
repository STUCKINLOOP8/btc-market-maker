from decimal import Decimal

from .models import BookLevel


def read_levels(entries: list[list[str]]) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(
            read_decimal(price, "level price"),
            read_decimal(quantity, "level quantity", allow_zero=True),
        )
        for price, quantity in entries
    )


def read_integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be a JSON integer")
    return value


def read_decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a JSON string containing a decimal")

    number = Decimal(value)
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    if number < 0 or (number.is_zero() and not allow_zero):
        requirement = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {requirement}")
    return number
