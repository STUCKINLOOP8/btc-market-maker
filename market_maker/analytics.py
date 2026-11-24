"""Measure performance and execution from a replay audit."""

from bisect import bisect_right
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal


MARKOUT_HORIZONS_MS = (1000, 5000, 30000, 60000)
EQUITY_SAMPLE_MS = 1000
ZERO = Decimal("0")


@dataclass(slots=True)
class _FillLot:
    direction: int
    price: Decimal
    quantity: Decimal
    fee: Decimal
    remaining: Decimal


class _SupportingMetrics:
    def __init__(self, btc: Decimal, usdt: Decimal) -> None:
        self.btc = btc
        self.usdt = usdt
        self.times: list[int] = []
        self.equity: list[tuple[Decimal | None, int]] = []
        self.open_lots: deque[_FillLot] = deque()
        self.closed_round_trips = 0
        self.winning_round_trips = 0

    def fill(self, details: dict, direction: int, quantity: Decimal, btc: Decimal) -> None:
        self.btc = btc
        self.usdt = Decimal(details["usdt"])
        lot = _FillLot(direction, Decimal(details["price"]), quantity, Decimal(details["fee_usdt"]), quantity)
        while lot.remaining > ZERO and self.open_lots and self.open_lots[0].direction != direction:
            opening = self.open_lots[0]
            matched = min(opening.remaining, lot.remaining)
            # Compare per-BTC proceeds and fees without dividing prorated commissions.
            gross_per_btc = opening.direction * (lot.price - opening.price)
            gross_scaled = gross_per_btc * opening.quantity * lot.quantity
            fees_scaled = opening.fee * lot.quantity + lot.fee * opening.quantity
            self.closed_round_trips += 1
            self.winning_round_trips += gross_scaled > fees_scaled
            opening.remaining -= matched
            lot.remaining -= matched
            if opening.remaining == ZERO:
                self.open_lots.popleft()
        if lot.remaining > ZERO:
            self.open_lots.append(lot)

    def mark(self, timestamp: int, mid: Decimal | None, feed_epoch: int) -> None:
        self.times.append(timestamp)
        self.equity.append((self.usdt + self.btc * mid if mid is not None else None, feed_epoch))

    def summarize(self, start_ms: int, end_ms: int) -> dict:
        returns: list[Decimal] = []
        previous = peak = drawdown = None
        previous_epoch = None
        for sample_time in range(start_ms, end_ms + 1, EQUITY_SAMPLE_MS):
            index = bisect_right(self.times, sample_time) - 1
            if index < 0:
                continue
            equity, epoch = self.equity[index]
            if equity is not None:
                if previous is not None and previous > ZERO and epoch == previous_epoch:
                    returns.append((equity - previous) / previous)
                if equity > ZERO and (peak is None or equity > peak):
                    peak = equity
                if peak is not None:
                    decline = (peak - equity) / peak * 100
                    drawdown = decline if drawdown is None else max(drawdown, decline)
            previous, previous_epoch = equity, epoch

        sharpe = None
        if len(returns) >= 2 and min(returns) != max(returns):
            mean = sum(returns, start=ZERO) / len(returns)
            variance = sum(((value - mean) ** 2 for value in returns), start=ZERO) / (len(returns) - 1)
            if variance > ZERO:
                sharpe = mean / variance.sqrt()

        return {
            "sharpe": sharpe,
            "sharpe_return_samples": len(returns),
            "max_drawdown_pct": drawdown,
            "win_rate_pct": (
                Decimal(100) * self.winning_round_trips / self.closed_round_trips
                if self.closed_round_trips else None
            ),
            "winning_round_trips": self.winning_round_trips,
            "closed_round_trips": self.closed_round_trips,
        }


def measure_audit(
    entries: Iterable[dict], *, start_ms: int, end_ms: int, starting_btc: Decimal,
    starting_usdt: Decimal | None = None,
) -> dict:
    """Measure elapsed time on the recorded clock and sample markouts as of each horizon."""
    if not 0 <= start_ms <= end_ms:
        raise ValueError("Invalid capture window")
    active: set[str] = set()
    last_ms = start_ms
    quoted_ms = market_ready_ms = 0
    inventory = filled_volume = ZERO
    integral = squared_integral = ZERO
    mid: Decimal | None = None
    feed_epoch = 0
    mark_times: list[int] = []
    marks: list[tuple[Decimal | None, int]] = []
    fills: list[tuple[int, int, Decimal | None, int]] = []
    supporting = _SupportingMetrics(starting_btc, starting_usdt) if starting_usdt is not None else None

    def accumulate(through_ms: int) -> None:
        nonlocal last_ms, quoted_ms, market_ready_ms, integral, squared_integral
        duration = through_ms - last_ms
        if mid is not None:
            market_ready_ms += duration
            if active:
                quoted_ms += duration
        integral += inventory * duration
        squared_integral += inventory * inventory * duration
        last_ms = through_ms

    previous_ms = -1
    for entry in entries:
        timestamp = entry["timestamp_ms"]
        if timestamp < previous_ms or timestamp > end_ms:
            raise ValueError("Audit timestamps must be ordered and within the recording end")
        previous_ms = timestamp
        accumulate(max(start_ms, timestamp))
        kind, details = entry["kind"], entry["details"]
        if kind == "FEED_UNAVAILABLE":
            mid = None
            feed_epoch += 1
            active.clear()
        elif kind == "BOOK":
            value = details["mid_price"]
            mid = Decimal(value) if value is not None else None
        if kind in ("BOOK", "FEED_UNAVAILABLE"):
            mark_times.append(timestamp)
            marks.append((mid, feed_epoch))

        if kind == "ORDER_ACTIVATED":
            active.add(details["order_id"])
        elif kind in ("ORDER_CANCELED", "ORDER_REJECTED"):
            active.discard(details["order_id"])
        elif kind == "FILL":
            direction = 1 if details["side"] == "buy" else -1
            fills.append((timestamp, direction, mid, feed_epoch))
            quantity = Decimal(details["fill_quantity"])
            btc = Decimal(details["btc"])
            filled_volume += quantity
            inventory = btc - starting_btc
            if supporting is not None:
                supporting.fill(details, direction, quantity, btc)
            if details["status"] == "FILLED":
                active.discard(details["order_id"])
        if supporting is not None and kind in ("BOOK", "FEED_UNAVAILABLE", "FILL"):
            supporting.mark(timestamp, mid, feed_epoch)

    accumulate(end_ms)
    duration_ms = end_ms - start_ms
    inventory_variance = None
    if duration_ms:
        inventory_variance = max(ZERO, squared_integral / duration_ms - (integral / duration_ms) ** 2)

    markouts = {}
    for horizon in MARKOUT_HORIZONS_MS:
        signed_sum = ZERO
        observations = 0
        for timestamp, direction, fill_mid, fill_epoch in fills:
            target = timestamp + horizon
            if target > end_ms or fill_mid is None:
                continue
            index = bisect_right(mark_times, target) - 1
            if index < 0:
                continue
            future_mid, future_epoch = marks[index]
            if future_mid is not None and future_epoch == fill_epoch:
                signed_sum += direction * (future_mid - fill_mid)
                observations += 1
        markouts[f"{horizon // 1000}s"] = {
            "mean_usdt_per_btc": signed_sum / observations if observations else None,
            "samples": observations,
            "unavailable": len(fills) - observations,
        }

    return {
        "capture_ms": duration_ms,
        "valid_midpoint_ms": market_ready_ms,
        "time_quoted_ms": quoted_ms,
        "time_quoted_pct": Decimal(100) * quoted_ms / market_ready_ms if market_ready_ms else None,
        "fills": len(fills),
        "filled_volume_btc": filled_volume,
        "inventory_variance_btc2": inventory_variance,
        "markouts": markouts,
        **(supporting.summarize(start_ms, end_ms) if supporting is not None else {}),
    }
