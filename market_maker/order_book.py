from decimal import Decimal
from heapq import nlargest, nsmallest
from operator import itemgetter

from .models import BookLevel, BookSnapshot, DepthUpdate, Side


class OrderBook:
    """Apply normalized snapshots and absolute depth updates for one market."""

    def __init__(self) -> None:
        self._depth: dict[Side, dict[Decimal, Decimal]] | None = None
        self.best_bid: Decimal | None = None
        self.best_ask: Decimal | None = None

    @property
    def initialized(self) -> bool:
        return self._depth is not None

    @property
    def mid_price(self) -> Decimal | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None or bid >= ask:
            return None
        return (bid + ask) / 2

    def apply(self, event: BookSnapshot | DepthUpdate) -> None:
        if isinstance(event, BookSnapshot):
            self._depth = {Side.BUY: {}, Side.SELL: {}}
        if self._depth is None:
            raise ValueError("A book snapshot is required before applying depth updates")

        for side, levels in ((Side.BUY, event.bids), (Side.SELL, event.asks)):
            depth = self._depth[side]
            for level in levels:
                if level.quantity == 0:
                    depth.pop(level.price, None)
                else:
                    depth[level.price] = level.quantity

        self.best_bid = max(self._depth[Side.BUY], default=None)
        self.best_ask = min(self._depth[Side.SELL], default=None)

    def quantity_at(self, side: Side, price: Decimal) -> Decimal:
        if self._depth is None:
            raise ValueError("A book snapshot is required before looking up depth")
        return self._depth[side].get(price, Decimal("0"))

    def snapshot(self, timestamp_ms: int, sequence: int, *, limit: int | None = None) -> BookSnapshot:
        if self._depth is None:
            raise ValueError("A book snapshot is required before exporting depth")
        bids = self._depth[Side.BUY].items()
        asks = self._depth[Side.SELL].items()
        if limit is not None:
            best_bids = nlargest(limit, bids, key=itemgetter(0))
            best_asks = nsmallest(limit, asks, key=itemgetter(0))
        else:
            best_bids = bids
            best_asks = asks
        return BookSnapshot(
            timestamp_ms,
            sequence,
            bids=tuple(BookLevel(price, quantity) for price, quantity in best_bids),
            asks=tuple(BookLevel(price, quantity) for price, quantity in best_asks),
        )
