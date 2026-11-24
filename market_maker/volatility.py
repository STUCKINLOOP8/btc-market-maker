"""Sample replay midpoints on an elapsed-time grid."""

from collections import deque
from decimal import Decimal


SAMPLE_INTERVAL_MS = 1000
VOLATILITY_WINDOW_SECONDS = 60


class MidpointReturnVariance:
    """Sample variance of up to 60 consecutive one-second log returns."""

    def __init__(self) -> None:
        self._returns: deque[Decimal] = deque(maxlen=VOLATILITY_WINDOW_SECONDS)
        self.reset()

    def reset(self) -> None:
        self._returns.clear()
        self._previous_mid: Decimal | None = None
        self._previous_sample: Decimal | None = None
        self._next_sample_ms: int | None = None
        self._variance: Decimal | None = None

    def observe(self, timestamp_ms: int, mid: Decimal | None) -> Decimal | None:
        """Use the latest known midpoint at each boundary, including exact ties."""
        if mid is None:
            self.reset()
            return None
        if self._next_sample_ms is None:
            self._next_sample_ms = -(-timestamp_ms // SAMPLE_INTERVAL_MS) * SAMPLE_INTERVAL_MS
        last_boundary = timestamp_ms - timestamp_ms % SAMPLE_INTERVAL_MS
        first_in_window = last_boundary - VOLATILITY_WINDOW_SECONDS * SAMPLE_INTERVAL_MS
        if self._next_sample_ms < first_in_window:
            # Older samples would be evicted before calculating the variance.
            self._next_sample_ms = first_in_window
            self._returns.clear()
            self._previous_sample = None

        if self._next_sample_ms <= timestamp_ms:
            while self._next_sample_ms <= timestamp_ms:
                sample = mid if self._next_sample_ms == timestamp_ms else self._previous_mid
                assert sample is not None
                if self._previous_sample is not None:
                    self._returns.append((sample / self._previous_sample).ln())
                self._previous_sample = sample
                self._next_sample_ms += SAMPLE_INTERVAL_MS
            if len(self._returns) >= 2:
                mean = sum(self._returns) / len(self._returns)
                self._variance = sum((value - mean) ** 2 for value in self._returns) / (len(self._returns) - 1)
        self._previous_mid = mid
        return self._variance
