"""
PMarb — Signal Aggregator.

Collects signals from all analytics modules, applies
weighted confidence scoring, and emits final trade signals.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from src.event_bus import EventBus
from src.models.events import ArbSignal, BaseEvent, EventType

logger = logging.getLogger(__name__)


class SignalAggregator:
    """Aggregates and filters signals from all analytics modules."""

    def __init__(
        self,
        bus: EventBus,
        min_strength: float = 0.3,
        min_confidence: float = 0.5,
        max_signals_per_hour: int = 5,
    ) -> None:
        self.bus = bus
        self.min_strength = min_strength
        self.min_confidence = min_confidence
        self.max_signals_per_hour = max_signals_per_hour

        self._signal_history: list[tuple[float, ArbSignal]] = []
        self._active_signals: dict[str, ArbSignal] = {}  # by strategy+slug

        self.bus.subscribe(EventType.SIGNAL, self._on_signal)

    async def _on_signal(self, event: BaseEvent) -> None:
        assert isinstance(event, ArbSignal)
        signal = event
        now = time.time()

        # Filter: minimum quality thresholds
        if signal.strength < self.min_strength:
            logger.debug(
                "[signals] Filtered (low strength=%.2f): %s",
                signal.strength,
                signal.strategy.value,
            )
            return

        if signal.confidence < self.min_confidence:
            logger.debug(
                "[signals] Filtered (low confidence=%.2f): %s",
                signal.confidence,
                signal.strategy.value,
            )
            return

        # Rate limit
        hour_ago = now - 3600
        self._signal_history = [(t, s) for t, s in self._signal_history if t > hour_ago]
        if len(self._signal_history) >= self.max_signals_per_hour:
            logger.warning("[signals] Rate limit reached (%d/hr)", self.max_signals_per_hour)
            return

        # Dedup: one active signal per strategy + market
        key = f"{signal.strategy.value}:{signal.pm_market_slug}"
        existing = self._active_signals.get(key)
        if existing:
            # Update only if new signal is stronger
            composite_new = signal.strength * signal.confidence
            composite_old = existing.strength * existing.confidence
            if composite_new <= composite_old:
                logger.debug("[signals] Existing signal stronger for %s", key)
                return

        # Accept signal
        self._signal_history.append((now, signal))
        self._active_signals[key] = signal

        logger.info(
            "[signals] ✅ ACCEPTED: %s | %s NO@$%.3f | str=%.2f conf=%.2f | hedge=%s %s",
            signal.strategy.value,
            signal.pm_market_slug,
            signal.pm_price,
            signal.strength,
            signal.confidence,
            signal.hedge_type.value,
            signal.hedge_symbol,
        )

    def get_active_signals(self) -> list[ArbSignal]:
        """Return all currently active signals."""
        return list(self._active_signals.values())

    def clear_signal(self, key: str) -> None:
        """Remove a signal after execution or expiry."""
        self._active_signals.pop(key, None)

    @property
    def stats(self) -> dict:
        return {
            "active_signals": len(self._active_signals),
            "signals_last_hour": len(self._signal_history),
            "strategies": list({s.strategy.value for _, s in self._signal_history}),
        }
