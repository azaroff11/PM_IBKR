"""
PMarb — Base collector ABC.

All data collectors inherit from this and implement
_poll() + _parse() for their specific data source.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

from src.event_bus import EventBus
from src.models.events import BaseEvent

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base for all data collectors."""

    name: str = "base"

    def __init__(self, bus: EventBus, poll_interval: int = 60) -> None:
        self.bus = bus
        self.poll_interval = poll_interval
        self._running = False
        self._task: asyncio.Task | None = None
        self._poll_count = 0
        self._error_count = 0

    async def start(self) -> None:
        """Start the polling loop."""
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("[%s] Collector started (interval=%ds)", self.name, self.poll_interval)

    async def stop(self) -> None:
        """Stop gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            "[%s] Collector stopped | polls=%d errors=%d",
            self.name,
            self._poll_count,
            self._error_count,
        )

    async def _loop(self) -> None:
        """Main polling loop with error recovery."""
        while self._running:
            try:
                events = await self.poll()
                self._poll_count += 1
                for event in events:
                    await self.bus.publish(event)
            except asyncio.CancelledError:
                break
            except Exception:
                self._error_count += 1
                logger.exception("[%s] Poll error (#%d)", self.name, self._error_count)

            await asyncio.sleep(self.poll_interval)

    @abstractmethod
    async def poll(self) -> list[BaseEvent]:
        """Fetch data and return events. Must be implemented by subclass."""
        ...
