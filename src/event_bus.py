"""
PMarb — Async Event Bus.

Decouples all modules via typed publish/subscribe.
Thread-safe, supports multiple subscribers per event type.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Coroutine

from src.models.events import BaseEvent, EventType

logger = logging.getLogger(__name__)

Handler = Callable[[BaseEvent], Coroutine[Any, Any, None]]


class EventBus:
    """Central async event bus for inter-module communication."""

    def __init__(self) -> None:
        self._handlers: dict[EventType, list[Handler]] = defaultdict(list)
        self._queue: asyncio.Queue[BaseEvent] = asyncio.Queue()
        self._running = False
        self._dispatch_task: asyncio.Task | None = None
        self._event_count: dict[EventType, int] = defaultdict(int)

    def subscribe(self, event_type: EventType, handler: Handler) -> None:
        """Register a handler for a specific event type."""
        self._handlers[event_type].append(handler)
        logger.debug("Subscribed %s to %s", handler.__qualname__, event_type.value)

    async def publish(self, event: BaseEvent) -> None:
        """Publish an event to the bus."""
        await self._queue.put(event)
        self._event_count[event.event_type] += 1

    def publish_sync(self, event: BaseEvent) -> None:
        """Non-async publish (for use in sync callbacks)."""
        self._queue.put_nowait(event)
        self._event_count[event.event_type] += 1

    async def start(self) -> None:
        """Start the dispatch loop."""
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())
        logger.info("EventBus started")

    async def stop(self) -> None:
        """Stop the dispatch loop gracefully."""
        self._running = False
        if self._dispatch_task:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
        logger.info("EventBus stopped | Events processed: %s", dict(self._event_count))

    async def _dispatch_loop(self) -> None:
        """Main dispatch loop — routes events to subscribers."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            handlers = self._handlers.get(event.event_type, [])
            if not handlers:
                logger.debug("No handlers for %s", event.event_type.value)
                continue

            for handler in handlers:
                try:
                    await handler(event)
                except Exception:
                    logger.exception(
                        "Handler %s failed for event %s",
                        handler.__qualname__,
                        event.event_type.value,
                    )

    @property
    def stats(self) -> dict[str, int]:
        return {k.value: v for k, v in self._event_count.items()}
