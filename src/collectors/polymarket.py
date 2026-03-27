"""
PMarb — Polymarket Price Collector.

Monitors Polymarket CLOB for contract prices, volume,
and liquidity depth on geopolitical markets.
Uses py-clob-client for REST polling.
"""

from __future__ import annotations

import logging

import aiohttp

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, PMPriceEvent

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"


class PolymarketCollector(BaseCollector):
    name = "polymarket"

    def __init__(
        self,
        bus: EventBus,
        market_slugs: dict[str, str] | None = None,
        poll_interval: int = 5,
    ) -> None:
        super().__init__(bus, poll_interval)
        # key=internal name, value=slug prefix on Polymarket
        self.market_slugs = market_slugs or {}
        self._session: aiohttp.ClientSession | None = None
        self._condition_cache: dict[str, dict] = {}

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
        )
        # Pre-resolve condition IDs
        await self._resolve_markets()
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        if self._session:
            await self._session.close()

    async def _resolve_markets(self) -> None:
        """Resolve market slugs to condition IDs via Gamma API."""
        assert self._session is not None

        for name, slug in self.market_slugs.items():
            try:
                # Try exact slug match first (more reliable)
                mkt = None
                for params in [
                    {"slug": slug},
                    {"slug_contains": slug, "closed": "false"},
                ]:
                    async with self._session.get(
                        f"{GAMMA_API}/markets",
                        params=params,
                    ) as resp:
                        resp.raise_for_status()
                        markets = await resp.json()

                    if markets:
                        mkt = markets[0]
                        break

                if mkt:
                    self._condition_cache[name] = {
                        "slug": mkt.get("slug", slug),
                        "condition_id": mkt.get("conditionId", ""),
                        "question": mkt.get("question", ""),
                        "clob_token_ids": mkt.get("clobTokenIds", ""),
                    }
                    logger.info(
                        "[polymarket] Resolved '%s' → %s (%s)",
                        name,
                        mkt.get("slug"),
                        mkt.get("question", "")[:60],
                    )
                else:
                    logger.warning("[polymarket] No market found for slug: %s", slug)
            except Exception:
                logger.exception("[polymarket] Failed to resolve market: %s", slug)

    async def poll(self) -> list[BaseEvent]:
        events: list[BaseEvent] = []
        assert self._session is not None

        for name, info in self._condition_cache.items():
            try:
                slug = info["slug"]
                async with self._session.get(
                    f"{GAMMA_API}/markets",
                    params={"slug": slug},
                ) as resp:
                    resp.raise_for_status()
                    markets = await resp.json()

                if not markets:
                    continue

                mkt = markets[0]
                # Polymarket prices: outcomePrices is a JSON string "[yes_price, no_price]"
                prices_raw = mkt.get("outcomePrices", "")
                yes_price = 0.0
                no_price = 0.0
                if prices_raw:
                    import json
                    try:
                        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                        if len(prices) >= 2:
                            yes_price = float(prices[0])
                            no_price = float(prices[1])
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass

                volume = float(mkt.get("volume", 0) or 0)
                volume_24h = float(mkt.get("volume24hr", 0) or 0)
                spread = abs(yes_price - (1 - no_price)) if yes_price and no_price else 0

                event = PMPriceEvent(
                    source="polymarket",
                    market_slug=slug,
                    condition_id=info.get("condition_id", ""),
                    yes_price=yes_price,
                    no_price=no_price,
                    volume_24h=volume_24h,
                    spread=spread,
                    liquidity_depth=volume,
                )
                events.append(event)

                logger.debug(
                    "[polymarket] %s | YES=$%.3f NO=$%.3f vol24h=$%.0f",
                    name,
                    yes_price,
                    no_price,
                    volume_24h,
                )

            except Exception:
                logger.exception("[polymarket] Failed to poll market: %s", name)

        return events

    def get_cached_markets(self) -> dict[str, dict]:
        """Return cached market info for other modules."""
        return dict(self._condition_cache)
