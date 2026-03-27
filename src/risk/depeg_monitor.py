"""
PMarb — USDC Depeg Monitor.

Monitors stablecoin peg health via DeFi Llama or Curve 3pool.
Alerts if USDC deviates from $1.00 by more than threshold.
"""

from __future__ import annotations

import logging

import aiohttp

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, DepegAlert

logger = logging.getLogger(__name__)

# DeFi Llama stablecoin price endpoint (no auth needed)
DEFILLAMA_PRICES = "https://coins.llama.fi/prices/current"
STABLECOINS = {
    "USDC": "coingecko:usd-coin",
    "USDT": "coingecko:tether",
    "DAI": "coingecko:dai",
}


class DepegMonitor(BaseCollector):
    name = "depeg_monitor"

    def __init__(
        self,
        bus: EventBus,
        alert_threshold_bps: float = 50,  # 0.5%
        poll_interval: int = 300,  # 5 min
    ) -> None:
        super().__init__(bus, poll_interval)
        self.alert_threshold_bps = alert_threshold_bps
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        if self._session:
            await self._session.close()

    async def poll(self) -> list[BaseEvent]:
        assert self._session is not None
        events: list[BaseEvent] = []

        coin_ids = ",".join(STABLECOINS.values())
        try:
            async with self._session.get(f"{DEFILLAMA_PRICES}/{coin_ids}") as resp:
                resp.raise_for_status()
                data = await resp.json()

            coins = data.get("coins", {})
            for name, coin_id in STABLECOINS.items():
                coin_data = coins.get(coin_id, {})
                price = coin_data.get("price", 1.0)
                deviation_bps = abs(price - 1.0) * 10000

                if deviation_bps >= self.alert_threshold_bps:
                    event = DepegAlert(
                        source="depeg_monitor",
                        token=name,
                        deviation_bps=deviation_bps,
                    )
                    events.append(event)
                    logger.warning(
                        "[depeg] ⚠️ %s DEVIATION: $%.4f (%.1f bps off peg)",
                        name,
                        price,
                        deviation_bps,
                    )

        except Exception:
            logger.exception("[depeg] Failed to check stablecoin prices")

        return events
