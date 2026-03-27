"""
PMarb — PM vs TradFi Spread Calculator.

Measures divergence between Polymarket implied probabilities
and TradFi options-implied probabilities for the same events.
"""

from __future__ import annotations

import logging
import math

from src.event_bus import EventBus
from src.models.events import BaseEvent, EventType, PMPriceEvent, TradFiEvent

logger = logging.getLogger(__name__)


class SpreadCalculator:
    """Calculates and tracks PM-TradFi probability spreads."""

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self._pm_prices: dict[str, PMPriceEvent] = {}
        self._tradfi_data: dict[str, TradFiEvent] = {}

        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)
        self.bus.subscribe(EventType.TRADFI, self._on_tradfi)

    async def _on_pm_price(self, event: BaseEvent) -> None:
        assert isinstance(event, PMPriceEvent)
        self._pm_prices[event.market_slug] = event

    async def _on_tradfi(self, event: BaseEvent) -> None:
        assert isinstance(event, TradFiEvent)
        self._tradfi_data[event.symbol] = event
        await self._calculate_spreads()

    async def _calculate_spreads(self) -> None:
        """Calculate probability divergence between PM and TradFi."""
        for slug, pm in self._pm_prices.items():
            # Map PM markets to TradFi instruments
            if "hormuz" in slug.lower() or "strait" in slug.lower():
                tradfi = self._tradfi_data.get("BNO") or self._tradfi_data.get("USO")
            elif "ceasefire" in slug.lower() or "peace" in slug.lower():
                tradfi = self._tradfi_data.get("USO")
            else:
                continue

            if not tradfi or tradfi.iv_atm <= 0:
                continue

            # PM implied probability = YES price
            pm_prob = pm.yes_price

            # TradFi implied probability from IV
            # Rough proxy: extreme IV skew signals tail risk pricing
            # High put IV → market fears drop → war continuation likely → ceasefire unlikely
            tradfi_fear_index = self._calculate_fear_index(tradfi)

            # Spread: positive = PM overpricing event probability vs TradFi
            spread_bps = (pm_prob - tradfi_fear_index) * 10000

            if abs(spread_bps) > 500:  # > 5% divergence
                logger.info(
                    "[spread] %s | PM_prob=%.1f%% TradFi_fear=%.1f%% spread=%+.0fbps",
                    slug,
                    pm_prob * 100,
                    tradfi_fear_index * 100,
                    spread_bps,
                )

    def _calculate_fear_index(self, tradfi: TradFiEvent) -> float:
        """
        Derive implied event probability from IV skew.

        Logic: High put IV relative to call IV → market fears downside
        → oil crash = peace/deescalation → ceasefire more likely
        In reverse: High call IV → market fears upside spike
        → blockade/war → Hormuz closure more likely
        """
        if tradfi.iv_atm <= 0:
            return 0.5  # No data, assume 50%

        # Put/Call IV ratio as proxy for directional fear
        if tradfi.iv_put_25d > 0 and tradfi.iv_call_25d > 0:
            skew_ratio = tradfi.iv_call_25d / tradfi.iv_put_25d
        else:
            skew_ratio = 1.0

        # Normalize to 0-1 probability-like metric
        # Call skew > 1.3 → high upside fear → ~30-50% implied likelihood
        # Call skew < 0.8 → market calm → ~5-10% implied likelihood
        normalized = max(0.0, min(1.0, (skew_ratio - 0.5) / 1.5))
        return normalized

    def get_current_spreads(self) -> dict[str, dict]:
        """Return current spread metrics for all tracked markets."""
        spreads = {}
        for slug, pm in self._pm_prices.items():
            tradfi = None
            if "hormuz" in slug.lower():
                tradfi = self._tradfi_data.get("BNO")
            elif "ceasefire" in slug.lower():
                tradfi = self._tradfi_data.get("USO")

            spreads[slug] = {
                "pm_yes": pm.yes_price,
                "pm_no": pm.no_price,
                "tradfi_iv_atm": tradfi.iv_atm if tradfi else 0,
                "tradfi_spot": tradfi.spot if tradfi else 0,
                "tradfi_iv_skew": (
                    (tradfi.iv_call_25d / tradfi.iv_put_25d)
                    if tradfi and tradfi.iv_put_25d > 0
                    else 0
                ),
            }
        return spreads
