"""
PMarb — Fake Ceasefire Detector.

Exploits the bilateral confirmation trap:
Polymarket ceasefire contracts require BOTH sides to confirm.
Unilateral Trump tweets trigger crowd FOMO → buy YES.
We detect this and signal BUY NO when bilateral = False.

Hedge: OTM Put on USO (if real peace → oil crashes → puts profit).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from src.event_bus import EventBus
from src.models.events import (
    ArbSignal,
    BaseEvent,
    EventType,
    HedgeType,
    PMPriceEvent,
    SentimentEvent,
    Side,
    Strategy,
)

logger = logging.getLogger(__name__)


class CeasefireDetector:
    """Detects fake ceasefire signals and generates arb opportunities."""

    def __init__(
        self,
        bus: EventBus,
        pm_yes_threshold: float = 0.15,
        signal_cooldown_sec: int = 300,
        hedge_symbol: str = "USO",
        hedge_strike_offset_pct: float = 0.10,  # 10% OTM
    ) -> None:
        self.bus = bus
        self.pm_yes_threshold = pm_yes_threshold
        self.signal_cooldown_sec = signal_cooldown_sec
        self.hedge_symbol = hedge_symbol
        self.hedge_strike_offset_pct = hedge_strike_offset_pct

        # State
        self._last_signal_time: float = 0
        self._latest_sentiment: SentimentEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}
        self._ceasefire_keywords_seen = False
        self._bilateral_confirmed = False

        # Subscribe to events
        self.bus.subscribe(EventType.SENTIMENT, self._on_sentiment)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)

    async def _on_sentiment(self, event: BaseEvent) -> None:
        """Process sentiment events for ceasefire keywords."""
        assert isinstance(event, SentimentEvent)
        self._latest_sentiment = event

        if "ceasefire" in event.keywords_matched:
            self._ceasefire_keywords_seen = True
            self._bilateral_confirmed = event.is_bilateral

            logger.info(
                "[ceasefire] Sentiment trigger: bilateral=%s keywords=%s src=%s",
                event.is_bilateral,
                event.keywords_matched,
                event.source_platform,
            )

            # Immediately check if we can generate a signal
            await self._evaluate()

    async def _on_pm_price(self, event: BaseEvent) -> None:
        """Track ceasefire market prices."""
        assert isinstance(event, PMPriceEvent)
        self._latest_pm_prices[event.market_slug] = event

        # Re-evaluate on price update if we have a sentiment trigger
        if self._ceasefire_keywords_seen:
            await self._evaluate()

    async def _evaluate(self) -> None:
        """Core logic: detect fake ceasefire and generate signal."""
        now = time.time()
        if now - self._last_signal_time < self.signal_cooldown_sec:
            return  # Cooldown active

        # Find the ceasefire market
        ceasefire_market: PMPriceEvent | None = None
        for slug, pm in self._latest_pm_prices.items():
            if "ceasefire" in slug.lower() or "peace" in slug.lower():
                ceasefire_market = pm
                break

        if not ceasefire_market:
            return

        yes_price = ceasefire_market.yes_price
        no_price = ceasefire_market.no_price

        # ═══ SIGNAL LOGIC ═══
        # Condition 1: YES price pumped above threshold (crowd buying YES)
        if yes_price < self.pm_yes_threshold:
            return  # No pump detected

        # Condition 2: Bilateral NOT confirmed (Iran hasn't agreed)
        if self._bilateral_confirmed:
            logger.warning("[ceasefire] BILATERAL CONFIRMED — NOT generating NO signal")
            return

        # Condition 3: We have sentiment trigger (unilateral statement)
        if not self._ceasefire_keywords_seen:
            return

        # ═══ CALCULATE SIGNAL ═══
        # Strength: higher YES = stronger signal (more mispricing)
        strength = min(1.0, yes_price / 0.50)  # normalized: 50% YES = max strength

        # Confidence: depends on source quality
        confidence = 0.7
        if self._latest_sentiment:
            if self._latest_sentiment.source_platform in ("truth_social", "twitter"):
                confidence = 0.85  # Unilateral political theater = high confidence NO
            if "iran_confirm" in self._latest_sentiment.keywords_matched:
                confidence = 0.3  # Iran saying something = lower confidence for NO

        # Reasoning
        reasoning_parts = [
            f"PM ДА=${yes_price:.3f} (выше порога {self.pm_yes_threshold})",
            f"Двусторонее подтверждение: {self._bilateral_confirmed}",
            f"Источник: {self._latest_sentiment.source_platform if self._latest_sentiment else 'неизвестно'}",
            "Стратегия: Купить НЕТ на PM + Купить OTM Пут USO как хедж",
        ]

        # ═══ CALCULATE INEFFICIENCY SIZE ═══
        # Edge: YES price vs estimated real probability
        # Without bilateral confirmation, real ceasefire prob ≈ 5%
        real_prob = 0.05 if not self._bilateral_confirmed else 0.40
        edge_pct = (yes_price - real_prob) * 100  # % mispricing

        # Available depth: PM liquidity at current price
        depth_usd = ceasefire_market.liquidity_depth or ceasefire_market.volume_24h * 0.1

        # Max profit: if we buy NO at no_price, profit = (1 - no_price) per dollar
        profit_per_dollar = 1.0 - no_price
        max_profit_usd = profit_per_dollar * depth_usd

        # Max loss: we invest at no_price, if wrong → lose full investment
        max_loss_usd = no_price * depth_usd

        # Risk/Reward ratio (>1 = favorable)
        risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0

        # Expected value: edge weighted by confidence
        ev_usd = edge_pct / 100 * confidence * depth_usd

        signal = ArbSignal(
            source="ceasefire_detector",
            strategy=Strategy.FAKE_CEASEFIRE,
            pm_market_slug=ceasefire_market.market_slug,
            pm_side=Side.BUY_NO,
            pm_price=no_price,
            pm_size_usd=0,  # Sized by risk module
            hedge_type=HedgeType.PUT,
            hedge_symbol=self.hedge_symbol,
            hedge_strike=0,  # Calculated by execution engine based on current spot
            hedge_expiry="",  # Selected by execution engine
            strength=strength,
            confidence=confidence,
            reasoning=" | ".join(reasoning_parts),
            edge_pct=round(edge_pct, 1),
            available_depth_usd=round(depth_usd, 0),
            max_profit_usd=round(max_profit_usd, 0),
            max_loss_usd=round(max_loss_usd, 0),
            ev_usd=round(ev_usd, 0),
            risk_reward=round(risk_reward, 2),
        )

        await self.bus.publish(signal)
        self._last_signal_time = now
        self._ceasefire_keywords_seen = False  # Reset trigger

        logger.info(
            "[ceasefire] ⚡ SIGNAL: BUY NO @ $%.3f | strength=%.2f conf=%.2f | %s",
            no_price,
            strength,
            confidence,
            signal.reasoning[:100],
        )
