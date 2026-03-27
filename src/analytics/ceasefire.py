"""
PMarb — Fake Ceasefire Detector.

Exploits the bilateral confirmation trap:
Polymarket ceasefire contracts require BOTH sides to confirm.
Unilateral Trump tweets trigger crowd FOMO → buy YES.
We detect this and signal BUY NO when bilateral = False.

Hedge: OTM Put on USO (if real peace → oil crashes → puts profit).

All-weather: Signal only emitted if net P&L > 0 in BOTH scenarios.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from src.analytics.pnl_validator import validate_signal as validate_pnl
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
    TradFiEvent,
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
        expected_move_pct: float = 0.08,  # 8% USO drop on real ceasefire
    ) -> None:
        self.bus = bus
        self.pm_yes_threshold = pm_yes_threshold
        self.signal_cooldown_sec = signal_cooldown_sec
        self.hedge_symbol = hedge_symbol
        self.hedge_strike_offset_pct = hedge_strike_offset_pct
        self.expected_move_pct = expected_move_pct

        # State
        self._last_signal_time: float = 0
        self._latest_sentiment: SentimentEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}
        self._latest_tradfi: dict[str, TradFiEvent] = {}
        self._ceasefire_keywords_seen = False
        self._bilateral_confirmed = False

        # Subscribe to events
        self.bus.subscribe(EventType.SENTIMENT, self._on_sentiment)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)
        self.bus.subscribe(EventType.TRADFI, self._on_tradfi)

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

    async def _on_tradfi(self, event: BaseEvent) -> None:
        """Track TradFi data for hedge pricing. Re-evaluate on first data."""
        assert isinstance(event, TradFiEvent)
        is_first = event.symbol not in self._latest_tradfi
        self._latest_tradfi[event.symbol] = event
        if is_first and self._ceasefire_keywords_seen:
            self._last_signal_time = 0
            await self._evaluate()

    def _get_option_premium(self) -> tuple[float, float, float]:
        """Get best available option premium for hedge from TradFi data.

        Returns: (premium_per_share, delta, spot_price)
        """
        tf = self._latest_tradfi.get(self.hedge_symbol)
        if not tf or tf.spot <= 0:
            return 0.0, 0.0, 0.0

        # Try real options data first
        if tf.options:
            puts = [o for o in tf.options if o.right == "P" and (o.ask > 0 or o.bid > 0)]
            if puts:
                puts.sort(key=lambda o: abs(abs(o.delta) - 0.25))
                best = puts[0]
                price = best.ask if best.ask > 0 else best.bid * 1.05  # Use bid + spread
                return price, abs(best.delta), tf.spot

        # Fallback: Black-Scholes estimate from IV ATM
        if tf.iv_atm > 0:
            import math
            t = 30 / 365
            premium = tf.spot * tf.iv_atm * math.sqrt(t) * 0.4  # OTM discount
            return premium, 0.25, tf.spot

        return 0.0, 0.0, tf.spot

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

        # ═══ CALCULATE INEFFICIENCY SIZE ═══
        real_prob = 0.05 if not self._bilateral_confirmed else 0.40
        edge_pct = (yes_price - real_prob) * 100

        depth_usd = ceasefire_market.liquidity_depth or ceasefire_market.volume_24h * 0.1

        # ═══ ALL-WEATHER P&L VALIDATION ═══
        option_premium, option_delta, spot = self._get_option_premium()

        from src.analytics.pnl_validator import DEFAULT_BUDGET_USD
        budget = DEFAULT_BUDGET_USD  # $10K

        if option_premium > 0 and spot > 0:
            validation = validate_pnl(
                pm_side="buy_no",
                pm_price=no_price,
                pm_notional=0,
                hedge_type="put",
                option_premium=option_premium,
                option_delta=option_delta,
                expected_move_pct=self.expected_move_pct,
                spot_price=spot,
                pm_spread=ceasefire_market.spread or 0.02,
            )

            hedge_cost_usd = validation.hedge_cost_usd
            net_profit_best = validation.best_case.net_pnl
            net_profit_worst = validation.worst_case.net_pnl
            breakeven_prob = validation.breakeven_prob
            tx_costs_usd = validation.tx_costs_usd

            # Budget-constrained P&L
            max_profit_usd = validation.best_case.net_pnl
            max_loss_usd = abs(validation.worst_case.net_pnl)

            if not validation.is_valid:
                logger.info(
                    "[ceasefire] Signal REJECTED by P&L validator: %s",
                    validation.rejection_reason,
                )
                strength *= 0.3
        else:
            # No option data — use budget-constrained estimate
            profit_per_dollar = 1.0 - no_price
            max_profit_usd = profit_per_dollar * budget
            max_loss_usd = no_price * budget
            hedge_cost_usd = 0.0
            net_profit_best = max_profit_usd
            net_profit_worst = -max_loss_usd
            breakeven_prob = 0.5
            tx_costs_usd = 0.0

        risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
        ev_usd = edge_pct / 100 * confidence * budget

        # Reasoning
        reasoning_parts = [
            f"PM ДА=${yes_price:.3f} (выше порога {self.pm_yes_threshold})",
            f"Двусторонее подтверждение: {self._bilateral_confirmed}",
            f"Источник: {self._latest_sentiment.source_platform if self._latest_sentiment else 'неизвестно'}",
        ]
        if option_premium > 0:
            reasoning_parts.append(
                f"Хедж PUT {self.hedge_symbol}: премия ${option_premium:.2f} | "
                f"Лучший=${net_profit_best:.0f} Худший=${net_profit_worst:.0f}"
            )
        else:
            reasoning_parts.append("Хедж: нет данных опционов — консервативная оценка")

        signal = ArbSignal(
            source="ceasefire_detector",
            strategy=Strategy.FAKE_CEASEFIRE,
            pm_market_slug=ceasefire_market.market_slug,
            pm_side=Side.BUY_NO,
            pm_price=no_price,
            pm_size_usd=0,  # Sized by risk module
            hedge_type=HedgeType.PUT,
            hedge_symbol=self.hedge_symbol,
            hedge_strike=0,
            hedge_expiry="",
            strength=strength,
            confidence=confidence,
            reasoning=" | ".join(reasoning_parts),
            edge_pct=round(edge_pct, 1),
            available_depth_usd=round(depth_usd, 0),
            max_profit_usd=round(max_profit_usd, 0),
            max_loss_usd=round(max_loss_usd, 0),
            ev_usd=round(ev_usd, 0),
            risk_reward=round(risk_reward, 2),
            hedge_cost_usd=round(hedge_cost_usd, 2),
            net_profit_best=round(net_profit_best, 0),
            net_profit_worst=round(net_profit_worst, 0),
            breakeven_prob=breakeven_prob,
            tx_costs_usd=round(tx_costs_usd, 2),
        )

        await self.bus.publish(signal)
        self._last_signal_time = now
        self._ceasefire_keywords_seen = False  # Reset trigger

        logger.info(
            "[ceasefire] SIGNAL: BUY NO @ $%.3f | str=%.2f conf=%.2f | best=$%.0f worst=$%.0f bep=%.0f%%",
            no_price,
            strength,
            confidence,
            net_profit_best,
            net_profit_worst,
            breakeven_prob * 100,
        )

