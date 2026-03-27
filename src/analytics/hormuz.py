"""
PMarb — Hormuz Definition Arbitrage.

Exploits the 80% PortWatch threshold:
Polymarket Hormuz contracts require 80% drop in transit per IMF data.
Real-world attacks may drop traffic 40-60% — enough for oil panic,
but NOT enough for PM to resolve YES.

We buy NO on PM + Call on Brent as hedge.
"""

from __future__ import annotations

import logging
import time

from src.event_bus import EventBus
from src.models.events import (
    ArbSignal,
    BaseEvent,
    EventType,
    HedgeType,
    PMPriceEvent,
    PortWatchEvent,
    Side,
    Strategy,
)

logger = logging.getLogger(__name__)


class HormuzArbEngine:
    """Detects Hormuz definition arbitrage opportunities."""

    def __init__(
        self,
        bus: EventBus,
        pm_yes_threshold: float = 0.30,
        portwatch_threshold_pct: float = 80.0,
        ais_stale_days: int = 7,
        signal_cooldown_sec: int = 600,
        hedge_symbol: str = "BNO",
    ) -> None:
        self.bus = bus
        self.pm_yes_threshold = pm_yes_threshold
        self.portwatch_threshold_pct = portwatch_threshold_pct
        self.ais_stale_days = ais_stale_days
        self.signal_cooldown_sec = signal_cooldown_sec
        self.hedge_symbol = hedge_symbol

        # State
        self._last_signal_time: float = 0
        self._latest_portwatch: PortWatchEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}

        # Subscribe
        self.bus.subscribe(EventType.PORTWATCH, self._on_portwatch)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)

    async def _on_portwatch(self, event: BaseEvent) -> None:
        assert isinstance(event, PortWatchEvent)
        self._latest_portwatch = event
        logger.info(
            "[hormuz] PortWatch update: drop=%.1f%% ais=%s fresh=%dd",
            event.pct_drop_vs_30d or 0,
            event.ais_quality,
            event.data_freshness_days,
        )
        await self._evaluate()

    async def _on_pm_price(self, event: BaseEvent) -> None:
        assert isinstance(event, PMPriceEvent)
        self._latest_pm_prices[event.market_slug] = event
        if self._latest_portwatch:
            await self._evaluate()

    async def _evaluate(self) -> None:
        now = time.time()
        if now - self._last_signal_time < self.signal_cooldown_sec:
            return

        # Find Hormuz market
        hormuz_market: PMPriceEvent | None = None
        for slug, pm in self._latest_pm_prices.items():
            if "hormuz" in slug.lower() or "strait" in slug.lower():
                hormuz_market = pm
                break

        if not hormuz_market:
            return

        pw = self._latest_portwatch
        yes_price = hormuz_market.yes_price
        no_price = hormuz_market.no_price

        # ═══ SIGNAL LOGIC ═══

        # Condition 1: PM YES pumped above threshold
        if yes_price < self.pm_yes_threshold:
            return

        # Condition 2: PortWatch data shows drop BELOW the 80% threshold
        actual_drop = pw.pct_drop_vs_30d if pw and pw.pct_drop_vs_30d is not None else 0

        # Three sub-scenarios that favor NO:
        reasoning_parts = []
        strength = 0.0

        if pw is None or pw.ais_quality == "dropout":
            # AIS dropout = no data = can't prove 80% → favors NO
            strength = 0.8
            reasoning_parts.append("AIS ВЫПАДЕНИЕ: нет данных для подтверждения порога 80%")
            reasoning_parts.append(f"PM ДА=${yes_price:.3f} — толпа переоценивает риск блокады")

        elif actual_drop < self.portwatch_threshold_pct:
            # Traffic dropped but NOT enough for 80%
            gap = self.portwatch_threshold_pct - actual_drop
            strength = min(1.0, gap / 40.0)  # 40% gap = max strength
            reasoning_parts.append(
                f"Падение трафика={actual_drop:.1f}% < порог={self.portwatch_threshold_pct}%"
            )
            reasoning_parts.append(f"Зазор до резолюции: {gap:.1f}пп")
            reasoning_parts.append(f"PM ДА=${yes_price:.3f} — миспрайсинг vs физ. данные")

        elif pw.data_freshness_days > self.ais_stale_days:
            # Data is stale — can't be used for resolution
            strength = 0.6
            reasoning_parts.append(
                f"Устаревшие данные: {pw.data_freshness_days}д (порог={self.ais_stale_days}д)"
            )
            reasoning_parts.append("Устаревший PortWatch не может надёжно подтвердить ДА")

        else:
            # Traffic actually dropped 80%+ AND data is fresh → DO NOT signal NO
            logger.warning(
                "[hormuz] РЕАЛЬНАЯ БЛОКАДА ОБНАРУЖЕНА: падение=%.1f%% — сигнал НЕТ подавлен",
                actual_drop,
            )
            return

        # Confidence
        confidence = 0.7
        if pw and pw.ais_quality == "normal" and pw.data_freshness_days <= 3:
            confidence = 0.9  # Fresh, reliable data showing sub-80% drop
        elif pw and pw.ais_quality == "degraded":
            confidence = 0.6

        # ═══ CALCULATE INEFFICIENCY SIZE ═══
        # Real prob of 80% threshold breach is very low even with partial blockade
        real_prob = 0.10 if actual_drop < 60 else 0.30
        if pw and pw.ais_quality == "dropout":
            real_prob = 0.15  # Uncertainty = slightly higher
        edge_pct = (yes_price - real_prob) * 100

        depth_usd = hormuz_market.liquidity_depth or hormuz_market.volume_24h * 0.1
        profit_per_dollar = 1.0 - no_price
        max_profit_usd = profit_per_dollar * depth_usd
        max_loss_usd = no_price * depth_usd
        risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
        ev_usd = edge_pct / 100 * confidence * depth_usd

        signal = ArbSignal(
            source="hormuz_arb",
            strategy=Strategy.HORMUZ_DEF_ARB,
            pm_market_slug=hormuz_market.market_slug,
            pm_side=Side.BUY_NO,
            pm_price=no_price,
            hedge_type=HedgeType.CALL,
            hedge_symbol=self.hedge_symbol,
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

        logger.info(
            "[hormuz] ⚡ SIGNAL: BUY NO @ $%.3f | strength=%.2f conf=%.2f | %s",
            no_price,
            strength,
            confidence,
            signal.reasoning[:120],
        )
