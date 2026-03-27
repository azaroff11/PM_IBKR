"""
PMarb — Hormuz Definition Arbitrage.

Exploits the 80% PortWatch threshold:
Polymarket Hormuz contracts require 80% drop in transit per IMF data.
Real-world attacks may drop traffic 40-60% — enough for oil panic,
but NOT enough for PM to resolve YES.

We buy NO on PM + Call on Brent as hedge.

All-weather: Signal only emitted if net P&L > 0 in BOTH scenarios.
"""

from __future__ import annotations

import logging
import time

from src.analytics.pnl_validator import validate_signal as validate_pnl
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
    TradFiEvent,
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
        expected_move_pct: float = 0.15,  # 15% BNO rally on Hormuz disruption
    ) -> None:
        self.bus = bus
        self.pm_yes_threshold = pm_yes_threshold
        self.portwatch_threshold_pct = portwatch_threshold_pct
        self.ais_stale_days = ais_stale_days
        self.signal_cooldown_sec = signal_cooldown_sec
        self.hedge_symbol = hedge_symbol
        self.expected_move_pct = expected_move_pct

        # State
        self._last_signal_time: float = 0
        self._latest_portwatch: PortWatchEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}
        self._latest_tradfi: dict[str, TradFiEvent] = {}

        # Subscribe
        self.bus.subscribe(EventType.PORTWATCH, self._on_portwatch)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)
        self.bus.subscribe(EventType.TRADFI, self._on_tradfi)

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

    async def _on_tradfi(self, event: BaseEvent) -> None:
        """Track TradFi data for hedge pricing. Re-evaluate on first data."""
        assert isinstance(event, TradFiEvent)
        is_first = event.symbol not in self._latest_tradfi
        self._latest_tradfi[event.symbol] = event
        if is_first and self._latest_portwatch:
            self._last_signal_time = 0
            await self._evaluate()

    def _get_option_premium(self) -> tuple[float, float, float]:
        """Get best available CALL option premium for hedge.

        Returns: (premium_per_share, delta, spot_price)
        """
        tf = self._latest_tradfi.get(self.hedge_symbol)
        if not tf or tf.spot <= 0:
            return 0.0, 0.0, 0.0

        if tf.options:
            calls = [o for o in tf.options if o.right == "C" and (o.ask > 0 or o.bid > 0)]
            if calls:
                calls.sort(key=lambda o: abs(abs(o.delta) - 0.25))
                best = calls[0]
                price = best.ask if best.ask > 0 else best.bid * 1.05
                return price, abs(best.delta), tf.spot

        if tf.iv_atm > 0:
            import math
            t = 30 / 365
            premium = tf.spot * tf.iv_atm * math.sqrt(t) * 0.4
            return premium, 0.25, tf.spot

        return 0.0, 0.0, tf.spot

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
            strength = 0.8
            reasoning_parts.append("AIS ВЫПАДЕНИЕ: нет данных для подтверждения порога 80%")
            reasoning_parts.append(f"PM ДА=${yes_price:.3f} — толпа переоценивает риск блокады")

        elif actual_drop < self.portwatch_threshold_pct:
            gap = self.portwatch_threshold_pct - actual_drop
            strength = min(1.0, gap / 40.0)
            reasoning_parts.append(
                f"Падение трафика={actual_drop:.1f}% < порог={self.portwatch_threshold_pct}%"
            )
            reasoning_parts.append(f"Зазор до резолюции: {gap:.1f}пп")
            reasoning_parts.append(f"PM ДА=${yes_price:.3f} — миспрайсинг vs физ. данные")

        elif pw.data_freshness_days > self.ais_stale_days:
            strength = 0.6
            reasoning_parts.append(
                f"Устаревшие данные: {pw.data_freshness_days}д (порог={self.ais_stale_days}д)"
            )
            reasoning_parts.append("Устаревший PortWatch не может надёжно подтвердить ДА")

        else:
            logger.warning(
                "[hormuz] РЕАЛЬНАЯ БЛОКАДА ОБНАРУЖЕНА: падение=%.1f%% — сигнал НЕТ подавлен",
                actual_drop,
            )
            return

        # Confidence
        confidence = 0.7
        if pw and pw.ais_quality == "normal" and pw.data_freshness_days <= 3:
            confidence = 0.9
        elif pw and pw.ais_quality == "degraded":
            confidence = 0.6

        # ═══ CALCULATE INEFFICIENCY SIZE ═══
        real_prob = 0.10 if actual_drop < 60 else 0.30
        if pw and pw.ais_quality == "dropout":
            real_prob = 0.15
        edge_pct = (yes_price - real_prob) * 100

        depth_usd = hormuz_market.liquidity_depth or hormuz_market.volume_24h * 0.1

        from src.analytics.pnl_validator import DEFAULT_BUDGET_USD
        budget = DEFAULT_BUDGET_USD

        # ═══ ALL-WEATHER P&L VALIDATION ═══
        option_premium, option_delta, spot = self._get_option_premium()

        if option_premium > 0 and spot > 0:
            validation = validate_pnl(
                pm_side="buy_no",
                pm_price=no_price,
                pm_notional=0,
                hedge_type="call",
                option_premium=option_premium,
                option_delta=option_delta,
                expected_move_pct=self.expected_move_pct,
                spot_price=spot,
                pm_spread=hormuz_market.spread or 0.02,
            )

            hedge_cost_usd = validation.hedge_cost_usd
            net_profit_best = validation.best_case.net_pnl
            net_profit_worst = validation.worst_case.net_pnl
            breakeven_prob = validation.breakeven_prob
            tx_costs_usd = validation.tx_costs_usd

            max_profit_usd = validation.best_case.net_pnl
            max_loss_usd = abs(validation.worst_case.net_pnl)

            if not validation.is_valid:
                logger.info(
                    "[hormuz] Signal REJECTED by P&L validator: %s",
                    validation.rejection_reason,
                )
                strength *= 0.3

            reasoning_parts.append(
                f"Хедж CALL {self.hedge_symbol}: премия ${option_premium:.2f} | "
                f"Лучший=${net_profit_best:.0f} Худший=${net_profit_worst:.0f}"
            )
        else:
            profit_per_dollar = 1.0 - no_price
            max_profit_usd = profit_per_dollar * budget
            max_loss_usd = no_price * budget
            hedge_cost_usd = 0.0
            net_profit_best = max_profit_usd
            net_profit_worst = -max_loss_usd
            breakeven_prob = 0.5
            tx_costs_usd = 0.0
            reasoning_parts.append("Хедж: нет данных опционов — консервативная оценка")

        risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
        ev_usd = edge_pct / 100 * confidence * budget

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
            hedge_cost_usd=round(hedge_cost_usd, 2),
            net_profit_best=round(net_profit_best, 0),
            net_profit_worst=round(net_profit_worst, 0),
            breakeven_prob=breakeven_prob,
            tx_costs_usd=round(tx_costs_usd, 2),
        )

        await self.bus.publish(signal)
        self._last_signal_time = now

        logger.info(
            "[hormuz] SIGNAL: BUY NO @ $%.3f | str=%.2f conf=%.2f | best=$%.0f worst=$%.0f bep=%.0f%%",
            no_price,
            strength,
            confidence,
            net_profit_best,
            net_profit_worst,
            breakeven_prob * 100,
        )

