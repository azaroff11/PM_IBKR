"""
PMarb — Latency Exploitation Engine.

Exploits publication schedule lags:
- PortWatch: updates Tuesday 9:00 ET
- EIA WPSR: updates Wednesday
- EIA PSM: ~2 month lag

If a contract expires BETWEEN data updates, the oracle
cannot prove the event → NO wins by default.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src.event_bus import EventBus
from src.models.events import (
    ArbSignal,
    BaseEvent,
    EIAEvent,
    EventType,
    HedgeType,
    PMPriceEvent,
    Side,
    Strategy,
)

logger = logging.getLogger(__name__)

# Publication schedules (day of week: 0=Mon, 1=Tue, 2=Wed, ...)
PORTWATCH_UPDATE_DAY = 1  # Tuesday
EIA_WPSR_UPDATE_DAY = 2   # Wednesday


class LatencyEngine:
    """Detects latency arbitrage windows based on data publication schedules."""

    def __init__(
        self,
        bus: EventBus,
        psm_lag_months: int = 2,
        signal_cooldown_sec: int = 3600,
    ) -> None:
        self.bus = bus
        self.psm_lag_months = psm_lag_months
        self.signal_cooldown_sec = signal_cooldown_sec

        self._latest_eia: EIAEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}
        self._last_signal_time: float = 0

        self.bus.subscribe(EventType.EIA, self._on_eia)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)

    async def _on_eia(self, event: BaseEvent) -> None:
        assert isinstance(event, EIAEvent)
        self._latest_eia = event

    async def _on_pm_price(self, event: BaseEvent) -> None:
        assert isinstance(event, PMPriceEvent)
        self._latest_pm_prices[event.market_slug] = event
        await self._evaluate()

    async def _evaluate(self) -> None:
        import time
        now_ts = time.time()
        if now_ts - self._last_signal_time < self.signal_cooldown_sec:
            return

        now = datetime.utcnow()

        for slug, pm in self._latest_pm_prices.items():
            # Check PortWatch latency window
            pw_signal = self._check_portwatch_window(slug, pm, now)
            if pw_signal:
                await self.bus.publish(pw_signal)
                self._last_signal_time = now_ts

            # Check EIA PSM latency window
            psm_signal = self._check_psm_window(slug, pm, now)
            if psm_signal:
                await self.bus.publish(psm_signal)
                self._last_signal_time = now_ts

    def _check_portwatch_window(
        self, slug: str, pm: PMPriceEvent, now: datetime
    ) -> ArbSignal | None:
        """Check if a contract expires in the PortWatch data gap (Wed-Mon before Tuesday)."""
        if "hormuz" not in slug.lower():
            return None

        # Find next Tuesday
        days_until_tuesday = (PORTWATCH_UPDATE_DAY - now.weekday()) % 7
        if days_until_tuesday == 0 and now.hour >= 14:  # After 9:00 ET ≈ 14:00 UTC
            days_until_tuesday = 7
        next_update = now + timedelta(days=days_until_tuesday)

        # If it's Wednesday-Monday (data won't update until next Tuesday)
        # and YES price is high → aggressive NO opportunity
        days_to_update = (next_update - now).days
        if days_to_update >= 2 and pm.yes_price > 0.20:
            # Inefficiency sizing
            real_prob = 0.05  # During data blackout, oracle can't confirm → ~5% real chance
            edge_pct = (pm.yes_price - real_prob) * 100
            depth_usd = pm.liquidity_depth or pm.volume_24h * 0.1
            max_profit_usd = (1.0 - pm.no_price) * depth_usd
            max_loss_usd = pm.no_price * depth_usd
            risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
            ev_usd = edge_pct / 100 * 0.75 * depth_usd

            return ArbSignal(
                source="latency_engine",
                strategy=Strategy.LATENCY_ARB,
                pm_market_slug=slug,
                pm_side=Side.BUY_NO,
                pm_price=pm.no_price,
                hedge_type=HedgeType.CALL,
                hedge_symbol="BNO",
                strength=min(1.0, pm.yes_price / 0.40),
                confidence=0.75,
                reasoning=(
                    f"PortWatch пауза: {days_to_update}д до обновления | "
                    f"PM ДА=${pm.yes_price:.3f} во время блэкаута данных | "
                    f"Событие в паузе не может быть подтверждено оракулом"
                ),
                edge_pct=round(edge_pct, 1),
                available_depth_usd=round(depth_usd, 0),
                max_profit_usd=round(max_profit_usd, 0),
                max_loss_usd=round(max_loss_usd, 0),
                ev_usd=round(ev_usd, 0),
                risk_reward=round(risk_reward, 2),
            )
        return None

    def _check_psm_window(
        self, slug: str, pm: PMPriceEvent, now: datetime
    ) -> ArbSignal | None:
        """Check if a contract references export data that PSM can't provide yet."""
        if "export" not in slug.lower() and "oil" not in slug.lower():
            return None

        if not self._latest_eia or self._latest_eia.report_type != "psm":
            return None

        # PSM data is ~2 months behind
        lag_days = self._latest_eia.lag_days
        if lag_days >= 45 and pm.yes_price > 0.15:  # 45+ days lag
            # Inefficiency sizing
            edge_pct = (pm.yes_price - 0.03) * 100  # Near-zero real probability
            depth_usd = pm.liquidity_depth or pm.volume_24h * 0.1
            max_profit_usd = (1.0 - pm.no_price) * depth_usd
            max_loss_usd = pm.no_price * depth_usd
            risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
            ev_usd = edge_pct / 100 * 0.85 * depth_usd

            return ArbSignal(
                source="latency_engine",
                strategy=Strategy.LATENCY_ARB,
                pm_market_slug=slug,
                pm_side=Side.BUY_NO,
                pm_price=pm.no_price,
                hedge_type=HedgeType.NONE,
                strength=0.9,
                confidence=0.85,
                reasoning=(
                    f"EIA PSM лаг: {lag_days}д ({lag_days // 30} мес.) | "
                    f"Оракул не может подтвердить ДА без офиц. данных | "
                    f"PM ДА=${pm.yes_price:.3f} — толпа торгует заголовки, не факты"
                ),
                edge_pct=round(edge_pct, 1),
                available_depth_usd=round(depth_usd, 0),
                max_profit_usd=round(max_profit_usd, 0),
                max_loss_usd=round(max_loss_usd, 0),
                ev_usd=round(ev_usd, 0),
                risk_reward=round(risk_reward, 2),
            )
        return None

    @staticmethod
    def get_next_update_schedule() -> dict[str, str]:
        """Return human-readable schedule of next data updates."""
        now = datetime.utcnow()
        schedule = {}

        # Next PortWatch (Tuesday 14:00 UTC ≈ 9:00 ET)
        days_to_tue = (PORTWATCH_UPDATE_DAY - now.weekday()) % 7
        if days_to_tue == 0 and now.hour >= 14:
            days_to_tue = 7
        next_pw = now + timedelta(days=days_to_tue)
        next_pw = next_pw.replace(hour=14, minute=0, second=0, microsecond=0)
        schedule["portwatch"] = next_pw.isoformat()

        # Next EIA WPSR (Wednesday)
        days_to_wed = (EIA_WPSR_UPDATE_DAY - now.weekday()) % 7
        if days_to_wed == 0 and now.hour >= 15:
            days_to_wed = 7
        next_eia = now + timedelta(days=days_to_wed)
        next_eia = next_eia.replace(hour=15, minute=30, second=0, microsecond=0)
        schedule["eia_wpsr"] = next_eia.isoformat()

        return schedule
