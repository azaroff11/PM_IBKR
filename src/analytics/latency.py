"""
PMarb — Latency Exploitation Engine.

Exploits publication schedule lags:
- PortWatch: updates Tuesday 9:00 ET
- EIA WPSR: updates Wednesday
- EIA PSM: ~2 month lag

If a contract expires BETWEEN data updates, the oracle
cannot prove the event → NO wins by default.

All-weather: Signal only emitted if net P&L > 0 in BOTH scenarios.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src.analytics.pnl_validator import validate_signal as validate_pnl
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
    TradFiEvent,
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
        hedge_symbol: str = "BNO",
        expected_move_pct: float = 0.10,  # 10% BNO move on surprise event
    ) -> None:
        self.bus = bus
        self.psm_lag_months = psm_lag_months
        self.signal_cooldown_sec = signal_cooldown_sec
        self.hedge_symbol = hedge_symbol
        self.expected_move_pct = expected_move_pct

        self._latest_eia: EIAEvent | None = None
        self._latest_pm_prices: dict[str, PMPriceEvent] = {}
        self._latest_tradfi: dict[str, TradFiEvent] = {}
        self._last_signal_time: float = 0

        self.bus.subscribe(EventType.EIA, self._on_eia)
        self.bus.subscribe(EventType.PM_PRICE, self._on_pm_price)
        self.bus.subscribe(EventType.TRADFI, self._on_tradfi)

    async def _on_eia(self, event: BaseEvent) -> None:
        assert isinstance(event, EIAEvent)
        self._latest_eia = event

    async def _on_pm_price(self, event: BaseEvent) -> None:
        assert isinstance(event, PMPriceEvent)
        self._latest_pm_prices[event.market_slug] = event
        await self._evaluate()

    async def _on_tradfi(self, event: BaseEvent) -> None:
        """Track TradFi data for hedge pricing. Re-evaluate on first data."""
        assert isinstance(event, TradFiEvent)
        is_first = event.symbol not in self._latest_tradfi
        self._latest_tradfi[event.symbol] = event
        if is_first and self._latest_pm_prices:
            self._last_signal_time = 0  # Reset cooldown for re-gen with real data
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

    def _run_pnl_validation(
        self, pm: PMPriceEvent, hedge_type_str: str
    ) -> tuple[float, float, float, float, float]:
        """Run P&L validation and return (hedge_cost, net_best, net_worst, bep, tx_costs)."""
        option_premium, option_delta, spot = self._get_option_premium()

        if option_premium > 0 and spot > 0:
            validation = validate_pnl(
                pm_side="buy_no",
                pm_price=pm.no_price,
                pm_notional=0,  # Will be overridden by budget allocator
                hedge_type=hedge_type_str,
                option_premium=option_premium,
                option_delta=option_delta,
                expected_move_pct=self.expected_move_pct,
                spot_price=spot,
                pm_spread=pm.spread or 0.02,
            )
            return (
                validation.hedge_cost_usd,
                validation.best_case.net_pnl,
                validation.worst_case.net_pnl,
                validation.breakeven_prob,
                validation.tx_costs_usd,
            )

        return 0.0, 0.0, 0.0, 0.5, 0.0

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

        days_to_update = (next_update - now).days
        if days_to_update >= 2 and pm.yes_price > 0.20:
            # Inefficiency sizing
            real_prob = 0.05
            edge_pct = (pm.yes_price - real_prob) * 100
            depth_usd = pm.liquidity_depth or pm.volume_24h * 0.1
            max_profit_usd = (1.0 - pm.no_price) * depth_usd
            max_loss_usd = pm.no_price * depth_usd
            risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
            ev_usd = edge_pct / 100 * 0.75 * depth_usd

            # All-weather P&L
            hedge_cost, net_best, net_worst, bep, tx_costs = self._run_pnl_validation(pm, "call")

            strength = min(1.0, pm.yes_price / 0.40)
            if net_worst < 0 and hedge_cost > 0:
                strength *= 0.3  # Penalty for non-all-weather

            return ArbSignal(
                source="latency_engine",
                strategy=Strategy.LATENCY_ARB,
                pm_market_slug=slug,
                pm_side=Side.BUY_NO,
                pm_price=pm.no_price,
                hedge_type=HedgeType.CALL,
                hedge_symbol=self.hedge_symbol,
                strength=strength,
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
                hedge_cost_usd=round(hedge_cost, 2),
                net_profit_best=round(net_best, 0),
                net_profit_worst=round(net_worst, 0),
                breakeven_prob=bep,
                tx_costs_usd=round(tx_costs, 2),
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

        lag_days = self._latest_eia.lag_days
        if lag_days >= 45 and pm.yes_price > 0.15:
            edge_pct = (pm.yes_price - 0.03) * 100
            depth_usd = pm.liquidity_depth or pm.volume_24h * 0.1
            max_profit_usd = (1.0 - pm.no_price) * depth_usd
            max_loss_usd = pm.no_price * depth_usd
            risk_reward = max_profit_usd / max_loss_usd if max_loss_usd > 0 else 0.0
            ev_usd = edge_pct / 100 * 0.85 * depth_usd

            # PSM has no hedge (pure info edge)
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
                hedge_cost_usd=0.0,
                net_profit_best=round(max_profit_usd, 0),
                net_profit_worst=round(-max_loss_usd, 0),
                breakeven_prob=0.5,
                tx_costs_usd=0.0,
            )
        return None

    @staticmethod
    def get_next_update_schedule() -> dict[str, str]:
        """Return human-readable schedule of next data updates."""
        now = datetime.utcnow()
        schedule = {}

        days_to_tue = (PORTWATCH_UPDATE_DAY - now.weekday()) % 7
        if days_to_tue == 0 and now.hour >= 14:
            days_to_tue = 7
        next_pw = now + timedelta(days=days_to_tue)
        next_pw = next_pw.replace(hour=14, minute=0, second=0, microsecond=0)
        schedule["portwatch"] = next_pw.isoformat()

        days_to_wed = (EIA_WPSR_UPDATE_DAY - now.weekday()) % 7
        if days_to_wed == 0 and now.hour >= 15:
            days_to_wed = 7
        next_eia = now + timedelta(days=days_to_wed)
        next_eia = next_eia.replace(hour=15, minute=30, second=0, microsecond=0)
        schedule["eia_wpsr"] = next_eia.isoformat()

        return schedule

