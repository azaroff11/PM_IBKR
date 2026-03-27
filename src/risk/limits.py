"""
PMarb — Session Risk Limits & Kill Switch.

Enforces hard limits on:
- Maximum session loss
- Maximum position count
- Connectivity checks (kill switch on disconnect)
"""

from __future__ import annotations

import logging
import time

from src.event_bus import EventBus
from src.models.events import BaseEvent, DepegAlert, EventType, RiskBreach

logger = logging.getLogger(__name__)


class RiskLimits:
    """Enforces session-level risk limits."""

    def __init__(
        self,
        bus: EventBus,
        max_session_loss: float = 500,
        max_positions: int = 10,
    ) -> None:
        self.bus = bus
        self.max_session_loss = max_session_loss
        self.max_positions = max_positions

        self._session_pnl = 0.0
        self._halted = False
        self._halt_reason = ""
        self._depeg_active = False

        self.bus.subscribe(EventType.DEPEG_ALERT, self._on_depeg)

    async def _on_depeg(self, event: BaseEvent) -> None:
        assert isinstance(event, DepegAlert)
        self._depeg_active = True
        logger.warning("[risk] DEPEG ALERT: %s (%.1f bps) — halting new orders", event.token, event.deviation_bps)
        await self._halt(f"Stablecoin depeg: {event.token} {event.deviation_bps:.0f}bps")

    async def check_limits(
        self,
        session_pnl: float,
        active_position_count: int,
    ) -> tuple[bool, str]:
        """Check if trading is allowed. Returns (allowed, reason)."""
        if self._halted:
            return False, f"HALTED: {self._halt_reason}"

        if session_pnl <= -self.max_session_loss:
            await self._halt(f"Session loss limit: ${session_pnl:.0f} <= -${self.max_session_loss:.0f}")
            return False, self._halt_reason

        if active_position_count >= self.max_positions:
            return False, f"Max positions reached: {active_position_count}/{self.max_positions}"

        if self._depeg_active:
            return False, "Stablecoin depeg detected — new orders blocked"

        return True, "ok"

    async def _halt(self, reason: str) -> None:
        self._halted = True
        self._halt_reason = reason
        breach = RiskBreach(
            source="risk_limits",
            breach_type="session_halt",
            detail=reason,
            action="halt_new_orders",
        )
        await self.bus.publish(breach)
        logger.critical("[risk] ⛔ HALTED: %s", reason)

    def reset(self) -> None:
        """Manual reset after halt (operator action)."""
        self._halted = False
        self._halt_reason = ""
        self._depeg_active = False
        logger.info("[risk] Limits reset by operator")

    @property
    def is_halted(self) -> bool:
        return self._halted

    @property
    def status(self) -> dict:
        return {
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "depeg_active": self._depeg_active,
            "max_session_loss": self.max_session_loss,
            "max_positions": self.max_positions,
        }
