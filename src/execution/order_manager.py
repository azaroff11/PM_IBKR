"""
PMarb — Cross-Leg Order Manager.

Tracks arbitrage positions across Polymarket + TradFi.
Each position = PM leg + TradFi hedge leg.

Lifecycle: SIGNAL → PM_PENDING → PM_FILLED → TRADFI_PENDING →
           TRADFI_FILLED → ACTIVE → SETTLED
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

from src.models.events import ArbSignal, OrderStatus, OrderUpdate, Strategy

logger = logging.getLogger(__name__)


class Position:
    """Single cross-market arbitrage position."""

    def __init__(self, signal: ArbSignal) -> None:
        self.id = str(uuid.uuid4())[:8]
        self.created_at = datetime.utcnow()
        self.strategy = signal.strategy
        self.status = OrderStatus.SIGNAL

        # PM leg
        self.pm_market_slug = signal.pm_market_slug
        self.pm_side = signal.pm_side
        self.pm_target_price = signal.pm_price
        self.pm_fill_price = 0.0
        self.pm_fill_size = 0.0

        # TradFi leg
        self.hedge_type = signal.hedge_type
        self.hedge_symbol = signal.hedge_symbol
        self.hedge_strike = signal.hedge_strike
        self.hedge_expiry = signal.hedge_expiry
        self.tradfi_fill_price = 0.0
        self.tradfi_fill_size = 0.0

        # P&L
        self.total_cost = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        self.signal_strength = signal.strength
        self.signal_confidence = signal.confidence
        self.reasoning = signal.reasoning

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat(),
            "strategy": self.strategy.value,
            "status": self.status.value,
            "pm_market": self.pm_market_slug,
            "pm_side": self.pm_side.value if self.pm_side else "",
            "pm_target": self.pm_target_price,
            "pm_fill": self.pm_fill_price,
            "pm_size": self.pm_fill_size,
            "hedge_type": self.hedge_type.value,
            "hedge_symbol": self.hedge_symbol,
            "hedge_strike": self.hedge_strike,
            "tradfi_fill": self.tradfi_fill_price,
            "tradfi_size": self.tradfi_fill_size,
            "total_cost": self.total_cost,
            "pnl": self.unrealized_pnl,
            "strength": self.signal_strength,
            "confidence": self.signal_confidence,
        }


class OrderManager:
    """Manages cross-leg arbitrage positions with full audit trail."""

    def __init__(self, data_dir: str = "./data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._positions: dict[str, Position] = {}
        self._history: list[dict] = []
        self._load_history()

    def create_position(self, signal: ArbSignal) -> Position:
        """Create a new cross-leg position from a signal."""
        pos = Position(signal)
        self._positions[pos.id] = pos
        self._log_event(pos, "created", signal.reasoning)
        logger.info(
            "[orders] Position %s created: %s %s | PM=%s hedge=%s %s",
            pos.id,
            pos.strategy.value,
            pos.pm_side.value if pos.pm_side else "?",
            pos.pm_market_slug,
            pos.hedge_type.value,
            pos.hedge_symbol,
        )
        return pos

    def update_pm_fill(self, pos_id: str, fill_price: float, fill_size: float) -> None:
        """Record PM leg fill."""
        pos = self._positions.get(pos_id)
        if not pos:
            return
        pos.pm_fill_price = fill_price
        pos.pm_fill_size = fill_size
        pos.total_cost += fill_price * fill_size
        pos.status = OrderStatus.PM_FILLED
        self._log_event(pos, "pm_filled", f"price={fill_price} size={fill_size}")

    def update_tradfi_fill(self, pos_id: str, fill_price: float, fill_size: float) -> None:
        """Record TradFi hedge leg fill."""
        pos = self._positions.get(pos_id)
        if not pos:
            return
        pos.tradfi_fill_price = fill_price
        pos.tradfi_fill_size = fill_size
        pos.total_cost += fill_price * fill_size * 100  # Options multiplier
        pos.status = OrderStatus.ACTIVE
        self._log_event(pos, "tradfi_filled", f"price={fill_price} size={fill_size}")

    def settle_position(self, pos_id: str, pm_payout: float, tradfi_pnl: float) -> None:
        """Settle a completed position."""
        pos = self._positions.get(pos_id)
        if not pos:
            return
        pos.realized_pnl = (pm_payout - pos.pm_fill_price * pos.pm_fill_size) + tradfi_pnl
        pos.status = OrderStatus.SETTLED
        self._log_event(
            pos,
            "settled",
            f"pm_payout={pm_payout} tradfi_pnl={tradfi_pnl} total_pnl={pos.realized_pnl}",
        )

    def get_active_positions(self) -> list[Position]:
        return [p for p in self._positions.values() if p.status not in (OrderStatus.SETTLED, OrderStatus.FAILED, OrderStatus.CANCELLED)]

    def get_locked_capital(self) -> float:
        """Calculate total capital locked in active positions."""
        return sum(p.total_cost for p in self.get_active_positions())

    def get_all_positions(self) -> list[dict]:
        return [p.to_dict() for p in self._positions.values()]

    def _log_event(self, pos: Position, action: str, detail: str) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "position_id": pos.id,
            "action": action,
            "status": pos.status.value,
            "detail": detail,
        }
        self._history.append(event)
        self._save_history()

    def _save_history(self) -> None:
        path = self.data_dir / "positions.json"
        try:
            data = {
                "positions": {pid: p.to_dict() for pid, p in self._positions.items()},
                "history": self._history[-1000:],  # Keep last 1000 events
            }
            path.write_text(json.dumps(data, indent=2, default=str))
        except Exception:
            logger.exception("[orders] Failed to save history")

    def _load_history(self) -> None:
        path = self.data_dir / "positions.json"
        if path.exists():
            try:
                data = json.loads(path.read_text())
                self._history = data.get("history", [])
                logger.info("[orders] Loaded %d historical events", len(self._history))
            except Exception:
                logger.exception("[orders] Failed to load history")
