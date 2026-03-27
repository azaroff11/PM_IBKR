"""
PMarb — Capital & Position Sizing.

Controls capital allocation considering:
- UMA dispute lockup (4-6 days)
- Kelly criterion for position sizing
- Maximum exposure limits
"""

from __future__ import annotations

import logging
import math

logger = logging.getLogger(__name__)


class CapitalManager:
    """Manages capital allocation and position sizing."""

    def __init__(
        self,
        total_capital_usd: float = 10000,
        max_locked_capital_usd: float = 5000,
        max_single_position_usd: float = 1000,
        lockup_buffer_days: int = 7,
    ) -> None:
        self.total_capital = total_capital_usd
        self.max_locked = max_locked_capital_usd
        self.max_single = max_single_position_usd
        self.lockup_buffer_days = lockup_buffer_days
        self._locked_capital = 0.0

    def update_locked(self, locked: float) -> None:
        """Update locked capital from OrderManager."""
        self._locked_capital = locked

    @property
    def available_capital(self) -> float:
        return max(0, self.total_capital - self._locked_capital)

    def can_open_position(self, size_usd: float) -> tuple[bool, str]:
        """Check if a new position can be opened."""
        if size_usd > self.available_capital:
            return False, f"Insufficient: need ${size_usd:.0f}, have ${self.available_capital:.0f}"

        if self._locked_capital + size_usd > self.max_locked:
            return False, f"Would exceed lockup limit: ${self._locked_capital + size_usd:.0f} > ${self.max_locked:.0f}"

        if size_usd > self.max_single:
            return False, f"Exceeds single position limit: ${size_usd:.0f} > ${self.max_single:.0f}"

        return True, "ok"

    def calculate_position_size(
        self,
        win_prob: float,
        win_payoff_ratio: float = 0.33,  # 33% for NO @ $0.75 → $1.00
        loss_ratio: float = 1.0,  # can lose 100% of PM leg
    ) -> float:
        """
        Kelly Criterion position sizing.

        f* = (p * b - q) / b
        where:
            p = probability of winning (NO resolves correctly)
            b = payoff ratio (net gain / bet)
            q = 1 - p

        Adjusted to half-Kelly for safety.
        """
        if win_prob <= 0 or win_prob >= 1:
            return 0

        q = 1 - win_prob
        b = win_payoff_ratio / loss_ratio if loss_ratio > 0 else 0

        if b <= 0:
            return 0

        kelly = (win_prob * b - q) / b
        kelly = max(0, kelly)

        # Half-Kelly for safety
        half_kelly = kelly / 2

        # Size in USD
        position_usd = self.available_capital * half_kelly
        position_usd = min(position_usd, self.max_single)

        logger.debug(
            "[capital] Kelly: p=%.2f b=%.2f → f*=%.3f half=%.3f → $%.0f",
            win_prob,
            b,
            kelly,
            half_kelly,
            position_usd,
        )

        return round(position_usd, 2)
