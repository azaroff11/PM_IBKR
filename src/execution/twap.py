"""
PMarb — TWAP Execution Algorithm.

Time-Weighted Average Price: splits large orders into
N smaller slices executed at regular intervals to minimize
slippage in thin Polymarket AMM pools.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class TWAPConfig:
    total_amount_usd: float
    n_slices: int = 5
    interval_sec: int = 60
    max_slippage_bps: int = 200  # 2%
    max_single_slice_usd: float = 500


@dataclass
class TWAPResult:
    status: str = "pending"  # pending, executing, completed, aborted
    slices_executed: int = 0
    total_filled_usd: float = 0.0
    avg_price: float = 0.0
    fills: list[dict] = field(default_factory=list)
    aborted_reason: str = ""


class TWAPExecutor:
    """Execute large orders via TWAP to minimize slippage."""

    def __init__(self, executor_fn=None) -> None:
        """
        Args:
            executor_fn: async callable(size_usd, max_price) -> dict with fill info
        """
        self.executor_fn = executor_fn
        self._active_twaps: dict[str, TWAPResult] = {}

    async def execute(
        self,
        twap_id: str,
        config: TWAPConfig,
        initial_price: float,
    ) -> TWAPResult:
        """
        Run TWAP execution loop.

        Splits config.total_amount_usd into config.n_slices,
        executing one per config.interval_sec.
        Aborts if price slips beyond max_slippage_bps.
        """
        result = TWAPResult()
        self._active_twaps[twap_id] = result

        slice_amount = config.total_amount_usd / config.n_slices
        slice_amount = min(slice_amount, config.max_single_slice_usd)
        max_acceptable_price = initial_price * (1 + config.max_slippage_bps / 10000)

        logger.info(
            "[twap:%s] Starting: $%.0f / %d slices @ %ds interval | max_price=$%.4f",
            twap_id,
            config.total_amount_usd,
            config.n_slices,
            config.interval_sec,
            max_acceptable_price,
        )

        result.status = "executing"

        for i in range(config.n_slices):
            if result.status == "aborted":
                break

            try:
                if self.executor_fn:
                    fill = await self.executor_fn(slice_amount, max_acceptable_price)
                else:
                    # Dry-run simulation
                    fill = {
                        "status": "dry_run",
                        "size_usd": slice_amount,
                        "price": initial_price,
                        "slice": i + 1,
                    }

                fill_price = fill.get("price", 0)
                fill_size = fill.get("size_usd", slice_amount)

                # Slippage check
                if fill_price > 0 and fill_price > max_acceptable_price:
                    result.status = "aborted"
                    result.aborted_reason = (
                        f"Slippage: fill_price=${fill_price:.4f} > max=${max_acceptable_price:.4f}"
                    )
                    logger.warning("[twap:%s] ABORTED: %s", twap_id, result.aborted_reason)
                    break

                result.slices_executed += 1
                result.total_filled_usd += fill_size
                result.fills.append(fill)

                # Update average price
                if result.total_filled_usd > 0 and fill_price > 0:
                    total_cost = sum(
                        f.get("price", 0) * f.get("size_usd", 0)
                        for f in result.fills
                        if f.get("price", 0) > 0
                    )
                    result.avg_price = total_cost / result.total_filled_usd

                logger.info(
                    "[twap:%s] Slice %d/%d: $%.0f @ $%.4f | total=$%.0f avg=$%.4f",
                    twap_id,
                    i + 1,
                    config.n_slices,
                    fill_size,
                    fill_price,
                    result.total_filled_usd,
                    result.avg_price,
                )

            except Exception:
                logger.exception("[twap:%s] Slice %d failed", twap_id, i + 1)

            # Wait between slices (except last)
            if i < config.n_slices - 1:
                await asyncio.sleep(config.interval_sec)

        if result.status != "aborted":
            result.status = "completed"

        logger.info(
            "[twap:%s] %s: %d/%d slices | $%.0f filled | avg=$%.4f",
            twap_id,
            result.status.upper(),
            result.slices_executed,
            config.n_slices,
            result.total_filled_usd,
            result.avg_price,
        )

        return result

    def abort(self, twap_id: str) -> None:
        """Abort a running TWAP execution."""
        if twap_id in self._active_twaps:
            self._active_twaps[twap_id].status = "aborted"
            self._active_twaps[twap_id].aborted_reason = "Manual abort"
