"""
PMarb — Polymarket CLOB Executor.

Manages order placement on Polymarket via py-clob-client.
Supports dry-run mode, FOK/GTC orders, and position building.
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.models.events import ArbSignal, Side

logger = logging.getLogger(__name__)


class PolymarketExecutor:
    """Execute trades on Polymarket CLOB (Polygon network)."""

    def __init__(
        self,
        private_key: str = "",
        wallet_address: str = "",
        clob_host: str = "https://clob.polymarket.com",
        chain_id: int = 137,
        rpc_url: str = "https://polygon-bor-rpc.publicnode.com",
        dry_run: bool = True,
    ) -> None:
        self.dry_run = dry_run
        self._private_key = private_key
        self._wallet_address = wallet_address
        self._clob_host = clob_host
        self._chain_id = chain_id
        self._rpc_url = rpc_url
        self._client = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize Polymarket CLOB client."""
        if not self._private_key or not self._wallet_address:
            logger.warning("[pm_exec] No wallet credentials — running in observe mode")
            return

        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                host=self._clob_host,
                chain_id=self._chain_id,
                key=self._private_key,
                signature_type=0,
                funder=self._wallet_address,
            )
            # Derive or load API keys
            self._client.set_api_creds(self._client.create_or_derive_api_creds())
            self._initialized = True
            logger.info("[pm_exec] Polymarket CLOB client initialized")
        except ImportError:
            logger.error("[pm_exec] py-clob-client not installed")
        except Exception:
            logger.exception("[pm_exec] Failed to initialize CLOB client")

    async def buy_no(
        self,
        signal: ArbSignal,
        size_usd: float,
        max_price: float = 0.95,
    ) -> dict:
        """
        Buy NO tokens for a Polymarket market.

        In CTF architecture, buying NO = buying the complement token.
        """
        if self.dry_run:
            result = {
                "status": "dry_run",
                "market": signal.pm_market_slug,
                "side": "NO",
                "size_usd": size_usd,
                "price": signal.pm_price,
                "max_price": max_price,
                "timestamp": datetime.utcnow().isoformat(),
            }
            logger.info("[pm_exec] DRY RUN: BUY NO $%.2f @ $%.3f | %s", size_usd, signal.pm_price, signal.pm_market_slug)
            return result

        if not self._initialized or not self._client:
            logger.error("[pm_exec] Client not initialized — cannot execute")
            return {"status": "error", "reason": "not_initialized"}

        try:
            # Calculate number of shares
            price = min(signal.pm_price, max_price)
            if price <= 0:
                return {"status": "error", "reason": "invalid_price"}

            shares = int(size_usd / price)
            if shares <= 0:
                return {"status": "error", "reason": "size_too_small"}

            # Place GTC order (NO side)
            # Note: on Polymarket, buying NO = selling YES equivalent
            order = self._client.create_order(
                token_id=signal.pm_market_slug,  # needs actual token_id
                price=price,
                size=shares,
                side="BUY",
            )
            result = self._client.post_order(order, order_type="GTC")

            logger.info(
                "[pm_exec] ORDER PLACED: BUY %d NO @ $%.3f | %s | result=%s",
                shares,
                price,
                signal.pm_market_slug,
                str(result)[:100],
            )

            return {
                "status": "submitted",
                "order_id": result.get("orderID", ""),
                "shares": shares,
                "price": price,
                "result": result,
            }

        except Exception as e:
            logger.exception("[pm_exec] Order failed")
            return {"status": "error", "reason": str(e)[:200]}

    async def get_positions(self) -> list[dict]:
        """Get current open positions on Polymarket."""
        if not self._initialized or not self._client:
            return []
        try:
            # py-clob-client doesn't have a direct positions endpoint
            # Would need to track locally or use subgraph
            return []
        except Exception:
            logger.exception("[pm_exec] Failed to fetch positions")
            return []
