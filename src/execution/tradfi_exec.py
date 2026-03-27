"""
PMarb — TradFi Executor (Interactive Brokers).

Places options orders via ib_insync:
- OTM Puts on USO (ceasefire hedge)
- OTM Calls on BNO (Hormuz hedge)
- Futures options on CME WTI/Brent

Supports dry-run and paper trading modes.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from src.models.events import ArbSignal, HedgeType

logger = logging.getLogger(__name__)


class TradFiExecutor:
    """Execute options orders via Interactive Brokers."""

    def __init__(
        self,
        ibkr_host: str = "127.0.0.1",
        ibkr_port: int = 4002,
        ibkr_client_id: int = 2,  # Different from collector
        dry_run: bool = True,
    ) -> None:
        self.ibkr_host = ibkr_host
        self.ibkr_port = ibkr_port
        self.ibkr_client_id = ibkr_client_id
        self.dry_run = dry_run
        self._ib = None
        self._connected = False

    async def initialize(self) -> None:
        """Connect to IB Gateway."""
        if self.dry_run:
            logger.info("[tradfi_exec] DRY RUN mode — orders will be logged, not placed")
            return

        try:
            from ib_insync import IB

            self._ib = IB()
            await self._ib.connectAsync(
                host=self.ibkr_host,
                port=self.ibkr_port,
                clientId=self.ibkr_client_id,
            )
            self._connected = True
            logger.info("[tradfi_exec] Connected to IBKR (port=%d)", self.ibkr_port)
        except ImportError:
            logger.error("[tradfi_exec] ib_insync not installed")
        except Exception:
            logger.exception("[tradfi_exec] Failed to connect to IBKR")

    async def shutdown(self) -> None:
        if self._ib and self._connected:
            self._ib.disconnect()
            logger.info("[tradfi_exec] Disconnected from IBKR")

    async def execute_hedge(self, signal: ArbSignal, spot_price: float = 0) -> dict:
        """Execute the TradFi hedge leg based on signal."""
        if signal.hedge_type == HedgeType.PUT:
            return await self.buy_put(
                symbol=signal.hedge_symbol,
                spot=spot_price,
                strike_offset_pct=0.10,
                qty=signal.hedge_size or 1,
            )
        elif signal.hedge_type == HedgeType.CALL:
            return await self.buy_call(
                symbol=signal.hedge_symbol,
                spot=spot_price,
                strike_offset_pct=0.15,
                qty=signal.hedge_size or 1,
            )
        else:
            return {"status": "no_hedge", "reason": "hedge_type=NONE"}

    async def buy_put(
        self,
        symbol: str,
        spot: float,
        strike_offset_pct: float = 0.10,
        qty: int = 1,
        expiry: str = "",
    ) -> dict:
        """Buy OTM Put option."""
        strike = round(spot * (1 - strike_offset_pct), 0) if spot > 0 else 0

        if self.dry_run:
            result = {
                "status": "dry_run",
                "action": "BUY PUT",
                "symbol": symbol,
                "strike": strike,
                "expiry": expiry or "auto-select",
                "qty": qty,
                "spot": spot,
                "timestamp": datetime.utcnow().isoformat(),
            }
            logger.info(
                "[tradfi_exec] DRY RUN: BUY %d PUT %s strike=$%.0f (spot=$%.2f)",
                qty, symbol, strike, spot,
            )
            return result

        return await self._place_option_order(symbol, strike, "P", qty, expiry)

    async def buy_call(
        self,
        symbol: str,
        spot: float,
        strike_offset_pct: float = 0.15,
        qty: int = 1,
        expiry: str = "",
    ) -> dict:
        """Buy OTM Call option."""
        strike = round(spot * (1 + strike_offset_pct), 0) if spot > 0 else 0

        if self.dry_run:
            result = {
                "status": "dry_run",
                "action": "BUY CALL",
                "symbol": symbol,
                "strike": strike,
                "expiry": expiry or "auto-select",
                "qty": qty,
                "spot": spot,
                "timestamp": datetime.utcnow().isoformat(),
            }
            logger.info(
                "[tradfi_exec] DRY RUN: BUY %d CALL %s strike=$%.0f (spot=$%.2f)",
                qty, symbol, strike, spot,
            )
            return result

        return await self._place_option_order(symbol, strike, "C", qty, expiry)

    async def _place_option_order(
        self,
        symbol: str,
        strike: float,
        right: str,
        qty: int,
        expiry: str,
    ) -> dict:
        """Place an options order via IBKR."""
        if not self._connected or not self._ib:
            return {"status": "error", "reason": "not_connected"}

        try:
            from ib_insync import Option, LimitOrder, MarketOrder

            # Auto-select expiry if not specified
            if not expiry:
                expiry = await self._find_best_expiry(symbol, strike, right)
                if not expiry:
                    return {"status": "error", "reason": "no_suitable_expiry"}

            contract = Option(symbol, expiry, strike, right, "SMART", currency="USD")
            qualified = await self._ib.qualifyContractsAsync(contract)
            if not qualified:
                return {"status": "error", "reason": f"cannot_qualify_{symbol}_{strike}_{right}"}

            contract = qualified[0]

            # Get current market price for limit order
            self._ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(2)
            ticker = self._ib.ticker(contract)

            if ticker and ticker.ask and ticker.ask > 0:
                # Place limit order at mid-price
                mid = (ticker.bid + ticker.ask) / 2 if ticker.bid else ticker.ask
                order = LimitOrder("BUY", qty, round(mid, 2))
            else:
                # Fallback to market order
                order = MarketOrder("BUY", qty)

            trade = self._ib.placeOrder(contract, order)
            await asyncio.sleep(1)

            logger.info(
                "[tradfi_exec] ORDER PLACED: BUY %d %s %s $%.0f exp=%s | status=%s",
                qty, right, symbol, strike, expiry, trade.orderStatus.status,
            )

            return {
                "status": "submitted",
                "order_id": trade.order.orderId,
                "symbol": symbol,
                "strike": strike,
                "right": right,
                "expiry": expiry,
                "qty": qty,
                "order_status": trade.orderStatus.status,
            }

        except Exception as e:
            logger.exception("[tradfi_exec] Order failed")
            return {"status": "error", "reason": str(e)[:200]}

    async def _find_best_expiry(
        self, symbol: str, strike: float, right: str
    ) -> str | None:
        """Find the nearest expiry with 14-90 DTE."""
        if not self._ib:
            return None

        try:
            from ib_insync import Stock

            underlying = Stock(symbol, "SMART", "USD")
            qualified = await self._ib.qualifyContractsAsync(underlying)
            if not qualified:
                return None

            chains = await self._ib.reqSecDefOptParamsAsync(
                symbol, "", "STK", qualified[0].conId
            )
            if not chains:
                return None

            now = datetime.now()
            for chain in chains:
                for exp in sorted(chain.expirations):
                    try:
                        exp_date = datetime.strptime(exp, "%Y%m%d")
                        dte = (exp_date - now).days
                        if 14 <= dte <= 90 and strike in chain.strikes:
                            return exp
                    except ValueError:
                        continue

        except Exception:
            logger.exception("[tradfi_exec] Failed to find expiry for %s", symbol)

        return None

    async def get_option_chain(self, symbol: str) -> list[dict]:
        """Retrieve full options chain for a symbol."""
        if not self._connected or not self._ib:
            return []

        try:
            from ib_insync import Stock

            underlying = Stock(symbol, "SMART", "USD")
            qualified = await self._ib.qualifyContractsAsync(underlying)
            if not qualified:
                return []

            chains = await self._ib.reqSecDefOptParamsAsync(
                symbol, "", "STK", qualified[0].conId
            )
            result = []
            for chain in chains:
                result.append({
                    "exchange": chain.exchange,
                    "expirations": list(chain.expirations),
                    "strikes": list(chain.strikes),
                })
            return result
        except Exception:
            logger.exception("[tradfi_exec] Failed to get chain for %s", symbol)
            return []
