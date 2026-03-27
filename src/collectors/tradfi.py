"""
PMarb — TradFi Collector (Interactive Brokers).

Uses ib_insync to connect to IB Gateway/TWS and fetch:
- Spot prices for USO, BNO, CL, BZ
- Options chain with IV, Greeks, bid/ask
- IV skew (25-delta put vs call)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, OptionData, TradFiEvent

logger = logging.getLogger(__name__)


class TradFiCollector(BaseCollector):
    name = "tradfi"

    def __init__(
        self,
        bus: EventBus,
        ibkr_host: str = "127.0.0.1",
        ibkr_port: int = 4002,
        ibkr_client_id: int = 1,
        instruments: list[dict] | None = None,
        options_config: dict | None = None,
        poll_interval: int = 30,
    ) -> None:
        super().__init__(bus, poll_interval)
        self.ibkr_host = ibkr_host
        self.ibkr_port = ibkr_port
        self.ibkr_client_id = ibkr_client_id
        self.instruments = instruments or [
            {"symbol": "USO", "exchange": "ARCA", "type": "stock"},
            {"symbol": "BNO", "exchange": "ARCA", "type": "stock"},
        ]
        self.options_config = options_config or {"target_delta": 0.25, "min_dte": 14, "max_dte": 90}
        self._ib = None
        self._connected = False

    async def start(self) -> None:
        await self._connect()
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        if self._ib and self._connected:
            self._ib.disconnect()
            logger.info("[tradfi] Disconnected from IBKR")

    async def _connect(self) -> None:
        """Connect to IB Gateway via ib_insync."""
        try:
            from ib_insync import IB
            self._ib = IB()
            await self._ib.connectAsync(
                host=self.ibkr_host,
                port=self.ibkr_port,
                clientId=self.ibkr_client_id,
            )
            self._connected = True
            logger.info(
                "[tradfi] Connected to IBKR at %s:%d (client=%d)",
                self.ibkr_host,
                self.ibkr_port,
                self.ibkr_client_id,
            )
        except ImportError:
            logger.error("[tradfi] ib_insync not installed — pip install ib_insync")
            self._connected = False
        except Exception:
            logger.exception("[tradfi] Failed to connect to IBKR")
            self._connected = False

    async def poll(self) -> list[BaseEvent]:
        if not self._connected or not self._ib:
            logger.debug("[tradfi] Not connected to IBKR — skipping poll")
            return []

        events: list[BaseEvent] = []

        for inst in self.instruments:
            try:
                event = await self._poll_instrument(inst)
                if event:
                    events.append(event)
            except Exception:
                logger.exception("[tradfi] Failed to poll %s", inst.get("symbol"))

        return events

    async def _poll_instrument(self, inst: dict) -> TradFiEvent | None:
        """Fetch spot + options data for a single instrument."""
        from ib_insync import Stock, Future, Option

        symbol = inst["symbol"]
        exchange = inst.get("exchange", "SMART")
        inst_type = inst.get("type", "stock")

        # Construct contract
        if inst_type == "stock":
            contract = Stock(symbol, exchange, "USD")
        elif inst_type == "future":
            contract = Future(symbol, exchange=exchange)
        else:
            return None

        # Qualify contract
        contracts = await self._ib.qualifyContractsAsync(contract)
        if not contracts:
            logger.warning("[tradfi] Could not qualify contract: %s", symbol)
            return None
        contract = contracts[0]

        # Request market data (streaming, not snapshot — paper trading often NaN on snapshots)
        self._ib.reqMarketDataType(3)  # 3 = Delayed data (works without live subscription)
        self._ib.reqMktData(contract, genericTickList="", snapshot=False)
        await asyncio.sleep(3)  # Wait for data to arrive

        ticker = self._ib.ticker(contract)

        # Robust spot extraction: try multiple sources
        import math
        spot = 0.0
        if ticker:
            for price_source in [
                ticker.marketPrice(),
                ticker.last,
                ticker.close,
                getattr(ticker, 'previousClose', None),
                (ticker.bid + ticker.ask) / 2 if ticker.bid and ticker.ask and
                    not math.isnan(ticker.bid) and not math.isnan(ticker.ask) and
                    ticker.bid > 0 and ticker.ask > 0 else None,
            ]:
                if price_source is not None and not math.isnan(price_source) and price_source > 0:
                    spot = float(price_source)
                    break

        if not spot or spot != spot:  # paranoid final NaN check
            spot = 0.0

        # Options chain (for stocks only)
        options_data: list[OptionData] = []
        iv_put_25d = 0.0
        iv_call_25d = 0.0
        iv_atm = 0.0

        if inst_type == "stock" and spot > 0:
            try:
                chains = await self._ib.reqSecDefOptParamsAsync(
                    contract.symbol, "", contract.secType, contract.conId
                )
                logger.info("[tradfi] %s options chains found: %d", symbol, len(chains) if chains else 0)
                if chains:
                    chain = chains[0]
                    target_delta = self.options_config.get("target_delta", 0.25)
                    min_dte = self.options_config.get("min_dte", 14)
                    max_dte = self.options_config.get("max_dte", 90)

                    # Filter expiries within DTE range
                    now = datetime.now()
                    valid_expiries = []
                    for exp in sorted(chain.expirations):
                        try:
                            exp_date = datetime.strptime(exp, "%Y%m%d")
                            dte = (exp_date - now).days
                            if min_dte <= dte <= max_dte:
                                valid_expiries.append(exp)
                        except ValueError:
                            continue

                    if valid_expiries:
                        target_exp = valid_expiries[0]
                        logger.info("[tradfi] %s using expiry %s (DTE %d), exchange=%s",
                                    symbol, target_exp, (datetime.strptime(target_exp, '%Y%m%d') - now).days,
                                    chain.exchange)

                        # Select strikes around ATM (6 each side to capture 25-delta)
                        strikes = sorted(chain.strikes)
                        atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
                        atm_strike = strikes[atm_idx]
                        put_strikes = strikes[max(0, atm_idx - 6) : atm_idx + 1]
                        call_strikes = strikes[atm_idx : min(len(strikes), atm_idx + 7)]
                        logger.info("[tradfi] %s ATM strike=%.1f, fetching %d puts + %d calls",
                                    symbol, atm_strike, len(put_strikes), len(call_strikes))

                        # Use SMART routing for options (chain.exchange like CBOE2/BATS often fails)
                        opt_exchange = "SMART"

                        # Fetch puts
                        for strike in put_strikes:
                            opt = Option(symbol, target_exp, strike, "P", opt_exchange)
                            opt_data = await self._get_option_data(opt, strike, target_exp, "P")
                            if opt_data:
                                options_data.append(opt_data)
                                if abs(opt_data.delta) > 0 and abs(abs(opt_data.delta) - target_delta) < 0.1:
                                    iv_put_25d = opt_data.iv

                        # Fetch calls
                        for strike in call_strikes:
                            opt = Option(symbol, target_exp, strike, "C", opt_exchange)
                            opt_data = await self._get_option_data(opt, strike, target_exp, "C")
                            if opt_data:
                                options_data.append(opt_data)
                                if abs(opt_data.delta) > 0 and abs(abs(opt_data.delta) - target_delta) < 0.1:
                                    iv_call_25d = opt_data.iv

                        # Fallback: if no exact 25d match, use closest OTM
                        if not iv_put_25d:
                            puts = [od for od in options_data if od.right == "P" and od.iv > 0 and od.strike < atm_strike]
                            if puts:
                                puts.sort(key=lambda o: abs(abs(o.delta) - target_delta))
                                iv_put_25d = puts[0].iv
                        if not iv_call_25d:
                            calls = [od for od in options_data if od.right == "C" and od.iv > 0 and od.strike > atm_strike]
                            if calls:
                                calls.sort(key=lambda o: abs(abs(o.delta) - target_delta))
                                iv_call_25d = calls[0].iv

                        # ATM IV = average of ATM call + ATM put IV
                        atm_ivs = [od.iv for od in options_data if od.strike == atm_strike and od.iv > 0]
                        if atm_ivs:
                            iv_atm = sum(atm_ivs) / len(atm_ivs)
                    else:
                        logger.warning("[tradfi] %s no valid expiries in DTE range %d-%d", symbol, min_dte, max_dte)

            except Exception:
                logger.exception("[tradfi] Options chain error for %s", symbol)

        event = TradFiEvent(
            source="ibkr",
            symbol=symbol,
            spot=spot,
            iv_put_25d=iv_put_25d,
            iv_call_25d=iv_call_25d,
            iv_atm=iv_atm,
            bid_ask_spread=(ticker.ask - ticker.bid) if ticker and ticker.ask and ticker.bid else 0.0,
            options=options_data,
        )

        logger.info(
            "[tradfi] %s | spot=$%.2f iv_atm=%.1f%% iv_put25d=%.1f%% iv_call25d=%.1f%% opts=%d",
            symbol,
            spot,
            iv_atm * 100,
            iv_put_25d * 100,
            iv_call_25d * 100,
            len(options_data),
        )

        return event

    async def _get_option_data(
        self, contract, strike: float, expiry: str, right: str
    ) -> OptionData | None:
        """Request market data for a single option contract."""
        try:
            qualified = await self._ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.info("[tradfi] Cannot qualify option: %s %s %s", strike, expiry, right)
                return None

            # Use delayed-frozen data (type 4) — most permissive for options
            self._ib.reqMarketDataType(4)
            self._ib.reqMktData(qualified[0], genericTickList="106", snapshot=False)
            await asyncio.sleep(2)  # Give time for greeks to arrive via streaming

            ticker = self._ib.ticker(qualified[0])
            if not ticker:
                return None

            # Try modelGreeks first, then lastGreeks, then computedGreeks
            greeks = ticker.modelGreeks or ticker.lastGreeks
            if not greeks and hasattr(ticker, 'computedGreeks'):
                greeks = ticker.computedGreeks

            import math
            iv = greeks.impliedVol if greeks and greeks.impliedVol and not math.isnan(greeks.impliedVol) else 0.0
            delta = greeks.delta if greeks and greeks.delta and not math.isnan(greeks.delta) else 0.0
            gamma = greeks.gamma if greeks and greeks.gamma and not math.isnan(greeks.gamma) else 0.0
            vega = greeks.vega if greeks and greeks.vega and not math.isnan(greeks.vega) else 0.0
            theta = greeks.theta if greeks and greeks.theta and not math.isnan(greeks.theta) else 0.0

            # Cancel streaming to avoid connection overload
            self._ib.cancelMktData(qualified[0])

            logger.info(
                "[tradfi] Option %s %s %s%s: iv=%.1f%% delta=%.3f greeks=%s",
                contract.symbol, expiry, right, strike,
                iv * 100, delta, 'yes' if greeks else 'no'
            )

            if iv <= 0 and delta == 0:
                return None

            return OptionData(
                strike=strike,
                expiry=expiry,
                right=right,
                iv=iv,
                delta=delta,
                gamma=gamma,
                vega=vega,
                theta=theta,
                bid=ticker.bid if ticker.bid and not math.isnan(ticker.bid) and ticker.bid > 0 else 0.0,
                ask=ticker.ask if ticker.ask and not math.isnan(ticker.ask) and ticker.ask > 0 else 0.0,
            )
        except Exception:
            logger.debug("[tradfi] Option data unavailable: %s %s %s", strike, expiry, right)
            return None
