"""
PMarb — EIA Data Collector.

Fetches US Energy Information Administration data:
- WPSR (Weekly Petroleum Status Report) — Wednesdays
- PSM (Petroleum Supply Monthly) — ~2 month lag

Key insight: PSM lag creates arb windows for contracts
with short expiry referencing export volumes.
"""

from __future__ import annotations

import logging
from datetime import datetime

import aiohttp

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, EIAEvent

logger = logging.getLogger(__name__)

EIA_API_BASE = "https://api.eia.gov/v2"


class EIACollector(BaseCollector):
    name = "eia"

    def __init__(
        self,
        bus: EventBus,
        api_key: str = "",
        poll_interval: int = 3600,  # 1 hour
    ) -> None:
        super().__init__(bus, poll_interval)
        self.api_key = api_key
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        if self._session:
            await self._session.close()

    async def poll(self) -> list[BaseEvent]:
        events: list[BaseEvent] = []

        # WPSR — Weekly crude stocks
        wpsr = await self._fetch_wpsr()
        if wpsr:
            events.append(wpsr)

        # PSM — Monthly (check what's latest available)
        psm = await self._fetch_psm()
        if psm:
            events.append(psm)

        return events

    async def _fetch_wpsr(self) -> EIAEvent | None:
        """Fetch Weekly Petroleum Status Report — crude stocks delta."""
        if not self.api_key:
            logger.debug("[eia] No API key — skipping WPSR")
            return None

        assert self._session is not None

        url = f"{EIA_API_BASE}/petroleum/sum/sndw/data/"
        params = {
            "api_key": self.api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPC0",  # Crude oil stocks
            "facets[process][]": "SAE",   # Ending stocks
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 2,
        }

        try:
            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            records = data.get("response", {}).get("data", [])
            if len(records) >= 2:
                latest_val = float(records[0].get("value", 0))
                prev_val = float(records[1].get("value", 0))
                delta = latest_val - prev_val
                period = records[0].get("period", "")

                report_date = None
                if period:
                    try:
                        report_date = datetime.strptime(period, "%Y-%m-%d")
                    except ValueError:
                        pass

                lag_days = (datetime.utcnow() - report_date).days if report_date else 0

                event = EIAEvent(
                    source="eia_wpsr",
                    report_type="wpsr",
                    crude_stocks_delta_mmbbl=delta / 1000,  # thousand barrels → million
                    report_date=report_date,
                    lag_days=lag_days,
                )
                logger.info(
                    "[eia] WPSR | stocks Δ=%.1f MMbbl | date=%s lag=%dd",
                    event.crude_stocks_delta_mmbbl,
                    period,
                    lag_days,
                )
                return event
        except Exception:
            logger.exception("[eia] WPSR fetch failed")

        return None

    async def _fetch_psm(self) -> EIAEvent | None:
        """Fetch Petroleum Supply Monthly — check latest available date."""
        if not self.api_key:
            return None

        assert self._session is not None

        url = f"{EIA_API_BASE}/petroleum/sup/sum/data/"
        params = {
            "api_key": self.api_key,
            "frequency": "monthly",
            "data[0]": "value",
            "facets[product][]": "EPC0",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 1,
        }

        try:
            async with self._session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            records = data.get("response", {}).get("data", [])
            if records:
                period = records[0].get("period", "")
                report_date = None
                if period:
                    try:
                        report_date = datetime.strptime(period, "%Y-%m")
                    except ValueError:
                        pass

                lag_days = (datetime.utcnow() - report_date).days if report_date else 0

                event = EIAEvent(
                    source="eia_psm",
                    report_type="psm",
                    report_date=report_date,
                    lag_days=lag_days,
                )
                logger.info(
                    "[eia] PSM | latest data=%s lag=%dd (%.0f months)",
                    period,
                    lag_days,
                    lag_days / 30,
                )
                return event

        except Exception:
            logger.exception("[eia] PSM fetch failed")

        return None
