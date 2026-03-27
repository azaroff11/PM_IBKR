"""
PMarb — IMF PortWatch Collector.

Scrapes IMF PortWatch ArcGIS endpoint for Strait of Hormuz
transit data. Calculates 7-day MA, % drop, AIS quality.

Key insight: data updates WEEKLY on Tuesdays 9:00 ET.
Contracts expiring between updates create arb windows.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import aiohttp

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, PortWatchEvent

logger = logging.getLogger(__name__)

# IMF PortWatch ArcGIS REST endpoint for chokepoint transit data
PORTWATCH_API = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
    "/Port_Watch_Daily_Chokepoint_Transit_Calls/FeatureServer/0/query"
)


class PortWatchCollector(BaseCollector):
    name = "portwatch"

    def __init__(
        self,
        bus: EventBus,
        chokepoint: str = "Strait of Hormuz",
        poll_interval: int = 21600,  # 6 hours
    ) -> None:
        super().__init__(bus, poll_interval)
        self.chokepoint = chokepoint
        self._session: aiohttp.ClientSession | None = None
        self._last_data_date: datetime | None = None

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
        assert self._session is not None

        try:
            records = await self._fetch_transit_data()
        except Exception:
            logger.exception("[portwatch] Failed to fetch data")
            return []

        if not records:
            logger.warning("[portwatch] No records returned for %s", self.chokepoint)
            return [
                PortWatchEvent(
                    source="portwatch",
                    chokepoint=self.chokepoint,
                    ais_quality="dropout",
                    data_freshness_days=999,
                )
            ]

        # Sort by date descending
        records.sort(key=lambda r: r.get("date", 0), reverse=True)

        # Calculate metrics
        latest = records[0]
        daily_transits = latest.get("total_calls", None)

        # 7-day MA
        recent_7 = records[:7]
        ma_7d = None
        if len(recent_7) >= 7:
            vals = [r.get("total_calls", 0) for r in recent_7 if r.get("total_calls") is not None]
            if vals:
                ma_7d = sum(vals) / len(vals)

        # 30-day baseline for % drop
        baseline_30 = records[7:37] if len(records) > 37 else records[7:]
        pct_drop = None
        if baseline_30 and ma_7d is not None:
            baseline_vals = [
                r.get("total_calls", 0)
                for r in baseline_30
                if r.get("total_calls") is not None
            ]
            if baseline_vals:
                baseline_avg = sum(baseline_vals) / len(baseline_vals)
                if baseline_avg > 0:
                    pct_drop = ((baseline_avg - ma_7d) / baseline_avg) * 100

        # Data freshness
        data_date = None
        freshness_days = 0
        latest_ts = latest.get("date")
        if latest_ts:
            # ArcGIS returns epoch milliseconds
            data_date = datetime.utcfromtimestamp(latest_ts / 1000) if latest_ts > 1e12 else datetime.utcfromtimestamp(latest_ts)
            freshness_days = (datetime.utcnow() - data_date).days

        # AIS quality assessment
        ais_quality = "normal"
        if freshness_days > 7:
            ais_quality = "degraded"
        if freshness_days > 14:
            ais_quality = "dropout"

        event = PortWatchEvent(
            source="portwatch",
            chokepoint=self.chokepoint,
            daily_transits=daily_transits,
            ma_7d=ma_7d,
            pct_drop_vs_30d=pct_drop,
            data_date=data_date,
            data_freshness_days=freshness_days,
            ais_quality=ais_quality,
        )

        logger.info(
            "[portwatch] %s | transits=%s ma7d=%s drop=%.1f%% fresh=%dd ais=%s",
            self.chokepoint,
            daily_transits,
            f"{ma_7d:.1f}" if ma_7d else "N/A",
            pct_drop or 0,
            freshness_days,
            ais_quality,
        )

        return [event]

    async def _fetch_transit_data(self) -> list[dict]:
        """Query ArcGIS FeatureServer for last 60 days of transit data."""
        assert self._session is not None

        # Request last 60 days of data for the target chokepoint
        cutoff = datetime.utcnow() - timedelta(days=60)
        cutoff_epoch = int(cutoff.timestamp() * 1000)

        params = {
            "where": f"chokepoint_name='{self.chokepoint}' AND date >= {cutoff_epoch}",
            "outFields": "date,total_calls,tanker_calls,cargo_calls,chokepoint_name",
            "orderByFields": "date DESC",
            "resultRecordCount": 60,
            "f": "json",
        }

        async with self._session.get(PORTWATCH_API, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        features = data.get("features", [])
        return [f.get("attributes", {}) for f in features]
