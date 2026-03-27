"""
PMarb — Sentiment Collector.

Monitors RSS feeds and keyword triggers for geopolitical events
that drive Polymarket pricing (Trump Truth Social, IRNA, etc.).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime

import aiohttp

from src.collectors.base import BaseCollector
from src.event_bus import EventBus
from src.models.events import BaseEvent, SentimentEvent

logger = logging.getLogger(__name__)


class SentimentCollector(BaseCollector):
    name = "sentiment"

    def __init__(
        self,
        bus: EventBus,
        rss_urls: list[str] | None = None,
        keywords: dict[str, list[str]] | None = None,
        poll_interval: int = 30,
    ) -> None:
        super().__init__(bus, poll_interval)
        self.rss_urls = rss_urls or []
        self.keywords = keywords or {
            "ceasefire": ["ceasefire", "peace deal", "truce", "negotiations", "postpone"],
            "escalation": ["strike", "attack", "blockade", "retaliation", "nuclear"],
            "iran_confirm": ["irna", "presstv", "tehran confirms", "iran agrees"],
        }
        self._seen_ids: set[str] = set()
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "PMarb/1.0 Sentiment Monitor"},
        )
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        if self._session:
            await self._session.close()

    async def poll(self) -> list[BaseEvent]:
        events: list[BaseEvent] = []

        for url in self.rss_urls:
            try:
                items = await self._fetch_rss(url)
                for item in items:
                    item_id = item.get("guid") or item.get("link") or item.get("title", "")
                    if item_id in self._seen_ids:
                        continue
                    self._seen_ids.add(item_id)

                    title = item.get("title", "")
                    description = item.get("description", "")
                    text = f"{title} {description}".lower()

                    matched = self._match_keywords(text)
                    if matched:
                        is_bilateral = self._check_bilateral(text)
                        event = SentimentEvent(
                            source=f"rss:{url[:50]}",
                            source_platform=self._detect_platform(url),
                            text=f"{title}: {description}"[:500],
                            keywords_matched=matched,
                            is_bilateral=is_bilateral,
                            author=item.get("author", ""),
                            url=item.get("link", ""),
                        )
                        events.append(event)
                        logger.info(
                            "[sentiment] MATCH: %s | keywords=%s bilateral=%s",
                            title[:80],
                            matched,
                            is_bilateral,
                        )
            except Exception:
                logger.exception("[sentiment] Failed to fetch RSS: %s", url[:60])

        return events

    async def _fetch_rss(self, url: str) -> list[dict[str, str]]:
        """Parse RSS/Atom feed into list of items."""
        assert self._session is not None
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            content = await resp.text()

        items: list[dict[str, str]] = []
        try:
            root = ET.fromstring(content)
            # RSS 2.0
            for item in root.iter("item"):
                entry = {}
                for child in item:
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    entry[tag] = (child.text or "").strip()
                items.append(entry)
            # Atom
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry_el in root.findall("atom:entry", ns):
                entry = {}
                title_el = entry_el.find("atom:title", ns)
                if title_el is not None:
                    entry["title"] = (title_el.text or "").strip()
                summary_el = entry_el.find("atom:summary", ns)
                if summary_el is not None:
                    entry["description"] = (summary_el.text or "").strip()
                link_el = entry_el.find("atom:link", ns)
                if link_el is not None:
                    entry["link"] = link_el.get("href", "")
                id_el = entry_el.find("atom:id", ns)
                if id_el is not None:
                    entry["guid"] = (id_el.text or "").strip()
                if entry:
                    items.append(entry)
        except ET.ParseError:
            logger.warning("[sentiment] Failed to parse RSS XML from %s", url[:60])

        return items

    def _match_keywords(self, text: str) -> list[str]:
        """Return all matched keyword categories."""
        matched = []
        for category, words in self.keywords.items():
            for word in words:
                if word.lower() in text:
                    matched.append(category)
                    break
        return matched

    def _check_bilateral(self, text: str) -> bool:
        """Check if text indicates bilateral (both sides) confirmation."""
        us_indicators = ["white house", "trump", "us confirms", "washington"]
        iran_indicators = ["iran confirms", "tehran", "irna", "khamenei", "presstv"]

        has_us = any(ind in text for ind in us_indicators)
        has_iran = any(ind in text for ind in iran_indicators)
        return has_us and has_iran

    def _detect_platform(self, url: str) -> str:
        if "truth" in url.lower():
            return "truth_social"
        if "twitter" in url.lower() or "nitter" in url.lower():
            return "twitter"
        if "irna" in url.lower():
            return "irna"
        if "presstv" in url.lower():
            return "presstv"
        return "rss"
