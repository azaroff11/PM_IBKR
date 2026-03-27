"""
PMarb — Polymarket Auto-Discovery Scanner.

Scans ALL active Polymarket markets via Gamma API to find
arbitrage-relevant pairs. Groups by category and ranks by
volume/liquidity for the dashboard.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"

# Categories that map to tradeable TradFi instruments
CROSS_MARKET_TAGS = {
    "oil": ["USO", "BNO", "CL", "BZ"],
    "crypto": ["BTC", "ETH"],
    "politics-us": ["SPY", "VIX"],
    "geopolitics": ["USO", "GLD", "VIX"],
    "economics": ["SPY", "TLT", "DXY"],
    "fed": ["TLT", "SPY", "GLD"],
    "china": ["FXI", "EEM"],
    "war": ["USO", "GLD", "VIX"],
    "iran": ["USO", "BNO", "GLD"],
    "middle-east": ["USO", "GLD"],
    "climate": ["USO", "UNG"],
    "sports": [],
    "entertainment": [],
    "science": [],
}

# Minimum thresholds for a market to be "arb-worthy"
MIN_VOLUME_24H = 10_000      # $10K daily volume
MIN_LIQUIDITY = 50_000       # $50K liquidity depth
MIN_YES_PRICE = 0.03         # Not dust/spam
MAX_YES_PRICE = 0.97         # Not already resolved


class MarketScanner:
    """Scans Polymarket for all active, liquid markets."""

    def __init__(
        self,
        min_volume_24h: float = MIN_VOLUME_24H,
        min_liquidity: float = MIN_LIQUIDITY,
    ) -> None:
        self.min_volume_24h = min_volume_24h
        self.min_liquidity = min_liquidity
        self._session: aiohttp.ClientSession | None = None
        self.markets: list[dict] = []
        self.last_scan: datetime | None = None

    async def scan(self) -> list[dict]:
        """Fetch all active markets, filter and rank them."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )

        all_markets: list[dict] = []

        # Paginate through all markets (Gamma API max 100 per page)
        for offset in range(0, 500, 100):
            try:
                async with self._session.get(
                    f"{GAMMA_API}/markets",
                    params={
                        "closed": "false",
                        "limit": 100,
                        "offset": offset,
                        "order": "volume24hr",
                        "ascending": "false",
                    },
                ) as resp:
                    resp.raise_for_status()
                    batch = await resp.json()

                if not batch:
                    break
                all_markets.extend(batch)
            except Exception:
                logger.exception("[scanner] Failed to fetch page offset=%d", offset)
                break

        # Parse and filter
        parsed = []
        for m in all_markets:
            try:
                prices_raw = m.get("outcomePrices", "")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) and prices_raw else []
                yes_price = float(prices[0]) if len(prices) > 0 else 0
                no_price = float(prices[1]) if len(prices) > 1 else 0

                volume_24h = float(m.get("volume24hr", 0) or 0)
                liquidity = float(m.get("liquidity", 0) or 0)
                volume_total = float(m.get("volume", 0) or 0)

                # Filter
                if volume_24h < self.min_volume_24h:
                    continue
                if liquidity < self.min_liquidity:
                    continue
                if yes_price < MIN_YES_PRICE or yes_price > MAX_YES_PRICE:
                    continue

                tags = m.get("tags", []) or []
                question = m.get("question", "")
                slug = m.get("slug", "")
                hedgeable = self._find_hedge_instruments(tags, slug, question)
                spread = abs(yes_price - (1 - no_price))

                parsed.append({
                    "slug": m.get("slug", ""),
                    "question": m.get("question", ""),
                    "condition_id": m.get("conditionId", ""),
                    "clob_token_ids": m.get("clobTokenIds", ""),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "spread": spread,
                    "volume_24h": volume_24h,
                    "volume_total": volume_total,
                    "liquidity": liquidity,
                    "tags": tags,
                    "hedge_instruments": hedgeable,
                    "has_hedge": len(hedgeable) > 0,
                    "end_date": m.get("endDate", ""),
                    "arb_score": self._calc_arb_score(
                        yes_price, volume_24h, liquidity, spread, len(hedgeable),
                    ),
                })
            except Exception:
                continue

        # Sort by arb score
        parsed.sort(key=lambda x: x["arb_score"], reverse=True)
        self.markets = parsed
        self.last_scan = datetime.utcnow()

        logger.info(
            "[scanner] Scan complete: %d markets found (%d with hedge pairs)",
            len(parsed),
            sum(1 for m in parsed if m["has_hedge"]),
        )
        return parsed

    def _find_hedge_instruments(self, tags: list[str], slug: str = "", question: str = "") -> list[str]:
        """Map market content to hedgeable TradFi instruments using keyword analysis."""
        text = f"{slug} {question}".lower()
        instruments: set[str] = set()

        # Keyword → instrument mapping
        keyword_map = {
            # Oil & Energy
            ("oil", "crude", "petroleum", "opec", "brent", "wti", "energia"): ["USO", "BNO", "CL"],
            ("hormuz", "strait", "blockade", "tanker"): ["USO", "BNO", "CL"],
            ("iran", "tehran", "khamenei", "persian"): ["USO", "GLD", "VIX"],
            # Geopolitics
            ("war", "invasion", "military", "strike", "bomb", "attack"): ["GLD", "VIX", "USO"],
            ("ceasefire", "peace", "truce", "negotiate"): ["USO", "GLD"],
            ("israel", "lebanon", "hezbollah", "hamas", "gaza"): ["GLD", "VIX"],
            ("russia", "ukraine", "putin", "zelensky"): ["VIX", "GLD", "UNG"],
            ("china", "taiwan", "beijing", "xi-jinping"): ["FXI", "EEM"],
            ("north-korea", "pyongyang", "kim-jong"): ["VIX", "GLD"],
            # US Politics & Economics
            ("trump", "biden", "president", "white-house"): ["SPY", "VIX"],
            ("fed", "interest-rate", "fomc", "powell"): ["TLT", "SPY", "GLD"],
            ("recession", "gdp", "inflation", "cpi"): ["SPY", "TLT"],
            ("tariff", "trade-war", "sanctions"): ["SPY", "EEM", "FXI"],
            ("election", "senate", "congress", "vote"): ["SPY", "VIX"],
            # Crypto
            ("bitcoin", "btc", "crypto"): ["BTC", "IBIT"],
            ("ethereum", "eth"): ["ETH", "ETHA"],
            ("solana", "sol"): ["SOL"],
            # Climate/Energy
            ("hurricane", "earthquake", "climate", "weather"): ["USO", "UNG"],
            ("nuclear", "atomic"): ["URA", "VIX"],
        }

        for keywords, symbols in keyword_map.items():
            if any(kw in text for kw in keywords):
                instruments.update(symbols)

        # Also check original tags if present
        for tag in tags:
            tag_lower = tag.lower()
            for category, symbols in CROSS_MARKET_TAGS.items():
                if category in tag_lower:
                    instruments.update(symbols)

        return sorted(instruments)

    def _calc_arb_score(
        self,
        yes_price: float,
        volume_24h: float,
        liquidity: float,
        spread: float,
        hedge_count: int,
    ) -> float:
        """Calculate an arbitrage opportunity score (0-100).

        Factors:
        - Volume: higher = more liquid/executable
        - Price positioning: mid-range (0.3-0.7) = more volatile
        - Hedge availability: more hedges = better risk management
        - Spread: lower = better execution
        """
        # Volume score (log scale, max at $1M/day)
        import math
        vol_score = min(math.log10(max(volume_24h, 1)) / 6, 1.0) * 30

        # Price volatility potential (bell curve centered at 0.5)
        price_vol = 1 - abs(yes_price - 0.5) * 2
        price_score = max(price_vol, 0) * 25

        # Liquidity score
        liq_score = min(math.log10(max(liquidity, 1)) / 6, 1.0) * 20

        # Hedge score
        hedge_score = min(hedge_count / 3, 1.0) * 15

        # Spread penalty (lower is better)
        spread_penalty = max(0, spread * 100) * 2  # -2 points per 1% spread

        return max(0, vol_score + price_score + liq_score + hedge_score - spread_penalty)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def get_summary(self) -> dict:
        """Summary for dashboard API."""
        return {
            "total_markets": len(self.markets),
            "hedgeable_markets": sum(1 for m in self.markets if m["has_hedge"]),
            "last_scan": self.last_scan.isoformat() if self.last_scan else None,
            "top_arb": self.markets[:20] if self.markets else [],
            "by_category": self._group_by_tag(),
        }

    def _group_by_tag(self) -> dict[str, int]:
        cats: dict[str, int] = {}
        for m in self.markets:
            for tag in m.get("tags", []) or ["other"]:
                cats[tag] = cats.get(tag, 0) + 1
        return dict(sorted(cats.items(), key=lambda x: -x[1]))
