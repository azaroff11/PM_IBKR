"""
PMarb — Dashboard API Server.

FastAPI backend serving real-time system state:
- Data feed status
- Polymarket prices
- TradFi data
- Arb signals & positions
- Risk metrics

Runs alongside the main bot loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.signal_config import SignalConfig

from src.config import Settings, load_config
from src.event_bus import EventBus
from src.models.events import (
    ArbSignal,
    BaseEvent,
    DepegAlert,
    EIAEvent,
    EventType,
    PMPriceEvent,
    PortWatchEvent,
    RiskBreach,
    SentimentEvent,
    TradFiEvent,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════
# Global State Store (fed by EventBus)
# ═══════════════════════════════════════

class DashboardState:
    """In-memory state store aggregated from EventBus events."""

    def __init__(self) -> None:
        self.boot_time = datetime.utcnow()
        self.collectors: dict[str, dict] = {
            "sentiment": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
            "portwatch": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
            "eia": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
            "polymarket": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
            "tradfi": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
            "depeg": {"status": "idle", "last_poll": None, "events": 0, "last_data": None},
        }
        self.pm_prices: dict[str, dict] = {}
        self.tradfi_data: dict[str, dict] = {}
        self.portwatch_data: dict | None = None
        self.eia_data: dict | None = None
        self.sentiment_events: list[dict] = []
        self.signals: list[dict] = []
        self.positions: list[dict] = []
        self.risk: dict = {"halted": False, "depeg": False, "session_pnl": 0}
        self.event_log: list[dict] = []
        self._ws_clients: list[WebSocket] = []

    def snapshot(self) -> dict:
        """Full state snapshot for API."""
        return {
            "uptime_sec": (datetime.utcnow() - self.boot_time).total_seconds(),
            "timestamp": datetime.utcnow().isoformat(),
            "collectors": self.collectors,
            "pm_prices": self.pm_prices,
            "tradfi": self.tradfi_data,
            "portwatch": self.portwatch_data,
            "eia": self.eia_data,
            "sentiment": self.sentiment_events[-20:],
            "signals": self.signals[-50:],
            "positions": self.positions,
            "risk": self.risk,
            "event_log": self.event_log[-100:],
        }

    async def push_ws(self, data: dict) -> None:
        """Push update to all WebSocket clients."""
        dead = []
        for ws in self._ws_clients:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._ws_clients.remove(ws)


state = DashboardState()


# ═══════════════════════════════════════
# Event Handlers (subscribe to EventBus)
# ═══════════════════════════════════════

async def on_pm_price(event: BaseEvent) -> None:
    assert isinstance(event, PMPriceEvent)
    data = {
        "slug": event.market_slug,
        "yes": event.yes_price,
        "no": event.no_price,
        "vol24h": event.volume_24h,
        "spread": event.spread,
        "ts": event.timestamp.isoformat(),
    }
    state.pm_prices[event.market_slug] = data
    state.collectors["polymarket"]["status"] = "active"
    state.collectors["polymarket"]["events"] += 1
    state.collectors["polymarket"]["last_poll"] = event.timestamp.isoformat()
    state.collectors["polymarket"]["last_data"] = data
    await state.push_ws({"type": "pm_price", "data": data})


async def on_tradfi(event: BaseEvent) -> None:
    assert isinstance(event, TradFiEvent)
    data = {
        "symbol": event.symbol,
        "spot": event.spot,
        "iv_atm": event.iv_atm,
        "iv_put_25d": event.iv_put_25d,
        "iv_call_25d": event.iv_call_25d,
        "spread": event.bid_ask_spread,
        "options_count": len(event.options),
        "ts": event.timestamp.isoformat(),
    }
    state.tradfi_data[event.symbol] = data
    state.collectors["tradfi"]["status"] = "active"
    state.collectors["tradfi"]["events"] += 1
    state.collectors["tradfi"]["last_poll"] = event.timestamp.isoformat()
    state.collectors["tradfi"]["last_data"] = data
    await state.push_ws({"type": "tradfi", "data": data})


async def on_portwatch(event: BaseEvent) -> None:
    assert isinstance(event, PortWatchEvent)
    data = {
        "chokepoint": event.chokepoint,
        "daily_transits": event.daily_transits,
        "ma_7d": event.ma_7d,
        "pct_drop": event.pct_drop_vs_30d,
        "freshness_days": event.data_freshness_days,
        "ais_quality": event.ais_quality,
        "ts": event.timestamp.isoformat(),
    }
    state.portwatch_data = data
    state.collectors["portwatch"]["status"] = "active"
    state.collectors["portwatch"]["events"] += 1
    state.collectors["portwatch"]["last_poll"] = event.timestamp.isoformat()
    state.collectors["portwatch"]["last_data"] = data
    await state.push_ws({"type": "portwatch", "data": data})


async def on_eia(event: BaseEvent) -> None:
    assert isinstance(event, EIAEvent)
    data = {
        "report_type": event.report_type,
        "stocks_delta": event.crude_stocks_delta_mmbbl,
        "iran_export": event.iran_export_estimate_mbpd,
        "lag_days": event.lag_days,
        "ts": event.timestamp.isoformat(),
    }
    state.eia_data = data
    state.collectors["eia"]["status"] = "active"
    state.collectors["eia"]["events"] += 1
    state.collectors["eia"]["last_poll"] = event.timestamp.isoformat()
    await state.push_ws({"type": "eia", "data": data})


async def on_sentiment(event: BaseEvent) -> None:
    assert isinstance(event, SentimentEvent)
    data = {
        "platform": event.source_platform,
        "text": event.text[:200],
        "keywords": event.keywords_matched,
        "bilateral": event.is_bilateral,
        "ts": event.timestamp.isoformat(),
    }
    state.sentiment_events.append(data)
    state.sentiment_events = state.sentiment_events[-50:]
    state.collectors["sentiment"]["status"] = "active"
    state.collectors["sentiment"]["events"] += 1
    state.collectors["sentiment"]["last_poll"] = event.timestamp.isoformat()
    await state.push_ws({"type": "sentiment", "data": data})


async def on_signal(event: BaseEvent) -> None:
    assert isinstance(event, ArbSignal)

    # Gate: check if strategy is enabled
    cfg = SignalConfig()
    if not cfg.is_strategy_enabled(event.strategy.value):
        logger.debug("[signal] Dropped %s — strategy disabled", event.strategy.value)
        return

    # Gate: check signal filters
    filters = cfg.get_all().get("filters", {})
    if event.edge_pct < filters.get("min_edge_pct", 0):
        return
    if event.strength < filters.get("min_strength", 0):
        return
    if event.confidence < filters.get("min_confidence", 0):
        return

    data = {
        "strategy": event.strategy.value,
        "pm_slug": event.pm_market_slug,
        "pm_side": event.pm_side.value,
        "pm_price": event.pm_price,
        "hedge_type": event.hedge_type.value,
        "hedge_symbol": event.hedge_symbol,
        "strength": event.strength,
        "confidence": event.confidence,
        "reasoning": event.reasoning,
        "edge_pct": event.edge_pct,
        "available_depth_usd": event.available_depth_usd,
        "max_profit_usd": event.max_profit_usd,
        "max_loss_usd": event.max_loss_usd,
        "ev_usd": event.ev_usd,
        "risk_reward": event.risk_reward,
        "ts": event.timestamp.isoformat(),
    }
    state.signals.append(data)
    state.signals = state.signals[-100:]
    await state.push_ws({"type": "signal", "data": data})


async def on_depeg(event: BaseEvent) -> None:
    assert isinstance(event, DepegAlert)
    state.risk["depeg"] = True
    state.collectors["depeg"]["status"] = "alert"
    state.collectors["depeg"]["events"] += 1
    await state.push_ws({"type": "depeg_alert", "data": {"token": event.token, "bps": event.deviation_bps}})


async def on_risk_breach(event: BaseEvent) -> None:
    assert isinstance(event, RiskBreach)
    state.risk["halted"] = True
    await state.push_ws({"type": "risk_breach", "data": {"type": event.breach_type, "detail": event.detail}})


def register_handlers(bus: EventBus) -> None:
    """Register all dashboard event handlers on the bus."""
    bus.subscribe(EventType.PM_PRICE, on_pm_price)
    bus.subscribe(EventType.TRADFI, on_tradfi)
    bus.subscribe(EventType.PORTWATCH, on_portwatch)
    bus.subscribe(EventType.EIA, on_eia)
    bus.subscribe(EventType.SENTIMENT, on_sentiment)
    bus.subscribe(EventType.SIGNAL, on_signal)
    bus.subscribe(EventType.DEPEG_ALERT, on_depeg)
    bus.subscribe(EventType.RISK_BREACH, on_risk_breach)


# ═══════════════════════════════════════
# FastAPI App
# ═══════════════════════════════════════

app = FastAPI(title="PMarb Dashboard", version="0.1.0")


@app.get("/")
async def index():
    """Serve the dashboard HTML."""
    html_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if html_path.exists():
        return FileResponse(html_path, media_type="text/html")
    return HTMLResponse("<h1>PMarb Dashboard</h1><p>dashboard/index.html not found</p>")


def _sanitize(obj):
    """Replace NaN/Inf floats with None for JSON safety."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


@app.get("/api/state")
async def get_state():
    """Full state snapshot."""
    return _sanitize(state.snapshot())


@app.get("/api/signals")
async def get_signals():
    return {"signals": state.signals[-50:]}


@app.get("/api/positions")
async def get_positions():
    return {"positions": state.positions}


@app.get("/api/risk")
async def get_risk():
    return state.risk


@app.get("/api/scanner")
async def get_scanner():
    """Market scanner results — all discovered PM markets ranked by arb score."""
    scanner = getattr(state, "_scanner", None)
    if scanner is None:
        return {"error": "Scanner not initialized", "markets": []}
    summary = scanner.get_summary()
    return _sanitize(summary)


@app.get("/api/config")
async def get_config():
    """Parser configuration — keyword maps, thresholds, scoring."""
    from src.collectors.scanner import MIN_VOLUME_24H, MIN_LIQUIDITY, MIN_YES_PRICE, MAX_YES_PRICE

    scanner = getattr(state, "_scanner", None)
    markets = scanner.markets if scanner else []

    # Category icons (clean, no emoji — DeltaZero style)
    icons = {
        "Нефть": "◆", "Ормуз": "◆", "Иран": "◆", "Война": "◆",
        "Перемирие": "◆", "Израиль": "◆", "Россия/Украина": "◆",
        "Китай": "◆", "КНДР": "◆", "US Политика": "◆", "ФРС": "◆",
        "Экономика": "◆", "Тарифы": "◆", "Выборы": "◆",
        "BTC": "◆", "ETH": "◆", "SOL": "◆",
        "Климат": "◆", "Ядерное": "◆",
    }

    keyword_map_ru = [
        {"name": "Нефть", "keywords": ["oil", "crude", "petroleum", "opec", "brent", "wti"], "hedges": ["USO", "BNO", "CL"]},
        {"name": "Ормуз", "keywords": ["hormuz", "strait", "blockade", "tanker"], "hedges": ["USO", "BNO", "CL"]},
        {"name": "Иран", "keywords": ["iran", "tehran", "khamenei", "persian"], "hedges": ["USO", "GLD", "VIX"]},
        {"name": "Война", "keywords": ["war", "invasion", "military", "strike", "bomb", "attack"], "hedges": ["GLD", "VIX", "USO"]},
        {"name": "Перемирие", "keywords": ["ceasefire", "peace", "truce", "negotiate"], "hedges": ["USO", "GLD"]},
        {"name": "Израиль", "keywords": ["israel", "lebanon", "hezbollah", "hamas", "gaza"], "hedges": ["GLD", "VIX"]},
        {"name": "Россия/Украина", "keywords": ["russia", "ukraine", "putin", "zelensky"], "hedges": ["VIX", "GLD", "UNG"]},
        {"name": "Китай", "keywords": ["china", "taiwan", "beijing", "xi-jinping"], "hedges": ["FXI", "EEM"]},
        {"name": "КНДР", "keywords": ["north-korea", "pyongyang", "kim-jong"], "hedges": ["VIX", "GLD"]},
        {"name": "US Политика", "keywords": ["trump", "biden", "president", "white-house"], "hedges": ["SPY", "VIX"]},
        {"name": "ФРС", "keywords": ["fed", "interest-rate", "fomc", "powell"], "hedges": ["TLT", "SPY", "GLD"]},
        {"name": "Экономика", "keywords": ["recession", "gdp", "inflation", "cpi"], "hedges": ["SPY", "TLT"]},
        {"name": "Тарифы", "keywords": ["tariff", "trade-war", "sanctions"], "hedges": ["SPY", "EEM", "FXI"]},
        {"name": "Выборы", "keywords": ["election", "senate", "congress", "vote"], "hedges": ["SPY", "VIX"]},
        {"name": "BTC", "keywords": ["bitcoin", "btc", "crypto"], "hedges": ["BTC", "IBIT"]},
        {"name": "ETH", "keywords": ["ethereum", "eth"], "hedges": ["ETH", "ETHA"]},
        {"name": "SOL", "keywords": ["solana", "sol"], "hedges": ["SOL"]},
        {"name": "Климат", "keywords": ["hurricane", "earthquake", "climate", "weather"], "hedges": ["USO", "UNG"]},
        {"name": "Ядерное", "keywords": ["nuclear", "atomic"], "hedges": ["URA", "VIX"]},
    ]

    # Count matches per category
    for cat in keyword_map_ru:
        cat["icon"] = icons.get(cat["name"], "📌")
        count = 0
        for m in markets:
            text = f"{m.get('slug', '')} {m.get('question', '')}".lower()
            if any(kw in text for kw in cat["keywords"]):
                count += 1
        cat["matches"] = count

    return {
        "categories": keyword_map_ru,
        "thresholds": {
            "min_volume_24h": MIN_VOLUME_24H,
            "min_liquidity": MIN_LIQUIDITY,
            "min_yes_price": MIN_YES_PRICE,
            "max_yes_price": MAX_YES_PRICE,
            "max_pages": 5,
            "markets_per_page": 100,
        },
        "scoring": {
            "volume_weight": 30,
            "price_weight": 25,
            "liquidity_weight": 20,
            "hedge_weight": 15,
            "spread_penalty": 2,
        },
        "total_markets": len(markets),
        "total_hedgeable": sum(1 for m in markets if m.get("has_hedge")),
    }


@app.get("/api/signal-settings")
async def get_signal_settings():
    """Get current signal configuration."""
    cfg = SignalConfig()
    return cfg.get_all()


@app.post("/api/signal-settings")
async def update_signal_settings(request: Request):
    """Update signal configuration and persist to disk."""
    body = await request.json()
    cfg = SignalConfig()
    cfg.update(body)

    # Apply to running engines if available
    _apply_config_to_engines(cfg)

    return {"status": "ok", "config": cfg.get_all()}


def _apply_config_to_engines(cfg: SignalConfig) -> None:
    """Push updated config values to running engine instances."""
    # Ceasefire
    engine = getattr(state, "_ceasefire", None)
    if engine:
        engine.pm_yes_threshold = cfg.get("ceasefire", "pm_yes_threshold", 0.15)
        engine.signal_cooldown_sec = cfg.get("ceasefire", "signal_cooldown_sec", 300)
        engine.hedge_symbol = cfg.get("ceasefire", "hedge_symbol", "USO")
        engine.hedge_strike_offset_pct = cfg.get("ceasefire", "hedge_strike_offset_pct", 0.10)

    # Hormuz
    engine = getattr(state, "_hormuz", None)
    if engine:
        engine.pm_yes_threshold = cfg.get("hormuz", "pm_yes_threshold", 0.30)
        engine.portwatch_threshold_pct = cfg.get("hormuz", "portwatch_threshold_pct", 80.0)
        engine.ais_stale_days = cfg.get("hormuz", "ais_stale_days", 7)
        engine.signal_cooldown_sec = cfg.get("hormuz", "signal_cooldown_sec", 600)
        engine.hedge_symbol = cfg.get("hormuz", "hedge_symbol", "BNO")

    # Latency
    engine = getattr(state, "_latency", None)
    if engine:
        engine._psm_lag_months = cfg.get("latency", "psm_lag_months", 2)
        engine.signal_cooldown_sec = cfg.get("latency", "signal_cooldown_sec", 3600)

    logger.info("[config] Applied signal settings to running engines")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time updates via WebSocket."""
    await ws.accept()
    state._ws_clients.append(ws)
    try:
        # Send initial state
        await ws.send_json({"type": "init", "data": state.snapshot()})
        while True:
            # Keep connection alive, listen for commands
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        if ws in state._ws_clients:
            state._ws_clients.remove(ws)
