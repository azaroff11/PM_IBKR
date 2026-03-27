"""
PMarb — Shared Pydantic event models.

All inter-module communication flows through typed events
published on the EventBus.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ═══════════════════════════════════════
# Base
# ═══════════════════════════════════════

class EventType(str, Enum):
    # Collectors
    SENTIMENT = "sentiment"
    PORTWATCH = "portwatch"
    EIA = "eia"
    PM_PRICE = "pm_price"
    TRADFI = "tradfi"

    # Analytics
    SIGNAL = "signal"

    # Risk
    DEPEG_ALERT = "depeg_alert"
    RISK_BREACH = "risk_breach"

    # Execution
    ORDER_UPDATE = "order_update"


class BaseEvent(BaseModel):
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = ""


# ═══════════════════════════════════════
# Collector Events
# ═══════════════════════════════════════

class SentimentEvent(BaseEvent):
    event_type: EventType = EventType.SENTIMENT
    source_platform: str  # "truth_social", "twitter", "irna"
    text: str
    keywords_matched: list[str] = Field(default_factory=list)
    is_bilateral: bool = False  # True if both sides confirmed
    author: str = ""
    url: str = ""


class PortWatchEvent(BaseEvent):
    event_type: EventType = EventType.PORTWATCH
    chokepoint: str = "Strait of Hormuz"
    daily_transits: float | None = None
    ma_7d: float | None = None
    pct_drop_vs_30d: float | None = None
    data_date: datetime | None = None
    data_freshness_days: int = 0
    ais_quality: str = "normal"  # "normal", "degraded", "dropout"


class EIAEvent(BaseEvent):
    event_type: EventType = EventType.EIA
    report_type: str  # "wpsr", "psm"
    crude_stocks_delta_mmbbl: float | None = None
    iran_export_estimate_mbpd: float | None = None
    report_date: datetime | None = None
    lag_days: int = 0


class PMPriceEvent(BaseEvent):
    event_type: EventType = EventType.PM_PRICE
    market_slug: str
    condition_id: str = ""
    yes_price: float = 0.0
    no_price: float = 0.0
    volume_24h: float = 0.0
    spread: float = 0.0
    liquidity_depth: float = 0.0


class OptionData(BaseModel):
    strike: float
    expiry: str
    right: str  # "P" or "C"
    iv: float = 0.0
    delta: float = 0.0
    gamma: float = 0.0
    vega: float = 0.0
    theta: float = 0.0
    bid: float = 0.0
    ask: float = 0.0


class TradFiEvent(BaseEvent):
    event_type: EventType = EventType.TRADFI
    symbol: str
    spot: float = 0.0
    iv_put_25d: float = 0.0
    iv_call_25d: float = 0.0
    iv_atm: float = 0.0
    bid_ask_spread: float = 0.0
    options: list[OptionData] = Field(default_factory=list)


# ═══════════════════════════════════════
# Signal / Analytics Events
# ═══════════════════════════════════════

class Strategy(str, Enum):
    FAKE_CEASEFIRE = "fake_ceasefire"
    HORMUZ_DEF_ARB = "hormuz_definition_arb"
    LATENCY_ARB = "latency_arb"
    STRIKE_LAG = "strike_lag"


class Side(str, Enum):
    BUY_YES = "buy_yes"
    BUY_NO = "buy_no"


class HedgeType(str, Enum):
    PUT = "put"
    CALL = "call"
    FUTURE = "future"
    NONE = "none"


class ArbSignal(BaseEvent):
    event_type: EventType = EventType.SIGNAL
    strategy: Strategy
    # PM leg
    pm_market_slug: str
    pm_side: Side
    pm_price: float  # entry price for the side we're buying
    pm_size_usd: float = 0.0
    # TradFi hedge leg
    hedge_type: HedgeType = HedgeType.NONE
    hedge_symbol: str = ""
    hedge_strike: float = 0.0
    hedge_expiry: str = ""
    hedge_size: int = 0  # contracts
    # Signal quality
    strength: float = 0.0  # 0.0 - 1.0
    confidence: float = 0.0  # 0.0 - 1.0
    reasoning: str = ""
    # Inefficiency sizing
    edge_pct: float = 0.0          # Mispricing as % (e.g., 35.0 = 35% edge)
    available_depth_usd: float = 0.0  # Liquidity at current price level
    max_profit_usd: float = 0.0    # edge × depth = max extractable profit
    max_loss_usd: float = 0.0      # Capital at risk if position goes to $0
    ev_usd: float = 0.0            # Expected value = edge × confidence × depth
    risk_reward: float = 0.0       # max_profit / max_loss (>1 = favorable)


# ═══════════════════════════════════════
# Risk Events
# ═══════════════════════════════════════

class DepegAlert(BaseEvent):
    event_type: EventType = EventType.DEPEG_ALERT
    token: str  # "USDC", "USDT"
    deviation_bps: float = 0.0
    pool_tvl: float = 0.0


class RiskBreach(BaseEvent):
    event_type: EventType = EventType.RISK_BREACH
    breach_type: str  # "session_loss", "capital_lockup", "max_positions"
    detail: str = ""
    action: str = "halt_new_orders"


# ═══════════════════════════════════════
# Order / Execution Events
# ═══════════════════════════════════════

class OrderStatus(str, Enum):
    SIGNAL = "signal"
    PM_PENDING = "pm_pending"
    PM_FILLED = "pm_filled"
    TRADFI_PENDING = "tradfi_pending"
    TRADFI_FILLED = "tradfi_filled"
    ACTIVE = "active"
    SETTLED = "settled"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OrderUpdate(BaseEvent):
    event_type: EventType = EventType.ORDER_UPDATE
    order_id: str
    status: OrderStatus
    strategy: Strategy
    # PM details
    pm_market_slug: str = ""
    pm_side: Side | None = None
    pm_fill_price: float = 0.0
    pm_fill_size: float = 0.0
    # TradFi details
    tradfi_symbol: str = ""
    tradfi_fill_price: float = 0.0
    tradfi_fill_size: float = 0.0
    # P&L
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    detail: str = ""
