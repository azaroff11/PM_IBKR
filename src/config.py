"""
PMarb — Configuration loader.

Loads from config.yaml + .env with Pydantic validation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# ═══════════════════════════════════════
# Env-based settings (.env)
# ═══════════════════════════════════════

class Settings(BaseSettings):
    # Polymarket
    poly_private_key: str = ""
    poly_wallet_address: str = ""
    poly_rpc: str = "https://polygon-bor-rpc.publicnode.com"
    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    poly_relayer_api_key: str = ""
    poly_relayer_address: str = ""

    # IBKR
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4002  # 4001=live, 4002=paper
    ibkr_client_id: int = 1

    # Sentiment
    sentiment_rss_url: str = ""  # legacy single URL
    sentiment_rss_urls: str = ""  # comma-separated list of RSS URLs
    sentiment_poll_interval: int = 30

    # EIA
    eia_api_key: str = ""

    # Risk
    max_session_loss_usd: float = 500
    max_single_position_usd: float = 1000
    max_locked_capital_usd: float = 5000
    max_positions: int = 10

    # Mode
    dry_run: bool = True
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# ═══════════════════════════════════════
# YAML config models
# ═══════════════════════════════════════

class MarketConfig(BaseModel):
    slug: str
    condition_id: str = ""
    description: str = ""
    resolution_source: str = ""
    bilateral_required: bool = False
    threshold_pct: float = 0.0
    threshold_absolute: float = 0.0


class InstrumentConfig(BaseModel):
    symbol: str
    exchange: str
    type: str  # "stock", "future"
    use_for: str = ""
    description: str = ""


class OptionsConfig(BaseModel):
    target_delta: float = 0.25
    min_dte: int = 14
    max_dte: int = 90
    max_bid_ask_spread: float = 0.20


class PortWatchConfig(BaseModel):
    base_url: str = "https://portwatch.imf.org"
    chokepoint: str = "Strait of Hormuz"
    poll_interval_sec: int = 21600
    update_day: str = "Tuesday"
    update_time_et: str = "09:00"


class EIAConfig(BaseModel):
    base_url: str = "https://api.eia.gov/v2"
    series: dict[str, str] = Field(default_factory=dict)
    poll_interval_sec: int = 3600


class CeasefireAnalytics(BaseModel):
    pm_yes_threshold: float = 0.15
    signal_cooldown_sec: int = 300


class HormuzAnalytics(BaseModel):
    pm_yes_threshold: float = 0.30
    ais_stale_days: int = 7


class LatencyAnalytics(BaseModel):
    portwatch_update_day: str = "Tuesday"
    eia_wpsr_update_day: str = "Wednesday"
    eia_psm_lag_months: int = 2


class AnalyticsConfig(BaseModel):
    ceasefire: CeasefireAnalytics = Field(default_factory=CeasefireAnalytics)
    hormuz: HormuzAnalytics = Field(default_factory=HormuzAnalytics)
    latency: LatencyAnalytics = Field(default_factory=LatencyAnalytics)


class RiskConfig(BaseModel):
    max_session_loss_usd: float = 500
    max_single_position_usd: float = 1000
    max_locked_capital_usd: float = 5000
    max_positions: int = 10
    capital_lockup_buffer_days: int = 7
    depeg_alert_threshold_bps: float = 50
    curve_3pool_check_interval: int = 300


class SentimentKeywords(BaseModel):
    ceasefire: list[str] = Field(default_factory=list)
    escalation: list[str] = Field(default_factory=list)
    iran_confirm: list[str] = Field(default_factory=list)
    oil_disruption: list[str] = Field(default_factory=list)


class SentimentConfig(BaseModel):
    keywords: SentimentKeywords = Field(default_factory=SentimentKeywords)
    sources: list[dict[str, str]] = Field(default_factory=list)


class AppConfig(BaseModel):
    """Full application config loaded from config.yaml."""

    polymarket: dict[str, Any] = Field(default_factory=dict)
    tradfi: dict[str, Any] = Field(default_factory=dict)
    portwatch: PortWatchConfig = Field(default_factory=PortWatchConfig)
    eia: EIAConfig = Field(default_factory=EIAConfig)
    sentiment: SentimentConfig = Field(default_factory=SentimentConfig)
    analytics: AnalyticsConfig = Field(default_factory=AnalyticsConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)

    # Parsed helpers
    pm_markets: dict[str, MarketConfig] = Field(default_factory=dict)
    instruments: list[InstrumentConfig] = Field(default_factory=list)
    options: OptionsConfig = Field(default_factory=OptionsConfig)


def load_config(config_path: str | Path = "config.yaml") -> AppConfig:
    """Load and validate config from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = AppConfig(**raw)

    # Parse nested polymarket markets
    pm_raw = raw.get("polymarket", {})
    for name, mkt in pm_raw.get("markets", {}).items():
        cfg.pm_markets[name] = MarketConfig(**mkt)

    # Parse tradfi instruments
    tf_raw = raw.get("tradfi", {})
    for inst in tf_raw.get("instruments", []):
        cfg.instruments.append(InstrumentConfig(**inst))
    opts = tf_raw.get("options", {})
    if opts:
        cfg.options = OptionsConfig(**opts)

    return cfg
