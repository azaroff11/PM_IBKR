"""
PMarb — Signal Configuration Store.

Persistent JSON-based configuration for signal generation parameters.
Reads/writes to signal_config.json in project root.
All engines read parameters from this store at signal generation time.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "signal_config.json"

DEFAULTS: dict[str, Any] = {
    "strategies": {
        "fake_ceasefire": {"enabled": True},
        "hormuz_definition_arb": {"enabled": True},
        "latency_arb": {"enabled": True},
    },
    "ceasefire": {
        "pm_yes_threshold": 0.15,
        "signal_cooldown_sec": 300,
        "hedge_symbol": "USO",
        "hedge_strike_offset_pct": 0.10,
    },
    "hormuz": {
        "pm_yes_threshold": 0.30,
        "portwatch_threshold_pct": 80.0,
        "ais_stale_days": 7,
        "signal_cooldown_sec": 600,
        "hedge_symbol": "BNO",
    },
    "latency": {
        "psm_lag_months": 2,
        "signal_cooldown_sec": 3600,
        "portwatch_min_yes": 0.20,
        "psm_min_yes": 0.15,
    },
    "filters": {
        "min_edge_pct": 5.0,
        "min_strength": 0.3,
        "min_confidence": 0.5,
    },
}


class SignalConfig:
    """Singleton signal configuration store with JSON persistence."""

    _instance: SignalConfig | None = None

    def __new__(cls) -> SignalConfig:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._loaded = False
        return cls._instance

    def __init__(self) -> None:
        if self._loaded:
            return
        self._data: dict[str, Any] = {}
        self._load()
        self._loaded = True

    def _load(self) -> None:
        """Load config from disk, merging with defaults."""
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH) as f:
                    saved = json.load(f)
                logger.info("[config] Loaded signal config from %s", CONFIG_PATH)
            except Exception:
                logger.exception("[config] Failed to read config, using defaults")
                saved = {}
        else:
            saved = {}

        # Deep merge: defaults + saved overrides
        self._data = _deep_merge(DEFAULTS, saved)

    def _save(self) -> None:
        """Persist config to disk."""
        try:
            with open(CONFIG_PATH, "w") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            logger.info("[config] Saved signal config to %s", CONFIG_PATH)
        except Exception:
            logger.exception("[config] Failed to save config")

    def get_all(self) -> dict[str, Any]:
        """Return full config dict."""
        return self._data

    def get(self, section: str, key: str, default: Any = None) -> Any:
        """Get a single config value."""
        return self._data.get(section, {}).get(key, default)

    def is_strategy_enabled(self, strategy: str) -> bool:
        """Check if a strategy is enabled."""
        return self._data.get("strategies", {}).get(strategy, {}).get("enabled", True)

    def update(self, new_data: dict[str, Any]) -> None:
        """Update config from partial dict and save."""
        self._data = _deep_merge(self._data, new_data)
        self._save()

    def reset(self) -> None:
        """Reset to defaults and save."""
        self._data = json.loads(json.dumps(DEFAULTS))
        self._save()


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
