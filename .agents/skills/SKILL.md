---
name: PMarb System Architecture
description: Complete guide to the PMarb prediction market arbitrage system — architecture, data flows, strategies, and development patterns
---

# PMarb — Prediction Market Arbitrage Terminal

## System Overview

PMarb is an event-driven arbitrage engine that exploits structural inefficiencies between **Polymarket** prediction contracts and **TradFi** hedging instruments (IBKR ETF options).

**Core thesis**: Prediction markets misprice geopolitical events due to crowd psychology (unilateral vs bilateral confirmation, definition gaps, oracle data latency). PMarb detects these mispricings and constructs hedged positions.

## Architecture

```
Event Bus (async pub/sub)
    │
    ├── Collectors (data ingestion)
    │   ├── polymarket.py     — CLOB prices via Gamma API (5s poll)
    │   ├── sentiment.py      — RSS feeds, keyword scoring (30s poll)
    │   ├── portwatch.py      — IMF AIS shipping data (6h poll)
    │   ├── eia.py            — EIA petroleum WPSR/PSM (1h poll)
    │   ├── tradfi.py         — IBKR spot + options chain (30s poll)
    │   ├── scanner.py        — Polymarket market discovery
    │   └── depeg.py          — Stablecoin depeg monitor
    │
    ├── Analytics (signal generation)
    │   ├── ceasefire.py      — Fake Ceasefire detector
    │   ├── hormuz.py         — Hormuz Definition arb
    │   └── latency.py        — Data Latency exploitation
    │
    ├── Execution (order routing)
    │   ├── order_manager.py  — Position tracking
    │   ├── polymarket_exec.py— Polymarket CTF orders
    │   └── tradfi_exec.py    — IBKR order routing
    │
    ├── Risk (pre-trade validation)
    │   ├── limits.py         — Position/session limits
    │   ├── capital.py        — Capital allocation
    │   └── depeg_monitor.py  — Stablecoin safety
    │
    └── Dashboard
        ├── dashboard_api.py  — FastAPI + WebSocket (:8877)
        └── index.html        — Single-page UI
```

## Key Files

### Entry Point
- **`src/main.py`** — Orchestrator. Initializes all collectors, analytics engines, event bus, and dashboard API. CLI via `python -m src.main run --dry-run`.

### Configuration
- **`config.yaml`** — Markets (Polymarket slugs, condition IDs), instruments (USO, BNO, CL, BZ), poll intervals, risk limits. This is the source of truth for market definitions.
- **`src/config.py`** — Settings loader. Reads `config.yaml` + `.env` (via pydantic). All secrets come from env vars, never hardcoded.
- **`.env`** — Secrets: `POLY_PRIVATE_KEY`, `POLY_WALLET_ADDRESS`, `EIA_API_KEY`, `IBKR_HOST/PORT`. Not in git.
- **`src/signal_config.py`** — Persistent JSON config for signal thresholds, strategy toggles, cooldowns. Editable via dashboard UI. Saved to `signal_config.json`.

### Event Bus
- **`src/event_bus.py`** — Async pub/sub. Collectors emit typed events (TradFiEvent, PolymarketEvent, SentimentEvent, etc.). Analytics engines subscribe and emit ArbSignal events.

### Data Models
- **`src/models/events.py`** — All event types: `PolymarketEvent`, `TradFiEvent`, `SentimentEvent`, `PortWatchEvent`, `EIAEvent`, `ArbSignal`, `OptionData`, `DepegAlert`. Each has typed fields with defaults.

### Dashboard
- **`src/dashboard_api.py`** — FastAPI server on port 8877. Endpoints:
  - `GET /api/state` — Full system state (markets, signals, feeds, risk)
  - `GET /api/scanner` — Live Polymarket market scanner
  - `GET /api/signal-settings` / `POST /api/signal-settings` — Signal config CRUD
  - `WS /ws` — Real-time updates pushed to UI
  - `on_signal()` — Central signal handler with strategy enable gates and filter thresholds

- **`dashboard/index.html`** — Single-page dashboard. Three columns:
  - Left: data sources, Hormuz transit, sentiment feed
  - Center: arbitrage map (PM ↔ IBKR cards), arb signals, parser config, position calculator
  - Right: signal settings panel, live signals with risk metrics

## Strategies

### 1. Fake Ceasefire (`ceasefire.py`)
**Edge**: Market prices unilateral peace signals as bilateral ceasefire. Bilateral requires both parties to confirm — historically rare.
- **Trigger**: PM ceasefire YES > threshold (default 15%)
- **PM Leg**: BUY NO (ceasefire won't happen)
- **Hedge**: BUY PUT on USO OTM
- **Key fields**: `pm_yes_threshold`, `signal_cooldown_sec`, `hedge_symbol`, `otm_offset`

### 2. Hormuz Definition (`hormuz.py`)
**Edge**: Market defines "blockade" as 80% traffic reduction. Real attacks cause 40-60% disruption — below threshold.
- **Trigger**: PM blockade YES > threshold (default 30%) + PortWatch traffic drop
- **PM Leg**: BUY NO (definition won't be met)
- **Hedge**: BUY CALL on BNO OTM
- **Key fields**: `pm_yes_threshold`, `portwatch_threshold_pct`, `ais_stale_days`, `hedge_symbol`

### 3. Latency Arb (`latency.py`)
**Edge**: EIA publishes petroleum data on fixed schedule (WPSR=Wednesday, PSM=2-month lag). During blackout, oracle can't verify events → NO wins by default.
- **Trigger**: Data publication gap detected
- **PM Leg**: BUY NO during blackout
- **Hedge**: BUY CALL on BNO
- **Key fields**: `portwatch_update_day`, `eia_wpsr_update_day`, `eia_psm_lag_months`

## Options Chain (IBKR)

The `tradfi.py` collector fetches real options data every 30 seconds:
- **Spot prices**: USO, BNO via streaming (delayed-frozen, type 4)
- **Options chain**: `reqSecDefOptParamsAsync` → get available chains
- **ATM IV**: Average of ATM call + put IV
- **25-delta skew**: Put IV vs Call IV for OTM options (~6 strikes per side)
- **Exchange routing**: Always use `SMART` (not chain.exchange like CBOE2/BATS)
- **Data flow**: streaming mode (not snapshot) → 2s wait → capture greeks → cancel subscription

## Signal Flow

```
Collector emits event
    → Event Bus dispatches to analytics engines
    → Engine checks thresholds (from SignalConfig)
    → Engine emits ArbSignal
    → on_signal() in dashboard_api:
        1. Check strategy enabled (SignalConfig.is_strategy_enabled)
        2. Check global filters (min_edge, min_strength, min_confidence)
        3. Calculate risk metrics (max_profit, max_loss, risk_reward, ev)
        4. Push to WebSocket + store in state
```

## Risk Metrics Per Signal

Each ArbSignal gets enriched with:
- `max_profit` = (1 - entry_price) × depth
- `max_loss` = entry_price × depth + hedge_cost
- `risk_reward` = max_profit / max_loss
- `ev` = max_profit × strength - max_loss × (1 - strength)

## Development Patterns

### Adding a New Collector
1. Create `src/collectors/new_source.py` inheriting `BaseCollector`
2. Implement `poll()` → returns list of events
3. Add event type to `src/models/events.py`
4. Register in `src/main.py` `_start_collectors()`

### Adding a New Strategy
1. Create `src/analytics/new_strategy.py`
2. Subscribe to relevant events via event bus
3. Emit `ArbSignal` when conditions met
4. Add defaults to `src/signal_config.py` DEFAULT_CONFIG
5. Add toggle + params to dashboard settings panel

### Running
```bash
# Prerequisites: IB Gateway running, .env configured
source .venv/bin/activate
SSL_CERT_FILE=$(python -c "import certifi; print(certifi.where())") \
    python -m src.main run --dry-run
# Dashboard: http://localhost:8877
```

### Environment
- Python 3.11+
- IB Gateway/TWS on port 4002 (paper) or 4001 (live)
- `SSL_CERT_FILE` must be set for ib_insync SSL
- `DRY_RUN=true` in .env — no real orders

## Local Setup Requirements

After cloning, you must create `.env` from `.env.example` and fill in these values:

### Required Secrets

| Variable | Where to Get | Required For |
|----------|-------------|-------------|
| `POLY_PRIVATE_KEY` | MetaMask → Account Details → Export Private Key | Polymarket order execution |
| `POLY_WALLET_ADDRESS` | MetaMask → Copy address (0x...) | Polymarket wallet identity |
| `POLY_RELAYER_API_KEY` | polymarket.com → Settings → API Keys → Create New | Polymarket CLOB relay |
| `POLY_RELAYER_ADDRESS` | Same as wallet address | Polymarket relay auth |
| `EIA_API_KEY` | https://www.eia.gov/opendata/register.php (free, instant) | EIA petroleum data |

### Required Services

| Service | How to Start | Default Port |
|---------|-------------|-------------|
| **IB Gateway** or **TWS** | Download from interactivebrokers.com → Login → Accept API connections | 4002 (paper) / 4001 (live) |

### Setup Steps

```bash
# 1. Clone and enter
git clone https://github.com/azaroff11/PM_IBKR.git
cd PM_IBKR

# 2. Create venv
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 3. Create .env from template
cp .env.example .env
# Edit .env — fill in POLY_PRIVATE_KEY, POLY_WALLET_ADDRESS, EIA_API_KEY

# 4. Start IB Gateway (must be running before launch)
# Open IB Gateway → Login with paper account → API Settings → Enable socket port 4002

# 5. Launch (dry-run mode — no real orders)
SSL_CERT_FILE=$(python -c "import certifi; print(certifi.where())") \
    python -m src.main run --dry-run

# 6. Open dashboard
open http://localhost:8877
```

### Configuration Files (no secrets, committed to git)

- **`config.yaml`** — Market definitions, instruments, poll intervals, risk limits. Edit to add/remove Polymarket markets or change risk parameters.
- **`signal_config.json`** — Auto-generated on first run. Editable via dashboard UI (Settings panel). Strategy toggles, thresholds, cooldowns.

### Notes
- `SSL_CERT_FILE` env var is required for `ib_insync` SSL certificate verification
- `DRY_RUN=true` is the default — system will NOT place real orders until you explicitly set `DRY_RUN=false`
- IB Gateway must be running and accepting API connections before starting PMarb
- Polymarket keys are only needed for live order execution; data collection works without them

## Important Constraints

1. **DRY_RUN=true by default** — Never send real orders without explicit toggle
2. **No hardcoded secrets** — All keys from `.env`, never in code
3. **Fail-closed** — Missing data = block trading, not assume
4. **Event-driven** — No polling-based state, everything via event bus
5. **UI is projection only** — Never source of truth for positions or prices
6. **Sequence integrity** — All events logged with timestamps, raw data preserved
