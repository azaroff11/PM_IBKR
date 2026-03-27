"""
PMarb — Main Orchestrator.

CLI entry point that wires all modules together:
Collectors → EventBus → Analytics → Signals → Execution → Risk

Usage:
    python -m src.main run [--dry-run]
    python -m src.main status
    python -m src.main schedule
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.config import Settings, load_config
from src.event_bus import EventBus

console = Console()

# ═══════════════════════════════════════
# Logging
# ═══════════════════════════════════════

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(name)-20s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libs
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("ib_insync").setLevel(logging.WARNING)


# ═══════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════

async def run(dry_run: bool = True) -> None:
    """Main run loop — starts all modules."""
    settings = Settings()
    config = load_config()
    setup_logging(settings.log_level)

    if dry_run:
        settings.dry_run = True

    # ─── Banner ───
    console.print(Panel.fit(
        "[bold cyan]PMarb — Cross-Market Prediction Arbitrage[/bold cyan]\n"
        f"[dim]Mode: {'🔒 DRY RUN' if settings.dry_run else '🔴 LIVE'}[/dim]\n"
        f"[dim]IBKR: {settings.ibkr_host}:{settings.ibkr_port}[/dim]\n"
        f"[dim]PM Wallet: {settings.poly_wallet_address[:20]}...[/dim]" if settings.poly_wallet_address else "",
        title="🎯 PMarb v0.1.0",
        border_style="cyan",
    ))

    # ─── Event Bus ───
    bus = EventBus()
    await bus.start()

    # ─── Dashboard ───
    from src.dashboard_api import register_handlers as register_dash, app as dash_app
    import uvicorn

    register_dash(bus)
    dash_config = uvicorn.Config(dash_app, host="0.0.0.0", port=8877, log_level="warning")
    dash_server = uvicorn.Server(dash_config)
    dash_task = asyncio.create_task(dash_server.serve())

    # ─── Module 1: Collectors ───
    from src.collectors.sentiment import SentimentCollector
    from src.collectors.portwatch import PortWatchCollector
    from src.collectors.eia import EIACollector
    from src.collectors.polymarket import PolymarketCollector
    from src.collectors.tradfi import TradFiCollector
    from src.collectors.scanner import MarketScanner

    # Prepare PM market slugs from config
    pm_slugs = {name: mkt.slug for name, mkt in config.pm_markets.items()}

    # Parse RSS URLs: prefer plural env var, fall back to legacy single
    rss_urls: list[str] = []
    if settings.sentiment_rss_urls:
        rss_urls = [u.strip() for u in settings.sentiment_rss_urls.split(",") if u.strip()]
    elif settings.sentiment_rss_url:
        rss_urls = [settings.sentiment_rss_url]
    # Also load from config.yaml sentiment sources
    for src in config.sentiment.sources:
        url = src.get("url", "")
        if url and url not in rss_urls:
            rss_urls.append(url)

    collectors = [
        SentimentCollector(
            bus=bus,
            rss_urls=rss_urls,
            keywords=config.sentiment.keywords.model_dump() if config.sentiment.keywords else None,
            poll_interval=settings.sentiment_poll_interval,
        ),
        PortWatchCollector(
            bus=bus,
            chokepoint=config.portwatch.chokepoint,
            poll_interval=config.portwatch.poll_interval_sec,
        ),
        EIACollector(
            bus=bus,
            api_key=settings.eia_api_key,
            poll_interval=config.eia.poll_interval_sec,
        ),
        PolymarketCollector(
            bus=bus,
            market_slugs=pm_slugs,
            poll_interval=5,
        ),
        TradFiCollector(
            bus=bus,
            ibkr_host=settings.ibkr_host,
            ibkr_port=settings.ibkr_port,
            ibkr_client_id=settings.ibkr_client_id,
            instruments=[inst.model_dump() for inst in config.instruments],
            options_config=config.options.model_dump(),
            poll_interval=30,
        ),
    ]

    # ─── Module 2: Analytics ───
    from src.analytics.ceasefire import CeasefireDetector
    from src.analytics.hormuz import HormuzArbEngine
    from src.analytics.latency import LatencyEngine
    from src.analytics.spread import SpreadCalculator
    from src.analytics.signals import SignalAggregator

    ceasefire = CeasefireDetector(
        bus=bus,
        pm_yes_threshold=config.analytics.ceasefire.pm_yes_threshold,
        signal_cooldown_sec=config.analytics.ceasefire.signal_cooldown_sec,
    )
    hormuz = HormuzArbEngine(
        bus=bus,
        pm_yes_threshold=config.analytics.hormuz.pm_yes_threshold,
        ais_stale_days=config.analytics.hormuz.ais_stale_days,
    )
    latency = LatencyEngine(bus=bus)
    spread = SpreadCalculator(bus=bus)
    signals = SignalAggregator(bus=bus)

    # ─── Module 3: Execution ───
    from src.execution.order_manager import OrderManager

    order_manager = OrderManager(data_dir="./data")

    # ─── Module 4: Risk ───
    from src.risk.depeg_monitor import DepegMonitor
    from src.risk.capital import CapitalManager
    from src.risk.limits import RiskLimits

    depeg = DepegMonitor(
        bus=bus,
        alert_threshold_bps=config.risk.depeg_alert_threshold_bps,
        poll_interval=config.risk.curve_3pool_check_interval,
    )
    capital = CapitalManager(
        max_locked_capital_usd=config.risk.max_locked_capital_usd,
        max_single_position_usd=config.risk.max_single_position_usd,
    )
    risk_limits = RiskLimits(
        bus=bus,
        max_session_loss=config.risk.max_session_loss_usd,
        max_positions=config.risk.max_positions,
    )

    # ─── Start all collectors ───
    console.print("\n[bold green]Starting collectors...[/bold green]")
    for c in collectors:
        try:
            await c.start()
            console.print(f"  ✅ {c.name}")
        except Exception as e:
            console.print(f"  ❌ {c.name}: {e}")

    await depeg.start()
    console.print("  ✅ depeg_monitor")

    console.print(f"\n[bold green]Analytics engines initialized:[/bold green]")
    console.print("  ✅ ceasefire_detector")
    console.print("  ✅ hormuz_arb_engine")
    console.print("  ✅ latency_engine")
    console.print("  ✅ spread_calculator")
    console.print("  ✅ signal_aggregator")

    console.print(f"\n[bold yellow]System running. Press Ctrl+C to stop.[/bold yellow]")
    console.print(f"[bold cyan]📊 Dashboard: http://localhost:8877[/bold cyan]\n")

    # ─── Market Scanner (auto-discover PM markets) ───
    scanner = MarketScanner(min_volume_24h=5000, min_liquidity=10000)
    try:
        discovered = await scanner.scan()
        console.print(f"  🔍 Scanner: {len(discovered)} markets found ({sum(1 for m in discovered if m['has_hedge'])} with hedge)")
        # Inject top markets into PM collector for live tracking
        pm_collector = next(c for c in collectors if c.name == "polymarket")
        for mkt in discovered[:50]:  # Track top 50 by arb score
            key = mkt['slug'].replace('-', '_')[:30]
            if key not in pm_collector._condition_cache:
                pm_collector._condition_cache[key] = {
                    'slug': mkt['slug'],
                    'condition_id': mkt['condition_id'],
                    'question': mkt['question'],
                    'clob_token_ids': mkt.get('clob_token_ids', ''),
                }
        console.print(f"  📡 Now tracking {len(pm_collector._condition_cache)} markets")
    except Exception as e:
        console.print(f"  ⚠️  Scanner error: {e}")

    # Register scanner state in dashboard
    from src.dashboard_api import state as dash_state
    dash_state._scanner = scanner

    # ─── Main loop ───
    try:
        scan_counter = 0
        while True:
            await asyncio.sleep(10)
            scan_counter += 1

            # Re-scan PM markets every 5 minutes (30 * 10s)
            if scan_counter % 30 == 0:
                try:
                    discovered = await scanner.scan()
                    pm_collector = next(c for c in collectors if c.name == "polymarket")
                    for mkt in discovered[:50]:
                        key = mkt['slug'].replace('-', '_')[:30]
                        if key not in pm_collector._condition_cache:
                            pm_collector._condition_cache[key] = {
                                'slug': mkt['slug'],
                                'condition_id': mkt['condition_id'],
                                'question': mkt['question'],
                                'clob_token_ids': mkt.get('clob_token_ids', ''),
                            }
                except Exception:
                    pass

            # Update capital from order manager
            capital.update_locked(order_manager.get_locked_capital())

            # Check risk limits
            active = order_manager.get_active_positions()
            allowed, reason = await risk_limits.check_limits(0, len(active))

            # Process signals → execution
            for sig in signals.get_active_signals():
                if not allowed:
                    break

                size = capital.calculate_position_size(
                    win_prob=sig.confidence,
                    win_payoff_ratio=(1 - sig.pm_price) / sig.pm_price if sig.pm_price > 0 else 0,
                )

                can_open, msg = capital.can_open_position(size)
                if not can_open:
                    continue

                # Create position (in dry-run, just log)
                pos = order_manager.create_position(sig)
                signals.clear_signal(f"{sig.strategy.value}:{sig.pm_market_slug}")

                console.print(
                    f"  [cyan]📋 Position {pos.id}:[/cyan] "
                    f"{sig.strategy.value} | {sig.pm_side.value} @ ${sig.pm_price:.3f} | "
                    f"hedge={sig.hedge_type.value} {sig.hedge_symbol} | "
                    f"size=${size:.0f} | conf={sig.confidence:.2f}"
                )

    except KeyboardInterrupt:
        console.print("\n[bold red]Shutting down...[/bold red]")

    # ─── Cleanup ───
    for c in collectors:
        await c.stop()
    await depeg.stop()
    await bus.stop()

    # Final report
    table = Table(title="EventBus Stats")
    table.add_column("Event Type")
    table.add_column("Count", justify="right")
    for etype, count in bus.stats.items():
        table.add_row(etype, str(count))
    console.print(table)

    pos_table = Table(title="Positions")
    pos_table.add_column("ID")
    pos_table.add_column("Strategy")
    pos_table.add_column("Status")
    pos_table.add_column("PM Side")
    pos_table.add_column("Cost")
    for p in order_manager.get_all_positions():
        pos_table.add_row(p["id"], p["strategy"], p["status"], p["pm_side"], f"${p['total_cost']:.0f}")
    if order_manager.get_all_positions():
        console.print(pos_table)


def show_schedule() -> None:
    """Show upcoming data publication schedule."""
    from src.analytics.latency import LatencyEngine
    schedule = LatencyEngine.get_next_update_schedule()

    table = Table(title="📅 Data Publication Schedule")
    table.add_column("Source")
    table.add_column("Next Update (UTC)")
    for src, dt in schedule.items():
        table.add_row(src, dt)
    console.print(table)


def cli() -> None:
    """CLI entry point."""
    args = sys.argv[1:]
    cmd = args[0] if args else "run"

    if cmd == "run":
        dry_run = "--dry-run" in args or "-d" in args
        asyncio.run(run(dry_run=dry_run))
    elif cmd == "schedule":
        show_schedule()
    elif cmd == "status":
        from src.execution.order_manager import OrderManager
        om = OrderManager()
        positions = om.get_all_positions()
        if positions:
            for p in positions:
                console.print(p)
        else:
            console.print("[dim]No positions[/dim]")
    else:
        console.print(f"Unknown command: {cmd}")
        console.print("Usage: pmarb [run|status|schedule] [--dry-run]")


if __name__ == "__main__":
    cli()
