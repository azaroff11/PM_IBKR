"""
PMarb — Cross-Scenario P&L Validator + Transaction Cost Model.

Ensures every signal profits in BOTH scenarios:
- Scenario A: Event does NOT happen (our thesis is right)
- Scenario B: Event DOES happen (our thesis is wrong, hedge saves us)

A signal is valid ONLY if net_profit > 0 in both scenarios.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════
# Transaction Costs
# ═══════════════════════════════════════

@dataclass
class TxCosts:
    """Round-trip transaction costs for a PM + hedge position."""
    pm_spread_cost: float = 0.0    # PM bid-ask / 2 × notional
    option_spread_cost: float = 0.0  # (ask-bid) / 2 × contracts × 100
    ibkr_commission: float = 0.0   # $0.65/contract options
    pm_gas: float = 0.02           # Polygon gas ~$0.02
    slippage_pct: float = 0.002    # 0.2% market impact

    @property
    def total(self) -> float:
        return (
            self.pm_spread_cost
            + self.option_spread_cost
            + self.ibkr_commission
            + self.pm_gas
        )


def estimate_tx_costs(
    pm_notional: float,
    pm_spread: float,
    option_contracts: int = 1,
    option_bid_ask_spread: float = 0.10,
) -> TxCosts:
    """Estimate total round-trip transaction costs."""
    return TxCosts(
        pm_spread_cost=pm_spread * pm_notional,  # spread as fraction of notional
        option_spread_cost=option_bid_ask_spread / 2 * option_contracts * 100,
        ibkr_commission=0.65 * option_contracts,
        pm_gas=0.02,
    )


# ═══════════════════════════════════════
# Scenario Analysis
# ═══════════════════════════════════════

@dataclass
class ScenarioResult:
    """P&L breakdown for a single scenario."""
    name: str
    pm_pnl: float
    hedge_pnl: float
    costs: float
    net_pnl: float


@dataclass
class ValidationResult:
    """Result of cross-scenario validation."""
    is_valid: bool
    best_case: ScenarioResult
    worst_case: ScenarioResult
    breakeven_prob: float
    hedge_cost_usd: float
    tx_costs_usd: float
    rejection_reason: str = ""


def validate_signal(
    pm_side: str,           # "buy_no" or "buy_yes"
    pm_price: float,        # Entry price (e.g., 0.435 for NO)
    pm_notional: float,     # USD size of PM position
    hedge_type: str,        # "put", "call", "none"
    option_premium: float,  # Per-share option premium (ask price)
    option_delta: float,    # Option delta (absolute)
    expected_move_pct: float,  # Expected underlying move if event happens
    spot_price: float,      # Current underlying spot
    pm_spread: float = 0.02,
    option_bid_ask: float = 0.10,
    min_net_profit: float = 0.0,
) -> ValidationResult:
    """
    Validate that a signal profits in BOTH scenarios.

    Returns ValidationResult with is_valid=True only if net P&L > min_net_profit
    in both best and worst case.
    """

    # ─── PM LEG P&L ───
    if pm_side == "buy_no":
        # Best case: event doesn't happen → NO wins
        pm_pnl_best = pm_notional * (1.0 - pm_price) / pm_price
        # Worst case: event happens → NO loses
        pm_pnl_worst = -pm_notional
    else:
        pm_pnl_best = pm_notional * (1.0 - pm_price) / pm_price
        pm_pnl_worst = -pm_notional

    # ─── HEDGE LEG ───
    if hedge_type == "none":
        hedge_cost = 0.0
        hedge_pnl_best = 0.0
        hedge_pnl_worst = 0.0
        n_contracts = 0
    else:
        # How many contracts needed to cover PM loss
        expected_move_usd = spot_price * expected_move_pct
        payoff_per_contract = expected_move_usd * 100  # 100 shares per contract

        if payoff_per_contract > 0:
            # Size hedge to cover PM loss
            n_contracts = max(1, int(abs(pm_pnl_worst) / payoff_per_contract + 0.5))
        else:
            n_contracts = 1

        hedge_cost = option_premium * 100 * n_contracts

        # Best case (event doesn't happen): option expires worthless
        hedge_pnl_best = -hedge_cost

        # Worst case (event happens): option profits
        hedge_pnl_worst = (payoff_per_contract * n_contracts) - hedge_cost

    # ─── TRANSACTION COSTS ───
    costs = estimate_tx_costs(
        pm_notional=pm_notional,
        pm_spread=pm_spread,
        option_contracts=n_contracts,
        option_bid_ask_spread=option_bid_ask,
    )

    # ─── NET P&L ───
    net_best = pm_pnl_best + hedge_pnl_best - costs.total
    net_worst = pm_pnl_worst + hedge_pnl_worst - costs.total

    # ─── BREAKEVEN PROBABILITY ───
    # At what event probability does EV = 0?
    # EV = (1-p) * net_best + p * net_worst = 0
    # p = net_best / (net_best - net_worst)
    denom = net_best - net_worst
    if denom != 0:
        breakeven_prob = net_best / denom
        breakeven_prob = max(0.0, min(1.0, breakeven_prob))
    else:
        breakeven_prob = 0.5

    # ─── VALIDATION ───
    best_case = ScenarioResult(
        name="event_not_happens",
        pm_pnl=pm_pnl_best,
        hedge_pnl=hedge_pnl_best,
        costs=costs.total,
        net_pnl=net_best,
    )
    worst_case = ScenarioResult(
        name="event_happens",
        pm_pnl=pm_pnl_worst,
        hedge_pnl=hedge_pnl_worst,
        costs=costs.total,
        net_pnl=net_worst,
    )

    is_valid = net_best > min_net_profit and net_worst > min_net_profit
    rejection_reason = ""

    if not is_valid:
        if net_best <= min_net_profit:
            rejection_reason = f"Best case P&L ${net_best:.0f} <= ${min_net_profit:.0f} (hedge too expensive)"
        elif net_worst <= min_net_profit:
            rejection_reason = f"Worst case P&L ${net_worst:.0f} <= ${min_net_profit:.0f} (hedge insufficient)"

    return ValidationResult(
        is_valid=is_valid,
        best_case=best_case,
        worst_case=worst_case,
        breakeven_prob=round(breakeven_prob, 3),
        hedge_cost_usd=round(hedge_cost, 2),
        tx_costs_usd=round(costs.total, 2),
        rejection_reason=rejection_reason,
    )
