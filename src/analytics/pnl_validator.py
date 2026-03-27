"""
PMarb — Cross-Scenario P&L Validator + Budget Allocator + TX Cost Model.

Ensures every signal profits in BOTH scenarios:
- Scenario A: Event does NOT happen (our thesis is right)
- Scenario B: Event DOES happen (our thesis is wrong, hedge saves us)

A signal is valid ONLY if net_profit > 0 in both scenarios.

Budget allocator: given total budget (e.g. $10K), calculates optimal
PM vs options hedge split.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default budget for signal modeling
DEFAULT_BUDGET_USD = 10_000.0


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
# Budget Allocation
# ═══════════════════════════════════════

@dataclass
class BudgetAllocation:
    """How to split total budget between PM and hedge."""
    total_budget: float
    pm_allocation: float        # USD to PM leg
    hedge_allocation: float     # USD to options hedge
    pm_shares: float            # Number of PM shares (= pm_alloc / pm_price)
    hedge_contracts: int        # Number of option contracts
    option_premium_total: float # Total option cost
    pm_pct: float               # % of budget to PM
    hedge_pct: float            # % of budget to hedge


def allocate_budget(
    total_budget: float,
    pm_price: float,
    option_premium: float,
    spot_price: float,
    expected_move_pct: float,
    hedge_type: str = "put",
    max_hedge_pct: float = 0.40,
) -> BudgetAllocation:
    """
    Optimally allocate budget between PM and hedge legs.

    Solves analytically for the hedge fraction f that maximizes
    min(best_case_pnl, worst_case_pnl).

    Best case (event doesn't happen):
      net = r × (1-f) × B - f × B   where r = (1-pm_price)/pm_price
    Worst case (event happens):
      net = (R-1) × f × B - (1-f) × B   where R = payoff/cost per contract

    Optimal f = r / (r + R)   (where best = worst)
    """
    if hedge_type == "none" or option_premium <= 0 or spot_price <= 0:
        return BudgetAllocation(
            total_budget=total_budget,
            pm_allocation=total_budget,
            hedge_allocation=0,
            pm_shares=total_budget / pm_price if pm_price > 0 else 0,
            hedge_contracts=0,
            option_premium_total=0,
            pm_pct=1.0,
            hedge_pct=0.0,
        )

    import math

    expected_move_usd = spot_price * expected_move_pct
    payoff_per_contract = expected_move_usd * 100  # 100 shares
    cost_per_contract = option_premium * 100

    if payoff_per_contract <= 0 or cost_per_contract <= 0:
        return BudgetAllocation(
            total_budget=total_budget,
            pm_allocation=total_budget,
            hedge_allocation=0,
            pm_shares=total_budget / pm_price if pm_price > 0 else 0,
            hedge_contracts=0,
            option_premium_total=0,
            pm_pct=1.0,
            hedge_pct=0.0,
        )

    # Brute-force: try each contract count, pick one that maximizes min(best, worst)
    r = (1.0 - pm_price) / pm_price if pm_price > 0 else 0  # PM profit margin

    max_affordable = int(total_budget * 0.50 / cost_per_contract)  # Max 50% to hedge
    best_n = 0
    best_min_pnl = -float("inf")

    for n in range(0, max_affordable + 1):
        hc = n * cost_per_contract
        pm = total_budget - hc
        best_pnl = pm * r - hc       # PM wins, hedge expires worthless
        worst_pnl = -pm + n * payoff_per_contract - hc  # PM loses, hedge pays
        min_pnl = min(best_pnl, worst_pnl)

        if min_pnl > best_min_pnl:
            best_min_pnl = min_pnl
            best_n = n

    n_contracts = best_n
    actual_hedge_cost = n_contracts * cost_per_contract
    pm_budget = total_budget - actual_hedge_cost
    pm_shares = pm_budget / pm_price if pm_price > 0 else 0

    return BudgetAllocation(
        total_budget=total_budget,
        pm_allocation=round(pm_budget, 2),
        hedge_allocation=round(actual_hedge_cost, 2),
        pm_shares=round(pm_shares, 1),
        hedge_contracts=n_contracts,
        option_premium_total=round(actual_hedge_cost, 2),
        pm_pct=round(pm_budget / total_budget, 3),
        hedge_pct=round(actual_hedge_cost / total_budget, 3),
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
    budget: BudgetAllocation | None = None
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
    total_budget: float = DEFAULT_BUDGET_USD,
) -> ValidationResult:
    """
    Validate that a signal profits in BOTH scenarios.

    Uses budget allocator to determine optimal PM/hedge split,
    then validates net P&L > min_net_profit in both cases.
    """

    # ─── BUDGET ALLOCATION ───
    budget = allocate_budget(
        total_budget=total_budget,
        pm_price=pm_price,
        option_premium=option_premium,
        spot_price=spot_price,
        expected_move_pct=expected_move_pct,
        hedge_type=hedge_type,
    )

    pm_notional = budget.pm_allocation
    n_contracts = budget.hedge_contracts

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
    if hedge_type == "none" or n_contracts == 0:
        hedge_cost = 0.0
        hedge_pnl_best = 0.0
        hedge_pnl_worst = 0.0
    else:
        expected_move_usd = spot_price * expected_move_pct
        payoff_per_contract = expected_move_usd * 100

        hedge_cost = budget.option_premium_total

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
        budget=budget,
        rejection_reason=rejection_reason,
    )

