from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cost calculation method codes — these end up in
# UsageEvent.meta_json.cost_calculation_method.
# ---------------------------------------------------------------------------

COST_METHOD_API_TOKENS = "api_usage_tokens_x_static_price_table"
COST_METHOD_ESTIMATED = "estimated_tokens_x_static_price_table"

# Negative-path codes:
COST_METHOD_PRICING_UNAVAILABLE = "pricing_unavailable"
"""llm_pricing table doesn't exist yet OR DB error reading it."""

COST_METHOD_NO_PRICING = "no_pricing_in_db"
"""Table exists, but no active row for this ai_model — admin hasn't configured it."""

COST_METHOD_NO_LLM_CALL = "no_llm_call"
"""LLM was not actually called (empty context, short-circuit) — cost is 0."""


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PricingRow:
    """Snapshot of one active row from llm_pricing."""
    ai_model: str
    input_price_per_1m_usd: float
    output_price_per_1m_usd: float


@dataclass(frozen=True)
class CostResult:
    """
    Outcome of a cost-estimation attempt. Safe to merge into
    UsageEvent.meta_json.

    The price-snapshot fields preserve the rate in effect at the time
    of the LLM call so historical UsageEvents don't shift when admin
    changes pricing later.
    """
    estimated_cost_usd: Optional[float]
    cost_calculation_method: str
    input_price_per_1m_usd_snapshot: Optional[float]
    output_price_per_1m_usd_snapshot: Optional[float]


# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------
#
# We cache both positive (row exists) and negative (no row / table missing)
# lookups for 60 seconds so that high-frequency Q&A and subscription runs
# don't hit llm_pricing on every call.
#
# `invalidate_pricing_cache()` should be called by the admin pricing
# endpoint after any update — Stage 6 will wire that in.
# ---------------------------------------------------------------------------

_PRICING_CACHE_TTL_SECONDS = 60.0

_pricing_cache: dict[str, tuple[Optional[PricingRow], float]] = {}
_table_missing_at: Optional[float] = None


def _now() -> float:
    return time.monotonic()


def _is_fresh(cached_at: float) -> bool:
    return (_now() - cached_at) < _PRICING_CACHE_TTL_SECONDS


def invalidate_pricing_cache() -> None:
    """Drop all cached pricing entries. Call after admin updates llm_pricing."""
    global _table_missing_at
    _pricing_cache.clear()
    _table_missing_at = None


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------
#
# We deliberately use a raw `sa.text(...)` query rather than declaring an
# ORM model. Reasons:
#   1) the user is writing the Alembic migration herself, and we don't
#      want a stray duplicate model declaration in db/models.py before
#      the migration lands.
#   2) when the table doesn't exist yet, SQLAlchemy raises a generic
#      ProgrammingError that we catch and translate into
#      COST_METHOD_PRICING_UNAVAILABLE without breaking the calling
#      request.
# ---------------------------------------------------------------------------

async def _fetch_pricing_row(
    db: AsyncSession,
    ai_model: str,
) -> Optional[PricingRow]:
    stmt = sa.text(
        """
        SELECT
            ai_model,
            input_price_per_1m_usd,
            output_price_per_1m_usd
        FROM llm_pricing
        WHERE ai_model = :ai_model
          AND is_active = true
        LIMIT 1
        """
    )
    res = await db.execute(stmt, {"ai_model": ai_model})
    row = res.fetchone()
    if row is None:
        return None
    return PricingRow(
        ai_model=str(row[0]),
        input_price_per_1m_usd=float(row[1]),
        output_price_per_1m_usd=float(row[2]),
    )


async def get_active_pricing(
    db: AsyncSession,
    ai_model: str,
) -> tuple[Optional[PricingRow], str]:
    """
    Return (row, status) where status is one of:
      - "ok"             — active row found
      - "no_pricing"     — table exists but no active row for this model
      - "table_missing"  — llm_pricing doesn't exist yet or DB read failed
    """
    global _table_missing_at

    # If we recently saw "table missing", short-circuit.
    if _table_missing_at is not None and _is_fresh(_table_missing_at):
        return None, "table_missing"

    # Per-model cache lookup
    cached = _pricing_cache.get(ai_model)
    if cached is not None:
        row, cached_at = cached
        if _is_fresh(cached_at):
            return row, ("ok" if row is not None else "no_pricing")

    try:
        row = await _fetch_pricing_row(db, ai_model)
    except SQLAlchemyError as e:
        # Most likely: table doesn't exist (migration not applied yet).
        log.warning(
            "llm_pricing read failed (likely table-missing or schema error): %s",
            e,
        )
        _table_missing_at = _now()
        return None, "table_missing"
    except Exception as e:  # noqa: BLE001
        # Network blip / driver hiccup — degrade gracefully, retry next time.
        log.warning("llm_pricing read failed (unexpected): %s", e)
        return None, "table_missing"

    _pricing_cache[ai_model] = (row, _now())
    return row, ("ok" if row is not None else "no_pricing")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def estimate_llm_cost_usd(
    db: AsyncSession,
    *,
    ai_model: str,
    input_tokens: Optional[int],
    output_tokens: Optional[int],
    tokens_source: str,
) -> CostResult:
    """
    Compute the estimated USD cost of a single LLM call.

    `tokens_source` must be one of the values from llm.usage:
      - "api_usage"        — tokens came from provider response.usage
      - "estimated_chars"  — tokens were estimated from char counts
      - "empty"            — no LLM call was made (empty context)

    Behavior:
      - "empty"         → cost = 0.0, method = "no_llm_call"
      - table missing   → cost = None, method = "pricing_unavailable"
      - no active row   → cost = None, method = "no_pricing_in_db"
      - happy path      → cost computed, method reflects tokens_source,
                          snapshot of prices included
    """
    if tokens_source == "empty" or (
        not int(input_tokens or 0) and not int(output_tokens or 0)
    ):
        return CostResult(
            estimated_cost_usd=0.0,
            cost_calculation_method=COST_METHOD_NO_LLM_CALL,
            input_price_per_1m_usd_snapshot=None,
            output_price_per_1m_usd_snapshot=None,
        )

    row, status = await get_active_pricing(db, ai_model)

    if status == "table_missing":
        return CostResult(
            estimated_cost_usd=None,
            cost_calculation_method=COST_METHOD_PRICING_UNAVAILABLE,
            input_price_per_1m_usd_snapshot=None,
            output_price_per_1m_usd_snapshot=None,
        )

    if row is None:
        return CostResult(
            estimated_cost_usd=None,
            cost_calculation_method=COST_METHOD_NO_PRICING,
            input_price_per_1m_usd_snapshot=None,
            output_price_per_1m_usd_snapshot=None,
        )

    in_tok = int(input_tokens or 0)
    out_tok = int(output_tokens or 0)

    cost = (
        (in_tok / 1_000_000.0) * row.input_price_per_1m_usd
        + (out_tok / 1_000_000.0) * row.output_price_per_1m_usd
    )
    cost = round(cost, 6)

    method = (
        COST_METHOD_API_TOKENS
        if tokens_source == "api_usage"
        else COST_METHOD_ESTIMATED
    )

    return CostResult(
        estimated_cost_usd=cost,
        cost_calculation_method=method,
        input_price_per_1m_usd_snapshot=float(row.input_price_per_1m_usd),
        output_price_per_1m_usd_snapshot=float(row.output_price_per_1m_usd),
    )


def cost_kwargs_for_meta(cost: CostResult) -> dict:
    """
    Map a CostResult into the cost-related kwargs of `record_qa_success`
    / `record_qa_failure`. Use with `**cost_kwargs_for_meta(cost)`.
    """
    return {
        "estimated_cost_usd": cost.estimated_cost_usd,
        "cost_calculation_method": cost.cost_calculation_method,
        "input_price_per_1m_usd_snapshot": cost.input_price_per_1m_usd_snapshot,
        "output_price_per_1m_usd_snapshot": cost.output_price_per_1m_usd_snapshot,
    }
