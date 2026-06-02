"""Aggregate/summary endpoints for BI dashboards."""
from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import fetch_all, fetch_one, get_db
from ..schemas import (
    RiskDistribution,
    SegmentDistribution,
    SegmentSummary,
    SpendTrendSummary,
    TopCustomer,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/segments/distribution", response_model=list[SegmentDistribution])
def segment_distribution(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[SegmentDistribution]:
    """Count of customers per segment_name."""
    rows = fetch_all(
        con,
        """
        SELECT segment_name, COUNT(*) AS customer_count
        FROM customer_master_profile
        GROUP BY segment_name
        ORDER BY customer_count DESC
        """,
    )
    return [SegmentDistribution(**r) for r in rows]


@router.get("/risk/distribution", response_model=list[RiskDistribution])
def risk_distribution(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[RiskDistribution]:
    """Count of customers per risk_tier."""
    rows = fetch_all(
        con,
        """
        SELECT risk_tier, COUNT(*) AS customer_count
        FROM customer_master_profile
        GROUP BY risk_tier
        ORDER BY customer_count DESC
        """,
    )
    return [RiskDistribution(**r) for r in rows]


@router.get("/spend/trends", response_model=list[SpendTrendSummary])
def spend_trends(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[SpendTrendSummary]:
    """Breakdown of customers by monthly_spend_trend (UP/DOWN/STABLE)."""
    rows = fetch_all(
        con,
        """
        SELECT monthly_spend_trend, COUNT(*) AS customer_count
        FROM transaction_analytics
        GROUP BY monthly_spend_trend
        ORDER BY customer_count DESC
        """,
    )
    return [SpendTrendSummary(**r) for r in rows]


@router.get("/segments/{segment_name}/summary", response_model=SegmentSummary)
def segment_summary(
    segment_name: str, con: duckdb.DuckDBPyConnection = Depends(get_db)
) -> SegmentSummary:
    """Avg lifetime value, avg composite risk score, and count for a segment."""
    row = fetch_one(
        con,
        """
        SELECT
            COUNT(*) AS customer_count,
            AVG(lifetime_value_score) AS avg_lifetime_value,
            AVG(composite_risk_score) AS avg_composite_risk_score
        FROM customer_master_profile
        WHERE segment_name = ?
        """,
        [segment_name],
    )
    if row is None or row["customer_count"] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No customers found for segment '{segment_name}'",
        )
    return SegmentSummary(segment_name=segment_name, **row)


@router.get("/top-customers", response_model=list[TopCustomer])
def top_customers(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
    n: int = Query(20, ge=1, le=500),
) -> list[TopCustomer]:
    """Top N customers by lifetime_value_score."""
    rows = fetch_all(
        con,
        """
        SELECT customer_id, full_name, segment_name, lifetime_value_score,
               risk_tier, total_balance
        FROM customer_master_profile
        ORDER BY lifetime_value_score DESC
        LIMIT ?
        """,
        [n],
    )
    return [TopCustomer(**r) for r in rows]
