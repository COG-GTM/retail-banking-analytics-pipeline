"""CRUD endpoints for the primary BI entity: CUSTOMER_MASTER_PROFILE."""
from __future__ import annotations

from typing import Literal, Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from ..db import fetch_all, fetch_one, get_db
from ..schemas import (
    CustomerDetail,
    CustomerListItem,
    FlagUpdate,
    PaginatedResponse,
    RiskDetail,
    SegmentDetail,
    TransactionDetail,
)

router = APIRouter(prefix="/customers", tags=["customers"])

# Columns that may be used for sorting (whitelist to prevent SQL injection).
SORTABLE_COLUMNS = {
    "customer_id",
    "full_name",
    "age",
    "state_code",
    "segment_name",
    "risk_tier",
    "lifetime_value_score",
    "engagement_score",
    "composite_risk_score",
    "total_balance",
    "probability_of_default",
}

FLAG_COLUMNS = (
    "cross_sell_flag",
    "upsell_flag",
    "retention_risk_flag",
    "watch_list_flag",
)


@router.get("", response_model=PaginatedResponse[CustomerListItem])
def list_customers(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
    segment_name: Optional[str] = Query(None),
    risk_tier: Optional[str] = Query(None),
    state_code: Optional[str] = Query(None),
    min_lifetime_value: Optional[float] = Query(None),
    max_lifetime_value: Optional[float] = Query(None),
    cross_sell_flag: Optional[Literal["Y", "N"]] = Query(None),
    upsell_flag: Optional[Literal["Y", "N"]] = Query(None),
    retention_risk_flag: Optional[Literal["Y", "N"]] = Query(None),
    watch_list_flag: Optional[Literal["Y", "N"]] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("customer_id"),
    order: Literal["asc", "desc"] = Query("asc"),
) -> PaginatedResponse[CustomerListItem]:
    """List/search customers from CUSTOMER_MASTER_PROFILE with filters."""
    if sort_by not in SORTABLE_COLUMNS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid sort_by '{sort_by}'. Allowed: {sorted(SORTABLE_COLUMNS)}",
        )

    where: list[str] = []
    params: list = []

    def add_eq(column: str, value) -> None:
        if value is not None:
            where.append(f"{column} = ?")
            params.append(value)

    add_eq("segment_name", segment_name)
    add_eq("risk_tier", risk_tier)
    add_eq("state_code", state_code)
    add_eq("cross_sell_flag", cross_sell_flag)
    add_eq("upsell_flag", upsell_flag)
    add_eq("retention_risk_flag", retention_risk_flag)
    add_eq("watch_list_flag", watch_list_flag)

    if min_lifetime_value is not None:
        where.append("lifetime_value_score >= ?")
        params.append(min_lifetime_value)
    if max_lifetime_value is not None:
        where.append("lifetime_value_score <= ?")
        params.append(max_lifetime_value)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = f"ORDER BY {sort_by} {order.upper()}"

    total = fetch_one(
        con,
        f"SELECT COUNT(*) AS n FROM customer_master_profile {where_sql}",
        params,
    )["n"]

    rows = fetch_all(
        con,
        f"""
        SELECT customer_id, full_name, age, state_code, segment_name, risk_tier,
               lifetime_value_score, engagement_score, composite_risk_score,
               total_balance, cross_sell_flag, upsell_flag, retention_risk_flag,
               watch_list_flag
        FROM customer_master_profile
        {where_sql}
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    )

    return PaginatedResponse[CustomerListItem](
        items=[CustomerListItem(**r) for r in rows],
        total=int(total),
        limit=limit,
        offset=offset,
    )


def _get_master_or_404(con: duckdb.DuckDBPyConnection, customer_id: int) -> dict:
    row = fetch_one(
        con,
        "SELECT * FROM customer_master_profile WHERE customer_id = ?",
        [customer_id],
    )
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"Customer {customer_id} not found"
        )
    return row


@router.get("/{customer_id}", response_model=CustomerDetail)
def get_customer(
    customer_id: int, con: duckdb.DuckDBPyConnection = Depends(get_db)
) -> CustomerDetail:
    """Full detail for one customer, joining data from all 4 tables."""
    master = _get_master_or_404(con, customer_id)

    segment = fetch_one(
        con, "SELECT * FROM customer_segments WHERE customer_id = ?", [customer_id]
    )
    transactions = fetch_one(
        con,
        "SELECT * FROM transaction_analytics WHERE customer_id = ?",
        [customer_id],
    )
    risk = fetch_one(
        con,
        "SELECT * FROM customer_risk_scores WHERE customer_id = ?",
        [customer_id],
    )

    return CustomerDetail(
        **master,
        segment=SegmentDetail(**segment) if segment else None,
        transactions=TransactionDetail(**transactions) if transactions else None,
        risk=RiskDetail(**risk) if risk else None,
    )


@router.patch("/{customer_id}/flags", response_model=CustomerDetail)
def update_customer_flags(
    customer_id: int,
    payload: FlagUpdate,
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> CustomerDetail:
    """Update actionable flags (Y/N) on the master profile (write-back).

    Simulates a write-back to Teradata DATA_PRODUCTS_DB in production.
    """
    _get_master_or_404(con, customer_id)

    updates = payload.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(
            status_code=422,
            detail=f"Provide at least one flag to update: {list(FLAG_COLUMNS)}",
        )

    set_clauses = [f"{col} = ?" for col in updates]
    params = list(updates.values()) + [customer_id]
    con.execute(
        f"UPDATE customer_master_profile SET {', '.join(set_clauses)} "
        "WHERE customer_id = ?",
        params,
    )

    return get_customer(customer_id, con)


@router.get("/{customer_id}/segments", response_model=SegmentDetail)
def get_customer_segment(
    customer_id: int, con: duckdb.DuckDBPyConnection = Depends(get_db)
) -> SegmentDetail:
    """Drill-down to the customer_segments row for a customer."""
    _get_master_or_404(con, customer_id)
    row = fetch_one(
        con, "SELECT * FROM customer_segments WHERE customer_id = ?", [customer_id]
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No segment record for customer {customer_id}",
        )
    return SegmentDetail(**row)


@router.get("/{customer_id}/transactions", response_model=TransactionDetail)
def get_customer_transactions(
    customer_id: int, con: duckdb.DuckDBPyConnection = Depends(get_db)
) -> TransactionDetail:
    """Drill-down to the transaction_analytics row for a customer."""
    _get_master_or_404(con, customer_id)
    row = fetch_one(
        con,
        "SELECT * FROM transaction_analytics WHERE customer_id = ?",
        [customer_id],
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No transaction analytics record for customer {customer_id}",
        )
    return TransactionDetail(**row)


@router.get("/{customer_id}/risk", response_model=RiskDetail)
def get_customer_risk(
    customer_id: int, con: duckdb.DuckDBPyConnection = Depends(get_db)
) -> RiskDetail:
    """Drill-down to the customer_risk_scores row for a customer."""
    _get_master_or_404(con, customer_id)
    row = fetch_one(
        con,
        "SELECT * FROM customer_risk_scores WHERE customer_id = ?",
        [customer_id],
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No risk record for customer {customer_id}",
        )
    return RiskDetail(**row)
