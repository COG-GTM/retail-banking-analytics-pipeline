"""Pydantic request/response models for the BI API."""
from __future__ import annotations

from datetime import date, datetime
from typing import Generic, Literal, Optional, TypeVar

from pydantic import BaseModel

# Flags are stored in the data products as single-character 'Y'/'N' strings.
Flag = Literal["Y", "N"]

T = TypeVar("T")


class CustomerListItem(BaseModel):
    """Subset of CUSTOMER_MASTER_PROFILE fields shown in list/search results."""

    customer_id: int
    full_name: Optional[str] = None
    age: Optional[int] = None
    state_code: Optional[str] = None
    segment_name: Optional[str] = None
    risk_tier: Optional[str] = None
    lifetime_value_score: Optional[float] = None
    engagement_score: Optional[float] = None
    composite_risk_score: Optional[float] = None
    total_balance: Optional[float] = None
    cross_sell_flag: Optional[str] = None
    upsell_flag: Optional[str] = None
    retention_risk_flag: Optional[str] = None
    watch_list_flag: Optional[str] = None


class SegmentDetail(BaseModel):
    """A row from customer_segments."""

    customer_id: int
    segment_name: Optional[str] = None
    segment_id: Optional[int] = None
    subsegment_id: Optional[int] = None
    lifetime_value_score: Optional[float] = None
    engagement_score: Optional[float] = None
    digital_adoption_score: Optional[float] = None
    product_breadth_index: Optional[float] = None
    tenure_group: Optional[str] = None
    age_group: Optional[str] = None
    balance_tier: Optional[str] = None
    channel_preference: Optional[str] = None
    cross_sell_flag: Optional[str] = None
    upsell_flag: Optional[str] = None
    retention_risk_flag: Optional[str] = None
    model_version: Optional[str] = None
    effective_date: Optional[date] = None
    load_ts: Optional[datetime] = None


class TransactionDetail(BaseModel):
    """A row from transaction_analytics."""

    customer_id: int
    total_accounts: Optional[int] = None
    active_accounts: Optional[int] = None
    total_transactions: Optional[int] = None
    total_debit_amt: Optional[float] = None
    total_credit_amt: Optional[float] = None
    total_fees: Optional[float] = None
    top_spend_category: Optional[str] = None
    net_cash_flow: Optional[float] = None
    avg_transaction_size: Optional[float] = None
    digital_txn_pct: Optional[float] = None
    monthly_spend_trend: Optional[str] = None
    fee_income: Optional[float] = None
    interest_income: Optional[float] = None
    revenue_contribution: Optional[float] = None
    spend_percentile: Optional[float] = None
    anomaly_flag: Optional[str] = None
    reporting_period: Optional[str] = None
    model_version: Optional[str] = None
    effective_date: Optional[date] = None
    load_ts: Optional[datetime] = None


class RiskDetail(BaseModel):
    """A row from customer_risk_scores."""

    customer_id: int
    composite_risk_score: Optional[float] = None
    risk_tier: Optional[str] = None
    probability_of_default: Optional[float] = None
    credit_risk_component: Optional[float] = None
    behaviour_risk_component: Optional[float] = None
    velocity_risk_component: Optional[float] = None
    bureau_score_component: Optional[float] = None
    payment_history_component: Optional[float] = None
    primary_risk_driver: Optional[str] = None
    secondary_risk_driver: Optional[str] = None
    score_delta_30d: Optional[float] = None
    watch_list_flag: Optional[str] = None
    review_required_flag: Optional[str] = None
    model_version: Optional[str] = None
    effective_date: Optional[date] = None
    load_ts: Optional[datetime] = None


class CustomerDetail(BaseModel):
    """Full CUSTOMER_MASTER_PROFILE (Golden Record) with nested drill-down data."""

    customer_id: int
    full_name: Optional[str] = None
    age: Optional[int] = None
    state_code: Optional[str] = None
    customer_since: Optional[date] = None
    tenure_months: Optional[int] = None
    customer_status: Optional[str] = None
    segment_name: Optional[str] = None
    lifetime_value_score: Optional[float] = None
    engagement_score: Optional[float] = None
    total_accounts: Optional[int] = None
    active_accounts: Optional[float] = None
    total_balance: Optional[float] = None
    total_credit_limit: Optional[float] = None
    credit_utilization_pct: Optional[float] = None
    monthly_transactions: Optional[int] = None
    monthly_spend: Optional[float] = None
    net_cash_flow: Optional[float] = None
    top_spend_category: Optional[str] = None
    digital_txn_pct: Optional[float] = None
    composite_risk_score: Optional[float] = None
    risk_tier: Optional[str] = None
    probability_of_default: Optional[float] = None
    watch_list_flag: Optional[str] = None
    cross_sell_flag: Optional[str] = None
    upsell_flag: Optional[str] = None
    retention_risk_flag: Optional[str] = None
    model_version: Optional[str] = None
    effective_date: Optional[date] = None
    load_ts: Optional[datetime] = None

    # Nested drill-down data from the component tables.
    segment: Optional[SegmentDetail] = None
    transactions: Optional[TransactionDetail] = None
    risk: Optional[RiskDetail] = None


class FlagUpdate(BaseModel):
    """PATCH body for actionable flags. All optional; values constrained to 'Y'/'N'."""

    cross_sell_flag: Optional[Flag] = None
    upsell_flag: Optional[Flag] = None
    retention_risk_flag: Optional[Flag] = None
    watch_list_flag: Optional[Flag] = None


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic wrapper for paginated list responses."""

    items: list[T]
    total: int
    limit: int
    offset: int


class SegmentDistribution(BaseModel):
    segment_name: Optional[str] = None
    customer_count: int


class RiskDistribution(BaseModel):
    risk_tier: Optional[str] = None
    customer_count: int


class SpendTrendSummary(BaseModel):
    monthly_spend_trend: Optional[str] = None
    customer_count: int


class SegmentSummary(BaseModel):
    segment_name: str
    customer_count: int
    avg_lifetime_value: Optional[float] = None
    avg_composite_risk_score: Optional[float] = None


class TopCustomer(BaseModel):
    customer_id: int
    full_name: Optional[str] = None
    segment_name: Optional[str] = None
    lifetime_value_score: Optional[float] = None
    risk_tier: Optional[str] = None
    total_balance: Optional[float] = None
