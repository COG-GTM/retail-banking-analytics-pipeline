"""FastAPI application serving BI users access to the pipeline's data products.

On startup, the four SAS data product CSVs are loaded into an in-memory DuckDB
database. DuckDB is used as the analytical query engine, consistent with the
existing pipeline's use of DuckDB in export_data.py.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import init_db
from .routers import analytics, customers


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the SAS data product CSVs into in-memory DuckDB at startup.
    init_db()
    yield


app = FastAPI(
    title="Retail Banking Analytics — BI API",
    description=(
        "Actionable access to the retail banking pipeline's data products. "
        "Primary entity is CUSTOMER_MASTER_PROFILE (the Golden Record), with "
        "drill-down into customer_segments, transaction_analytics, and "
        "customer_risk_scores."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(customers.router)
app.include_router(analytics.router)


@app.get("/", tags=["meta"])
def root() -> dict:
    return {
        "service": "Retail Banking Analytics BI API",
        "docs": "/docs",
        "entities": [
            "customer_master_profile",
            "customer_segments",
            "transaction_analytics",
            "customer_risk_scores",
        ],
    }


@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}
