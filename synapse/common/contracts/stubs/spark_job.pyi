"""Contract stub for every Spark job entry point in synapse/spark/ (T7, T8, T9). See C7 §3.

Each job file (customer_segments.py, txn_analytics.py, risk_scoring.py, data_products.py)
implements these three functions and ends with:

    if __name__ == "__main__":
        sys.exit(main())
"""
import argparse
from typing import Any, Optional
from pyspark.sql import SparkSession
from common.config import PipelineConfig

JOB_NAME: str  # audit JOB_NAME from C2 §4, e.g. "01_CUSTOMER_SEG"
MODEL_VERSION: str  # e.g. "SEG_V3.2"
TARGET_TABLE: str  # e.g. "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS"

def build_arg_parser() -> argparse.ArgumentParser: ...  # common args via config.add_common_args + job-specific args
def run(spark: SparkSession, cfg: PipelineConfig, **job_params: Any) -> int: ...  # returns rows written
def main(argv: Optional[list[str]] = None) -> int: ...
