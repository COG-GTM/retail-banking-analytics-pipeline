"""Contract stub for synapse/common/audit.py (owner: T9). See C2."""
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, Literal, Optional
from common.config import PipelineConfig

Status = Literal["START", "SUCCESS", "WARNING", "ERROR"]
ENGINE_SPARK: str  # "SPARK"
STEP_FULL_LOAD: str  # "FULL_LOAD"

class AuditHandle:
    job_name: str
    step_name: str
    start_ts: datetime
    row_count: Optional[int]  # set by the caller before the context exits
    message: Optional[str]

def init_audit(cfg: PipelineConfig) -> None: ...
def log_step(
    cfg: PipelineConfig,
    job_name: str,
    step_name: str,
    status: Status,
    row_count: Optional[int] = None,
    message: Optional[str] = None,
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
) -> None: ...
def audit_step(cfg: PipelineConfig, job_name: str, step_name: str) -> AbstractContextManager[AuditHandle]: ...
def get_run_log(cfg: PipelineConfig, run_id: Optional[str] = None) -> list[dict[str, Any]]: ...
