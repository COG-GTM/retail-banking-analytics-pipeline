# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline bootstrap
# MAGIC Shared helper imported by every step notebook. Puts the ``databricks/``
# MAGIC project root (which contains the ``common``, ``jobs`` and ``orchestration``
# MAGIC packages) on ``sys.path`` and builds the :class:`PipelineConfig` from the job
# MAGIC widgets, so each step notebook is a thin wrapper around a tested step function.

# COMMAND ----------

import os
import sys


def _project_root() -> str:
    """Locate the ``databricks/`` dir that holds the ``common`` package."""
    candidates = [os.path.abspath(os.path.join(os.getcwd(), "..")), os.getcwd()]
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        nb_path = ctx.notebookPath().get()
        # Deployed layout: <root>/databricks/notebooks/<name>; add <root>/databricks.
        candidates.insert(0, os.path.dirname(os.path.dirname("/Workspace" + nb_path)))
    except Exception:
        pass
    for cand in candidates:
        if os.path.isdir(os.path.join(cand, "common")):
            return cand
    return candidates[0]


sys.path.insert(0, _project_root())

# COMMAND ----------

from common.config import load_config  # noqa: E402
from common.spark_utils import get_spark  # noqa: E402

dbutils.widgets.text("catalog", "retail_banking_analytics")  # noqa: F821
dbutils.widgets.text("min_rows", "1000")  # noqa: F821
dbutils.widgets.text("run_date", "")  # noqa: F821

spark = get_spark()
config = load_config(spark=spark)
min_rows = int(dbutils.widgets.get("min_rows"))  # noqa: F821
