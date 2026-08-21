#!/usr/bin/env python3
"""spark-submit entrypoint for the risk scoring job (Synapse Spark job definition).

spark-submit --py-files risk_scoring.zip spark/jobs/03_risk_scoring.py --source snowflake
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from risk_scoring.job import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
