"""Contract test: the gold schemas must match ``ddl/02_data_product_tables.sql``.

Column names, Teradata -> Delta types and ordering are all compared position by
position, so any drift between the Databricks implementation and the published
downstream contract fails here rather than in a consumer's dashboard.

    python -m pytest databricks/tests/test_gold_schema_contract.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "databricks"))

from shared.schemas import GOLD_SCHEMAS  # noqa: E402

DDL_PATH = REPO_ROOT / "ddl" / "02_data_product_tables.sql"

TYPE_MAP = {
    "BIGINT": "bigint",
    "INTEGER": "int",
    "SMALLINT": "smallint",
    "DATE": "date",
}


def teradata_to_delta(teradata_type: str) -> str:
    teradata_type = teradata_type.strip().upper()
    if teradata_type in TYPE_MAP:
        return TYPE_MAP[teradata_type]
    if teradata_type.startswith(("VARCHAR", "CHAR")):
        return "string"
    if teradata_type.startswith("TIMESTAMP"):
        return "timestamp"
    if teradata_type.startswith("DECIMAL"):
        return "decimal(" + teradata_type.split("(", 1)[1].split(")", 1)[0].replace(" ", "") + ")"
    raise AssertionError(f"unmapped Teradata type: {teradata_type}")


def parse_ddl(text: str) -> dict[str, list[tuple[str, str]]]:
    tables: dict[str, list[tuple[str, str]]] = {}
    pattern = re.compile(
        r"CREATE\s+MULTISET\s+TABLE\s+DATA_PRODUCTS_DB\.(\w+)[^(]*\((.*?)\n\)", re.DOTALL
    )
    column = re.compile(
        r"^\s*(\w+)\s+"
        r"(BIGINT|INTEGER|SMALLINT|DATE|TIMESTAMP\(\d+\)|DECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)"
        r"|VARCHAR\s*\(\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\))",
        re.IGNORECASE,
    )
    for name, body in pattern.findall(text):
        columns = []
        for line in body.splitlines():
            line = line.split("--", 1)[0]
            match = column.match(line)
            if match:
                columns.append((match.group(1).upper(), teradata_to_delta(match.group(2))))
        tables[name.upper()] = columns
    return tables


def test_gold_schemas_match_ddl() -> None:
    ddl_tables = parse_ddl(DDL_PATH.read_text())
    assert set(GOLD_SCHEMAS) <= set(ddl_tables), (
        f"tables missing from DDL: {set(GOLD_SCHEMAS) - set(ddl_tables)}"
    )
    for table, spec in GOLD_SCHEMAS.items():
        assert spec == ddl_tables[table], (
            f"{table} does not match the DDL contract:\n"
            f"  implementation: {spec}\n"
            f"  ddl:            {ddl_tables[table]}"
        )


if __name__ == "__main__":
    test_gold_schemas_match_ddl()
    print(f"OK — {len(GOLD_SCHEMAS)} gold tables match {DDL_PATH.relative_to(REPO_ROOT)}")
