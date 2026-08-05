"""Render the end-to-end run's metrics JSON as one self-contained HTML file.

Everything — data, CSS and the little bit of script that draws the bar charts — is inlined, so
the file renders from ``file://`` and as a downloaded attachment. Charts are plain SVG built from
the metrics at generation time rather than fetched at load time, because a browser will not let a
local file load anything.

Nothing here computes a metric: every number rendered comes from the JSON produced by the actual
run (:mod:`orchestration.metrics`).
"""

from __future__ import annotations

import argparse
import html
import json
import logging
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path

LOGGER = logging.getLogger(__name__)

STATUS_CLASS = {
    "SUCCESS": "ok",
    "PASS": "ok",
    "WARNING": "warn",
    "WARN": "warn",
    "FAILED": "bad",
    "FAIL": "bad",
    "SKIPPED": "muted",
}

CSS = """
:root {
  --bg: #0f1420; --panel: #171e2e; --panel-2: #1e2739; --line: #2a3550;
  --fg: #e8edf7; --muted: #93a1bd; --ok: #3ddc97; --warn: #ffc857; --bad: #ff6b6b;
  --accent: #6aa8ff; --accent-2: #b18cff;
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--bg); color: var(--fg);
  font: 14px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
header {
  padding: 32px 40px 24px; border-bottom: 1px solid var(--line);
  background: linear-gradient(135deg, #16203a 0%, #0f1420 70%);
}
h1 { margin: 0 0 6px; font-size: 26px; letter-spacing: -0.02em; }
h2 { font-size: 19px; margin: 0 0 4px; }
h3 { font-size: 15px; margin: 22px 0 8px; color: var(--muted); font-weight: 600; }
.sub { color: var(--muted); font-size: 13px; }
main { padding: 8px 40px 64px; max-width: 1400px; margin: 0 auto; }
section { margin: 34px 0; }
.section-head { border-left: 3px solid var(--accent); padding-left: 12px; margin-bottom: 16px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }
.card {
  background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px;
}
.card .k { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .06em; }
.card .v { font-size: 22px; font-weight: 650; margin-top: 4px; }
.card .v.small { font-size: 15px; font-weight: 550; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
tbody tr:hover { background: rgba(106,168,255,.06); }
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
.scroll { overflow-x: auto; }
.pill { display: inline-block; padding: 1px 9px; border-radius: 999px; font-size: 11.5px; font-weight: 650; }
.pill.ok { background: rgba(61,220,151,.15); color: var(--ok); }
.pill.warn { background: rgba(255,200,87,.15); color: var(--warn); }
.pill.bad { background: rgba(255,107,107,.15); color: var(--bad); }
.pill.muted { background: rgba(147,161,189,.15); color: var(--muted); }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px; }
.bar-row { display: grid; grid-template-columns: 220px 1fr 90px; gap: 10px; align-items: center; margin: 5px 0; }
.bar { height: 16px; border-radius: 4px; background: linear-gradient(90deg, var(--accent), var(--accent-2)); }
.bar-bg { background: var(--panel-2); border-radius: 4px; }
.lineage { display: grid; gap: 12px; }
.hop { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }
.hop .flow { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 8px; }
.node { background: var(--panel-2); border: 1px solid var(--line); border-radius: 6px; padding: 3px 9px; font-size: 12px; }
.node.out { border-color: var(--accent); color: var(--accent); }
.arrow { color: var(--muted); }
.rule { color: var(--fg); font-size: 13px; }
.rule .label { color: var(--muted); text-transform: uppercase; font-size: 11px; letter-spacing: .06em; display: block; margin-bottom: 2px; }
details { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; margin: 8px 0; }
summary { cursor: pointer; padding: 12px 16px; font-weight: 600; }
summary::marker { color: var(--accent); }
details > div { padding: 0 16px 16px; }
.grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(430px, 1fr)); gap: 16px; }
footer { color: var(--muted); font-size: 12px; padding: 24px 40px 48px; border-top: 1px solid var(--line); }
.warnbox { border-left: 3px solid var(--warn); background: rgba(255,200,87,.07); padding: 10px 14px; border-radius: 0 8px 8px 0; margin: 10px 0; }
"""

SCRIPT = """
document.querySelectorAll('[data-tabs]').forEach(function (group) {
  var buttons = group.querySelectorAll('button[data-tab]');
  buttons.forEach(function (button) {
    button.addEventListener('click', function () {
      buttons.forEach(function (other) { other.classList.remove('active'); });
      button.classList.add('active');
      group.querySelectorAll('[data-panel]').forEach(function (panel) {
        panel.style.display = panel.dataset.panel === button.dataset.tab ? 'block' : 'none';
      });
    });
  });
});
"""


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _fmt(value: object) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return esc(value)


def pill(status: str) -> str:
    return f'<span class="pill {STATUS_CLASS.get(status, "muted")}">{esc(status)}</span>'


def card(key: str, value: object, *, small: bool = False) -> str:
    klass = "v small" if small else "v"
    return f'<div class="card"><div class="k">{esc(key)}</div><div class="{klass}">{_fmt(value)}</div></div>'


def table(
    headers: Sequence[str], rows: Iterable[Sequence[str]], *, numeric: Sequence[int] = ()
) -> str:
    head = "".join(
        f'<th class="num">{esc(h)}</th>' if i in numeric else f"<th>{esc(h)}</th>"
        for i, h in enumerate(headers)
    )
    body = []
    for row in rows:
        cells = "".join(
            f'<td class="num">{cell}</td>' if i in numeric else f"<td>{cell}</td>"
            for i, cell in enumerate(row)
        )
        body.append(f"<tr>{cells}</tr>")
    return (
        f'<div class="scroll"><table><thead><tr>{head}</tr></thead>'
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


def bar_chart(pairs: Sequence[tuple[str, float]], *, unit: str = "") -> str:
    if not pairs:
        return '<p class="sub">No data.</p>'
    largest = max(value for _, value in pairs) or 1.0
    rows = []
    for label, value in pairs:
        width = max(1.0, 100.0 * value / largest)
        rows.append(
            f'<div class="bar-row"><div class="mono">{esc(label)}</div>'
            f'<div class="bar-bg"><div class="bar" style="width:{width:.1f}%"></div></div>'
            f'<div class="num mono">{value:,.2f}{esc(unit)}</div></div>'
        )
    return "".join(rows)


def _topology_section(topology: dict[str, object]) -> str:
    schema_rows = [
        (f"<code>{esc(legacy)}</code>", f"<code>{esc(schema)}</code>")
        for legacy, schema in sorted(dict(topology.get("schema_map") or {}).items())
    ]
    conf = dict(topology.get("spark_conf") or {})
    cards = "".join(
        [
            card("Engine", f"{topology.get('engine')} {topology.get('spark_version')}", small=True),
            card("Database", topology.get("database"), small=True),
            card("Master", topology.get("master"), small=True),
            card("Python", topology.get("python_version"), small=True),
            card("Run date", topology.get("run_date"), small=True),
            card("Lookback months", topology.get("lookback_months")),
            card("Risk score threshold", topology.get("risk_score_threshold")),
            card("min_rows (this run)", topology.get("min_rows")),
        ]
    )
    threshold_note = ""
    if topology.get("min_rows") != topology.get("min_rows_production"):
        threshold_note = (
            f'<div class="warnbox">The legacy <code>%validate_table</code> row floor is '
            f"<b>{_fmt(topology.get('min_rows_production'))}</b> rows in production. This run used "
            f"<b>{_fmt(topology.get('min_rows'))}</b> because the committed sample extract is a "
            f"500-customer slice; the check itself is unchanged.</div>"
        )
    return f"""
<section>
  <div class="section-head"><h2>Deployment topology</h2>
  <div class="sub">Where this run executed and against what.</div></div>
  <div class="cards">{cards}</div>
  {threshold_note}
  <div class="grid-2" style="margin-top:16px">
    <div class="panel"><h3>Legacy database &rarr; PostgreSQL schema</h3>
      {table(["Teradata database", "PostgreSQL schema"], schema_rows)}
      <p class="sub" style="margin-top:10px">JDBC: <code>{esc(topology.get("jdbc_url"))}</code></p>
    </div>
    <div class="panel"><h3>Spark configuration</h3>
      {table(["Setting", "Value"], [(f"<code>{esc(k)}</code>", f"<code>{esc(v)}</code>") for k, v in conf.items()])}
    </div>
  </div>
</section>"""


def _jobs_section(metrics: dict[str, object]) -> str:
    jobs = list(metrics.get("jobs") or [])
    run = dict(metrics.get("run") or {})
    rows = []
    for job in jobs:
        rows.append(
            (
                f"<code>{esc(job.get('job_name'))}</code>",
                f"<code>{esc(job.get('legacy_source'))}</code>",
                pill(str(job.get("status"))),
                _fmt(job.get("row_count")),
                f"{float(job.get('elapsed_seconds') or 0):.2f}",
                f"<code>{esc(job.get('target_table'))}</code>",
                esc(job.get("error") or ""),
            )
        )
    timings = [
        (str(job.get("job_name")), float(job.get("elapsed_seconds") or 0.0))
        for job in jobs
        if job.get("status") != "SKIPPED"
    ]
    succeeded = sum(1 for job in jobs if job.get("status") == "SUCCESS")
    total_rows = sum(int(job.get("row_count") or 0) for job in jobs)
    cards = "".join(
        [
            card("Jobs succeeded", f"{succeeded} / {len(jobs)}", small=True),
            card("Wall clock (s)", float(run.get("elapsed_seconds") or 0.0)),
            card("Rows written", total_rows),
            card("Return code", run.get("return_code")),
        ]
    )
    return f"""
<section>
  <div class="section-head"><h2>Pipeline execution</h2>
  <div class="sub">One end-to-end DAG run against PostgreSQL, in legacy dependency order.</div></div>
  <div class="cards">{cards}</div>
  <div class="panel" style="margin-top:16px">
    {
        table(
            ["Job", "Legacy source", "Status", "Rows", "Seconds", "Target table", "Error"],
            rows,
            numeric=(3, 4),
        )
    }
  </div>
  <div class="panel" style="margin-top:16px"><h3>Wall clock per job</h3>{
        bar_chart(timings, unit="s")
    }</div>
</section>"""


def _lineage_section(metrics: dict[str, object]) -> str:
    hops = []
    row_counts = dict(metrics.get("row_counts") or {})
    for edge in metrics.get("lineage") or []:
        inputs = "".join(
            f'<span class="node">{esc(name)}<span class="sub"> {_fmt(row_counts.get(name, 0))}</span></span>'
            f'<span class="arrow">+</span>'
            for name in edge.get("inputs") or []
        )
        inputs = inputs.removesuffix('<span class="arrow">+</span>')
        output = edge.get("output")
        hops.append(
            f"""<div class="hop">
  <div class="flow">{inputs}<span class="arrow">&rarr;</span>
    <span class="node out">{esc(output)}<span class="sub"> {_fmt(row_counts.get(output, 0))}</span></span>
  </div>
  <div class="rule"><span class="label">{esc(edge.get("job"))} &middot; {esc(edge.get("legacy_source"))}</span>
    {esc(edge.get("business_rule"))}</div>
</div>"""
        )
    return f"""
<section>
  <div class="section-head"><h2>Forward-engineering lineage</h2>
  <div class="sub">Source &rarr; staging &rarr; data product, with the business rule applied at each hop and the rows it produced.</div></div>
  <div class="lineage">{"".join(hops)}</div>
</section>"""


def _validation_section(metrics: dict[str, object]) -> str:
    rows = []
    for validation in metrics.get("validations") or []:
        for check in validation.get("checks") or []:
            rows.append(
                (
                    f"<code>{esc(validation.get('table'))}</code>",
                    esc(check.get("name")),
                    pill(str(check.get("status"))),
                    esc(check.get("detail")),
                )
            )
    audit_rows = [
        (
            esc(record.get("LOG_TS")),
            f"<code>{esc(record.get('JOB_NAME'))}</code>",
            pill(str(record.get("STATUS"))),
            _fmt(record.get("ROW_COUNT")) if record.get("ROW_COUNT") is not None else "",
            esc(record.get("MESSAGE")),
        )
        for record in metrics.get("audit_trail") or []
    ]
    failures = sum(
        1
        for validation in metrics.get("validations") or []
        for check in validation.get("checks") or []
        if check.get("status") == "FAIL"
    )
    return f"""
<section>
  <div class="section-head"><h2>Data quality</h2>
  <div class="sub">The legacy <code>%validate_table</code> checks, re-run against the tables as they sit in PostgreSQL
  ({failures} failing).</div></div>
  <div class="panel">{table(["Table", "Check", "Status", "Detail"], rows)}</div>
  <h3>Persisted audit trail (ETL_STAGING_DB.PIPELINE_AUDIT)</h3>
  <div class="panel">{table(["Logged at", "Job", "Status", "Rows", "Message"], audit_rows, numeric=(3,))}</div>
</section>"""


def _profile_section(metrics: dict[str, object]) -> str:
    blocks = []
    for profile in metrics.get("tables") or []:
        column_rows = [
            (
                f"<code>{esc(column.get('name'))}</code>",
                f"<code>{esc(column.get('type'))}</code>",
                _fmt(column.get("nulls")),
                f"{float(column.get('null_pct') or 0):.1f}%",
                _fmt(column.get("distinct")),
                _fmt(column.get("min")),
                _fmt(column.get("max")),
                _fmt(column.get("mean")) if column.get("mean") is not None else "",
            )
            for column in profile.get("columns") or []
        ]
        sample = profile.get("sample") or []
        sample_headers = list(sample[0].keys()) if sample else []
        sample_rows = [[_fmt(row.get(key)) for key in sample_headers] for row in sample]
        partition = ", ".join(profile.get("partition_by") or []) or "&mdash;"
        blocks.append(
            f"""<details>
  <summary>{esc(profile.get("table"))} &middot; {_fmt(profile.get("row_count"))} rows
    &middot; {len(profile.get("columns") or [])} columns</summary>
  <div>
    <p class="sub">DDL: <code>{esc(profile.get("source_ddl"))}</code> &middot;
      primary index: <code>{esc(", ".join(profile.get("primary_index") or []) or "—")}</code> &middot;
      partitioned by: <code>{partition}</code></p>
    <h3>Column profile</h3>
    {table(["Column", "Type", "Nulls", "Null %", "Distinct", "Min", "Max", "Mean"], column_rows, numeric=(2, 3, 4, 5, 6, 7))}
    <h3>Row sample</h3>
    {table(sample_headers, sample_rows)}
  </div>
</details>"""
        )
    return f"""
<section>
  <div class="section-head"><h2>Table profiles and samples</h2>
  <div class="sub">Every source, staging and data-product table as persisted by this run.</div></div>
  {"".join(blocks)}
</section>"""


def _insight_tables(insights: dict[str, object]) -> str:
    blocks = []

    segments = insights.get("segment_distribution") or []
    if segments:
        rows = [
            (
                esc(row.get("SEGMENT_NAME")),
                _fmt(row.get("CUSTOMERS")),
                _fmt(row.get("AVG_LTV")),
                _fmt(row.get("AVG_ENGAGEMENT")),
                _fmt(row.get("CROSS_SELL")),
                _fmt(row.get("RETENTION_RISK")),
            )
            for row in segments
        ]
        chart = bar_chart(
            [(str(r.get("SEGMENT_NAME")), float(r.get("CUSTOMERS") or 0)) for r in segments]
        )
        blocks.append(
            f'<div class="panel"><h3>Customer segments</h3>{chart}'
            f"{table(['Segment', 'Customers', 'Avg LTV', 'Avg engagement', 'Cross-sell', 'Retention risk'], rows, numeric=(1, 2, 3, 4, 5))}</div>"
        )

    risk = insights.get("risk_tier_distribution") or []
    if risk:
        rows = [
            (
                pill(str(row.get("RISK_TIER"))),
                _fmt(row.get("CUSTOMERS")),
                _fmt(row.get("AVG_SCORE")),
                _fmt(row.get("WATCH_LIST")),
                _fmt(row.get("REVIEW")),
            )
            for row in risk
        ]
        chart = bar_chart([(str(r.get("RISK_TIER")), float(r.get("CUSTOMERS") or 0)) for r in risk])
        blocks.append(
            f'<div class="panel"><h3>Risk tiers</h3>{chart}'
            f"{table(['Tier', 'Customers', 'Avg score', 'Watch list', 'Review required'], rows, numeric=(1, 2, 3, 4))}</div>"
        )

    drivers = insights.get("risk_drivers") or []
    if drivers:
        chart = bar_chart(
            [(str(r.get("PRIMARY_RISK_DRIVER")), float(r.get("CUSTOMERS") or 0)) for r in drivers]
        )
        blocks.append(f'<div class="panel"><h3>Primary risk driver</h3>{chart}</div>')

    completeness = insights.get("completeness") or {}
    if completeness:
        total = float(completeness.get("TOTAL") or 1)
        rows = [
            (
                esc(key.replace("_", " ").title()),
                _fmt(value),
                f"{100.0 * float(value) / total:.1f}%",
            )
            for key, value in completeness.items()
            if key != "TOTAL"
        ]
        blocks.append(
            f'<div class="panel"><h3>Master profile completeness '
            f"({_fmt(completeness.get('TOTAL'))} customers)</h3>"
            f"{table(['Attribute', 'Customers', 'Share'], rows, numeric=(1, 2))}</div>"
        )

    return f'<div class="grid-2">{"".join(blocks)}</div>' if blocks else ""


def _insights_section(metrics: dict[str, object]) -> str:
    insights = dict(metrics.get("insights") or {})
    summary = dict(insights.get("transaction_summary") or {})
    cards = "".join(
        [
            card("Customers analysed", summary.get("CUSTOMERS", 0)),
            card("Transactions", summary.get("TRANSACTIONS", 0)),
            card("Debit volume", summary.get("DEBIT_VOLUME", 0)),
            card("Revenue contribution", summary.get("REVENUE", 0)),
            card("Avg digital share %", summary.get("AVG_DIGITAL_PCT", 0)),
            card("Spend anomalies", summary.get("ANOMALIES", 0)),
        ]
    )
    return f"""
<section>
  <div class="section-head"><h2>Business insights</h2>
  <div class="sub">Aggregated from the data products this run wrote — the same summaries the legacy SAS jobs printed to their listings.</div></div>
  <div class="cards">{cards}</div>
  <div style="margin-top:16px">{_insight_tables(insights)}</div>
</section>"""


def _scale_section(metrics: dict[str, object]) -> str:
    rows = [
        (esc(item.get("topic")), esc(item.get("detail")))
        for item in metrics.get("scale_recommendations") or []
    ]
    return f"""
<section>
  <div class="section-head"><h2>Scale recommendations</h2>
  <div class="sub">Extrapolated from this run's measurements toward the 10M-customer target.</div></div>
  <div class="panel">{table(["Topic", "Recommendation"], rows)}</div>
</section>"""


def render_html(metrics: dict[str, object]) -> str:
    """Build the whole report as one string."""

    topology = dict(metrics.get("topology") or {})
    run = dict(metrics.get("run") or {})
    generated = metrics.get("generated_at") or datetime.now().isoformat(timespec="seconds")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Retail Banking Analytics &mdash; PySpark migration run report</title>
<style>{CSS}</style>
</head>
<body>
<header>
  <h1>Retail Banking Analytics &mdash; PySpark migration run report</h1>
  <div class="sub">
    Teradata BTEQ + SAS pipeline re-implemented in PySpark, deployed to PostgreSQL and executed end to end.<br>
    Run {esc(run.get("run_timestamp"))} &middot; business date {esc(run.get("run_date"))} &middot;
    report generated {esc(generated)}
  </div>
</header>
<main>
{_topology_section(topology)}
{_jobs_section(metrics)}
{_lineage_section(metrics)}
{_validation_section(metrics)}
{_insights_section(metrics)}
{_profile_section(metrics)}
{_scale_section(metrics)}
</main>
<footer>
  Every figure in this report is read from the metrics JSON emitted by the run above
  (<code>orchestration.metrics</code>); nothing is hand-entered. Deviations from the legacy
  behaviour are documented in <code>pyspark/docs/MIGRATION_NOTES.md</code>.
</footer>
<script>{SCRIPT}</script>
</body>
</html>
"""


def write_report(metrics: dict[str, object], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_html(metrics), encoding="utf-8")
    LOGGER.info("report written to %s (%s bytes)", target, target.stat().st_size)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="render the run metrics as a self-contained HTML report"
    )
    parser.add_argument("--metrics-json", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    metrics = json.loads(Path(args.metrics_json).read_text(encoding="utf-8"))
    write_report(metrics, args.out)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
