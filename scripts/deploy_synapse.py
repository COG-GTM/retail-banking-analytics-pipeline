#!/usr/bin/env python3
"""Render and deploy the Synapse orchestration artifacts for one environment.

The artifacts under synapse/ are environment neutral. This script applies the
values from synapse/config/<env>.parameters.json - Spark pool name and the
default pipeline parameters that replace config/pipeline_config.cfg - and then
publishes the result with the Azure CLI.

Examples:
    python scripts/deploy_synapse.py --environment dev --dry-run
    python scripts/deploy_synapse.py --environment prod
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACTS_DIR = REPO_ROOT / "synapse"
ENTRY_PIPELINE = "pl_retail_banking_analytics"
DEPLOY_ORDER = ("linkedService", "pipeline", "trigger")
AZ_SUBCOMMAND = {
    "linkedService": "linked-service",
    "pipeline": "pipeline",
    "trigger": "trigger",
}


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def apply_spark_pool(node: object, spark_pool: str) -> None:
    if isinstance(node, dict):
        if node.get("type") == "BigDataPoolReference" and "referenceName" in node:
            node["referenceName"] = spark_pool
        for value in node.values():
            apply_spark_pool(value, spark_pool)
    elif isinstance(node, list):
        for item in node:
            apply_spark_pool(item, spark_pool)


def apply_pipeline_parameters(pipeline: dict, values: dict) -> None:
    declared = pipeline["properties"].get("parameters", {})
    for name, value in values.items():
        if name not in declared:
            raise SystemExit(
                f"{ENTRY_PIPELINE} does not declare parameter '{name}'; "
                "run scripts/validate_synapse_artifacts.py"
            )
        declared[name]["defaultValue"] = value


def execute_pipeline_references(node: object) -> set[str]:
    references: set[str] = set()
    if isinstance(node, dict):
        if node.get("type") == "PipelineReference" and "referenceName" in node:
            references.add(node["referenceName"])
        for value in node.values():
            references |= execute_pipeline_references(value)
    elif isinstance(node, list):
        for item in node:
            references |= execute_pipeline_references(item)
    return references


def order_pipelines(bodies: dict[str, dict]) -> list[str]:
    """Callee-before-caller order so ExecutePipeline references always resolve."""
    ordered: list[str] = []
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in ordered or name in visiting:
            return
        visiting.add(name)
        for dependency in sorted(execute_pipeline_references(bodies[name]) & set(bodies)):
            if dependency != name:
                visit(dependency)
        visiting.discard(name)
        ordered.append(name)

    for name in sorted(bodies):
        visit(name)
    return ordered


def render(artifacts_dir: Path, config: dict, out_dir: Path) -> list[tuple[str, str, Path]]:
    rendered: list[tuple[str, str, Path]] = []
    spark_pool = config["workspace"]["sparkPool"]
    for kind in DEPLOY_ORDER:
        source_dir = artifacts_dir / kind
        if not source_dir.is_dir():
            continue
        target_dir = out_dir / kind
        target_dir.mkdir(parents=True, exist_ok=True)
        bodies = {}
        for path in sorted(source_dir.glob("*.json")):
            body = load_json(path)
            apply_spark_pool(body, spark_pool)
            if kind == "pipeline" and body["name"] == ENTRY_PIPELINE:
                apply_pipeline_parameters(body, config["pipelineParameters"])
            target = target_dir / path.name
            target.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
            bodies[body["name"]] = (body, target)

        names = order_pipelines({name: body for name, (body, _) in bodies.items()}) if kind == "pipeline" else sorted(bodies)
        rendered.extend((kind, name, bodies[name][1]) for name in names)
    return rendered


def az_command(kind: str, name: str, path: Path, workspace: str) -> list[str]:
    return [
        "az",
        "synapse",
        AZ_SUBCOMMAND[kind],
        "create",
        "--workspace-name",
        workspace,
        "--name",
        name,
        "--file",
        f"@{path}",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--environment", required=True, choices=("dev", "uat", "prod"))
    parser.add_argument("--artifacts-dir", type=Path, default=DEFAULT_ARTIFACTS_DIR)
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "build" / "synapse")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="render the artifacts and print the deployment commands without calling Azure",
    )
    args = parser.parse_args()

    config_path = args.artifacts_dir / "config" / f"{args.environment}.parameters.json"
    config = load_json(config_path)
    out_dir: Path = args.out_dir / args.environment
    rendered = render(args.artifacts_dir, config, out_dir)
    workspace = config["workspace"]["name"]

    print(f"Rendered {len(rendered)} artifact(s) for {args.environment.upper()} into {out_dir}")
    for kind, name, path in rendered:
        command = az_command(kind, name, path, workspace)
        if args.dry_run:
            print(f"  would run: {shlex.join(command)}")
            continue
        print(f"  deploying {kind} {name}")
        subprocess.run(command, check=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
