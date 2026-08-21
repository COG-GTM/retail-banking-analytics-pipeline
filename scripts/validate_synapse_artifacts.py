#!/usr/bin/env python3
"""Static validation for the Synapse orchestration artifacts under synapse/.

Runs without an Azure or Snowflake connection: it parses every artifact, checks
cross-references, enforces the sequential/fail-fast activity ordering that the
shell orchestrator had, and asserts that no configuration value or credential is
hardcoded outside the per-environment parameter files.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACTS_DIR = REPO_ROOT / "synapse"
ENVIRONMENTS = ("dev", "uat", "prod")

# Notebooks delivered by TICKET-03..TICKET-09; referenced here by name only.
EXPECTED_NOTEBOOKS = {
    "nb_run_dbt",
    "nb_customer_segments",
    "nb_txn_analytics",
    "nb_risk_scoring",
    "nb_data_products",
}

SECRET_LITERAL_PATTERNS = (
    re.compile(r'"(password|privateKey|privateKeyPassphrase|accessToken|clientSecret)"\s*:\s*"[^"@]'),
    re.compile(r"\{SAS004\}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)


class Findings:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            self.errors.append(message)


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def iter_activities(activities: list[dict]):
    """Yield (scope_name, activities_list) for the pipeline and every nested scope."""
    yield activities
    for activity in activities:
        props = activity.get("typeProperties", {})
        for key in ("ifTrueActivities", "ifFalseActivities", "activities"):
            nested = props.get(key)
            if nested:
                yield from iter_activities(nested)
        for case in props.get("cases", []) or []:
            nested = case.get("activities")
            if nested:
                yield from iter_activities(nested)
        default_activities = props.get("defaultActivities")
        if default_activities:
            yield from iter_activities(default_activities)


def validate_names(artifacts: dict[str, dict[str, Path]], findings: Findings) -> None:
    for kind, entries in artifacts.items():
        for name, path in entries.items():
            findings.check(
                path.stem == name,
                f"{kind} '{name}' is defined in {path.name}; file name must match the artifact name",
            )


def validate_pipeline(
    name: str,
    body: dict,
    pipelines: set[str],
    linked_services: dict[str, dict],
    findings: Findings,
) -> None:
    props = body["properties"]
    activities = props.get("activities", [])
    findings.check(bool(activities), f"pipeline '{name}' has no activities")

    for scope in iter_activities(activities):
        names_in_scope = [activity["name"] for activity in scope]
        findings.check(
            len(names_in_scope) == len(set(names_in_scope)),
            f"pipeline '{name}' has duplicate activity names in one scope: {names_in_scope}",
        )
        for index, activity in enumerate(scope):
            depends = activity.get("dependsOn", [])
            if index > 0:
                findings.check(
                    bool(depends),
                    f"pipeline '{name}' activity '{activity['name']}' has no dependsOn; "
                    "sequential fail-fast ordering would be lost",
                )
            for dependency in depends:
                findings.check(
                    dependency["activity"] in names_in_scope,
                    f"pipeline '{name}' activity '{activity['name']}' depends on unknown "
                    f"activity '{dependency['activity']}'",
                )

            activity_props = activity.get("typeProperties", {})
            if activity.get("type") == "ExecutePipeline":
                target = activity_props["pipeline"]["referenceName"]
                findings.check(
                    target in pipelines,
                    f"pipeline '{name}' activity '{activity['name']}' calls unknown pipeline '{target}'",
                )
            if activity.get("type") == "SynapseNotebook":
                notebook = activity_props["notebook"]["referenceName"]
                findings.check(
                    notebook in EXPECTED_NOTEBOOKS,
                    f"pipeline '{name}' activity '{activity['name']}' references unexpected "
                    f"notebook '{notebook}'",
                )
                findings.check(
                    "sparkPool" in activity_props,
                    f"pipeline '{name}' activity '{activity['name']}' has no sparkPool reference",
                )
            linked_service = activity.get("linkedServiceName")
            if linked_service:
                ls_name = linked_service["referenceName"]
                findings.check(
                    ls_name in linked_services,
                    f"pipeline '{name}' activity '{activity['name']}' references unknown "
                    f"linked service '{ls_name}'",
                )
                if ls_name in linked_services:
                    declared = set(linked_services[ls_name]["properties"].get("parameters", {}))
                    supplied = set(linked_service.get("parameters", {}))
                    missing = declared - supplied
                    findings.check(
                        not missing,
                        f"pipeline '{name}' activity '{activity['name']}' omits linked service "
                        f"parameters {sorted(missing)}",
                    )


def validate_environments(entry_pipeline: dict, artifacts_dir: Path, findings: Findings) -> None:
    entry_params = entry_pipeline["properties"]["parameters"]
    required = {n for n, spec in entry_params.items() if "defaultValue" not in spec}

    key_sets: dict[str, set[str]] = {}
    for env in ENVIRONMENTS:
        path = artifacts_dir / "config" / f"{env}.parameters.json"
        findings.check(path.exists(), f"missing environment parameter file {path.name}")
        if not path.exists():
            continue
        config = load_json(path)
        for key in ("environment", "workspace", "pipelineParameters"):
            findings.check(key in config, f"{path.name} is missing top-level key '{key}'")
        workspace = config.get("workspace", {})
        for key in ("name", "resourceGroup", "sparkPool"):
            findings.check(key in workspace, f"{path.name} workspace block is missing '{key}'")

        params = config.get("pipelineParameters", {})
        key_sets[env] = set(params)
        unknown = set(params) - set(entry_params)
        findings.check(
            not unknown,
            f"{path.name} sets parameters that pl_retail_banking_analytics does not declare: {sorted(unknown)}",
        )
        missing = required - set(params)
        findings.check(
            not missing,
            f"{path.name} does not supply required pipeline parameters {sorted(missing)}",
        )

    if len(key_sets) > 1:
        reference_env, reference_keys = next(iter(key_sets.items()))
        for env, keys in key_sets.items():
            findings.check(
                keys == reference_keys,
                f"{env}.parameters.json parameter keys differ from {reference_env}.parameters.json: "
                f"{sorted(keys.symmetric_difference(reference_keys))}",
            )


def validate_no_secret_literals(artifacts_dir: Path, findings: Findings) -> None:
    for path in sorted(artifacts_dir.rglob("*.json")):
        text = path.read_text(encoding="utf-8")
        display = path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path
        for pattern in SECRET_LITERAL_PATTERNS:
            findings.check(
                pattern.search(text) is None,
                f"{display} appears to contain a hardcoded credential (matched {pattern.pattern})",
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", type=Path, default=DEFAULT_ARTIFACTS_DIR)
    args = parser.parse_args()
    artifacts_dir: Path = args.artifacts_dir

    findings = Findings()

    artifact_paths = {
        kind: {path.stem: path for path in sorted((artifacts_dir / kind).glob("*.json"))}
        for kind in ("pipeline", "linkedService", "trigger")
    }
    validate_names(artifact_paths, findings)

    pipelines = {name: load_json(path) for name, path in artifact_paths["pipeline"].items()}
    linked_services = {name: load_json(path) for name, path in artifact_paths["linkedService"].items()}
    triggers = {name: load_json(path) for name, path in artifact_paths["trigger"].items()}

    findings.check(
        "pl_retail_banking_analytics" in pipelines,
        "entry pipeline pl_retail_banking_analytics is missing",
    )

    for name, body in pipelines.items():
        validate_pipeline(name, body, set(pipelines), linked_services, findings)

    for name, body in triggers.items():
        for reference in body["properties"].get("pipelines", []):
            target = reference["pipelineReference"]["referenceName"]
            findings.check(
                target in pipelines,
                f"trigger '{name}' references unknown pipeline '{target}'",
            )

    if "pl_retail_banking_analytics" in pipelines:
        validate_environments(pipelines["pl_retail_banking_analytics"], artifacts_dir, findings)

    validate_no_secret_literals(artifacts_dir, findings)

    if findings.errors:
        print(f"FAILED: {len(findings.errors)} problem(s) found")
        for error in findings.errors:
            print(f"  - {error}")
        return 1

    print(
        f"OK: {len(pipelines)} pipeline(s), {len(linked_services)} linked service(s), "
        f"{len(triggers)} trigger(s) validated"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
