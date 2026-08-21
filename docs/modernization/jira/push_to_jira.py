#!/usr/bin/env python3
"""Create the modernization tickets defined in tickets.json as Jira Cloud issues.

Reads connection settings from the environment:

    JIRA_BASE_URL     e.g. https://your-site.atlassian.net
    JIRA_EMAIL        Atlassian account email used for basic auth
    JIRA_API_TOKEN    Atlassian API token (https://id.atlassian.com/manage-profile/security/api-tokens)
    JIRA_PROJECT_KEY  Target project key, e.g. MBA

Usage:
    python push_to_jira.py --dry-run
    python push_to_jira.py
    python push_to_jira.py --tickets-file tickets.json --no-links
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth

DEFAULT_TICKETS_FILE = Path(__file__).with_name("tickets.json")
REQUIRED_ENV_VARS = ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT_KEY")
LINK_TYPE = "Blocks"
TIMEOUT_SECONDS = 60


class JiraError(RuntimeError):
    """Raised when the Jira REST API returns an error response."""


def load_config() -> Dict[str, str]:
    config = {name: os.environ.get(name, "").strip() for name in REQUIRED_ENV_VARS}
    missing = [name for name, value in config.items() if not value]
    if missing:
        raise SystemExit(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + "\nSee docs/modernization/jira/README.md for setup instructions."
        )
    config["JIRA_BASE_URL"] = config["JIRA_BASE_URL"].rstrip("/")
    return config


def load_tickets(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise SystemExit(f"Tickets file not found: {path}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Tickets file {path} is not valid JSON: {exc}")


def bullet_list(items: List[str]) -> List[Dict[str, Any]]:
    return [
        {
            "type": "bulletList",
            "content": [
                {
                    "type": "listItem",
                    "content": [text_paragraph(item)],
                }
                for item in items
            ],
        }
    ]


def text_paragraph(text: str) -> Dict[str, Any]:
    return {"type": "paragraph", "content": [{"type": "text", "text": text}]}


def heading(text: str) -> Dict[str, Any]:
    return {
        "type": "heading",
        "attrs": {"level": 3},
        "content": [{"type": "text", "text": text}],
    }


def build_description(ticket: Dict[str, Any], repository: str) -> Dict[str, Any]:
    """Build an Atlassian Document Format description for a ticket."""
    content: List[Dict[str, Any]] = [
        text_paragraph(f"Repository: {repository}"),
        heading("Context"),
        text_paragraph(ticket["context"]),
        heading("Scope"),
    ]
    content.extend(bullet_list(ticket["scope"]))
    content.append(heading("Acceptance criteria"))
    content.extend(bullet_list(ticket["acceptance_criteria"]))

    affected_files = ticket.get("affected_files") or []
    if affected_files:
        content.append(heading("Affected files"))
        content.extend(bullet_list(affected_files))

    dependencies = ticket.get("dependencies") or []
    content.append(heading("Dependencies"))
    if dependencies:
        content.extend(bullet_list([f"Blocked by {dep}" for dep in dependencies]))
    else:
        content.append(text_paragraph("None."))

    return {"type": "doc", "version": 1, "content": content}


def build_payload(ticket: Dict[str, Any], spec: Dict[str, Any], project_key: str) -> Dict[str, Any]:
    labels = ticket.get("labels") or spec.get("default_labels") or []
    issue_type = ticket.get("issue_type") or spec.get("default_issue_type", "Story")
    return {
        "fields": {
            "project": {"key": project_key},
            "issuetype": {"name": issue_type},
            "summary": ticket["summary"],
            "labels": sorted(set(labels)),
            "description": build_description(ticket, spec.get("repository", "")),
        }
    }


def describe_error(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:500]}"
    messages = body.get("errorMessages") or []
    field_errors = body.get("errors") or {}
    details = "; ".join(
        messages + [f"{field}: {message}" for field, message in field_errors.items()]
    )
    return f"HTTP {response.status_code}: {details or json.dumps(body)[:500]}"


class JiraClient:
    def __init__(self, base_url: str, email: str, api_token: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(email, api_token)
        self.session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )

    def create_issue(self, payload: Dict[str, Any]) -> str:
        response = self.session.post(
            f"{self.base_url}/rest/api/3/issue", json=payload, timeout=TIMEOUT_SECONDS
        )
        if response.status_code >= 400:
            raise JiraError(describe_error(response))
        return response.json()["key"]

    def create_link(self, blocker_key: str, blocked_key: str) -> None:
        payload = {
            "type": {"name": LINK_TYPE},
            "inwardIssue": {"key": blocker_key},
            "outwardIssue": {"key": blocked_key},
        }
        response = self.session.post(
            f"{self.base_url}/rest/api/3/issueLink", json=payload, timeout=TIMEOUT_SECONDS
        )
        if response.status_code >= 400:
            raise JiraError(describe_error(response))


def run(args: argparse.Namespace) -> int:
    spec = load_tickets(Path(args.tickets_file))
    tickets = spec.get("tickets") or []
    if not tickets:
        raise SystemExit(f"No tickets found in {args.tickets_file}")

    if args.dry_run:
        project_key = os.environ.get("JIRA_PROJECT_KEY", "DRYRUN").strip() or "DRYRUN"
        for ticket in tickets:
            payload = build_payload(ticket, spec, project_key)
            print(f"--- {ticket['id']} ---")
            print(json.dumps(payload, indent=2))
        print(f"\nDry run: {len(tickets)} issue(s) would be created in project {project_key}.")
        if not args.no_links:
            planned = [
                (dep, ticket["id"])
                for ticket in tickets
                for dep in ticket.get("dependencies") or []
            ]
            print(f"Dry run: {len(planned)} '{LINK_TYPE}' link(s) would be created:")
            for blocker, blocked in planned:
                print(f"  {blocker} blocks {blocked}")
        return 0

    config = load_config()
    client = JiraClient(
        config["JIRA_BASE_URL"], config["JIRA_EMAIL"], config["JIRA_API_TOKEN"]
    )

    created: Dict[str, str] = {}
    failures: List[str] = []

    for ticket in tickets:
        payload = build_payload(ticket, spec, config["JIRA_PROJECT_KEY"])
        try:
            key = client.create_issue(payload)
        except JiraError as exc:
            failures.append(f"{ticket['id']}: {exc}")
            print(f"FAILED  {ticket['id']}: {exc}", file=sys.stderr)
            continue
        created[ticket["id"]] = key
        print(f"CREATED {ticket['id']} -> {key} {config['JIRA_BASE_URL']}/browse/{key}")

    if not args.no_links:
        for ticket in tickets:
            blocked_key: Optional[str] = created.get(ticket["id"])
            if blocked_key is None:
                continue
            for dep in ticket.get("dependencies") or []:
                blocker_key = created.get(dep)
                if blocker_key is None:
                    print(
                        f"SKIPPED link {dep} blocks {ticket['id']}: {dep} was not created",
                        file=sys.stderr,
                    )
                    continue
                try:
                    client.create_link(blocker_key, blocked_key)
                except JiraError as exc:
                    failures.append(f"link {dep} -> {ticket['id']}: {exc}")
                    print(
                        f"FAILED  link {blocker_key} blocks {blocked_key}: {exc}",
                        file=sys.stderr,
                    )
                    continue
                print(f"LINKED  {blocker_key} blocks {blocked_key}")

    print(f"\nCreated {len(created)}/{len(tickets)} issue(s).")
    if failures:
        print(f"{len(failures)} operation(s) failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--tickets-file",
        default=str(DEFAULT_TICKETS_FILE),
        help="Path to the tickets definition file (default: tickets.json next to this script)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the issue payloads and planned links without calling Jira",
    )
    parser.add_argument(
        "--no-links",
        action="store_true",
        help="Create the issues but skip the dependency issue links",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
