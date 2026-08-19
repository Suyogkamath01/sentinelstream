"""Validate required documentation, local Markdown links, and workflow YAML."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CHANGELOG.md",
    "LICENSE",
    ".env.example",
    "docker-compose.yml",
    "docs/architecture.md",
    "docs/setup.md",
    "docs/api.md",
    "docs/model_card.md",
    "docs/dataset_card.md",
    "docs/features.md",
    "docs/research.md",
    "docs/demo.md",
)
LINK_PATTERN = re.compile(r"!??\[[^]]*\]\(([^)]+)\)")


def _check_required_files() -> list[str]:
    return [path for path in REQUIRED_FILES if not (ROOT / path).is_file()]


def _local_links(path: Path) -> list[str]:
    links: list[str] = []
    for raw_target in LINK_PATTERN.findall(path.read_text(encoding="utf-8")):
        target = raw_target.strip().split("#", 1)[0].split("?", 1)[0]
        if target and not target.startswith(("http://", "https://", "mailto:", "data:")):
            links.append(target)
    return links


def _check_markdown_links() -> list[str]:
    errors: list[str] = []
    for path in ROOT.rglob("*.md"):
        if any(part in {".venv", ".git", "mlruns", "reports"} for part in path.parts):
            continue
        for target in _local_links(path):
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT.resolve())
            except ValueError:
                errors.append(f"{path}: link escapes repository: {target}")
                continue
            if not resolved.exists():
                errors.append(f"{path}: missing link target: {target}")
    return errors


def _check_yaml_files() -> list[str]:
    errors: list[str] = []
    for path in sorted((ROOT / ".github/workflows").glob("*.y*ml")):
        try:
            payload: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            errors.append(f"{path}: invalid YAML: {exc}")
            continue
        if not isinstance(payload, dict) or "jobs" not in payload:
            errors.append(f"{path}: workflow must define jobs")
    try:
        compose: Any = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        if not isinstance(compose, dict) or not isinstance(compose.get("services"), dict):
            errors.append("docker-compose.yml: services mapping is required")
        else:
            for service_name, service in compose["services"].items():
                if not isinstance(service, dict):
                    continue
                build = service.get("build")
                if isinstance(build, dict) and isinstance(build.get("dockerfile"), str):
                    dockerfile = ROOT / build["dockerfile"]
                    if not dockerfile.is_file():
                        errors.append(
                            "docker-compose.yml: "
                            f"{service_name} references missing {build['dockerfile']}"
                        )
    except yaml.YAMLError as exc:
        errors.append(f"docker-compose.yml: invalid YAML: {exc}")
    for workflow in sorted((ROOT / ".github/workflows").glob("*.y*ml")):
        try:
            workflow_payload: Any = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        if not isinstance(workflow_payload, dict):
            continue
        for job_name, job in workflow_payload.get("jobs", {}).items():
            if not isinstance(job, dict):
                continue
            for step in job.get("steps", []):
                if not isinstance(step, dict):
                    continue
                with_values = step.get("with")
                dockerfile = with_values.get("file") if isinstance(with_values, dict) else None
                if (
                    isinstance(dockerfile, str)
                    and dockerfile.endswith(".Dockerfile")
                    and not (ROOT / dockerfile).is_file()
                ):
                    errors.append(f"{workflow}: {job_name} references missing {dockerfile}")
    return errors


def main() -> int:
    errors = [f"missing required file: {path}" for path in _check_required_files()]
    errors.extend(_check_markdown_links())
    errors.extend(_check_yaml_files())
    if errors:
        print("Documentation validation failed:")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("Documentation, local links, workflows, and Compose YAML are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
