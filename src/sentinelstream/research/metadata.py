"""Environment and source metadata captured with every research output."""

from __future__ import annotations

import importlib.metadata
import os
import platform
import sys
from pathlib import Path
from typing import Any

from sentinelstream.research.config import ResearchConfig


def _source_revision() -> str | None:
    github_revision = os.getenv("GITHUB_SHA")
    if github_revision:
        return github_revision
    root = Path(__file__).resolve().parents[3]
    git_dir = root / ".git"
    try:
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            ref_path = git_dir / head.removeprefix("ref: ")
            return ref_path.read_text(encoding="utf-8").strip() or None
        return head or None
    except OSError:
        return None


def collect_reproducibility_metadata(
    config: ResearchConfig, *, dataset_metadata: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Capture only non-sensitive metadata needed to reproduce a run."""

    try:
        package_version = importlib.metadata.version("sentinelstream")
    except importlib.metadata.PackageNotFoundError:
        package_version = "editable-or-unknown"
    return {
        "experiment_id": config.experiment_id,
        "dataset_version": config.dataset_version,
        "feature_set_version": config.feature_set_version,
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "package_version": package_version,
        "source_revision": _source_revision(),
        "random_seed": config.random_seed,
        "dataset_metadata": dataset_metadata,
    }
