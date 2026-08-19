"""Small content-addressed dataset metadata store."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from sentinelstream.monitoring.drift import DatasetMetadata, build_dataset_metadata


class DatasetVersionStore:
    """Persist dataset manifests without duplicating source data."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def create(
        self,
        frame: pd.DataFrame,
        *,
        version: str,
        source: str,
        feature_set_version: str,
        split_metadata: dict[str, object] | None = None,
    ) -> DatasetMetadata:
        metadata = build_dataset_metadata(
            frame,
            version=version,
            source=source,
            feature_set_version=feature_set_version,
            split_metadata=split_metadata,
        )
        self.save(metadata)
        return metadata

    def save(self, metadata: DatasetMetadata) -> Path:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"{metadata.version}.json"
        path.write_text(json.dumps(metadata.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def load(self, version: str) -> DatasetMetadata:
        path = self.directory / f"{version}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        return DatasetMetadata(
            version=payload["version"],
            row_count=int(payload["row_count"]),
            columns=tuple(payload["columns"]),
            dtypes=dict(payload["dtypes"]),
            checksum=payload["checksum"],
            generated_at=payload["generated_at"],
            source=payload["source"],
            feature_set_version=payload["feature_set_version"],
            split_metadata=dict(payload["split_metadata"]),
        )
