from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class SearchRequest:
    start_date: str
    end_date: str
    intersects_with: str
    max_results: int | None
    relative_orbit: int | None = None


@dataclass
class SearchResultItem:
    index: int
    granule_id: str
    acquisition_time: str
    relative_orbit: str | None
    orbit_direction: str | None = None
    polarization: str | None = None
    size_mb: float | None = None
    download_url: str = ""
    footprint_wkt: str | None = None


@dataclass
class DownloadTask:
    task_id: str
    source_manifest_path: Path
    target_dir: Path
    created_at: datetime


@dataclass
class DownloadStatusRecord:
    task_id: str
    granule_id: str
    status: str
    local_path: str
    error: str
    elapsed_sec: float
    timestamp: str
    attempt: int = 1
    error_type: str = ""
