from __future__ import annotations

import csv
import uuid
from datetime import datetime, timezone
from pathlib import Path

from s1downloader.models import DownloadStatusRecord, SearchResultItem

SEARCH_MANIFEST_FIELDS = [
    "query_id",
    "index",
    "granule_id",
    "acquisition_time",
    "relative_orbit",
    "orbit_direction",
    "polarization",
    "size_mb",
    "download_url",
    "footprint_wkt",
]

DOWNLOAD_STATUS_FIELDS = [
    "task_id",
    "granule_id",
    "status",
    "local_path",
    "error",
    "error_type",
    "attempt",
    "elapsed_sec",
    "timestamp",
]

FAILED_MANIFEST_FIELDS = [
    "granule_id",
    "download_url",
    "reason",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_query_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"q_{ts}_{uuid.uuid4().hex[:6]}"


def generate_task_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"d_{ts}_{uuid.uuid4().hex[:6]}"


def write_search_manifest(path: Path, query_id: str, items: list[SearchResultItem]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SEARCH_MANIFEST_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(
                {
                    "query_id": query_id,
                    "index": item.index,
                    "granule_id": item.granule_id,
                    "acquisition_time": item.acquisition_time,
                    "relative_orbit": item.relative_orbit or "",
                    "orbit_direction": item.orbit_direction or "",
                    "polarization": item.polarization or "",
                    "size_mb": "" if item.size_mb is None else item.size_mb,
                    "download_url": item.download_url,
                    "footprint_wkt": item.footprint_wkt or "",
                }
            )


def read_search_manifest(path: Path) -> list[SearchResultItem]:
    if not path.exists():
        raise FileNotFoundError(f"Search manifest not found: {path}")

    items: list[SearchResultItem] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            size_text = (row.get("size_mb") or "").strip()
            size_mb = float(size_text) if size_text else None
            items.append(
                SearchResultItem(
                    index=int(row["index"]),
                    granule_id=row.get("granule_id", ""),
                    acquisition_time=row.get("acquisition_time", ""),
                    relative_orbit=(row.get("relative_orbit") or "").strip() or None,
                    orbit_direction=(row.get("orbit_direction") or "").strip() or None,
                    polarization=(row.get("polarization") or "").strip() or None,
                    size_mb=size_mb,
                    download_url=row.get("download_url", ""),
                    footprint_wkt=(row.get("footprint_wkt") or "").strip() or None,
                )
            )
    return items


def append_download_status(path: Path, record: DownloadStatusRecord) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DOWNLOAD_STATUS_FIELDS)
        if write_header:
            writer.writeheader()

        writer.writerow(
            {
                "task_id": record.task_id,
                "granule_id": record.granule_id,
                "status": record.status,
                "local_path": record.local_path,
                "error": record.error,
                "error_type": record.error_type,
                "attempt": record.attempt,
                "elapsed_sec": f"{record.elapsed_sec:.2f}",
                "timestamp": record.timestamp,
            }
        )


def write_failed_manifest(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FAILED_MANIFEST_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "granule_id": row.get("granule_id", ""),
                    "download_url": row.get("download_url", ""),
                    "reason": row.get("reason", ""),
                }
            )
