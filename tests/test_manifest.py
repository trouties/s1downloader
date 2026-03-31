from pathlib import Path

from s1downloader.manifest import (
    append_download_status,
    read_search_manifest,
    write_failed_manifest,
    write_search_manifest,
)
from s1downloader.models import DownloadStatusRecord, SearchResultItem


def test_write_and_read_search_manifest(tmp_path: Path):
    manifest = tmp_path / "search.csv"
    items = [
        SearchResultItem(
            index=1,
            granule_id="S1_TEST_001",
            acquisition_time="2024-01-01T01:00:00Z",
            relative_orbit="12",
            orbit_direction="DESCENDING",
            polarization="VV",
            size_mb=1024.5,
            download_url="https://example.org/data/S1_TEST_001.zip",
            footprint_wkt="POLYGON((120 30,121 30,121 31,120 31,120 30))",
        )
    ]

    write_search_manifest(manifest, "q_test", items)
    loaded = read_search_manifest(manifest)

    assert len(loaded) == 1
    assert loaded[0].granule_id == "S1_TEST_001"
    assert loaded[0].index == 1
    assert loaded[0].orbit_direction == "DESCENDING"
    assert loaded[0].footprint_wkt is not None


def test_append_download_status_and_write_failed_manifest(tmp_path: Path):
    status_path = tmp_path / "download.csv"
    append_download_status(
        status_path,
        DownloadStatusRecord(
            task_id="d_test",
            granule_id="S1_TEST_001",
            status="failed",
            local_path="/tmp/S1_TEST_001.zip",
            error="timeout",
            elapsed_sec=12.3,
            timestamp="2026-03-28T12:00:00+00:00",
            attempt=3,
            error_type="timeout",
        ),
    )
    text = status_path.read_text(encoding="utf-8")
    assert "attempt" in text
    assert "error_type" in text

    failed_path = tmp_path / "failed.csv"
    write_failed_manifest(
        failed_path,
        [
            {
                "granule_id": "S1_TEST_001",
                "download_url": "https://example.org/a.zip",
                "reason": "timeout",
            }
        ],
    )
    failed_text = failed_path.read_text(encoding="utf-8")
    assert "granule_id,download_url,reason" in failed_text
