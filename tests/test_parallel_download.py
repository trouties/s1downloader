import logging
import threading
from pathlib import Path

import requests

from s1downloader.download_service import (
    _download_one_item,
    _DownloadResult,
    _ProgressAggregator,
    _SessionPool,
    run_download_from_manifest,
)
from s1downloader.manifest import write_search_manifest
from s1downloader.models import SearchResultItem


def _make_items(count: int) -> list[SearchResultItem]:
    return [
        SearchResultItem(
            index=i,
            granule_id=f"S1_ITEM_{i:03d}",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="DESCENDING",
            download_url=f"https://example.org/item_{i:03d}.zip",
        )
        for i in range(1, count + 1)
    ]


def test_session_pool_creates_one_session_per_thread():
    pool = _SessionPool("u", "p")
    sessions: list[object] = []
    barrier = threading.Barrier(3)

    def worker():
        s = pool.get()
        barrier.wait()
        sessions.append(id(s))
        # Same thread should get the same session
        assert pool.get() is s

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    pool.close_all()
    # Each thread should have its own unique session
    assert len(set(sessions)) == 3


def test_session_pool_close_all():
    pool = _SessionPool("u", "p")
    s = pool.get()
    assert s is not None
    pool.close_all()
    assert len(pool._sessions) == 0


def test_download_one_item_skips_existing_file(tmp_path: Path):
    item = SearchResultItem(
        index=1,
        granule_id="S1_EXISTS",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit="42",
        download_url="https://example.org/exists.zip",
    )
    download_root = tmp_path / "dataset"
    target = download_root / "20240101" / "exists.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"already here")

    pool = _SessionPool("u", "p")
    result = _download_one_item(
        session_pool=pool,
        item=item,
        download_root=download_root,
        timeout_sec=30,
        logger=logging.getLogger("test.skip"),
    )
    pool.close_all()

    assert result.status == "skipped"
    assert result.attempt == 0


def test_download_one_item_respects_shutdown_event(tmp_path: Path):
    item = SearchResultItem(
        index=1,
        granule_id="S1_SHUTDOWN",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit="42",
        download_url="https://example.org/shutdown.zip",
    )
    pool = _SessionPool("u", "p")
    shutdown = threading.Event()
    shutdown.set()

    result = _download_one_item(
        session_pool=pool,
        item=item,
        download_root=tmp_path / "dataset",
        timeout_sec=30,
        logger=logging.getLogger("test.shutdown"),
        shutdown_event=shutdown,
    )
    pool.close_all()

    assert result.status == "cancelled"


def test_download_one_item_returns_success(tmp_path: Path, monkeypatch):
    item = SearchResultItem(
        index=1,
        granule_id="S1_OK",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit="42",
        download_url="https://example.org/ok.zip",
    )

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        target_path.write_bytes(b"downloaded-content")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)

    pool = _SessionPool("u", "p")
    result = _download_one_item(
        session_pool=pool,
        item=item,
        download_root=tmp_path / "dataset",
        timeout_sec=30,
        logger=logging.getLogger("test.success"),
    )
    pool.close_all()

    assert result.status == "success"
    assert result.file_size == len(b"downloaded-content")
    assert result.attempt == 1


def test_parallel_download_multiple_items(tmp_path: Path, monkeypatch):
    items = _make_items(6)
    manifest_path = tmp_path / "search.csv"
    write_search_manifest(manifest_path, "q_test", items)

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        target_path.write_bytes(b"ok")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_: None)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=tmp_path / "dataset",
        status_manifest_dir=tmp_path / "manifests",
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.parallel"),
        show_progress=False,
        workers=3,
    )

    assert summary["total"] == 6
    assert summary["success"] == 6
    assert summary["failed"] == 0
    assert summary["skipped"] == 0


def test_parallel_download_mixed_success_and_failure(tmp_path: Path, monkeypatch):
    items = _make_items(4)
    manifest_path = tmp_path / "search.csv"
    write_search_manifest(manifest_path, "q_test", items)

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        if "item_002" in url or "item_004" in url:
            raise requests.Timeout("timeout")
        target_path.write_bytes(b"ok")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_: None)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=tmp_path / "dataset",
        status_manifest_dir=tmp_path / "manifests",
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.parallel.mixed"),
        show_progress=False,
        workers=2,
    )

    assert summary["total"] == 4
    assert summary["success"] == 2
    assert summary["failed"] == 2
    assert summary["failed_manifest"]
    assert Path(str(summary["failed_manifest"])).exists()


def test_parallel_download_workers_1_sequential(tmp_path: Path, monkeypatch):
    """workers=1 produces same results as parallel."""
    items = _make_items(3)
    manifest_path = tmp_path / "search.csv"
    write_search_manifest(manifest_path, "q_test", items)

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        target_path.write_bytes(b"data")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_: None)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=tmp_path / "dataset",
        status_manifest_dir=tmp_path / "manifests",
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.sequential"),
        show_progress=False,
        workers=1,
    )

    assert summary["total"] == 3
    assert summary["success"] == 3


def test_progress_aggregator_thread_safety():
    agg = _ProgressAggregator(10, show_progress=False)
    barrier = threading.Barrier(4)

    def worker(gid: str):
        barrier.wait()
        for i in range(20):
            agg.on_item_progress(gid, i * 1024, 20 * 1024)
        result = _DownloadResult(
            item=SearchResultItem(
                index=1,
                granule_id=gid,
                acquisition_time="2024-01-01T00:00:00Z",
                relative_orbit="42",
                download_url=f"https://example.org/{gid}.zip",
            ),
            target_path=Path(f"/tmp/{gid}.zip"),
            status="success",
            attempt=1,
            error="",
            error_type="",
            elapsed_sec=1.0,
            file_size=20 * 1024,
        )
        agg.on_item_complete(result)

    threads = [threading.Thread(target=worker, args=(f"G{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert agg._completed == 4
    assert agg._success == 4
