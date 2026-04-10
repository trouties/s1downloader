"""
Integration test: mock HTTP server + real parallel download flow.

Spins up a local HTTP server that serves fake Sentinel-1 zip files,
creates a search manifest pointing to it, and runs the full
run_download_from_manifest() with multiple workers to verify
parallel download works end-to-end.
"""

import csv
import http.server
import logging
import socketserver
import threading
import time
from pathlib import Path

from s1downloader.download_service import run_download_from_manifest
from s1downloader.manifest import write_search_manifest
from s1downloader.models import SearchResultItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Fake file content: 64 KB per "scene" with unique bytes
_FILE_SIZE = 64 * 1024


def _make_fake_content(index: int) -> bytes:
    """Generate deterministic fake file content for a given item index."""
    chunk = f"FAKE_SLC_DATA_ITEM_{index:04d}_".encode()
    repeats = (_FILE_SIZE // len(chunk)) + 1
    return (chunk * repeats)[:_FILE_SIZE]


class _FakeDownloadHandler(http.server.BaseHTTPRequestHandler):
    """Serves fake .zip files and simulates latency."""

    # Shared state set before server starts
    file_map: dict[str, bytes] = {}
    latency_sec: float = 0.05

    def do_GET(self):  # noqa: N802
        path = self.path.lstrip("/")
        content = self.file_map.get(path)
        if content is None:
            self.send_error(404, f"Not found: {path}")
            return
        # Simulate download latency
        time.sleep(self.latency_sec)
        self.send_response(200)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Content-Type", "application/zip")
        self.end_headers()
        # Send in chunks to exercise progress hooks
        chunk_size = 8 * 1024
        for offset in range(0, len(content), chunk_size):
            self.wfile.write(content[offset : offset + chunk_size])
            self.wfile.flush()

    def log_message(self, format, *args):
        # Suppress request logging to keep test output clean
        pass


def _start_mock_server(file_map: dict[str, bytes], latency_sec: float = 0.05):
    """Start a threaded HTTP server and return (server, port)."""
    _FakeDownloadHandler.file_map = file_map
    _FakeDownloadHandler.latency_sec = latency_sec

    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _FakeDownloadHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def _create_manifest(tmp_path: Path, port: int, item_count: int) -> Path:
    """Create a search manifest CSV pointing to the mock server."""
    items = []
    for i in range(1, item_count + 1):
        items.append(
            SearchResultItem(
                index=i,
                granule_id=f"S1A_IW_SLC__1SDV_20240101T{i:06d}_20240101T{i:06d}_000001_000001_{i:04X}",
                acquisition_time="2024-01-01T00:00:00Z",
                relative_orbit="42",
                orbit_direction="DESCENDING",
                polarization="VV+VH",
                size_mb=round(_FILE_SIZE / (1024 * 1024), 2),
                download_url=f"http://127.0.0.1:{port}/scene_{i:04d}.zip",
            )
        )
    manifest_path = tmp_path / "search_manifest.csv"
    write_search_manifest(manifest_path, "q_integration_test", items)
    return manifest_path


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


def test_parallel_download_integration_4_workers(tmp_path: Path):
    """Download 8 files with 4 workers via a real HTTP server."""
    item_count = 8
    file_map = {f"scene_{i:04d}.zip": _make_fake_content(i) for i in range(1, item_count + 1)}

    server, port = _start_mock_server(file_map, latency_sec=0.02)
    try:
        manifest_path = _create_manifest(tmp_path, port, item_count)
        download_root = tmp_path / "dataset"
        status_dir = tmp_path / "manifests"

        logger = logging.getLogger("test.integration.parallel")
        logger.setLevel(logging.DEBUG)

        t0 = time.perf_counter()
        summary = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=download_root,
            status_manifest_dir=status_dir,
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logger,
            show_progress=False,
            workers=4,
        )
        elapsed = time.perf_counter() - t0

        # All items should succeed
        assert summary["total"] == item_count
        assert summary["success"] == item_count
        assert summary["failed"] == 0
        assert summary["skipped"] == 0

        # Verify all files exist and have correct content
        downloaded_files = list(download_root.rglob("*.zip"))
        assert len(downloaded_files) == item_count
        for i in range(1, item_count + 1):
            found = [f for f in downloaded_files if f"scene_{i:04d}" in f.name or f"{i:04X}" in f.name]
            assert len(found) == 1, f"Expected exactly one file for item {i}, found {len(found)}"
            assert found[0].stat().st_size == _FILE_SIZE

        # Verify status manifest CSV exists and has correct rows
        status_csv_path = Path(str(summary["status_manifest"]))
        assert status_csv_path.exists()
        with status_csv_path.open("r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == item_count
        assert all(r["status"] == "success" for r in rows)

        # No failed manifest should be written
        assert summary["failed_manifest"] == ""

        print(f"\n[Integration] {item_count} files x {_FILE_SIZE // 1024}KB, 4 workers, elapsed={elapsed:.2f}s")
    finally:
        server.shutdown()


def test_parallel_download_integration_1_worker_sequential(tmp_path: Path):
    """Download 4 files with 1 worker (sequential) via a real HTTP server."""
    item_count = 4
    file_map = {f"scene_{i:04d}.zip": _make_fake_content(i) for i in range(1, item_count + 1)}

    server, port = _start_mock_server(file_map, latency_sec=0.02)
    try:
        manifest_path = _create_manifest(tmp_path, port, item_count)

        summary = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=tmp_path / "dataset",
            status_manifest_dir=tmp_path / "manifests",
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logging.getLogger("test.integration.sequential"),
            show_progress=False,
            workers=1,
        )

        assert summary["total"] == item_count
        assert summary["success"] == item_count
        assert summary["failed"] == 0
    finally:
        server.shutdown()


def test_parallel_download_integration_skip_existing(tmp_path: Path):
    """Files that already exist should be skipped."""
    item_count = 4
    file_map = {f"scene_{i:04d}.zip": _make_fake_content(i) for i in range(1, item_count + 1)}

    server, port = _start_mock_server(file_map)
    try:
        manifest_path = _create_manifest(tmp_path, port, item_count)
        download_root = tmp_path / "dataset"

        # Pre-create 2 of the 4 target files
        for i in [1, 3]:
            target_dir = download_root / "20240101"
            target_dir.mkdir(parents=True, exist_ok=True)
            # Find the expected filename from the manifest
            with manifest_path.open("r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row["index"] == str(i):
                        url = row["download_url"]
                        fname = url.split("/")[-1]
                        (target_dir / fname).write_bytes(b"pre-existing")

        summary = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=download_root,
            status_manifest_dir=tmp_path / "manifests",
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logging.getLogger("test.integration.skip"),
            show_progress=False,
            workers=2,
        )

        assert summary["total"] == item_count
        assert summary["skipped"] == 2
        assert summary["success"] == 2
        assert summary["failed"] == 0
    finally:
        server.shutdown()


def test_parallel_download_integration_mixed_404(tmp_path: Path):
    """Some items return 404 - should fail gracefully while others succeed."""
    # Only serve items 1, 2, 3 - item 4 will 404
    file_map = {f"scene_{i:04d}.zip": _make_fake_content(i) for i in range(1, 4)}

    server, port = _start_mock_server(file_map)
    try:
        manifest_path = _create_manifest(tmp_path, port, 4)

        summary = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=tmp_path / "dataset",
            status_manifest_dir=tmp_path / "manifests",
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logging.getLogger("test.integration.mixed404"),
            show_progress=False,
            workers=2,
        )

        assert summary["total"] == 4
        assert summary["success"] == 3
        assert summary["failed"] == 1
        assert summary["failed_manifest"] != ""
        assert Path(str(summary["failed_manifest"])).exists()
    finally:
        server.shutdown()


def test_parallel_vs_sequential_speed_comparison(tmp_path: Path):
    """Parallel (4 workers) should be faster than sequential (1 worker) with latency."""
    item_count = 8
    latency = 0.1  # 100ms per request to make parallelism visible
    file_map = {f"scene_{i:04d}.zip": _make_fake_content(i) for i in range(1, item_count + 1)}

    server, port = _start_mock_server(file_map, latency_sec=latency)
    try:
        # --- Sequential run ---
        manifest_path = _create_manifest(tmp_path / "seq", port, item_count)
        (tmp_path / "seq").mkdir(parents=True, exist_ok=True)

        t0 = time.perf_counter()
        summary_seq = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=tmp_path / "seq" / "dataset",
            status_manifest_dir=tmp_path / "seq" / "manifests",
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logging.getLogger("test.speed.seq"),
            show_progress=False,
            workers=1,
        )
        time_seq = time.perf_counter() - t0

        # --- Parallel run ---
        manifest_path = _create_manifest(tmp_path / "par", port, item_count)
        (tmp_path / "par").mkdir(parents=True, exist_ok=True)

        t0 = time.perf_counter()
        summary_par = run_download_from_manifest(
            manifest_path=manifest_path,
            track_filter=None,
            download_root=tmp_path / "par" / "dataset",
            status_manifest_dir=tmp_path / "par" / "manifests",
            timeout_sec=30,
            credentials=("mock_user", "mock_pass"),
            logger=logging.getLogger("test.speed.par"),
            show_progress=False,
            workers=4,
        )
        time_par = time.perf_counter() - t0

        # Both should succeed fully
        assert summary_seq["success"] == item_count
        assert summary_par["success"] == item_count

        # Parallel should be meaningfully faster (at least 1.5x)
        speedup = time_seq / time_par if time_par > 0 else 999
        print(f"\n[Speed] seq={time_seq:.2f}s, par={time_par:.2f}s, speedup={speedup:.1f}x")
        assert speedup > 1.5, f"Expected parallel to be >1.5x faster, got {speedup:.1f}x"
    finally:
        server.shutdown()
