import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from s1downloader.download_service import (
    MAX_DOWNLOAD_ATTEMPTS,
    _acquisition_date_folder,
    _download_url_with_retries,
    _download_with_retries,
    _EarthdataSession,
    _EOFEntry,
    _fetch_eof_entries,
    _match_eof_name,
    _match_track_direction,
    _parse_eof_entries,
    _parse_scene_satellite_and_time,
    _progress_bar,
    _safe_filename,
    build_download_preview,
    run_download_from_manifest,
)
from s1downloader.manifest import write_search_manifest
from s1downloader.models import SearchResultItem


def test_acquisition_date_folder_is_yyyymmdd():
    folder = _acquisition_date_folder("2024-01-31T23:59:59Z")
    assert folder == "20240131"


def test_safe_filename_uses_url_basename():
    item = SearchResultItem(
        index=1,
        granule_id="S1_TEST_001",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit=None,
        orbit_direction=None,
        polarization=None,
        size_mb=None,
        download_url="https://example.org/path/S1_TEST_001.zip",
    )
    assert _safe_filename(item) == "S1_TEST_001.zip"


def test_match_track_direction_tokens():
    assert _match_track_direction("ASCENDING", {"ASC"})
    assert _match_track_direction("DESCENDING", {"DES"})
    assert not _match_track_direction("ASCENDING", {"DES"})


def test_build_download_preview_filters_by_track(tmp_path: Path):
    manifest = tmp_path / "search.csv"
    items = [
        SearchResultItem(
            index=1,
            granule_id="S1_A",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="ASCENDING",
            download_url="https://example.org/a.zip",
        ),
        SearchResultItem(
            index=2,
            granule_id="S1_D",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="DESCENDING",
            download_url="https://example.org/d.zip",
        ),
    ]
    write_search_manifest(manifest, "q_test", items)

    preview = build_download_preview(manifest_path=manifest, track_filter="DES")
    assert preview["manifest_total"] == 2
    assert preview["filtered_total"] == 1
    assert preview["track_tokens"] == ["DES"]


def test_progress_bar_format():
    text = _progress_bar(processed=3, total=10, success=2, failed=1, skipped=0)
    assert "3/10" in text
    assert "ok=2" in text


def test_progress_bar_includes_attempt_info():
    text = _progress_bar(
        processed=3,
        total=10,
        success=2,
        failed=1,
        skipped=0,
        attempt_info="S1_TEST attempt 2/3",
    )
    assert "attempt 2/3" in text


def test_download_with_retries_succeeds_on_third_attempt(tmp_path: Path, monkeypatch):
    item = SearchResultItem(
        index=1,
        granule_id="S1_RETRY_OK",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit=None,
        orbit_direction=None,
        download_url="https://example.org/retry-ok.zip",
    )
    attempts: list[int] = []
    call_count = {"value": 0}

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        call_count["value"] += 1
        if call_count["value"] < 3:
            raise requests.Timeout("temporary timeout")
        target_path.write_bytes(b"ok")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_args, **_kwargs: None)

    result = _download_with_retries(
        session=requests.Session(),
        item=item,
        target_path=tmp_path / "ok.zip",
        timeout_sec=30,
        logger=logging.getLogger("test.retry.success"),
        max_attempts=MAX_DOWNLOAD_ATTEMPTS,
        on_attempt=lambda attempt: attempts.append(attempt),
    )

    assert result.status == "success"
    assert result.attempt == 3
    assert attempts == [1, 2, 3]


def test_download_with_retries_stops_on_non_retryable_http_error(tmp_path: Path, monkeypatch):
    item = SearchResultItem(
        index=1,
        granule_id="S1_HTTP_403",
        acquisition_time="2024-01-01T00:00:00Z",
        relative_orbit=None,
        orbit_direction=None,
        download_url="https://example.org/forbidden.zip",
    )

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        response = requests.Response()
        response.status_code = 403
        raise requests.HTTPError("403 Forbidden", response=response)

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_args, **_kwargs: None)

    result = _download_with_retries(
        session=requests.Session(),
        item=item,
        target_path=tmp_path / "forbidden.zip",
        timeout_sec=30,
        logger=logging.getLogger("test.retry.nonretryable"),
        max_attempts=MAX_DOWNLOAD_ATTEMPTS,
    )

    assert result.status == "failed"
    assert result.attempt == 1
    assert result.error_type == "http_403"


def test_download_url_with_retries_retries_http_401_when_enabled(tmp_path: Path, monkeypatch):
    call_count = {"value": 0}

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        call_count["value"] += 1
        if call_count["value"] < 3:
            response = requests.Response()
            response.status_code = 401
            raise requests.HTTPError("401 Unauthorized", response=response)
        target_path.write_bytes(b"ok")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_args, **_kwargs: None)

    result = _download_url_with_retries(
        session=requests.Session(),
        url="https://example.org/eof.EOF",
        target_path=tmp_path / "eof.EOF",
        timeout_sec=30,
        logger=logging.getLogger("test.eof.retry.401"),
        label="EOF_TEST",
        max_attempts=MAX_DOWNLOAD_ATTEMPTS,
        retry_auth_errors=True,
    )

    assert result.status == "success"
    assert result.attempt == 3
    assert call_count["value"] == 3


def test_run_download_from_manifest_writes_failed_manifest(tmp_path: Path, monkeypatch):
    manifest_path = tmp_path / "search.csv"
    download_root = tmp_path / "dataset"
    status_dir = tmp_path / "manifests"
    items = [
        SearchResultItem(
            index=1,
            granule_id="S1_OK",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="DESCENDING",
            download_url="https://example.org/ok.zip",
        ),
        SearchResultItem(
            index=2,
            granule_id="S1_FAIL",
            acquisition_time="2024-01-13T00:00:00Z",
            relative_orbit="42",
            orbit_direction="DESCENDING",
            download_url="https://example.org/fail.zip",
        ),
    ]
    write_search_manifest(manifest_path, "q_test", items)

    def _fake_download_file(session, url, target_path, timeout_sec, progress_hook=None):
        if "fail.zip" in url:
            raise requests.Timeout("network timeout")
        target_path.write_bytes(b"ok")

    monkeypatch.setattr("s1downloader.download_service._download_file", _fake_download_file)
    monkeypatch.setattr("s1downloader.download_service.time.sleep", lambda *_args, **_kwargs: None)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=download_root,
        status_manifest_dir=status_dir,
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.download.flow"),
        show_progress=False,
    )

    assert summary["total"] == 2
    assert summary["success"] == 1
    assert summary["failed"] == 1
    assert summary["failed_manifest"]
    assert Path(str(summary["failed_manifest"])).exists()


def test_run_download_from_manifest_uses_direct_session(tmp_path: Path, monkeypatch):
    manifest_path = tmp_path / "search.csv"
    status_dir = tmp_path / "manifests"
    items = [
        SearchResultItem(
            index=1,
            granule_id="S1_DIRECT",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="DESCENDING",
            download_url="https://example.org/direct.zip",
        )
    ]
    write_search_manifest(manifest_path, "q_test", items)

    captured = {"trust_env": None}

    class _FakeSession:
        def __init__(self, username, password):
            self.auth = (username, password)
            self.trust_env = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("s1downloader.download_service._EarthdataSession", _FakeSession)

    def _fake_download_with_retries(**kwargs):
        captured["trust_env"] = kwargs["session"].trust_env
        return type(
            "Attempt",
            (),
            {"status": "success", "attempt": 1, "error": "", "error_type": ""},
        )()

    monkeypatch.setattr("s1downloader.download_service._download_with_retries", _fake_download_with_retries)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=tmp_path / "dataset",
        status_manifest_dir=status_dir,
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.download.direct_session"),
        show_progress=False,
    )

    assert summary["success"] == 1
    assert captured["trust_env"] is False


def test_earthdata_session_keeps_auth_for_urs_redirect():
    session = _EarthdataSession("u", "p")
    prepared = requests.Request("GET", "https://urs.earthdata.nasa.gov/oauth/authorize").prepare()
    prepared.headers.pop("Authorization", None)
    session.rebuild_auth(prepared, None)
    assert prepared.headers.get("Authorization", "").startswith("Basic ")


def test_parse_scene_satellite_and_time_from_granule_id():
    item = SearchResultItem(
        index=1,
        granule_id="S1A_IW_SLC__1SDV_20231116T135256_20231116T135324_051243_062E82_BDF4",
        acquisition_time="",
        relative_orbit="42",
        download_url="https://example.org/a.zip",
    )
    parsed = _parse_scene_satellite_and_time(item)
    assert parsed is not None
    sat, dt = parsed
    assert sat == "S1A"
    assert dt.isoformat() == "2023-11-16T13:52:56+00:00"


def test_parse_eof_entries_and_match():
    html = """
    <a href="S1A_OPER_AUX_POEORB_OPOD_20231120T120000_V20231115T225942_20231117T005942.EOF">x</a>
    <a href="S1A_OPER_AUX_POEORB_OPOD_20231121T120000_V20231116T120000_20231118T000000.EOF">y</a>
    """
    entries = _parse_eof_entries(html)
    scene_time = datetime(2023, 11, 16, 13, 52, 56, tzinfo=timezone.utc)
    matched = _match_eof_name(entries, "S1A", scene_time)
    assert matched == "S1A_OPER_AUX_POEORB_OPOD_20231121T120000_V20231116T120000_20231118T000000.EOF"


def test_fetch_eof_entries_uses_direct_session(monkeypatch):
    captured = {"trust_env": None}

    class _FakeResponse:
        text = '<a href="S1A_OPER_AUX_POEORB_OPOD_20231120T120000_V20231115T225942_20231117T005942.EOF">x</a>'

        def raise_for_status(self):
            return None

    class _FakeSession:
        def __init__(self):
            self.trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            captured["trust_env"] = self.trust_env
            return _FakeResponse()

    monkeypatch.setattr("s1downloader.download_service.requests.Session", _FakeSession)
    entries = _fetch_eof_entries(timeout_sec=10, logger=logging.getLogger("test.eof.fetch"))
    assert entries
    assert captured["trust_env"] is False


def test_run_download_with_eof_for_success_and_skipped_items(tmp_path: Path, monkeypatch):
    manifest_path = tmp_path / "search.csv"
    download_root = tmp_path / "dataset"
    status_dir = tmp_path / "manifests"
    item_skipped = SearchResultItem(
        index=1,
        granule_id="S1A_IW_SLC__1SDV_20231116T135256_20231116T135324_051243_062E82_BDF4",
        acquisition_time="2023-11-16T13:52:56Z",
        relative_orbit="42",
        orbit_direction="DESCENDING",
        download_url="https://example.org/skip.zip",
    )
    item_success = SearchResultItem(
        index=2,
        granule_id="S1A_IW_SLC__1SDV_20231116T140000_20231116T140028_051243_062E82_ABCD",
        acquisition_time="2023-11-16T14:00:00Z",
        relative_orbit="42",
        orbit_direction="DESCENDING",
        download_url="https://example.org/success.zip",
    )
    write_search_manifest(manifest_path, "q_test", [item_skipped, item_success])

    # Force first item to be skipped by creating local SLC path.
    skip_target = download_root / "20231116" / "skip.zip"
    skip_target.parent.mkdir(parents=True, exist_ok=True)
    skip_target.write_bytes(b"x")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "s1downloader.download_service._fetch_eof_entries",
        lambda **kwargs: [
            _EOFEntry(
                satellite="S1A",
                valid_start=datetime(2023, 11, 16, 0, 0, 0, tzinfo=timezone.utc),
                valid_end=datetime(2023, 11, 17, 0, 0, 0, tzinfo=timezone.utc),
                name="S1A_OPER_AUX_POEORB_OPOD_20231120T120000_V20231116T000000_20231117T000000.EOF",
            )
        ],
    )

    monkeypatch.setattr(
        "s1downloader.download_service._download_with_retries",
        lambda **kwargs: type("Attempt", (), {"status": "success", "attempt": 1, "error": "", "error_type": ""})(),
    )
    eof_calls = {"count": 0}

    def _fake_download_eof(**kwargs):
        eof_calls["count"] += 1
        kwargs["target_path"].write_bytes(b"eof")
        return type("Attempt", (), {"status": "success", "attempt": 1, "error": "", "error_type": ""})()

    monkeypatch.setattr("s1downloader.download_service._download_url_with_retries", _fake_download_eof)

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=download_root,
        status_manifest_dir=status_dir,
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.download.with_eof"),
        show_progress=False,
        download_eof=True,
    )

    assert summary["success"] == 1
    assert summary["skipped"] == 1
    assert summary["eof_success"] == 1
    assert summary["eof_failed"] == 0
    assert eof_calls["count"] == 1
    assert (tmp_path / "Orbit").exists()


def test_run_download_with_eof_skips_existing_orbit_file(tmp_path: Path, monkeypatch):
    manifest_path = tmp_path / "search.csv"
    download_root = tmp_path / "dataset"
    status_dir = tmp_path / "manifests"
    item = SearchResultItem(
        index=1,
        granule_id="S1A_IW_SLC__1SDV_20231116T135256_20231116T135324_051243_062E82_BDF4",
        acquisition_time="2023-11-16T13:52:56Z",
        relative_orbit="42",
        orbit_direction="DESCENDING",
        download_url="https://example.org/skip.zip",
    )
    write_search_manifest(manifest_path, "q_test", [item])

    skip_target = download_root / "20231116" / "skip.zip"
    skip_target.parent.mkdir(parents=True, exist_ok=True)
    skip_target.write_bytes(b"x")

    monkeypatch.chdir(tmp_path)
    orbit_dir = tmp_path / "Orbit"
    orbit_dir.mkdir(parents=True, exist_ok=True)
    eof_name = "S1A_OPER_AUX_POEORB_OPOD_20231120T120000_V20231116T000000_20231117T000000.EOF"
    (orbit_dir / eof_name).write_bytes(b"existing")

    monkeypatch.setattr(
        "s1downloader.download_service._fetch_eof_entries",
        lambda **kwargs: [
            _EOFEntry(
                satellite="S1A",
                valid_start=datetime(2023, 11, 16, 0, 0, 0, tzinfo=timezone.utc),
                valid_end=datetime(2023, 11, 17, 0, 0, 0, tzinfo=timezone.utc),
                name=eof_name,
            )
        ],
    )
    monkeypatch.setattr(
        "s1downloader.download_service._download_url_with_retries",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("EOF file exists, should not download")),
    )

    summary = run_download_from_manifest(
        manifest_path=manifest_path,
        track_filter=None,
        download_root=download_root,
        status_manifest_dir=status_dir,
        timeout_sec=30,
        credentials=("u", "p"),
        logger=logging.getLogger("test.download.eof.skip_existing"),
        show_progress=False,
        download_eof=True,
    )

    assert summary["skipped"] == 1
    assert summary["eof_skipped"] == 1
    assert summary["eof_success"] == 0
