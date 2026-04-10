import logging
from types import SimpleNamespace

from s1downloader.config import AppConfig
from s1downloader.main import EXIT_NETWORK_OR_API_ERROR, EXIT_OK, MAX_RESULTS_USE_CONFIG, main, run_search
from s1downloader.models import SearchResultItem
from s1downloader.search_service import NetworkError


def _build_config(tmp_path):
    return AppConfig(
        project_root=tmp_path,
        manifest_dir=tmp_path / "manifests",
        log_dir=tmp_path / "logs",
        timeout_sec=120,
        max_results=50,
        log_level="INFO",
    )


def test_run_search_generates_png_when_items_exist(tmp_path, monkeypatch, capsys):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        start_date="20240101",
        end_date="20240131",
        wkt=None,
        bbox="120.0,30.0,121.0,31.0",
        aoi_file=None,
        allow_aoi_fallback_prompt=False,
        max_results=MAX_RESULTS_USE_CONFIG,
        manifest_path=None,
    )
    logger = logging.getLogger("test.main.search")
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "s1downloader.main.search_sentinel1_slc",
        lambda request, logger: [
            SearchResultItem(
                index=1,
                granule_id="S1_TEST_001",
                acquisition_time="2024-01-01T00:00:00Z",
                relative_orbit="12",
                orbit_direction="ASCENDING",
                polarization="VV",
                size_mb=123.4,
                download_url="https://example.org/S1_TEST_001.zip",
                footprint_wkt="POLYGON((120 30,121 30,121 31,120 31,120 30))",
            )
        ],
    )

    def _fake_render(*, output_path, **kwargs):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"png")

    monkeypatch.setattr("s1downloader.plot_service.render_search_overview_png", _fake_render)

    code = run_search(args, config, logger)
    assert code == EXIT_OK

    output = capsys.readouterr().out
    assert "Search overview PNG saved:" in output
    assert list(tmp_path.glob("search_*.png"))


def test_run_search_skips_png_when_no_results(tmp_path, monkeypatch, capsys):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        start_date="20240101",
        end_date="20240131",
        wkt=None,
        bbox="120.0,30.0,121.0,31.0",
        aoi_file=None,
        allow_aoi_fallback_prompt=False,
        max_results=MAX_RESULTS_USE_CONFIG,
        manifest_path=None,
    )
    logger = logging.getLogger("test.main.search.empty")
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("s1downloader.main.search_sentinel1_slc", lambda request, logger: [])
    monkeypatch.setattr(
        "s1downloader.plot_service.render_search_overview_png",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("plot should not be called")),
    )

    code = run_search(args, config, logger)
    assert code == EXIT_OK

    output = capsys.readouterr().out
    assert "Search overview PNG saved:" not in output


def test_run_search_prints_elapsed_time(tmp_path, monkeypatch, capsys):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        start_date="20240101",
        end_date="20240131",
        wkt=None,
        bbox="120.0,30.0,121.0,31.0",
        aoi_file=None,
        allow_aoi_fallback_prompt=False,
        max_results=MAX_RESULTS_USE_CONFIG,
        manifest_path=None,
    )
    logger = logging.getLogger("test.main.search.timing")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("s1downloader.main.search_sentinel1_slc", lambda request, logger: [])
    monkeypatch.setattr(
        "s1downloader.plot_service.render_search_overview_png",
        lambda **kwargs: None,
    )

    code = run_search(args, config, logger)
    assert code == EXIT_OK
    output = capsys.readouterr().out
    assert "Search completed:" in output
    assert "result(s) in" in output


def test_main_network_error_returns_correct_exit_code(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.yaml"
    config_file.write_text("log_dir: logs\nmanifest_dir: manifests\n", encoding="utf-8")

    def _raise_network_error(request, logger):
        raise NetworkError("ASF search failed after 3 attempt(s): timeout")

    monkeypatch.setattr("s1downloader.main.search_sentinel1_slc", _raise_network_error)

    code = main(
        [
            "--config",
            str(config_file),
            "search",
            "-s",
            "20240101",
            "-e",
            "20240131",
            "--bbox",
            "120,30,121,31",
        ]
    )
    assert code == EXIT_NETWORK_OR_API_ERROR
    err = capsys.readouterr().err
    assert "Network/API error:" in err
    assert "Hint:" in err
