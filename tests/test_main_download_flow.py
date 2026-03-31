import logging
from types import SimpleNamespace

from s1downloader.config import AppConfig
from s1downloader.main import EXIT_OK, EXIT_PARTIAL_DOWNLOAD_FAILURE, run_download


def _build_config(tmp_path):
    return AppConfig(
        project_root=tmp_path,
        manifest_dir=tmp_path / "manifests",
        log_dir=tmp_path / "logs",
        timeout_sec=120,
        max_results=50,
        log_level="INFO",
    )


def test_run_download_uses_explicit_output_dir(tmp_path, monkeypatch):
    config = _build_config(tmp_path)
    output_dir = tmp_path / "custom_out"
    args = SimpleNamespace(
        manifest=tmp_path / "search.csv",
        download_dir=output_dir,
        track=None,
        eof=False,
    )
    logger = logging.getLogger("test.main.download.output")

    monkeypatch.setattr("s1downloader.main.get_or_create_credentials", lambda **kwargs: ("u", "p"))
    monkeypatch.setattr(
        "s1downloader.main.build_download_preview",
        lambda **kwargs: {"manifest_total": 10, "filtered_total": 5, "track_tokens": []},
    )
    captured = {}

    def _fake_run_download_from_manifest(**kwargs):
        captured["download_root"] = kwargs["download_root"]
        return {
            "task_id": "d_test",
            "status_manifest": str(tmp_path / "status.csv"),
            "total": 1,
            "success": 1,
            "failed": 0,
            "skipped": 0,
            "missing": 0,
        }

    monkeypatch.setattr("s1downloader.main.run_download_from_manifest", _fake_run_download_from_manifest)

    code = run_download(args, config, logger)
    assert code == EXIT_OK
    assert captured["download_root"] == output_dir
    assert output_dir.exists()


def test_run_download_defaults_to_pwd_dataset(tmp_path, monkeypatch):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        manifest=tmp_path / "search.csv",
        download_dir=None,
        track=None,
        eof=False,
    )
    logger = logging.getLogger("test.main.download.default")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("s1downloader.main.get_or_create_credentials", lambda **kwargs: ("u", "p"))
    monkeypatch.setattr(
        "s1downloader.main.build_download_preview",
        lambda **kwargs: {"manifest_total": 10, "filtered_total": 10, "track_tokens": []},
    )
    captured = {}

    def _fake_run_download_from_manifest(**kwargs):
        captured["download_root"] = kwargs["download_root"]
        return {
            "task_id": "d_test",
            "status_manifest": str(tmp_path / "status.csv"),
            "total": 1,
            "success": 1,
            "failed": 0,
            "skipped": 0,
            "missing": 0,
        }

    monkeypatch.setattr("s1downloader.main.run_download_from_manifest", _fake_run_download_from_manifest)

    code = run_download(args, config, logger)
    assert code == EXIT_OK
    assert captured["download_root"] == (tmp_path / "dataset")
    assert (tmp_path / "dataset").exists()


def test_run_download_returns_nonzero_when_failed_items_exist(tmp_path, monkeypatch):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        manifest=tmp_path / "search.csv",
        download_dir=None,
        track=None,
        eof=False,
    )
    logger = logging.getLogger("test.main.download.fail")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("s1downloader.main.get_or_create_credentials", lambda **kwargs: ("u", "p"))
    monkeypatch.setattr(
        "s1downloader.main.build_download_preview",
        lambda **kwargs: {"manifest_total": 10, "filtered_total": 10, "track_tokens": []},
    )

    def _fake_run_download_from_manifest(**kwargs):
        return {
            "task_id": "d_test",
            "status_manifest": str(tmp_path / "status.csv"),
            "failed_manifest": str(tmp_path / "failed.csv"),
            "total": 2,
            "success": 1,
            "failed": 1,
            "skipped": 0,
            "missing": 0,
        }

    monkeypatch.setattr("s1downloader.main.run_download_from_manifest", _fake_run_download_from_manifest)

    code = run_download(args, config, logger)
    assert code == EXIT_PARTIAL_DOWNLOAD_FAILURE


def test_run_download_passes_eof_flag_and_prints_eof_summary(tmp_path, monkeypatch, capsys):
    config = _build_config(tmp_path)
    args = SimpleNamespace(
        manifest=tmp_path / "search.csv",
        download_dir=None,
        track=None,
        eof=True,
    )
    logger = logging.getLogger("test.main.download.eof")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("s1downloader.main.get_or_create_credentials", lambda **kwargs: ("u", "p"))
    monkeypatch.setattr(
        "s1downloader.main.build_download_preview",
        lambda **kwargs: {"manifest_total": 3, "filtered_total": 3, "track_tokens": []},
    )
    captured = {}

    def _fake_run_download_from_manifest(**kwargs):
        captured["download_eof"] = kwargs["download_eof"]
        return {
            "task_id": "d_test",
            "status_manifest": str(tmp_path / "status.csv"),
            "total": 3,
            "success": 3,
            "failed": 0,
            "skipped": 0,
            "missing": 0,
            "eof_success": 2,
            "eof_skipped": 1,
            "eof_failed": 0,
        }

    monkeypatch.setattr("s1downloader.main.run_download_from_manifest", _fake_run_download_from_manifest)

    code = run_download(args, config, logger)
    assert code == EXIT_OK
    assert captured["download_eof"] is True
    output = capsys.readouterr().out
    assert "EOF Summary:" in output
