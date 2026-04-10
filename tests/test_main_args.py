import argparse

import pytest

from s1downloader.config import load_config
from s1downloader.main import _parse_max_results_arg, _parse_track_arg, build_parser


def test_parse_max_results_arg_accepts_integer():
    assert _parse_max_results_arg("200") == 200


def test_parse_max_results_arg_accepts_max_keyword():
    assert _parse_max_results_arg("max") is None


def test_parse_max_results_arg_rejects_invalid():
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_max_results_arg("0")


def test_parse_track_arg_normalizes_values():
    assert _parse_track_arg("ascending") == "ASC"
    assert _parse_track_arg("DES") == "DES"


def test_parse_track_arg_rejects_invalid_values():
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_track_arg("left")


def test_config_allows_max_results_keyword(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("max_results: max\n", encoding="utf-8")
    config = load_config(project_root=tmp_path, config_path=config_file)
    assert config.max_results is None


def test_verbose_flag_accepted_by_parser():
    parser = build_parser()
    args = parser.parse_args(["-v", "search", "-s", "20240101", "-e", "20240131", "--bbox", "1,2,3,4"])
    assert args.verbose is True


def test_verbose_flag_defaults_to_false():
    parser = build_parser()
    args = parser.parse_args(["search", "-s", "20240101", "-e", "20240131", "--bbox", "1,2,3,4"])
    assert args.verbose is False


def test_workers_flag_accepted_by_parser():
    parser = build_parser()
    args = parser.parse_args(["download", "--manifest", "x.csv", "--workers", "8"])
    assert args.workers == 8


def test_workers_flag_defaults_to_none():
    parser = build_parser()
    args = parser.parse_args(["download", "--manifest", "x.csv"])
    assert args.workers is None


def test_workers_config_from_yaml(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("workers: 8\n", encoding="utf-8")
    config = load_config(project_root=tmp_path, config_path=config_file)
    assert config.workers == 8


def test_workers_config_defaults_to_four(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("timeout_sec: 60\n", encoding="utf-8")
    config = load_config(project_root=tmp_path, config_path=config_file)
    assert config.workers == 4
