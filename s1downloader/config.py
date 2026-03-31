from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class AppConfig:
    project_root: Path
    manifest_dir: Path
    log_dir: Path
    timeout_sec: int = 120
    max_results: int | None = 200
    log_level: str = "INFO"


DEFAULT_CONFIG_FILE = "config.yaml"


def _resolve_path(project_root: Path, value: str | None, fallback: str) -> Path:
    raw = value or fallback
    p = Path(raw)
    if not p.is_absolute():
        p = project_root / p
    return p


def load_config(project_root: Path, config_path: Path | None) -> AppConfig:
    data: dict[str, Any] = {}
    source = config_path or (project_root / DEFAULT_CONFIG_FILE)
    if source.exists():
        with source.open("r", encoding="utf-8") as f:
            parsed = yaml.safe_load(f) or {}
            if isinstance(parsed, dict):
                data = parsed

    manifest_dir = _resolve_path(project_root, data.get("manifest_dir"), "data/manifests")
    log_dir = _resolve_path(project_root, data.get("log_dir"), "logs")

    max_results_raw = data.get("max_results", 200)
    if isinstance(max_results_raw, str) and max_results_raw.strip().lower() == "max":
        max_results: int | None = None
    else:
        max_results = int(max_results_raw)
        if max_results <= 0:
            raise ValueError("config max_results must be a positive integer or 'max'")

    timeout_sec = int(data.get("timeout_sec", 120))
    if timeout_sec <= 0:
        raise ValueError("config timeout_sec must be a positive integer")

    return AppConfig(
        project_root=project_root,
        manifest_dir=manifest_dir,
        log_dir=log_dir,
        timeout_sec=timeout_sec,
        max_results=max_results,
        log_level=str(data.get("log_level", "INFO")).upper(),
    )


def ensure_directories(config: AppConfig) -> None:
    config.manifest_dir.mkdir(parents=True, exist_ok=True)
    config.log_dir.mkdir(parents=True, exist_ok=True)
