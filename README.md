# s1downloader

`s1downloader` is a Python CLI tool for searching and downloading Sentinel-1 SLC data from ASF.

`s1downloader` 是一个用于检索并下载 ASF Sentinel-1 SLC 数据的 Python 命令行工具。

## Features | 功能

- Two-phase workflow: `search` then `download`
- AOI input by `--bbox`, `--wkt`, or `--aoi-file` (`.shp`/`.kml`)
- Search result export to CSV + optional AOI/frame preview PNG
- Manifest-driven downloader with retry, failure manifest, and EOF support
- Legacy Python entry compatibility: `python -m s1downloader.main`

- 两阶段流程：先 `search` 再 `download`
- AOI 支持 `--bbox`、`--wkt`、`--aoi-file`（`.shp`/`.kml`）
- 搜索结果导出 CSV，并生成 AOI/框架示意图 PNG
- 基于 manifest 的下载器，支持重试、失败清单、EOF 下载
- 兼容旧入口：`python -m s1downloader.main`

## Requirements | 环境要求

- Python `3.10` / `3.11` / `3.12`
- Officially tested: Linux / WSL
- macOS: likely compatible but not fully verified in CI yet
- Native Windows: use WSL recommended (not officially supported yet)

## Install | 安装

### Option A: Git clone + editable install (recommended)

```bash
git clone https://github.com/example/s1downloader.git
cd s1downloader
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

### Option B: install directly from GitHub

```bash
python -m pip install "git+https://github.com/example/s1downloader.git"
```

### Option C: install a GitHub Release asset

1. Open GitHub Releases: `https://github.com/example/s1downloader/releases`
2. Download the wheel file (`.whl`) from the target version.
3. Install:

```bash
python -m pip install ./s1downloader-<version>-py3-none-any.whl
```

## Quick Start | 快速开始

### 1) Search

```bash
s1downloader search \
  -s 20240101 \
  -e 20240131 \
  --bbox 120.0,30.0,121.0,31.0 \
  --max-results 200
```

Output in current directory:
- `search_YYYYMMDD_HHMMSS.csv`
- `search_YYYYMMDD_HHMMSS.png` (when search result is not empty)

### 2) Download from manifest

```bash
s1downloader download \
  --manifest search_20240131_120000.csv \
  -t DES \
  -d ./dataset \
  --eof
```

## CLI Reference | 命令说明

### `search`

```bash
s1downloader search -s YYYYMMDD -e YYYYMMDD \
  (--bbox minLon,minLat,maxLon,maxLat | --wkt WKT | --aoi-file AOI_FILE)
```

Options:
- `--relative-orbit INT`
- `--max-results INT|max`
- `--manifest-path PATH`
- `--allow-aoi-fallback-prompt` (interactive fallback only)

### `download`

```bash
s1downloader download --manifest SEARCH_MANIFEST.csv [options]
```

Options:
- `-d, --download-dir, --output PATH`
- `-t, --track ASC|DES`
- `--eof`

### Exit Codes

- `0`: success
- `2`: input validation error
- `3`: authentication error
- `4`: network/API error
- `5`: file error
- `6`: partial download failure

## Project Layout | 项目结构

- `s1downloader/`: main source package
- `app/`: compatibility wrappers for legacy imports
- `tests/`: unit tests
- `docs/`: usage, architecture, FAQ, release notes
- `.github/workflows/`: CI and release workflows

## Development | 开发

```bash
python -m pip install -e .[dev]
ruff check s1downloader tests
ruff format s1downloader tests
pytest
tox
```

## Release & Versioning | 发布与版本管理

- SemVer: `MAJOR.MINOR.PATCH`
- Example tags: `v0.2.0`, `v0.2.1`
- Release channel: GitHub Releases (manual workflow dispatch)

Release steps:

```bash
git tag v0.2.0
git push origin v0.2.0
```

Then run GitHub Action `Release` and input the tag.

## FAQ / Troubleshooting | 常见问题

See:
- [`docs/tutorial.md`](docs/tutorial.md)
- [`docs/faq.md`](docs/faq.md)
- [`docs/man/s1downloader.1.md`](docs/man/s1downloader.1.md)

## Contributing | 贡献指南

1. Fork and create a feature branch.
2. Add/update tests.
3. Ensure `ruff` + `pytest` + `tox` pass.
4. Open a PR with a clear change summary.

欢迎提交 Issue / PR。请在提交前确保测试通过并附上变更说明。
