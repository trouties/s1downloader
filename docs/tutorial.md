# Tutorial

## 1. Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

## 2. Prepare AOI

You can pass AOI in three ways:
- `--bbox minLon,minLat,maxLon,maxLat`
- `--wkt "POLYGON((...))"`
- `--aoi-file path/to/aoi.kml` or `.shp`

## 3. Search products

```bash
s1downloader search -s 20240101 -e 20240131 --bbox 120.0,30.0,121.0,31.0 --max-results max
```

Expected outputs in current directory:
- `search_*.csv`
- `search_*.png` (if results are found)

## 4. Download selected data

```bash
s1downloader download --manifest ./search_20240131_120000.csv -t ASC -d ./dataset
```

### Optional EOF download

```bash
s1downloader download --manifest ./search_20240131_120000.csv -d ./dataset --eof
```

## 5. Validate status manifests

Download task writes:
- `data/manifests/download_<task_id>.csv`
- `data/manifests/failed_<task_id>.csv` (only when failures exist)

## 6. Development checks

```bash
ruff check s1downloader tests
pytest
tox
```
