# CLI Usage

## Show help

```bash
s1downloader --help
s1downloader search --help
s1downloader download --help
```

Compatibility entry for old automation:

```bash
python -m s1downloader.main --help
```

## Search examples

### BBOX search

```bash
s1downloader search \
  -s 20240101 \
  -e 20240131 \
  --bbox 120.0,30.0,121.0,31.0 \
  --relative-orbit 42 \
  --max-results 200
```

### KML/SHP input

```bash
s1downloader search -s 20240101 -e 20240131 --aoi-file ./aoi.kml
```

If you want interactive bbox fallback when AOI parsing fails:

```bash
s1downloader search ... --aoi-file ./broken.kml --allow-aoi-fallback-prompt
```

## Download examples

### Download only descending track

```bash
s1downloader download --manifest ./search_20240131_120000.csv -t DES -d ./dataset
```

### Download + EOF

```bash
s1downloader download --manifest ./search_20240131_120000.csv -d ./dataset --eof
```

## Common options

- `--config PATH`: custom config file path
- `--manifest-path PATH`: custom output manifest for search
- `--download-dir PATH`: custom download root for download

## Exit codes

- `0` success
- `2` input validation error
- `3` auth error
- `4` network/API error
- `5` file system error
- `6` partial download failure
