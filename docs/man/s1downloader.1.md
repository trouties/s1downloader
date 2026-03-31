# s1downloader(1)

## NAME
`s1downloader` - Search and download Sentinel-1 SLC products from ASF.

## SYNOPSIS
- `s1downloader search -s YYYYMMDD -e YYYYMMDD (--wkt WKT | --bbox BBOX | --aoi-file FILE) [options]`
- `s1downloader download --manifest FILE [options]`

## DESCRIPTION
`s1downloader` uses a two-phase workflow:
1. Search ASF and export a CSV manifest (+ optional PNG overview).
2. Download products from an existing manifest.

## SEARCH OPTIONS
- `-s, --start-date`: Start date in `YYYYMMDD`.
- `-e, --end-date`: End date in `YYYYMMDD`.
- `--wkt`: AOI WKT polygon.
- `--bbox`: AOI bbox (`minLon,minLat,maxLon,maxLat`).
- `--aoi-file`: AOI path (`.shp` or `.kml`).
- `--allow-aoi-fallback-prompt`: Allow interactive bbox fallback if AOI file parse fails.
- `--relative-orbit`: Filter by relative orbit number.
- `--max-results`: Positive integer or `max`.
- `--manifest-path`: Output manifest path.

## DOWNLOAD OPTIONS
- `--manifest`: Input search CSV.
- `-d, --download-dir, --output`: Download directory (default `./dataset`).
- `-t, --track`: `ASC` or `DES`.
- `--eof`: Also download matching EOF files to `./Orbit`.

## EXIT STATUS
- `0`: Success.
- `2`: Input validation error.
- `3`: Authentication error.
- `4`: Network/API error.
- `5`: File system error.
- `6`: Partial download failure.
