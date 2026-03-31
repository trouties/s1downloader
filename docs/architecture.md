# Architecture

## Design Goals

- Keep CLI thin and service layer reusable.
- Preserve a two-phase pipeline: search first, download later.
- Maintain compatibility for old import path (`app.*`) during migration.

## Layers

1. Entry layer
- `s1downloader/main.py`: argparse setup, command dispatch, exit codes.

2. Service layer
- `s1downloader/search_service.py`: ASF search and product mapping.
- `s1downloader/plot_service.py`: AOI and frame preview rendering.
- `s1downloader/download_service.py`: manifest-based downloader and EOF logic.

3. Infrastructure layer
- `s1downloader/aoi.py`: AOI parsing and normalization.
- `s1downloader/auth.py`: Earthdata credential lifecycle (`~/.netrc`).
- `s1downloader/config.py`: config load and runtime paths.
- `s1downloader/manifest.py`: CSV manifest persistence.
- `s1downloader/logging_setup.py`: structured app logger.

## Data Flow

### Search flow

1. Parse/validate dates and AOI.
2. Call ASF API with normalized request.
3. Map products to internal `SearchResultItem`.
4. Write search CSV manifest.
5. Render PNG overview when there are results.

### Download flow

1. Load search manifest.
2. Apply optional track filter (`ASC`/`DES`).
3. Download SLC files with retries.
4. Append status manifest per file.
5. Optionally resolve and download EOF files.
6. Write failed manifest when needed.

## Compatibility

- Source package is `s1downloader`.
- `app/*` modules are wrappers for transition and should be treated as deprecated.
