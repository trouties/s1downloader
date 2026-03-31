# FAQ

## Q1: Why does `pytest` command fail with "command not found"?

Install dev dependencies first:

```bash
python -m pip install -e .[dev]
```

Then run:

```bash
python -m pytest
```

## Q2: Why does authentication fail repeatedly?

- Confirm Earthdata username/password are correct.
- Check if `~/.netrc` contains `machine urs.earthdata.nasa.gov`.
- Remove broken `~/.netrc` entries and retry.

## Q3: Why does `search` fail for KML/SHP?

- Ensure file extension is `.kml` or `.shp`.
- Verify geometry is valid and not empty.
- For interactive fallback, add `--allow-aoi-fallback-prompt`.

## Q4: Why does download fail for some scenes?

Possible causes:
- transient network timeout
- authorization mismatch
- scene URL no longer valid

The tool retries up to 3 times per file and writes `failed_<task_id>.csv`.

## Q5: Can I install without PyPI?

Yes.
- Install from GitHub repository (`pip install git+...`)
- Or install wheel from GitHub Releases assets.

## Q6: How to create a release?

1. Tag the commit: `git tag vX.Y.Z`
2. Push tag: `git push origin vX.Y.Z`
3. Run GitHub Action `Release` manually and provide the tag.
