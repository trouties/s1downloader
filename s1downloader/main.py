from __future__ import annotations

import argparse
import platform
import sys
import time
from datetime import date, datetime
from pathlib import Path

from s1downloader import __version__
from s1downloader.aoi import AOIError, normalize_aoi_to_wkt
from s1downloader.auth import AuthError, get_or_create_credentials
from s1downloader.config import ensure_directories, load_config
from s1downloader.download_service import build_download_preview, format_bytes, run_download_from_manifest
from s1downloader.logging_setup import setup_logging
from s1downloader.manifest import generate_query_id, write_search_manifest
from s1downloader.models import SearchRequest, SearchResultItem
from s1downloader.search_service import NetworkError, search_sentinel1_slc

EXIT_OK = 0
EXIT_INPUT_ERROR = 2
EXIT_AUTH_ERROR = 3
EXIT_NETWORK_OR_API_ERROR = 4
EXIT_FILE_ERROR = 5
EXIT_PARTIAL_DOWNLOAD_FAILURE = 6

MAX_RESULTS_USE_CONFIG = -1


def _parse_compact_date(text: str) -> date:
    raw = (text or "").strip()
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError("Dates must use YYYYMMDD format")
    try:
        return date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
    except ValueError as exc:
        raise ValueError("Dates must use YYYYMMDD format") from exc


def _validate_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    try:
        start = _parse_compact_date(start_date)
        end = _parse_compact_date(end_date)
    except ValueError as exc:
        raise ValueError("Dates must use YYYYMMDD format") from exc

    if start > end:
        raise ValueError("start-date must be earlier than or equal to end-date")
    return (start.isoformat(), end.isoformat())


def _parse_max_results_arg(value: str) -> int | None:
    raw = (value or "").strip().lower()
    if raw == "max":
        return None
    try:
        number = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("max-results must be a positive integer or 'max'") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("max-results must be a positive integer or 'max'")
    return number


def _parse_track_arg(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().upper()
    if not text:
        return None
    if text.startswith("ASC"):
        return "ASC"
    if text.startswith("DES"):
        return "DES"
    raise argparse.ArgumentTypeError("track must be ASC or DES")


def _format_table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, col in enumerate(row):
            widths[i] = max(widths[i], len(col))

    def render_line(values: list[str]) -> str:
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(values))

    sep = "-+-".join("-" * w for w in widths)
    lines = [render_line(headers), sep]
    lines.extend(render_line(r) for r in rows)
    return "\n".join(lines)


def _print_search_results_table(items: list[SearchResultItem]) -> None:
    if not items:
        print("No results found.")
        return

    headers = ["idx", "granule_id", "acquisition_time", "orbit", "dir", "pol", "size_mb"]
    rows = []
    for item in items:
        rows.append(
            [
                str(item.index),
                item.granule_id,
                item.acquisition_time,
                item.relative_orbit or "",
                item.orbit_direction or "",
                item.polarization or "",
                "" if item.size_mb is None else str(item.size_mb),
            ]
        )

    print(_format_table(rows, headers))


def _default_search_output_path(ext: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = Path.cwd() / f"search_{stamp}.{ext}"
    if not candidate.exists():
        return candidate
    for i in range(1, 1000):
        p = Path.cwd() / f"search_{stamp}_{i}.{ext}"
        if not p.exists():
            return p
    raise RuntimeError("Could not allocate output filename")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="s1downloader",
        description="Search and download Sentinel-1 SLC data from ASF in two phases.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config.yaml (default: project_root/config.yaml)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG-level) logging output",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    search = sub.add_parser("search", help="Run ASF search and save manifest only")
    search.add_argument("-s", "--start-date", required=True, help="Start date (YYYYMMDD)")
    search.add_argument("-e", "--end-date", required=True, help="End date (YYYYMMDD)")
    search.add_argument(
        "--relative-orbit",
        type=int,
        default=None,
        help="Filter query by relative orbit number",
    )

    aoi_group = search.add_mutually_exclusive_group(required=True)
    aoi_group.add_argument("--wkt", help="AOI in WKT")
    aoi_group.add_argument("--bbox", help="AOI bbox: minLon,minLat,maxLon,maxLat")
    aoi_group.add_argument("--aoi-file", help="AOI file path (.shp or .kml)")
    search.add_argument(
        "--allow-aoi-fallback-prompt",
        action="store_true",
        help="Allow interactive bbox prompt when --aoi-file parsing fails",
    )

    search.add_argument(
        "--max-results",
        type=_parse_max_results_arg,
        default=MAX_RESULTS_USE_CONFIG,
        help="Maximum number of results, or 'max' for all available results",
    )
    search.add_argument("--manifest-path", type=Path, default=None, help="Output search manifest path")

    download = sub.add_parser("download", help="Download from a search manifest")
    download.add_argument("--manifest", type=Path, required=True, help="Path to search manifest CSV")
    download.add_argument(
        "--download-dir",
        "--output",
        "-d",
        dest="download_dir",
        type=Path,
        default=None,
        help="Download directory (default: ./dataset in current working directory)",
    )
    download.add_argument(
        "-t",
        "--track",
        type=_parse_track_arg,
        default=None,
        help="Optional orbit-direction filter",
    )
    download.add_argument(
        "--eof",
        action="store_true",
        help="Also download matching Sentinel-1 EOF files into ./Orbit",
    )

    return parser


def run_search(args, config, logger) -> int:
    normalized_start_date, normalized_end_date = _validate_date_range(args.start_date, args.end_date)

    intersects_with = normalize_aoi_to_wkt(
        wkt_text=args.wkt,
        bbox_text=args.bbox,
        aoi_file=args.aoi_file,
        allow_prompt_fallback=bool(args.allow_aoi_fallback_prompt and sys.stdin.isatty()),
    )

    if args.max_results == MAX_RESULTS_USE_CONFIG:
        max_results = config.max_results
    else:
        max_results = args.max_results
    request = SearchRequest(
        start_date=normalized_start_date,
        end_date=normalized_end_date,
        intersects_with=intersects_with,
        max_results=max_results,
        relative_orbit=getattr(args, "relative_orbit", None),
    )

    search_started = time.perf_counter()
    items = search_sentinel1_slc(request, logger)
    search_elapsed = time.perf_counter() - search_started
    logger.info("Search completed: %d result(s) in %.1fs", len(items), search_elapsed)
    _print_search_results_table(items)
    print(f"Search completed: {len(items)} result(s) in {search_elapsed:.1f}s")

    query_id = generate_query_id()
    manifest_path = args.manifest_path or _default_search_output_path("csv")
    write_search_manifest(manifest_path, query_id, items)

    print(f"Search manifest saved: {manifest_path}")
    if items:
        if args.manifest_path:
            plot_path = args.manifest_path.with_suffix(".png")
        else:
            plot_path = manifest_path.with_suffix(".png")
        try:
            from s1downloader.plot_service import render_search_overview_png

            render_search_overview_png(
                aoi_wkt=intersects_with,
                items=items,
                output_path=plot_path,
                logger=logger,
            )
            print(f"Search overview PNG saved: {plot_path}")
        except Exception as exc:
            logger.warning("Failed to generate search overview PNG: %s", exc)
            print(f"Search overview PNG skipped: {exc}")
    print("Search phase completed. No files were downloaded.")
    return EXIT_OK


def run_download(args, config, logger) -> int:
    credentials = get_or_create_credentials(logger=logger, interactive=sys.stdin.isatty())

    download_dir = args.download_dir or (Path.cwd() / "dataset")
    download_dir.mkdir(parents=True, exist_ok=True)
    preview = build_download_preview(manifest_path=args.manifest, track_filter=args.track)

    track_text = ",".join(preview["track_tokens"]) if preview["track_tokens"] else "ALL"
    print("Download task overview:")
    print(f"- Manifest: {args.manifest}")
    print(f"- Download dir: {download_dir}")
    print(f"- Track filter: {track_text}")
    print(f"- Items in manifest: {preview['manifest_total']}")
    print(f"- Items to process: {preview['filtered_total']}")
    print("- Progress: a single live progress line will be shown below")
    print(f"- EOF download: {'ON (./Orbit)' if args.eof else 'OFF'}")

    summary = run_download_from_manifest(
        manifest_path=args.manifest,
        track_filter=args.track,
        download_root=download_dir,
        status_manifest_dir=config.manifest_dir,
        timeout_sec=config.timeout_sec,
        credentials=credentials,
        logger=logger,
        show_progress=True,
        download_eof=bool(args.eof),
    )

    elapsed_sec = summary.get("elapsed_sec", 0.0)
    total_bytes = summary.get("total_bytes", 0)
    print("Download completed.")
    print(f"Task ID: {summary['task_id']}")
    print(f"Status manifest: {summary['status_manifest']}")
    if summary.get("failed_manifest"):
        print(f"Failed items manifest: {summary['failed_manifest']}")
    print(
        "Summary: "
        f"total={summary['total']}, success={summary['success']}, "
        f"failed={summary['failed']}, skipped={summary['skipped']}"
    )
    if elapsed_sec > 0:
        throughput = format_bytes(int(total_bytes / elapsed_sec)) + "ps" if total_bytes > 0 else "N/A"
        print(f"Elapsed: {elapsed_sec:.1f}s | Downloaded: {format_bytes(total_bytes)} | Avg speed: {throughput}")
    if args.eof:
        print(
            "EOF Summary: "
            f"success={summary.get('eof_success', 0)}, "
            f"skipped={summary.get('eof_skipped', 0)}, "
            f"failed={summary.get('eof_failed', 0)}"
        )
    if int(summary["failed"]) > 0:
        return EXIT_PARTIAL_DOWNLOAD_FAILURE
    return EXIT_OK


_ERROR_HINTS: dict[str, str] = {
    "input": "Run with --verbose for details. Use --help to check argument format.",
    "auth": "Check your Earthdata credentials at ~/.netrc or re-run to enter them interactively.",
    "network": "Run with --verbose for a full traceback. Check network connectivity.",
    "file": "Check file permissions and available disk space.",
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    config = load_config(project_root=project_root, config_path=args.config)
    ensure_directories(config)

    log_level = "DEBUG" if getattr(args, "verbose", False) else config.log_level
    logger = setup_logging(config.log_dir, log_level)

    logger.debug(
        "s1downloader %s | Python %s | %s %s (%s)",
        __version__,
        sys.version.split()[0],
        platform.system(),
        platform.release(),
        platform.machine(),
    )
    if platform.system() == "Windows":
        logger.warning("Native Windows is not officially supported. Consider using WSL.")

    try:
        if args.command == "search":
            return run_search(args, config, logger)
        if args.command == "download":
            return run_download(args, config, logger)
        raise ValueError(f"Unsupported command: {args.command}")
    except (ValueError, AOIError) as exc:
        logger.error("Input error: %s", exc)
        print(f"Input error: {exc}", file=sys.stderr)
        print(f"Hint: {_ERROR_HINTS['input']}", file=sys.stderr)
        return EXIT_INPUT_ERROR
    except AuthError as exc:
        logger.error("Auth error: %s", exc)
        print(f"Auth error: {exc}", file=sys.stderr)
        print(f"Hint: {_ERROR_HINTS['auth']}", file=sys.stderr)
        return EXIT_AUTH_ERROR
    except NetworkError as exc:
        logger.error("Network/API error: %s", exc)
        print(f"Network/API error: {exc}", file=sys.stderr)
        print(f"Hint: {_ERROR_HINTS['network']}", file=sys.stderr)
        return EXIT_NETWORK_OR_API_ERROR
    except OSError as exc:
        logger.error("File error: %s", exc)
        print(f"File error: {exc}", file=sys.stderr)
        print(f"Hint: {_ERROR_HINTS['file']}", file=sys.stderr)
        return EXIT_FILE_ERROR
    except Exception as exc:  # pragma: no cover
        logger.exception("Unexpected error")
        print(f"Unexpected error: {exc}", file=sys.stderr)
        print(f"Hint: {_ERROR_HINTS['network']}", file=sys.stderr)
        return EXIT_NETWORK_OR_API_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
