from __future__ import annotations

import logging
import re
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from s1downloader.manifest import (
    append_download_status,
    generate_task_id,
    read_search_manifest,
    utc_now_iso,
    write_failed_manifest,
)
from s1downloader.models import DownloadStatusRecord, SearchResultItem

_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)
MAX_DOWNLOAD_ATTEMPTS = 3
_RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}
_RETRY_DELAYS_SEC = (2.0, 5.0)
_EARTHDATA_HOST = "urs.earthdata.nasa.gov"
_LIVE_BAR_WIDTH = 18
_LIVE_REFRESH_INTERVAL_SEC = 0.4
_EOF_INDEX_URL = "https://s1qc.asf.alaska.edu/aux_poeorb"
_EOF_FILENAME_RE = re.compile(r"(S1[AB].*?_V\d{8}T\d{6}_\d{8}T\d{6}\.EOF)")
_EOF_RANGE_RE = re.compile(r"^(S1[AB]).*_V(\d{8}T\d{6})_(\d{8}T\d{6})\.EOF$")
_SCENE_ID_RE = re.compile(r"^(S1[AB])_.*?_(\d{8}T\d{6})_\d{8}T\d{6}_")


@dataclass
class _AttemptResult:
    status: str
    attempt: int
    error: str = ""
    error_type: str = ""


@dataclass(frozen=True)
class _EOFEntry:
    satellite: str
    valid_start: datetime
    valid_end: datetime
    name: str


class _EarthdataSession(requests.Session):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.auth = (username, password)
        # Keep auth/download path independent from shell proxy env vars by default.
        self.trust_env = False

    def rebuild_auth(self, prepared_request, response):
        # Requests strips auth headers after cross-host redirects by default.
        # Keep Earthdata auth when redirected to URS OAuth endpoint.
        host = (urlparse(prepared_request.url).hostname or "").lower()
        if host == _EARTHDATA_HOST and self.auth:
            prepared_request.prepare_auth(self.auth, prepared_request.url)
            return
        super().rebuild_auth(prepared_request, response)


def _parse_compact_utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


def _parse_scene_satellite_and_time(item: SearchResultItem) -> tuple[str, datetime] | None:
    granule = (item.granule_id or "").strip()
    match = _SCENE_ID_RE.match(granule)
    if match:
        satellite = match.group(1)
        scene_time = _parse_compact_utc(match.group(2))
        return satellite, scene_time

    acq_text = (item.acquisition_time or "").strip()
    if acq_text:
        if acq_text.endswith("Z"):
            acq_text = acq_text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(acq_text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            sat_fallback = granule[:3] if granule.startswith("S1") and len(granule) >= 3 else ""
            if sat_fallback in {"S1A", "S1B"}:
                return sat_fallback, parsed
        except ValueError:
            return None
    return None


def _parse_eof_entries(index_html: str) -> list[_EOFEntry]:
    entries: list[_EOFEntry] = []
    seen: set[str] = set()
    for raw_name in _EOF_FILENAME_RE.findall(index_html):
        name = raw_name.strip()
        if name in seen:
            continue
        seen.add(name)
        match = _EOF_RANGE_RE.match(name)
        if not match:
            continue
        try:
            entries.append(
                _EOFEntry(
                    satellite=match.group(1),
                    valid_start=_parse_compact_utc(match.group(2)),
                    valid_end=_parse_compact_utc(match.group(3)),
                    name=name,
                )
            )
        except ValueError:
            continue

    entries.sort(key=lambda e: (e.satellite, e.valid_start, e.name))
    return entries


def _fetch_eof_entries(timeout_sec: int, logger: logging.Logger) -> list[_EOFEntry]:
    with requests.Session() as session:
        session.trust_env = False
        response = session.get(_EOF_INDEX_URL, timeout=timeout_sec)
        response.raise_for_status()
        entries = _parse_eof_entries(response.text)
    if not entries:
        logger.warning("No EOF entries parsed from %s", _EOF_INDEX_URL)
    return entries


def _match_eof_name(entries: list[_EOFEntry], satellite: str, scene_time: datetime) -> str | None:
    candidates = [
        entry for entry in entries if entry.satellite == satellite and entry.valid_start <= scene_time <= entry.valid_end
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda e: (e.valid_start, e.valid_end))
    return candidates[-1].name


def format_bytes(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "?"
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(value)}{units[idx]}"
    return f"{value:.1f}{units[idx]}"


def _render_live_line(
    *,
    item_no: int,
    total_items: int,
    success: int,
    failed: int,
    skipped: int,
    downloaded_bytes: int,
    total_bytes: int | None,
    speed_bps: float,
) -> str:
    if total_bytes and total_bytes > 0:
        ratio = min(1.0, max(0.0, downloaded_bytes / total_bytes))
        filled = int(ratio * _LIVE_BAR_WIDTH)
        bar = "#" * filled + "-" * (_LIVE_BAR_WIDTH - filled)
        pct = f"{int(ratio * 100):>3d}%"
        size_text = f"{format_bytes(downloaded_bytes)}/{format_bytes(total_bytes)}"
    else:
        bar = "#" * min(_LIVE_BAR_WIDTH, max(1, (downloaded_bytes // (1024 * 1024)) % (_LIVE_BAR_WIDTH + 1)))
        bar = bar.ljust(_LIVE_BAR_WIDTH, "-")
        pct = " --%"
        size_text = f"{format_bytes(downloaded_bytes)}/?"

    speed_text = f"{format_bytes(int(max(speed_bps, 0.0)))}ps"
    return f"[{item_no}/{total_items}] [{bar}] {pct} {size_text} {speed_text} (ok={success}, skip={skipped}, fail={failed})"


def _acquisition_date_folder(acquisition_time: str) -> str:
    text = (acquisition_time or "").strip()
    if not text:
        return "unknown_date"

    if text.endswith("Z") and "T" in text:
        text = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text).strftime("%Y%m%d")
        except ValueError:
            pass

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(acquisition_time, fmt).strftime("%Y%m%d")
        except ValueError:
            continue

    return "unknown_date"


def _safe_filename(item: SearchResultItem) -> str:
    parsed = urlparse(item.download_url)
    candidate = unquote(Path(parsed.path).name)

    if not candidate:
        candidate = f"{item.granule_id}.zip"
    if "." not in candidate:
        candidate = f"{candidate}.zip"

    # Keep filenames shell-safe and portable.
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "_", candidate)
    return candidate


def _build_target_path(download_root: Path, item: SearchResultItem) -> Path:
    day_folder = _acquisition_date_folder(item.acquisition_time)
    target_dir = download_root / day_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / _safe_filename(item)


def _part_path(target_path: Path) -> Path:
    return target_path.with_name(f"{target_path.name}.part")


def _cleanup_part_file(target_path: Path) -> None:
    part_path = _part_path(target_path)
    if part_path.exists():
        part_path.unlink()


def _download_file(
    session: requests.Session,
    url: str,
    target_path: Path,
    timeout_sec: int,
    progress_hook: Callable[[int, int | None], None] | None = None,
) -> None:
    part_path = _part_path(target_path)
    with session.get(url, stream=True, timeout=timeout_sec) as response:
        response.raise_for_status()
        total_bytes: int | None = None
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                total_bytes = int(content_length)
            except (TypeError, ValueError):
                total_bytes = None
        downloaded = 0
        if progress_hook:
            progress_hook(downloaded, total_bytes)
        with part_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_hook:
                        progress_hook(downloaded, total_bytes)
    part_path.replace(target_path)


def _retry_delay_seconds(attempt: int) -> float:
    idx = max(0, attempt - 1)
    if idx < len(_RETRY_DELAYS_SEC):
        return float(_RETRY_DELAYS_SEC[idx])
    return float(_RETRY_DELAYS_SEC[-1])


def _classify_download_exception(exc: Exception) -> tuple[str, bool]:
    if isinstance(exc, requests.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        status_label = "http_error" if status_code is None else f"http_{status_code}"
        if status_code in (401, 403):
            return status_label, False
        if status_code is not None and 400 <= status_code < 500 and status_code not in _RETRYABLE_HTTP_STATUS:
            return status_label, False
        return status_label, True
    if isinstance(exc, requests.Timeout):
        return "timeout", True
    if isinstance(exc, requests.ConnectionError):
        return "connection_error", True
    if isinstance(exc, (requests.InvalidURL, requests.MissingSchema, requests.InvalidSchema)):
        return "invalid_url", False
    if isinstance(exc, requests.RequestException):
        return "request_error", True
    if isinstance(exc, ValueError):
        return "input_error", False
    if isinstance(exc, OSError):
        return "file_error", False
    return "unknown_error", False


def _download_with_retries(
    *,
    session: requests.Session,
    item: SearchResultItem,
    target_path: Path,
    timeout_sec: int,
    logger: logging.Logger,
    max_attempts: int = MAX_DOWNLOAD_ATTEMPTS,
    on_attempt: Callable[[int], None] | None = None,
    on_progress: Callable[[int, int | None, float], None] | None = None,
) -> _AttemptResult:
    for attempt in range(1, max_attempts + 1):
        if on_attempt:
            on_attempt(attempt)
        attempt_started = time.perf_counter()
        try:
            progress_hook = None
            if on_progress is not None:
                # Bind per-attempt start timestamp to avoid late-bound closure issues.
                def progress_hook(
                    downloaded: int,
                    total_bytes: int | None,
                    _attempt_started: float = attempt_started,
                ) -> None:
                    on_progress(
                        downloaded,
                        total_bytes,
                        time.perf_counter() - _attempt_started,
                    )

            _download_file(
                session,
                item.download_url,
                target_path,
                timeout_sec,
                progress_hook=progress_hook,
            )
            return _AttemptResult(status="success", attempt=attempt)
        except Exception as exc:
            error_text = (str(exc) or "").strip() or exc.__class__.__name__
            error_type, is_retryable = _classify_download_exception(exc)
            label = (item.granule_id or "").strip() or "unknown"
            if (not is_retryable) or (attempt >= max_attempts):
                logger.warning(
                    "Download failed [%s] after %d attempt(s) (%s): %s",
                    label,
                    attempt,
                    error_type,
                    error_text,
                )
            else:
                logger.warning(
                    "Download issue [%s]: %s (retry %d/%d)",
                    label,
                    error_type,
                    attempt + 1,
                    max_attempts,
                )
            _cleanup_part_file(target_path)

            if (not is_retryable) or (attempt >= max_attempts):
                return _AttemptResult(
                    status="failed",
                    attempt=attempt,
                    error=error_text,
                    error_type=error_type,
                )

            delay_sec = _retry_delay_seconds(attempt)
            logger.debug("Waiting %.1fs before next attempt", delay_sec)
            time.sleep(delay_sec)

    return _AttemptResult(status="failed", attempt=max_attempts, error_type="unknown_error")


def _download_url_with_retries(
    *,
    session: requests.Session,
    url: str,
    target_path: Path,
    timeout_sec: int,
    logger: logging.Logger,
    label: str,
    max_attempts: int = MAX_DOWNLOAD_ATTEMPTS,
    retry_auth_errors: bool = False,
) -> _AttemptResult:
    for attempt in range(1, max_attempts + 1):
        try:
            _download_file(session, url, target_path, timeout_sec)
            return _AttemptResult(status="success", attempt=attempt)
        except Exception as exc:
            error_text = (str(exc) or "").strip() or exc.__class__.__name__
            error_type, is_retryable = _classify_download_exception(exc)
            if retry_auth_errors and error_type in {"http_401", "http_403"}:
                is_retryable = True
            if (not is_retryable) or (attempt >= max_attempts):
                logger.warning(
                    "EOF download failed [%s] after %d attempt(s) (%s): %s",
                    label,
                    attempt,
                    error_type,
                    error_text,
                )
                _cleanup_part_file(target_path)
                return _AttemptResult(
                    status="failed",
                    attempt=attempt,
                    error=error_text,
                    error_type=error_type,
                )

            logger.warning(
                "EOF download issue [%s]: %s (retry %d/%d)",
                label,
                error_type,
                attempt + 1,
                max_attempts,
            )
            _cleanup_part_file(target_path)
            time.sleep(_retry_delay_seconds(attempt))

    return _AttemptResult(status="failed", attempt=max_attempts, error_type="unknown_error")


def _normalize_track_tokens(text: str | None) -> set[str]:
    if not text:
        return set()
    tokens: set[str] = set()
    for raw in re.split(r"[\s,;/+]+", text):
        token = raw.strip().upper()
        if not token:
            continue
        if token.startswith("ASC"):
            tokens.add("ASC")
        elif token.startswith("DES"):
            tokens.add("DES")
        else:
            tokens.add(token)
    return tokens


def _match_track_direction(item_orbit_direction: str | None, requested_tokens: set[str]) -> bool:
    if not requested_tokens:
        return True
    text = (item_orbit_direction or "").strip().upper()
    if text.startswith("ASC"):
        item_token = "ASC"
    elif text.startswith("DES"):
        item_token = "DES"
    else:
        item_token = text
    return bool(item_token) and (item_token in requested_tokens)


def build_download_preview(
    *,
    manifest_path: Path,
    track_filter: str | None,
) -> dict[str, object]:
    items = read_search_manifest(manifest_path)
    total_items = len(items)
    track_tokens = _normalize_track_tokens(track_filter)

    filtered_items = items
    if track_tokens:
        filtered_items = [item for item in items if _match_track_direction(item.orbit_direction, track_tokens)]

    return {
        "manifest_total": total_items,
        "filtered_total": len(filtered_items),
        "track_tokens": sorted(track_tokens),
    }


def _progress_bar(
    *,
    processed: int,
    total: int,
    success: int,
    failed: int,
    skipped: int,
    attempt_info: str | None = None,
    width: int = 28,
) -> str:
    if total <= 0:
        return "[no items]"
    ratio = processed / total
    filled = min(width, max(0, int(ratio * width)))
    bar = "#" * filled + "-" * (width - filled)
    pct = int(ratio * 100)
    text = f"[{bar}] {processed}/{total} {pct:>3d}% (ok={success}, skip={skipped}, fail={failed})"
    if attempt_info:
        text = f"{text} | now: {attempt_info}"
    return text


def run_download_from_manifest(
    *,
    manifest_path: Path,
    track_filter: str | None,
    download_root: Path,
    status_manifest_dir: Path,
    timeout_sec: int,
    credentials: tuple[str, str],
    logger: logging.Logger,
    show_progress: bool = True,
    download_eof: bool = False,
) -> dict[str, object]:
    items = read_search_manifest(manifest_path)
    selected_items = list(items)

    if not selected_items:
        raise ValueError("Search manifest is empty")

    track_tokens = _normalize_track_tokens(track_filter)
    if track_tokens:
        selected_items = [item for item in selected_items if _match_track_direction(item.orbit_direction, track_tokens)]
        logger.info("Applied track filter: %s -> %d item(s)", sorted(track_tokens), len(selected_items))

    if not selected_items:
        if track_tokens:
            raise ValueError("No manifest items matched the requested track filter")
        raise ValueError("No downloadable items found in the search manifest")

    task_id = generate_task_id()
    status_manifest_path = status_manifest_dir / f"download_{task_id}.csv"
    failed_manifest_path = status_manifest_dir / f"failed_{task_id}.csv"

    username, password = credentials

    run_started = time.perf_counter()
    total_downloaded_bytes = 0
    summary = {
        "task_id": task_id,
        "status_manifest": str(status_manifest_path),
        "failed_manifest": "",
        "total": len(selected_items),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "missing": 0,
        "eof_success": 0,
        "eof_skipped": 0,
        "eof_failed": 0,
    }
    failed_rows: list[dict[str, str]] = []
    live_line_width = 0
    last_live_update_ts = 0.0
    orbit_dir = (Path.cwd() / "Orbit") if download_eof else None
    eof_entries: list[_EOFEntry] | None = None
    eof_index_error: str | None = None
    eof_seen_names: set[str] = set()

    if orbit_dir is not None:
        orbit_dir.mkdir(parents=True, exist_ok=True)

    def _print_live_line(text: str, *, force: bool = False) -> None:
        nonlocal live_line_width, last_live_update_ts
        now = time.perf_counter()
        if (not force) and (now - last_live_update_ts < _LIVE_REFRESH_INTERVAL_SEC):
            return
        last_live_update_ts = now

        term_width = shutil.get_terminal_size(fallback=(120, 20)).columns
        shown = text
        if term_width > 4 and len(shown) > term_width - 1:
            shown = shown[: term_width - 1]

        padded = shown
        if live_line_width > len(shown):
            padded = shown + (" " * (live_line_width - len(shown)))
        live_line_width = max(live_line_width, len(shown))
        print(padded, end="\r", flush=True, file=sys.stdout)

    processed = 0

    def _maybe_download_eof(item: SearchResultItem, eof_session: requests.Session) -> str:
        nonlocal eof_entries, eof_index_error

        parsed = _parse_scene_satellite_and_time(item)
        if parsed is None:
            summary["eof_failed"] += 1
            logger.warning("EOF skip: unable to parse satellite/acquisition time for %s", item.granule_id)
            return "EOF: parse_failed"

        satellite, scene_time = parsed
        if eof_entries is None and eof_index_error is None:
            try:
                eof_entries = _fetch_eof_entries(timeout_sec=timeout_sec, logger=logger)
            except Exception as exc:
                eof_index_error = str(exc)
                logger.warning("EOF index fetch failed: %s", exc)

        if eof_index_error is not None:
            summary["eof_failed"] += 1
            return "EOF: index_failed"

        assert eof_entries is not None
        eof_name = _match_eof_name(eof_entries, satellite, scene_time)
        if not eof_name:
            summary["eof_failed"] += 1
            logger.warning("EOF match not found for %s (%s %s)", item.granule_id, satellite, scene_time.isoformat())
            return "EOF: no_match"

        if eof_name in eof_seen_names:
            return f"EOF: reused {eof_name}"

        eof_seen_names.add(eof_name)
        assert orbit_dir is not None
        eof_target = orbit_dir / eof_name
        if eof_target.exists():
            summary["eof_skipped"] += 1
            return f"EOF: exists {eof_name}"

        result = _download_url_with_retries(
            session=eof_session,
            url=f"{_EOF_INDEX_URL}/{eof_name}",
            target_path=eof_target,
            timeout_sec=timeout_sec,
            logger=logger,
            label=eof_name,
            max_attempts=MAX_DOWNLOAD_ATTEMPTS,
            retry_auth_errors=True,
        )
        if result.status == "success":
            summary["eof_success"] += 1
            return f"EOF: downloaded {eof_name}"
        else:
            summary["eof_failed"] += 1
            return f"EOF: failed {eof_name}"

    with _EarthdataSession(username, password) as session:
        eof_session = session if download_eof else None
        total_items = len(selected_items)
        try:
            for item_no, item in enumerate(selected_items, start=1):
                started = time.perf_counter()
                target_path = _build_target_path(download_root, item)
                full_label = (item.granule_id or "").strip() or "unknown"

                if show_progress:
                    print(f"\nScene [{item_no}/{total_items}]: {full_label}", file=sys.stdout, flush=True)

                status = "pending"
                error = ""
                error_type = ""
                attempt = 0
                live_used = {"value": False}

                try:
                    if not item.download_url:
                        raise ValueError("Missing download_url in search manifest")

                    if target_path.exists():
                        status = "skipped"
                        attempt = 0
                        summary["skipped"] += 1
                        _cleanup_part_file(target_path)
                    else:
                        logger.debug("Downloading [%s] -> %s", full_label, target_path)

                        def _on_attempt(
                            _current_attempt: int,
                            *,
                            _live_used: dict[str, bool] = live_used,
                            _item_no: int = item_no,
                            _total_items: int = total_items,
                        ) -> None:
                            if not show_progress:
                                return
                            _live_used["value"] = True
                            _print_live_line(
                                _render_live_line(
                                    item_no=_item_no,
                                    total_items=_total_items,
                                    success=summary["success"],
                                    failed=summary["failed"],
                                    skipped=summary["skipped"],
                                    downloaded_bytes=0,
                                    total_bytes=None,
                                    speed_bps=0.0,
                                ),
                                force=True,
                            )

                        progress_callback = None
                        if show_progress:

                            def progress_callback(
                                downloaded: int,
                                total_bytes: int | None,
                                elapsed: float,
                                *,
                                _item_no: int = item_no,
                                _total_items: int = total_items,
                            ) -> None:
                                _print_live_line(
                                    _render_live_line(
                                        item_no=_item_no,
                                        total_items=_total_items,
                                        success=summary["success"],
                                        failed=summary["failed"],
                                        skipped=summary["skipped"],
                                        downloaded_bytes=downloaded,
                                        total_bytes=total_bytes,
                                        speed_bps=(0.0 if elapsed <= 0 else downloaded / elapsed),
                                    )
                                )

                        result = _download_with_retries(
                            session=session,
                            item=item,
                            target_path=target_path,
                            timeout_sec=timeout_sec,
                            logger=logger,
                            max_attempts=MAX_DOWNLOAD_ATTEMPTS,
                            on_attempt=_on_attempt,
                            on_progress=progress_callback,
                        )
                        status = result.status
                        attempt = result.attempt
                        error = result.error
                        error_type = result.error_type

                        if status == "success":
                            summary["success"] += 1
                            if target_path.exists():
                                total_downloaded_bytes += target_path.stat().st_size
                        else:
                            summary["failed"] += 1
                            failed_rows.append(
                                {
                                    "granule_id": item.granule_id,
                                    "download_url": item.download_url,
                                    "reason": error or error_type or "download failed",
                                }
                            )
                except Exception as exc:
                    status = "failed"
                    error = str(exc)
                    error_type, _ = _classify_download_exception(exc)
                    attempt = max(attempt, 1)
                    summary["failed"] += 1
                    failed_rows.append(
                        {
                            "granule_id": item.granule_id,
                            "download_url": item.download_url,
                            "reason": error or error_type or "download failed",
                        }
                    )
                    logger.exception("Download failed for %s", item.granule_id)

                elapsed = time.perf_counter() - started
                record = DownloadStatusRecord(
                    task_id=task_id,
                    granule_id=item.granule_id,
                    status=status,
                    local_path=str(target_path),
                    error=error,
                    elapsed_sec=elapsed,
                    timestamp=utc_now_iso(),
                    attempt=attempt,
                    error_type=error_type,
                )
                append_download_status(status_manifest_path, record)
                processed += 1

                eof_note = "EOF: off"
                if download_eof and eof_session is not None and status in {"success", "skipped"}:
                    eof_note = _maybe_download_eof(item, eof_session)
                elif download_eof and status == "failed":
                    eof_note = "EOF: skipped (SLC failed)"

                if show_progress and status in {"success", "skipped", "failed"}:
                    if live_used["value"]:
                        print(file=sys.stdout)
                    print(
                        f"[{processed}/{total_items}] "
                        f"SLC={status.upper()} | {eof_note} | "
                        f"ok={summary['success']} skip={summary['skipped']} fail={summary['failed']}",
                        file=sys.stdout,
                        flush=True,
                    )
        finally:
            if eof_session is not None and eof_session is not session:
                eof_session.close()

    if show_progress and selected_items:
        print(file=sys.stdout)

    if failed_rows:
        write_failed_manifest(failed_manifest_path, failed_rows)
        summary["failed_manifest"] = str(failed_manifest_path)

    summary["elapsed_sec"] = round(time.perf_counter() - run_started, 2)
    summary["total_bytes"] = total_downloaded_bytes

    return summary
