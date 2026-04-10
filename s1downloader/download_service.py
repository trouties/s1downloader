from __future__ import annotations

import logging
import re
import shutil
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
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


@dataclass
class _DownloadResult:
    item: SearchResultItem
    target_path: Path
    status: str
    attempt: int
    error: str
    error_type: str
    elapsed_sec: float
    file_size: int


class _SessionPool:
    """Thread-local pool of _EarthdataSession instances."""

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        self._local = threading.local()
        self._sessions: list[_EarthdataSession] = []
        self._lock = threading.Lock()

    def get(self) -> _EarthdataSession:
        session = getattr(self._local, "session", None)
        if session is None:
            session = _EarthdataSession(self._username, self._password)
            self._local.session = session
            with self._lock:
                self._sessions.append(session)
        return session

    def close_all(self) -> None:
        with self._lock:
            for s in self._sessions:
                s.close()
            self._sessions.clear()


class _ProgressAggregator:
    """Thread-safe aggregate progress display for parallel downloads."""

    def __init__(self, total_items: int, show_progress: bool):
        self._lock = threading.Lock()
        self._total_items = total_items
        self._show = show_progress
        self._completed = 0
        self._success = 0
        self._failed = 0
        self._skipped = 0
        self._active: dict[str, tuple[int, int | None]] = {}
        self._last_update = 0.0
        self._line_width = 0

    def on_item_progress(self, granule_id: str, downloaded: int, total_bytes: int | None) -> None:
        with self._lock:
            self._active[granule_id] = (downloaded, total_bytes)
            self._maybe_refresh()

    def on_item_complete(self, result: _DownloadResult) -> None:
        with self._lock:
            self._active.pop(result.item.granule_id, None)
            self._completed += 1
            if result.status == "success":
                self._success += 1
            elif result.status == "skipped":
                self._skipped += 1
            else:
                self._failed += 1

    def _maybe_refresh(self) -> None:
        now = time.perf_counter()
        if now - self._last_update < _LIVE_REFRESH_INTERVAL_SEC:
            return
        self._refresh()

    def _refresh(self) -> None:
        if not self._show:
            return
        self._last_update = time.perf_counter()
        active_count = len(self._active)
        total_active_bytes = sum(d for d, _ in self._active.values())
        line = (
            f"[{self._completed}/{self._total_items}] "
            f"active={active_count} "
            f"downloading={format_bytes(total_active_bytes)} "
            f"(ok={self._success}, skip={self._skipped}, fail={self._failed})"
        )
        term_width = shutil.get_terminal_size(fallback=(120, 20)).columns
        if term_width > 4 and len(line) > term_width - 1:
            line = line[: term_width - 1]
        padded = line
        if self._line_width > len(line):
            padded = line + " " * (self._line_width - len(line))
        self._line_width = max(self._line_width, len(line))
        print(padded, end="\r", flush=True, file=sys.stdout)

    def print_item_summary(self, result: _DownloadResult, eof_note: str) -> None:
        if not self._show:
            return
        with self._lock:
            print(
                f"\n[{self._completed}/{self._total_items}] "
                f"{result.item.granule_id[:60]} "
                f"SLC={result.status.upper()} | {eof_note} | "
                f"ok={self._success} skip={self._skipped} fail={self._failed}",
                file=sys.stdout,
                flush=True,
            )


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


def _download_one_item(
    *,
    session_pool: _SessionPool,
    item: SearchResultItem,
    download_root: Path,
    timeout_sec: int,
    logger: logging.Logger,
    progress: _ProgressAggregator | None = None,
    shutdown_event: threading.Event | None = None,
) -> _DownloadResult:
    """Download a single item in a worker thread. Does not mutate shared state."""
    started = time.perf_counter()
    target_path = _build_target_path(download_root, item)
    try:
        if shutdown_event is not None and shutdown_event.is_set():
            return _DownloadResult(
                item=item,
                target_path=target_path,
                status="cancelled",
                attempt=0,
                error="",
                error_type="",
                elapsed_sec=0.0,
                file_size=0,
            )

        if not item.download_url:
            raise ValueError("Missing download_url in search manifest")

        if target_path.exists():
            _cleanup_part_file(target_path)
            return _DownloadResult(
                item=item,
                target_path=target_path,
                status="skipped",
                attempt=0,
                error="",
                error_type="",
                elapsed_sec=time.perf_counter() - started,
                file_size=0,
            )

        session = session_pool.get()
        granule_id = item.granule_id
        logger.debug("Downloading [%s] -> %s", granule_id, target_path)

        on_progress = None
        if progress is not None:

            def on_progress(
                downloaded: int,
                total_bytes: int | None,
                _elapsed: float,
                *,
                _gid: str = granule_id,
                _prog: _ProgressAggregator = progress,
            ) -> None:
                _prog.on_item_progress(_gid, downloaded, total_bytes)

        result = _download_with_retries(
            session=session,
            item=item,
            target_path=target_path,
            timeout_sec=timeout_sec,
            logger=logger,
            max_attempts=MAX_DOWNLOAD_ATTEMPTS,
            on_progress=on_progress,
        )

        file_size = 0
        if result.status == "success" and target_path.exists():
            file_size = target_path.stat().st_size

        return _DownloadResult(
            item=item,
            target_path=target_path,
            status=result.status,
            attempt=result.attempt,
            error=result.error,
            error_type=result.error_type,
            elapsed_sec=time.perf_counter() - started,
            file_size=file_size,
        )
    except Exception as exc:
        error_text = str(exc)
        error_type, _ = _classify_download_exception(exc)
        logger.exception("Download failed for %s", item.granule_id)
        return _DownloadResult(
            item=item,
            target_path=target_path,
            status="failed",
            attempt=1,
            error=error_text,
            error_type=error_type,
            elapsed_sec=time.perf_counter() - started,
            file_size=0,
        )


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
    workers: int = 4,
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
    summary: dict[str, object] = {
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
    total_items = len(selected_items)
    effective_workers = min(max(workers, 1), total_items)

    # -- Phase 1: parallel SLC downloads --
    session_pool = _SessionPool(username, password)
    progress = _ProgressAggregator(total_items, show_progress)
    shutdown_event = threading.Event()
    completed_results: list[_DownloadResult] = []

    try:
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(
                    _download_one_item,
                    session_pool=session_pool,
                    item=item,
                    download_root=download_root,
                    timeout_sec=timeout_sec,
                    logger=logger,
                    progress=progress if show_progress else None,
                    shutdown_event=shutdown_event,
                ): item
                for item in selected_items
            }
            try:
                for future in as_completed(futures):
                    result = future.result()
                    completed_results.append(result)
                    progress.on_item_complete(result)

                    # Write CSV status record (main thread only)
                    record = DownloadStatusRecord(
                        task_id=task_id,
                        granule_id=result.item.granule_id,
                        status=result.status,
                        local_path=str(result.target_path),
                        error=result.error,
                        elapsed_sec=result.elapsed_sec,
                        timestamp=utc_now_iso(),
                        attempt=result.attempt,
                        error_type=result.error_type,
                    )
                    append_download_status(status_manifest_path, record)

                    # Update summary counters (main thread only)
                    if result.status == "success":
                        summary["success"] = int(summary["success"]) + 1
                        total_downloaded_bytes += result.file_size
                    elif result.status == "skipped":
                        summary["skipped"] = int(summary["skipped"]) + 1
                    elif result.status != "cancelled":
                        summary["failed"] = int(summary["failed"]) + 1
                        failed_rows.append(
                            {
                                "granule_id": result.item.granule_id,
                                "download_url": result.item.download_url,
                                "reason": result.error or result.error_type or "download failed",
                            }
                        )

                    eof_note = "EOF: deferred" if download_eof else "EOF: off"
                    progress.print_item_summary(result, eof_note)
            except KeyboardInterrupt:
                shutdown_event.set()
                logger.warning("Download interrupted by user, finishing active downloads...")
    finally:
        session_pool.close_all()

    # -- Phase 2: sequential EOF downloads --
    if download_eof:
        orbit_dir = Path.cwd() / "Orbit"
        orbit_dir.mkdir(parents=True, exist_ok=True)
        eof_entries: list[_EOFEntry] | None = None
        eof_index_error: str | None = None
        eof_seen_names: set[str] = set()

        with _EarthdataSession(username, password) as eof_session:
            for result in completed_results:
                if result.status not in {"success", "skipped"}:
                    continue

                parsed = _parse_scene_satellite_and_time(result.item)
                if parsed is None:
                    summary["eof_failed"] = int(summary["eof_failed"]) + 1
                    logger.warning("EOF skip: unable to parse satellite/acquisition time for %s", result.item.granule_id)
                    continue

                satellite, scene_time = parsed
                if eof_entries is None and eof_index_error is None:
                    try:
                        eof_entries = _fetch_eof_entries(timeout_sec=timeout_sec, logger=logger)
                    except Exception as exc:
                        eof_index_error = str(exc)
                        logger.warning("EOF index fetch failed: %s", exc)

                if eof_index_error is not None:
                    summary["eof_failed"] = int(summary["eof_failed"]) + 1
                    continue

                assert eof_entries is not None
                eof_name = _match_eof_name(eof_entries, satellite, scene_time)
                if not eof_name:
                    summary["eof_failed"] = int(summary["eof_failed"]) + 1
                    logger.warning(
                        "EOF match not found for %s (%s %s)", result.item.granule_id, satellite, scene_time.isoformat()
                    )
                    continue

                if eof_name in eof_seen_names:
                    continue

                eof_seen_names.add(eof_name)
                eof_target = orbit_dir / eof_name
                if eof_target.exists():
                    summary["eof_skipped"] = int(summary["eof_skipped"]) + 1
                    continue

                eof_result = _download_url_with_retries(
                    session=eof_session,
                    url=f"{_EOF_INDEX_URL}/{eof_name}",
                    target_path=eof_target,
                    timeout_sec=timeout_sec,
                    logger=logger,
                    label=eof_name,
                    max_attempts=MAX_DOWNLOAD_ATTEMPTS,
                    retry_auth_errors=True,
                )
                if eof_result.status == "success":
                    summary["eof_success"] = int(summary["eof_success"]) + 1
                else:
                    summary["eof_failed"] = int(summary["eof_failed"]) + 1

    if show_progress and selected_items:
        print(file=sys.stdout)

    if failed_rows:
        write_failed_manifest(failed_manifest_path, failed_rows)
        summary["failed_manifest"] = str(failed_manifest_path)

    summary["elapsed_sec"] = round(time.perf_counter() - run_started, 2)
    summary["total_bytes"] = total_downloaded_bytes

    return summary
