from __future__ import annotations

import logging
import time
from typing import Any

import asf_search as asf
from shapely import wkt as shapely_wkt
from shapely.geometry import shape

from s1downloader.models import SearchRequest, SearchResultItem

DEFAULT_CMR_TIMEOUT_SEC = 120
DEFAULT_SEARCH_RETRY_ATTEMPTS = 3
DEFAULT_SEARCH_RETRY_WAIT_SEC = 2.0


def _pick(props: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = props.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_properties(product: Any) -> dict[str, Any]:
    props: dict[str, Any] = {}

    raw_props = getattr(product, "properties", None)
    if isinstance(raw_props, dict):
        props.update(raw_props)

    if hasattr(product, "geojson"):
        try:
            geojson = product.geojson()
            if isinstance(geojson, dict):
                gj_props = geojson.get("properties", {})
                if isinstance(gj_props, dict):
                    props.update(gj_props)
        except Exception:
            pass

    return props


def _to_mb(size_value: Any) -> float | None:
    if size_value in (None, ""):
        return None

    try:
        number = float(size_value)
    except (TypeError, ValueError):
        return None

    # If this looks like bytes, convert to MB.
    if number > 10_000_000:
        return round(number / (1024 * 1024), 2)
    return round(number, 2)


def _extract_footprint_wkt(product: Any) -> str | None:
    geometry: Any = None

    if hasattr(product, "geometry"):
        geometry = product.geometry

    if hasattr(product, "geojson"):
        try:
            geojson = product.geojson()
            if isinstance(geojson, dict) and isinstance(geojson.get("geometry"), dict):
                geometry = geojson["geometry"]
        except Exception:
            pass

    if geometry in (None, ""):
        return None

    try:
        if isinstance(geometry, str):
            return shapely_wkt.loads(geometry).wkt
        if isinstance(geometry, dict):
            return shape(geometry).wkt
        if hasattr(geometry, "__geo_interface__"):
            return shape(geometry.__geo_interface__).wkt
    except Exception:
        return None

    return None


def _map_product(product: Any, index: int) -> SearchResultItem:
    props = _extract_properties(product)

    granule_id = str(
        _pick(props, ["sceneName", "granuleName", "fileID", "beamModeType", "ummName"])
        or getattr(product, "fileID", "")
        or f"item_{index}"
    )
    acquisition_time = str(_pick(props, ["startTime", "startTimeUtc", "sceneDate", "processingDate"]) or "")

    rel_orbit_value = _pick(props, ["pathNumber", "relativeOrbit", "orbit"])
    rel_orbit = None if rel_orbit_value in (None, "") else str(rel_orbit_value)
    orbit_direction_value = _pick(props, ["flightDirection", "passDirection", "orbitDirection"])
    orbit_direction = None if orbit_direction_value in (None, "") else str(orbit_direction_value)

    polarization_value = _pick(props, ["polarization", "polarizationChannels"])
    polarization = None if polarization_value in (None, "") else str(polarization_value)

    size_mb = _to_mb(_pick(props, ["sizeMB", "bytes", "fileSize"]))

    download_url = str(_pick(props, ["url", "downloadUrl", "fileURL", "httpsUrl"]) or "")
    footprint_wkt = _extract_footprint_wkt(product)

    return SearchResultItem(
        index=index,
        granule_id=granule_id,
        acquisition_time=acquisition_time,
        relative_orbit=rel_orbit,
        orbit_direction=orbit_direction,
        polarization=polarization,
        size_mb=size_mb,
        download_url=download_url,
        footprint_wkt=footprint_wkt,
    )


def _set_cmr_timeout(timeout_sec: int, logger: logging.Logger) -> None:
    # ASF Search uses a global CMR timeout constant.
    try:
        if hasattr(asf, "constants") and hasattr(asf.constants, "INTERNAL"):
            asf.constants.INTERNAL.CMR_TIMEOUT = int(timeout_sec)
            logger.info("ASF CMR timeout set to %ss", int(timeout_sec))
    except Exception as exc:
        logger.warning("Failed to set ASF CMR timeout, fallback to library default: %s", exc)


def _is_timeout_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "timeout" in message or "timed out" in message


def search_sentinel1_slc(
    request: SearchRequest,
    logger: logging.Logger,
    *,
    cmr_timeout_sec: int = DEFAULT_CMR_TIMEOUT_SEC,
    retry_attempts: int = DEFAULT_SEARCH_RETRY_ATTEMPTS,
    retry_wait_sec: float = DEFAULT_SEARCH_RETRY_WAIT_SEC,
) -> list[SearchResultItem]:
    _set_cmr_timeout(cmr_timeout_sec, logger)

    params = {
        "platform": "Sentinel-1",
        "processingLevel": "SLC",
        "start": request.start_date,
        "end": request.end_date,
        "maxResults": request.max_results,
        "intersectsWith": request.intersects_with,
    }
    if request.relative_orbit is not None:
        params["relativeOrbit"] = int(request.relative_orbit)
    logger.info("Starting ASF search with params: %s", params)

    last_error: Exception | None = None
    for attempt in range(1, max(int(retry_attempts), 1) + 1):
        try:
            results = asf.search(**params)
            break
        except Exception as exc:
            last_error = exc
            if attempt >= int(retry_attempts) or not _is_timeout_error(exc):
                raise
            logger.warning(
                "ASF search timeout on attempt %d/%d, retrying in %.1fs: %s",
                attempt,
                int(retry_attempts),
                float(retry_wait_sec),
                exc,
            )
            time.sleep(float(retry_wait_sec))
    else:  # pragma: no cover
        raise RuntimeError(f"ASF search failed unexpectedly: {last_error}")

    items = [_map_product(product, idx) for idx, product in enumerate(results, start=1)]

    logger.info("ASF search completed with %d result(s)", len(items))
    return items
