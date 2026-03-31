from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path

import shapefile
from shapely import wkt as shapely_wkt
from shapely.geometry import Polygon, box, shape
from shapely.ops import unary_union


class AOIError(ValueError):
    pass


def _validate_geometry(geom):
    if geom.is_empty:
        raise AOIError("AOI geometry is empty")
    if not geom.is_valid:
        geom = geom.buffer(0)
    if geom.is_empty or not geom.is_valid:
        raise AOIError("AOI geometry is invalid")
    return geom


def parse_bbox_to_wkt(bbox_text: str) -> str:
    parts = [p.strip() for p in bbox_text.split(",")]
    if len(parts) != 4:
        raise AOIError("bbox must have four comma-separated numbers: minLon,minLat,maxLon,maxLat")

    try:
        min_lon, min_lat, max_lon, max_lat = [float(x) for x in parts]
    except ValueError as exc:
        raise AOIError("bbox values must be numeric") from exc

    if min_lon >= max_lon or min_lat >= max_lat:
        raise AOIError("bbox bounds are invalid (min must be less than max)")

    geom = _validate_geometry(box(min_lon, min_lat, max_lon, max_lat))
    return geom.wkt


def parse_wkt_to_wkt(wkt_text: str) -> str:
    try:
        geom = shapely_wkt.loads(wkt_text)
    except Exception as exc:  # pragma: no cover - shapely raises different subclasses
        raise AOIError(f"Invalid WKT: {exc}") from exc
    geom = _validate_geometry(geom)
    return geom.wkt


def _parse_shp(path: Path) -> str:
    reader = shapefile.Reader(str(path))
    shapes = reader.shapes()
    if not shapes:
        raise AOIError("SHP file contains no shapes")

    geoms = [shape(s.__geo_interface__) for s in shapes]
    merged = _validate_geometry(unary_union(geoms))
    return merged.wkt


def _parse_kml(path: Path) -> str:
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError as exc:
        raise AOIError(f"Invalid KML XML: {exc}") from exc

    coords: list[tuple[float, float]] = []
    for elem in root.iter():
        if elem.tag.lower().endswith("coordinates") and elem.text:
            raw = elem.text.strip().replace("\n", " ")
            for token in raw.split():
                parts = token.split(",")
                if len(parts) < 2:
                    continue
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                except ValueError:
                    continue
                coords.append((lon, lat))

    if len(coords) < 3:
        raise AOIError("KML file does not include valid polygon coordinates")

    if coords[0] != coords[-1]:
        coords.append(coords[0])

    geom = _validate_geometry(Polygon(coords))
    return geom.wkt


def parse_aoi_file_to_wkt(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        raise AOIError(f"AOI file does not exist: {path}")

    suffix = path.suffix.lower()
    if suffix == ".shp":
        return _parse_shp(path)
    if suffix == ".kml":
        return _parse_kml(path)

    raise AOIError("AOI file must use .shp or .kml extension")


def normalize_aoi_to_wkt(
    *,
    wkt_text: str | None,
    bbox_text: str | None,
    aoi_file: str | None,
    allow_prompt_fallback: bool = False,
    input_fn: Callable[[str], str] = input,
) -> str:
    if wkt_text:
        return parse_wkt_to_wkt(wkt_text)

    if bbox_text:
        return parse_bbox_to_wkt(bbox_text)

    if aoi_file:
        try:
            return parse_aoi_file_to_wkt(aoi_file)
        except Exception as exc:
            if not allow_prompt_fallback:
                raise AOIError(str(exc)) from exc
            prompt = "Failed to parse AOI file. Enter fallback bbox (minLon,minLat,maxLon,maxLat): "
            fallback_bbox = input_fn(prompt).strip()
            return parse_bbox_to_wkt(fallback_bbox)

    raise AOIError("One AOI input is required: --wkt, --bbox, or --aoi-file")
