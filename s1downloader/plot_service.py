from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from shapely import wkt as shapely_wkt
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from s1downloader.models import SearchResultItem


def _iter_polygons(geom):
    if isinstance(geom, Polygon):
        yield geom
    elif isinstance(geom, MultiPolygon):
        for part in geom.geoms:
            if isinstance(part, Polygon):
                yield part
    elif isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            yield from _iter_polygons(part)


def _bounds_with_padding(bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    min_x, min_y, max_x, max_y = bounds
    width = max(max_x - min_x, 0.001)
    height = max(max_y - min_y, 0.001)
    pad_x = width * 0.08
    pad_y = height * 0.08
    return (min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y)


def _draw_geometry_outline(ax, geom, *, color: str, linewidth: float, alpha: float, zorder: int) -> None:
    for poly in _iter_polygons(geom):
        x, y = poly.exterior.xy
        ax.plot(x, y, color=color, linewidth=linewidth, alpha=alpha, zorder=zorder)


def _draw_geometry_fill(ax, geom, *, color: str, alpha: float, zorder: int) -> None:
    for poly in _iter_polygons(geom):
        x, y = poly.exterior.xy
        ax.fill(x, y, facecolor=color, edgecolor="none", alpha=alpha, zorder=zorder)


def _relative_orbit_key(item: SearchResultItem) -> str:
    return (item.relative_orbit or "unknown").strip() or "unknown"


def _build_orbit_color_map(items: list[SearchResultItem]) -> dict[str, str]:
    orbit_keys = sorted({_relative_orbit_key(item) for item in items})
    cmap = plt.get_cmap("tab20")
    return {key: cmap(i % 20) for i, key in enumerate(orbit_keys)}


def _short_orbit_direction(text: str | None) -> str:
    if not text:
        return "UNK"
    upper = text.strip().upper()
    if upper.startswith("ASC"):
        return "ASC"
    if upper.startswith("DES"):
        return "DES"
    return upper[:3]


def _orbit_group_key(item: SearchResultItem) -> tuple[str, str]:
    return (_relative_orbit_key(item), _short_orbit_direction(item.orbit_direction))


def _resolve_label_position(
    base_x: float,
    base_y: float,
    existing: list[tuple[float, float]],
    *,
    x_step: float,
    y_step: float,
    max_tries: int = 12,
) -> tuple[float, float]:
    x = base_x
    y = base_y
    for _ in range(max_tries):
        has_collision = any(abs(x - ex) < x_step and abs(y - ey) < y_step for ex, ey in existing)
        if not has_collision:
            existing.append((x, y))
            return (x, y)
        y += y_step
    existing.append((x, y))
    return (x, y)


def _build_legend_handles(orbit_colors: dict[str, str]) -> list:
    handles: list = [
        Patch(facecolor="#f26b6b", edgecolor="#d64545", alpha=0.25, label="AOI"),
        Line2D([0], [0], color="#3f3f46", lw=1.0, label="Frame (colored by relative_orbit)"),
    ]

    sorted_items = sorted(orbit_colors.items(), key=lambda kv: (kv[0] == "unknown", kv[0]))
    max_orbit_legend = 12
    for orbit_key, color in sorted_items[:max_orbit_legend]:
        handles.append(Line2D([0], [0], color=color, lw=2.0, label=f"R{orbit_key}"))

    if len(sorted_items) > max_orbit_legend:
        handles.append(Line2D([0], [0], color="#666", lw=2.0, label=f"+{len(sorted_items) - max_orbit_legend} more"))
    return handles


def render_search_overview_png(
    *,
    aoi_wkt: str,
    items: list[SearchResultItem],
    output_path: Path,
    logger: logging.Logger,
) -> None:
    aoi_geom = shapely_wkt.loads(aoi_wkt)
    frame_geoms: list[tuple[object, SearchResultItem]] = []

    for item in items:
        if not item.footprint_wkt:
            continue
        try:
            frame_geoms.append((shapely_wkt.loads(item.footprint_wkt), item))
        except Exception:
            logger.warning("Invalid footprint_wkt for item idx=%s", item.index)

    fig, ax = plt.subplots(figsize=(10, 10))
    fig.patch.set_facecolor("#f7f8fb")
    ax.set_facecolor("#fbfcfe")

    min_x, min_y, max_x, max_y = aoi_geom.bounds
    for geom, _ in frame_geoms:
        gx1, gy1, gx2, gy2 = geom.bounds
        min_x, min_y = min(min_x, gx1), min(min_y, gy1)
        max_x, max_y = max(max_x, gx2), max(max_y, gy2)

    pmin_x, pmin_y, pmax_x, pmax_y = _bounds_with_padding((min_x, min_y, max_x, max_y))
    map_width = max(pmax_x - pmin_x, 0.001)
    map_height = max(pmax_y - pmin_y, 0.001)

    _draw_geometry_fill(ax, aoi_geom, color="#f26b6b", alpha=0.25, zorder=1)
    _draw_geometry_outline(ax, aoi_geom, color="#d64545", linewidth=2.0, alpha=0.95, zorder=5)

    orbit_colors = _build_orbit_color_map([item for _, item in frame_geoms]) if frame_geoms else {}
    label_positions: list[tuple[float, float]] = []
    label_dx = map_width * 0.04
    label_dy = map_height * 0.03

    grouped: dict[tuple[str, str], list[tuple[object, SearchResultItem]]] = defaultdict(list)
    for geom, item in frame_geoms:
        grouped[_orbit_group_key(item)].append((geom, item))

    for (orbit_key, orbit_dir), entries in grouped.items():
        color = orbit_colors.get(orbit_key, "#3f3f46")
        gmin_x, gmin_y = float("inf"), float("inf")
        gmax_x, gmax_y = float("-inf"), float("-inf")

        for geom, _item in entries:
            _draw_geometry_outline(ax, geom, color=color, linewidth=1.3, alpha=0.9, zorder=3)

            gx1, gy1, gx2, gy2 = geom.bounds
            gmin_x, gmin_y = min(gmin_x, gx1), min(gmin_y, gy1)
            gmax_x, gmax_y = max(gmax_x, gx2), max(gmax_y, gy2)

        label_x = (gmin_x + gmax_x) / 2.0
        label_y = gmax_y + map_height * 0.015
        resolved_x, resolved_y = _resolve_label_position(
            label_x,
            label_y,
            label_positions,
            x_step=label_dx,
            y_step=label_dy,
        )
        label_text = f"R{orbit_key} {orbit_dir} (n={len(entries)})"
        ax.text(
            resolved_x,
            resolved_y,
            label_text,
            fontsize=8,
            color="#1f2937",
            ha="center",
            va="bottom",
            bbox={"facecolor": "white", "alpha": 0.78, "edgecolor": "#cbd5e1", "pad": 1.5},
            zorder=6,
        )

    ax.set_xlim(pmin_x, pmax_x)
    ax.set_ylim(pmin_y, pmax_y)
    ax.grid(True, linestyle="--", linewidth=0.4, color="#cbd5e1", alpha=0.8)
    ax.set_axisbelow(True)
    ax.set_title("Sentinel-1 Search Overview (AOI + Frames)", fontsize=12, color="#111827")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    legend_handles = _build_legend_handles(orbit_colors)
    ax.legend(handles=legend_handles, loc="lower left", fontsize=8, framealpha=0.9)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
