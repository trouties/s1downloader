import logging

from s1downloader.models import SearchResultItem
from s1downloader.plot_service import render_search_overview_png


def test_render_search_overview_png_with_frame_labels(tmp_path):
    out = tmp_path / "search.png"
    items = [
        SearchResultItem(
            index=1,
            granule_id="S1_TEST_001",
            acquisition_time="2024-01-01T00:00:00Z",
            relative_orbit="42",
            orbit_direction="ASCENDING",
            polarization="VV",
            size_mb=None,
            download_url="https://example.org/S1_TEST_001.zip",
            footprint_wkt="POLYGON((120 30,121 30,121 31,120 31,120 30))",
        ),
        SearchResultItem(
            index=2,
            granule_id="S1_TEST_002",
            acquisition_time="2024-01-02T00:00:00Z",
            relative_orbit="43",
            orbit_direction="DESCENDING",
            polarization="VH",
            size_mb=None,
            download_url="https://example.org/S1_TEST_002.zip",
            footprint_wkt="POLYGON((120.4 30.2,121.3 30.2,121.3 31.1,120.4 31.1,120.4 30.2))",
        ),
    ]

    render_search_overview_png(
        aoi_wkt="POLYGON((120.2 30.2,120.8 30.2,120.8 30.8,120.2 30.8,120.2 30.2))",
        items=items,
        output_path=out,
        logger=logging.getLogger("test"),
    )

    assert out.exists()
    assert out.stat().st_size > 0
