import logging

import pytest

from s1downloader.models import SearchRequest
from s1downloader.search_service import NetworkError, _map_product, search_sentinel1_slc


class _FakeProduct:
    def __init__(self, *, properties=None, geometry=None):
        self.properties = properties or {}
        self.geometry = geometry

    def geojson(self):
        return {
            "type": "Feature",
            "properties": self.properties,
            "geometry": self.geometry,
        }


def test_map_product_extracts_footprint_wkt():
    product = _FakeProduct(
        properties={
            "sceneName": "S1_TEST_001",
            "startTime": "2024-01-01T00:00:00Z",
            "flightDirection": "ASCENDING",
            "url": "https://example.org/S1_TEST_001.zip",
        },
        geometry={
            "type": "Polygon",
            "coordinates": [[[120.0, 30.0], [121.0, 30.0], [121.0, 31.0], [120.0, 31.0], [120.0, 30.0]]],
        },
    )

    item = _map_product(product, index=1)
    assert item.granule_id == "S1_TEST_001"
    assert item.orbit_direction == "ASCENDING"
    assert item.footprint_wkt is not None
    assert item.footprint_wkt.startswith("POLYGON")


def test_map_product_with_no_geometry_has_empty_footprint():
    product = _FakeProduct(
        properties={
            "sceneName": "S1_TEST_002",
            "startTime": "2024-01-02T00:00:00Z",
            "url": "https://example.org/S1_TEST_002.zip",
        },
        geometry=None,
    )

    item = _map_product(product, index=1)
    assert item.granule_id == "S1_TEST_002"
    assert item.footprint_wkt is None


def test_search_raises_network_error_on_failure(monkeypatch):
    def _fake_search(**kwargs):
        raise RuntimeError("Connection refused")

    monkeypatch.setattr("s1downloader.search_service.asf.search", _fake_search)
    logger = logging.getLogger("test.search.network_error")
    request = SearchRequest(
        start_date="2024-01-01",
        end_date="2024-01-31",
        intersects_with="POLYGON((120 30,121 30,121 31,120 31,120 30))",
        max_results=10,
    )
    with pytest.raises(NetworkError, match="ASF search failed"):
        search_sentinel1_slc(request, logger, retry_attempts=1)
