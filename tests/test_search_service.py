from s1downloader.search_service import _map_product


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
