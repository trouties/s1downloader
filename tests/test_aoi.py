import pytest

from s1downloader.aoi import AOIError, normalize_aoi_to_wkt, parse_bbox_to_wkt


def test_parse_bbox_to_wkt_success():
    wkt = parse_bbox_to_wkt("120.0,30.0,121.0,31.0")
    assert wkt.startswith("POLYGON")


def test_parse_bbox_to_wkt_invalid_order():
    with pytest.raises(AOIError):
        parse_bbox_to_wkt("121.0,31.0,120.0,30.0")


def test_aoi_file_fallback_to_bbox(tmp_path):
    broken_kml = tmp_path / "broken.kml"
    broken_kml.write_text("<kml></kml>", encoding="utf-8")

    wkt = normalize_aoi_to_wkt(
        wkt_text=None,
        bbox_text=None,
        aoi_file=str(broken_kml),
        allow_prompt_fallback=True,
        input_fn=lambda _: "120.0,30.0,121.0,31.0",
    )

    assert wkt.startswith("POLYGON")
