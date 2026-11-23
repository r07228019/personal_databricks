import json
import struct
import zipfile
from pathlib import Path

import pytest

from giscovery_crawler.geo import (
    fix_kml_namespaces,
    json_points_features,
    layer_output_name,
    parse_coordinate,
    parse_geo_config,
    read_address_table,
    stale_outputs,
)

GEO_JSONL = Path(__file__).resolve().parents[3] / "src" / "jobs" / "config" / "data" / "cfh-gis20-geo-config.jsonl"


def test_repo_geo_config_is_valid():
    tasks = [parse_geo_config(json.loads(line)) for line in GEO_JSONL.read_text(encoding="utf-8").splitlines() if line]
    assert {t.task_name for t in tasks} >= {"cathayATM", "geoSensitiveArea", "historicalTyphoonFloodArea"}
    assert next(t for t in tasks if t.task_name == "geoSensitiveArea").crs == "EPSG:3857"


@pytest.mark.parametrize(
    "cfg",
    [
        {"task_name": "a", "reader": "raster"},
        {"task_name": "a", "reader": "json_points", "x": "lon"},
        {"task_name": "a", "reader": "geocode"},
    ],
)
def test_parse_geo_config_rejects_invalid(cfg):
    with pytest.raises(ValueError):
        parse_geo_config(cfg)


def test_fix_kml_namespaces_adds_missing_xsi_once():
    kml = '<?xml version="1.0"?>\n<kml xmlns="http://www.opengis.net/kml/2.2"><Document xsi:schemaLocation="x"/></kml>'
    fixed = fix_kml_namespaces(kml)
    assert fixed.count("xmlns:xsi=") == 1 and fixed.startswith('<?xml version="1.0"?>\n<kml xmlns:xsi=')
    assert fix_kml_namespaces(fixed) == fixed
    assert fix_kml_namespaces("<kml><Document/></kml>") == "<kml><Document/></kml>"


@pytest.mark.parametrize("raw, expected", [("24.35181200", 24.351812), (121.5, 121.5), ("", None), (None, None), ("0", None), ("abc", None), ("nan", None)])
def test_parse_coordinate(raw, expected):
    assert parse_coordinate(raw) == expected


def test_json_points_features_groups_flattens_and_skips_invalid():
    cfg = parse_geo_config({"task_name": "atm", "reader": "json_points", "x": "Longitude", "y": "Latitude", "group_field": "county", "flatten": ["Type"]})
    data = {
        "台中市": [
            {"Sno": "0", "Latitude": "24.35", "Longitude": "120.61", "Type": {"H24": "1"}, "Tags": ["a"]},
            {"Sno": "1", "Latitude": "", "Longitude": "120.6"},
        ],
        "台北市": [{"Sno": "2", "Latitude": "125", "Longitude": "25"}],  # lat out of range
    }
    features, skipped = json_points_features(data, cfg)
    assert skipped == 2
    assert features == [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [120.61, 24.35]},
            "properties": {"county": "台中市", "Sno": "0", "Latitude": "24.35", "Longitude": "120.61", "Type_H24": "1", "Tags": '["a"]'},
        }
    ]


@pytest.mark.parametrize(
    "layer, count, expected",
    [
        ("historicalTyphoonFloodArea polygons", 2, "historicalTyphoonFloodArea_polygons.parquet"),
        ("anything", 1, "debrisStream.parquet"),
        ("other name", 3, "debrisStream_other_name.parquet"),
        ("debrisStream", 2, "debrisStream_layer.parquet"),
    ],
)
def test_layer_output_name(layer, count, expected):
    task = expected.split("_")[0].removesuffix(".parquet")
    assert layer_output_name(task, layer, count) == expected


def test_read_address_table_csv_keeps_strings(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("Name,Address,Capacity\nA,新北市石門區,2640\nB,,\n", encoding="utf-8")
    df = read_address_table(str(p), "csv")
    assert df.to_dict("records") == [
        {"Name": "A", "Address": "新北市石門區", "Capacity": "2640"},
        {"Name": "B", "Address": None, "Capacity": None},
    ]
    with pytest.raises(ValueError):
        read_address_table(str(p), "json")


def test_stale_outputs(tmp_path):
    for name in ("t.parquet", "t_points.parquet", "notes.txt"):
        (tmp_path / name).write_text("x")
    assert stale_outputs(str(tmp_path), ["t.parquet"]) == [str(tmp_path / "t_points.parquet")]
    assert stale_outputs(str(tmp_path / "missing"), []) == []


# ---------------------------------------------------------------------------
# RustGIS-backed tests: run when rustgis (Python >= 3.12) is installed.
# ---------------------------------------------------------------------------


def _write_point_shapefile_zip(path: Path, x: float, y: float) -> None:
    """Minimal one-point Shapefile (no .prj) with a single NAME field."""
    shp_header = lambda length: struct.pack(">7i", 9994, 0, 0, 0, 0, 0, length) + struct.pack("<2i4d4d", 1000, 1, x, y, x, y, 0, 0, 0, 0)
    record = struct.pack(">2i", 1, 10) + struct.pack("<i2d", 1, x, y)
    shp = shp_header(50 + 14) + record
    shx = shp_header(50 + 4) + struct.pack(">2i", 50, 10)
    dbf = (
        struct.pack("<4BIHH20x", 3, 125, 1, 1, 1, 32 + 32 + 1, 1 + 10)
        + b"NAME".ljust(11, b"\0") + b"C" + b"\0" * 4 + bytes([10, 0]) + b"\0" * 14
        + b"\r" + b" " + b"A".ljust(10) + b"\x1a"
    )
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("p.shp", shp)
        z.writestr("p.shx", shx)
        z.writestr("p.dbf", dbf)


def test_export_vector_file_reprojects_with_crs_override(tmp_path):
    pytest.importorskip("rustgis")
    pq = pytest.importorskip("pyarrow.parquet")
    from giscovery_crawler.geo import export_vector_file

    src = tmp_path / "geoSensitiveArea.zip"
    _write_point_shapefile_zip(src, 13391596.57, 2638000.79)  # Web Mercator, Tainan
    (layer,) = export_vector_file(str(src), "geoSensitiveArea", str(tmp_path), crs="EPSG:3857")
    assert layer.file_name == "geoSensitiveArea.parquet" and layer.features == 1
    west, south, _, _ = layer.bounds
    assert west == pytest.approx(120.2987, abs=1e-3) and south == pytest.approx(23.0494, abs=1e-3)
    table = pq.read_table(layer.path)
    assert table.column("NAME").to_pylist() == ["A"]
    assert json.loads(table.schema.metadata[b"geo"])["primary_column"] == "geometry"


def test_export_features_writes_geoparquet(tmp_path):
    pytest.importorskip("rustgis")
    pq = pytest.importorskip("pyarrow.parquet")
    from giscovery_crawler.geo import export_features, point_feature

    (layer,) = export_features([point_feature(121.5, 25.0, {"name": "x"})], "cathayATM", str(tmp_path))
    assert layer.file_name == "cathayATM.parquet"
    assert pq.read_table(layer.path).column("name").to_pylist() == ["x"]
    with pytest.raises(ValueError):
        export_features([], "cathayATM", str(tmp_path))
