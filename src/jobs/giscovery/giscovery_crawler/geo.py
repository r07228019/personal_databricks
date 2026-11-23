"""地理標準化：把 etldestined 的各種地理資料統一轉成 EPSG:4326 的 GeoParquet（由 run_geo notebook 呼叫）。

geo_config 每筆設定的 reader 決定怎麼讀：

    vector       Shapefile zip、KML 等 RustGIS 能直接讀的檔案；crs 可覆蓋 .prj（沒有 .prj 時必填）
    json_points  依群組分好的 JSON 點位（{group: [record, ...]}），x / y 指定經緯度欄位
    geocode      只有地址的表格（CSV / ODS），地址經 Google Geocoding 轉成點位（見 geocode.py）

RustGIS 只在 Databricks 上的 job 環境（serverless environment 3 以上，Python 3.12）才需要；
本模組的純函式不 import rustgis，方便在 CI 測試。
"""

import json
import math
import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

READERS = ("vector", "json_points", "geocode")
OUTPUT_CRS = "EPSG:4326"

_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"


@dataclass(frozen=True)
class GeoTaskConfig:
    task_name: str
    reader: str
    crs: str | None = None
    x: str | None = None
    y: str | None = None
    group_field: str | None = None
    flatten: tuple[str, ...] = ()
    address_column: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def parse_geo_config(cfg: dict[str, Any]) -> GeoTaskConfig:
    """驗證並轉換 geo_config 的一筆設定。"""
    reader = cfg.get("reader")
    if reader not in READERS:
        raise ValueError(f"{cfg.get('task_name')}: reader 必須是 {READERS} 之一，收到 {reader!r}")
    if reader == "json_points" and not (cfg.get("x") and cfg.get("y")):
        raise ValueError(f"{cfg['task_name']}: json_points 需要 x 與 y")
    if reader == "geocode" and not cfg.get("address_column"):
        raise ValueError(f"{cfg['task_name']}: geocode 需要 address_column")
    known = {"task_name", "reader", "crs", "x", "y", "group_field", "flatten", "address_column"}
    return GeoTaskConfig(
        task_name=cfg["task_name"],
        reader=reader,
        crs=cfg.get("crs"),
        x=cfg.get("x"),
        y=cfg.get("y"),
        group_field=cfg.get("group_field"),
        flatten=tuple(cfg.get("flatten") or ()),
        address_column=cfg.get("address_column"),
        extra={k: v for k, v in cfg.items() if k not in known},
    )


def fix_kml_namespaces(text: str) -> str:
    """補上 KML 用到但沒宣告的 xsi namespace。

    水利署的颱風淹水 KML 在 <Document> 用了 xsi:schemaLocation 卻沒宣告 xmlns:xsi，
    RustGIS 0.8 的 XML parser 會直接拒讀（unknown namespace prefix 'xsi'）。
    """
    if "xsi:" not in text or "xmlns:xsi" in text:
        return text
    return re.sub(r"<kml\b", f'<kml xmlns:xsi="{_XSI_NS}"', text, count=1)


def parse_coordinate(value: Any) -> float | None:
    """把字串或數字座標轉成 float；空值、非數字、非有限值、0 都視為沒有座標。"""
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except ValueError:
        return None
    if not math.isfinite(number) or number == 0:
        return None
    return number


def valid_lon_lat(lon: float | None, lat: float | None) -> bool:
    return lon is not None and lat is not None and -180 <= lon <= 180 and -90 <= lat <= 90


def point_feature(lon: float, lat: float, properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": properties}


def feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _flatten_record(record: dict[str, Any], flatten: Iterable[str]) -> dict[str, Any]:
    flatten = set(flatten)
    props: dict[str, Any] = {}
    for key, value in record.items():
        if key in flatten and isinstance(value, dict):
            props.update({f"{key}_{k}": v for k, v in value.items()})
        elif isinstance(value, (dict, list)):
            props[key] = json.dumps(value, ensure_ascii=False)
        else:
            props[key] = value
    return props


def json_points_features(data: Any, cfg: GeoTaskConfig) -> tuple[list[dict[str, Any]], int]:
    """把 {group: [record, ...]}（或直接是 [record, ...]）轉成點位 feature，回傳 (features, 略過筆數)。

    group 的 key 寫進 group_field 欄位；flatten 列出的 dict 欄位會展開成 <欄位>_<子欄位>。
    """
    if isinstance(data, dict):
        groups = data.items()
    elif isinstance(data, list):
        groups = [(None, data)]
    else:
        raise ValueError(f"{cfg.task_name}: JSON 最外層必須是 object 或 array，收到 {type(data).__name__}")

    features, skipped = [], 0
    for group, records in groups:
        for record in records:
            lon, lat = parse_coordinate(record.get(cfg.x)), parse_coordinate(record.get(cfg.y))
            if not valid_lon_lat(lon, lat):
                skipped += 1
                continue
            props = _flatten_record(record, cfg.flatten)
            if cfg.group_field and group is not None:
                props = {cfg.group_field: group, **props}
            features.append(point_feature(lon, lat, props))
    return features, skipped


def layer_output_name(task_name: str, layer_name: str, layer_count: int) -> str:
    """單一圖層 → <task_name>.parquet；多圖層（例如 KML 同時有面與點）→ <task_name>_<圖層>.parquet。

    RustGIS 會把多圖層命名成「<檔名> polygons」，這裡去掉檔名前綴，只留圖層部分。
    """
    if layer_count <= 1:
        return f"{task_name}.parquet"
    suffix = layer_name[len(task_name):] if layer_name.startswith(task_name) else layer_name
    suffix = re.sub(r"[^0-9A-Za-z]+", "_", suffix).strip("_") or "layer"
    return f"{task_name}_{suffix}.parquet"


def read_address_table(path: str, file_format: str):
    """讀只有地址的表格成 pandas DataFrame（全部欄位當字串，空值為 None）。"""
    import pandas as pd

    if file_format == "csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    elif file_format in ("ods", "xlsx", "xls"):
        df = pd.read_excel(path, dtype=str, keep_default_na=False, engine="odf" if file_format == "ods" else None)
    else:
        raise ValueError(f"geocode 不支援 file_format={file_format!r}")
    return df.replace({"": None})


def stale_outputs(output_dir: str, keep: Iterable[str]) -> list[str]:
    """task 輸出目錄裡這次沒產出的舊 parquet（例如圖層改名後留下的檔）。"""
    if not os.path.isdir(output_dir):
        return []
    keep = set(keep)
    return sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.endswith(".parquet") and name not in keep
    )


# ---------------------------------------------------------------------------
# RustGIS：讀檔、轉 EPSG:4326、寫 GeoParquet
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExportedLayer:
    file_name: str
    path: str
    layer_name: str
    geometry_type: str
    features: int
    bounds: tuple[float, float, float, float] | None


def _session():
    import rustgis

    return rustgis.Session()


def _print_notices(session) -> None:
    for message, is_error in session.notices():
        print(f"  [rustgis {'error' if is_error else 'notice'}] {message}")


def _export(session, layers: list, task_name: str, out_dir: str) -> list[ExportedLayer]:
    exported = []
    for layer in layers:
        file_name = layer_output_name(task_name, layer.name, len(layers))
        path = os.path.join(out_dir, file_name)
        # GeoParquet without a "crs" key means OGC:CRS84 (lon/lat), i.e. EPSG:4326 axis order lon, lat
        result = layer.to_file(path, crs=OUTPUT_CRS)
        exported.append(
            ExportedLayer(file_name, path, layer.name, layer.type, int(result.get("features", layer.count)), layer.bounds)
        )
    _print_notices(session)
    return exported


def export_vector_file(path: str, task_name: str, out_dir: str, *, crs: str | None = None) -> list[ExportedLayer]:
    """用 RustGIS 讀 Shapefile zip / KML 等檔案，每個圖層各寫一個 EPSG:4326 GeoParquet。

    path 的檔名主檔名應為 task_name（多圖層時才能正確去掉前綴）。crs 會覆蓋 Shapefile 的 .prj。
    """
    session = _session()
    params = {"crs": crs} if crs else {}
    # No name=: RustGIS names layers after the file ("<stem> polygons" ...), which layer_output_name relies on
    layers = session.add_data(path, **params)
    _print_notices(session)
    layers = layers if isinstance(layers, list) else [layers]
    if not layers or not hasattr(layers[0], "to_file"):
        raise ValueError(f"{path} 沒有可輸出的地理圖層（RustGIS 回傳 {layers!r}）")
    return _export(session, layers, task_name, out_dir)


def export_features(features: list[dict[str, Any]], task_name: str, out_dir: str) -> list[ExportedLayer]:
    """把 EPSG:4326 的 GeoJSON features 寫成 <task_name>.parquet。"""
    if not features:
        raise ValueError(f"{task_name}: 沒有任何有座標的資料")
    session = _session()
    layer = session.add_geojson(feature_collection(features), name=task_name)
    _print_notices(session)
    return _export(session, [layer], task_name, out_dir)
