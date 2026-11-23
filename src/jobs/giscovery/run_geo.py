# Databricks notebook source
# MAGIC %md
# MAGIC # run_geo
# MAGIC
# MAGIC giscovery 的地理標準化層：把 etldestined 的地理資料統一轉成 **EPSG:4326 的 GeoParquet**，
# MAGIC 寫到 `/Volumes/<catalog>/<schema>/<output_volume>/<task_name>/<task_name>.parquet`。
# MAGIC 爬蟲 job 的 `geo` task 在 crawlerEtl 完成後以 Run Job task 觸發（只帶 `task_name`），也可以手動執行。
# MAGIC 讀檔、重投影與寫 GeoParquet 用 [RustGIS](https://github.com/opengeos/RustGIS)（需 serverless environment 3 以上）。
# MAGIC
# MAGIC 依 geo_config 的 `reader` 分三種，細節見 `giscovery_crawler/geo.py`：
# MAGIC - `vector`：Shapefile zip、KML 等，`crs` 可覆蓋 / 補上 `.prj`；KML 缺 `xmlns:xsi` 會先補上
# MAGIC - `json_points`：依群組分好的 JSON 點位（cathayATM）
# MAGIC - `geocode`：只有地址的 CSV / ODS，用 Google Geocoding 轉座標，結果存在快取表，每月有呼叫上限（見 `giscovery_crawler/geocode.py`）
# MAGIC
# MAGIC 參數都由 job 定義傳入（`resources/geo_normalize.job.yml`），notebook 不設預設值，下表的預設是 job 端的值。
# MAGIC
# MAGIC | 參數 | 說明 |
# MAGIC |---|---|
# MAGIC | `task_name` | 必填，對應 crawler_config 與 geo_config 的 `task_name` |
# MAGIC | `crawler_config_table` / `geo_config_table` | 設定表完整名稱，預設 `test.giscovery.crawler_config` / `test.giscovery.geo_config` |
# MAGIC | `catalog` / `schema` | 輸入與輸出 Volume 所在位置，預設 `test` / `giscovery` |
# MAGIC | `source_volume` / `output_volume` | 預設 `etldestined` / `geoready`（輸出 Volume 不存在會自動建立） |
# MAGIC | `geocode_cache_table` | 地址快取表，預設 `test.giscovery.geocode_cache`（不存在會自動建立） |
# MAGIC | `geocode_monthly_limit` | 每月（台北時間）Google Geocoding 呼叫上限，預設 `9000` |
# MAGIC | `geocode_max_qps` | Google Geocoding 每秒最多幾次（所有 worker 合計），預設 `20`；超過速率限制時會退避重試 |
# MAGIC | `secret_scope` / `secret_key` | Google API key 所在的 secret，預設 `gis-scope` / `googlemaps-api-key` |
# MAGIC | `repo_root` | repo 根目錄（bundle 部署時為 `${workspace.file_path}`），用來 import `common` |

# COMMAND ----------

import os
import sys

# Parameters come from the job definition (resources/*.yml); read them with dbutils.widgets.get only.

# A bundle deploys to a plain workspace folder, not a Git folder, so only the notebook directory is on
# sys.path. Add this directory (giscovery_crawler) and the repo root (common) explicitly.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/Workspace" + os.path.dirname(_notebook_path)
_repo_root = dbutils.widgets.get("repo_root").strip() or os.path.abspath(os.path.join(_notebook_dir, "../../.."))
for _p in (_notebook_dir, _repo_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# COMMAND ----------

import json
import shutil
import tempfile

from common.storage import copy_file, volume_path
from giscovery_crawler import geo, geocode
from giscovery_crawler.config import load_task_config

task_name = dbutils.widgets.get("task_name").strip()
if not task_name:
    raise ValueError("task_name 不可為空")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
output_volume = dbutils.widgets.get("output_volume").strip()
source_dir = volume_path(catalog, schema, dbutils.widgets.get("source_volume"), task_name)
output_dir = volume_path(catalog, schema, output_volume, task_name)

crawler_cfg = load_task_config(spark, dbutils.widgets.get("crawler_config_table"), task_name)
geo_cfg = geo.parse_geo_config(load_task_config(spark, dbutils.widgets.get("geo_config_table"), task_name))
file_format = crawler_cfg["file_format"]
input_path = os.path.join(source_dir, f"{task_name}.{file_format}")
print(f"task={task_name} reader={geo_cfg.reader} input={input_path}\noutput_dir={output_dir}\ngeo_config={geo_cfg}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{output_volume}`")
work_dir = tempfile.mkdtemp(prefix=f"geo_{task_name}_")
local_input = os.path.join(work_dir, os.path.basename(input_path))
shutil.copyfile(input_path, local_input)

# COMMAND ----------

def build_geocoded_features():
    """地址 → 快取 / Google Geocoding → 點位 features。查不到或延後到下個月的列不會出現在輸出。"""
    cache_table = dbutils.widgets.get("geocode_cache_table").strip()
    monthly_limit = int(dbutils.widgets.get("geocode_monthly_limit"))
    df = geo.read_address_table(local_input, file_format)
    if geo_cfg.address_column not in df.columns:
        raise KeyError(f"{task_name}: 找不到地址欄位 {geo_cfg.address_column!r}，現有欄位 {list(df.columns)}")
    df["_address"] = df[geo_cfg.address_column].map(geocode.normalize_address)
    addresses = set(df["_address"].dropna())

    geocode.ensure_cache_table(spark, cache_table)
    cached = geocode.load_cache(spark, cache_table, addresses)
    used = geocode.calls_since(spark, cache_table, geocode.month_start_utc())
    todo, deferred = geocode.plan_lookups(addresses, cached, monthly_limit - used)
    print(
        f"rows={len(df):,} addresses={len(addresses):,} cached={len(cached):,} "
        f"month_used={used:,}/{monthly_limit:,} to_query={len(todo):,} deferred={deferred:,}"
    )

    if todo:
        geocoder = geocode.GoogleGeocoder(
            dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key")),
            max_qps=float(dbutils.widgets.get("geocode_max_qps")),
        )
        results, error = geocode.geocode_many(geocoder.geocode, todo)
        print(f"rate-limited responses backed off and retried: {geocoder.rate_limited:,}")
        # Cache whatever was looked up (it is billed) before surfacing an error
        geocode.append_cache(spark, cache_table, results)
        print(f"geocoded {len(results):,} address(es): " + json.dumps(
            {s: sum(r.status == s for r in results) for s in sorted({r.status for r in results})}
        ))
        if error is not None:
            raise error
        cached.update({r.address: r for r in results})

    features, missing = [], 0
    for record in df.to_dict("records"):
        hit = cached.get(record.pop("_address"))
        if hit is None or hit.status != "OK" or not geo.valid_lon_lat(hit.lon, hit.lat):
            missing += 1
            continue
        props = {**record, "geocode_formatted_address": hit.formatted_address, "geocode_location_type": hit.location_type}
        features.append(geo.point_feature(hit.lon, hit.lat, props))
    print(f"located {len(features):,} row(s); {missing:,} row(s) without coordinates (not found or deferred)")
    return features

# COMMAND ----------

try:
    if geo_cfg.reader == "vector":
        if file_format == "kml":
            with open(local_input, encoding="utf-8") as f:
                text = f.read()
            with open(local_input, "w", encoding="utf-8") as f:
                f.write(geo.fix_kml_namespaces(text))
        exported = geo.export_vector_file(local_input, task_name, work_dir, crs=geo_cfg.crs)
    elif geo_cfg.reader == "json_points":
        with open(local_input, encoding=crawler_cfg.get("file_encoding") or "utf-8") as f:
            features, skipped = geo.json_points_features(json.load(f), geo_cfg)
        print(f"{len(features):,} point(s), {skipped:,} record(s) skipped for missing/invalid coordinates")
        exported = geo.export_features(features, task_name, work_dir)
    else:
        exported = geo.export_features(build_geocoded_features(), task_name, work_dir)

    for layer in exported:
        result = copy_file(layer.path, os.path.join(output_dir, layer.file_name))
        print(
            f"* {layer.layer_name} ({layer.geometry_type}, {layer.features:,} features, bounds={layer.bounds}) "
            f"-> {result.path} ({result.size:,} bytes)"
        )
    for stale in geo.stale_outputs(output_dir, [layer.file_name for layer in exported]):
        os.remove(stale)
        print(f"* removed stale {stale}")
finally:
    shutil.rmtree(work_dir, ignore_errors=True)

dbutils.notebook.exit(output_dir)
