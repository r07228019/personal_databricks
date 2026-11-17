# Databricks notebook source
# MAGIC %md
# MAGIC # run_etl
# MAGIC
# MAGIC giscovery 爬蟲的後處理，取代原本 AWS 的「S3 事件 → glue-trigger Lambda → Glue job / 直接複製」。
# MAGIC 每支爬蟲 job 爬完後以 Run Job task 觸發（只帶 `task_name`），也可以手動執行。
# MAGIC job 開啟並行（`max_concurrent_runs`），不同 task 可以同時跑。
# MAGIC
# MAGIC 依 crawler_config 的 `to_etl` 分兩種（與 Lambda 的判斷相同）：
# MAGIC - `to_etl=true`：讀 etl_config → 讀 crawlersink 的檔案 → 依 etl_config 轉換 →
# MAGIC   寫到 `/Volumes/<catalog>/<schema>/<output_volume>/<task_name>/<同檔名>`（同名直接覆蓋）。轉換規則見 `giscovery_crawler/etl.py`。
# MAGIC - 其他：把 crawlersink 的 `<task_name>/` 底下的檔案原封不動複製到 etldestined 的同一路徑，
# MAGIC   只複製目的端沒有或有變動的檔。見 `giscovery_crawler/passthrough.py`。
# MAGIC
# MAGIC | 參數 | 說明 |
# MAGIC |---|---|
# MAGIC | `task_name` | 必填，對應兩張設定表的 `task_name` |
# MAGIC | `input_path` | 輸入檔完整路徑；空白 = ETL 讀 `/Volumes/<catalog>/<schema>/<source_volume>/<task_name>/<task_name>.<file_format>`，複製則處理整個 `<task_name>/` 目錄 |
# MAGIC | `crawler_config_table` / `etl_config_table` | 設定表完整名稱，預設 `test.giscovery.crawler_config` / `test.giscovery.etl_config` |
# MAGIC | `catalog` / `schema` | 輸入與輸出 Volume 所在位置，預設 `test` / `giscovery` |
# MAGIC | `source_volume` / `output_volume` | 預設 `crawlersink` / `etldestined` |
# MAGIC | `repo_root` | repo 根目錄（bundle 部署時為 `${workspace.file_path}`），用來 import `common` |

# COMMAND ----------

import os
import sys

dbutils.widgets.text("task_name", "", "task_name (必填)")
dbutils.widgets.text("input_path", "", "input_path (空白=依設定推算)")
dbutils.widgets.text("crawler_config_table", "test.giscovery.crawler_config", "crawler_config_table")
dbutils.widgets.text("etl_config_table", "test.giscovery.etl_config", "etl_config_table")
dbutils.widgets.text("catalog", "test", "catalog")
dbutils.widgets.text("schema", "giscovery", "schema")
dbutils.widgets.text("source_volume", "crawlersink", "source_volume")
dbutils.widgets.text("output_volume", "etldestined", "output_volume")
dbutils.widgets.text("repo_root", "", "repo_root (空白=由 notebook 位置推算)")

# A bundle deploys to a plain workspace folder, not a Git folder, so only the notebook directory is on
# sys.path. Add this directory (giscovery_crawler) and the repo root (common) explicitly.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/Workspace" + os.path.dirname(_notebook_path)
_repo_root = dbutils.widgets.get("repo_root").strip() or os.path.abspath(os.path.join(_notebook_dir, "../../.."))
for _p in (_notebook_dir, _repo_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# COMMAND ----------

from common.storage import volume_path, write_bytes
from giscovery_crawler.config import load_task_config
from giscovery_crawler.etl import apply_etl, build_read_options, spark_readable_input, to_csv_bytes
from giscovery_crawler.passthrough import copy_task_files

task_name = dbutils.widgets.get("task_name").strip()
if not task_name:
    raise ValueError("task_name 不可為空")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
input_path = dbutils.widgets.get("input_path").strip()
source_dir = volume_path(catalog, schema, dbutils.widgets.get("source_volume"), task_name)
output_dir = volume_path(catalog, schema, dbutils.widgets.get("output_volume"), task_name)

crawler_cfg = load_task_config(spark, dbutils.widgets.get("crawler_config_table"), task_name)
# Same rule as the glue-trigger Lambda: only the string "true" (case-insensitive) runs ETL
run_etl = str(crawler_cfg.get("to_etl", "false")).lower() == "true"
print(f"task={task_name} to_etl={crawler_cfg.get('to_etl')!r} -> {'ETL' if run_etl else 'copy'}")

# COMMAND ----------

if not run_etl:
    # Copy the crawler output unchanged (Lambda copy_file); an explicit input_path copies just that file
    if input_path:
        copied = copy_task_files(os.path.dirname(input_path), output_dir, [os.path.basename(input_path)])
    else:
        copied = copy_task_files(source_dir, output_dir)
    print(f"* Done. {len(copied)} file(s) copied to {output_dir}")
    dbutils.notebook.exit(output_dir)

# COMMAND ----------

etl_cfg = load_task_config(spark, dbutils.widgets.get("etl_config_table"), task_name)
input_path = input_path or os.path.join(source_dir, f"{task_name}.{crawler_cfg['file_format']}")
output_path = os.path.join(output_dir, os.path.basename(input_path))
file_encoding = crawler_cfg.get("file_encoding")
print(f"input={input_path} (encoding={file_encoding})\noutput={output_path}\netl_config={etl_cfg}")

# Non-Spark charsets (e.g. big5) are transcoded to a temp UTF-8 file next to the output; it is removed on exit,
# so the lazy DataFrame must be fully materialized (to_csv_bytes) inside the with block.
with spark_readable_input(input_path, file_encoding, output_dir) as (read_path, spark_encoding):
    read_options = build_read_options(etl_cfg, spark_encoding)
    print(f"read_options={read_options}")
    df = spark.read.options(**read_options).csv(read_path)
    print(f"Read {read_path} done, columns={df.columns}")
    content = to_csv_bytes(apply_etl(df, etl_cfg))

result = write_bytes(output_path, content, overwrite=True)
print(f"* {'overwrote' if result.overwritten else 'saved'} {result.size:,} bytes -> {result.path}")
dbutils.notebook.exit(result.path)
