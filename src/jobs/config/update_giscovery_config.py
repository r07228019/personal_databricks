# Databricks notebook source
# MAGIC %md
# MAGIC # updateGiscoveryConfig
# MAGIC
# MAGIC 把 `data/` 底下的 jsonl 設定檔同步到 Delta table，取代原本 AWS 的 `update_dynamodb_table.py`
# MAGIC （建表 → 清空 → 逐行 put）。這裡用 overwrite 一次替換整張表，表不存在會自動建立。
# MAGIC
# MAGIC | jsonl | Delta table |
# MAGIC |---|---|
# MAGIC | `data/cfh-gis20-ddb-crawler-config.jsonl` | `<catalog>.<schema>.crawler_config` |
# MAGIC | `data/cfh-gis20-ddb-etl-config.jsonl` | `<catalog>.<schema>.etl_config` |
# MAGIC
# MAGIC 表結構：`task_name STRING`、`config_json STRING`（整行 JSON）、`updated_at TIMESTAMP`。
# MAGIC 不把 JSON 展開成欄位，因為 etl_config 的 nested key 含 `(`、`/` 等 Delta 不允許的字元。
# MAGIC
# MAGIC 參數都由 job 定義傳入（`resources/update_giscovery_config.job.yml`），notebook 不設預設值，下表的預設是 job 端的值。
# MAGIC
# MAGIC | 參數 | 說明 |
# MAGIC |---|---|
# MAGIC | `catalog` / `schema` | 目的地，預設 `test` / `giscovery` |
# MAGIC | `data_dir` | jsonl 所在目錄，空白 = 本 notebook 同目錄的 `data/` |

# COMMAND ----------

import os
import sys

# Parameters come from the job definition (resources/*.yml); read them with dbutils.widgets.get only.

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/Workspace" + os.path.dirname(_notebook_path)
if _notebook_dir not in sys.path:
    sys.path.insert(0, _notebook_dir)

# COMMAND ----------

from config_sync import TABLE_FILES, load_jsonl_rows, write_config_table

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
data_dir = dbutils.widgets.get("data_dir").strip() or os.path.join(_notebook_dir, "data")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

for table_name, file_name in TABLE_FILES.items():
    rows = load_jsonl_rows(os.path.join(data_dir, file_name))
    full_name = f"{catalog}.{schema}.{table_name}"
    write_config_table(spark, full_name, rows)
    print(f"* {full_name}: {len(rows)} row(s) written from {file_name}")
