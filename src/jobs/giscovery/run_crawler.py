# Databricks notebook source
# MAGIC %md
# MAGIC # run_crawler
# MAGIC
# MAGIC giscovery 13 支爬蟲共用的進入點。每個爬蟲 job 都跑這個 notebook，只差 `task_name` 參數。
# MAGIC
# MAGIC 流程：從 Delta table 讀該 task 的設定 → 載入 `crawlers/<task_name>.py` → 呼叫 `main(sink, params)` →
# MAGIC 檔案寫進 `/Volumes/<catalog>/<schema>/<volume>/<task_name>/`。
# MAGIC
# MAGIC 參數都由 job 定義傳入（`gen_crawler_jobs.py` 產生），notebook 不設預設值，下表的預設是 job 端的值。
# MAGIC
# MAGIC | 參數 | 說明 |
# MAGIC |---|---|
# MAGIC | `task_name` | 必填，對應 `crawlers/<task_name>.py` 與設定表的 `task_name` |
# MAGIC | `config_table` | 設定表完整名稱，預設 `test.giscovery.crawler_config` |
# MAGIC | `sink_catalog` / `sink_schema` / `sink_volume` | 輸出 Volume，預設 `test` / `giscovery` / `crawlersink` |
# MAGIC | `repo_root` | repo 根目錄（bundle 部署時為 `${workspace.file_path}`），用來 import `common` |

# COMMAND ----------

import os
import sys

# Parameters come from the job definition (resources/*.yml); read them with dbutils.widgets.get only.

# 讓 notebook 可以 import repo 根目錄的 common/ 與本目錄的 giscovery_crawler/
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/Workspace" + os.path.dirname(_notebook_path)
_repo_root = dbutils.widgets.get("repo_root").strip() or os.path.abspath(os.path.join(_notebook_dir, "../../.."))
for _p in (_notebook_dir, _repo_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# COMMAND ----------

from giscovery_crawler.config import load_task_config
from giscovery_crawler.runner import run_crawler
from giscovery_crawler.sink import VolumeSink

task_name = dbutils.widgets.get("task_name").strip()
if not task_name:
    raise ValueError("task_name 不可為空")

params = load_task_config(spark, dbutils.widgets.get("config_table"), task_name)
sink = VolumeSink(
    catalog=dbutils.widgets.get("sink_catalog"),
    schema=dbutils.widgets.get("sink_schema"),
    volume=dbutils.widgets.get("sink_volume"),
    task_name=task_name,
)
print(f"task={task_name}\nparams={ {k: v for k, v in params.items() if k != 'browser_headers'} }\nsink={sink.base_dir}")

# COMMAND ----------

result = run_crawler(task_name, params, sink, secrets=dbutils.secrets.get)
print(f"* Done. {len(sink.written)} file(s) written to {sink.base_dir}")
dbutils.notebook.exit(sink.base_dir)
