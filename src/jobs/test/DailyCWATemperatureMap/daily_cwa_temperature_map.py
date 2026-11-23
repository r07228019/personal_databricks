# Databricks notebook source
# MAGIC %md
# MAGIC # dailyCWATemperatureMap
# MAGIC
# MAGIC 抓取中央氣象署（CWA）指定日期、小時的溫度分布圖，存入 UC Volume（同名檔案直接覆蓋）。
# MAGIC 排程每天 18:00（台北）執行，預設抓當天 13:00 的圖。
# MAGIC
# MAGIC 本 notebook 只是進入點，負責串起大流程；實作都在模組裡：
# MAGIC
# MAGIC | 模組 | 位置 | 職責 |
# MAGIC |---|---|---|
# MAGIC | `common.config` | repo 根目錄 `common/` | 解析 config 路徑、讀 JSON |
# MAGIC | `common.databricks_utils` | repo 根目錄 `common/` | 取得 notebook 所在目錄 |
# MAGIC | `common.http_utils` | repo 根目錄 `common/` | 帶重試的 HTTP 下載 |
# MAGIC | `common.storage` | repo 根目錄 `common/` | Volume 路徑組裝、安全寫檔（暫存檔 → 確認 → 取代） |
# MAGIC | `cwa_temperature_map.params` | 本目錄 | `data_date` / `data_hour` 解析與檢查 |
# MAGIC | `cwa_temperature_map.source` | 本目錄 | 組 CWA URL 與目的地路徑 |
# MAGIC
# MAGIC 可抽換的設定（時區、預設日期與小時、URL template、Volume 位置、檔名規則、重試與檢查參數）都在同目錄的
# MAGIC `config.json`。排程時間屬於 job 定義，不在 config 內。
# MAGIC
# MAGIC 參數（job parameter，由 job 定義傳入，notebook 不設預設值）：預設值的唯一來源是 config 的 `defaults`，job parameter 的預設都是空白。
# MAGIC
# MAGIC | 參數 | 格式 | 空白時 | 說明 |
# MAGIC |---|---|---|---|
# MAGIC | `data_date` | `yyyy-MM-dd` 或 `today` | config `defaults.data_date`（目前 `today`） | `today` = config 時區的當天 |
# MAGIC | `data_hour` | `0`～`23` 的整數 | config `defaults.data_hour`（目前 `13`） | |
# MAGIC | `config_path` | 絕對路徑 | 本 notebook 同目錄的 `config.json` | |
# MAGIC | `repo_root` | 絕對路徑 | 由本 notebook 位置往上推四層 | repo 根目錄，用來 import `common`；bundle 部署時為 `${workspace.file_path}` |
# MAGIC
# MAGIC bundle 部署的位置是一般 workspace 資料夾（不是 Git folder），Databricks 只會自動把 notebook 所在目錄加進
# MAGIC `sys.path`，所以第一個程式 cell 會先把 repo 根目錄加進去，之後才 import `common`。
# MAGIC
# MAGIC 檢查規則：日期必須是真實存在的日期、小時必須是 0～23 的整數，且資料時間不可晚於現在、
# MAGIC 也不可早於 `validation.max_lookback_hours`（CWA 只保留最近約 5 天的圖）。config 的預設值也走同一套檢查。
# MAGIC 不合法時 job 直接失敗，不會下載。
# MAGIC
# MAGIC 單元測試在 repo 根目錄的 `tests/`（本機執行：`pytest`，設定見根目錄 `pytest.ini`）。

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import sys

# Parameters come from the job definition (resources/*.yml); read them with dbutils.widgets.get only.

# A bundle deploys to a plain workspace folder, not a Git folder, so Databricks only puts the
# notebook directory on sys.path. Add the repo root explicitly so `import common` resolves.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/Workspace" + os.path.dirname(_notebook_path)
_repo_root = dbutils.widgets.get("repo_root").strip() or os.path.abspath(os.path.join(_notebook_dir, "../../../.."))
for _p in (_notebook_dir, _repo_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

from common.config import load_json_config, resolve_config_path
from common.databricks_utils import get_notebook_dir
from common.http_utils import download_bytes
from common.storage import write_bytes
from cwa_temperature_map.params import parse_and_validate
from cwa_temperature_map.source import build_dest_path, build_source_url

# COMMAND ----------

# 1. 讀取 config
config_path = resolve_config_path(dbutils.widgets.get("config_path"), get_notebook_dir(dbutils))
cfg = load_json_config(config_path)
now = datetime.now(ZoneInfo(cfg["timezone"]))

# 2. 解析並檢查參數（不合法直接拋錯，不會下載）
params = parse_and_validate(
    dbutils.widgets.get("data_date"),
    dbutils.widgets.get("data_hour"),
    now=now,
    default_date=cfg["defaults"]["data_date"],
    default_hour=cfg["defaults"]["data_hour"],
    max_lookback_hours=cfg["validation"]["max_lookback_hours"],
)

# 3. 組出來源 URL 與目的地路徑
src_url = build_source_url(cfg["source"], params)
dest_path = build_dest_path(cfg["destination"], params)
print(f"config={config_path}\ndata={params.data_date} {params.data_hour:02d}:00\nsrc={src_url}\ndest={dest_path}")

# COMMAND ----------

# 4. 下載並寫入 Volume（先寫暫存檔、確認後才取代同名檔案；失敗時舊檔維持不動）
content = download_bytes(
    src_url,
    max_attempts=cfg["http"]["max_attempts"],
    retry_wait_seconds=cfg["http"]["retry_wait_seconds"],
    timeout_seconds=cfg["http"]["timeout_seconds"],
    expected_content_type=cfg["source"]["expected_content_type"],
)
result = write_bytes(dest_path, content, overwrite=True)
print(f"{'overwrote' if result.overwritten else 'saved'} {result.size:,} bytes -> {result.path}")

dbutils.notebook.exit(result.path)
