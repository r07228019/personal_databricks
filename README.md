# personal_databricks

個人 Databricks workspace 的 job 集合，以 Databricks Asset Bundle（DAB）管理、GitHub Actions 部署。

目前有三類 job：

| 類別 | Job | 內容 |
|---|---|---|
| cwa | `dailyCWATemperatureMap` | 每天 18:00（台北）抓中央氣象署當天 13:00 的溫度分布圖到 UC Volume |
| giscovery | `<task_name>` × 13（如 `cathayATM`、`debrisCoverage`） | 從 [app-aws-crawler](https://dev.azure.com/cfhclouddevops/D.tech-GIS2.0/_git/app-aws-crawler) 移植的 13 支爬蟲，各自排程、各自一個 job，下載檔案到 UC Volume |
| config | `updateGiscoveryConfig` | 把 jsonl 設定檔同步到 Delta table（取代原本的 DynamoDB），無排程、手動執行 |

Job 命名一律用 lowerCamelCase、不加專案前綴（爬蟲 job 直接沿用 `task_name`，例如 `CWB`）；專案與類型用 tags 區分：

| Tag | 值 |
|---|---|
| `project-name` | `giscovery` / `cwa` |
| `job-type` | `crawler` / `config` |
| `category`、`task_name` | 只有 giscovery 爬蟲有，來自 jsonl（如 `geo` / `medical`） |

## 目錄結構

```
.
├── databricks.yml                  # bundle 定義（target: prod）
├── resources/
│   ├── daily_cwa_temperature_map.job.yml
│   ├── update_giscovery_config.job.yml
│   └── giscovery/<task_name>.job.yml   # 13 個爬蟲 job，由 scripts/gen_crawler_jobs.py 產生
├── common/                          # 跨 job 共用模組（config 讀取、HTTP 重試、Volume 寫檔）
├── src/jobs/
│   ├── test/DailyCWATemperatureMap/ # CWA 溫度圖 job（notebook + config.json + 專屬套件）
│   ├── giscovery/
│   │   ├── run_crawler.py           # 13 個爬蟲 job 共用的 notebook 進入點（參數 task_name）
│   │   ├── crawlers/<task_name>.py  # 各支爬蟲，介面 main(sink, params)
│   │   └── giscovery_crawler/       # 共用模組：config / sink / datagov / http / runner / schedule
│   └── config/
│       ├── update_giscovery_config.py   # jsonl → Delta table 的 notebook
│       ├── config_sync.py
│       └── data/*.jsonl             # 設定檔（沿用原 DynamoDB 的檔名）
├── scripts/gen_crawler_jobs.py      # 依 jsonl 產生 resources/giscovery/*.job.yml
├── tests/                           # pytest 單元測試
└── .github/workflows/deploy.yml     # push main → 測試 → bundle deploy
```

## giscovery 爬蟲

### 設定與資料流

```
src/jobs/config/data/cfh-gis20-ddb-crawler-config.jsonl
        │  updateGiscoveryConfig job（手動執行）
        ▼
test.giscovery.crawler_config（Delta；task_name / config_json / updated_at）
        │  run_crawler notebook 依 task_name 讀一行
        ▼
crawlers/<task_name>.py main(sink, params)
        │
        ▼
/Volumes/test/giscovery/crawlersink/<task_name>/<檔案>
```

- 設定表只有 `task_name`、`config_json`（整行 JSON 字串）、`updated_at` 三欄，不把 JSON 展開，因為 `etl_config` 的 nested key 含 `(`、`/` 等 Delta 不允許的字元。
- 檔名規則沿用原本 S3 的 key：多數是 `<task_name>.<file_format>`；`soilLiquefaction` 是每個縣市一個 `liquefaction_<代碼>_<縣市>_<l|m|h>.json`；`CWB` 是 `<資料夾>_<原檔名>`。
- Volume 內同名檔案直接覆蓋（`common.storage.write_bytes`：先寫暫存檔、確認大小後 `os.replace`）。

### 排程

jsonl 的 `schedule` 欄位沿用 AWS EventBridge 的 6 欄位 cron（分 時 日 月 週 年），`scripts/gen_crawler_jobs.py` 會轉成 Databricks 的 Quartz cron（前面補秒），時區 `Asia/Taipei`。

改排程的流程：改 jsonl → `python scripts/gen_crawler_jobs.py` → commit 兩者。CI 會用 `--check` 確認 yml 與 jsonl 一致。

`CWB` 走 SFTP，帳密需放在 Databricks secret scope `giscovery`（key：`cwb_sftp_hostname` / `cwb_sftp_username` / `cwb_sftp_password`），而且 serverless 對外連 port 22 尚未驗證，所以它的排程固定 `PAUSED`（見 `gen_crawler_jobs.py` 的 `PAUSED_TASKS`）。

### 新增一支爬蟲

1. 在 `src/jobs/config/data/cfh-gis20-ddb-crawler-config.jsonl` 加一行（至少要有 `task_name`、`category`）。
2. 新增 `src/jobs/giscovery/crawlers/<task_name>.py`，實作 `main(sink, params) -> list[str]`：
   - `params` 是 jsonl 那一行的 dict。
   - `sink.write(file_name, content)` 寫檔（`bytes` 或 `str`），回傳完整路徑。
   - 需要密碼時改成 `main(sink, params, secrets)`，`secrets(scope, key)` 對應 `dbutils.secrets.get`。
   - data.gov.tw 的搜尋用 `giscovery_crawler.datagov`，HTTP 用 `giscovery_crawler.http`。
3. `python scripts/gen_crawler_jobs.py` 產生 job yml。
4. 部署後先跑一次 `updateGiscoveryConfig`，再跑新 job。

## 本機開發

```bash
# 單元測試（不需要 Databricks）；套件清單與 .github/workflows/deploy.yml 的 test job 相同
pip install pytest requests pandas beautifulsoup4 paramiko
pytest

# 驗證 / 部署 / 執行（profile 由 ~/.databrickscfg 決定）
databricks bundle validate -t prod -p personal
databricks bundle deploy   -t prod -p personal
databricks bundle run updateGiscoveryConfig -t prod -p personal
databricks bundle run cathayATM -t prod -p personal
```

`pytest.ini` 的 `pythonpath` 模擬 Databricks 上的 import 行為（repo 根目錄 + 各 job 目錄）；新增 job 目錄時記得加進去。

### notebook 如何 import `common`

bundle 部署的位置（`.bundle/personal_databricks/prod/files`）是一般 workspace 資料夾，不是 Git folder，Databricks 只會自動把 notebook 所在目錄加進 `sys.path`，不會加 repo 根目錄（[官方說明](https://docs.databricks.com/aws/en/files/workspace-modules)）。

所以凡是要 import `common` 的 notebook，都要有 job parameter `repo_root`（預設 `${workspace.file_path}`），並在第一個程式 cell 把它加進 `sys.path`；`repo_root` 空白時改由 notebook 位置往上推算。現有做法見 `run_crawler.py` 與 `daily_cwa_temperature_map.py`，新增 job 時比照辦理。

## 部署（GitHub Actions）

`.github/workflows/deploy.yml` 對應原本 Azure DevOps Classic Release「[Old] Lab Deploy」的流程：

| Release 步驟 | GitHub Actions |
|---|---|
| Python / Databricks CLI 環境確認 | `databricks/setup-cli` 固定 `DATABRICKS_CLI_VERSION` |
| Auth（SP 換 OAuth token 寫 `.databrickscfg`） | 改用 GitHub Secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN`（個人帳號 PAT），CLI 直接讀環境變數 |
| Substitute（sed 置換 `<CDL_*>` 佔位符） | 不需要：host 不寫在 `databricks.yml`，`run_as` 與通知信箱由 `${workspace.current_user}` 推得 |
| Validate / Deploy `--force-lock` | 相同 |
| Check Deploy（`workspace list`） | 相同，路徑改由 `bundle summary` 取得 |
| Clean up | 不需要（runner 用完即丟） |

觸發條件：push 到 `main`（或手動 `workflow_dispatch`）。先跑 `test` job（`gen_crawler_jobs.py --check` + pytest），通過才 `deploy`。

需要在 GitHub repo 設定的 secrets（Settings → Secrets and variables → Actions，environment `prod`）：

| Secret | 值 |
|---|---|
| `DATABRICKS_HOST` | workspace URL，例如 `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | 以部署者帳號建立的 personal access token |

部署後的 bundle 根目錄在 `/Workspace/Users/<user>/.bundle/personal_databricks/prod`，job 以部署者本人（`run_as`）執行。

## 參考

- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Bundle authentication](https://docs.databricks.com/aws/en/dev-tools/bundles/authentication)
- [Run a CI/CD workflow with a bundle and GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd-bundles)
- 原始爬蟲專案：[app-aws-crawler](https://dev.azure.com/cfhclouddevops/D.tech-GIS2.0/_git/app-aws-crawler)
