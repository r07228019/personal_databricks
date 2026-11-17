# personal_databricks

個人 Databricks workspace 的 job 集合，以 Databricks Asset Bundle（DAB）管理、GitHub Actions 部署。

目前有四類 job：

| 類別 | Job | 內容 |
|---|---|---|
| cwa | `dailyCWATemperatureMap` | 每天 18:00（台北）抓中央氣象署當天 13:00 的溫度分布圖到 UC Volume |
| giscovery | `<task_name>` × 13（如 `cathayATM`、`debrisCoverage`） | 從 [app-aws-crawler](https://dev.azure.com/cfhclouddevops/D.tech-GIS2.0/_git/app-aws-crawler) 移植的 13 支爬蟲，各自排程、各自一個 job，下載檔案到 UC Volume |
| giscovery | `crawlerEtl` | 爬蟲的後處理（移植自原本的 glue-trigger Lambda 與 Glue job），每支爬蟲 job 爬完後觸發，可多個 run 並行：`to_etl=true` 做 ETL，其餘原封不動複製到 etldestined |
| config | `updateGiscoveryConfig` | 把 jsonl 設定檔同步到 Delta table（取代原本的 DynamoDB），無排程；GitHub Actions 部署完會自動執行，也可手動執行 |

Job 命名一律用 lowerCamelCase、不加專案前綴（爬蟲 job 直接沿用 `task_name`，例如 `CWB`）；專案與類型用 tags 區分：

| Tag | 值 |
|---|---|
| `project-name` | `giscovery` / `cwa` |
| `job-type` | `crawler` / `etl` / `config` |
| `category`、`task_name` | 只有 giscovery 爬蟲有，來自 jsonl（如 `geo` / `medical`） |

## 目錄結構

```
.
├── databricks.yml                  # bundle 定義（target: prod）
├── resources/
│   ├── daily_cwa_temperature_map.job.yml
│   ├── crawler_etl.job.yml          # 共用的後處理 job（crawlerEtl：ETL 或複製）
│   ├── update_giscovery_config.job.yml
│   └── giscovery/<task_name>.job.yml   # 13 個爬蟲 job，由 scripts/gen_crawler_jobs.py 產生
├── common/                          # 跨 job 共用模組（config 讀取、HTTP 重試、Volume 寫檔）
├── src/jobs/
│   ├── test/DailyCWATemperatureMap/ # CWA 溫度圖 job（notebook + config.json + 專屬套件）
│   ├── giscovery/
│   │   ├── run_crawler.py           # 13 個爬蟲 job 共用的 notebook 進入點（參數 task_name）
│   │   ├── run_etl.py               # crawlerEtl job 的 notebook（參數 task_name）
│   │   ├── crawlers/<task_name>.py  # 各支爬蟲，介面 main(sink, params)
│   │   └── giscovery_crawler/       # 共用模組：config / sink / datagov / http / runner / schedule / etl / passthrough
│   └── config/
│       ├── update_giscovery_config.py   # jsonl → Delta table 的 notebook
│       ├── config_sync.py
│       └── data/*.jsonl             # 設定檔（沿用原 DynamoDB 的檔名）
├── scripts/gen_crawler_jobs.py      # 依 jsonl 產生 resources/giscovery/*.job.yml
├── tests/                           # pytest 單元測試
└── .github/workflows/deploy.yml     # push main → 測試 → bundle deploy → 同步設定表
```

## giscovery 爬蟲

### 設定與資料流

```
src/jobs/config/data/cfh-gis20-ddb-crawler-config.jsonl
        │  updateGiscoveryConfig job（部署時自動執行，或手動執行）
        ▼
test.giscovery.crawler_config（Delta；task_name / config_json / updated_at）
        │  run_crawler notebook 依 task_name 讀一行
        ▼
crawlers/<task_name>.py main(sink, params)
        │
        ▼
/Volumes/test/giscovery/crawlersink/<task_name>/<檔案>
        │  爬蟲 job 的 etl task（Run Job）帶 task_name 觸發 crawlerEtl
        ▼
run_etl notebook 依 crawler_config 的 to_etl 分流
        ├─ to_etl=true：依 etl_config（test.giscovery.etl_config）轉換
        └─ 其他：原封不動複製
        │
        ▼
/Volumes/test/giscovery/etldestined/<task_name>/<同檔名>
```

- 設定表只有 `task_name`、`config_json`（整行 JSON 字串）、`updated_at` 三欄，不把 JSON 展開，因為 `etl_config` 的 nested key 含 `(`、`/` 等 Delta 不允許的字元。
- 檔名規則沿用原本 S3 的 key：多數是 `<task_name>.<file_format>`；`soilLiquefaction` 是每個縣市一個 `liquefaction_<代碼>_<縣市>_<l|m|h>.json`；`CWB` 是 `<資料夾>_<原檔名>`。
- Volume 內同名檔案直接覆蓋（`common.storage.write_bytes`：先寫暫存檔、確認大小後 `os.replace`）。

### ETL 階段

取代原本 AWS 的「S3 `ObjectCreated` 事件 → glue-trigger Lambda（在 terraform-aws-infra repo）→ Glue job / 直接複製」。

- **觸發**：`gen_crawler_jobs.py` 替每支爬蟲 job 都產生一個 `etl` task，`crawl` 成功後以 `run_job_task` 觸發 `crawlerEtl`，只帶 `task_name`。
- **分流**：與 Lambda 相同，`crawler_config` 的 `to_etl` 為字串 `true`（不分大小寫）才做 ETL，目前有 5 支：`countyAvgHospitalizedDay`、`potentialWindField`（排程暫停中）、`renewableEnergyPlant`、`solarPowerGeneration`、`windPowerGeneration`；其餘 8 支走複製。
- **並行**：`crawlerEtl` 的 `max_concurrent_runs` 是 50（與原本 Glue job 相同），並開啟 queue，超過上限的 run 會排隊而不是被略過。
- **轉換**：`giscovery_crawler/etl.py`，依 `etl_config` 依序做 rename → drop → filter → cast，與 Glue 版本相同；`fillna` 在 Glue 版本被註解掉、沒有生效，這裡也不做。
- **與 Glue 版本的差異**：
  - 讀檔時依 `crawler_config` 的 `file_encoding` 解碼。Spark 4 的 CSV 不支援 `big5`，會先轉成 UTF-8 暫存檔（放在輸出目錄，跑完即刪）再讀。
  - serverless 是 Spark 4（ANSI 模式預設開啟），`cast_columns` 與 `>` 比較改用 `try_cast`，維持 Glue（Spark 3.3）「轉不過就是 null」的行為。
  - 看不懂的 `filter_conditions` 會直接拋錯，Glue 版本則默默略過。
  - 輸出改用 pandas 寫單一 UTF-8 CSV，不再走 `coalesce(1)` → 改名 part 檔的流程。
- **複製（`to_etl` 不是 `true`）**：`giscovery_crawler/passthrough.py`，把 crawlersink 的 `<task_name>/` 第一層的檔案原封不動複製到 etldestined 的同一路徑，對應 Lambda 的 `copy_object`。Lambda 是每個 S3 事件複製一個檔；這裡是爬完後處理整個目錄，只複製目的端沒有、大小不同或來源較新的檔，所以爬蟲沒重新下載的檔（例如 `CWB` 已存在的檔）不會重複複製。目的端多出來的檔不會刪除，與 Lambda 相同。
- **手動執行**：`databricks bundle run crawlerEtl -t prod -p personal --params task_name=windPowerGeneration`。

### 排程

jsonl 的 `schedule` 欄位沿用 AWS EventBridge 的 6 欄位 cron（分 時 日 月 週 年），`scripts/gen_crawler_jobs.py` 會轉成 Databricks 的 Quartz cron（前面補秒），時區 `Asia/Taipei`。

改排程的流程：改 jsonl → `python scripts/gen_crawler_jobs.py` → commit 兩者。CI 會用 `--check` 確認 yml 與 jsonl 一致。

### 排程固定暫停的爬蟲

下列三支目前在 Databricks 上跑不起來，排程固定 `PAUSED`（`gen_crawler_jobs.py` 的 `PAUSED_TASKS`，不論 jsonl 怎麼寫），暫停原因也會寫進 job 的 description。workspace 在 AWS us-east-2，serverless 對外是美國 IP。

| Job | 原因 |
|---|---|
| `CWB` | 需連到中央氣象署內部的 SFTP，Databricks 連不到，因此無法實際執行。程式仍保留：帳密讀 secret scope `giscovery`（key：`cwb_sftp_hostname` / `cwb_sftp_username` / `cwb_sftp_password`） |
| `potentialWindField` | 下載來源 `www.moeaea.gov.tw` 對 Databricks 的連線回 HTTP 403，從台灣 IP 可正常下載 |
| `soilLiquefaction` | 目錄檔來源 `www.gsmma.gov.tw` 對 Databricks 的連線回 HTTP 403，從台灣 IP 可正常下載；另外 51 個 GeoJSON 在 `www.geologycloud.tw`，連續下載疑似會被限流（未證實） |

要恢復排程：先解決連線問題（例如改由台灣 IP 連線），再把該 task 從 `PAUSED_TASKS` 移除並重新產生 yml。

### 新增一支爬蟲

1. 在 `src/jobs/config/data/cfh-gis20-ddb-crawler-config.jsonl` 加一行（至少要有 `task_name`、`category`）。
2. 新增 `src/jobs/giscovery/crawlers/<task_name>.py`，實作 `main(sink, params) -> list[str]`：
   - `params` 是 jsonl 那一行的 dict。
   - `sink.write(file_name, content)` 寫檔（`bytes` 或 `str`），回傳完整路徑。
   - 需要密碼時改成 `main(sink, params, secrets)`，`secrets(scope, key)` 對應 `dbutils.secrets.get`。
   - data.gov.tw 的搜尋用 `giscovery_crawler.datagov`，HTTP 用 `giscovery_crawler.http`。
3. `python scripts/gen_crawler_jobs.py` 產生 job yml。
4. 需要 ETL 時：jsonl 設 `"to_etl": "true"`，並在 `cfh-gis20-ddb-etl-config.jsonl` 加同一個 `task_name` 的轉換設定；否則爬完的檔會原封不動複製到 etldestined。
5. 經 GitHub Actions 部署時會自動跑 `updateGiscoveryConfig`；本機部署則要先手動跑一次，再跑新 job。

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
| （新增）Sync Config | `bundle run updateGiscoveryConfig` 並等它跑完，把 jsonl 同步到 Delta 設定表；job 失敗、被取消或逾時，整個部署就算失敗 |

觸發條件：push 到 `main`（或手動 `workflow_dispatch`）。先跑 `test` job（`gen_crawler_jobs.py --check` + pytest），通過才 `deploy`；`deploy` 的最後一步是同步設定表，成功才算部署成功。

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
