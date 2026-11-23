"""giscovery 爬蟲共用邏輯（從 app-aws-crawler 的 Lambda 版本移植到 Databricks）。

- config：從 Delta table 讀取單一 task 的設定
- sink：把下載內容寫進 UC Volume（取代原本的 S3 bucket）
- datagov：data.gov.tw 搜尋 API 的共用查詢
- schedule：EventBridge cron → Quartz cron 轉換（產生 job yml 用）
- runner：依 task_name 載入 crawlers/<task_name>.py 並執行 main()
- etl：依 etl_config 轉換爬蟲下載的檔案（移植自 Glue job，由 run_etl notebook 呼叫）
- passthrough：to_etl=false 的檔案原封不動複製到 etldestined（移植自 glue-trigger Lambda 的 copy_file）
- geo：依 geo_config 把 etldestined 的地理資料用 RustGIS 轉成 EPSG:4326 GeoParquet（由 run_geo notebook 呼叫）
- geocode：地址轉座標（Google Geocoding），含 Delta 快取與每月呼叫上限
"""
