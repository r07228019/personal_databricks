"""giscovery 爬蟲共用邏輯（從 app-aws-crawler 的 Lambda 版本移植到 Databricks）。

- config：從 Delta table 讀取單一 task 的設定
- sink：把下載內容寫進 UC Volume（取代原本的 S3 bucket）
- datagov：data.gov.tw 搜尋 API 的共用查詢
- schedule：EventBridge cron → Quartz cron 轉換（產生 job yml 用）
- runner：依 task_name 載入 crawlers/<task_name>.py 並執行 main()
"""
