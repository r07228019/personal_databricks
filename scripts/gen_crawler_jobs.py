"""依 crawler_config jsonl 產生 resources/giscovery/<task_name>.job.yml。

對應原本 AWS 的 pipeline_script/sync_schedules.py：jsonl 的 schedule 欄位是排程的唯一來源，
這裡把它轉成 Databricks job 定義（Quartz cron），而不是去 reconcile EventBridge。

    python scripts/gen_crawler_jobs.py          # 產生 / 更新
    python scripts/gen_crawler_jobs.py --check  # 只檢查是否與 jsonl 一致（CI 用）

- 沒有 schedule 欄位的 task 不建排程（job 仍會建立，可手動執行）
- schedule_enabled 為 "false" 的 task 排程 PAUSED
- 每支都有 etl task：crawl 成功後以 Run Job task 觸發共用的 crawlerEtl job（只帶 task_name），
  由 crawlerEtl 依 to_etl 決定做 ETL 或原封不動複製到 etldestined，取代原本 S3 事件 → glue-trigger Lambda 的串接
- cfh-gis20-geo-config.jsonl 有列的 task 再加一個 geo task：etl 成功後觸發 geoNormalize，轉成 EPSG:4326 GeoParquet
- PAUSED_TASKS 裡的 task 不論 jsonl 怎麼寫都 PAUSED，暫停原因會一併寫進 job 的 description
  （目前在 Databricks 上跑不起來的三支：CWB、potentialWindField、soilLiquefaction）
"""

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src" / "jobs" / "giscovery"))

from giscovery_crawler.schedule import eventbridge_to_quartz  # noqa: E402

JSONL_PATH = REPO_ROOT / "src" / "jobs" / "config" / "data" / "cfh-gis20-ddb-crawler-config.jsonl"
GEO_JSONL_PATH = REPO_ROOT / "src" / "jobs" / "config" / "data" / "cfh-gis20-geo-config.jsonl"
OUTPUT_DIR = REPO_ROOT / "resources" / "giscovery"
NOTEBOOK_PATH = "../../src/jobs/giscovery/run_crawler.py"
TIMEZONE = "Asia/Taipei"
# Resource key of the shared ETL job (resources/crawler_etl.job.yml)
ETL_JOB_KEY = "crawlerEtl"
# Resource key of the shared geo normalization job (resources/geo_normalize.job.yml)
GEO_JOB_KEY = "geoNormalize"
# task_name -> why the schedule is forced to PAUSED. The reason is appended to the job description.
# The workspace runs on AWS us-east-2, so serverless egress leaves from US IP addresses.
PAUSED_TASKS = {
    "CWB": "需連到中央氣象署內部的 SFTP，Databricks 連不到，因此無法實際執行",
    "potentialWindField": (
        "下載來源 www.moeaea.gov.tw 對 Databricks serverless（AWS us-east-2）的連線回 HTTP 403，"
        "從台灣 IP 可正常下載；需改由台灣 IP 連線才能啟用"
    ),
    "soilLiquefaction": (
        "目錄檔來源 www.gsmma.gov.tw 對 Databricks serverless（AWS us-east-2）的連線回 HTTP 403，"
        "從台灣 IP 可正常下載；另外 51 個 GeoJSON 在 www.geologycloud.tw，連續下載疑似會被限流（未證實）"
    ),
}
HEADER = (
    "# 由 scripts/gen_crawler_jobs.py 依 src/jobs/config/data/cfh-gis20-ddb-crawler-config.jsonl 自動產生，\n"
    "# 不要手改；改 jsonl 後重新執行 `python scripts/gen_crawler_jobs.py`。\n"
)


def load_tasks(path: Path) -> list[dict]:
    tasks = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                tasks.append(json.loads(line))
    return tasks


def render_job(task: dict, geo_enabled: bool = False) -> str:
    name = task["task_name"]
    # job key / name 直接用 task_name，不加前綴；專案與類型改用 tags 區分
    job_key = name
    category = task.get("category", "")
    url = task.get("download_url", "")

    description = f"giscovery 爬蟲 {name}（category={category}）{('，來源 ' + url) if url else ''}"
    if name in PAUSED_TASKS:
        description += f"。【排程暫停】{PAUSED_TASKS[name]}"

    lines = [
        HEADER,
        "resources:",
        "  jobs:",
        f"    {job_key}:",
        f"      name: {job_key}",
        f"      description: \"{description}\"",
        "      tags:",
        "        project-name: giscovery",
        "        job-type: crawler",
        f"        task_name: {name}",
        f"        category: {category}",
        "      max_concurrent_runs: 1",
        "      performance_target: STANDARD",
    ]

    expression = task.get("schedule")
    if expression:
        enabled = str(task.get("schedule_enabled", "true")).lower() == "true" and name not in PAUSED_TASKS
        lines += [
            "      schedule:",
            f"        quartz_cron_expression: \"{eventbridge_to_quartz(expression)}\"",
            f"        timezone_id: {TIMEZONE}",
            f"        pause_status: {'UNPAUSED' if enabled else 'PAUSED'}",
        ]

    lines += [
        "      email_notifications:",
        "        on_failure:",
        "          - ${var.notification_email}",
        "      environments:",
        "        - environment_key: crawler",
        "          spec:",
        "            environment_version: \"3\"",
        "            dependencies:",
        "              - beautifulsoup4",
        "              - paramiko",
        "      parameters:",
        "        - name: task_name",
        f"          default: {name}",
        "        - name: config_table",
        "          default: ${var.catalog}.${var.giscovery_schema}.crawler_config",
        "        - name: sink_catalog",
        "          default: ${var.catalog}",
        "        - name: sink_schema",
        "          default: ${var.giscovery_schema}",
        "        - name: sink_volume",
        "          default: ${var.crawlersink_volume}",
        "        - name: repo_root",
        "          default: ${workspace.file_path}",
        "      tasks:",
        "        - task_key: crawl",
        "          environment_key: crawler",
        "          notebook_task:",
        f"            notebook_path: {NOTEBOOK_PATH}",
        "          timeout_seconds: 3600",
        "          max_retries: 1",
        "          min_retry_interval_millis: 300000",
        # crawlerEtl decides ETL vs. plain copy from to_etl, like the glue-trigger Lambda did
        "        - task_key: etl",
        "          depends_on:",
        "            - task_key: crawl",
        "          run_job_task:",
        f"            job_id: ${{resources.jobs.{ETL_JOB_KEY}.id}}",
        "            job_parameters:",
        f"              task_name: {name}",
    ]
    if geo_enabled:
        # Normalize the etldestined output to EPSG:4326 GeoParquet (tasks listed in the geo config jsonl)
        lines += [
            "        - task_key: geo",
            "          depends_on:",
            "            - task_key: etl",
            "          run_job_task:",
            f"            job_id: ${{resources.jobs.{GEO_JOB_KEY}.id}}",
            "            job_parameters:",
            f"              task_name: {name}",
        ]
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="只檢查，與 jsonl 不一致時以非零離開碼結束")
    args = parser.parse_args()

    tasks = load_tasks(JSONL_PATH)
    geo_tasks = {t["task_name"] for t in load_tasks(GEO_JSONL_PATH)}
    unknown = geo_tasks - {t["task_name"] for t in tasks}
    if unknown:
        print(f"{GEO_JSONL_PATH.name} 有 crawler_config 沒有的 task：{sorted(unknown)}")
        return 1
    expected = {
        OUTPUT_DIR / f"{t['task_name']}.job.yml": render_job(t, t["task_name"] in geo_tasks) for t in tasks
    }

    existing = set(OUTPUT_DIR.glob("*.job.yml")) if OUTPUT_DIR.exists() else set()
    stale = existing - set(expected)
    changed = [p for p, content in expected.items() if not p.exists() or p.read_text(encoding="utf-8") != content]

    if args.check:
        for p in changed:
            print(f"OUT OF DATE: {p.relative_to(REPO_ROOT)}")
        for p in stale:
            print(f"STALE: {p.relative_to(REPO_ROOT)}")
        if changed or stale:
            print("resources/giscovery 與 jsonl 不一致，請執行 python scripts/gen_crawler_jobs.py")
            return 1
        print(f"OK: {len(expected)} job yml 與 jsonl 一致")
        return 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for p, content in expected.items():
        p.write_text(content, encoding="utf-8")
        print(f"wrote {p.relative_to(REPO_ROOT)}")
    for p in stale:
        p.unlink()
        print(f"removed {p.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
