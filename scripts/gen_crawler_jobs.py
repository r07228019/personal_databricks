"""依 crawler_config jsonl 產生 resources/giscovery/<task_name>.job.yml。

對應原本 AWS 的 pipeline_script/sync_schedules.py：jsonl 的 schedule 欄位是排程的唯一來源，
這裡把它轉成 Databricks job 定義（Quartz cron），而不是去 reconcile EventBridge。

    python scripts/gen_crawler_jobs.py          # 產生 / 更新
    python scripts/gen_crawler_jobs.py --check  # 只檢查是否與 jsonl 一致（CI 用）

- 沒有 schedule 欄位的 task 不建排程（job 仍會建立，可手動執行）
- schedule_enabled 為 "false" 的 task 排程 PAUSED
- PAUSED_TASKS 裡的 task 不論 jsonl 怎麼寫都 PAUSED（目前只有 CWB：SFTP 帳密與 serverless 對外 port 22 尚未驗證）
"""

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src" / "jobs" / "giscovery"))

from giscovery_crawler.schedule import eventbridge_to_quartz  # noqa: E402

JSONL_PATH = REPO_ROOT / "src" / "jobs" / "config" / "data" / "cfh-gis20-ddb-crawler-config.jsonl"
OUTPUT_DIR = REPO_ROOT / "resources" / "giscovery"
NOTEBOOK_PATH = "../../src/jobs/giscovery/run_crawler.py"
TIMEZONE = "Asia/Taipei"
PAUSED_TASKS = {"CWB"}
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


def render_job(task: dict) -> str:
    name = task["task_name"]
    # job key / name 直接用 task_name，不加前綴；專案與類型改用 tags 區分
    job_key = name
    category = task.get("category", "")
    url = task.get("download_url", "")

    lines = [
        HEADER,
        "resources:",
        "  jobs:",
        f"    {job_key}:",
        f"      name: {job_key}",
        f"      description: \"giscovery 爬蟲 {name}（category={category}）{('，來源 ' + url) if url else ''}\"",
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
        "            client: \"2\"",
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
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="只檢查，與 jsonl 不一致時以非零離開碼結束")
    args = parser.parse_args()

    tasks = load_tasks(JSONL_PATH)
    expected = {OUTPUT_DIR / f"{t['task_name']}.job.yml": render_job(t) for t in tasks}

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
