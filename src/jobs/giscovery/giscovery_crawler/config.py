"""從 Delta table 讀取爬蟲設定。

設定表結構（由 src/jobs/config 的 job 依 jsonl 維護）：
    task_name   STRING     主鍵，對應 crawlers/<task_name>.py
    config_json STRING     jsonl 的整行 JSON（含 task_name）
    updated_at  TIMESTAMP  最後更新時間
"""

import json
from typing import Any


class TaskConfigNotFound(LookupError):
    """設定表裡沒有這個 task_name。"""


def parse_config_json(config_json: str) -> dict[str, Any]:
    """把 config_json 欄位解析成 dict，並拿掉值為 None 的 key（與 DynamoDB 行為一致：沒有的欄位就是不存在）。"""
    cfg = json.loads(config_json)
    if not isinstance(cfg, dict):
        raise ValueError("config_json 必須是 JSON object")
    return {k: v for k, v in cfg.items() if v is not None}


def load_task_config(spark, table_full_name: str, task_name: str) -> dict[str, Any]:
    """用 task_name 查設定表，回傳解析後的 dict。查不到就拋 TaskConfigNotFound。"""
    from pyspark.sql import functions as F

    rows = (
        spark.table(table_full_name)
        .where(F.col("task_name") == task_name)
        .select("config_json")
        .limit(1)
        .collect()
    )
    if not rows:
        raise TaskConfigNotFound(f"{table_full_name} 裡找不到 task_name={task_name!r}")
    return parse_config_json(rows[0]["config_json"])
