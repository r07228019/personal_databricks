"""jsonl → Delta 設定表的轉換邏輯（notebook update_giscovery_config 的實作）。"""

import json
from dataclasses import dataclass

# Delta table 名稱 → data/ 底下的 jsonl 檔名（沿用原本 DynamoDB table 的檔名）
TABLE_FILES = {
    "crawler_config": "cfh-gis20-ddb-crawler-config.jsonl",
    "etl_config": "cfh-gis20-ddb-etl-config.jsonl",
}

KEY_FIELD = "task_name"


@dataclass(frozen=True)
class ConfigRow:
    task_name: str
    config_json: str


def load_jsonl_rows(path: str) -> list[ConfigRow]:
    """讀 jsonl，每行一筆；略過空行，task_name 缺少或重複都視為錯誤。"""
    rows: list[ConfigRow] = []
    seen: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            item = json.loads(line)
            task_name = item.get(KEY_FIELD)
            if not isinstance(task_name, str) or not task_name:
                raise ValueError(f"{path}:{line_no} 缺少 {KEY_FIELD}")
            if task_name in seen:
                raise ValueError(f"{path}:{line_no} {KEY_FIELD}={task_name!r} 重複")
            seen.add(task_name)
            rows.append(ConfigRow(task_name=task_name, config_json=json.dumps(item, ensure_ascii=False)))
    return rows


def write_config_table(spark, table_full_name: str, rows: list[ConfigRow]) -> None:
    """以 overwrite 整張替換（對應原本的 truncate + put）。"""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("task_name", StringType(), nullable=False),
            StructField("config_json", StringType(), nullable=False),
        ]
    )
    df = spark.createDataFrame([(r.task_name, r.config_json) for r in rows], schema=schema).withColumn(
        "updated_at", F.current_timestamp()
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_full_name)
