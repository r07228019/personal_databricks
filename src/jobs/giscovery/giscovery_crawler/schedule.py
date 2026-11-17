"""EventBridge Scheduler 的 cron → Databricks（Quartz）cron。

jsonl 的 schedule 欄位沿用原本 AWS 的「裸」6 欄位格式：分 時 日 月 週 年（例如 `0 16 1 * ? *`）。
Databricks 用 Quartz：秒 分 時 日 月 週 [年]，所以在前面補上秒 = 0。
"""

EVENTBRIDGE_FIELD_COUNT = 6


def eventbridge_to_quartz(expression: str) -> str:
    expression = expression.strip()
    if expression.startswith("cron(") or expression.startswith("rate("):
        raise ValueError(f"schedule 必須是不帶 cron(...) 外框的裸 cron 運算式，得到 {expression!r}")
    fields = expression.split()
    if len(fields) != EVENTBRIDGE_FIELD_COUNT:
        raise ValueError(
            f"EventBridge cron 需要 6 個欄位（分 時 日 月 週 年），得到 {len(fields)} 個：{expression!r}"
        )
    return " ".join(["0", *fields])
