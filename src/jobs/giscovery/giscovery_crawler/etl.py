"""ETL 階段：依 etl_config 對爬蟲下載的檔案做欄位轉換，輸出到 etldestined Volume。

移植自 app-aws-crawler 的 Glue job（script/glue/glue_etl_job/glue_etl_job.py），轉換步驟與順序相同：
    0. 讀檔：header=True，再套用 etl_config 的 load_file（例如 delimiter）
    1. rename_columns：只改存在的欄位
    2. drop_columns：只刪存在的欄位
    3. filter_conditions：支援 `>`、`!=`、`=`、` contains ` 四種寫法
    4. fillna：Glue 版本整段被註解掉、沒有生效，這裡維持不做（設定檔裡的 fillna 會被忽略）
    5. cast_columns：只轉存在的欄位

與 Glue 版本的差異：
    - 讀檔時依 crawler_config 的 file_encoding 解碼（例如 countyAvgHospitalizedDay 是 big5）。
      Glue 版本沒有指定編碼，Spark 以 UTF-8 讀，中文欄名會變亂碼、rename / filter 都對不到欄位。
      Spark 4 的 CSV 只接受 SPARK_CSV_CHARSETS 這幾種編碼，其他編碼（如 big5）先轉成 UTF-8 暫存檔再讀。
    - Glue 4.0 是 Spark 3.3（ANSI 模式關閉），轉型失敗會變 null；Databricks serverless 是 Spark 4（ANSI 預設開啟），
      cast 遇到不合法的值會直接報錯。cast_columns 與 `>` 比較改用 try_cast，維持「轉不過就是 null」的行為。
    - 看不懂的 filter 條件直接拋錯；Glue 版本會默默略過，等於少篩一段資料。
    - 輸出改成在 driver 端轉 pandas 寫單一 CSV（資料量都在 MB 以下），
      不再需要 coalesce(1) → 等 30 秒 → 找 part 檔改名 → 刪暫存 這段繞 S3 的流程。
"""

import codecs
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from common.storage import write_bytes

FILTER_OPERATORS = (">", "!=", "=", " contains ")

# Charsets accepted by Spark 4 CSVOptions (spark.sql.legacy.javaCharsets=false)
SPARK_CSV_CHARSETS = ("iso-8859-1", "us-ascii", "utf-16", "utf-16be", "utf-16le", "utf-32", "utf-8")
_SPARK_CSV_CODECS = {codecs.lookup(c).name for c in SPARK_CSV_CHARSETS}


@dataclass(frozen=True)
class FilterCondition:
    column: str
    operator: str  # one of FILTER_OPERATORS, stripped
    value: str | float


def needs_transcode(file_encoding: str | None) -> bool:
    """Spark 4 的 CSV 讀不了這個編碼時回傳 True（例如 big5）；沒指定編碼視為 UTF-8。"""
    return bool(file_encoding) and codecs.lookup(file_encoding).name not in _SPARK_CSV_CODECS


@contextmanager
def spark_readable_input(input_path: str, file_encoding: str | None, scratch_dir: str) -> Iterator[tuple[str, str | None]]:
    """回傳 (Spark 要讀的路徑, 要給 Spark 的 encoding)。

    Spark 讀得了的編碼直接回傳原檔；讀不了的先在 scratch_dir 寫一份 UTF-8 暫存檔（Spark 只讀得到 Volume 上的檔案，
    所以暫存檔也要放在 Volume），離開 with 區塊時刪除。DataFrame 是 lazy 的，要在 with 區塊內把結果算完。
    """
    if not needs_transcode(file_encoding):
        yield input_path, file_encoding
        return
    with open(input_path, "rb") as f:
        utf8 = f.read().decode(file_encoding).encode("utf-8")
    tmp_path = os.path.join(scratch_dir, f"etl_input_{uuid.uuid4().hex}_{os.path.basename(input_path)}")
    write_bytes(tmp_path, utf8)
    print(f"* transcoded {file_encoding} -> utf-8: {tmp_path}")
    try:
        yield tmp_path, "utf-8"
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def build_read_options(etl_config: dict[str, Any], file_encoding: str | None = None) -> dict[str, Any]:
    """組出 spark.read 的 options：encoding → load_file → header 強制為 True。

    Glue 版本最後又呼叫一次 .option("header", True)，所以 load_file 就算寫了 header 也會被蓋掉，這裡照做。
    """
    options: dict[str, Any] = {}
    if file_encoding:
        options["encoding"] = file_encoding
    options.update(etl_config.get("load_file") or {})
    options["header"] = True
    return options


def parse_filter_condition(condition: str) -> FilterCondition:
    """把設定檔的 filter 字串解析成 (欄位, 運算子, 值)。

    判斷順序與 Glue 版本相同（先 `>`、再 `!=`、再 `=`、最後 ` contains `）；
    `>` 的值轉成 float，其餘去掉前後的單／雙引號。
    """
    for op in FILTER_OPERATORS:
        if op not in condition:
            continue
        column, _, raw = condition.partition(op)
        column, raw = column.strip(), raw.strip()
        if not column or not raw:
            break
        if op == ">":
            return FilterCondition(column, ">", float(raw))
        return FilterCondition(column, op.strip(), raw.strip("'").strip('"'))
    raise ValueError(f"看不懂的 filter 條件：{condition!r}（支援 >、!=、=、contains）")


def apply_etl(df, etl_config: dict[str, Any]):
    """依 etl_config 依序做 rename → drop → filter → cast，回傳新的 DataFrame。"""
    from pyspark.sql import functions as F

    def try_cast(column: str, dtype: str):
        # Backtick-quote the column name: config columns may contain spaces, "(" or "/"
        return F.expr(f"try_cast(`{column.replace('`', '``')}` AS {dtype})")

    renames = etl_config.get("rename_columns") or {}
    if renames:
        print("columns before rename:", df.columns)
        for old, new in renames.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        print("Renaming columns done")

    drops = [c for c in (etl_config.get("drop_columns") or []) if c in df.columns]
    if etl_config.get("drop_columns"):
        df = df.drop(*drops)
        print("Dropping columns done")

    conditions = etl_config.get("filter_conditions") or []
    for cond in map(parse_filter_condition, conditions):
        col = F.col(cond.column)
        if cond.operator == ">":
            df = df.filter(try_cast(cond.column, "DOUBLE") > cond.value)
        elif cond.operator == "!=":
            df = df.filter(col != cond.value)
        elif cond.operator == "=":
            df = df.filter(col == cond.value)
        else:  # contains
            df = df.filter(col.contains(cond.value))
    if conditions:
        print("Filtering rows done")

    casts = etl_config.get("cast_columns") or {}
    for column, dtype in casts.items():
        if column in df.columns:
            df = df.withColumn(column, try_cast(column, dtype))
    if casts:
        print("Casting columns done")

    return df


def to_csv_bytes(df) -> bytes:
    """把（小量的）Spark DataFrame 轉成含表頭的 UTF-8 CSV。"""
    return df.toPandas().to_csv(index=False).encode("utf-8")
