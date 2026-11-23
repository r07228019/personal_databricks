import csv
import io

import pytest

from giscovery_crawler.etl import (
    FilterCondition,
    build_read_options,
    needs_transcode,
    parse_filter_condition,
    spark_readable_input,
    to_csv_bytes,
)


def test_read_options_encoding_then_load_file_then_forced_header():
    opts = build_read_options({"load_file": {"delimiter": "\\t", "header": False}}, "big5")
    assert opts == {"encoding": "big5", "multiLine": True, "delimiter": "\\t", "header": True}


def test_read_options_defaults():
    assert build_read_options({}) == {"multiLine": True, "header": True}


def test_read_options_load_file_can_disable_multiline():
    assert build_read_options({"load_file": {"multiLine": False}})["multiLine"] is False


@pytest.mark.parametrize("enc, expected", [(None, False), ("utf-8", False), ("UTF8", False), ("big5", True), ("cp950", True)])
def test_needs_transcode(enc, expected):
    assert needs_transcode(enc) is expected


def test_spark_readable_input_transcodes_and_cleans_up(tmp_path):
    src = tmp_path / "in.csv"
    src.write_bytes("縣市別\n臺北市\n".encode("big5"))
    with spark_readable_input(str(src), "big5", str(tmp_path / "out")) as (path, enc):
        assert enc == "utf-8" and path != str(src)
        assert open(path, encoding="utf-8").read() == "縣市別\n臺北市\n"
    assert not (tmp_path / "out").exists() or not list((tmp_path / "out").iterdir())
    with spark_readable_input(str(src), "utf-8", str(tmp_path)) as (path, enc):
        assert (path, enc) == (str(src), "utf-8")


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("energy_type contains '風力'", FilterCondition("energy_type", "contains", "風力")),
        ("energy_type='太陽能/Solar Power'", FilterCondition("energy_type", "=", "太陽能/Solar Power")),
        ("county != '合計'", FilterCondition("county", "!=", "合計")),
        ("capacity > 10", FilterCondition("capacity", ">", 10.0)),
    ],
)
def test_parse_filter_condition(raw, expected):
    assert parse_filter_condition(raw) == expected


@pytest.mark.parametrize("raw", ["county", "county ~ 'x'", "= 'x'"])
def test_parse_filter_condition_rejects_unknown(raw):
    with pytest.raises(ValueError):
        parse_filter_condition(raw)


# ---------------------------------------------------------------------------
# Spark-backed tests: run locally when pyspark is installed (CI skips them).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("test_etl")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except RuntimeError as e:
        # databricks-connect ships a pyspark that refuses local sessions; treat it like pyspark being absent
        pytest.skip(f"local SparkSession unavailable: {e}")
    yield session
    session.stop()


def _read(spark, tmp_path, name, content: bytes, options):
    path = tmp_path / name
    path.write_bytes(content)
    return spark.read.options(**options).csv(str(path))


def _rows(df):
    return list(csv.reader(io.StringIO(to_csv_bytes(df).decode("utf-8"))))


def test_apply_etl_power_generation(spark, tmp_path):
    from giscovery_crawler.etl import apply_etl

    src = (
        "年度/Year,月份/Month,能源別/Energy Type,發電站名稱/Station Name,發電量(度)/Power Generation(kWh)\n"
        "2025,1,風力/Wind,A,100\n"
        "2025,1,太陽能/Solar,B,\n"
        "2025,2,風力/Wind,C,abc\n"
    ).encode("utf-8")
    cfg = {
        "rename_columns": {
            "年度/Year": "year",
            "發電站名稱/Station Name": "station_name",
            "發電量(度)/Power Generation(kWh)": "power_generation",
            "能源別/Energy Type": "energy_type",
        },
        "drop_columns": ["月份/Month", "not_exists"],
        "filter_conditions": ["energy_type contains '風力'"],
        "cast_columns": {"power_generation": "double", "not_exists": "int"},
        "fillna": {"power_generation": 0},  # ignored, same as the Glue job
    }
    df = apply_etl(_read(spark, tmp_path, "w.csv", src, build_read_options(cfg, "utf-8")), cfg)
    assert _rows(df) == [
        ["year", "energy_type", "station_name", "power_generation"],
        ["2025", "風力/Wind", "A", "100.0"],
        ["2025", "風力/Wind", "C", ""],  # non-numeric cast -> null, fillna not applied
    ]


def test_apply_etl_reads_big5_with_file_encoding(spark, tmp_path):
    from giscovery_crawler.etl import apply_etl

    src = "縣市別,病床別,ALOS\n臺北市,急性,6.1\n合計,急性,7.0\n".encode("big5")
    cfg = {"filter_conditions": ["county != '合計'"], "rename_columns": {"ALOS": "avgHosDay", "病床別": "type", "縣市別": "county"}}
    (tmp_path / "c.csv").write_bytes(src)
    with spark_readable_input(str(tmp_path / "c.csv"), "big5", str(tmp_path / "scratch")) as (path, enc):
        df = apply_etl(spark.read.options(**build_read_options(cfg, enc)).csv(path), cfg)
        rows = _rows(df)
    assert rows == [["county", "type", "avgHosDay"], ["臺北市", "急性", "6.1"]]


def test_greater_than_filter_tolerates_non_numeric(spark, tmp_path):
    from giscovery_crawler.etl import apply_etl

    cfg = {"filter_conditions": ["capacity > 10"]}
    df = apply_etl(_read(spark, tmp_path, "g.csv", b"name,capacity\na,20\nb,5\nc,n/a\n", build_read_options(cfg)), cfg)
    assert _rows(df) == [["name", "capacity"], ["a", "20"]]


def test_apply_etl_tab_delimiter_from_load_file(spark, tmp_path):
    from giscovery_crawler.etl import apply_etl

    src = "場址編號(項次)\t點位編碼\n1\t1-1\n".encode("utf-8")
    cfg = {"load_file": {"delimiter": "\\t"}, "rename_columns": {"場址編號(項次)": "SiteID", "點位編碼": "PointID"}}
    df = apply_etl(_read(spark, tmp_path, "p.csv", src, build_read_options(cfg, "utf-8")), cfg)
    assert _rows(df) == [["SiteID", "PointID"], ["1", "1-1"]]


def test_quoted_newline_stays_in_one_row(spark, tmp_path):
    from giscovery_crawler.etl import apply_etl

    src = 'id,remarks\n1,"輪轂高度:64公尺\n試運轉中"\n2,ok\n'.encode("utf-8")
    cfg = {}
    df = apply_etl(_read(spark, tmp_path, "m.csv", src, build_read_options(cfg, "utf-8")), cfg)
    assert _rows(df) == [["id", "remarks"], ["1", "輪轂高度:64公尺\n試運轉中"], ["2", "ok"]]
