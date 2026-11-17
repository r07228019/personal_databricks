import json
from datetime import date
from pathlib import Path

import pytest

from cwa_temperature_map.params import RunParams
from cwa_temperature_map.source import build_dest_path, build_source_url

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "src" / "jobs" / "test" / "DailyCWATemperatureMap" / "config.json"


@pytest.fixture(scope="module")
def cfg():
    """使用 repo 裡實際的 config.json，確保 config 與程式碼一致。"""
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def test_source_url_zero_pads_hour(cfg):
    params = RunParams(date(2026, 11, 14), 8)
    assert build_source_url(cfg["source"], params) == "https://www.cwa.gov.tw/Data/temperature/2026-11-14_0800.GTP8w.jpg"


def test_source_url_default_hour(cfg):
    params = RunParams(date(2026, 11, 15), 13)
    assert build_source_url(cfg["source"], params) == "https://www.cwa.gov.tw/Data/temperature/2026-11-15_1300.GTP8w.jpg"


def test_dest_path(cfg):
    params = RunParams(date(2026, 11, 14), 8)
    assert build_dest_path(cfg["destination"], params) == (
        "/Volumes/test/cwa_data/data/temperature/temperature_map/202611140800.jpg"
    )


def test_dest_path_tolerates_slashes_in_sub_dir(cfg):
    dest_cfg = {**cfg["destination"], "sub_dir": "/temperature/temperature_map/"}
    params = RunParams(date(2026, 11, 15), 0)
    assert build_dest_path(dest_cfg, params) == (
        "/Volumes/test/cwa_data/data/temperature/temperature_map/202611150000.jpg"
    )


def test_config_has_defaults_used_by_notebook(cfg):
    """#1：預設值的唯一來源是 config 的 defaults。"""
    assert cfg["defaults"]["data_date"] == "today"
    assert cfg["defaults"]["data_hour"] == 13
