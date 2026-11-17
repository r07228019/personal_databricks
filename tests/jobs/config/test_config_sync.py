import json
from pathlib import Path

import pytest

from config_sync import TABLE_FILES, load_jsonl_rows

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "src" / "jobs" / "config" / "data"


@pytest.mark.parametrize("file_name", list(TABLE_FILES.values()))
def test_repo_jsonl_files_are_loadable(file_name):
    rows = load_jsonl_rows(str(DATA_DIR / file_name))
    assert rows
    for r in rows:
        assert json.loads(r.config_json)["task_name"] == r.task_name


def test_crawler_config_has_13_tasks():
    rows = load_jsonl_rows(str(DATA_DIR / TABLE_FILES["crawler_config"]))
    assert len(rows) == 13


def test_skips_blank_lines_and_keeps_unicode(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"task_name": "a", "kw": "中文"}\n\n{"task_name": "b"}\n', encoding="utf-8")
    rows = load_jsonl_rows(str(p))
    assert [r.task_name for r in rows] == ["a", "b"]
    assert "中文" in rows[0].config_json


@pytest.mark.parametrize("content", ['{"x": 1}\n', '{"task_name": "a"}\n{"task_name": "a"}\n', '{"task_name": ""}\n'])
def test_rejects_missing_or_duplicate_task_name(tmp_path, content):
    p = tmp_path / "x.jsonl"
    p.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        load_jsonl_rows(str(p))
