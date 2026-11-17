from pathlib import Path

import pytest

from giscovery_crawler import runner

REPO_ROOT = Path(__file__).resolve().parents[3]
CRAWLERS_DIR = REPO_ROOT / "src" / "jobs" / "giscovery" / "crawlers"
JSONL_PATH = REPO_ROOT / "src" / "jobs" / "config" / "data" / "cfh-gis20-ddb-crawler-config.jsonl"


def _task_names():
    import json

    return [json.loads(line)["task_name"] for line in JSONL_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.mark.parametrize("task_name", _task_names())
def test_every_configured_task_has_a_crawler_with_main(task_name):
    """jsonl 裡的每個 task 都要有對應的 crawlers/<task_name>.py，且定義 main()。"""
    module = runner.load_crawler_module(task_name)
    assert callable(module.main)


def test_crawlers_dir_matches_jsonl():
    files = {p.stem for p in CRAWLERS_DIR.glob("*.py") if p.stem != "__init__"}
    assert files == set(_task_names())


def test_run_crawler_passes_sink_and_params(tmp_path):
    (tmp_path / "demo.py").write_text("def main(sink, params):\n    return [sink, params['x']]\n", encoding="utf-8")
    out = runner.run_crawler("demo", {"task_name": "demo", "x": 1}, sink="S", crawlers_dir=str(tmp_path))
    assert out == ["S", 1]


def test_run_crawler_passes_secrets_when_requested(tmp_path):
    (tmp_path / "demo.py").write_text(
        "def main(sink, params, secrets):\n    return secrets('scope', 'key')\n", encoding="utf-8"
    )
    out = runner.run_crawler("demo", {"task_name": "demo"}, sink=None, secrets=lambda s, k: f"{s}/{k}", crawlers_dir=str(tmp_path))
    assert out == "scope/key"
    with pytest.raises(ValueError):
        runner.run_crawler("demo", {"task_name": "demo"}, sink=None, crawlers_dir=str(tmp_path))


def test_run_crawler_rejects_mismatched_task_name():
    with pytest.raises(ValueError):
        runner.run_crawler("cathayATM", {"task_name": "other"}, sink=None)


def test_missing_crawler(tmp_path):
    with pytest.raises(FileNotFoundError):
        runner.load_crawler_module("nope", crawlers_dir=str(tmp_path))
