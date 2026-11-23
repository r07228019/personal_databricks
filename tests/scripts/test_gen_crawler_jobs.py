import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_crawler_jobs.py"


@pytest.fixture(scope="module")
def gen():
    spec = importlib.util.spec_from_file_location("gen_crawler_jobs", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("to_etl", ["TRUE", "false", None])
def test_every_job_triggers_crawler_etl(gen, to_etl):
    task = {"task_name": "a", "category": "geo"}
    if to_etl is not None:
        task["to_etl"] = to_etl
    out = gen.render_job(task)
    assert "run_job_task:" in out and "job_id: ${resources.jobs.crawlerEtl.id}" in out and "task_name: a" in out
    assert "depends_on:\n            - task_key: crawl" in out


def test_paused_task_reason_in_description(gen):
    out = gen.render_job({"task_name": "CWB", "category": "geo", "schedule": "0 16 1 * ? *"})
    assert "pause_status: PAUSED" in out and "【排程暫停】" in out


def test_geo_task_only_when_enabled(gen):
    task = {"task_name": "a", "category": "geo"}
    assert "task_key: geo" not in gen.render_job(task)
    out = gen.render_job(task, geo_enabled=True)
    assert "- task_key: geo\n          depends_on:\n            - task_key: etl" in out
    assert "job_id: ${resources.jobs.geoNormalize.id}" in out


def test_crawler_environment_is_version_3(gen):
    out = gen.render_job({"task_name": "a", "category": "geo"})
    assert 'environment_version: "3"' in out and "client:" not in out
