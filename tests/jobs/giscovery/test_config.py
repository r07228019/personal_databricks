import pytest

from giscovery_crawler.config import parse_config_json


def test_parse_drops_none_values():
    cfg = parse_config_json('{"task_name": "a", "file_encoding": null, "browser_headers": {"User-Agent": "ua"}}')
    assert cfg == {"task_name": "a", "browser_headers": {"User-Agent": "ua"}}


def test_parse_rejects_non_object():
    with pytest.raises(ValueError):
        parse_config_json("[1, 2]")
