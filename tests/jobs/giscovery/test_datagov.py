from datetime import date
from unittest.mock import MagicMock

import pytest

from giscovery_crawler import datagov


def _response(results):
    res = MagicMock()
    res.json.return_value = {"payload": {"search_result": results}}
    return res


def test_pick_newest_year():
    notes = ["108年資料", "110年資料", "109年資料"]
    assert datagov.pick_newest_year(notes) == (1, "110年資料")


def test_pick_newest_year_without_year_falls_back_to_first():
    assert datagov.pick_newest_year(["a", "b"]) == (0, "a")


def test_first_url_by_title(monkeypatch):
    post = MagicMock(return_value=_response([{"title": "t", "all_url": ["http://x/1", "http://x/2"]}]))
    monkeypatch.setattr(datagov.http, "post_json", post)
    assert datagov.first_url_by_title("http://api", "kw", headers={"User-Agent": "ua"}) == "http://x/1"
    post.assert_called_once_with("http://api", {"bool": [{"title": {"value": "kw"}}]}, headers={"User-Agent": "ua"})


def test_first_url_by_title_empty_raises(monkeypatch):
    monkeypatch.setattr(datagov.http, "post_json", MagicMock(return_value=_response([])))
    with pytest.raises(LookupError):
        datagov.first_url_by_title("http://api", "kw")


def test_newest_year_url_by_title(monkeypatch):
    results = [{"title": "t", "dataset_resource_note": "108年;110年;109年", "all_url": ["u108", "u110", "u109"]}]
    monkeypatch.setattr(datagov.http, "post_json", MagicMock(return_value=_response(results)))
    assert datagov.newest_year_url_by_title("http://api", "kw") == "u110"


def test_first_url_by_roc_year_falls_back_to_previous_year(monkeypatch):
    calls = []

    def fake_post(url, payload, headers=None):
        calls.append(payload)
        year = payload["bool"][1]["title"]["value"]
        return _response([{"title": f"{year} data", "all_url": [f"http://x/{year}"]}] if year == "113" else [])

    monkeypatch.setattr(datagov.http, "post_json", fake_post)
    url = datagov.first_url_by_roc_year("http://api", "kw", keyword_field="fulltext", today=date(2026, 11, 17))
    assert url == "http://x/113"
    assert [c["bool"][1]["title"]["value"] for c in calls] == ["115", "114", "113"]
    assert calls[0]["bool"][0] == {"fulltext": {"value": "kw"}}


def test_first_url_by_roc_year_gives_up(monkeypatch):
    monkeypatch.setattr(datagov.http, "post_json", MagicMock(return_value=_response([])))
    with pytest.raises(LookupError):
        datagov.first_url_by_roc_year("http://api", "kw", today=date(2026, 1, 1), max_years_back=3)
