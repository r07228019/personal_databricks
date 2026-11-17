from unittest.mock import MagicMock

import pytest

from giscovery_crawler import runner

soil = runner.load_crawler_module("soilLiquefaction")


@pytest.mark.parametrize(
    "title, number, expected",
    [
        ("臺北市-低潛勢", "63", "liquefaction_63_臺北市_l"),
        ("屏東縣-中潛勢", "10013", "liquefaction_10013_屏東縣_m"),
        ("恆春-高潛勢", "10013", "liquefaction_10013_恆春_h"),
    ],
)
def test_build_file_name(title, number, expected):
    assert soil.build_file_name(title, number) == expected


def _res(content: bytes, content_type="text/csv"):
    res = MagicMock()
    res.content = content
    res.headers = {"Content-Type": content_type}
    return res


def test_fetch_catalog_csv_retries_on_waf_page(monkeypatch):
    good = "﻿名稱,縣市代碼,資源網址,檔案格式\n臺北市-低潛勢,63,http://x/63.json,json\n".encode()
    responses = [_res(b"<html>challenge</html>", "text/html"), _res(good)]
    monkeypatch.setattr(soil.http, "get", MagicMock(side_effect=responses))
    sleeps = []
    headers, rows = soil.fetch_catalog_csv("http://catalog", {"User-Agent": "ua"}, sleep=sleeps.append)
    assert headers == ["名稱", "縣市代碼", "資源網址", "檔案格式"]
    assert rows == [["臺北市-低潛勢", "63", "http://x/63.json", "json"]]
    assert sleeps == [1.5]


def test_fetch_catalog_csv_retries_on_http_error(monkeypatch):
    good = "名稱,縣市代碼,資源網址,檔案格式\n".encode()
    monkeypatch.setattr(soil.http, "get", MagicMock(side_effect=[soil.http.FetchError("403"), _res(good)]))
    headers, rows = soil.fetch_catalog_csv("http://catalog", None, sleep=lambda _: None)
    assert headers[0] == "名稱" and rows == []


def test_fetch_catalog_csv_gives_up(monkeypatch):
    monkeypatch.setattr(soil.http, "get", MagicMock(return_value=_res(b"<html></html>", "text/html")))
    with pytest.raises(ValueError):
        soil.fetch_catalog_csv("http://catalog", None, retries=2, sleep=lambda _: None)
