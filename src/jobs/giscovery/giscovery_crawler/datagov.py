"""data.gov.tw 資料集搜尋 API（https://data.gov.tw/api/front/dataset/list）的共用查詢。

API 以 POST JSON 查詢，回傳 payload.search_result 陣列；每筆有 title、all_url（下載連結清單）、
dataset_resource_note（各連結的說明，以分號分隔）。原本各支 Lambda 各自實作，這裡集中成三種用法。
"""

import re
from datetime import date
from typing import Any

from . import http

_YEAR_PATTERN = re.compile(r"(\d+)年")


def search(api_url: str, clauses: list[dict[str, Any]], *, headers: dict[str, str] | None = None) -> list[dict]:
    """送出 {"bool": clauses} 查詢，回傳 search_result 陣列（可能為空）。"""
    res = http.post_json(api_url, {"bool": clauses}, headers=headers)
    return res.json()["payload"]["search_result"]


def first_url_by_title(api_url: str, keyword: str, *, headers: dict[str, str] | None = None) -> str:
    """用 title 關鍵字搜尋，取第一筆結果的第一個下載連結。"""
    results = search(api_url, [{"title": {"value": keyword}}], headers=headers)
    if not results:
        raise LookupError(f"data.gov.tw 找不到 title 含 {keyword!r} 的資料集")
    print("Dataset Title:", results[0]["title"])
    return results[0]["all_url"][0]


def newest_year_url_by_title(api_url: str, keyword: str, *, headers: dict[str, str] | None = None) -> str:
    """用 title 關鍵字搜尋，從第一筆結果的 dataset_resource_note 挑出年份最大的那個連結。"""
    results = search(api_url, [{"title": {"value": keyword}}], headers=headers)
    if not results:
        raise LookupError(f"data.gov.tw 找不到 title 含 {keyword!r} 的資料集")
    first = results[0]
    print("Dataset Title:", first["title"])
    idx, note = pick_newest_year(first["dataset_resource_note"].split(";"))
    print("Newest Data Title:", note)
    return first["all_url"][idx]


def pick_newest_year(notes: list[str]) -> tuple[int, str]:
    """回傳 (index, note)：note 裡「N年」數字最大的那一個；都沒有年份時取第一個。"""
    def year_of(note: str) -> int:
        m = _YEAR_PATTERN.search(note)
        return int(m.group(1)) if m else -1

    idx = max(range(len(notes)), key=lambda i: year_of(notes[i]))
    return idx, notes[idx]


def first_url_by_roc_year(
    api_url: str,
    keyword: str,
    *,
    keyword_field: str = "fulltext",
    headers: dict[str, str] | None = None,
    today: date | None = None,
    max_years_back: int = 10,
) -> str:
    """用「關鍵字 + 民國年」搜尋，從今年往回找到第一個有結果的年份，取其第一個下載連結。

    原本的 Lambda 是無上限的 while True 往回找；這裡加 max_years_back 避免 API 異常時無限迴圈。
    """
    roc_year = (today or date.today()).year - 1911
    for year in range(roc_year, roc_year - max_years_back, -1):
        results = search(
            api_url,
            [{keyword_field: {"value": keyword}}, {"title": {"value": str(year)}}],
            headers=headers,
        )
        if results:
            print("title:", results[0]["title"])
            return results[0]["all_url"][0]
    raise LookupError(f"data.gov.tw 往回 {max_years_back} 年都找不到 {keyword!r}（民國 {roc_year} 年起）")
