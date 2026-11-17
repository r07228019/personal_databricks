"""土壤液化潛勢：從 data.gov.tw 資料集頁面找出目錄檔 csv 連結，再逐筆下載各縣市的 GeoJSON。

目錄檔來源站（gsmma.gov.tw）偶爾會以 HTTP 200 回傳 WAF 挑戰頁而非 CSV，所以驗證欄位、不符就重試。
"""

import csv
import json
import re
import time

from bs4 import BeautifulSoup

from giscovery_crawler import http

REQUIRED_COLUMNS = ("資源網址", "檔案格式")
_LINK_TITLE_PATTERN = re.compile(r"(?=.*csv)(?=.*下載)", re.IGNORECASE)
_CITY_NAME_PATTERN = re.compile(r".*?(?=-)")


def fetch_catalog_csv(url, headers, *, retries=3, backoff=1.5, sleep=time.sleep):
    """下載目錄檔 CSV，回傳 (headers, rows)。以 utf-8-sig 解碼去掉 BOM。"""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            res = http.get(url, headers=headers, timeout=30)
        except http.FetchError as e:
            # 原本的 Lambda 不檢查 status code，只看欄位；這裡把非 200 也當成一次失敗重試
            last_err = f"{e} (attempt {attempt}/{retries})"
        else:
            reader = csv.reader(res.content.decode("utf-8-sig", errors="replace").splitlines())
            csv_headers = next(reader, [])
            if all(col in csv_headers for col in REQUIRED_COLUMNS):
                return csv_headers, list(reader)
            last_err = (
                f"未取得預期 CSV (attempt {attempt}/{retries}, "
                f"Content-Type={res.headers.get('Content-Type', '')!r}, headers={csv_headers[:5]})"
            )
        print(f"[warn] {last_err}, retry after {backoff}s")
        sleep(backoff)
    raise ValueError(f"無法取得有效的目錄檔 CSV: {last_err}")


def build_file_name(title: str, city_number: str) -> str:
    """liquefaction_<縣市代碼>_<縣市名>_<l|m|h>；屏東和恆春代碼相同，所以加上縣市名區分。"""
    city_name = next((s for s in _CITY_NAME_PATTERN.findall(title) if s), "")
    name = f"liquefaction_{city_number}_{city_name}"
    if "低潛勢" in title:
        return name + "_l"
    if "中潛勢" in title:
        return name + "_m"
    return name + "_h"


def collect_downloads(page_html: str, headers) -> list[dict]:
    soup = BeautifulSoup(page_html, "html.parser")
    downloads = []
    for link in soup.find_all("a", attrs={"title": _LINK_TITLE_PATTERN}):
        href = link.get("href")
        if not href:
            continue
        csv_headers, rows = fetch_catalog_csv(href, headers)
        src_idx = csv_headers.index("資源網址")
        fmt_idx = csv_headers.index("檔案格式")
        for row in rows:
            downloads.append(
                {
                    "filename": build_file_name(row[0], row[1]),
                    "source_url": row[src_idx],
                    "file_format": row[fmt_idx],
                }
            )
    return downloads


def main(sink, params):
    headers = params.get("browser_headers")
    page = http.get(params["download_url"], headers=headers, timeout=30)
    downloads = collect_downloads(page.text, headers)
    print(f"* {len(downloads)} file(s) to download")

    written = []
    for item in downloads:
        print("準備下載:", item["source_url"])
        res = http.get(item["source_url"], headers=headers, timeout=60)
        content = json.dumps(json.loads(res.text), ensure_ascii=False, indent=4)
        written.append(sink.write(f"{item['filename']}.{item['file_format']}", content))
    return written
