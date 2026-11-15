"""帶重試的 HTTP 下載。"""

import time
from collections.abc import Callable

import requests


class DownloadError(RuntimeError):
    """重試用盡仍無法取得預期內容。"""


def download_bytes(
    url: str,
    *,
    max_attempts: int = 3,
    retry_wait_seconds: float = 30,
    timeout_seconds: float = 60,
    expected_content_type: str | None = None,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """下載 url 並回傳內容。

    非 200、內容為空、或 Content-Type 不符 expected_content_type（前綴比對）時視為失敗並重試；
    重試用盡後拋出 DownloadError。session / sleep 可替換，方便測試。
    """
    http = session or requests
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = http.get(url, timeout=timeout_seconds)
            content_type = resp.headers.get("Content-Type", "")
            type_ok = expected_content_type is None or content_type.startswith(expected_content_type)
            if resp.status_code == 200 and type_ok and resp.content:
                return resp.content
            last_error = f"HTTP {resp.status_code}, Content-Type={content_type!r}, size={len(resp.content)}"
        except requests.RequestException as e:
            last_error = repr(e)
        print(f"attempt {attempt}/{max_attempts} failed: {last_error}")
        if attempt < max_attempts:
            sleep(retry_wait_seconds)
    raise DownloadError(f"下載失敗：{url}（{last_error}）")
