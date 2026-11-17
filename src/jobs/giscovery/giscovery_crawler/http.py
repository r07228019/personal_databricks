"""爬蟲用的 HTTP 小工具：統一加 timeout、非 200 直接拋錯。"""

from typing import Any

import requests

DEFAULT_TIMEOUT = 60


class FetchError(RuntimeError):
    """HTTP 回應不是 200。"""


def get(url: str, *, headers: dict[str, str] | None = None, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
    res = requests.get(url, headers=headers, timeout=timeout)
    if res.status_code != 200:
        raise FetchError(f"GET {url} -> HTTP {res.status_code}")
    return res


def post_json(
    url: str, payload: Any, *, headers: dict[str, str] | None = None, timeout: float = DEFAULT_TIMEOUT
) -> requests.Response:
    res = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if res.status_code != 200:
        raise FetchError(f"POST {url} -> HTTP {res.status_code}")
    return res
