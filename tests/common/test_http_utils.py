import pytest
import requests

from common.http_utils import DownloadError, download_bytes


class FakeResponse:
    def __init__(self, status_code=200, content=b"img", content_type="image/jpeg"):
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Type": content_type}


class FakeSession:
    """依序回傳預先準備的結果；元素是 Exception 時改為拋出。"""

    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class SleepRecorder:
    def __init__(self):
        self.waits = []

    def __call__(self, seconds):
        self.waits.append(seconds)


def _download(session, **kwargs):
    sleep = SleepRecorder()
    params = {"max_attempts": 3, "retry_wait_seconds": 5, "timeout_seconds": 7, "session": session, "sleep": sleep}
    params.update(kwargs)
    return download_bytes("https://example.com/a.jpg", **params), sleep


def test_success_on_first_attempt():
    session = FakeSession(FakeResponse(content=b"jpeg"))
    content, sleep = _download(session, expected_content_type="image/jpeg")
    assert content == b"jpeg"
    assert session.calls == [("https://example.com/a.jpg", 7)]
    assert sleep.waits == []


def test_retries_then_succeeds():
    session = FakeSession(FakeResponse(status_code=404, content=b"x"), FakeResponse(content=b"jpeg"))
    content, sleep = _download(session, expected_content_type="image/jpeg")
    assert content == b"jpeg"
    assert len(session.calls) == 2
    assert sleep.waits == [5]


def test_request_exception_is_retried():
    session = FakeSession(requests.ConnectionError("boom"), FakeResponse(content=b"jpeg"))
    content, _ = _download(session)
    assert content == b"jpeg"


def test_raises_after_max_attempts():
    session = FakeSession(*[FakeResponse(status_code=500) for _ in range(3)])
    with pytest.raises(DownloadError, match="HTTP 500"):
        _download(session)
    assert len(session.calls) == 3


def test_sleep_count_is_attempts_minus_one():
    session = FakeSession(*[FakeResponse(status_code=500) for _ in range(3)])
    sleep = SleepRecorder()
    with pytest.raises(DownloadError):
        download_bytes("u", max_attempts=3, retry_wait_seconds=5, session=session, sleep=sleep)
    assert sleep.waits == [5, 5]


def test_content_type_mismatch_is_failure():
    session = FakeSession(*[FakeResponse(content=b"<html>", content_type="text/html") for _ in range(2)])
    with pytest.raises(DownloadError, match="text/html"):
        _download(session, max_attempts=2, expected_content_type="image/jpeg")


def test_content_type_prefix_match():
    session = FakeSession(FakeResponse(content_type="image/jpeg; charset=binary"))
    content, _ = _download(session, expected_content_type="image/jpeg")
    assert content == b"img"


def test_empty_content_is_failure():
    session = FakeSession(FakeResponse(content=b""))
    with pytest.raises(DownloadError):
        _download(session, max_attempts=1)


def test_no_expected_content_type_accepts_anything():
    session = FakeSession(FakeResponse(content_type="application/octet-stream"))
    content, _ = _download(session, expected_content_type=None)
    assert content == b"img"
