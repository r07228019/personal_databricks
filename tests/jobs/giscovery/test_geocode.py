from datetime import datetime, timezone

import pytest
import requests

from giscovery_crawler.geocode import (
    GeocodeError,
    GeocodeResult,
    GoogleGeocoder,
    geocode_many,
    month_start_utc,
    normalize_address,
    parse_google_response,
    plan_lookups,
)


@pytest.mark.parametrize("raw, expected", [("  臺北市  松山區\n長春路 ", "臺北市 松山區 長春路"), ("", None), ("   ", None), (None, None)])
def test_normalize_address(raw, expected):
    assert normalize_address(raw) == expected


def test_month_start_utc_uses_taipei_calendar():
    # 2026-11-30 17:00 UTC is already 2026-12-01 01:00 in Taipei
    assert month_start_utc(datetime(2026, 11, 30, 17, tzinfo=timezone.utc)) == datetime(2026, 11, 30, 16, tzinfo=timezone.utc)
    assert month_start_utc(datetime(2026, 11, 15, tzinfo=timezone.utc)) == datetime(2026, 10, 31, 16, tzinfo=timezone.utc)


def test_plan_lookups_skips_cached_and_respects_budget():
    todo, deferred = plan_lookups(["c", "a", "b", "a", None, "d"], cached=["b"], remaining=2)
    assert (todo, deferred) == (["a", "c"], 1)
    assert plan_lookups(["a"], [], remaining=-5) == ([], 1)


def test_parse_google_response_ok_and_zero_results():
    payload = {
        "status": "OK",
        "results": [{"formatted_address": "台灣台北市", "place_id": "p1", "geometry": {"location": {"lat": 25.05, "lng": 121.56}, "location_type": "ROOFTOP"}}],
    }
    assert parse_google_response("a", payload) == GeocodeResult("a", "OK", 121.56, 25.05, "台灣台北市", "ROOFTOP", "p1")
    assert parse_google_response("a", {"status": "ZERO_RESULTS", "results": []}) == GeocodeResult("a", "ZERO_RESULTS")


@pytest.mark.parametrize("status", ["REQUEST_DENIED", "OVER_QUERY_LIMIT", "OVER_DAILY_LIMIT"])
def test_parse_google_response_raises_on_account_errors(status):
    with pytest.raises(GeocodeError, match=status):
        parse_google_response("a", {"status": status, "error_message": "bad"})


class _FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code, self._payload = status_code, payload

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params, timeout):
        self.calls.append(params)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_google_geocoder_retries_and_hides_key(monkeypatch):
    monkeypatch.setattr("giscovery_crawler.geocode.time.sleep", lambda s: None)
    session = _FakeSession([
        requests.ConnectionError("https://maps.googleapis.com/...&key=SECRET"),
        _FakeResponse(200, {"status": "UNKNOWN_ERROR"}),
        _FakeResponse(200, {"status": "ZERO_RESULTS", "results": []}),
    ])
    geocoder = GoogleGeocoder("SECRET", session=session)
    assert geocoder.geocode("x").status == "ZERO_RESULTS"
    assert session.calls[0] == {"address": "x", "key": "SECRET", "language": "zh-TW", "region": "tw"}

    failing = GoogleGeocoder("SECRET", session=_FakeSession([requests.ConnectionError("key=SECRET")] * 3))
    with pytest.raises(GeocodeError) as e:
        failing.geocode("x")
    assert "SECRET" not in str(e.value)


def test_google_geocoder_requires_key():
    with pytest.raises(ValueError):
        GoogleGeocoder("")


def test_geocode_many_returns_partial_results_with_first_error():
    def fake(address):
        if address == "bad":
            raise GeocodeError("OVER_QUERY_LIMIT")
        return GeocodeResult(address, "OK", 121.0, 25.0)

    results, error = geocode_many(fake, ["a", "bad", "b"], workers=2)
    assert sorted(r.address for r in results) == ["a", "b"] and isinstance(error, GeocodeError)
    assert geocode_many(fake, []) == ([], None)


def test_over_query_limit_is_rate_limited_subclass():
    from giscovery_crawler.geocode import GeocodeRateLimited

    with pytest.raises(GeocodeRateLimited):
        parse_google_response("a", {"status": "OVER_QUERY_LIMIT"})
    with pytest.raises(GeocodeError) as e:
        parse_google_response("a", {"status": "OVER_DAILY_LIMIT"})
    assert not isinstance(e.value, GeocodeRateLimited)


def test_rate_limiter_spaces_calls():
    from giscovery_crawler.geocode import RateLimiter

    now, slept = [100.0], []
    limiter = RateLimiter(4, clock=lambda: now[0], sleep=slept.append)
    for _ in range(3):
        limiter.wait()
    assert slept == [0.25, 0.5]
    now[0] = 200.0  # idle for a while: no waiting
    limiter.wait()
    assert slept == [0.25, 0.5]
    with pytest.raises(ValueError):
        RateLimiter(0)


def _no_limit():
    from giscovery_crawler.geocode import RateLimiter

    return RateLimiter(1e9)


def test_google_geocoder_backs_off_on_rate_limit(monkeypatch):
    sleeps = []
    monkeypatch.setattr("giscovery_crawler.geocode.time.sleep", sleeps.append)
    session = _FakeSession([
        _FakeResponse(200, {"status": "OVER_QUERY_LIMIT", "error_message": "rate-limit"}),
        _FakeResponse(429),
        _FakeResponse(200, {"status": "ZERO_RESULTS", "results": []}),
    ])
    geocoder = GoogleGeocoder("k", session=session, rate_limiter=_no_limit())
    assert geocoder.geocode("x").status == "ZERO_RESULTS"
    assert sleeps == [2, 4] and geocoder.rate_limited == 2


def test_google_geocoder_gives_up_after_rate_limit_retries(monkeypatch):
    from giscovery_crawler.geocode import GeocodeRateLimited

    monkeypatch.setattr("giscovery_crawler.geocode.time.sleep", lambda s: None)
    session = _FakeSession([_FakeResponse(200, {"status": "OVER_QUERY_LIMIT"})] * 4)
    geocoder = GoogleGeocoder("k", session=session, rate_limiter=_no_limit(), rate_limit_retries=3)
    with pytest.raises(GeocodeRateLimited):
        geocoder.geocode("x")
    assert len(session.calls) == 4


def test_google_geocoder_does_not_retry_account_errors(monkeypatch):
    monkeypatch.setattr("giscovery_crawler.geocode.time.sleep", lambda s: None)
    session = _FakeSession([_FakeResponse(200, {"status": "REQUEST_DENIED"})])
    with pytest.raises(GeocodeError, match="REQUEST_DENIED"):
        GoogleGeocoder("k", session=session, rate_limiter=_no_limit()).geocode("x")
