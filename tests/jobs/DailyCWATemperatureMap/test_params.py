from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from cwa_temperature_map.params import ParamValidationError, RunParams, parse_and_validate

TZ = ZoneInfo("Asia/Taipei")
NOW = datetime(2026, 11, 15, 20, 50, tzinfo=TZ)


def _parse(raw_date="", raw_hour="", *, now=NOW, default_date="today", default_hour=13, max_lookback_hours=120):
    return parse_and_validate(
        raw_date,
        raw_hour,
        now=now,
        default_date=default_date,
        default_hour=default_hour,
        max_lookback_hours=max_lookback_hours,
    )


def _errors(**kwargs) -> list[str]:
    with pytest.raises(ParamValidationError) as exc_info:
        _parse(**kwargs)
    return exc_info.value.errors


class TestDefaults:
    def test_blank_uses_config_defaults(self):
        assert _parse("", "") == RunParams(date(2026, 11, 15), 13)

    def test_whitespace_counts_as_blank(self):
        assert _parse("  ", "  ") == RunParams(date(2026, 11, 15), 13)

    def test_none_counts_as_blank(self):
        assert _parse(None, None) == RunParams(date(2026, 11, 15), 13)

    def test_changing_config_default_hour_takes_effect(self):
        """#1 的回歸測試：config 的預設值必須真的生效。"""
        assert _parse("", "", default_hour=8).data_hour == 8

    def test_config_default_hour_as_string(self):
        assert _parse("", "", default_hour="9").data_hour == 9

    def test_config_default_date_can_be_explicit_date(self):
        assert _parse("", "", default_date="2026-11-14").data_date == date(2026, 11, 14)

    def test_invalid_config_default_is_rejected(self):
        errors = _errors(raw_date="", raw_hour="", default_hour=25)
        assert any("data_hour='25'" in e for e in errors)

    def test_user_input_overrides_defaults(self):
        assert _parse("2026-11-14", "8", default_date="today", default_hour=13) == RunParams(date(2026, 11, 14), 8)


class TestDate:
    @pytest.mark.parametrize("raw", ["today", "TODAY", " Today "])
    def test_today_keyword_case_insensitive(self, raw):
        assert _parse(raw, "13").data_date == date(2026, 11, 15)

    @pytest.mark.parametrize("raw", ["2026/11/14", "20261114", "2026-11-1", "abc"])
    def test_bad_format(self, raw):
        assert any("格式錯誤" in e for e in _errors(raw_date=raw, raw_hour="13"))

    def test_nonexistent_date(self):
        assert any("不是存在的日期" in e for e in _errors(raw_date="2026-02-30", raw_hour="13"))


class TestHour:
    @pytest.mark.parametrize("raw, expected", [("0", 0), ("8", 8), ("08", 8), ("13", 13)])
    def test_valid(self, raw, expected):
        assert _parse("2026-11-14", raw).data_hour == expected

    @pytest.mark.parametrize("raw", ["24", "-1", "13:00", "1300", "abc", "1.5"])
    def test_invalid(self, raw):
        assert any("data_hour=" in e for e in _errors(raw_date="2026-11-14", raw_hour=raw))


class TestTimeRange:
    def test_future_is_rejected(self):
        assert any("晚於現在" in e for e in _errors(raw_date="today", raw_hour="21"))

    def test_current_hour_is_allowed(self):
        assert _parse("today", "20").data_hour == 20

    def test_too_old_is_rejected(self):
        assert any("早於 120 小時前" in e for e in _errors(raw_date="2026-11-10", raw_hour="13"))

    def test_exactly_at_lookback_boundary_is_allowed(self):
        assert _parse("2026-11-10", "20", now=datetime(2026, 11, 15, 20, 0, tzinfo=TZ)).data_hour == 20

    def test_one_hour_past_lookback_boundary_is_rejected(self):
        errors = _errors(raw_date="2026-11-10", raw_hour="19", now=datetime(2026, 11, 15, 20, 0, tzinfo=TZ))
        assert any("早於" in e for e in errors)


class TestErrorReporting:
    def test_collects_all_errors(self):
        errors = _errors(raw_date="2026-02-30", raw_hour="25")
        assert len(errors) == 2

    def test_error_message_lists_each_error(self):
        with pytest.raises(ParamValidationError) as exc_info:
            _parse("2026-02-30", "25")
        assert "- data_date=" in str(exc_info.value)
        assert "- data_hour=" in str(exc_info.value)

    def test_naive_now_is_programming_error(self):
        with pytest.raises(ValueError, match="tz-aware"):
            _parse("today", "13", now=datetime(2026, 11, 15, 20, 50))
