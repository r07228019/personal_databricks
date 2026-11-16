"""data_date / data_hour 的解析與檢查。"""

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, tzinfo

TODAY_KEYWORD = "today"


class ParamValidationError(ValueError):
    """使用者輸入的參數不合法；errors 為所有錯誤訊息。"""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("參數檢查失敗：\n- " + "\n- ".join(errors))


@dataclass(frozen=True)
class RunParams:
    data_date: date
    data_hour: int

    def data_datetime(self, tz: tzinfo) -> datetime:
        return datetime.combine(self.data_date, time(hour=self.data_hour), tzinfo=tz)


def parse_and_validate(
    raw_date: str,
    raw_hour: str,
    *,
    now: datetime,
    default_date: str,
    default_hour: int | str,
    max_lookback_hours: float,
) -> RunParams:
    """解析並檢查參數，所有錯誤一次收集後以 ParamValidationError 拋出。

    - raw_date：yyyy-MM-dd 或 today；空白 = default_date（來自 config 的 defaults.data_date）
    - raw_hour：0～23 的整數；空白 = default_hour（來自 config 的 defaults.data_hour）
    - 預設值與使用者輸入走同一套檢查，config 的預設值寫錯一樣會被擋下
    - 資料時間不可晚於 now，也不可早於 now - max_lookback_hours
    now 必須帶時區（tz-aware），資料時間以 now 的時區解讀。
    """
    if now.tzinfo is None:
        raise ValueError("now 必須帶時區（tz-aware）")

    errors: list[str] = []
    data_date = _parse_date(_or_default(raw_date, default_date), now, errors)
    data_hour = _parse_hour(_or_default(raw_hour, default_hour), errors)

    if data_date is not None and data_hour is not None:
        data_dt = datetime.combine(data_date, time(hour=data_hour), tzinfo=now.tzinfo)
        if data_dt > now:
            errors.append(f"資料時間 {data_dt:%Y-%m-%d %H:00} 晚於現在（{now:%Y-%m-%d %H:%M} {now.tzinfo}），圖還沒產出")
        elif now - data_dt > timedelta(hours=max_lookback_hours):
            errors.append(
                f"資料時間 {data_dt:%Y-%m-%d %H:00} 早於 {max_lookback_hours:g} 小時前，"
                "超出 CWA 保留範圍（可調整 config 的 validation.max_lookback_hours）"
            )

    if errors:
        raise ParamValidationError(errors)
    return RunParams(data_date=data_date, data_hour=data_hour)


def _or_default(raw: str | None, default: int | str) -> str:
    """使用者輸入為空白時改用預設值（統一轉成字串，交給後續同一套解析）。"""
    s = (raw or "").strip()
    return s if s else str(default).strip()


def _parse_date(raw: str, now: datetime, errors: list[str]) -> date | None:
    s = raw.strip()
    if s.lower() == TODAY_KEYWORD:
        return now.date()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        errors.append(f"data_date={raw!r} 格式錯誤，應為 yyyy-MM-dd（例如 2026-11-15）或 {TODAY_KEYWORD}")
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        errors.append(f"data_date={raw!r} 不是存在的日期")
        return None


def _parse_hour(raw: str, errors: list[str]) -> int | None:
    s = raw.strip()
    if not re.fullmatch(r"\d{1,2}", s) or not 0 <= int(s) <= 23:
        errors.append(f"data_hour={raw!r} 不合法，應為 0～23 的整數（例如 13）")
        return None
    return int(s)
