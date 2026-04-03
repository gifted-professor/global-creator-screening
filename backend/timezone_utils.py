from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def coerce_datetime_to_shanghai(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    try:
        parsed = pd.to_datetime(cleaned)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    if getattr(parsed, "tzinfo", None) is not None:
        return parsed.tz_convert(SHANGHAI_TZ)
    return parsed.tz_localize(SHANGHAI_TZ)


def format_shanghai_date(value: Any, fmt: str = "%Y/%m/%d") -> str:
    parsed = coerce_datetime_to_shanghai(value)
    if parsed is None:
        return ""
    return parsed.strftime(fmt)


def isoformat_shanghai_datetime(value: Any) -> str:
    parsed = coerce_datetime_to_shanghai(value)
    if parsed is None:
        return ""
    return parsed.isoformat()


def shanghai_day_start_ms(value: Any) -> int | None:
    parsed = coerce_datetime_to_shanghai(value)
    if parsed is None:
        return None
    dt = datetime(parsed.year, parsed.month, parsed.day, tzinfo=SHANGHAI_TZ)
    return int(dt.timestamp() * 1000)
