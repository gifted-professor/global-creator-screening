from __future__ import annotations

from calendar import monthrange
from datetime import date


DEFAULT_SYNC_LOOKBACK_MONTHS = 3


def subtract_calendar_months(value: date, months: int) -> date:
    if months < 0:
        raise ValueError("months 不能是负数。")

    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1

    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def default_sync_sent_since(*, today: date | None = None) -> date:
    return today or date.today()


def resolve_sync_sent_since(value: str | None, *, today: date | None = None) -> date:
    raw = str(value or "").strip()
    if raw:
        return date.fromisoformat(raw)
    return default_sync_sent_since(today=today)
