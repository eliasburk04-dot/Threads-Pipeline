from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, List, Union
from zoneinfo import ZoneInfo

from threads_github_bot.config import Settings
from threads_github_bot.models import ScheduleSlotPlan


def build_slot_plan(
    settings: Settings,
    local_date: Union[str, date],
    slot_name: str,
) -> ScheduleSlotPlan:
    tz = ZoneInfo(settings.runtime.timezone)
    slot_date = _coerce_date(local_date)
    base_time_value = settings.schedule.morning_time if slot_name == "morning" else settings.schedule.evening_time
    base_local = datetime.combine(slot_date, _parse_time(base_time_value), tz)
    jitter_minutes = _deterministic_jitter(settings, slot_date.isoformat(), slot_name)
    planned_local = base_local + timedelta(minutes=jitter_minutes)
    return ScheduleSlotPlan(
        slot_key="{0}:{1}".format(slot_date.isoformat(), slot_name),
        local_date=slot_date.isoformat(),
        slot_name=slot_name,
        base_local=base_local,
        planned_local=planned_local,
        planned_at_utc=planned_local.astimezone(timezone.utc),
        jitter_minutes=jitter_minutes,
    )


def plan_next_slots(settings: Settings, now: datetime, count: int = 4) -> List[ScheduleSlotPlan]:
    tz = ZoneInfo(settings.runtime.timezone)
    local_now = now.astimezone(tz)
    slots: List[ScheduleSlotPlan] = []
    cursor = local_now.date()
    while len(slots) < count:
        if cursor.weekday() in settings.schedule.allowed_weekdays:
            day_slots = [
                build_slot_plan(settings, cursor, "morning"),
                build_slot_plan(settings, cursor, "evening"),
            ]
            for slot in day_slots:
                if slot.planned_at_utc >= now:
                    slots.append(slot)
                    if len(slots) == count:
                        break
        cursor += timedelta(days=1)
    return sorted(slots, key=lambda slot: slot.planned_at_utc)


def slot_is_due(slot: ScheduleSlotPlan, now: datetime, settings: Settings) -> bool:
    grace = timedelta(minutes=max(1, settings.schedule.check_grace_minutes))
    return slot.planned_at_utc <= now < (slot.planned_at_utc + grace)


def iter_today_slots(settings: Settings, now: datetime) -> Iterable[ScheduleSlotPlan]:
    tz = ZoneInfo(settings.runtime.timezone)
    local_now = now.astimezone(tz)
    if local_now.date().weekday() not in settings.schedule.allowed_weekdays:
        return []
    return (
        build_slot_plan(settings, local_now.date(), "morning"),
        build_slot_plan(settings, local_now.date(), "evening"),
    )


def _deterministic_jitter(settings: Settings, local_date: str, slot_name: str) -> int:
    max_jitter = max(0, settings.schedule.jitter_minutes)
    if max_jitter == 0:
        return 0
    token = "{0}:{1}:{2}".format(settings.schedule.jitter_seed, local_date, slot_name)
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    number = int.from_bytes(digest[:8], "big")
    spread = (max_jitter * 2) + 1
    return (number % spread) - max_jitter


def _parse_time(value: str) -> time:
    hour_text, minute_text = value.split(":", 1)
    return time(hour=int(hour_text), minute=int(minute_text))


def _coerce_date(value: Union[str, date]) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)
