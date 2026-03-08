from datetime import datetime, timezone

from threads_github_bot.config import Settings
from threads_github_bot.scheduler import build_slot_plan, plan_next_slots, slot_is_due


def test_slot_plan_is_deterministic_and_within_jitter(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "APP_TIMEZONE": "Europe/Berlin",
            "SCHEDULE_MORNING_TIME": "08:30",
            "SCHEDULE_JITTER_MINUTES": "10",
            "SCHEDULE_JITTER_SEED": "fixed-seed",
        }
    )

    first = build_slot_plan(settings, local_date="2026-03-10", slot_name="morning")
    second = build_slot_plan(settings, local_date="2026-03-10", slot_name="morning")

    assert first.slot_key == "2026-03-10:morning"
    assert first.slot_key == second.slot_key
    assert first.jitter_minutes == second.jitter_minutes
    assert -10 <= first.jitter_minutes <= 10
    assert first.planned_local.hour == 8
    assert 20 <= first.planned_local.minute <= 40


def test_plan_next_slots_respects_weekday_filter_and_orders_results(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "APP_TIMEZONE": "Europe/Berlin",
            "SCHEDULE_ALLOWED_WEEKDAYS": "0,1,2,3,4",
            "SCHEDULE_MORNING_TIME": "08:30",
            "SCHEDULE_EVENING_TIME": "19:30",
        }
    )

    now = datetime(2026, 3, 6, 18, 0, tzinfo=timezone.utc)
    slots = plan_next_slots(settings, now=now, count=4)

    assert len(slots) == 4
    assert slots == sorted(slots, key=lambda slot: slot.planned_at_utc)
    assert all(slot.planned_local.weekday() < 5 for slot in slots)


def test_slot_is_due_only_inside_grace_window(tmp_path) -> None:
    settings = Settings.from_env(
        {
            "APP_BASE_DIR": str(tmp_path),
            "APP_TIMEZONE": "Europe/Berlin",
            "SCHEDULE_MORNING_TIME": "08:30",
            "SCHEDULE_JITTER_MINUTES": "0",
            "SCHEDULE_CHECK_GRACE_MINUTES": "6",
        }
    )
    slot = build_slot_plan(settings, local_date="2026-03-10", slot_name="morning")

    assert slot_is_due(slot, datetime(2026, 3, 10, 7, 31, tzinfo=timezone.utc), settings) is True
    assert slot_is_due(slot, datetime(2026, 3, 10, 7, 38, tzinfo=timezone.utc), settings) is False
