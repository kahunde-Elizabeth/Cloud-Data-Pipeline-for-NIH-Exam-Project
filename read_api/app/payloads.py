"""Turn coverage rows into the JSON the cache holds. Keep identical to aggregator/app/payloads.py."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime


def iso_week(day: date) -> str:
    year, week, _ = day.isocalendar()
    return f"{year}-W{week:02d}"


def by_iso_week(day_rows):
    """(district, vaccine, dose, day, doses) -> (district, vaccine, dose, iso_week, doses)."""
    totals: dict[tuple, int] = defaultdict(int)
    for district_id, vaccine, dose, day, doses in day_rows:
        totals[(district_id, vaccine, int(dose), iso_week(day))] += int(doses)
    return [(*key, n) for key, n in sorted(totals.items())]


def build_payloads(rows, now: datetime):
    """rows: (district_id, vaccine_code, dose_no, iso_week, doses).

    Returns (per-district payloads, national payload, national doses by (vaccine, dose)).
    National data holds totals only per district and week, never child-level data.
    """
    stamp = now.isoformat()
    per_district: dict[str, list] = defaultdict(list)
    district_week: dict[tuple, int] = defaultdict(int)
    by_vaccine: dict[tuple, int] = defaultdict(int)
    for district_id, vaccine, dose, week, doses in rows:
        doses = int(doses)
        per_district[district_id].append(
            {"vaccine": vaccine, "dose": int(dose), "week": week, "doses": doses})
        district_week[(district_id, week)] += doses
        by_vaccine[(vaccine, int(dose))] += doses

    districts = {}
    for district_id, items in per_district.items():
        items.sort(key=lambda r: (r["week"], r["vaccine"], r["dose"]))
        districts[district_id] = {"district_id": district_id, "updated_at": stamp,
                                  "total_doses": sum(r["doses"] for r in items), "rows": items}
    national = {
        "updated_at": stamp,
        "total_doses": sum(by_vaccine.values()),
        "rows": [{"district_id": d, "week": w, "doses": n}
                 for (d, w), n in sorted(district_week.items())],
    }
    return districts, national, dict(by_vaccine)
