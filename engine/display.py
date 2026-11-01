"""Watchman display strings, one place (CONTRACT-v4 §4, MASTER-PLAN §C class 9).

display_when(when, freq|kind, ref=None) -> the date string a page prints, at the grain of the data:
  daily / item       "Oct 29"            ("Oct 29, 2025" when not in the reference year)
  weekly (Monday)    "week of Oct 26"    EIA-style weeks are dated by their first day
  weekly (other day) "week ending Oct 24" FRED weeks (ICSA ends Sat, MORTGAGE30US ends Thu)
  monthly            "September 2026"
  quarterly          "Q3 2026"
  annual             "2025"
Pages print this string; they never format a date grain themselves. Item times are shown in ET.
display_value(v) -> "159,044", "4.269", "4.1" (never "159044.0").
"""
import datetime as dt

import clock

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
FREQ_ALIASES = {"d": "daily", "day": "daily", "daily": "daily", "w": "weekly", "week": "weekly", "weekly": "weekly",
                "m": "monthly", "month": "monthly", "monthly": "monthly", "q": "quarterly", "quarter": "quarterly", "quarterly": "quarterly",
                "a": "annual", "y": "annual", "year": "annual", "yearly": "annual", "annual": "annual"}


def _day(d, ref_year):
    s = f"{d.strftime('%b')} {d.day}"
    return s if ref_year is None or d.year == ref_year else f"{s}, {d.year}"


def _as_date(when):
    """Observation date (YYYY-MM-DD) stays a calendar date; a timestamp becomes its ET calendar date."""
    if isinstance(when, dt.date) and not isinstance(when, dt.datetime):
        return when
    if isinstance(when, str) and len(when) == 10:
        return dt.date.fromisoformat(when)
    t = clock.parse(when)
    return t.astimezone(clock.ET).date() if t else None


def display_when(when, freq=None, ref=None):
    """`freq` is a series frequency or an item kind (anything not a frequency is shown as a day)."""
    d = _as_date(when)
    if d is None:
        return None
    ref_year = (_as_date(ref) if ref is not None else None)
    ref_year = ref_year.year if ref_year else None
    f = FREQ_ALIASES.get(str(freq or "").lower(), "daily")
    if f == "weekly":
        return ("week of " if d.weekday() == 0 else "week ending ") + _day(d, ref_year)
    if f == "monthly":
        return f"{MONTHS[d.month - 1]} {d.year}"
    if f == "quarterly":
        return f"Q{(d.month - 1) // 3 + 1} {d.year}"
    if f == "annual":
        return str(d.year)
    return _day(d, ref_year)


def display_value(v, unit=""):
    """R20: display-ready value ('159,044', '4.269', '4.1'); never '159044.0'."""
    if not isinstance(v, (int, float)):
        return str(v)
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.3f}".rstrip("0").rstrip(".") if abs(v) < 1000 else f"{v:,.1f}"
