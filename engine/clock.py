"""Watchman engine clock (CONTRACT-v4 §1, MASTER-PLAN §C class 1).

This is the ONLY engine module that reads the wall clock. Every other module asks `clock.now()`.
A run can be frozen (tests, fixtures, byte-stable reruns) with `freeze(t)` or the env var
WATCHMAN_NOW=<ISO UTC>; a frozen clock never advances, so two runs over the same replay cache
write identical batches.

  Clock(run_started, data_as_of, next_due, tz)   one value object per run (MASTER-PLAN §D)
"""
import datetime as dt
import os
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
TZ = ZoneInfo("America/Indiana/Indianapolis")    # engine local day (same offset as ET)
ET = ZoneInfo("America/New_York")                 # page display zone (time.js fmt)
HOURLY_MINUTE = 51                                # operator, 2026-10-31 22:08 ET: hourly run at :51
MORNING_AT = (8, 51)
CADENCE_S = {"hourly": 3600, "morning": 86400}

_frozen = None


def parse(s):
    """ISO string (Z or offset) -> aware UTC datetime; None passes through."""
    if s is None or s == "":
        return None
    if isinstance(s, dt.datetime):
        return (s if s.tzinfo else s.replace(tzinfo=UTC)).astimezone(UTC)
    s = str(s).strip()
    if len(s) == 10:
        return dt.datetime.fromisoformat(s).replace(tzinfo=UTC)
    d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(UTC)


def iso(d):
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def freeze(t):
    """Freeze the clock at t (datetime or ISO). freeze(None) unfreezes."""
    global _frozen
    _frozen = parse(t).replace(microsecond=0) if t is not None else None


def frozen():
    return _frozen is not None


def now():
    """Aware UTC datetime, whole seconds. The only wall-clock read in the engine."""
    if _frozen is not None:
        return _frozen
    return dt.datetime.now(UTC).replace(microsecond=0)


def next_due(mode, ref):
    """Next scheduled run after `ref`: hourly at :51 local, morning at 08:51 local."""
    loc = ref.astimezone(TZ)
    if mode == "hourly":
        n = loc.replace(minute=HOURLY_MINUTE, second=0, microsecond=0)
        if n <= loc:
            n += dt.timedelta(hours=1)
    else:
        n = loc.replace(hour=MORNING_AT[0], minute=MORNING_AT[1], second=0, microsecond=0)
        if n <= loc:
            n += dt.timedelta(days=1)
    return iso(n)


class Clock:
    """Run clock: when the run started, the newest fetch inside its data, when the next run is due."""
    __slots__ = ("run_started", "data_as_of", "next_due", "tz", "mode")

    def __init__(self, run_started, data_as_of=None, next_due=None, tz=TZ, mode="morning"):
        self.run_started = parse(run_started)
        self.data_as_of = parse(data_as_of)
        self.next_due = parse(next_due)
        self.tz, self.mode = tz, mode

    @property
    def cadence_s(self):
        return CADENCE_S.get(self.mode, 86400)

    def to_dict(self):
        return {"started": iso(self.run_started), "as_of": iso(self.data_as_of), "next_due": iso(self.next_due), "cadence_s": self.cadence_s}


if os.environ.get("WATCHMAN_NOW"):
    freeze(os.environ["WATCHMAN_NOW"])
