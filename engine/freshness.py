"""Watchman freshness contract (CONTRACT-v4 §1-2, MASTER-PLAN §C class 2).

stamp(coll, doc, at)      -> doc + as_of (newest fetch time inside it, else `_at`), ttl_s, stale_after
expect(docs, now, scope)  -> meta/health.missing[] {collection, expected, found, reason}
  live     newest live doc as_of no older than 2 h (2 x the hourly engine cadence)
  records  a Federal Register document dated every business day of the last 5 (weekends and
           federal holidays are expected gaps, never missing)
  series   latest observation within its own release cadence (`freq`)
Cadences come from the Sheet's Heartbeat tab (sheet/Heartbeat.json): every feed the engine reads
is polled by the hourly run, so a collection is due no later than the engine cadence; the Federal
Register row says "business days". `heartbeat_cadences()` exposes the parsed tab and
`check_ttl_table()` proves the §2 table is consistent with it.
Missing data is reported, never filled in. Staleness for display is computed on the page
(store.fresh) from as_of/ttl_s/stale_after; this module only writes those fields and the
expectations.
"""
import datetime as dt
import json
import os
import re

import clock

HERE = os.path.dirname(os.path.abspath(__file__))
HEARTBEAT = os.path.join(HERE, "..", "sheet", "Heartbeat.json")
ENGINE_CADENCE_S = clock.CADENCE_S["hourly"]

# CONTRACT-v4 §2 (frozen)
TTL_S = {**{c: 2 * ENGINE_CADENCE_S for c in ("live", "events", "wire", "news", "stories", "meta", "time")},
         **{c: 86400 for c in ("records", "series", "places", "brief", "clocks", "civic")},
         **{c: None for c in ("atlas", "essays", "gaps", "glossary", "img")}}
FETCH_KEYS = ("fetched", "fetched_at", "last_good", "finished", "tail_checked_at")
LIVE_MAX_AGE_S = 2 * 3600
FR_BUSINESS_DAYS = 5
FR_DAY_DONE_ET = (17, 0)          # regular public-inspection filings post at 16:15 ET; a day is judged after 17:00 ET
# a series' latest observation date may trail the clock by at most this many days (release lag + one period)
SERIES_MAX_LAG_D = {"daily": 7, "weekly": 16, "monthly": 75, "quarterly": 190, "annual": 550}
FR_HOST = "federalregister.gov"

_UNIT = {"s": 1, "sec": 1, "second": 1, "seconds": 1, "min": 60, "minute": 60, "minutes": 60, "h": 3600, "hour": 3600, "hours": 3600}


class UnknownCollection(KeyError):
    pass


def ttl_for(coll):
    if coll not in TTL_S:
        raise UnknownCollection(f"collection {coll!r} has no ttl_s in CONTRACT-v4 §2")
    return TTL_S[coll]


# ------------------------------------------------------------------ Heartbeat tab
def parse_cadence(text):
    """'60 s' -> 60, 'poll 5 min' -> 300, 'hourly' -> 3600, 'daily' -> 86400, 'business days' -> 'business_days',
    'hours' -> 'hours'; static / event / n/a / continuous -> None. Smallest number named wins ('1-5 s' -> 1)."""
    t = (text or "").lower()
    if "business day" in t:
        return "business_days"
    if t.startswith("hourly"):
        return 3600
    if t.startswith("daily"):
        return 86400
    if t.startswith("weekly"):
        return 7 * 86400
    m = re.search(r"(\d+)(?:\s*-\s*\d+)?\s*(seconds|second|sec|s|minutes|minute|min|hours|hour|h)\b", t)
    if m:
        return int(m.group(1)) * _UNIT[m.group(2)]
    if t.startswith("hours"):
        return "hours"
    return None


def heartbeat_cadences(path=HEARTBEAT):
    rows = json.load(open(path, encoding="utf-8"))
    head = rows[0]
    i_feed, i_cad, i_cls = head.index("feed"), head.index("cadence"), head.index("class")
    return {r[i_feed]: {"cadence": r[i_cad], "cadence_s": parse_cadence(r[i_cad]), "class": r[i_cls]} for r in rows[1:]}


def check_ttl_table(path=HEARTBEAT):
    """[] when §2 agrees with the Heartbeat tab: every numeric LIVE cadence is <= the engine cadence (so 2 x cadence
    covers one missed run), and the Federal Register feed is business-day."""
    bad = []
    hb = heartbeat_cadences(path)
    for feed, r in hb.items():
        if r["class"].startswith("LIVE") and isinstance(r["cadence_s"], int) and r["cadence_s"] > ENGINE_CADENCE_S and r["cadence_s"] != 86400:
            bad.append(f"{feed}: cadence {r['cadence']} slower than the engine's hourly run")
    fr = [r for f, r in hb.items() if f.startswith("Federal Register")]
    if not fr or fr[0]["cadence_s"] != "business_days":
        bad.append("Heartbeat: Federal Register row is not 'business days'")
    if TTL_S["live"] != 2 * ENGINE_CADENCE_S:
        bad.append("ttl_s.live is not 2 x engine cadence")
    return bad


# ------------------------------------------------------------------ as_of / ttl / stale_after
def max_fetched(obj):
    """Newest fetch time anywhere inside obj (keys in FETCH_KEYS, at any depth), or None."""
    best = None
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            for k, v in o.items():
                if k in FETCH_KEYS and isinstance(v, str):
                    try:
                        t = clock.parse(v)
                    except ValueError:
                        t = None
                    if t and (best is None or t > best):
                        best = t
                elif isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(o, list):
            stack.extend(x for x in o if isinstance(x, (dict, list)))
    return best


def stamp(coll, doc, at):
    """Return a copy of doc with as_of, ttl_s, stale_after (CONTRACT-v4 §1). `at` = write time (`_at`)."""
    a = max_fetched(doc) or clock.parse(at)
    ttl = ttl_for(coll)
    return {**doc, "as_of": clock.iso(a), "ttl_s": ttl, "stale_after": clock.iso(a + dt.timedelta(seconds=ttl)) if ttl is not None else None}


# ------------------------------------------------------------------ business days (US federal holidays, 5 U.S.C. 6103)
def _nth_weekday(y, m, wd, n):
    d = dt.date(y, m, 1)
    d += dt.timedelta(days=(wd - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def _last_weekday(y, m, wd):
    d = dt.date(y, m + 1, 1) - dt.timedelta(days=1) if m < 12 else dt.date(y, 12, 31)
    return d - dt.timedelta(days=(d.weekday() - wd) % 7)


def _observed(d):
    return d - dt.timedelta(days=1) if d.weekday() == 5 else (d + dt.timedelta(days=1) if d.weekday() == 6 else d)


def federal_holidays(y):
    fixed = [dt.date(y, 1, 1), dt.date(y, 6, 19), dt.date(y, 7, 4), dt.date(y, 11, 11), dt.date(y, 12, 25)]
    out = {_observed(d) for d in fixed}
    out |= {_nth_weekday(y, 1, 0, 3), _nth_weekday(y, 2, 0, 3), _last_weekday(y, 5, 0), _nth_weekday(y, 9, 0, 1),
            _nth_weekday(y, 10, 0, 2), _nth_weekday(y, 11, 3, 4)}
    if dt.date(y + 1, 1, 1).weekday() == 5:      # Jan 1 on a Saturday is observed Fri Dec 31
        out.add(dt.date(y, 12, 31))
    return out


def is_business_day(d):
    return d.weekday() < 5 and d not in federal_holidays(d.year)


def recent_business_days(now, n=FR_BUSINESS_DAYS):
    """The last n business days whose Federal Register day is complete at `now` (ET), newest first."""
    loc = now.astimezone(clock.ET)
    d = loc.date()
    if (loc.hour, loc.minute) < FR_DAY_DONE_ET:
        d -= dt.timedelta(days=1)
    out = []
    while len(out) < n:
        if is_business_day(d):
            out.append(d)
        d -= dt.timedelta(days=1)
    return out


# ------------------------------------------------------------------ expectations
def _items(doc):
    for k in ("items", "events", "wire", "breaking", "news", "clusters"):
        for it in doc.get(k) or []:
            if isinstance(it, dict):
                yield it


def _is_fr(it):
    return any(FR_HOST in str(s.get("host") or s.get("url") or "") for s in (it.get("sources") or []) if isinstance(s, dict))


def _age(s):
    s = int(s)
    return f"{s // 3600} h {s % 3600 // 60} min" if s >= 3600 else f"{s // 60} min"


def expect(docs, now, scope=None):
    """docs: {"collection/doc_id": data} (a run's output or a whole store view). scope: collections to judge
    (default: every collection that has at least one doc in `docs`, plus `live` and `records` when listed)."""
    by = {}
    for path, d in docs.items():
        by.setdefault(path.split("/", 1)[0], []).append((path, d))
    scope = set(scope) if scope is not None else set(by)
    missing = []
    if "live" in scope:
        live = sorted(((clock.parse(d.get("as_of") or d.get("_at")), p) for p, d in by.get("live", []) if d.get("as_of") or d.get("_at")), reverse=True)
        cut = now - dt.timedelta(seconds=LIVE_MAX_AGE_S)
        if not live:
            missing.append({"collection": "live", "expected": f"a live doc with as_of at or after {clock.iso(cut)}", "found": "no live doc", "reason": "no hourly run wrote a live doc"})
        elif live[0][0] < cut:
            missing.append({"collection": "live", "expected": f"a live doc with as_of at or after {clock.iso(cut)}", "found": f"{live[0][1]} as_of {clock.iso(live[0][0])} ({_age((now - live[0][0]).total_seconds())} old)",
                            "reason": "newest live doc is older than 2 h"})
    if "records" in scope:
        have = set()
        for _, d in by.get("records", []):
            for it in _items(d):
                if _is_fr(it):
                    for k in ("t", "published", "publication_date"):
                        t = it.get(k)
                        if t:
                            try:
                                have.add(clock.parse(t).astimezone(clock.ET).date() if len(str(t)) > 10 else dt.date.fromisoformat(t))
                            except ValueError:
                                pass
        for day in recent_business_days(now):
            if day not in have:
                missing.append({"collection": "records", "expected": f"a Federal Register document dated {day.isoformat()} ({day.strftime('%a')}, business day)",
                                "found": "none", "reason": "no Federal Register item in records/* is dated that business day"})
    if "series" in scope:
        for path, d in sorted(by.get("series", [])):
            freq = str(d.get("freq") or "").lower()
            lat = (d.get("latest") or {}).get("date")
            lim = SERIES_MAX_LAG_D.get(freq)
            if not lim:
                continue
            if not lat:
                missing.append({"collection": "series", "expected": f"{path}: a latest observation", "found": "none", "reason": "series doc has no latest value"})
                continue
            lag = (now.astimezone(clock.ET).date() - dt.date.fromisoformat(lat[:10])).days
            if lag > lim:
                missing.append({"collection": "series", "expected": f"{path}: a {freq} observation dated within {lim} days", "found": f"latest {lat[:10]} ({lag} days back)",
                                "reason": f"{freq} series is behind its release cadence"})
    return missing
