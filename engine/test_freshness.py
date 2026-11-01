#!/usr/bin/env python3
"""P1 freshness tests (stdlib; `python3 test_freshness.py` or pytest). CONTRACT-v4 §1-2, MASTER-PLAN §C class 2."""
import datetime as dt, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import clock, freshness, gates

NOW = clock.parse("2026-10-30T22:00:00Z")       # Fri 18:00 ET


def test_as_of_is_max_fetched():
    doc = {"items": [{"sources": [{"fetched": "2026-10-30T20:01:00Z"}, {"fetched": "2026-10-30T21:15:30Z"}]},
                     {"sources": [{"fetched": "2026-10-30T19:00:00Z"}]}],
           "fetched_at": "2026-10-30T18:00:00Z", "t": "2026-10-30T21:59:00Z", "_at": "2026-10-30T21:58:00Z"}
    d = freshness.stamp("events", doc, "2026-10-30T21:58:00Z")
    assert d["as_of"] == "2026-10-30T21:15:30Z", d["as_of"]           # newest fetch, never the write time or an event time
    assert d["ttl_s"] == 7200 and d["stale_after"] == "2026-10-30T23:15:30Z"
    w = freshness.stamp("series", {"x": 1}, "2026-10-30T21:58:00Z")    # no fetch time inside -> _at
    assert w["as_of"] == "2026-10-30T21:58:00Z" and w["ttl_s"] == 86400
    t = freshness.stamp("glossary", {"source": {"fetched": "2026-10-01T00:00:00Z"}}, "2026-10-30T21:58:00Z")
    assert t["ttl_s"] is None and t["stale_after"] is None             # timeless shelf
    for k in ("last_good", "finished", "tail_checked_at"):
        assert freshness.stamp("meta", {"a": {k: "2026-10-30T21:00:00Z"}}, "2026-10-30T21:58:00Z")["as_of"] == "2026-10-30T21:00:00Z"


def test_gate_refuses_write_time_as_of():
    d = {"_run": "r", "_at": "2026-10-30T21:58:00Z", "v": 4, "fetched_at": "2026-10-30T20:00:00Z",
         "as_of": "2026-10-30T21:58:00Z", "ttl_s": 86400, "stale_after": "2026-10-31T21:58:00Z"}
    f = gates.check([("series", "x", d)], NOW)
    assert any("not the newest fetch time" in x for x in f), f
    ok = freshness.stamp("series", {k: v for k, v in d.items() if k not in ("as_of", "ttl_s", "stale_after")}, d["_at"])
    assert gates.check([("series", "x", ok)], NOW) == []
    fut = freshness.stamp("series", {"_run": "r", "_at": "2026-10-30T21:58:00Z", "v": 4, "fetched_at": "2026-10-30T23:00:00Z"}, "2026-10-30T21:58:00Z")
    assert any("later than the run clock" in x for x in gates.check([("series", "x", fut)], NOW))


def _live(as_of):
    return {"live/x": {"as_of": as_of, "_at": as_of}}


def test_live_older_than_2h_fails():
    m = freshness.expect(_live("2026-10-30T19:30:00Z"), NOW, scope={"live"})        # 2 h 30 min old
    assert len(m) == 1 and m[0]["collection"] == "live" and "older than 2 h" in m[0]["reason"] and "2 h 30 min" in m[0]["found"], m
    assert freshness.expect(_live("2026-10-30T20:30:00Z"), NOW, scope={"live"}) == []
    assert freshness.expect({}, NOW, scope={"live"})[0]["found"] == "no live doc"


def _fr(*days):
    items = [{"id": f"pi-{d}", "t": f"{d}T20:15:00Z", "sources": [{"host": "federalregister.gov", "url": "https://www.federalregister.gov/x"}]} for d in days]
    return {"records/public_inspection": {"items": items}}


def test_fr_weekday_gap_fails():
    # Fri Oct 30 18:00 ET: the last 5 business days are Oct 26-30. Wed Oct 28 is absent.
    m = freshness.expect(_fr("2026-10-26", "2026-10-27", "2026-10-29", "2026-10-30"), NOW, scope={"records"})
    assert [x["expected"][:58] for x in m] == ["a Federal Register document dated 2026-10-28 (Wed, busines"], m
    assert freshness.expect(_fr("2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"), NOW, scope={"records"}) == []


def test_fr_weekend_and_holiday_are_not_missing():
    sun = clock.parse("2026-11-01T18:00:00Z")      # Sunday: window is Oct 26-30, the weekend is expected
    assert freshness.expect(_fr("2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"), sun, scope={"records"}) == []
    # Wed Nov 11 2026 is Veterans Day: no FR day; window ending Fri Nov 13 = Nov 6, 9, 10, 12, 13
    fri = clock.parse("2026-11-13T23:00:00Z")
    assert freshness.expect(_fr("2026-11-06", "2026-11-09", "2026-11-10", "2026-11-12", "2026-11-13"), fri, scope={"records"}) == []
    assert dt.date(2026, 11, 26) in freshness.federal_holidays(2026)      # Thanksgiving
    morning = clock.parse("2026-10-30T13:00:00Z")   # Fri 09:00 ET: Friday is not judged until 17:00 ET
    assert freshness.recent_business_days(morning)[0] == dt.date(2026, 10, 29)


def test_series_behind_cadence():
    docs = {"series/w": {"freq": "weekly", "latest": {"date": "2026-10-01"}}, "series/m": {"freq": "monthly", "latest": {"date": "2026-09-01"}}}
    m = freshness.expect(docs, NOW, scope={"series"})
    assert [x["expected"].split(":")[0] for x in m] == ["series/w"], m


def test_ttl_table_matches_heartbeat():
    assert freshness.check_ttl_table() == []
    hb = freshness.heartbeat_cadences()
    assert hb["USGS earthquakes"]["cadence_s"] == 60 and hb["GDACS"]["cadence_s"] == 300
    assert hb["Federal Register public inspection"]["cadence_s"] == "business_days"


if __name__ == "__main__":
    rc = 0
    for n, f in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
        try:
            f(); print("PASS", n)
        except AssertionError as e:
            rc = 1; print("FAIL", n, "-", str(e)[:300])
    sys.exit(rc)
