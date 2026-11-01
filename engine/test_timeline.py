#!/usr/bin/env python3
"""P1 timeline/display tests (stdlib; `python3 test_timeline.py` or pytest). CONTRACT-v4 §3, §4, §8."""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import clock, display, timeline

REF = clock.parse("2026-10-31T23:53:46Z")


def test_week_of_label():
    assert display.display_when("2026-10-26", "weekly", REF) == "week of Oct 26"          # EIA gas, Monday-dated week
    assert display.display_when("2026-10-24", "weekly", REF) == "week ending Oct 24"      # FRED ICSA, week ending Saturday
    assert display.display_when("2026-09-01", "monthly", REF) == "September 2026"
    assert display.display_when("2026-07-01", "quarterly", REF) == "Q3 2026"
    assert display.display_when("2026-10-29", "daily", REF) == "Oct 29"
    assert display.display_when("2025-12-31", "daily", REF) == "Dec 31, 2025"
    assert display.display_when("2026-10-30T02:30:00Z", "quake", REF) == "Oct 29"           # item time shown as its ET day


def _docs():
    a = {"id": "a", "t": "2026-10-31T14:05:00Z", "first_seen": "2026-10-31T15:10:00Z", "layer": "us", "kind": "quake"}
    b = {"id": "b", "t": "2026-10-31T15:20:00Z", "layer": "world", "kind": "news"}
    c = {"id": "c", "t": "2026-10-30T23:59:00Z", "first_seen": "2026-10-30T23:59:59Z", "layer": "us", "kind": "rule"}
    return [("events", "2026-10-31", {"items": [a]}), ("wire", "2026-10-31", {"items": [dict(a), b]}),
            ("records", "presidential", {"items": [c]}), ("news", "2026-10-31", {"clusters": [dict(b, items=[{"t": "2026-10-01T00:00:00Z"}])]}),
            ("live", "2026-10-31T1551", {"events": [], "wire": [], "breaking": [], "news": []}), ("series", "x", {"points": []})]


def test_time_docs():
    t = {d: data for _, d, data in timeline.time_docs(_docs())}
    assert sorted(t) == ["2026-10-30", "2026-10-31"]
    h = t["2026-10-31"]["hours"]
    assert sorted(h) == ["14", "15"]                                # events bin by own t (a at 14); wire/news by first_seen (a 15, b 15); live doc at its own hour
    assert h["14"]["counts"] == {"us": 1}                           # a counted once, at its earliest binned hour
    assert h["15"]["counts"] == {"world": 1}                        # b; nested cluster articles ignored
    assert h["14"]["doc_ids"] == ["events/2026-10-31"]
    assert h["15"]["doc_ids"] == ["live/2026-10-31T1551", "news/2026-10-31", "wire/2026-10-31"]
    assert t["2026-10-30"]["hours"] == {"23": {"doc_ids": ["records/presidential"], "counts": {"us": 1}}}


def test_late_seen_wire_bins_by_own_t():
    """A stateless run stamps first_seen = run time; wire found >6 h after its own t bins by t, events always by t."""
    run = "2026-10-31T09:00:02Z"
    items = [{"id": f"w{i}", "t": "2026-10-30T00:00:00Z", "first_seen": run, "layer": "world", "kind": "current_event"} for i in range(13)]
    ev = [{"id": f"e{i}", "t": f"2026-10-31T0{i % 9}:30:00Z", "first_seen": run, "layer": "us", "kind": "quake"} for i in range(30)]
    t = {d: data for _, d, data in timeline.time_docs([("wire", "2026-10-31", {"items": items}), ("events", "2026-10-31", {"items": ev})])}
    assert "09" not in t["2026-10-31"]["hours"], t["2026-10-31"]["hours"].keys()
    assert t["2026-10-30"]["hours"]["00"]["counts"] == {"world": 13}


C1 = os.path.join(HERE, "..", "app", "dev", "fixtures", "C1.json")


def _single_release(d, date, hh):
    """>=80% of the hour's items carry one identical source-stated t (binned by t, never by the engine's first_seen):
    a real scheduled release (Federal Register public inspection at 08:45 ET) or a day-dated record list, not an engine stamp."""
    n, by = 0, {}
    for p, v in d.items():
        coll = p.split("/")[0]
        if coll == "time" or not isinstance(v, dict):
            continue
        for key, lst in timeline.item_lists(coll, v):
            for it in lst:
                if not isinstance(it, dict) or not it.get("id"):
                    continue
                t = timeline._hour_key(it, coll, key)
                if t and t.strftime("%Y-%m-%d") == date and t.strftime("%H") == hh:
                    n += 1
                    if (coll, key) not in timeline.SEEN_KEYED:
                        by[it.get("t")] = by.get(it.get("t"), 0) + 1
    top = max(by.values()) if by else 0
    return n and top >= 0.8 * n, top


def test_morning_run_no_spike():
    """FABLE-MIDBUILD #2: on C1, no hour exceeds 5x the median of that day's non-empty hours (a single scheduled
    release stamped by its own source is named, not hidden)."""
    import json, statistics
    if not os.path.exists(C1):
        raise AssertionError(f"C1 fixture missing: {C1}")
    d = json.load(open(C1, encoding="utf-8"))
    docs = [(p.split("/", 1)[0], p.split("/", 1)[1], v) for p, v in d.items() if "/" in p and not p.startswith("time/")]
    bad, named = [], []
    for _, date, doc in timeline.time_docs(docs):
        vals = {hh: sum(h["counts"].values()) for hh, h in doc["hours"].items()}
        if len(vals) < 3:
            continue
        med = statistics.median(vals.values())
        for hh, v in vals.items():
            if v > 5 * med:
                rel, top = _single_release(d, date, hh)
                (named if rel else bad).append(f"{date} {hh}h: {v} items vs median {med}" + (f" (one release of {top})" if rel else ""))
    assert not bad, bad
    print("    named single releases:", named or "none")


def test_annotate_display_when_and_stable():
    d1, d2 = _docs(), _docs()
    r1, r2 = timeline.annotate(d1, REF), timeline.annotate(d2, REF)
    assert r1 == r2 and d1 == d2
    assert d1[0][2]["items"][0]["display_when"] == "Oct 31"
    assert "display_when" not in d1[3][2]["clusters"][0]["items"][0]       # article inside a cluster is not an item


def test_echoes_and_first_print():
    s = {"freq": "monthly", "latest": {"date": "2026-09-01"},
         "points": [["2025-01-01", 10.0], ["2025-02-01", 10.5], ["2025-03-01", 10.4], ["2025-08-01", 11.0], ["2025-09-01", 11.1], ["2026-08-01", 12.0], ["2026-09-01", 12.4]],
         "vintages": [{"date": "2026-09-01", "first": {"value": 12.3, "published": "2026-10-15"}, "current": {"value": 12.4, "as_of": "2026-10-30"}}]}
    e = timeline.echoes(s)
    assert e[0] == {"date": "2026-08-01", "move": 0.9, "after_6m": None}, e     # +0.9 >= +0.4; no point 6 months later -> null, never guessed
    assert e[1] == {"date": "2025-08-01", "move": 0.6, "after_6m": None}, e     # next point is 12 months on: not "6 months later"
    assert e[2] == {"date": "2025-02-01", "move": 0.5, "after_6m": 0.6}, e     # 2025-09-01 is 212 days on, within the slack
    assert timeline.first_print(s) == {"date": "2026-09-01", "value": 12.3, "published": "2026-10-15"}
    assert timeline.first_print({"latest": {"date": "2026-09-01"}}) is None


def test_story_states():
    st = {"timeline": [{"t": "2026-10-28T10:00:00Z"}, {"t": "2026-10-28T12:00:00Z"}, {"t": "2026-10-30T09:00:00Z"}]}
    assert timeline.story_states(st, REF) == [
        {"t": "2026-10-28T10:00:00Z", "state": "active", "stage": None}, {"t": "2026-10-29T12:00:00Z", "state": "quiet", "stage": None},
        {"t": "2026-10-30T09:00:00Z", "state": "active", "stage": None}, {"t": "2026-10-31T09:00:00Z", "state": "quiet", "stage": None}]


if __name__ == "__main__":
    rc = 0
    for n, f in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
        try:
            f(); print("PASS", n)
        except AssertionError as e:
            rc = 1; print("FAIL", n, "-", str(e)[:300])
    sys.exit(rc)
