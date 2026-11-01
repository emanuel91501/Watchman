"""Watchman write gate (MASTER-PLAN §D: gates.py refuses batches on violations; exit 2, report.json.failed[]).

check(writes, now) -> failed[] (strings). `writes` = [(collection, doc_id, data)] exactly as they will be written.
A batch set is refused when any doc breaks CONTRACT-v4 §1-4:
  - v == 4, _run, _at, as_of, ttl_s, stale_after present; ttl_s equals the §2 table; stale_after == as_of + ttl_s
  - as_of is the newest fetch time inside the doc (never a write time when a fetch time exists)
  - no as_of or _at later than the run clock (the "times later than now" defect)
  - every item in events/wire/records/news/live carries display_when
  - time/<date> docs: HH keys are 00-23 and every listed doc id is in this write set or is a live doc
Missing data (freshness.expect) is NOT a gate failure: it is written to meta/health.missing and rendered.
"""
import datetime as dt

import clock
import freshness
import timeline

V = 4


def check(writes, now):
    failed = []
    paths = {f"{c}/{d}" for c, d, _ in writes}
    for coll, did, d in writes:
        p = f"{coll}/{did}"
        if d.get("v") != V:
            failed.append(f"{p}: v != {V}")
        for k in ("_run", "_at", "as_of", "ttl_s", "stale_after"):
            if k not in d:
                failed.append(f"{p}: missing {k}")
        if "as_of" not in d:
            continue
        try:
            ttl = freshness.ttl_for(coll)
        except freshness.UnknownCollection as e:
            failed.append(f"{p}: {e}")
            continue
        if d.get("ttl_s") != ttl:
            failed.append(f"{p}: ttl_s {d.get('ttl_s')} != {ttl}")
        a = clock.parse(d["as_of"])
        want = freshness.max_fetched({k: v for k, v in d.items() if k not in ("as_of", "stale_after")}) or clock.parse(d.get("_at"))
        if a != want:
            failed.append(f"{p}: as_of {d['as_of']} is not the newest fetch time {clock.iso(want)}")
        exp = clock.iso(a + dt.timedelta(seconds=ttl)) if ttl is not None else None
        if d.get("stale_after") != exp:
            failed.append(f"{p}: stale_after {d.get('stale_after')} != {exp}")
        for k in ("as_of", "_at"):
            t = clock.parse(d.get(k))
            if t and t > now:
                failed.append(f"{p}: {k} {d[k]} is later than the run clock {clock.iso(now)}")
        for key, lst in timeline.item_lists(coll, d):
            for i, it in enumerate(lst):
                if isinstance(it, dict) and "display_when" not in it:
                    failed.append(f"{p}: {key}[{i}] has no display_when"); break
        if coll == "time":
            for hh, h in (d.get("hours") or {}).items():
                if not (len(hh) == 2 and hh.isdigit() and 0 <= int(hh) <= 23):
                    failed.append(f"{p}: hour key {hh!r}")
                for ref in h.get("doc_ids") or []:
                    if ref not in paths and not ref.startswith("live/"):
                        failed.append(f"{p}: hour {hh} lists {ref}, which is not written")
    return failed
