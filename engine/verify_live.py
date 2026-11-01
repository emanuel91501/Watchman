#!/usr/bin/env python3
"""verify_live.py (M4): photo coverage and freshness measured on the LIVE db, on the docs the UI actually renders.

Usage: python3 verify_live.py DB.json [--now ISO] [--json]
DB.json = {"collection/doc_id": data, ...} (the shape of app/dev/fixtures/*.json, built from ArtifactData list results).
`now` = --now, else the db's "__clock__", else the wall clock. Never run it on a fixture and call the result live: it prints
the newest doc ids it read and their runs so the reader can check.

What the UI reads (app/src/parts/data.js items(), rooms/today.js pickLead()): every cluster of every dated news/<date> doc
(chunks merged), newest first by the row's `t`; the lead is the cluster with the most members (newest on a tie). Which mode
wrote the docs does not matter: the newest docs are measured. The newest live/* doc is a second line (hourly beat).

Checks (exit 1 if any fails):
  lead_matched        the UI lead cluster (most members, then newest, across news/*) has image.kind == "matched"
  top30_matched_pct   >= 80 % of the 30 newest clusters (the order Today lists them) are matched
  hours_with_photo    every UTC hour with >= 5 items (live + dated docs) has >= 1 matched photo (the Contact Sheet)
  photos_attached     meta/run.photos.attached >= 1, or the newest live doc's photos.attached >= 1
  live_fresh_min      the newest live/* doc finished <= 70 min before now (WORDS L68/L72: one per hour)
  cadence_min         now - newest run finish (meta/run or newest live doc) <= 70 min, measured, not the config value
Info lines: top30 by members, newest live doc's own news ratio, brief hero, meta/run.cadence_s (config).
"""
import datetime as dt
import json, re, sys
from collections import defaultdict

TOP_N, TOP_PCT, HOUR_MIN_ITEMS, MAX_AGE_MIN = 30, 80.0, 5, 70
ITEM_KEYS = ("news", "events", "wire", "breaking", "items", "clusters")
LIVE_RE = re.compile(r"live/(\d{4}-\d{2}-\d{2}T\d{4})(_\d+)?")
NEWS_RE = re.compile(r"news/(\d{4}-\d{2}-\d{2})(_\d+)?")


def _matched(it):
    return isinstance(it, dict) and (it.get("image") or {}).get("kind") == "matched"


def _hour(t):
    m = re.match(r"(\d{4}-\d{2}-\d{2})T(\d{2})", str(t or ""))
    return f"{m.group(1)}T{m.group(2)}" if m else None


def _t(s):
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _pct(xs):
    return round(100.0 * sum(_matched(x) for x in xs) / len(xs), 1) if xs else 0.0


def ui_clusters(db):
    """Every news cluster the UI lists: all dated news docs, chunks merged, de-duplicated by id, newest first by `t`."""
    out = {}
    for k in sorted((k for k in db if NEWS_RE.fullmatch(k)), key=lambda k: (NEWS_RE.fullmatch(k).group(1), int((NEWS_RE.fullmatch(k).group(2) or "_1")[1:]))):
        d = db[k] if isinstance(db[k], dict) else {}
        for c in d.get("clusters") or []:
            if isinstance(c, dict) and c.get("id") and c.get("title") and c["id"] not in out:
                out[c["id"]] = c
    return sorted(out.values(), key=lambda c: ((_t(c.get("t")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)), c["id"]), reverse=True)


def ui_lead(clusters):
    """today.js pickLead, without the photo filter (the check asks whether the true lead has a picture)."""
    return sorted(clusters, key=lambda c: (-(c.get("n") or 0), -((_t(c.get("t")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)).timestamp())))[0] if clusters else None


def newest_live(db):
    stamps = sorted({LIVE_RE.fullmatch(k).group(1) for k in db if LIVE_RE.fullmatch(k)})
    if not stamps:
        return None, {}, []
    sid = "live/" + stamps[-1]
    ld = db.get(sid) or {}
    news = list(ld.get("news") or [])
    for k in sorted((k for k in db if k.startswith(sid + "_")), key=lambda k: int(k.rsplit("_", 1)[1])):
        news += (db[k] or {}).get("news") or []
    return sid, ld, news


def measure(db, now=None):
    now = now or _t(db.get("__clock__")) or dt.datetime.now(dt.timezone.utc)
    clusters = ui_clusters(db)
    top = clusters[:TOP_N]
    by_n = sorted(clusters, key=lambda c: (-(c.get("n") or 0), c["id"]))[:TOP_N]
    lead = ui_lead(clusters)
    sid, ld, live_news = newest_live(db)
    hours = defaultdict(lambda: [0, 0])
    seen_ids = set()
    for k, d in db.items():
        if k.split("/")[0] not in ("live", "news", "events", "wire", "records") or not isinstance(d, dict):
            continue
        for key in ITEM_KEYS:
            for it in d.get(key) or []:
                if not isinstance(it, dict) or it.get("id") in seen_ids:
                    continue
                seen_ids.add(it.get("id"))
                h = _hour(it.get("first_seen") or it.get("t"))
                if h:
                    hours[h][0] += 1; hours[h][1] += _matched(it)
    busy = {h: v for h, v in hours.items() if v[0] >= HOUR_MIN_ITEMS}
    blind = sorted(h for h, v in busy.items() if v[1] == 0)
    run = db.get("meta/run") or {}
    photos = run.get("photos") or {}
    attached = max(int(photos.get("attached") or 0), int((ld.get("photos") or {}).get("attached") or 0))
    live_fin = _t(ld.get("finished"))
    run_fin = _t(run.get("finished"))
    newest_fin = max([x for x in (live_fin, run_fin) if x], default=None)
    age = lambda x: round((now - x).total_seconds() / 60.0, 1) if x else None
    pct = _pct(top)
    brief_k = sorted(k for k in db if re.fullmatch(r"brief/\d{4}-\d{2}-\d{2}", k))
    brief = db.get(brief_k[-1]) if brief_k else None
    checks = {
        "lead_matched": {"ok": bool(lead and _matched(lead)), "value": ((lead or {}).get("image") or {}).get("kind") if lead else "no lead", "lead": (lead or {}).get("title"), "n": (lead or {}).get("n")},
        "top30_matched_pct": {"ok": bool(top) and pct >= TOP_PCT, "value": pct, "of": len(top), "need": TOP_PCT, "order": "newest first (Today)"},
        "hours_with_photo": {"ok": bool(busy) and not blind, "value": f"{len(busy) - len(blind)} of {len(busy)}", "hours_without": blind[:48]},
        "photos_attached": {"ok": attached >= 1, "value": attached, "meta_run": photos or None, "live": ld.get("photos")},
        "live_fresh_min": {"ok": live_fin is not None and age(live_fin) <= MAX_AGE_MIN, "value": age(live_fin), "doc": sid, "written_by": ld.get("written_by") or ld.get("mode"), "max": MAX_AGE_MIN},
        "cadence_min": {"ok": newest_fin is not None and age(newest_fin) <= MAX_AGE_MIN, "value": age(newest_fin), "max": MAX_AGE_MIN, "now": now.strftime("%Y-%m-%dT%H:%M:%SZ")},
    }
    info = {
        "top30_by_members_pct": _pct(by_n),
        "clusters_rendered": len(clusters),
        "live_news_pct": _pct(live_news[:TOP_N]), "live_news_of": len(live_news[:TOP_N]),
        "brief": brief_k[-1] if brief_k else None, "brief_hero": ((brief or {}).get("hero") or {}).get("title"),
        "cadence_s_config": run.get("cadence_s"),
    }
    return {"newest_live": sid, "live_run": ld.get("run"), "meta_run": run.get("run"), "newest_news": sorted({NEWS_RE.fullmatch(k).group(1) for k in db if NEWS_RE.fullmatch(k)})[-1:] or None,
            "ok": all(c["ok"] for c in checks.values()), "checks": checks, "info": info}


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    db = json.load(open(sys.argv[1], encoding="utf-8"))
    now = _t(sys.argv[sys.argv.index("--now") + 1]) if "--now" in sys.argv else None
    r = measure(db, now)
    if "--json" in sys.argv:
        print(json.dumps(r, indent=1))
    else:
        print(f"newest live doc {r['newest_live']} (run {r['live_run']}); meta/run {r['meta_run']}; newest news/{(r['newest_news'] or ['-'])[0]}")
        for n, c in r["checks"].items():
            print(("PASS " if c["ok"] else "FAIL ") + n + ": " + json.dumps({k: v for k, v in c.items() if k != "ok"}, default=str)[:300])
        print("info " + json.dumps(r["info"], default=str)[:400])
    sys.exit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
