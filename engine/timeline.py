"""Watchman timeline (CONTRACT-v4 §3, §4, §8; MASTER-PLAN §I "Rewind").

Pure functions over a run's docs [(collection, doc_id, data)]; nothing here reads the wall clock
(the reference time is passed in) and nothing is invented: every field is derived from values
already stored in the docs.

  item_lists(coll, doc)          the top-level item lists of a doc (events/wire/records items, news clusters, live lists)
  annotate_items(docs, ref)      every item gets display_when (display.py)
  time_docs(docs)                time/<YYYY-MM-DD> = {date, hours:{HH:{doc_ids[], counts{layer:n}}}}
  first_print(series)            {date, value, published} from ALFRED vintages for the latest date, else None
  echoes(series)                 [{date, move, after_6m}] earlier periods that moved at least as much, same direction
  story_states(story, ref)       [{t, state, stage}] from the story's own timeline (active <24 h after a move, else quiet)
  annotate(docs, ref)            all of the above applied in place; returns the new time docs
"""
import datetime as dt

import clock
import display

ITEM_LISTS = {"events": ("items",), "wire": ("items",), "records": ("items",), "news": ("clusters",),
              "live": ("events", "wire", "breaking", "news")}
ECHO_MAX = 3
SIX_MONTHS_D = 182
SIX_MONTHS_SLACK_D = 45      # the point read as "6 months later" must fall within 182-227 days, else after_6m is null
STORY_QUIET_S = 86400       # same rule the engine uses for stories.state (engine.py: active if last move < 24 h)


def item_lists(coll, doc):
    for k in ITEM_LISTS.get(coll, ()):
        lst = doc.get(k)
        if isinstance(lst, list):
            yield k, lst


def annotate_items(docs, ref):
    n = 0
    for coll, _, d in docs:
        for _, lst in item_lists(coll, d):
            for it in lst:
                if isinstance(it, dict) and it.get("t"):
                    it["display_when"] = display.display_when(it["t"], it.get("kind"), ref)
                    n += 1
                elif isinstance(it, dict):
                    it["display_when"] = None
    return n


SEEN_LATE_S = 6 * 3600    # wire/news first seen more than 6 h after their own t (a stateless run found them late): bin by t
SEEN_KEYED = {("wire", "items"), ("news", "clusters"), ("live", "wire"), ("live", "news")}


def _hour_key(it, coll=None, key=None):
    """FABLE-MIDBUILD #2: events, records and live events/breaking bin by the item's own `t`; only wire/news bin by
    first_seen (when the engine first saw them). A stateless run stamps first_seen = run time, so keying everything
    on first_seen piles a whole morning into one hour."""
    def p(v):
        try:
            return clock.parse(v)
        except (TypeError, ValueError):
            return None
    own, seen = p(it.get("t")), p(it.get("first_seen"))
    if (coll, key) in SEEN_KEYED and seen and (own is None or (seen - own).total_seconds() <= SEEN_LATE_S):
        return seen
    return own or seen


def time_docs(docs):
    """One doc per UTC day. HH = UTC hour of the item's own t (events, records, live events/breaking) or first_seen (wire, news); live/<date>T<HHMM> docs are listed under
    their own hour. Each item id is counted once per hour even when several docs carry it. Empty hours are absent (quiet)."""
    days = {}            # date -> HH -> {"ids": set(paths), "seen": {item id: layer}}
    first = {}           # item id -> (earliest binned hour time, layer)

    def slot(t):
        return days.setdefault(t.strftime("%Y-%m-%d"), {}).setdefault(t.strftime("%H"), {"ids": set(), "seen": {}})

    for coll, did, d in docs:
        path = f"{coll}/{did}"
        if coll == "live":
            base = did.split("_", 1)[0]
            try:
                t = dt.datetime.strptime(base, "%Y-%m-%dT%H%M").replace(tzinfo=clock.UTC)
                slot(t)["ids"].add(path)
            except ValueError:
                pass
        for key, lst in item_lists(coll, d):
            for it in lst:
                if not isinstance(it, dict) or not it.get("id"):
                    continue
                t = _hour_key(it, coll, key)
                if not t:
                    continue
                slot(t)["ids"].add(path)
                prev = first.get(it["id"])
                if prev is None or t < prev[0]:
                    first[it["id"]] = (t, it.get("layer") or "unknown")
    for iid, (t, layer) in sorted(first.items()):      # each item counted once, at its earliest binned hour
        slot(t)["seen"][iid] = layer
    out = []
    for date in sorted(days):
        hours = {}
        for hh in sorted(days[date]):
            s = days[date][hh]
            counts = {}
            for layer in s["seen"].values():
                counts[layer] = counts.get(layer, 0) + 1
            hours[hh] = {"doc_ids": sorted(s["ids"]), "counts": dict(sorted(counts.items()))}
        out.append(("time", date, {"date": date, "hours": hours}))
    return out


# ------------------------------------------------------------------ series
def first_print(series):
    lat = (series.get("latest") or {}).get("date")
    for row in series.get("vintages") or []:
        f = row.get("first") or {}
        if row.get("date") == lat and f.get("value") is not None:
            return {"date": lat, "value": f["value"], "published": f.get("published")}
    return None


def _pts(series):
    out = []
    for p in series.get("points") or []:
        if isinstance(p, (list, tuple)) and len(p) >= 2 and isinstance(p[1], (int, float)) and p[0]:
            out.append((str(p[0])[:10], float(p[1])))
    out.sort()
    return out


def echoes(series):
    pts = _pts(series)
    if len(pts) < 3:
        return []
    move = pts[-1][1] - pts[-2][1]
    if move == 0:
        return []
    found = []
    for i in range(len(pts) - 2, 0, -1):
        m = pts[i][1] - pts[i - 1][1]
        if m * move > 0 and abs(m) >= abs(move):
            d0 = dt.date.fromisoformat(pts[i][0])
            tgt = (d0 + dt.timedelta(days=SIX_MONTHS_D)).isoformat()
            lim = (d0 + dt.timedelta(days=SIX_MONTHS_D + SIX_MONTHS_SLACK_D)).isoformat()
            later = next((p for p in pts[i + 1:] if p[0] >= tgt), None)
            later = later if later and later[0] <= lim else None
            found.append({"date": pts[i][0], "move": round(m, 6), "after_6m": round(later[1] - pts[i][1], 6) if later else None})
            if len(found) >= ECHO_MAX:
                break
    return found


# ------------------------------------------------------------------ stories
def story_states(story, ref):
    ts = sorted(t for t in (clock.parse(e.get("t")) for e in story.get("timeline") or [] if isinstance(e, dict)) if t)
    states, cur, last = [], None, None
    q = dt.timedelta(seconds=STORY_QUIET_S)
    for t in ts:
        if cur == "active" and last is not None and t - last >= q:
            states.append({"t": clock.iso(last + q), "state": "quiet", "stage": None}); cur = "quiet"
        if cur != "active":
            states.append({"t": clock.iso(t), "state": "active", "stage": None}); cur = "active"
        last = t
    if cur == "active" and last is not None and ref - last >= q:
        states.append({"t": clock.iso(last + q), "state": "quiet", "stage": None})
    return states


def annotate(docs, ref):
    """In place. ref = the run clock (frozen in tests). Returns the time/<date> docs to append."""
    annotate_items(docs, ref)
    for coll, _, d in docs:
        if coll == "series":
            d["first_print"] = first_print(d)
            d["echoes"] = echoes(d)
            lat = (d.get("latest") or {}).get("date")
            d["display_when"] = display.display_when(lat, d.get("freq"), ref) if lat else None
        elif coll == "stories":
            d["states"] = story_states(d, ref)
    return time_docs(docs)
