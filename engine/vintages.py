"""R17 vintages: CPI first print -> revised, from FRED/ALFRED.
fetch_vintages(get, base, key, fid, n=3) -> [{date, first:{value, published}, current:{value, as_of}}], newest month first.
`get(url, params=...)` returns a requests-like response (the engine's Session.get works)."""

def _obs(r):
    out = {}
    try:
        for o in r.json().get("observations", []):
            if o.get("value") not in (".", None):
                out[o["date"]] = (float(o["value"]), o.get("realtime_start"))
    except Exception:
        pass
    return out

def build_vintages(first, current, n=3):
    """first/current: {date: (value, realtime_start)}. Pure; latest n months present in both."""
    rows = []
    for d in sorted(set(first) & set(current), reverse=True)[:n]:
        rows.append({"date": d,
                     "first": {"value": first[d][0], "published": first[d][1]},
                     "current": {"value": current[d][0], "as_of": current[d][1]}})
    return rows

def fetch_vintages(get, base, key, fid, n=3, start=None):
    start = start or _months_back(n + 2)
    p = {**key, "series_id": fid, "observation_start": start}
    r1 = get(base + "series/observations", params={**p, "realtime_start": "1776-07-04", "realtime_end": "9999-12-31", "output_type": "4"})
    r2 = get(base + "series/observations", params=p)
    if not (r1.ok and r2.ok):
        return None
    return build_vintages(_obs(r1), _obs(r2), n)

def _months_back(m):
    import datetime as dt
    t = dt.date.today(); y, mo = t.year, t.month - m
    while mo < 1:
        mo += 12; y -= 1
    return f"{y:04d}-{mo:02d}-01"
