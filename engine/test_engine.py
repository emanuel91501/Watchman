#!/usr/bin/env python3
"""Watchman engine tests (stdlib only).

  python3 test_engine.py --out DIR [--out2 DIR2]      run all tests against real outputs (out2 = a second morning run for idempotency)
  python3 test_engine.py --out DIR --out2 DIR2 --demo each test is run on a deliberately bad copy (must FAIL), then on the real output (must PASS)
"""
import imgfeeds
import argparse, copy, glob, json, os, re, sys, tempfile
from collections import defaultdict
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FIXED_COLLS = {"meta", "series", "places", "records", "clocks", "stories", "atlas", "gaps", "glossary"}
LIVE_ID = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{4}(_\d+)?$")
TRUTH = {"primary", "confirmed", "single", "analysis", "conflict", "not_fetched"}
ITEM_KEYS = ["id", "kind", "title", "plain", "place", "t", "first_seen", "updated", "sev", "layer", "truth", "sources", "prov", "score", "reach", "domain", "atlas"]
SCORE_KEYS = ["stake", "prox", "irrev", "urg", "total"]
FIXTURE_PHRASES = re.compile(r"lorem ipsum|\bfixture\b|\bdummy\b|\bplaceholder\b|test item|test title|\bfoo bar\b|\basdf\b|\bxxx+\b|\bTODO\b|fake news item|sample item", re.I)
FIXTURE_TITLES = {"test", "sample", "example", "demo", "foo", "bar", "untitled", "title", "n/a", "none", "null"}


def keys():
    out = {}
    p = os.path.join(HERE, ".keys.env")
    if os.path.exists(p):
        for l in open(p):
            if "=" in l and not l.startswith("#"):
                k, v = l.strip().split("=", 1); out[k] = v
    return out


def load(out):
    """-> dict(batches={file: [writes]}, files={name: raw text}, report=dict)"""
    batches, raw = {}, {}
    for f in sorted(glob.glob(os.path.join(out, "*.json"))):
        txt = open(f, encoding="utf-8").read(); raw[os.path.basename(f)] = txt
        if os.path.basename(f).startswith("batch_"):
            batches[os.path.basename(f)] = json.loads(txt)
    for extra in ("collections.txt", os.path.join("img", "manifest.json")):
        q = os.path.join(out, extra)
        if os.path.exists(q):
            raw[extra.replace(os.sep, "/")] = open(q, encoding="utf-8").read()
    rep = json.load(open(os.path.join(out, "report.json")))
    return {"batches": batches, "raw": raw, "report": rep, "out": out, "mode": rep["mode"]}


def writes(run):
    return [w for b in run["batches"].values() for w in b]


def items_of(run):
    """Every contract item (wire/events/records) as (collection, doc_id, item)."""
    for w in writes(run):
        if w["collection"] in ("wire", "events", "records"):
            for it in w["data"].get("items", []):
                yield w["collection"], w["doc_id"], it
        elif w["collection"] == "live":
            for key in ("events", "wire", "breaking"):
                for it in w["data"].get(key, []):
                    yield "live/" + key, w["doc_id"], it


def reg(host):
    p = host.split(".")
    return ".".join(p[-3:]) if len(p) >= 3 and ".".join(p[-2:]) in {"co.uk", "com.au", "co.jp", "com.cn", "co.in"} else ".".join(p[-2:])


# ---------------------------------------------------------------- the tests; each returns a list of failure strings
SKIP = "SKIP: "


def offline_missing(run, sid_fid):
    """True when this output came from a replay run that could not read FRED for this series (its request was not in
    the replay cache), so a check that needs that response has no input offline. Never true on a networked run."""
    rep = run["report"]
    if not rep.get("replay"):
        return False
    return any(m.endswith(" series_id=" + sid_fid) for m in rep.get("replay_misses") or [])


def fid_of(d):
    u = str((d.get("source") or {}).get("url", ""))
    return u.rstrip("/").rsplit("/", 1)[-1] if "fred.stlouisfed.org/series/" in u else None


def split_skips(run, fails):
    """fails: [(sid_fid or None, message)]. Failures whose input is missing offline become one SKIP line; the rest stay failures."""
    real = [m for f, m in fails if not (f and offline_missing(run, f))]
    gone = [m for f, m in fails if f and offline_missing(run, f)]
    if real or not gone:
        return real
    return [SKIP + f"{len(gone)} check(s) need FRED/ALFRED responses this replay cache does not hold (report.replay_misses); rerun on a networked run"]


def t_no_key(run, ctx):
    bad = []
    for k, v in keys().items():
        if len(v) < 6:
            continue
        for name, txt in run["raw"].items():
            if v in txt:
                bad.append(f"{k} value found in {name}")
        for f in glob.glob(os.path.join(run["out"], "**", "*"), recursive=True):
            if os.path.isfile(f) and v.encode() in open(f, "rb").read():
                bad.append(f"{k} value found in {os.path.relpath(f, run['out'])}")
        for cdir in ctx.get("cache_dirs", []):
            for f in glob.glob(os.path.join(cdir, "*")):
                if v.encode() in open(f, "rb").read():
                    bad.append(f"{k} value found in cache {os.path.basename(f)}")
    return bad


def t_size_limits(run, ctx):
    bad = []
    for name, b in run["batches"].items():
        if len(b) > 50:
            bad.append(f"{name}: {len(b)} writes > 50")
        size = len(run["raw"][name].encode("utf-8"))
        if size > 900 * 1024:
            bad.append(f"{name}: {size} bytes > 900 KB")
        for w in b:
            n = len(json.dumps(w["data"], ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            if n > 220 * 1024:
                bad.append(f"{w['collection']}/{w['doc_id']}: {n} bytes > 220 KB")
            for k in ("op", "collection", "doc_id", "data"):
                if k not in w:
                    bad.append(f"{name}: write missing {k}")
            if w.get("op") != "set" or set(w) - {"op", "collection", "doc_id", "data", "if_version"}:
                bad.append(f"{name}: op {w.get('op')} is not set")
            for k in ("_run", "_at", "v"):
                if k not in w["data"]:
                    bad.append(f"{w['collection']}/{w['doc_id']}: missing {k}")
            if w["data"].get("v") != 4:      # CONTRACT-v4 (P1)
                bad.append(f"{w['collection']}/{w['doc_id']}: v != 4")
    return bad


def t_item_fields(run, ctx):
    bad = []; n = 0
    for coll, did, it in items_of(run):
        n += 1
        for k in ITEM_KEYS:
            if k not in it:
                bad.append(f"{coll}/{did} {it.get('id')}: missing {k}")
        if not it.get("sources"):
            bad.append(f"{coll}/{did} {it.get('id')}: no sources")
        if not (it.get("t") or it.get("updated")):
            bad.append(f"{coll}/{did} {it.get('id')}: no as-of time")
        if it.get("truth") not in TRUTH:
            bad.append(f"{coll}/{did} {it.get('id')}: truth {it.get('truth')!r}")
        if not all(k in (it.get("score") or {}) for k in SCORE_KEYS):
            bad.append(f"{coll}/{did} {it.get('id')}: score incomplete")
        for s in it.get("sources") or []:
            if not (s.get("name") and s.get("url") and "fetched" in s and s.get("host")):
                bad.append(f"{coll}/{did} {it.get('id')}: source incomplete {s}")
    if n == 0 and run["mode"] == "morning":
        bad.append("no items found at all")
    # series: source, latest, as-of
    for w in writes(run):
        if w["collection"] == "series":
            d = w["data"]
            if not (d.get("source") and d.get("latest") and d["latest"].get("date")):
                bad.append(f"series/{w['doc_id']}: missing source or latest")
    return bad[:25]


def t_confirmed_hosts(run, ctx):
    bad = []
    for coll, did, it in items_of(run):
        if it["truth"] == "confirmed":
            hosts = {reg(s["host"]) for s in it["sources"]}
            if len(hosts) < 2:
                bad.append(f"{coll}/{did} {it['id']}: confirmed with hosts {sorted(hosts)}")
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                if c["truth"] == "confirmed" and len(set(c["publishers"])) < 2:
                    bad.append(f"news cluster {c['id']}: confirmed with publishers {c['publishers']}")
    return bad[:25]


def t_idempotent_ids(run, ctx):
    run2 = ctx.get("run2")
    if run2 is None:
        if not run["report"].get("replay_dir"):
            return [SKIP + "networked output: pass --out2 = a second morning run (RUN.md d.2); replay outputs re-run themselves"]
        return ["no second run supplied (--out2) and the replay self-rerun failed"]
    skip = {"stories", "img"}  # content-keyed (cluster / event ids), so they legitimately differ when live data moves between runs
    a = {(w["collection"], w["doc_id"]) for w in writes(run) if w["collection"] not in skip}
    b = {(w["collection"], w["doc_id"]) for w in writes(run2) if w["collection"] not in skip}
    # a doc may differ only when the source that feeds it had a different status in the two runs (flaky/rate-limited source); that is reported, not hidden
    st1 = {h["id"]: h["status"] for h in run["report"]["sources"]}; st2 = {h["id"]: h["status"] for h in run2["report"]["sources"]}
    feeds = {"cpi_bls": "bls", "gas_us": "eia", "gas_midwest": "eia", "gdp_growth": "bea", "tga_close": "treasury_dts"}
    def explained(x):
        c, d = x
        if c == "series":
            sid = feeds.get(d, "fred")
            return st1.get(sid) != st2.get(sid)
        return False
    notes = [f"explained by source status change: {x}" for x in sorted((a ^ b)) if explained(x)]
    ctx["notes"] = notes
    bad = [f"doc only in run 1: {x}" for x in sorted(a - b) if not explained(x)] + [f"doc only in run 2: {x}" for x in sorted(b - a) if not explained(x)]
    ia = {it["id"] for _, _, it in items_of(run)}; ib = {it["id"] for _, _, it in items_of(run2)}
    ov = len(ia & ib) / max(1, len(ia | ib))
    ctx["idem_overlap"] = round(ov, 4)
    need = 1.0 if ctx.get("strict") else 0.8
    if ov < need:
        bad.append(f"item id overlap {ov:.2%} < {need:.0%}")
    return bad[:25]


def t_no_fixture(run, ctx):
    bad = []
    def chk(where, text):
        if not isinstance(text, str):
            return
        if FIXTURE_PHRASES.search(text):
            bad.append(f"{where}: fixture phrase in {text[:70]!r}")
        if text.strip().lower() in FIXTURE_TITLES:
            bad.append(f"{where}: fixture title {text!r}")
    for coll, did, it in items_of(run):
        chk(f"{coll}/{did} {it['id']} title", it["title"]); chk(f"{coll}/{did} {it['id']} plain", it["plain"])
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                chk(f"news {c['id']}", c["title"])
        if w["collection"] == "clocks":
            for c in w["data"]["items"]:
                chk(f"clock {c['id']}", c["title"])
    return bad[:25]


def t_brief_refs(run, ctx):
    ids = {it["id"] for _, _, it in items_of(run)}
    for w in writes(run):
        if w["collection"] == "news":
            ids |= {c["id"] for c in w["data"]["clusters"]}
    bad = []
    for w in writes(run):
        if w["collection"] == "brief":
            for i in w["data"]["items"]:
                for r in i["refs"]:
                    if r not in ids:
                        bad.append(f"brief item ref {r} not found in emitted items")
                if set(i["tags"]) - {"FACT"}:
                    bad.append("engine brief contains a non-FACT tag (model sentences are not produced by the engine)")
    return bad


# ---------------------------------------------------------------- tests for the cloud-run design (hourly = new docs only, versions, images as files)
def t_live_only(run, ctx):
    """hourly: only new live/<date>T<HHMM> docs, right shape, one batch, small, nothing older than the 2 h window."""
    from engine import parse_time
    bad, ws = [], writes(run)
    if not ws:
        return ["no writes"]
    for w in ws:
        if w["collection"] != "live":
            bad.append(f"{w['collection']}/{w['doc_id']}: hourly may write only live/* (anything else overwrites)")
        elif not LIVE_ID.match(w["doc_id"]):
            bad.append(f"live/{w['doc_id']}: bad id")
    live = [w for w in ws if w["collection"] == "live"]
    if live:
        d = live[0]["data"]
        for k in ("run", "mode", "started", "finished", "ok_sources", "total_sources", "health", "events", "wire", "breaking", "next_due"):
            if k not in d:
                bad.append(f"live/{live[0]['doc_id']}: missing {k}")
        started = d.get("started", "")
        if d.get("mode") != "hourly":
            bad.append("mode is not hourly")
        if not started.endswith("Z"):
            bad.append("started is not UTC (Z)")
        elif live[0]["doc_id"].split("_")[0] != started[:10] + "T" + started[11:13] + started[14:16]:
            bad.append(f"live id {live[0]['doc_id']} does not match UTC run start {started}")
        st = parse_time(started)
        for w in live:
            for key in ("events", "wire"):
                for it in w["data"].get(key, []):
                    if st and (st - parse_time(it["t"])).total_seconds() > 2 * 3600 + 60:
                        bad.append(f"{key} item {it['id']} is older than the 2 h window ({it['t']})")
            for it in w["data"].get("breaking", []):
                if not it["score"]["is_breaking"]:
                    bad.append(f"breaking item {it['id']} is not is_breaking")
            if len(json.dumps(w["data"], ensure_ascii=False).encode()) > 220 * 1024:
                bad.append(f"live/{w['doc_id']} over 220 KB; should have split at 200 KB")
    if len(run["batches"]) != 1:
        bad.append(f"{len(run['batches'])} batches; hourly must be 1")
    if sum(len(t.encode()) for n, t in run["raw"].items() if n.startswith("batch_")) > 400 * 1024:
        bad.append("hourly batch > 400 KB")
    return bad[:25]


def t_collections(run, ctx):
    """morning: collections.txt lists every collection holding a doc that gets overwritten."""
    txt = run["raw"].get("collections.txt")
    if txt is None:
        return ["collections.txt missing"]
    listed = set(txt.split())
    present = {w["collection"] for w in writes(run)}
    bad = [f"{c}: has fixed docs but is not in collections.txt" for c in sorted(present & FIXED_COLLS) if c not in listed]
    bad += [f"{c}: listed but has no writes (and is not img)" for c in sorted(listed - present - {"img"})]
    if len(txt.split()) != len(listed):
        bad.append("duplicate lines in collections.txt")
    return bad


def t_static_once(run, ctx):
    """morning: atlas/gaps/glossary only with --static-once (the report records which way the run went)."""
    flag = run["report"].get("static_once")
    got = {w["collection"] for w in writes(run)} & {"atlas", "gaps", "glossary"}
    if flag is None:
        return ["report has no static_once flag"]
    if not flag and got:
        return [f"{sorted(got)} emitted without --static-once"]
    if flag and got != {"atlas", "gaps", "glossary"}:
        return [f"--static-once given but only {sorted(got)} emitted"]
    return []


def t_images(run, ctx):
    """no image bytes inside db docs; files + manifest instead; items reference images by id only."""
    bad = []
    for name, txt in run["raw"].items():
        if name.startswith("batch_") and ("data:image" in txt or "base64," in txt):
            bad.append(f"{name}: embedded base64 image")
    if any(w["collection"] == "img" for w in writes(run)):
        bad.append("img/* doc written by the engine (the runner writes img/index)")
    man_txt = run["raw"].get("img/manifest.json")
    if man_txt is None:
        return bad + ["img/manifest.json missing"]
    man = json.loads(man_txt); ids = [m["id"] for m in man]
    if len(ids) != len(set(ids)):
        bad.append("duplicate ids in manifest")
    for m in man:
        for k in ("id", "file", "credit", "license", "src_url", "taken", "caption"):
            if not m.get(k):
                bad.append(f"manifest {m.get('id')}: missing {k}")
        if m.get("license") and re.search(r"\b(nc|nd|all rights reserved|fair use|unknown)\b", m["license"], re.I):
            bad.append(f"manifest {m.get('id')}: licence not reusable ({m['license']})")
        f = os.path.join(run["out"], m.get("file") or "x")
        if not os.path.exists(f):
            bad.append(f"manifest {m.get('id')}: file {m.get('file')} missing")
        elif open(f, "rb").read(3) != b"\xff\xd8\xff":
            bad.append(f"{m.get('file')}: not a JPEG")
        elif os.path.getsize(f) > (imgfeeds.PHOTO_MAX_BYTES if str(m.get("id", "")).startswith(("img-pub-", "img-gov-")) else 120_000):   # M2: R2 photos 1280 px / 180 KB
            bad.append(f"{m.get('file')}: {os.path.getsize(f)} bytes > cap")
        if m.get("file") != f"img/{m.get('id')}.jpg":
            bad.append(f"manifest {m.get('id')}: file should be img/<id>.jpg")
    for coll, did, it in items_of(run):
        if coll == "records" and did.startswith("presidential") and not it.get("img") and not it.get("img_note"):
            bad.append(f"{coll}/{did} {it['id']}: presidential record has neither an image nor an img_note reason")
        if it.get("img") and it["img"] not in ids:
            bad.append(f"{coll}/{did} {it['id']}: img {str(it['img'])[:40]!r} is not a manifest id")
        if isinstance(it.get("img"), (dict, str)) and str(it.get("img")).startswith("data:"):
            bad.append(f"{it['id']}: img is a data URI")
    return bad[:25]


def _synth_jpgs():
    import io, random
    from PIL import Image, ImageDraw
    random.seed(3)
    grey = Image.new("RGB", (640, 640), (120, 112, 105))
    ImageDraw.Draw(grey).rectangle([200, 200, 400, 400], fill=(126, 118, 110))
    col = Image.new("RGB", (640, 640))
    px = col.load()
    for x in range(640):
        for y in range(640):
            px[x, y] = (30 + (x * 3) % 200, 80 + (y * 5) % 150, 200 - (x + y) % 160) if (x // 16 + y // 16) % 2 else (230, 60 + x % 120, 40)
    out = []
    for im in (grey, col):
        b = io.BytesIO(); im.save(b, "JPEG", quality=85); out.append(b.getvalue())
    return out


def t_legibility(run, ctx):
    """FABLE-MIDBUILD #3: plate_score is retired; every manifest row carries the one picture measure (images/contract.py
    legibility + reasons); no row still carries plate_score; a synthetic grey crop is illegible."""
    from images import contract as C
    bad = []
    grey, _col = ctx.get("synth") or _synth_jpgs()
    g = C.legibility_score(grey, {"id": "img-modis-crop-synthetic", "crop": "event"})
    if g.get("legible"):
        bad.append(f"grey crop judged legible ({g.get('legibility')})")
    man = json.loads(run["raw"].get("img/manifest.json") or "[]")
    for m in man:
        if "plate_score" in m or "plate_ok" in m:
            bad.append(f"manifest {m.get('id')}: still carries plate_score")
        if not isinstance(m.get("legibility"), (int, float)) or not m.get("legibility_reasons") and not m.get("legible"):
            bad.append(f"manifest {m.get('id')}: no legibility / reasons")
    return bad[:25]


def t_display_ready(run, ctx):
    """R20: no raw 'NNNN.0' in any number line/source_value; series.latest and place numbers carry display; N1 alfred link is dated and counts revisions."""
    bad = []
    for w in writes(run):
        if w["collection"] != "series":
            continue
        d = w["data"]; nr = d.get("number_rung") or {}
        for k in ("line", "source_value"):
            if re.search(r"(?<![\d.])\d{4,}\.0\b", str(nr.get(k) or "")):
                bad.append(f"{d['id']}: raw float in number_rung.{k}: {nr.get(k)[:50]}")
        if not (d.get("latest") or {}).get("display"):
            bad.append(f"{d['id']}: latest.display missing")
        if nr.get("rung") == "N1" and nr.get("alfred_url") and "vintage_date=" not in nr["alfred_url"]:
            bad.append(f"{d['id']}: N1 alfred_url not dated to the vintage")
        if nr.get("rung") == "N1" and "revisions" not in nr:
            bad.append(f"{d['id']}: N1 without revisions field")
    return bad[:25]


def synth_apply():
    """Run the real apply_versions.py on synthetic batches; return what came out."""
    import apply_versions
    ws = [{"op": "set", "collection": "series", "doc_id": f"s{i}", "data": {"x": "y" * (300_000 if i % 20 == 0 else 2000)}} for i in range(130)]
    ws += [{"op": "set", "collection": "meta", "doc_id": "run", "data": {"x": 1}}]
    versions = {f"series/s{i}": 100 + i for i in range(0, 130, 3)}; versions["meta/run"] = 7; versions["gone/doc"] = 1
    with tempfile.TemporaryDirectory() as d:
        for i in range(0, len(ws), 60):
            json.dump(ws[i:i + 60], open(os.path.join(d, f"batch_{i // 60 + 1:03d}.json"), "w"))
        vf = os.path.join(d, "versions.json"); json.dump(versions, open(vf, "w"))
        import io, contextlib
        with contextlib.redirect_stdout(io.StringIO()):
            apply_versions.main(["--out", d, "--versions", vf])
        files = sorted(glob.glob(os.path.join(d, "batch_*.json")))
        batches = [json.load(open(f)) for f in files]; sizes = [os.path.getsize(f) for f in files]
    return {"input": ws, "versions": versions, "batches": batches, "sizes": sizes}


def t_apply_versions(run, ctx):
    r = run.get("apply")
    if not r:
        return ["no apply_versions result"]
    bad, out = [], [w for b in r["batches"] for w in b]
    if [(w["collection"], w["doc_id"]) for w in out] != [(w["collection"], w["doc_id"]) for w in r["input"]]:
        bad.append("writes lost, added or reordered")
    for w in out:
        key = f"{w['collection']}/{w['doc_id']}"
        if key in r["versions"] and w.get("if_version") != r["versions"][key]:
            bad.append(f"{key}: expected if_version {r['versions'][key]}, got {w.get('if_version')}")
        if key not in r["versions"] and "if_version" in w:
            bad.append(f"{key}: new doc carries if_version")
    for i, (b, sz) in enumerate(zip(r["batches"], r["sizes"]), 1):
        if len(b) > 50:
            bad.append(f"batch {i}: {len(b)} writes > 50")
        if sz > 900 * 1024:
            bad.append(f"batch {i}: {sz} bytes > 900 KB")
    if len(r["batches"]) < 3:
        bad.append("expected the 300 KB docs to force several batches")
    return bad[:25]


# ---------------------------------------------------------------- tests added for the data fixes (heads, NWS extent, names, relevance, dedupe, hourly news, brief)
def poly_points(g):
    if not g:
        return 0
    polys = [g["coordinates"]] if g["type"] == "Polygon" else g["coordinates"]
    return sum(len(r) for poly in polys for r in poly)


def t_heads(run, ctx):
    """places/*.who_runs (P10): a name only from an official page read in this run; otherwise name and kind are "not confirmed".
    A crowd-edited (Wikidata) value is never the printed name."""
    ev = run["report"].get("official_heads") or {}
    bad = []
    for w in writes(run):
        if w["collection"] != "places":
            continue
        pid, who = w["doc_id"], w["data"]["who_runs"]
        e = ev.get(pid)
        kind, name, src_ = who.get("kind"), who.get("name"), who.get("source", "")
        if pid == "world":
            continue
        if kind not in ("official", "not confirmed"):
            bad.append(f"{pid}: who_runs.kind {kind!r} (only official or not confirmed)")
        if e and e.get("readable") and e.get("name"):
            if kind != "official" or name != e["name"]:
                bad.append(f"{pid}: official page {e['url']} says {e['name']!r} but who_runs is {name!r} ({kind})")
        if kind == "official" and not (e and e.get("readable") and e.get("name") == name):
            bad.append(f"{pid}: labelled official without a matching page read")
        if kind == "not confirmed" and name != "not confirmed":
            bad.append(f"{pid}: kind not confirmed but name {name!r}")
    if not ev:
        bad.append("report has no official_heads evidence")
    return bad[:25]


def t_nws_extent(run, ctx):
    """every alert has an extent: a polygon of <= 200 points, or geom null with a geom_reason."""
    bad, n = [], 0
    for coll, did, it in items_of(run):
        if it.get("kind") != "alert":
            continue
        n += 1
        pl = it["place"]
        g = pl.get("geom")
        if g:
            if g.get("type") not in ("Polygon", "MultiPolygon"):
                bad.append(f"{it['id']}: geom type {g.get('type')}")
            if poly_points(g) > 200:
                bad.append(f"{it['id']}: {poly_points(g)} points > 200")
        elif "geom" not in pl or not pl.get("geom_reason"):
            bad.append(f"{it['id']}: no geom and no geom_reason ({it['title'][:40]})")
    return bad[:25]


def t_no_truncation(run, ctx):
    """no title / place name / headline cut with an ellipsis; Maps query = full place name + its state or country."""
    from urllib.parse import urlparse, parse_qs
    bad = []
    cut = lambda x: isinstance(x, str) and (x.rstrip().endswith("\u2026") or x.rstrip().endswith("..."))
    for coll, did, it in items_of(run):
        for label, val in (("title", it.get("title")), ("place.name", (it.get("place") or {}).get("name"))):
            if cut(val):
                bad.append(f"{it['id']} {label} is cut: {val[-30:]!r}")
        pl = it.get("place") or {}
        gm = pl.get("gmaps")
        if gm and pl.get("admin") and (pl.get("name") or "").strip().lower() not in ("world", "not stated", "united states"):
            q = parse_qs(urlparse(gm).query).get("query", [""])[0]
            first = (it.get("areas") or [pl["name"]])[0]
            if not (first in q and pl["admin"] in q) and not re.fullmatch(r"-?[\d.]+,-?[\d.]+", q):
                bad.append(f"{it['id']}: maps query {q[:50]!r} lacks full name {first[:30]!r} + {pl['admin']}")
    for w in writes(run):
        if w["collection"] == "brief":
            for i in w["data"]["items"]:
                if cut(i["headline"]):
                    bad.append("brief headline cut")
        if w["collection"] in ("news", "live"):
            cl = w["data"].get("clusters") or w["data"].get("news") or []
            for c in cl:
                if cut(c["title"]):
                    bad.append(f"news cluster title cut: {c['title'][-30:]!r}")
    return bad[:25]


def t_touches_you(run, ctx):
    """every item has touches_you {yes: bool, how: str}; distant weather is never yes; Indiana / Marion items are yes."""
    bad = []
    for coll, did, it in items_of(run):
        t = it.get("touches_you")
        if not (isinstance(t, dict) and isinstance(t.get("yes"), bool) and isinstance(t.get("how"), str) and t["how"]):
            bad.append(f"{it['id']}: touches_you missing or malformed {t!r}"); continue
        adm = (it.get("place") or {}).get("admin")
        if it["kind"] in ("alert", "storm", "storm_report", "lsr") and adm != "Indiana" and it["layer"] not in ("local", "indiana") and t["yes"]:
            bad.append(f"{it['id']}: distant weather ({it['title'][:40]}) marked for you")
        if (it["layer"] in ("local", "indiana") or adm == "Indiana") and not t["yes"]:
            bad.append(f"{it['id']}: Indiana item not marked for you")
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                if not isinstance(c.get("touches_you"), dict):
                    bad.append(f"news cluster {c['id']}: no touches_you")
        if w["collection"] == "live":
            for c in w["data"].get("news", []):
                if not isinstance(c.get("touches_you"), dict):
                    bad.append(f"live news {c['id']}: no touches_you")
    return bad[:25]


def news_clusters(run):
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                yield w["collection"], c
        elif w["collection"] == "live":
            for c in w["data"].get("news", []):
                yield "live", c


def norm_host(h):
    h = (h or "").lower().strip()
    h = h[4:] if h.startswith("www.") else h
    return reg(h)


OLD_HOW = ("Names a country you track", "Weather outside", "No link to Indiana", "Reaches gig", "About jobs and the labor", "A federal action with direct dollar")
ADJ = {"Illinois", "Ohio", "Kentucky", "Michigan"}
HAZ = {"alert", "storm", "storm_report", "lsr", "quake", "disaster", "fire", "volcano", "flood", "tsunami"}


def km_indy(p):
    import math
    la, lo = p.get("lat"), p.get("lon")
    if not isinstance(la, (int, float)) or not isinstance(lo, (int, float)):
        return None
    p1, p2 = math.radians(39.7684), math.radians(la); a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lo + 86.1581) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def t_touches_you_strict(run, ctx):
    """touches_you.yes only for Indiana/Marion, hazards in IN/adjacent/<=300 km, national US federal items, or US-wide price/benefit stories; never a foreign country mention or old generic reasons."""
    bad = []
    def chk(tag, iid, it):
        t = it.get("touches_you")
        if not (isinstance(t, dict) and t.get("yes") is True):
            return
        how = t.get("how") or ""
        pl = it.get("place") or {}; adm = pl.get("admin"); layer = it.get("layer"); kind = it.get("kind", "news")
        if any(how.startswith(o) for o in OLD_HOW) or len(how) < 8:
            bad.append(f"{tag} {iid}: weak or generic reason {how!r}")
        indiana = layer in ("local", "indiana") or adm == "Indiana"
        if kind in HAZ:
            d = km_indy(pl)
            if not (indiana or adm in ADJ or (d is not None and d <= 300)):
                bad.append(f"{tag} {iid}: {kind} yes but not in Indiana/adjacent/near ({adm}, {None if d is None else round(d)} km)")
        elif kind in ("public_inspection", "nomination", "bill", "vote", "ruling", "declaration", "outbreak", "space_weather") and not indiana:
            bad.append(f"{tag} {iid}: {kind} outside Indiana marked for you")
        elif not indiana:
            if layer == "world" and not re.search(r"U\.S\.|US|United States", how):
                bad.append(f"{tag} {iid}: foreign item yes without stated US effect ({how!r})")
            elif layer == "us" and adm not in (None, "United States"):
                bad.append(f"{tag} {iid}: item in {adm} marked for you")
    for coll, did, it in items_of(run):
        chk(coll, it["id"], it)
    for coll, c in news_clusters(run):
        chk(coll, c["id"], {"touches_you": c.get("touches_you"), "layer": c.get("layer"), "place": {"admin": None if c.get("layer") != "us" else "United States"}, "kind": "news"})
    return bad[:25]


def t_truth_consistent(run, ctx):
    """cluster truth agrees with distinct publisher hosts (www. stripped, registrable domain): confirmed iff >= 2, single iff exactly 1; item hosts agree; report counts agree."""
    bad = []
    n_conf = 0
    for coll, c in news_clusters(run):
        hosts = {norm_host(h) for h in c.get("publishers", [])}
        if c["truth"] == "confirmed":
            n_conf += 1
        if len(hosts) >= 2 and c["truth"] != "confirmed":
            bad.append(f"{coll} {c['id']}: {len(hosts)} distinct hosts but truth {c['truth']!r}")
        if len(hosts) == 1 and c["truth"] != "single":
            bad.append(f"{coll} {c['id']}: one host but truth {c['truth']!r}")
        if c["truth"] == "confirmed" and len(hosts) < 2:
            bad.append(f"{coll} {c['id']}: confirmed with hosts {sorted(hosts)}")
        ih = {norm_host(urlparse(i["url"]).netloc) for i in c.get("items", [])}
        if len(c.get("items", [])) == c.get("n") and ih != hosts:
            bad.append(f"{coll} {c['id']}: items hosts {sorted(ih)} differ from publishers {sorted(hosts)}")
    rep = run["report"]
    if run["mode"] == "hourly" and "news" in rep:
        live_conf = sum(1 for coll, c in news_clusters(run) if coll == "live" and c["truth"] == "confirmed")
        if rep["news"].get("confirmed_in_list") != live_conf:
            bad.append(f"report confirmed_in_list {rep['news'].get('confirmed_in_list')} != {live_conf} confirmed clusters in the live doc")
    if run["mode"] == "morning" and rep.get("clusters"):
        if rep["clusters"].get("confirmed", 0) < n_conf:
            bad.append(f"report clusters.confirmed {rep['clusters'].get('confirmed')} < {n_conf} confirmed clusters written")
    return bad[:25]


def t_nws_dedupe(run, ctx):
    """same NWS event type + same office within 6 h -> one item; count and areas[] present and consistent."""
    from engine import parse_time
    bad, by = [], defaultdict(list)
    for coll, did, it in items_of(run):
        if it.get("kind") != "alert":
            continue
        if not isinstance(it.get("count"), int) or it["count"] < 1 or not isinstance(it.get("areas"), list) or not it["areas"]:
            bad.append(f"{it['id']}: count/areas missing"); continue
        by[(it.get("event"), it.get("office"))].append(it)
    for k, L in by.items():
        seen = {}
        for it in L:
            seen[it["id"]] = it
        L = sorted(seen.values(), key=lambda i: i["t"])
        for a, b in zip(L, L[1:]):
            if (parse_time(b["t"]) - parse_time(a["t"])).total_seconds() <= 6 * 3600:
                bad.append(f"{k}: two items {a['id']} / {b['id']} within 6 h (should be one)")
    return bad[:25]


def t_live_news(run, ctx):
    """hourly: live doc carries news clusters from the last 2 h; confirmed only with >= 2 distinct hosts; each doc < 190 KB."""
    from engine import parse_time
    bad = []
    live = [w for w in writes(run) if w["collection"] == "live"]
    if not live:
        return ["no live doc"]
    st = parse_time(live[0]["data"]["started"])
    for w in live:
        if "news" not in w["data"]:
            bad.append(f"live/{w['doc_id']}: no news list")
        if len(json.dumps(w["data"], ensure_ascii=False).encode()) > 190 * 1024:
            bad.append(f"live/{w['doc_id']}: over 190 KB")
        for c in w["data"].get("news", []):
            hosts = {reg(h) for h in c["publishers"]}
            if c["truth"] == "confirmed" and len(hosts) < 2:
                bad.append(f"news {c['id']}: confirmed with hosts {sorted(hosts)}")
            newest = max(parse_time(i["t"]) for i in c["items"])
            if (st - newest).total_seconds() > 2 * 3600 + 60:
                bad.append(f"news {c['id']}: newest article {newest} is older than 2 h")
            if any(parse_time(i["t"]) > st + __import__("datetime").timedelta(minutes=5) for i in c["items"]):
                bad.append(f"news {c['id']}: article from the future")
    return bad[:25]


def t_cluster_same_event(run, ctx):
    """items share a cluster only when they are the same event: no two articles in a cluster name different sub-places ("Eastern Region" vs "Riyadh region") or each name a place/actor the other never mentions, and the cluster spans no more than the engine's event window."""
    from engine import event_conflict, proper_set, parse_time, EVENT_SPAN_H
    cls = [c for _, c in news_clusters(run)]
    proper = proper_set([])   # corpus-free: country and state names only (the engine also uses corpus-learned names, so this is the weaker, reproducible half of its rule)
    bad = []
    seen = set()
    for c in cls:
        if c["id"] in seen:
            continue
        seen.add(c["id"])
        its = c.get("items", [])
        ts = [parse_time(i["t"]) for i in its if parse_time(i["t"])]
        if ts and (max(ts) - min(ts)).total_seconds() > EVENT_SPAN_H * 3600 + 60:
            bad.append(f"{c['id']}: articles span {(max(ts) - min(ts)).total_seconds() / 3600:.0f} h, wider than {EVENT_SPAN_H} h")
        for x in range(len(its)):
            for y in range(x + 1, len(its)):
                a, b = its[x], its[y]
                if event_conflict(a["title"], a.get("summary"), b["title"], b.get("summary"), proper):
                    bad.append(f"{c['id']}: different events merged: {a['title'][:55]!r} vs {b['title'][:55]!r}")
                    break
            else:
                continue
            break
    return bad[:25]


def t_health_mode(run, ctx):
    """health states which run and mode produced it and its own total: meta/health + meta/run (morning) or the live doc head (hourly) carry mode, total, ok and a label that agrees with the sources listed and with report.json, so a page can say '29 of 31 hourly sources' and never mix it with morning totals."""
    bad = []
    rep = run["report"]; mode = run["mode"]
    word = "hourly" if mode == "hourly" else "morning"
    def check(tag, d, listed):
        if d.get("mode") != mode:
            bad.append(f"{tag}: mode {d.get('mode')!r} != run mode {mode!r}")
        if d.get("run") != rep.get("run"):
            bad.append(f"{tag}: run {d.get('run')!r} != report run {rep.get('run')!r}")
        if d.get("total") != listed or d.get("total") != rep.get("total_sources"):
            bad.append(f"{tag}: total {d.get('total')} vs {listed} sources listed vs report {rep.get('total_sources')}")
        ok = d.get("ok")
        if ok != rep.get("ok_sources"):
            bad.append(f"{tag}: ok {ok} != report ok_sources {rep.get('ok_sources')}")
        lab = d.get("label") or ""
        w_ = "morning" if d.get("written_by") == "morning" else word      # M4: the live doc a morning-mode run writes counts that run's sources
        if lab != f"{d.get('ok')} of {d.get('total')} {w_} sources returned data":
            bad.append(f"{tag}: label {lab!r} does not state '{d.get('ok')} of {d.get('total')} {w_} sources'")
    if mode == "morning":
        docs = {w["doc_id"]: w["data"] for w in writes(run) if w["collection"] == "meta"}
        h, r = docs.get("health"), docs.get("run")
        if not h or not r:
            return ["meta/health or meta/run missing"]
        check("meta/health", h, len(h["sources"]))
        check("meta/run", r, len(h["sources"]))
        if h.get("ok") is not None and h["ok"] != sum(1 for x in h["sources"] if x["status"] == "data"):
            bad.append("meta/health: ok does not match sources with status data")
        if h.get("totals", {}).get("sources") != h.get("total"):
            bad.append("meta/health: totals.sources != total")
    else:
        live = [w for w in writes(run) if w["collection"] == "live"]
        if not live:
            return ["no live doc"]
        for w in live:
            d = w["data"]
            if "health" in d:
                check(f"live/{w['doc_id']}", d, len(d["health"]))
                if d["ok"] != sum(1 for x in d["health"] if x["status"] == "data"):
                    bad.append(f"live/{w['doc_id']}: ok does not match sources with status data")
            else:
                check(f"live/{w['doc_id']}", d, rep.get("total_sources"))
    return bad[:25]


TELEMETRY = re.compile(r"sources returned data|this (?:morning|hourly) run|This run holds|news clusters|source\(s\)|publisher\(s\)", re.I)


def t_brief_skeleton(run, ctx):
    """bottom_line is an item's plain sentence, not run telemetry; likelihood/confidence are null, never strings."""
    ids = {it["id"]: it for _, _, it in items_of(run)}
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                ids.setdefault(c["id"], {"plain": c["plain"], "title": c["title"]})
    bad = []
    for w in writes(run):
        if w["collection"] != "brief":
            continue
        d = w["data"]
        if TELEMETRY.search(d["bottom_line"]):
            bad.append(f"bottom_line is telemetry: {d['bottom_line'][:60]!r}")
        if d["items"]:
            top = ids.get(d["items"][0]["refs"][0])
            sents = engine_sentences((top or {}).get("plain", ""))
            if top is None or not sents or d["bottom_line"] != (sents[0] if sents[0][-1] in ".!?" else sents[0] + "."):
                bad.append("bottom_line is not the first plain sentence of the top brief item")
        for i in d["items"]:
            for k in ("likelihood", "confidence"):
                if i.get(k) is not None:
                    bad.append(f"brief item {i['headline'][:30]!r}: {k} is {i.get(k)!r}, must be null")
            if TELEMETRY.search(i["so_what"]):
                bad.append(f"brief item so_what has lint: {i['so_what'][:50]!r}")
    return bad[:25]


def engine_sentences(t):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import engine as E
    return E.sentences(t)


DANGLING = re.compile(r"(?:\b(?:[A-Z]\.){1,3}[A-Z]|\b(?:Gov|Sen|Rep|Dr|Mr|Mrs|St|Jan|Feb|Aug|Sept?|Oct|Nov|Dec)|\bM\d)$")

def t_text_split(run, ctx):
    """titles and brief headlines never end mid-abbreviation ("...that U.S"); quake titles read 'M5.1 earthquake in/near/N miles',
    with no km and no dash; a brief item's so_what never restates its headline."""
    bad = []
    for coll, did, it in items_of(run):
        t = (it.get("title") or "").rstrip()
        if DANGLING.search(t):
            bad.append(f"{it['id']} title ends mid-abbreviation: {t[-30:]!r}")
        if it.get("kind") == "quake" and it.get("title", "").startswith("M") and (re.search(r"\bkm\b|\u2014|\u2013", it["title"]) or not re.match(r"^M\d+(\.\d)? earthquake( (in the|near|[\d,]+ miles) |$)", it["title"])):
            bad.append(f"{it['id']} quake title shape: {it['title'][:50]!r}")
    for w in writes(run):
        if w["collection"] == "brief":
            for i in w["data"]["items"]:
                h = i["headline"].rstrip()
                if DANGLING.search(h):
                    bad.append(f"brief headline cut mid-abbreviation: {h[-30:]!r}")
                if h and i["so_what"].startswith(h.rstrip(".")[:40]):
                    bad.append(f"brief so_what restates headline: {h[:40]!r}")
    return bad[:25]


# ---------------------------------------------------------------- round 15: receipts, backoff without block retry, SPC never in the future

PAGE_STATES = {"matched", "blocked", "mismatch", "unreadable", "not_checked"}


def t_receipts(run, ctx):
    """every cited news item carries the publisher's own feed item as first proof (feed url, fetched_at, verbatim feed title) and one page state; a mismatch is dropped, never written; a block says so and is never retried; every numeric series has fetched_at, a kept one keeps its original fetched_at and says 'kept from ...'; glossary quotes are present or honestly absent."""
    bad = []
    n = ok = 0
    for coll, c in news_clusters(run):
        for i in c.get("items", []):
            n += 1
            r = i.get("receipt") or {}
            if r.get("feed", "").startswith("http") and r.get("feed_at") and (r.get("feed_title") or "").strip():
                ok += 1
            if r and r.get("page") not in PAGE_STATES:
                bad.append(f"{coll} {c['id']}: page state {r.get('page')!r}")
            if r.get("page") == "mismatch":
                bad.append(f"{coll} {c['id']}: a mismatching page was written instead of dropped")
            if r.get("page") == "blocked" and not (re.search(r"block(s|ed) automated reading", r.get("note", "")) and ("its own feed at" in r.get("note", "") or "feed copy kept" in r.get("note", ""))):
                bad.append(f"{coll} {c['id']}: blocked page without the plain statement")
            if r.get("page") == "blocked" and r.get("page_http") not in (403, 406, 451):
                bad.append(f"{coll} {c['id']}: blocked with http {r.get('page_http')}")
            if r.get("feed_title") and re.sub(r"\W+", "", i["title"].lower())[:25] not in re.sub(r"\W+", "", re.sub(r"<[^>]+>", "", r["feed_title"]).lower()) and "&" not in r["feed_title"]:
                bad.append(f"{coll} {c['id']}: feed_title does not match the item title: {r['feed_title'][:40]!r}")
    if n and ok / n < 0.95:
        bad.append(f"only {ok} of {n} news items carry feed + feed_at + feed_title (need 95%)")
    for w in writes(run):
        if w["collection"] == "series":
            d = w["data"]
            if not parse_iso(d.get("fetched_at")):
                bad.append(f"series/{w['doc_id']}: no fetched_at")
            if d.get("kept") and (not str(d.get("kept_label", "")).startswith("kept from ") or d.get("fetched_at") == run["report"].get("started")):
                bad.append(f"series/{w['doc_id']}: kept without its original fetched_at and 'kept from' label")
        if w["collection"] == "glossary":
            for t in w["data"].get("terms", []):
                if t.get("quote_status") == "found" and not (t.get("quote") or "").strip():
                    bad.append(f"glossary {t['term']}: found but empty quote")
                if t.get("quote_status") != "found" and t.get("quote"):
                    bad.append(f"glossary {t['term']}: quote kept although status {t.get('quote_status')}")
    return bad[:25]


def parse_iso(x):
    try:
        import datetime as _d
        return _d.datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None


class _Resp:
    def __init__(self, code):
        self.status_code, self.headers = code, {"Content-Type": "text/html"}
    def iter_content(self, n):
        yield b"x"
    def close(self):
        pass


class _Sess:
    def __init__(self, seq):
        self.seq, self.calls, self.headers = list(seq), 0, {}
    def request(self, *a, **k):
        self.calls += 1
        v = self.seq.pop(0) if self.seq else 200
        if isinstance(v, Exception):
            raise v
        return _Resp(v)


class BadFetcherMod:
    """A fetcher that retries a block: used only by --demo to show the test failing."""
    @staticmethod
    def make(engine):
        class Bad(engine.Fetcher):
            def get(self, url, **kw):
                resp = None
                for a in range(4):
                    resp = super().get(url, **dict(kw, tries=1))
                    if resp.ok:
                        break
                    self.sleep(1)
                return resp
        return Bad


def t_backoff_no_block_retry(run, ctx):
    """timeouts and 5xx wait 2, 4, 8 s and retry; 403/406/451 (and any 4xx) are asked exactly once and never retried."""
    import engine, requests
    cls = run.get("bad_fetcher") and BadFetcherMod.make(engine) or engine.Fetcher
    bad = []
    def go(seq, **kw):
        F = cls(); sl = []; F.sleep = lambda x: sl.append(x)
        sess = _Sess(seq); F.session_factory = lambda: sess
        r = F.get("https://example.invalid/x", gap=0, **kw)
        return r, sess.calls, [x for x in sl if x >= 1]
    to = requests.exceptions.Timeout("t")
    r, calls, sl = go([to, to, to, to])
    if sl != [2, 4, 8] or calls != 4:
        bad.append(f"timeouts: waits {sl} over {calls} calls, want [2, 4, 8] over 4")
    r, calls, sl = go([503, 500, 200])
    if sl != [2, 4] or calls != 3 or not r.ok:
        bad.append(f"5xx then ok: waits {sl}, calls {calls}, ok {r.ok}")
    for code in (403, 406, 451, 404):
        r, calls, sl = go([code, 200, 200, 200])
        if calls != 1 or sl:
            bad.append(f"HTTP {code}: {calls} calls and waits {sl}; a block or 4xx must be asked once")
    return bad


def t_spc_no_future(run, ctx):
    """no SPC storm report is timed after the moment it was fetched, whatever the HHMM and the convective-day roll."""
    import engine, datetime as dtm
    bad = []
    started = parse_iso(run["report"].get("started"))
    for coll, did, it in items_of(run):
        if it.get("kind") != "storm_report":
            continue
        for k in ("t", "updated"):
            tv = parse_iso(it.get(k))
            if tv and started and tv > started:
                bad.append(f"{coll}/{did} {it['id']}: {k} {it.get(k)} is after the run started {run['report'].get('started')}")
    fn = run.get("spc_time_fn") or engine.spc_time
    for now in (dtm.datetime(2026, 10, 31, 0, 5, tzinfo=dtm.timezone.utc), dtm.datetime(2026, 10, 31, 11, 59, tzinfo=dtm.timezone.utc),
                dtm.datetime(2026, 10, 31, 12, 30, tzinfo=dtm.timezone.utc), dtm.datetime(2026, 10, 31, 18, 10, tzinfo=dtm.timezone.utc), dtm.datetime(2026, 10, 31, 23, 59, tzinfo=dtm.timezone.utc)):
        for hh in range(0, 24):
            for mm in (0, 30, 59):
                d = fn(f"{hh:02d}{mm:02d}", now)
                if d is not None and d > now:
                    bad.append(f"spc_time({hh:02d}{mm:02d}) at {now:%H:%MZ} gives {d:%d %H:%MZ}, in the future")
                    break
    return bad[:25]


# (name, fn, mode it applies to; None = any)
def _norm_t(x):
    return re.sub(r"[^a-z0-9]+", " ", re.sub(r"<[^>]+>", "", x or "").lower()).strip()


def t_proof_ladder(run, ctx):
    """every news item carries exactly one rung, computed from its receipts: R1 page matched | R2 feed headline + feed summary matched | R3 feed headline only | R4 unverified (carries UNVERIFIED).
    The cluster rung is the best of its items; a proof line is printed for each; meta/run.ladder (morning) counts the clusters written."""
    bad = []; n = 0; cnt = defaultdict(int)
    for coll, c in news_clusters(run):
        n += 1
        rg = c.get("rung")
        if rg not in ("R1", "R2", "R3", "R4"):
            bad.append(f"{coll} {c['id']}: rung {rg!r}"); continue
        cnt[rg] += 1
        if not (c.get("proof") or "").strip():
            bad.append(f"{coll} {c['id']}: no proof line")
        if rg == "R4" and "UNVERIFIED" not in (c.get("proof") or ""):
            bad.append(f"{coll} {c['id']}: R4 without UNVERIFIED")
        rungs = []
        for i in c.get("items", []):
            r = i.get("receipt") or {}
            ig = r.get("rung"); rungs.append(ig)
            if ig not in ("R1", "R2", "R3", "R4") or not (r.get("proof") or "").strip():
                bad.append(f"{coll} {c['id']}: item without rung/proof"); continue
            fs = _norm_t(r.get("feed_summary")); sm = _norm_t(re.sub(r"[\u2026.]+\s*$", "", i.get("summary") or ""))
            if ig == "R1" and r.get("page") != "matched":
                bad.append(f"{coll} {c['id']}: R1 but page {r.get('page')!r}")
            if ig != "R1" and r.get("page") == "matched":
                bad.append(f"{coll} {c['id']}: page matched but {ig}")
            if ig == "R2" and not (len(sm) >= 20 and sm in fs):
                bad.append(f"{coll} {c['id']}: R2 without a matching feed summary")
            if ig == "R3" and len(sm) >= 20 and sm in fs and r.get("page") != "matched":
                bad.append(f"{coll} {c['id']}: R3 although the feed summary matches (should be R2)")
            if ig in ("R2", "R3") and not (r.get("feed_title") or "").strip():
                bad.append(f"{coll} {c['id']}: {ig} without a feed headline")
            if ig == "R4" and "UNVERIFIED" not in r.get("proof", ""):
                bad.append(f"{coll} {c['id']}: item R4 without UNVERIFIED")
        if rungs and all(x in ("R1", "R2", "R3", "R4") for x in rungs) and rg > min(rungs):
            bad.append(f"{coll} {c['id']}: cluster rung {rg} is worse than its best item {min(rungs)}")
    for w in writes(run):
        if w["collection"] == "meta" and w["doc_id"] == "run" and run["mode"] == "morning":
            ld = w["data"].get("ladder") or {}
            morning_n = sum(1 for coll, c in news_clusters(run) if coll == "news")
            if ld.get("items") != morning_n or sum(ld.get(k, 0) for k in ("R1", "R2", "R3", "R4")) != morning_n:
                bad.append(f"meta/run ladder {ld} does not count the {morning_n} news clusters written")
            for k in ("R1", "R2", "R3", "R4"):
                real = sum(1 for coll, c in news_clusters(run) if coll == "news" and c.get("rung") == k)
                if ld.get(k) != real:
                    bad.append(f"meta/run ladder {k}={ld.get(k)} but {real} clusters carry it")
    if n == 0:
        bad.append("no news clusters in this output")
    return bad[:25]


def t_not_checked_zero(run, ctx):
    """morning: no cited news article is left not_checked. A page was asked (twice if it did not answer), was blocked (403/406/451, once), or is called unreadable. Never zero published pages with no state."""
    bad = []; n = 0
    for coll, c in news_clusters(run):
        for i in c.get("items", []):
            r = i.get("receipt") or {}
            n += 1
            if r.get("page") == "not_checked":
                bad.append(f"{coll} {c['id']}: page not_checked ({i['title'][:40]!r})")
            if r.get("page") == "unreadable" and not r.get("note"):
                bad.append(f"{coll} {c['id']}: unreadable without a reason")
    for w in writes(run):
        if w["collection"] == "news":
            for c in w["data"]["clusters"]:
                for i in c["items"]:
                    for s_ in []:
                        pass
    if n == 0:
        bad.append("no news items")
    return bad[:25]


def t_gloss_requires_quote(run, ctx):
    """a glossary term may be glossed only when quote_status is found with a verbatim sentence, a time and an http source; every other term is marked unverified and carries no quote."""
    bad = []
    for w in writes(run):
        if w["collection"] != "glossary":
            continue
        ts = w["data"].get("terms", [])
        for t in ts:
            ok = t.get("quote_status") == "found" and len((t.get("quote") or "").strip()) >= 40 and bool(t.get("quote_at")) and str(t.get("source", "")).startswith("http")
            if t.get("glossed") and not ok:
                bad.append(f"glossary {t['term']}: glossed without a found quote")
            if ok and not t.get("glossed"):
                bad.append(f"glossary {t['term']}: quote found but glossed false")
            if not t.get("glossed") and (t.get("quote") or not t.get("unverified")):
                bad.append(f"glossary {t['term']}: not glossed but not marked unverified, or still carries a quote")
        if w["data"].get("glossed") != sum(1 for t in ts if t.get("glossed")):
            bad.append("glossary: glossed count does not match the terms")
    return bad[:25]


def t_newer_value_check(run, ctx):
    """at publish every FRED series was re-read at its tail: tail_checked_at is set and the stored latest is not older than the newest dated value FRED held at that time (sp500-class: no stale-by-hours number)."""
    bad = []; n = 0
    for w in writes(run):
        if w["collection"] != "series":
            continue
        d = w["data"]
        if d.get("kept") or "fred.stlouisfed.org" not in str((d.get("source") or {}).get("url", "")):
            continue
        n += 1
        if not parse_iso(d.get("tail_checked_at")):
            bad.append((fid_of(d), f"series/{w['doc_id']}: tail never re-read at publish")); continue
        tn = d.get("tail_newest")
        if not tn:
            bad.append((fid_of(d), f"series/{w['doc_id']}: tail re-read returned nothing")); continue
        if tn["date"] > d["latest"]["date"]:
            bad.append((None, f"series/{w['doc_id']}: FRED holds {tn['date']} but latest is {d['latest']['date']}"))
        if d["points"] and d["points"][-1][0] != d["latest"]["date"]:
            bad.append((None, f"series/{w['doc_id']}: latest is not the last point"))
    if n == 0:
        bad.append((None, "no FRED series in this output"))
    return split_skips(run, bad)[:25]


def t_number_rung(run, ctx):
    """every series carries number_rung N1-N4 with a source url (N4 says UNVERIFIED); N1 line matches vintages (unrevised/revised); CPI SA/NSA are N1."""
    bad = []; n = 0; miss = []
    for w in writes(run):
        if w["collection"] != "series":
            continue
        n += 1; d = w["data"]; r = d.get("number_rung") or {}
        if r.get("rung") not in ("N1", "N2", "N3", "N4"):
            bad.append(f"series/{w['doc_id']}: no rung"); continue
        if r["rung"] != "N4" and not r.get("source_url"):
            bad.append(f"series/{w['doc_id']}: {r['rung']} without source url")
        if r["rung"] == "N4" and "UNVERIFIED" not in r.get("line", ""):
            bad.append(f"series/{w['doc_id']}: N4 not labelled UNVERIFIED")
        if r["rung"] == "N1":
            v = d["vintages"][0]
            if ("unrevised" in r["line"]) != (v["first"]["value"] == v["current"]["value"]):
                bad.append(f"series/{w['doc_id']}: N1 line disagrees with vintages")
        if w["doc_id"] in ("cpiaucsl", "cpiaucns") and r["rung"] != "N1":
            miss.append((fid_of(d), f"series/{w['doc_id']}: expected N1, got {r['rung']}"))
    if n == 0:
        bad.append("no series in this output")
    return (bad + split_skips(run, miss))[:25]


def t_verify_all(run, ctx):
    """R18 no receipt, no row: every news item carries a receipt (rung R1-R3, source url, checked_at), every other item a first-hand http source with a fetch time, every series an N1-N3 rung with a source url, every glossary term a found quote, every place number a source url and a time; meta/run.dropped counts what the gate removed. The gate itself is run on a synthetic bad set and must drop every bad row."""
    import engine as E
    bad = []
    for coll, c in news_clusters(run):
        for i in c.get("items", []):
            if not E._rc_news_ok(i.get("receipt"), i.get("url")):
                bad.append(f"{coll} {c['id']}: news item without receipt ({(i.get('title') or '')[:40]!r})")
    for coll, did, it in items_of(run):
        if coll.startswith("live"):
            continue
        if not E._item_receipt_ok(it):
            bad.append(f"{coll}/{did}: item {it.get('id')} without receipt")
    md = None
    for w in writes(run):
        d = w["data"]
        if w["collection"] == "series" and ((d.get("number_rung") or {}).get("rung") not in ("N1", "N2", "N3") or not str((d.get("number_rung") or {}).get("source_url", "")).startswith("http")):
            bad.append(f"series/{w['doc_id']}: no receipt")
        elif w["collection"] == "places":
            for n in d.get("numbers", []):
                if not E._num_ok(n):
                    bad.append(f"places/{w['doc_id']}: number {n.get('label')!r} without source url or time")
        elif w["collection"] == "glossary":
            for t in d.get("terms", []):
                if t.get("quote_status") != "found":
                    bad.append(f"glossary {t.get('term')}: not found yet written")
        elif w["collection"] == "meta" and w["doc_id"] == "run":
            md = d
    if md is None or not isinstance((md.get("dropped") or {}).get("total"), int):
        bad.append("meta/run.dropped missing")
    now = "2026-10-31T12:00:00Z"
    import hashlib as _h
    rc = {"rung": "R2", "feed": "https://x.example/rss", "feed_at": now, "page_at": None, "excerpt": "a | b", "sha256": _h.sha256(b"a | b").hexdigest()}
    docs = [("news", "2026-10-31", {"clusters": [{"id": "n1", "items": [{"title": "a", "url": "https://x.example/a", "receipt": rc}, {"title": "b", "url": "https://x.example/b"}]}, {"id": "n2", "items": [{"title": "c", "url": "https://x.example/c", "receipt": dict(rc, rung="R4")}]}]}),
            ("glossary", "all", {"terms": [{"term": "T", "quote_status": "not_found", "quote": None, "glossed": False}], "glossed": 0}),
            ("series", "s1", {"number_rung": {"rung": "N4", "source_url": None}, "fetched_at": now}),
            ("places", "p", {"numbers": [{"label": "x", "value": 1, "source": "nowhere", "fetched_at": now}, {"label": "y", "value": 2, "source": "S https://s.example/y", "fetched_at": now}]}),
            ("wire", "2026-10-31", {"items": [{"id": "n3", "kind": "news", "sources": [{"url": "https://x.example/z"}]}, {"id": "u1", "kind": "quake", "sources": [{"url": "https://usgs.example/u", "fetched": now}]}]})]
    out, dr = E.verify_all(docs)
    left = {c: d for c, _i, d in out}
    if left.get("news", {}).get("clusters") != [{"id": "n1", "items": [docs[0][2]["clusters"][0]["items"][0]]}] or "glossary" not in left or left["glossary"]["terms"] or "series" in left or len(left["places"]["numbers"]) != 1 or [i["id"] for i in left["wire"]["items"]] != ["u1"] or dr["total"] != 7:
        bad.append(f"verify_all gate did not drop exactly the bad rows (dropped {dr.get('total')}, expected 7)")
    return bad[:25]


def t_receipt_proof(run, ctx):
    """R19 the receipt carries its own proof: every news receipt has a frozen excerpt (<=400), a sha256 that matches it, the raw item sha256, fetched_at; a rolled-off item re-proves from the stored excerpt; a tampered excerpt fails; one page state per URL; every number receipt has source_value + sha256, N1 has an ALFRED link, weekly repeats are flagged."""
    import engine as E, hashlib
    bad = []; states = {}
    for coll, c in news_clusters(run):
        for i in c.get("items", []):
            r = i.get("receipt") or {}
            ex = r.get("excerpt") or ""
            if not ex or len(ex) > 400 or hashlib.sha256(ex.encode()).hexdigest() != r.get("sha256") or not r.get("item_sha256") or not r.get("fetched_at"):
                bad.append(f"{coll} {c['id']}: receipt without a valid excerpt/hash/fetched_at"); continue
            ok, st = E.reprove_receipt(r, "unrelated text from a feed that has rolled over")
            if not ok or st != "proved_at_fetch_rolled_off":
                bad.append(f"{coll} {c['id']}: rollover did not re-prove from the stored excerpt ({st})")
            ok2, st2 = E.reprove_receipt(dict(r, excerpt=ex + " tampered"))
            if ok2 or st2 != "hash_mismatch":
                bad.append(f"{coll} {c['id']}: a tampered excerpt was not rejected")
            states.setdefault(i["url"], set()).add((r.get("page"), r.get("rung")))
    for u, st in states.items():
        if len(st) > 1:
            bad.append(f"{u[:60]}: more than one status {sorted(map(str, st))}")
    for w in writes(run):
        if w["collection"] != "series":
            continue
        d = w["data"]; r = d.get("number_rung") or {}
        if r.get("rung") in ("N1", "N2", "N3"):
            row = json.dumps([d["latest"]["date"], d["latest"]["value"]], separators=(",", ":"))
            if not r.get("source_value") or r.get("sha256") != hashlib.sha256((row + "|" + r["source_url"]).encode()).hexdigest():
                bad.append(f"series/{w['doc_id']}: number receipt without a matching source value hash")
            if r["rung"] == "N1" and not str(r.get("alfred_url", "")).startswith("https://alfred."):
                bad.append(f"series/{w['doc_id']}: N1 without an ALFRED link")
            if d.get("freq") == "weekly":
                pts = d["points"]; k = 1
                while k < len(pts) and pts[-1 - k][1] == d["latest"]["value"]:
                    k += 1
                if (k - 1) != r.get("repeats", 0):
                    bad.append(f"series/{w['doc_id']}: repeated weekly value not flagged")
    return bad[:25]


def t_ldata(run, ctx):
    """R22 L-DATA: every R1 news receipt archives the raw feed item body with a matching body_sha256; every N2/N3 number row carries a vintage line;
    bot-wall/challenge pages are labelled blocked (never matched or refuted) with the 'feed copy kept' note."""
    import engine as E, hashlib
    bad = []
    for coll, c in news_clusters(run):
        for i in c.get("items", []):
            r = i.get("receipt") or {}
            if r.get("rung") == "R1" and (not r.get("item_body") or hashlib.sha256(r["item_body"].encode()).hexdigest() != r.get("body_sha256")):
                bad.append(f"{coll} {c['id']}: R1 receipt without an archived body/body_sha256")
            if r.get("page") == "blocked" and r.get("note") != E.BLOCKED_NOTE:
                bad.append(f"{coll} {c['id']}: blocked page without the feed-copy-kept note")
    for w in writes(run):
        if w["collection"] == "series":
            r = w["data"].get("number_rung") or {}
            if r.get("rung") in ("N2", "N3") and not r.get("vintage_line"):
                bad.append(f"series/{w['doc_id']}: {r['rung']} without a vintage/revision line")
    cf = '<html><head><title>Just a moment...</title></head><body><div id="cf-challenge-running"></div></body></html>'
    art = "<html><head><title>Storm hits coast</title></head><body>" + " ".join(f"word{k}" for k in range(900)) + "</body></html>"
    for st, body, want in ((200, cf, True), (503, cf, True), (403, "", False), (200, art, False), (200, art + "<script>g-recaptcha</script>", False)):
        if E.is_challenge(st, body) != want:
            bad.append(f"is_challenge({st}, ...) should be {want}")
    return bad[:25]


TESTS = [("no_key", t_no_key, None), ("size_limits", t_size_limits, None), ("item_fields", t_item_fields, None), ("confirmed_hosts", t_confirmed_hosts, None),
         ("idempotent_ids", t_idempotent_ids, "morning"), ("no_fixture", t_no_fixture, None), ("brief_refs", t_brief_refs, "morning"),
         ("live_only", t_live_only, "hourly"), ("collections", t_collections, "morning"), ("static_once", t_static_once, "morning"),
         ("images", t_images, "morning"), ("apply_versions", t_apply_versions, None),
         ("heads", t_heads, "morning"), ("nws_extent", t_nws_extent, None), ("no_truncation", t_no_truncation, None), ("touches_you", t_touches_you, None),
         ("nws_dedupe", t_nws_dedupe, None), ("live_news", t_live_news, "hourly"), ("brief_skeleton", t_brief_skeleton, "morning"),
         ("text_split", t_text_split, None), ("touches_you_strict", t_touches_you_strict, None), ("truth_consistent", t_truth_consistent, None),
         ("cluster_same_event", t_cluster_same_event, None), ("health_mode", t_health_mode, None),
         ("receipts", t_receipts, None), ("proof_ladder", t_proof_ladder, None), ("not_checked_zero", t_not_checked_zero, "morning"), ("gloss_requires_quote", t_gloss_requires_quote, "morning"), ("newer_value_check", t_newer_value_check, "morning"), ("number_rung", t_number_rung, "morning"), ("backoff_no_block_retry", t_backoff_no_block_retry, None), ("spc_no_future", t_spc_no_future, None), ("verify_all", t_verify_all, "morning"), ("receipt_proof", t_receipt_proof, "morning"), ("legibility", t_legibility, "morning"), ("display_ready", t_display_ready, "morning"), ("ldata", t_ldata, "morning")]


def inject(run, name, run2):
    """Return deliberately bad copies (run, run2) for the named test."""
    r = copy.deepcopy(run); r2 = run2
    ws = writes(r)
    if name == "display_ready":
        for w in ws:
            if w["collection"] == "series":
                w["data"]["number_rung"]["line"] = "value 159044.0 (2026-09-01)"; w["data"]["latest"].pop("display", None); break
    if name == "legibility":
        man = json.loads(r["raw"]["img/manifest.json"] or "[]")
        for m in man:
            m["plate_score"] = 50.0
        if not man:
            man = [{"id": "x", "plate_score": 50.0}]
        r["raw"]["img/manifest.json"] = json.dumps(man)
    if name == "receipt_proof":
        for coll, c in news_clusters(r):
            c["items"][0]["receipt"]["excerpt"] += " edited"; break
        for w in ws:
            if w["collection"] == "series" and (w["data"].get("number_rung") or {}).get("sha256"):
                w["data"]["number_rung"]["sha256"] = "0" * 64; break
    elif name == "ldata":
        for coll, c in news_clusters(r):
            for i in c.get("items", []):
                rc = i.setdefault("receipt", {})
                rc.update(rung="R1", page="blocked", note="page blocks automated reading."); rc.pop("body_sha256", None); break
            break
        for w in ws:
            if w["collection"] == "series" and (w["data"].get("number_rung") or {}).get("rung") in ("N2", "N3"):
                w["data"]["number_rung"].pop("vintage_line", None); break
    elif name == "no_key":
        k = next(v for v in keys().values() if len(v) > 6)
        fn = sorted(r["raw"])[0]; r["raw"][fn] += k   # a key value leaks into a file
    elif name == "size_limits":
        first = next(iter(r["batches"].values())); first.append({"op": "set", "collection": "wire", "doc_id": "huge", "data": {"x": "y" * 230000, "_run": "x", "_at": "x", "v": 3}})
        fn = next(iter(r["batches"])); r["raw"][fn] = json.dumps(first)
    elif name == "item_fields":
        for coll, did, it in items_of(r):
            it["sources"] = []; break
    elif name == "confirmed_hosts":
        for coll, did, it in items_of(r):
            it["truth"] = "confirmed"; it["sources"] = it["sources"][:1]; break
    elif name == "idempotent_ids":
        r2 = copy.deepcopy(run2); w = writes(r2)[0]; w["doc_id"] = w["doc_id"] + "_renamed"
    elif name == "no_fixture":
        for coll, did, it in items_of(r):
            it["title"] = "Test item lorem ipsum"; break
    elif name == "live_only":
        w = next(w for w in ws if w["collection"] == "live"); w["collection"] = "meta"      # an overwritten doc sneaks in
    elif name == "collections":
        r["raw"]["collections.txt"] = "\n".join(l for l in r["raw"]["collections.txt"].split() if l != "series")
    elif name == "static_once":
        r["report"]["static_once"] = not r["report"].get("static_once")
    elif name == "images":
        for coll, did, it in items_of(r):
            it["img"] = "data:image/jpeg;base64,AAAA"; break
        man = json.loads(r["raw"]["img/manifest.json"])
        if man:
            man[0]["license"] = ""; r["raw"]["img/manifest.json"] = json.dumps(man)
    elif name == "apply_versions":
        w = next(w for b in r["apply"]["batches"] for w in b if "if_version" in w); del w["if_version"]
    elif name == "heads":
        for w in ws:
            if w["collection"] == "places" and w["doc_id"] == "indiana":   # the stale Wikidata value wins over the official page
                w["data"]["who_runs"] = {"name": "Eric Holcomb", "source": "Wikidata property P6 (head of government), crowd-edited", "kind": "crowd-edited"}
    elif name == "nws_extent":
        for coll, did, it in items_of(r):
            if it.get("kind") == "alert":
                it["place"]["geom"] = None; it["place"].pop("geom_reason", None); break
    elif name == "no_truncation":
        for coll, did, it in items_of(r):
            if it.get("kind") == "alert":
                it["title"] = it["title"][:20] + "\u2026"; it["place"]["name"] = it["place"]["name"][:12] + "..."; break
    elif name == "touches_you":
        for coll, did, it in items_of(r):
            if it.get("kind") == "alert" and it["layer"] == "us":
                it["touches_you"] = {"yes": True, "how": "Weather alert"}; break
        for coll, did, it in items_of(r):
            it.pop("touches_you", None); break
    elif name == "touches_you_strict":
        for coll, did, it in items_of(r):
            if it.get("kind") == "quake" and it["layer"] == "world":
                it["touches_you"] = {"yes": True, "how": "Names a country you track: Russia"}; break
        for w in ws:
            if w["collection"] in ("news", "live"):
                cl = w["data"].get("clusters") if w["collection"] == "news" else w["data"].get("news")
                hit = next((c for c in cl or [] if c.get("layer") == "world"), None)
                if hit:
                    hit["touches_you"] = {"yes": True, "how": "About fuel prices somewhere"}; break
    elif name == "truth_consistent":
        for w in ws:
            if w["collection"] in ("news", "live"):
                cl = w["data"].get("clusters") if w["collection"] == "news" else w["data"].get("news")
                multi = [c for c in cl or [] if len({norm_host(h) for h in c["publishers"]}) >= 2]
                if multi:
                    multi[0]["truth"] = "single"; break
    elif name == "nws_dedupe":
        for coll, did, it in items_of(r):
            if it.get("kind") == "alert":
                dup = copy.deepcopy(it); dup["id"] = it["id"] + "-dup"
                for w in ws:
                    lst = w["data"].get("events") if w["collection"] == "live" else (w["data"].get("items") if w["collection"] == "events" else None)
                    if lst is not None and any(x["id"] == it["id"] for x in lst):
                        lst.append(dup); break
                break
    elif name == "live_news":
        for w in ws:
            if w["collection"] == "live" and w["data"].get("news"):
                c = w["data"]["news"][0]; c["truth"] = "confirmed"; c["publishers"] = c["publishers"][:1]
                c["items"][0]["t"] = "2020-01-01T00:00:00Z"; [i.__setitem__("t", "2020-01-01T00:00:00Z") for i in c["items"]]
                break
        else:
            for w in ws:
                if w["collection"] == "live":
                    w["data"].pop("news", None)
    elif name == "brief_skeleton":
        for w in ws:
            if w["collection"] == "brief":
                w["data"]["bottom_line"] = "60 of 74 sources returned data in this morning run (2026-10-31T09:07:06Z)."
                w["data"]["items"][0]["likelihood"] = "likelihood not stated"
    elif name == "text_split":
        for coll, did, it in items_of(r):
            if it.get("kind") == "quake":
                it["title"] = "M5.1 earthquake \u2014 236 km ESE of Zyryanka, Russia"; break
        for coll, did, it in items_of(r):
            it["title"] = "Civilian evacuations commence as part of a local ceasefire that U.S"; break
    elif name == "cluster_same_event":
        for w in ws:
            if w["collection"] in ("news", "live"):
                cl = w["data"].get("clusters") if w["collection"] == "news" else w["data"].get("news")
                multi = [c for c in cl or [] if len(c.get("items", [])) >= 2]
                if multi:
                    it = multi[0]["items"]
                    it[0]["title"] = "Saudi-led coalition says it downs drone over oil-rich Eastern Region"; it[0]["summary"] = ""
                    it[1]["title"] = "Saudi Arabia says intercepted hostile drone in Riyadh region"; it[1]["summary"] = ""
                    break
    elif name == "health_mode":
        if r["mode"] == "morning":
            for w in ws:
                if w["collection"] == "meta" and w["doc_id"] == "health":
                    w["data"]["mode"] = "hourly"; w["data"]["total"] = 31
                if w["collection"] == "meta" and w["doc_id"] == "run":
                    w["data"].pop("total", None)
        else:
            for w in ws:
                if w["collection"] == "live":
                    w["data"].pop("mode", None); w["data"]["total"] = 75; break
    elif name == "receipts":
        k = 0
        for coll, c in news_clusters(r):
            for i in c.get("items", []):
                k += 1
                if k % 3 == 0:
                    i.pop("receipt", None)
                elif k % 3 == 1 and i.get("receipt"):
                    i["receipt"]["page"] = "mismatch"
        for w in ws:
            if w["collection"] == "series":
                w["data"].pop("fetched_at", None); break
    elif name == "proof_ladder":
        cl = [c for coll, c in news_clusters(r)]
        cl[0]["rung"] = "R9"
        for c in cl[1:]:
            if any((i.get("receipt") or {}).get("rung") == "R2" for i in c["items"]):
                for i in c["items"]:
                    if (i.get("receipt") or {}).get("rung") == "R2":
                        i["receipt"]["feed_summary"] = ""
                break
        for c in cl[2:]:
            if c.get("proof"):
                c["proof"] = ""; break
    elif name == "not_checked_zero":
        for coll, c in news_clusters(r):
            c["items"][0]["receipt"]["page"] = "not_checked"; break
    elif name == "gloss_requires_quote":
        gw = [w for w in ws if w["collection"] == "glossary"]
        if not gw:
            gw = [{"op": "set", "collection": "glossary", "doc_id": "all", "data": {"terms": [{"term": "Federalism", "plain": "x", "source": "https://www.archives.gov/founding-docs", "quote": None, "quote_status": "found", "quote_at": "2026-10-31T00:00:00Z", "glossed": True}], "glossed": 1}}]
            next(iter(r["batches"].values())).append(gw[0])
        t0 = gw[0]["data"]["terms"][0]
        t0["glossed"] = True; t0["quote_status"] = "not_found"; t0["quote"] = None
    elif name == "newer_value_check":
        for w in ws:
            if w["collection"] == "series" and "fred.stlouisfed.org" in str((w["data"].get("source") or {}).get("url", "")) and not w["data"].get("kept"):
                w["data"]["tail_newest"] = {"date": "2999-01-01", "value": 1.0}; break
    elif name == "number_rung":
        for w in ws:
            if w["collection"] == "series":
                w["data"].pop("number_rung", None); break
    elif name == "backoff_no_block_retry":
        r["bad_fetcher"] = True
    elif name == "spc_no_future":
        import engine as _e, datetime as _d
        r["spc_time_fn"] = lambda hhmm, now: now.replace(hour=int(hhmm[:2]), minute=int(hhmm[2:4]))
        st = parse_iso(r["report"]["started"])
        fut = {"id": "storm_report-future", "kind": "storm_report", "t": (st + _d.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ"), "updated": None}
        for w in ws:
            if w["collection"] in ("events", "wire", "records"):
                w["data"].setdefault("items", []).append(fut); break
            if w["collection"] == "live":
                w["data"].setdefault("events", []).append(fut); break
    elif name == "verify_all":
        k = 0
        for coll, c in news_clusters(r):
            for i in c.get("items", []):
                k += 1
                if k == 1:
                    i.pop("receipt", None)
        for w in ws:
            if w["collection"] == "series":
                w["data"]["number_rung"] = {"rung": "N4", "source_url": None}; break
    elif name == "brief_refs":
        for w in ws:
            if w["collection"] == "brief":
                w["data"]["items"][0]["refs"] = ["does-not-exist"]
    return r, r2


def self_rerun(run):
    """idempotent_ids without --out2: a replay output is re-run here (same frozen clock = report.started, same replay
    cache, empty state, network proxied to a dead port) into a temp dir. A networked output still needs --out2."""
    import subprocess, tempfile
    rep = run["report"]
    tmp = tempfile.mkdtemp(prefix="wm_out2_")
    out2, st = os.path.join(tmp, "out"), os.path.join(tmp, "state")
    os.makedirs(st)
    env = dict(os.environ, WATCHMAN_NOW=rep["started"], HTTPS_PROXY="http://127.0.0.1:9", HTTP_PROXY="http://127.0.0.1:9")
    r = subprocess.run([sys.executable, os.path.join(HERE, "engine.py"), "--mode", "morning", "--out", out2, "--replay", rep["replay_dir"], "--state", st],
                       env=env, capture_output=True, text=True, timeout=900)
    if r.returncode != 0 or not os.path.exists(os.path.join(out2, "report.json")):
        print("    self-rerun failed:", (r.stdout + r.stderr)[-300:])
        return None, tmp
    print(f"    idempotent_ids: second run produced here from {rep['replay_dir']} at {rep['started']}")
    return load(out2), tmp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True); ap.add_argument("--out2"); ap.add_argument("--demo", action="store_true")
    ap.add_argument("--cache", action="append", default=[]); ap.add_argument("--strict", action="store_true", help="second run reused the same raw responses (replay): require 100% item-id overlap")
    a = ap.parse_args()
    run = load(a.out); run2 = load(a.out2) if a.out2 else None
    tmp2 = None
    if run2 is None and run["mode"] == "morning" and run["report"].get("replay_dir") and not a.demo:
        run2, tmp2 = self_rerun(run)
    run["apply"] = synth_apply()
    ctx = {"run2": run2, "cache_dirs": a.cache, "strict": a.strict}
    rc = 0
    mode = run["mode"]
    if a.demo:
        print(f"{'test':18} {'bad input':10} {'real output':12}   (mode {mode})")
        for name, fn, tmode in TESTS:
            if tmode and tmode != mode:
                print(f"{name:18} n/a for {mode} runs"); continue
            br, br2 = inject(run, name, run2)
            c = dict(ctx, run2=br2)
            bad = fn(br, c); good = fn(run, dict(ctx))
            ok = bool(bad) and not good
            rc |= 0 if ok else 1
            print(f"{name:18} {'FAIL (ok)' if bad else 'PASSED (BUG)':10} {'PASS' if not good else 'FAIL':12}  e.g. {(bad[0] if bad else '')[:70]}")
            for g in good[:3]:
                print("    real-output failure:", g)
    else:
        for name, fn, tmode in TESTS:
            if tmode and tmode != mode:
                continue
            c = dict(ctx); bad = fn(run, c)
            if bad and all(str(b).startswith(SKIP) for b in bad):
                print(f"SKIP {name} - " + "; ".join(str(b)[len(SKIP):] for b in bad)); continue
            print(f"{'PASS' if not bad else 'FAIL'} {name}" + (f"  (id overlap {c['idem_overlap']})" if "idem_overlap" in c and name == "idempotent_ids" else ""))
            for b in bad[:10]:
                print("    ", b)
            for n_ in c.get("notes", []):
                print("    note:", n_)
            rc |= 1 if bad else 0
    sys.exit(rc)


if __name__ == "__main__":
    main()
