"""P2 image tests over real fixtures. Run: python3 test_images.py"""
import base64, glob, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
from images import contract as C
import imgfeeds
PL = os.path.join(HERE, "images", "plates"); FX = os.path.join(HERE, "..", "app", "dev", "fixtures")
PROV = json.load(open(os.path.join(PL, "PROVENANCE.json")))


def _fixture():
    d = json.load(open(os.path.join(FX, "C1.json"))); b = json.load(open(os.path.join(FX, "blobs.json")))
    rows = d["img/index"]["images"]
    # the page index (img/index) is the public shape; the engine rows (caption, crop, crop_box) live in the run's img/manifest.json
    man = {m["id"]: m for m in json.load(open(os.path.join(HERE, "out", "v4-replay-0905", "img", "manifest.json")))}
    files = [(r["id"], dict(man.get(r["id"], {}), **r, jpg=base64.b64decode(b[r["url"]].split(",", 1)[1]))) for r in rows if r.get("url") in b]
    return d, files


def test_goes_colorado_r30_rejected():
    p = "known_bad/r30-goes-colorado.jpg"; row = PROV[p]["row"]
    L = C.legibility_score(open(os.path.join(PL, p), "rb").read(), row)
    assert not L["legible"], L
    ch = C.choose_for_item({"kind": "alert", "title": "Flood Watch", "place": {"admin": "Colorado"}}, row, leg=L)
    assert ch["kind"] == "plate", ch


def test_known_bad_and_good():
    for p, meta in PROV.items():
        L = C.legibility_score(open(os.path.join(PL, p), "rb").read(), meta["row"], None, meta["row"].get("caption"))
        assert L["legible"] == p.startswith("known_good"), (p, L["legibility"], L["reasons"])


def test_r3_never_on_news():
    d, files = _fixture()
    ids = [i for i, _ in files]
    docs = [(k.split("/")[0], k.split("/", 1)[1], json.loads(json.dumps(v))) for k, v in d.items() if k.split("/")[0] in ("events", "wire", "records", "live", "news")]
    # adversarial: give every item a place portrait (wiki) or the presidential portrait as its picture
    r3 = [i for i in ids if C.rung_of({"id": i, "crop": dict(files)[i].get("crop")}) == "R3"]
    assert r3, "fixture has no R3 picture to test with"
    n = 0
    for _, _, doc in docs:
        for key in imgfeeds.ITEM_LISTS:
            for j, it in enumerate(doc.get(key) or []):
                if isinstance(it, dict):
                    it["img"] = r3[(n + j) % len(r3)] if j % 2 else it.get("img"); n += 1
    imgfeeds.attach_items(docs, {}, files)
    seen = 0
    for coll, did, doc in docs:
        for key in imgfeeds.ITEM_LISTS:
            for it in doc.get(key) or []:
                if isinstance(it, dict):
                    seen += 1
                    assert "image" in it, (coll, did, it.get("id"))
                    assert C.validate(it["image"], "item") == [], (coll, did, it.get("id"), it["image"])
                    assert it["image"]["kind"] != "portrait" and it["image"].get("rung") != "R3"
    assert seen > 300, seen


def test_places_hero_may_be_r3():
    d, files = _fixture()
    docs = [("places", k.split("/", 1)[1], dict(v)) for k, v in d.items() if k.startswith("places/")]
    imgfeeds.attach_places(docs, {pid: doc.get("img") for _, pid, doc in docs if doc.get("img")}, files)
    kinds = [doc["hero"]["kind"] for _, _, doc in docs]
    assert all(C.validate(doc["hero"], "place") == [] for _, _, doc in docs)
    assert kinds.count("portrait") >= 10, kinds


EO_SAMPLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images", "samples", "nasa_science_eo_feed_2026-10-31.xml")


def test_eo_r2_feed_real_sample():
    """FABLE-MIDBUILD #3: R2 = NASA Earth Observatory photo. Parser runs on the real feed saved 2026-10-31
    (science.nasa.gov, where earthobservatory.nasa.gov/feeds/natural-hazards.rss now redirects); Photojournal/Mars items are dropped."""
    import imgfeeds
    ents = imgfeeds.eo_entries(open(EO_SAMPLE, "rb").read())
    titles = [e["title"] for e in ents]
    assert len(ents) >= 3 and "Arnab Strikes India" in titles and not any("Martian" in t for t in titles), titles
    assert all(e["img"].startswith("https://assets.science.nasa.gov/") and "/esd/eo/images/" in e["img"] for e in ents)
    storm = {"id": "x1", "kind": "storm", "title": "Typhoon Arnab makes landfall in India", "place": {"name": "Odisha, India"}}
    fire = {"id": "x2", "kind": "fire", "title": "Wildfires burn across New Guinea highlands", "place": {"name": "Papua New Guinea"}}
    quake = {"id": "x3", "kind": "quake", "title": "M5.1 earthquake in India", "place": {"name": "India"}}
    assert imgfeeds.eo_match(storm, ents)["title"] == "Arnab Strikes India"
    assert imgfeeds.eo_match(fire, ents)["title"] == "Fires Char New Guinea"
    assert imgfeeds.eo_match({"id": "x4", "kind": "news", "title": "India election results announced", "place": {"name": "India"}}, ents) is None
    assert imgfeeds.eo_match(quake, ents) is None                 # no quake photo in the feed: nothing, never a nearest guess
    row = {"id": "img-eo-arnab-strikes-india", "crop": "event", "credit": "NASA Earth Observatory", "license": imgfeeds.EO_LICENSE}
    assert C.rung_of(row) == "R2"
    ch = C.choose_for_item(storm, row, leg={"legible": True, "legibility": 0.9, "reasons": []})
    assert ch["kind"] == "matched" and ch["rung"] == "R2", ch


# ================================================================ P10 (LEAP-PLAN §3): R2-pub, R2-gov, budget, bytes, honest fallbacks
# Test pictures are drawn here in the test (gradient + texture) and served by a fake fetcher; nothing reaches the network.
class _R:
    def __init__(self, body=None, status=200, ct="image/jpeg"):
        self.ok, self.status, self.body, self.error, self.headers = status == 200 and body is not None, status, body, None, {"content-type": ct}
    def json(self):
        return json.loads(self.body)


class _F:
    def __init__(self, table):
        self.table, self.calls = table, []
    def get(self, url, params=None, **kw):
        self.calls.append(url)
        v = self.table.get(url)
        return v if isinstance(v, _R) else (_R(v) if v is not None else _R(None, 404))


def _photo(w=900, h=600, seed=1, flat=False, noise=False):
    import numpy as np
    from PIL import Image
    rng = np.random.default_rng(seed)
    if flat:
        a = np.full((h, w, 3), 128, np.uint8)
    elif noise:
        a = rng.integers(0, 256, (h, w, 3), dtype=np.uint8)
    else:
        from PIL import ImageDraw          # sharp-edged shapes at full resolution, like a real photo's detail
        im = Image.new("RGB", (w, h), (40, 60, 90)); dr = ImageDraw.Draw(im)
        for _ in range(max(160, w * h // 4000)):
            x0, y0 = int(rng.integers(0, w)), int(rng.integers(0, h)); s_ = int(rng.integers(4, max(6, w // 12)))
            dr.line([(x0, y0), (x0 + s_, y0 + int(rng.integers(-s_, s_)))], fill=tuple(int(c) for c in rng.integers(0, 256, 3)), width=int(rng.integers(1, 4)))
            dr.rectangle([x0, y0, x0 + s_ // 2, y0 + s_ // 3], outline=tuple(int(c) for c in rng.integers(0, 256, 3)))
        a = np.asarray(im)
    import io as _io
    b = _io.BytesIO(); Image.fromarray(a).save(b, "JPEG", quality=95); return b.getvalue()


FEED5 = """<?xml version="1.0"?><rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>T</title>""" + "".join(
    f"""<item><title>Story {i}</title><link>https://www.bbc.co.uk/news/articles/a{i}</link><pubDate>Sun, 01 Nov 2026 0{i}:00:00 GMT</pubDate>
    {'<media:content url="https://ichef.bbci.co.uk/p/%d.jpg" medium="image" width="976"/>' % i if i % 3 == 0 else ('<enclosure url="https://ichef.bbci.co.uk/p/%d.jpg" type="image/jpeg" length="1"/>' % i if i % 3 == 1 else '<media:thumbnail url="https://ichef.bbci.co.uk/p/%d.jpg" width="976"/>' % i)}</item>"""
    for i in range(5)) + "</channel></rss>"


def _clusters(n, img=lambda i: f"https://ichef.bbci.co.uk/p/{i}.jpg", pub="bbc", host="bbc.co.uk"):
    return [{"id": f"news{i}", "_cluster": {"members": [{"img": img(i), "url": f"https://www.{host}/news/articles/a{i}", "pub": pub, "pubname": "BBC News", "host": "www." + host, "t": f"2026-11-01T0{i % 10}:00:00Z"}]}} for i in range(n)]


def _news_docs(n, host="bbc.co.uk"):
    return [("live", "2026-11-01T0551", {"news": [{"id": f"news{i}", "title": f"Story {i}", "publishers": [host], "items": [{"url": f"https://www.{host}/news/articles/a{i}"}], "region": "Europe"} for i in range(n)]})]


def test_news_items_get_publisher_photo():
    import feedparser
    urls = [imgfeeds.entry_image(e) for e in feedparser.parse(FEED5.encode()).entries]
    assert urls == [f"https://ichef.bbci.co.uk/p/{i}.jpg" for i in range(5)], urls      # media:content, enclosure and media:thumbnail all read
    F = _F({u: _photo(seed=i) for i, u in enumerate(urls)})
    files, m, counts = imgfeeds.run_photos(F, _clusters(5), [], (), ("bbc",))
    docs = _news_docs(5); imgfeeds.attach_items(docs, m, files)
    ims = [it["image"] for it in docs[0][2]["news"]]
    assert [i["kind"] for i in ims] == ["matched"] * 5, [(i["kind"], i["why"]) for i in ims]
    for i, ch in enumerate(ims):
        assert ch["rung"] == "R2" and ch["credit"] == "BBC News" and ch["license"] == imgfeeds.PUB_LICENSE, ch
        assert ch["link"] == f"https://www.bbc.co.uk/news/articles/a{i}" and ch["posted"], ch      # always links through to the article
        assert C.validate(ch, "item") == [], C.validate(ch, "item")
    assert counts["fetched"] == 5 and counts["attached"] == 5 and all(u.startswith("https://ichef") for u in F.calls)   # feed pictures only, never an article page


def test_banner_url_rejected():
    F = _F({"https://ichef.bbci.co.uk/p/banner.jpg": _photo()})
    files, m, counts = imgfeeds.run_photos(F, _clusters(3, img=lambda i: "https://ichef.bbci.co.uk/p/banner.jpg"), [], ())
    # M4: the banner is never fetched; the og pass may read each member page once (here 404: nothing attached)
    assert m == {} and counts["rejected"].get("banner") == 3 and "https://ichef.bbci.co.uk/p/banner.jpg" not in F.calls, counts


def test_logo_rejected():
    F = _F({"https://cdn.example.org/static/logo-1200.jpg": _photo(), "https://cdn.example.org/img/site_flag.jpg": _photo()})
    cl = _clusters(2, img=lambda i: ["https://cdn.example.org/static/logo-1200.jpg", "https://cdn.example.org/img/site_flag.jpg"][i])
    files, m, counts = imgfeeds.run_photos(F, cl, [], ())
    assert m == {} and counts["rejected"].get("logo") == 2 and not any("cdn.example.org" in u for u in F.calls), counts


def test_pub_quality_gates():
    table = {"https://p.org/narrow.jpg": _photo(400, 266), "https://p.org/tall.jpg": _photo(600, 900), "https://p.org/wide.jpg": _photo(1500, 500),
             "https://p.org/flat.jpg": _photo(flat=True), "https://p.org/ok.jpg": _photo(),
             "https://p.org/page.html": _R(b"<html></html>", ct="text/html")}
    names = list(table)
    files, m, counts = imgfeeds.run_photos(_F(table), _clusters(len(names), img=lambda i: names[i], host="p.org"), [], ())
    r = counts["rejected"]
    assert (r.get("too_narrow"), r.get("aspect"), r.get("flat"), r.get("not_image")) == (1, 2, 1, 1), r
    assert list(m) == ["news4"] and counts["attached"] == 1, (m, counts)


def test_declined_and_blocked_publisher_never_used():
    pr = imgfeeds.PhotoRun(declined={"bbc.co.uk"}); F = _F({f"https://ichef.bbci.co.uk/p/{i}.jpg": _photo(seed=i) for i in range(2)})
    news = [{"id": c["id"], "img_candidates": imgfeeds.cluster_candidates(c["_cluster"]["members"])} for c in _clusters(2)]
    assert imgfeeds.pub_photos(F, news, pr) == {} and pr.rejected.get("declined") == 2 and set(pr.rejected) <= {"declined", "no_page"} and F.calls == []   # og pass reads nothing
    health = [{"id": "news_abc", "host": "www.bbc.co.uk", "status": "blocked"}]
    files, m, counts = imgfeeds.run_photos(F, _clusters(2), [], health)
    assert m == {} and counts["rejected"].get("blocked") >= 2 and F.calls == [], counts      # M4: feed and og pass both refuse it


def test_photo_budget():
    n = imgfeeds.PHOTO_BUDGET + 12
    F = _F({f"https://ichef.bbci.co.uk/p/{i}.jpg": _photo(640, 427, seed=i) for i in range(n)})
    files, m, counts = imgfeeds.run_photos(F, _clusters(n), [], (), top=n)
    assert counts["fetched"] == imgfeeds.PHOTO_BUDGET == len(F.calls) == 150, counts
    assert counts["rejected"].get("budget") == 12 and counts["attached"] == 150 and len(files) == 150, counts
    assert counts["news_top"] == n and counts["news_matched"] == 150 and counts["news_matched_pct"] == round(100 * 150 / n, 1), counts
    assert set(counts) >= {"fetched", "attached", "rejected"}


def test_photo_bytes_cap():
    big = _photo(3000, 2000, seed=7)
    files, m, counts = imgfeeds.run_photos(_F({"https://ichef.bbci.co.uk/p/0.jpg": big, "https://ichef.bbci.co.uk/p/1.jpg": _photo(1200, 800, noise=True)}), _clusters(2), [], ())
    for iid, d in files:
        assert len(d["jpg"]) <= imgfeeds.PHOTO_MAX_BYTES == 180_000 and max(d["width"], d["height"]) == 1280 and d["sharp"], (iid, len(d["jpg"]), d["width"])
        assert d["quality"] in imgfeeds.PHOTO_Q and d["ttl_s"] == 7 * 86400
    assert "news0" in m
    assert ("news1" in m) or counts["rejected"].get("bytes_cap") == 1, counts    # pure noise either fits at q60 or is refused, never stored over the cap


def test_m2_1280_never_upscaled():
    jpg, info = imgfeeds.photo_jpeg(_photo(700, 466, seed=4))
    assert info["w"] == 700 and not info["sharp"], info                       # a smaller original is stored at its own size, flagged not sharp
    jpg, info = imgfeeds.photo_jpeg(_photo(2400, 1600, seed=5))
    assert (info["w"], info["h"]) == (1280, 853) and info["sharp"] and info["q"] in imgfeeds.PHOTO_Q, info
    jpg, info = imgfeeds.photo_jpeg(_photo(2400, 1600, seed=5), max_bytes=imgfeeds.DATA_URI_MAX_BYTES)
    assert jpg is None or len(jpg) <= imgfeeds.DATA_URI_MAX_BYTES < 180_000


def test_m2_upsize_same_picture_then_fallback():
    bbc = "https://ichef.bbci.co.uk/ace/standard/240/cpsprodpb/d1d6/live/x.jpg"
    assert imgfeeds.upsize(bbc) == "https://ichef.bbci.co.uk/ace/standard/1536/cpsprodpb/d1d6/live/x.jpg"
    assert imgfeeds.upsize("https://static01.nyt.com/images/2026/11/01/multimedia/a/a-mediumSquareAt3X.jpg").endswith("/a-superJumbo.jpg")
    assert imgfeeds.upsize("https://stateline.org/wp-content/uploads/2026/10/ky-voting-1024x683.jpg") == "https://stateline.org/wp-content/uploads/2026/10/ky-voting.jpg"
    assert imgfeeds.upsize("https://thehill.com/wp-content/uploads/sites/2/2026/10/AP1.jpg?w=900") == "https://thehill.com/wp-content/uploads/sites/2/2026/10/AP1.jpg?w=1600"
    assert imgfeeds.upsize("https://i.guim.co.uk/img/media/a/master/3986.jpg?width=700&quality=85&s=abc") is None    # signed: left alone
    assert imgfeeds.upsize("https://cdn.i-scmp.com/x/1280x720/p.jpg?itok=1") is None
    big = bbc.replace("/240/", "/1536/")
    F = _F({big: _R(b"no", status=404), bbc: _photo(976, 549, seed=9)})     # larger rendition missing -> the shipped URL, host not blocked
    files, m, counts = imgfeeds.run_photos(F, _clusters(1, img=lambda i: bbc), [], ())
    assert F.calls == [big, bbc] and "news0" in m and files[0][1]["img_src"] == bbc and counts["rejected"] == {}, (F.calls, counts)
    F = _F({big: _photo(1536, 864, seed=8)})
    files, m, counts = imgfeeds.run_photos(F, _clusters(1, img=lambda i: bbc), [], ())
    assert F.calls == [big] and files[0][1]["width"] == 1280 and files[0][1]["sharp"], F.calls


def test_m2_feed_html_img_only_when_no_media():
    import feedparser
    x = """<?xml version="1.0"?><rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><title>T</title>
    <item><title>A</title><link>https://stateline.org/a</link><content:encoded><![CDATA[<p><img src="https://stateline.org/wp-content/uploads/2026/10/a-1024x683.jpg"/></p>]]></content:encoded></item>
    <item><title>B</title><link>https://stateline.org/b</link><description><![CDATA[<img src="https://feeds.feedburner.com/~r/x/1x1.gif"/> text]]></description></item>
    </channel></rss>"""
    urls = [imgfeeds.entry_image(e) for e in feedparser.parse(x.encode()).entries]
    assert urls == ["https://stateline.org/wp-content/uploads/2026/10/a-1024x683.jpg", None], urls


def test_m2_washington_true_match_only():
    assert imgfeeds.title_match("NASA Artemis II crew arrives at Kennedy", "Artemis II Crew Arrives at Kennedy Space Center")
    assert not imgfeeds.title_match("Executive Order on Federal Hiring", "Hiring fair at Goddard")
    rec = {"id": "rN", "title": "Proclamation on National Hunting Day", "t": "2026-10-31", "sources": [{"url": "https://www.nasa.gov/news/x"}]}
    hit = {"collection": {"items": [{"data": [{"title": "Apollo 11 launch"}], "links": [{"rel": "preview", "href": "https://images-assets.nasa.gov/a/a~thumb.jpg"}]}]}}
    F = _F({imgfeeds.NASA_IMAGES: _R(json.dumps(hit).encode(), ct="application/json")})
    files, m, counts = imgfeeds.run_photos(F, [], [rec], ())
    assert m == {} and counts["rejected"].get("no_title_match") == 1 and F.calls == [imgfeeds.NASA_IMAGES], (m, counts)
    wh = {"id": "rW", "title": "Executive Order Z", "sources": [{"url": "https://www.whitehouse.gov/presidential-actions/2026/10/z/"}], "stage": {"current": "signed"}}
    ch = C.choose_for_item(wh, None, cls="record")
    assert ch["kind"] in ("plate", "none") and ch["issuer"] == {"host": "whitehouse.gov", "name": "The White House"} and C.validate(ch) == [], ch
    news = {"id": "n1", "title": "x", "publishers": ["bbc.co.uk"]}
    assert "issuer" not in C.choose_for_item(news, None, cls="news")


def test_photo_sha_dedupe():
    same = _photo(seed=3)
    files, m, counts = imgfeeds.run_photos(_F({"https://a.org/x.jpg": same, "https://a.org/y.jpg": same}), _clusters(2, img=lambda i: ["https://a.org/x.jpg", "https://a.org/y.jpg"][i], host="a.org"), [], ())
    assert len(files) == 1 and m["news0"] == m["news1"] and files[0][1]["sha256"], (files, m)


WH = """<?xml version="1.0"?><rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>Presidential Actions</title>
<item><title>Executive Order A</title><link>https://www.whitehouse.gov/presidential-actions/2026/10/order-a/</link><media:content url="https://www.whitehouse.gov/wp-content/uploads/2026/10/a.jpg" medium="image"/></item>
<item><title>Executive Order B</title><link>https://www.whitehouse.gov/presidential-actions/2026/10/order-b/</link><media:content url="https://cdn.someagency.com/b.jpg" medium="image"/></item>
</channel></rss>"""


def test_record_photo_from_issuer_only():
    recA = {"id": "rA", "title": "Executive Order A", "t": "2026-10-31", "sources": [{"url": "https://www.whitehouse.gov/presidential-actions/2026/10/order-a/"}], "stage": {"current": "signed"}}
    recB = {"id": "rB", "title": "Executive Order B", "t": "2026-10-31", "sources": [{"url": "https://www.whitehouse.gov/presidential-actions/2026/10/order-b/"}], "stage": {"current": "signed"}}
    recC = {"id": "rC", "title": "Proposed rule", "t": "2026-10-31", "sources": [{"url": "https://www.federalregister.gov/d/2026-1"}], "stage": {"current": "published"}, "place": {"admin": "United States"}}
    F = _F({"https://www.whitehouse.gov/wp-content/uploads/2026/10/a.jpg": _photo(), "https://cdn.someagency.com/b.jpg": _photo(seed=2)})
    files, m, counts = imgfeeds.run_photos(F, [], [recA, recB, recC], (), wh_xml=WH.encode())
    assert m == {"rA": m["rA"]} and m["rA"].startswith("img-gov-"), m                      # B's picture is not on the issuer's host; C's issuer publishes none
    assert F.calls == ["https://www.whitehouse.gov/wp-content/uploads/2026/10/a.jpg"], F.calls
    docs = [("records", "presidential", {"items": [recA, recB]}), ("records", "rules_proposed", {"items": [recC]})]
    imgfeeds.attach_items(docs, m, files)
    a, b = docs[0][2]["items"]; c = docs[1][2]["items"][0]
    assert a["image"]["kind"] == "matched" and a["image"]["credit"] == "The White House" and a["image"]["link"].startswith("https://www.whitehouse.gov/"), a["image"]
    assert b["image"]["kind"] in ("plate", "none") and "R2-gov" in b["image"]["why"], b["image"]          # honest fallback names the rung tried
    assert c["image"]["kind"] == "plate" and "R2-gov" in c["image"]["why"], c["image"]
    # the contract refuses a gov photo on another issuer's record, and a publisher preview on any record
    row = imgfeeds.manifest_rows(files)[0]
    assert C.choose_for_item(recC, row, leg={"legible": True, "legibility": 0.9, "reasons": []}, cls="record")["kind"] != "matched"
    prow = dict(row, id="img-pub-x", publisher_host="bbc.co.uk")
    assert C.choose_for_item(recA, prow, leg={"legible": True, "legibility": 0.9, "reasons": []}, cls="record")["kind"] != "matched"


def test_fallback_is_honest():
    it = {"id": "n", "title": "x", "publishers": ["bbc.co.uk"]}
    ch = C.choose_for_item(it, None, cls="news")
    assert ch["kind"] == "none" and "R2-pub" in ch["why"] and "text-only" in ch["why"], ch
    ch = C.choose_for_item(dict(it, place={"admin": "France"}), None, cls="news")
    assert ch["kind"] == "plate" and "R2-pub" in ch["why"], ch


def test_picture_url_is_upload_only():
    """LIVE-VERIFY-2: an ImageChoice names its image id; its url is the uploaded /_blob/ one or absent, never img/<id>.jpg."""
    leg = {"legible": True, "legibility": 0.9, "reasons": []}
    row = {"id": "img-goes-x", "crop": "event", "crop_box": [30, -91, 31, -90], "credit": "NOAA", "license": "Public domain", "src_url": "https://gibs.example"}
    it = {"id": "e", "title": "Flood warning", "kind": "alert", "place": {"admin": "Louisiana"}}
    ch = C.choose_for_item(it, row, leg=leg, cls="event")
    if ch["kind"] == "matched":
        assert ch["img"] == "img-goes-x" and ch["url"] is None and not C.validate(ch), ch
    ch = C.choose_for_place("marion", dict(row, id="img-wiki-marion", url="/_blob/abc123"), leg=leg)
    assert ch["kind"] == "portrait" and ch["img"] == "img-wiki-marion" and ch["url"] == "/_blob/abc123" and not C.validate(ch, on="place"), ch
    ch = C.choose_for_place("marion", dict(row, id="img-wiki-marion", url="img/img-wiki-marion.jpg"), leg=leg)
    assert ch["url"] is None and ch["img"] == "img-wiki-marion", ch
    bad = dict(ch, url="img/img-wiki-marion.jpg")
    assert any("not an uploaded" in b for b in C.validate(bad, on="place")), C.validate(bad, on="place")
    src = open(os.path.join(HERE, "images", "contract.py")).read()
    assert 'f"img/{' not in src, "relative img/<id>.jpg fallback is back in contract.py"


def test_verify_live_thresholds():
    """M4: verify_live reads what the UI renders -- every news/<date> cluster newest first, lead = most members -- whichever mode
    wrote the docs; freshness is measured (now - finished), not the cadence config value."""
    import verify_live as V
    m = {"kind": "matched"}; p = {"kind": "plate"}
    cl = [{"id": f"n{i}", "title": f"t{i}", "n": 2 + (i == 4), "t": f"2026-11-01T05:{i:02d}:00Z", "image": m if i < 8 else p} for i in range(10)]
    db = {"news/2026-11-01": {"clusters": cl[:6]}, "news/2026-11-01_2": {"clusters": cl[6:]},
          "live/2026-11-01T0551": {"run": "m-x", "written_by": "morning", "finished": "2026-11-01T05:58:00Z", "news": [dict(cl[8])], "photos": {"attached": 8}},
          "live/2026-10-31T1645": {"run": "h-old", "finished": "2026-10-31T16:46:00Z", "news": []},
          "meta/run": {"run": "m-x", "cadence_s": 3600, "finished": "2026-11-01T05:58:00Z", "photos": {"attached": 3}}}
    now = V._t("2026-11-01T06:30:00Z")
    r = V.measure(db, now)
    assert r["ok"], r["checks"]
    assert r["newest_live"] == "live/2026-11-01T0551" and r["checks"]["lead_matched"]["lead"] == "t4" and r["checks"]["top30_matched_pct"]["value"] == 80.0
    assert r["info"]["live_news_pct"] == 0.0                                   # the live doc is a second line, not the UI's top 30
    cl[4]["image"] = p; cl[7]["image"] = p                                      # the UI lead loses its picture; 60 %
    r = V.measure(db, now)
    assert not r["checks"]["lead_matched"]["ok"] and not r["checks"]["top30_matched_pct"]["ok"], r["checks"]
    r = V.measure(db, V._t("2026-11-01T07:20:00Z"))                            # 82 min after the newest run: stale, whatever cadence_s says
    assert not r["checks"]["live_fresh_min"]["ok"] and not r["checks"]["cadence_min"]["ok"] and r["checks"]["cadence_min"]["value"] == 82.0


def test_state_courts_never_under_washington():
    import coverage as CV
    rf = {"rulings": [{"id": "a", "title": "Indiana Court of Appeals: Doe v. State", "layer": "indiana", "place": {"name": "Indiana Court of Appeals", "admin": "Indiana"}},
                      {"id": "b", "title": "U.S. Court of Appeals, 7th Circuit: X v. Y", "layer": "us", "place": {"name": "U.S. Court of Appeals, 7th Circuit"}},
                      {"id": "c", "title": "Indiana Supreme Court: In re Z", "layer": "indiana"},
                      {"id": "d", "title": "U.S. Supreme Court: A v. B", "layer": "us"}]}
    CV.refile_scope(rf)
    assert [i["id"] for i in rf["rulings"]] == ["b", "d"] and sorted(i["id"] for i in rf["state_rulings"]) == ["a", "c"], rf
    assert all(i["scope"]["under"] == "Washington" for i in rf["rulings"]) and all(i["scope"]["under"] == "Indiana" for i in rf["state_rulings"])


def test_who_runs_official_only():
    import records as R
    off = {"url": "https://www.in.gov/gov/", "readable": True, "name": "Mike Braun"}
    assert R.who_runs(off, ["Eric Holcomb"], "2026-11-01")["name"] == "Mike Braun"
    for o in (None, {"url": "https://x.gov", "readable": False, "why": "HTTP 403"}, {"url": "https://x.gov", "readable": True, "name": None}):
        w = R.who_runs(o, ["Eric Holcomb"], "2026-11-01")
        assert w["name"] == w["kind"] == "not confirmed" and "Holcomb" not in json.dumps(w), w


def test_one_display_when():
    import subprocess
    out = subprocess.run(["grep", "-rnE", r"def display_when|strftime\(.%b.\) \{|%B %Y", "--include=*.py", HERE, "--exclude-dir=superseded", "--exclude-dir=out", "--exclude-dir=cache"], capture_output=True, text=True).stdout
    defs = [l for l in out.splitlines() if "def display_when" in l and not l.split(":")[0].endswith("test_images.py")]
    assert len(defs) == 1 and defs[0].split(":")[0].endswith("display.py"), out



# ================================================================ M2 (Fable, M2-JUDGE §4): the publisher's og:image, never ranked by picture
OG_A = """<html><head><title>A</title><meta property="og:title" content="A"><meta property="og:image" content="https://cdn.cbs.example/a.jpg?resize=1920%2C1080&amp;quality=80"/></head><body>x</body></html>"""
OG_B = """<html><head><meta data-rh="true" content="/image/79567904_6.jpg" property="og:image:secure_url"/><meta name="twitter:image" content="https://static.dw.example/tw.jpg"></head>"""
OG_C = """<html><head><meta name='twitter:image' content='http://img.example.org/c.jpg'></head><body><meta property="og:image" content="https://late.example/body.jpg"></body>"""


def test_m2_og_parse_three_fixtures():
    assert imgfeeds.og_image(OG_A, "https://www.cbsnews.com/news/1") == "https://cdn.cbs.example/a.jpg?resize=1920%2C1080&quality=80"
    assert imgfeeds.og_image(OG_B, "https://www.dw.com/en/x/a-1") == "https://www.dw.com/image/79567904_6.jpg"     # attr order, relative, secure_url before twitter
    head = imgfeeds.head_only(OG_C)
    assert "late.example" not in head and imgfeeds.og_image(head, "https://e.org/") == "https://img.example.org/c.jpg"   # body never read; http -> https
    assert imgfeeds.og_image("<head><title>x</title></head>", "https://e.org/") is None


def _og_cluster(i, host="cbsnews.com", pub="cbs", name="CBS News", img=None):
    return {"id": f"og{i}", "_cluster": {"members": [{"img": img, "url": f"https://www.{host}/news/{i}", "pub": pub, "pubname": name, "host": "www." + host, "t": f"2026-11-01T0{i % 10}:00:00Z"}]}}


def _og_table(n, host="cbsnews.com", page=lambda i: None, robots=b"User-agent: *\nDisallow: /search\n", pic=lambda i: _photo(1200, 675, seed=40 + i)):
    t = {f"https://www.{host}/robots.txt": robots}
    for i in range(n):
        t[f"https://www.{host}/news/{i}"] = _R((page(i) or f'<html><head><meta property="og:image" content="https://cdn.{host}/p/{i}.jpg"></head><body>' + "w " * 5000 + "</body></html>").encode(), ct="text/html")
        t[f"https://cdn.{host}/p/{i}.jpg"] = pic(i)
    return t


def test_m2_og_used_when_feed_has_no_picture():
    F = _F(_og_table(3)); cl = [_og_cluster(i) for i in range(3)]
    files, m, counts = imgfeeds.run_photos(F, cl, [], (), ("cbs",))
    assert sorted(m) == ["og0", "og1", "og2"] and counts["news_og"] == 3 and counts["news_feed"] == 0 and counts["news_matched_pct"] == 100.0, counts
    pages = [u for u in F.calls if "/news/" in u]
    assert sorted(pages) == sorted(set(pages)) and len(pages) == 3, F.calls                  # exactly one page request per item
    assert F.calls.count("https://www.cbsnews.com/robots.txt") == 1                         # robots read once per host
    row = dict(files)[m["og0"]]
    assert row["via"] == "og" and row["credit"] == "CBS News" and row["src_url"] == "https://www.cbsnews.com/news/0" and row["width"] == 1200
    docs = [("live", "x", {"news": [{"id": "og0", "title": "S", "publishers": ["cbsnews.com"], "items": [{"url": "https://www.cbsnews.com/news/0"}]}]})]
    imgfeeds.attach_items(docs, m, files); ch = docs[0][2]["news"][0]["image"]
    assert ch["kind"] == "matched" and ch["via"] == "og" and ch["link"] == "https://www.cbsnews.com/news/0" and C.validate(ch, "item") == [], ch
    # the feed picture path stays first: a feed picture means no page read at all
    F2 = _F({"https://ichef.bbci.co.uk/p/0.jpg": _photo(seed=3)}); imgfeeds.run_photos(F2, _clusters(1), [], ())
    assert F2.calls == ["https://ichef.bbci.co.uk/p/0.jpg"], F2.calls


def test_m2_og_blocked_declined_paywalled_never_read():
    for host in ("npr.org", "aljazeera.com", "nytimes.com", "wsj.com"):                                   # BLOCKED_PUBLISHERS / PAYWALL_HOSTS: no request at all
        F = _F(_og_table(1, host=host)); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(0, host=host)], [], ())
        assert m == {} and F.calls == [], (host, F.calls, counts)
    pr = imgfeeds.PhotoRun(declined={"dw.com"}); F = _F(_og_table(1, host="dw.com"))
    news = [{"id": "og0", "img_candidates": [], "members": _og_cluster(0, host="dw.com")["_cluster"]["members"]}]
    assert imgfeeds.pub_photos(F, news, pr) == {} and F.calls == [] and pr.rejected.get("declined") == 1
    health = [{"id": "news_x", "host": "www.cbsnews.com", "status": "blocked"}]
    F = _F(_og_table(1)); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(0)], [], health)
    assert m == {} and F.calls == [] and counts["rejected"].get("blocked") == 1
    cl = _og_cluster(0); cl["_cluster"]["members"][0]["receipt"] = {"page": "blocked"}         # this run's page check hit a wall
    F = _F(_og_table(1)); files, m, counts = imgfeeds.run_photos(F, [cl], [], ())
    assert m == {} and F.calls == []
    # pages whose own markup or redirect says paywall / login wall
    walls = ['<head><script type="application/ld+json">{"isAccessibleForFree": false}</script><meta property="og:image" content="https://cdn.cbsnews.com/p/0.jpg"></head>',
             '<head><meta property="article:content_tier" content="locked"><meta property="og:image" content="https://cdn.cbsnews.com/p/1.jpg"></head>']
    F = _F(_og_table(2, page=lambda i: walls[i])); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(i) for i in range(2)], [], ())
    assert m == {} and counts["rejected"].get("paywall") == 2 and not any("/p/" in u for u in F.calls), counts
    assert imgfeeds.paywalled("<head></head>", "https://e.org/account/login?next=/a") and imgfeeds.paywalled("", "", 402)
    assert imgfeeds.paywalled(OG_A, "https://www.cbsnews.com/news/1") is None


def test_m2_og_robots_respected():
    F = _F(_og_table(2, robots=b"User-agent: *\nDisallow: /news/\n")); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(i) for i in range(2)], [], ())
    assert m == {} and counts["rejected"].get("robots") == 2 and F.calls == ["https://www.cbsnews.com/robots.txt"], (F.calls, counts)
    t = _og_table(1); t["https://www.cbsnews.com/robots.txt"] = _R(b"no", status=403)
    F = _F(t); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(0)], [], ())
    assert m == {} and F.calls == ["https://www.cbsnews.com/robots.txt"]
    F = _F(_og_table(1, robots=b"User-agent: Watchman\nDisallow: /\n\nUser-agent: *\nAllow: /\n")); files, m, counts = imgfeeds.run_photos(F, [_og_cluster(0)], [], ())
    assert m == {} and counts["rejected"].get("robots") == 1


def test_m2_og_size_floor_and_content_type():
    t = _og_table(3, pic=lambda i: [_photo(560, 315, seed=1), _photo(1200, 675, seed=2), _photo(1200, 675, seed=3)][i])
    t["https://cdn.cbsnews.com/p/2.jpg"] = _R(_photo(1200, 675, seed=3), ct="text/html")
    files, m, counts = imgfeeds.run_photos(_F(t), [_og_cluster(i) for i in range(3)], [], ())
    assert list(m) == ["og1"] and counts["rejected"].get("og_small") == 1 and counts["rejected"].get("not_image") == 1, counts
    # the same og:image on > BANNER_MAX items is the site's default card, not the article's picture
    t = _og_table(3, page=lambda i: '<head><meta property="og:image" content="https://cdn.cbsnews.com/p/0.jpg"></head>')
    files, m, counts = imgfeeds.run_photos(_F(t), [_og_cluster(i) for i in range(3)], [], ())
    assert m == {} and counts["rejected"].get("banner") == 3, counts


def test_m2_og_budget_counts_every_request():
    n = 70; t = {}
    for i in range(n):
        t.update(_og_table(1, host=f"pub{i}.org"))
    cl = [_og_cluster(0, host=f"pub{i}.org") for i in range(n)]
    for i, c in enumerate(cl):
        c["id"] = f"og{i}"
    F = _F(t); files, m, counts = imgfeeds.run_photos(F, cl, [], (), top=n)
    assert counts["fetched"] == len(F.calls) <= imgfeeds.PHOTO_BUDGET and counts["rejected"].get("budget"), counts


def test_m2_never_ranked_by_picture():
    import inspect, re as _re
    cl = [_og_cluster(i) for i in range(2)] + _clusters(2)
    ids = [c["id"] for c in cl]
    F = _F({**_og_table(2), **{f"https://ichef.bbci.co.uk/p/{i}.jpg": _photo(seed=i) for i in range(2)}})
    imgfeeds.run_photos(F, cl, [], ())
    assert [c["id"] for c in cl] == ids                                                  # the ranked list is never reordered
    src = open(os.path.join(HERE, "engine.py"), encoding="utf-8").read()
    rank_lines = [l for l in src.splitlines() if _re.search(r"rank_c\s*=|news_pick\s*=|def score_|\"score\"\]\s*=", l)]
    assert rank_lines and not any(_re.search(r"\bimg\b|image|photo", l) for l in rank_lines), rank_lines


# ================================================================ M4: photo coverage of what the page shows, live doc from the morning run
def test_m4_og_after_feed_gate_and_next_member():
    """A cluster whose feed picture fails a gate gets the publisher og:image; a member page with no og:image -> the next member."""
    t = {**_og_table(1), **_og_table(1, host="cbc.ca", page=lambda i: "<html><head><title>no og</title></head>")}
    t["https://www.cbsnews.com/news/0"] = t["https://www.cbsnews.com/news/0"]
    c = {"id": "x0", "_cluster": {"members": [
        {"img": "https://cdn.example.org/static/logo-1200.jpg", "url": "https://www.cbc.ca/news/0", "pub": "cbc", "pubname": "CBC", "host": "www.cbc.ca", "t": "2026-11-01T01:00:00Z"},
        {"img": None, "url": "https://www.cbsnews.com/news/0", "pub": "cbs", "pubname": "CBS News", "host": "www.cbsnews.com", "t": "2026-11-01T02:00:00Z"}]}}
    F = _F(t); files, m, counts = imgfeeds.run_photos(F, [c], [], ())
    assert list(m) == ["x0"] and counts["news_og"] == 1 and dict(files)[m["x0"]]["src_url"] == "https://www.cbsnews.com/news/0", counts
    assert "https://cdn.example.org/static/logo-1200.jpg" not in F.calls and counts["rejected"].get("logo") == 1 and counts["rejected"].get("no_og") == 1, counts
    assert len([u for u in F.calls if "/news/" in u]) == 2                       # one GET per member page, never retried


def test_m4_photo_targets_order_never_by_picture():
    import engine as E
    def cl(i, n, t, pic):
        return {"id": f"c{i}", "t": t, "score": {"breaking": 0, "total": 0}, "img_marker": pic,
                "_cluster": {"hosts": [f"h{j}" for j in range(n)], "members": [{"t": t, "url": f"https://e.org/{i}/{j}", "img": ("x" if pic else None)} for j in range(n)]}}
    keep = [cl(i, 1 + (i % 3), f"2026-11-01T0{i}:00:00Z", i % 2 == 0) for i in range(8)]
    a = [c["id"] for c in E.photo_targets(keep, [keep[2]], newest=3, most=2)]
    assert a[0] == "c2" and a[1:4] == ["c7", "c6", "c5"] and len(a) == len(set(a)) == 8, a       # brief ref, then newest first
    for c in keep:
        c["img_marker"] = not c["img_marker"]; c["_cluster"]["members"][0]["img"] = None if c["_cluster"]["members"][0]["img"] else "x"
    assert [c["id"] for c in E.photo_targets(keep, [keep[2]], newest=3, most=2)] == a              # flipping who has a picture changes nothing


def test_m4_live_reuses_only_same_article_picture():
    import engine as E
    files = [("img-pub-a", {"src_url": "https://e.org/a", "jpg": b"1"}), ("img-pub-b", {"src_url": "https://e.org/b", "jpg": b"2"}), ("img-gov-c", {"src_url": "https://wh.gov/c", "jpg": b"3"})]
    item_img = {"m1": "img-pub-a", "m2": "img-pub-b", "r1": "img-gov-c"}
    pick = [{"id": "L1", "_cluster": {"members": [{"url": "https://e.org/z"}, {"url": "https://e.org/a"}]}},
            {"id": "L2", "_cluster": {"members": [{"url": "https://e.org/q"}]}}]
    f, m, miss = E.reuse_photos(pick, files, item_img)
    assert m == {"L1": "img-pub-a"} and [i for i, _ in f] == ["img-pub-a"] and [c["id"] for c in miss] == ["L2"], (m, miss)


def test_m4_morning_run_writes_live_doc():
    """RUN.md (d) uploads OUT/hourly like hourly mode: live/<stamp> batch + img/manifest; the engine wires it by default."""
    src = open(os.path.join(HERE, "engine.py"), encoding="utf-8").read()
    assert 'build_live(os.path.join(a.out, "hourly")' in src and '"--no-live"' in src
    run = open(os.path.join(HERE, "RUN.md"), encoding="utf-8").read()
    assert "out/morning/hourly" in run and "img/h" in run


def _letterbox():
    import io as _io
    from PIL import Image
    big = Image.new("RGB", (1200, 675), (0, 0, 0)); small = Image.open(_io.BytesIO(_photo(240, 160, seed=9)))
    big.paste(small.resize((240, 160)), (480, 257)); b = _io.BytesIO(); big.save(b, "JPEG", quality=95); return b.getvalue()


def test_m4_illegible_feed_picture_tries_next():
    """A stored picture the page would refuse (legibility) is dropped and the next publisher's picture is used."""
    c = {"id": "L0", "_cluster": {"members": [
        {"img": "https://a.example/p/box.jpg", "url": "https://www.a.example/n/0", "pub": "a", "pubname": "A", "host": "www.a.example", "t": "2026-11-01T01:00:00Z"},
        {"img": "https://b.example/p/ok.jpg", "url": "https://www.b.example/n/0", "pub": "b", "pubname": "B", "host": "www.b.example", "t": "2026-11-01T02:00:00Z"}]}}
    F = _F({"https://a.example/p/box.jpg": _letterbox(), "https://b.example/p/ok.jpg": _photo(1200, 675, seed=5)})
    files, m, counts = imgfeeds.run_photos(F, [c], [], ())
    assert counts["rejected"].get("illegible") == 1 and dict(files)[m["L0"]]["credit"] == "B" and len(files) == 1, counts

if __name__ == "__main__":
    bad = 0
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            try:
                f(); print("PASS", n)
            except AssertionError as e:
                bad += 1; print("FAIL", n, str(e)[:400])
    sys.exit(1 if bad else 0)
