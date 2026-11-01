"""ImageChoice (CONTRACT-v4 section 5) and legibility_score, which replaces plate_score.

ImageChoice =
  | {kind:"matched",  rung:"R1"|"R2", img, url, w, h, credit, license, src_url, taken, legibility, why}   R1 event crop, R2 official event photo
  | {kind:"portrait", rung:"R3",      img, url, w, h, credit, license, src_url, taken, legibility, why}   ONLY places/<id>.hero
  | {kind:"plate", plate:{type:"outline"|"map", geo:<place id>|[w,s,e,n]}, why}
  | {kind:"none", why}

Rules enforced here, not by callers:
- news / event / record items may carry only matched | plate | none (choose_for_item can never return a portrait).
- a picture below LEGIBILITY_MIN becomes a plate, with the measured reason in `why`.
- R3 (place portrait) is built only by choose_for_place.

legibility = resolution x cloud_free x edge_coherence x outline, each 0..1, all measured from the pixels
(resolution also from the stored crop box and the sensor's native ground resolution when known). Pure; no network.
"""
import io, math, re

LEGIBILITY_MIN = 0.30
ITEM_KINDS = ("matched", "plate", "none")
ALL_KINDS = ("matched", "portrait", "plate", "none")

# Native ground resolution in km per pixel of the layers the engine requests (imgfeeds.py).
# GIBS GOES ABI GeoColor is served at 2 km; MODIS Terra corrected reflectance true colour at 250 m.
NATIVE_KM = {"goes": 2.0, "modis": 0.25, "gibs": 0.25}
KM_DEG = 111.0
STORM_WORDS = ("hurricane", "typhoon", "tropical storm", "tropical cyclone", "cyclone", "tropical depression")


# ---------------------------------------------------------------- source and rung of a stored picture
def source_of(row):
    """img id / credit -> 'goes' | 'modis' | 'gibs' | 'shake' | 'radar' | 'wh' | 'wiki' | 'other'."""
    iid = (row.get("id") or "").lower()
    for key in ("goes", "modis", "gibs", "shake", "radar", "wiki", "eo", "pub", "gov"):
        if iid.startswith("img-" + key):
            return key
    if iid.startswith("img-white-house"):
        return "wh"
    return "other"


def rung_of(row):
    """-> 'R1' | 'R2' | 'R3' | None.
    R1 = a crop of the event itself (GOES/MODIS crop to the event box with crop == 'event', or the USGS ShakeMap of that quake).
    R2 = an official photograph of the event: NASA Earth Observatory (imgfeeds.eo_entries/eo_match), matched by shared proper name.
    R3 = a place portrait (Wikimedia place photo, or the official presidential portrait: a picture of a person or place, not of an act).
    None = a regional tile ('Region, not the event'), a national radar mosaic or anything unclassified: never a matched picture."""
    src, crop = source_of(row), row.get("crop")
    if src in ("eo", "pub", "gov"):
        return "R2"          # pub = publisher feed preview (news only); gov = the record issuer's own photograph          # NASA Earth Observatory photograph of the named event (imgfeeds.eo_match)
    if src == "shake":
        return "R1"
    if src in ("goes", "modis") and crop == "event":
        return "R1"
    if src == "wiki" or src == "wh" or crop in ("place", "portrait"):
        return "R3"
    return None


def box_of(row):
    """[s, w, n, e] of a stored crop: `crop_box`, else the BBOX of its WMS request (EPSG:4326, WMS 1.3.0 order lat,lon)."""
    box = (row or {}).get("crop_box")
    if box and len(box) == 4:
        return [float(x) for x in box]
    m = re.search(r"[?&]BBOX=([-0-9.]+),([-0-9.]+),([-0-9.]+),([-0-9.]+)", (row or {}).get("src_url") or "")
    return [float(x) for x in m.groups()] if m else None


# ---------------------------------------------------------------- legibility
def _arr(jpg):
    import numpy as np
    from PIL import Image
    im = Image.open(io.BytesIO(jpg)); im.load()
    return np.asarray(im.convert("RGB"), dtype=np.float32), im.size


def _box_mean(a, k):
    """k x k box filter (valid region kept the same size by edge padding)."""
    import numpy as np
    p = k // 2
    b = np.pad(a, p, mode="edge")
    c = b.cumsum(0).cumsum(1)
    c = np.pad(c, ((1, 0), (1, 0)))
    s = c[k:, k:] - c[:-k, k:] - c[k:, :-k] + c[:-k, :-k]
    return s[:a.shape[0], :a.shape[1]] / (k * k)


def effective_factor(gray_img):
    """Largest down-sampling factor f the picture survives (down by f, back up, RMS error < 4 grey levels).
    A tile up-scaled 6x from a coarse sensor survives f = 4 or more: it carries only w/f real pixels."""
    import numpy as np
    from PIL import Image
    g = np.asarray(gray_img, dtype=np.float32)
    w, h = gray_img.size
    best = 1
    for f in (2, 3, 4, 6, 8):
        small = gray_img.resize((max(1, w // f), max(1, h // f)), Image.BOX)
        back = np.asarray(small.resize((w, h), Image.BILINEAR), dtype=np.float32)
        rms = float(np.sqrt(np.mean((back - g) ** 2)))
        if rms < 4.0:
            best = f
        else:
            break
    return best


def legibility_score(jpg, row=None, kind=None, title=None):
    """-> {legibility 0..1, legible bool, terms{resolution, cloud_free, edge_coherence, outline}, measured{...}, reasons[]}.
    `row` = the img/index (manifest) row: id, crop, crop_box [s,w,n,e]; `kind` = the item kind it would illustrate.
    Every number comes from the pixels or the stored crop box."""
    import numpy as np
    from PIL import Image
    row = row or {}
    a, (w, h) = _arr(jpg)
    src = source_of(row)
    photo = src in ("wiki", "wh", "other", "pub", "gov")
    drawn = src in ("shake", "radar")
    reasons = []
    mx, mn = a.max(axis=2), a.min(axis=2)
    v = mx / 255.0
    s = np.where(mx > 0, (mx - mn) / np.maximum(mx, 1), 0)
    r, g, b = a[..., 0], a[..., 1], a[..., 2]

    # 1. no-data (swath gaps, black wedges) and cloud
    nodata = float(np.mean(mx < 14))
    cloud = float(np.mean((v > 0.62) & (s < 0.22) & (mx >= 14)))
    is_storm = (kind == "storm") or any(wd in (title or "").lower() for wd in STORM_WORDS)
    if photo or drawn:
        cloud_free = 1.0 - nodata
    elif is_storm:
        cloud_free = 1.0 - nodata            # the cloud is the subject of a storm picture
    else:
        cloud_free = max(0.0, 1.0 - cloud - nodata)
    if nodata > 0.02:
        reasons.append(f"no-data area {nodata:.0%} (swath gap or blank edge)")
    if not (photo or drawn or is_storm) and cloud > 0.5:
        reasons.append(f"cloud covers {cloud:.0%} of the ground")

    # 2. resolution: real pixels, measured, and from the crop box when the sensor is known
    gray = Image.fromarray(a.mean(axis=2).astype("uint8"))
    f = effective_factor(gray)
    eff_px = min(w, h) / f
    eff_meta = None
    box = box_of(row)
    if src in NATIVE_KM and box and len(box) == 4:
        s_, w_, n_, e_ = box
        lat = math.radians((s_ + n_) / 2)
        ground_km = min((n_ - s_) * KM_DEG, (e_ - w_) * KM_DEG * max(0.2, math.cos(lat)))
        eff_meta = ground_km / NATIVE_KM[src]
        eff_px = min(eff_px, eff_meta)
    resolution = max(0.0, min(1.0, eff_px / 400.0))
    if resolution < 0.6:
        reasons.append(f"only about {eff_px:.0f} real pixels across (up-scaled {min(w, h) / max(eff_px, 1):.1f}x)")

    # 3. edge coherence (structure tensor at the picture's real resolution): coastlines, roads, spiral bands score high; blur and speckle low
    k = max(1, int(round(min(w, h) / max(eff_px, 1))))
    gs = gray.resize((max(8, w // k), max(8, h // k)), Image.BOX) if k > 1 else gray
    G = np.asarray(gs, dtype=np.float32)
    gy, gx = np.gradient(_box_mean(G, 3))
    jxx, jyy, jxy = _box_mean(gx * gx, 7), _box_mean(gy * gy, 7), _box_mean(gx * gy, 7)
    en = jxx + jyy
    coh = np.sqrt((jxx - jyy) ** 2 + 4 * jxy ** 2) / (en + 1e-6)
    strong = en > np.percentile(en, 60)
    coherence = float(np.sum(coh[strong] * en[strong]) / (np.sum(en[strong]) + 1e-6)) if strong.any() else 0.0
    edge = max(0.0, min(1.0, (coherence - 0.15) / 0.35))
    if edge < 0.5:
        reasons.append(f"edges incoherent ({coherence:.2f}): nothing traces a shape")

    # 4. outline: a satellite picture is identifiable by a coast, lake or river edge (land next to water)
    if photo or drawn:
        outline = 1.0
    else:
        water = (b > r + 8) & (b >= g) & (v < 0.5) & (mx >= 14)
        land = ~water & ~((v > 0.62) & (s < 0.22)) & (mx >= 14)
        wf, lf = float(np.mean(water)), float(np.mean(land))
        outline = 1.0 if (wf >= 0.04 and lf >= 0.04) else 0.6
        if outline < 1:
            reasons.append("no coast or lake edge to recognise the place by")

    score = round(resolution * cloud_free * edge * outline, 3)
    ok = score >= LEGIBILITY_MIN
    if ok and not reasons:
        reasons.append("legible: real resolution, ground or subject visible, coherent shapes")
    return {"legibility": score, "legible": ok, "threshold": LEGIBILITY_MIN,
            "terms": {"resolution": round(resolution, 3), "cloud_free": round(cloud_free, 3), "edge_coherence": round(edge, 3), "outline": outline},
            "measured": {"width": w, "height": h, "effective_px": round(eff_px, 1), "effective_px_from_box": (round(eff_meta, 1) if eff_meta is not None else None),
                         "cloud": round(cloud, 3), "nodata": round(nodata, 3), "coherence": round(coherence, 3)},
            "reasons": reasons}


# ---------------------------------------------------------------- constructors
def uploaded_url(u):
    """-> u when it is an uploaded artifact asset url (/_blob/<hash>), else None (the page resolves it by image id)."""
    return u if isinstance(u, str) and u.startswith("/_blob/") else None


def _pic(kind, rung, row, leg, why):
    # the picture is named by its image id; its url is ONLY the uploaded one (/_blob/<hash>), which the page resolves from
    # img/index or img/h* by that id. Never a relative "img/<id>.jpg" (unpublished: every one 404'd live, LIVE-VERIFY-2).
    return {"kind": kind, "rung": rung, "img": row.get("id"), "url": uploaded_url(row.get("url")),
            "w": row.get("width"), "h": row.get("height"), "credit": row.get("credit"), "license": row.get("license"), "src_url": row.get("src_url"),
            "taken": row.get("taken"), "legibility": leg, "why": why,
            **({"sharp": bool(row.get("sharp"))} if "sharp" in row else {}),
            **({"link": row.get("src_url"), "publisher": row.get("publisher") or row.get("credit"), "posted": row.get("posted") or row.get("taken"),
                "via": row.get("via") or "feed"} if source_of(row) == "pub" else {}),
            **({"link": row.get("src_url"), "issuer": row.get("issuer")} if source_of(row) == "gov" else {})}


def plate(geo, why, type_="outline"):
    return {"kind": "plate", "plate": {"type": type_, "geo": geo}, "why": why}


def none(why):
    return {"kind": "none", "why": why}


def _slug(s):
    import re
    return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")


def item_geo(item, row=None):
    """The plate geography for an item: the crop box of its event picture ([w,s,e,n]) when known, else the state/country it names."""
    box = box_of(row) if rung_of(row or {}) in ("R1", "R2") else None
    if box:
        s_, w_, n_, e_ = box
        return [w_, s_, e_, n_], "map"
    p = item.get("place") or {}
    adm = p.get("admin") or p.get("name")
    if adm and _slug(adm) not in ("world", "not_stated", ""):
        return _slug(adm), "outline"
    return None, None


def _reg(host):
    h = (host or "").lower().split(":")[0]
    h = h[4:] if h.startswith("www.") else h
    p = h.split(".")
    return ".".join(p[-3:]) if len(p) >= 3 and p[-2] in ("co", "com", "org", "gov", "ac", "net") and len(p[-1]) == 2 else ".".join(p[-2:])


def _hosts_of(item):
    from urllib.parse import urlparse
    hs = {_reg(h) for h in item.get("publishers") or []}
    for s in (item.get("sources") or []) + (item.get("items") or []):
        if isinstance(s, dict) and s.get("url"):
            hs.add(_reg(urlparse(s["url"]).netloc))
    return hs


SEAL_ISSUERS = {"whitehouse.gov": "The White House", "congress.gov": "U.S. Congress", "senate.gov": "U.S. Senate", "house.gov": "U.S. House",
                "supremecourt.gov": "Supreme Court of the United States", "uscourts.gov": "U.S. Courts", "federalregister.gov": "Federal Register",
                "govinfo.gov": "U.S. Government Publishing Office", "in.gov": "State of Indiana"}


def record_issuer_of(item):
    """{host, name} of a record's own first source when it is a known seal issuer, else None (pure; no picture fetched)."""
    from urllib.parse import urlparse
    first = ((item.get("sources") or [{}])[0] or {}).get("url")
    h = _reg(urlparse(first or "").netloc) if first else ""
    return {"host": h, "name": SEAL_ISSUERS[h]} if h in SEAL_ISSUERS else None


def issuer_ok(item, row, cls):
    """R2 provenance: a publisher preview only on a news item that publisher reported; a government photo only on a record
    of that same issuer. -> None if allowed, else the reason."""
    src = source_of(row or {})
    if src == "pub":
        if cls != "news":
            return "a publisher preview belongs only to that publisher's news item"
        ph = _reg(row.get("publisher_host") or "")
        if ph and _hosts_of(item) and ph not in _hosts_of(item):
            return "picture from a publisher that did not report this item"
    if src == "gov":
        if cls != "record":
            return "an official record photograph belongs only to its record"
        first = (item.get("sources") or [{}])[0].get("url")
        from urllib.parse import urlparse
        if not first or _reg(urlparse(first).netloc) != _reg(row.get("issuer") or ""):
            return "photograph not published by this record's issuer"
    return None


def choose_for_item(item, row=None, jpg=None, leg=None, cls=None):
    """news / event / record item -> matched | plate | none. Never a portrait, whatever `row` is.
    `why` on a plate/none names the rung tried and why it failed (LEAP-PLAN §3.6)."""
    geo, gtype = item_geo(item, row)
    fb = (lambda why: plate(geo, why, gtype)) if geo else (lambda why: none(why + "; no place to draw, so the row goes text-only"))
    iss = record_issuer_of(item) if cls == "record" else None
    # M2: a record with no true-match photograph names its issuer, so the page can draw that issuer's seal (a mark of who acted,
    # never a picture of the act). No picture is attached; the plate/none stays honest.
    fallback = (lambda why: dict(fb(why), issuer=iss)) if iss else fb
    if not row:
        tried = {"news": "R2-pub (no usable preview in the publisher's feed)", "record": "R2-gov (the issuer published no photograph)"}.get(cls, "R1/R2 (no event crop or official photo)")
        return fallback(f"tried {tried}")
    why_not = issuer_ok(item, row, cls)
    if why_not:
        return fallback(why_not)
    rung = rung_of(row)
    if rung == "R3":
        return fallback("a place or person portrait is not a picture of this event (portraits appear only on place pages)")
    if rung not in ("R1", "R2"):
        return fallback("regional picture, not the event")
    if leg is None:
        if jpg is None:
            return fallback("picture bytes not available to measure legibility")
        leg = legibility_score(jpg, row, item.get("kind"), item.get("title"))
    if not leg["legible"]:
        return fallback(f"picture below legibility {LEGIBILITY_MIN} ({leg['legibility']}): " + "; ".join(leg["reasons"]))
    why = "event crop" if rung == "R1" else {"pub": ("publisher preview (og:image), linked" if row.get("via") == "og" else "publisher preview, linked"), "gov": "issuer's official photograph"}.get(source_of(row), "official event photo")
    return _pic("matched", rung, row, leg["legibility"], f"{why}; legibility {leg['legibility']}")


def choose_for_place(place_id, row=None, jpg=None, leg=None):
    """places/<id>.hero -> portrait (R3) | plate(outline of the place) | none."""
    if not row:
        return plate(place_id, "no licensed place photograph stored")
    if leg is None and jpg is not None:
        leg = legibility_score(jpg, row)
    if leg is not None and not leg["legible"]:
        return plate(place_id, f"place photograph below legibility {LEGIBILITY_MIN}: " + "; ".join(leg["reasons"]))
    return _pic("portrait", "R3", row, (leg or {}).get("legibility"), "place portrait (Wikimedia Commons, licence checked)")


def validate(choice, on="item"):
    """-> list of problems (empty = valid). on = 'item' (news/event/record) or 'place'."""
    bad = []
    k = (choice or {}).get("kind")
    if k not in ALL_KINDS:
        return [f"unknown kind {k!r}"]
    if on == "item" and k not in ITEM_KINDS:
        bad.append(f"{k} on a news/event/record item (R3 only on place pages)")
    if k == "matched" and choice.get("rung") not in ("R1", "R2"):
        bad.append(f"matched with rung {choice.get('rung')!r}")
    if k == "portrait" and choice.get("rung") != "R3":
        bad.append("portrait without rung R3")
    if k in ("matched", "portrait") and not (choice.get("img") and choice.get("credit") and choice.get("license")):
        bad.append("picture without img/credit/license")
    if k in ("matched", "portrait") and choice.get("url") is not None and uploaded_url(choice.get("url")) is None:
        bad.append(f"picture url {str(choice.get('url'))[:40]!r} is not an uploaded /_blob/ url (resolve by image id)")
    if k == "matched" and str(choice.get("img") or "").startswith("img-pub-") and not choice.get("link"):
        bad.append("publisher preview without a link through to the article")
    if k == "plate" and not ((choice.get("plate") or {}).get("geo") and (choice.get("plate") or {}).get("type") in ("outline", "map")):
        bad.append("plate without geo/type")
    if not choice.get("why"):
        bad.append("no why")
    return bad
