"""Image feeds for the Picture Plate (round 12). Public-domain or CC images only, one licence check per source.

Every image: {jpg, credit, license, src_url, taken, caption}. jpg <= 120 KB, long side <= 720 px.
Nothing is invented: a source that does not answer leaves the item without a picture (records say why in `img_note`).

Sources and licence basis
- goes     NOAA/NESDIS STAR GOES GeoColor (sector, full disk): US government work, public domain.
- radar    NWS national radar composite (radar.weather.gov): US government work, public domain.
- shakemap USGS ShakeMap intensity map (event detail feed): US government work, public domain.
- gibs     NASA EOSDIS GIBS MODIS true colour (+ thermal anomalies for fires, the FIRMS product): NASA open data, public domain.
- wh       Official presidential portrait on whitehouse.gov: US government work, public domain (17 USC 105).
- wiki     Wikimedia Commons file via the Wikipedia API: accepted only if its own licence is Public domain, CC0, CC BY or CC BY-SA.
"""
import datetime as dt, io, json, math, re, time, html as _html

MAX_BYTES = 120_000
MAX_LONG = 720
RF = 'Region, not the event: the warning area is wider than this picture, which shows its centre. '
OK_WIKI_LICENSE = re.compile(r"^(public domain|pd\b|cc0|cc[- ]by(-sa)?[- ]\d(\.\d)?(\s|$)|cc[- ]by(-sa)?$|attribution|u\.?s\.? government|no restrictions)", re.I)
BAD_WIKI_LICENSE = re.compile(r"\b(nc|nd|fair use|non-?commercial|gfdl only)\b", re.I)

# GOES-19 (GOES-East) CONUS sectors: centre lat, lon of each sector (nearest wins inside CONUS).
SECTORS = {"pnw": (45.5, -118.5), "psw": (36.5, -119.5), "nr": (45.0, -109.5), "sr": (36.5, -108.5), "sp": (32.5, -99.0),
           "umv": (43.5, -92.0), "cgl": (42.0, -86.0), "smv": (33.0, -90.0), "ne": (42.5, -73.5), "se": (30.5, -81.5)}
SECTOR_NAME = {"pnw": "Pacific Northwest", "psw": "Pacific Southwest", "nr": "Northern Rockies", "sr": "Southern Rockies", "sp": "Southern Plains",
               "umv": "Upper Mississippi Valley", "cgl": "Central Great Lakes", "smv": "Southern Mississippi Valley", "ne": "Northeast", "se": "Southeast"}
PRECIP = re.compile(r"tornado|thunderstorm|flood|rain|snow|winter|blizzard|squall|ice storm|freezing rain|hail|lake effect|storm warning|storm watch|sleet|tropical|hurricane", re.I)

PORTRAIT = "https://www.whitehouse.gov/wp-content/uploads/2026/01/President-Donald-Trump-Official-Presidential-Portrait.png-1-1.jpg"

PLACE_TITLES = {"marion": "Indianapolis", "indiana": "Indiana", "illinois": "Illinois", "ohio": "Ohio", "kentucky": "Kentucky", "michigan": "Michigan",
                "california": "California", "united_states": "United States", "world": "Earth", "canada": "Canada", "mexico": "Mexico", "china": "China",
                "russia": "Russia", "iran": "Iran", "saudi_arabia": "Saudi Arabia", "ukraine": "Ukraine", "israel": "Israel"}
SCENIC = re.compile(r"skyline|panorama|downtown|capitol|kremlin|parliament|landscape|aerial|cityscape|city|view|mountain|lake|coast|bridge|square", re.I)
SKIP_FILE = re.compile(r"flag|seal|coat[_ ]of|locator|location|map|icon|logo|symbol|commons-|wikisource|wiktionary|question|edit-|emblem|signature|portrait|\.svg$|\.png$|\.gif$|\.tif|\.webp$", re.I)


def _day(now):
    return (now - dt.timedelta(hours=14 if now.hour < 20 else 0)).strftime("%Y-%m-%d")


def shrink(body, long=MAX_LONG):
    """-> JPEG bytes <= MAX_BYTES, or None."""
    from PIL import Image
    im = Image.open(io.BytesIO(body))
    if im.mode in ("P", "LA", "RGBA"):
        im = im.convert("RGBA"); bg = Image.new("RGB", im.size, (255, 255, 255)); bg.paste(im, mask=im.split()[-1]); im = bg
    else:
        im = im.convert("RGB")
    for _ in range(6):
        if max(im.size) > long:
            f = long / max(im.size); im = im.resize((max(1, int(im.size[0] * f)), max(1, int(im.size[1] * f))), Image.LANCZOS)
        for q in (74, 66, 58, 50, 42, 34):
            buf = io.BytesIO(); im.save(buf, "JPEG", quality=q, optimize=True, progressive=True)
            if buf.tell() <= MAX_BYTES:
                return buf.getvalue()
        long = int(long * 0.82)
    return None


def _img_get(F, url, params=None, cap=12_000_000, gap=0.25, tries=2, timeout=(10, 40)):
    r = F.get(url, params=params, timeout=timeout, cap=cap, tries=tries, gap=gap)
    if not r.ok:
        return None, f"HTTP {r.status} {r.error or ''}".strip(), r
    ct = (r.headers.get("content-type") or "").lower()
    if "image" not in ct:
        return None, f"not an image ({ct or 'no content-type'})", r
    return r.body, None, r


def _lastmod(r, now):
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(r.headers.get("last-modified")).astimezone(dt.timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return now.strftime("%Y-%m-%d")


def _where(it):
    return ((it.get("place") or {}).get("name") or "").split(";")[0].strip() or "location not stated"


def nearest_sector(lat, lon):
    if not (24 <= lat <= 50 and -125.5 <= lon <= -66):
        return None
    return min(SECTORS, key=lambda k: (SECTORS[k][0] - lat) ** 2 + ((SECTORS[k][1] - lon) * math.cos(math.radians(lat))) ** 2)


# ---------------------------------------------------------------- sources
def goes(F, now, sector):
    sat = "GOES19"
    if sector == "hi":
        sat, path = "GOES18", "SECTOR/hi/GEOCOLOR/1200x1200.jpg"
    elif sector == "fd":
        path = "FD/GEOCOLOR/1808x1808.jpg"
    else:
        path = f"SECTOR/{sector}/GEOCOLOR/1200x1200.jpg"
    url = f"https://cdn.star.nesdis.noaa.gov/{sat}/ABI/{path}"
    body, err, r = _img_get(F, url, cap=6_000_000)
    if err:
        return None, err
    jpg = shrink(body)
    if not jpg:
        return None, "could not reach 120 KB"
    area = "the whole Western Hemisphere disk" if sector == "fd" else ("Hawaii" if sector == "hi" else SECTOR_NAME[sector] + " sector")
    taken = _lastmod(r, now)
    sat_name = "GOES-West (GOES-18)" if sat == "GOES18" else "GOES-East (GOES-19)"
    return {"jpg": jpg, "credit": f"NOAA/NESDIS STAR, {sat_name} GeoColor imagery", "license": "Public domain (US government work, NOAA)",
            "src_url": url, "taken": taken, "crop": "region", "caption": f"Region, not the event: latest {sat_name} GeoColor satellite view of {area}."}, None


def radar(F, now):
    url = "https://radar.weather.gov/ridge/standard/CONUS-LARGE_0.gif"
    body, err, r = _img_get(F, url, cap=3_000_000)
    if err:
        return None, err
    jpg = shrink(body)
    if not jpg:
        return None, "could not reach 120 KB"
    return {"jpg": jpg, "credit": "National Weather Service, national radar mosaic (radar.weather.gov)", "license": "Public domain (US government work, NWS)",
            "src_url": url, "taken": _lastmod(r, now), "crop": "region", "caption": "Region, not the event: latest National Weather Service radar mosaic for the contiguous United States."}, None


def shakemap(F, it, now):
    """USGS ShakeMap intensity map for a quake that has one."""
    sid = None
    for s in it.get("sources") or []:
        m = re.search(r"/earthquakes/eventpage/([a-z0-9]+)", s.get("url") or "")
        if m:
            sid = m.group(1)
    if not sid:
        return None, "no USGS event id"
    r = F.get("https://earthquake.usgs.gov/fdsnws/event/1/query", params={"eventid": sid, "format": "geojson"}, timeout=(10, 25), tries=2)
    if not r.ok:
        return None, f"detail HTTP {r.status}"
    try:
        sm = (r.json()["properties"]["products"] or {}).get("shakemap")
        url = sm[0]["contents"]["download/intensity.jpg"]["url"]
    except Exception:
        return None, "no ShakeMap product for this event"
    body, err, _ = _img_get(F, url, cap=4_000_000)
    if err:
        return None, err
    jpg = shrink(body)
    if not jpg:
        return None, "could not reach 120 KB"
    mag = it.get("mag") or ""
    return {"jpg": jpg, "credit": "USGS Earthquake Hazards Program, ShakeMap", "license": "Public domain (US government work, USGS)",
            "src_url": url, "taken": (it.get("t") or now.strftime("%Y-%m-%d"))[:10], "crop": "event",
            "caption": f"USGS ShakeMap of estimated shaking intensity for {it.get('title')}."}, None


def _blank(body):
    from PIL import Image, ImageStat
    im = Image.open(io.BytesIO(body)).convert("L")
    return ImageStat.Stat(im).stddev[0] < 7


def gibs(F, now, lat, lon, fire=False, half=2.5):
    for back in (0, 1):
        d, err = _gibs(F, now - dt.timedelta(days=back), lat, lon, fire, half)
        if d:
            return d, None
    return None, err


def _gibs(F, now, lat, lon, fire=False, half=2.5):
    date = _day(now)
    lonh = min(half / max(0.3, math.cos(math.radians(lat))), 20)
    bbox = f"{max(-90, lat - half):.3f},{max(-180, lon - lonh):.3f},{min(90, lat + half):.3f},{min(180, lon + lonh):.3f}"
    layers = "MODIS_Terra_CorrectedReflectance_TrueColor" + (",MODIS_Terra_Thermal_Anomalies_All" if fire else "")
    params = {"SERVICE": "WMS", "REQUEST": "GetMap", "VERSION": "1.3.0", "LAYERS": layers, "CRS": "EPSG:4326", "BBOX": bbox, "WIDTH": "640", "HEIGHT": "640", "FORMAT": "image/jpeg", "TIME": date}
    url = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
    body, err, _ = _img_get(F, url, params=params, cap=3_000_000, timeout=(10, 30))
    if err:
        return None, err
    if _blank(body):
        return None, f"no imagery for that box on {date} (blank tile)"
    jpg = shrink(body)
    if not jpg:
        return None, "could not reach 120 KB"
    full = url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    prod = "MODIS Terra true colour with FIRMS thermal anomalies (fire detections)" if fire else "MODIS Terra true colour"
    return {"jpg": jpg, "credit": f"NASA EOSDIS Global Imagery Browse Services (GIBS), {prod}, {date}", "license": "NASA open data; public domain per NASA media usage guidelines",
            "src_url": full, "taken": date, "crop": "region",
            "caption": f"Region, not the event: NASA satellite view of the region around {abs(lat):.0f} degrees {'north' if lat >= 0 else 'south'}, {abs(lon):.0f} degrees {'east' if lon >= 0 else 'west'}, about {int(2 * half)} degrees across, {date}. Cloud can hide the ground."}, None



# ---------------------------------------------------------------- extents (round 14): the picture is of the event, not the region
KM_DEG = 111.0
PAD = 1.25
QUAKE_KM, STORM_KM = 150.0, 600.0
MAX_HALF_DEG = 6.0
MIN_HALF_DEG = 0.6


def _coords(g):
    if not isinstance(g, dict):
        return []
    out = []
    def walk(c):
        if c and isinstance(c[0], (int, float)):
            out.append((c[0], c[1]))
        else:
            for z in c or []:
                walk(z)
    walk(g.get("coordinates"))
    return out


def event_extent(it):
    """-> {"bbox": (s, w, n, e), "why": str, "capped": bool} or None. Alert polygon + 25% padding; quake +-150 km; named storm +-600 km.
    The box is made square on the ground (a degree of longitude is shorter away from the equator) so the picture is not stretched."""
    p = it.get("place") or {}
    lat, lon, k = p.get("lat"), p.get("lon"), it.get("kind")
    half = None; why = None
    pts = _coords(p.get("geom"))
    if k in ("alert", "flood", "storm_report", "lsr") and len(pts) >= 3:
        lons = [x for x, _ in pts]; lats = [y for _, y in pts]
        if max(lons) - min(lons) > 180:
            return None
        clat = (min(lats) + max(lats)) / 2; clon = (min(lons) + max(lons)) / 2
        half = max((max(lats) - min(lats)) / 2, (max(lons) - min(lons)) / 2 * math.cos(math.radians(clat))) * PAD
        lat, lon, why = clat, clon, "alert area plus 25% padding"
    elif k == "quake" and lat is not None and lon is not None:
        half, why = QUAKE_KM / KM_DEG, "epicentre plus or minus 150 km"
    elif k == "storm" and lat is not None and lon is not None:
        half, why = STORM_KM / KM_DEG, "storm centre plus or minus 600 km"
    if half is None or lat is None or lon is None:
        return None
    capped = half > MAX_HALF_DEG
    half = min(max(half, MIN_HALF_DEG), MAX_HALF_DEG)
    lonh = min(half / max(0.25, math.cos(math.radians(lat))), 25)
    bbox = (max(-90, lat - half), lon - lonh, min(90, lat + half), lon + lonh)
    return {"bbox": bbox, "why": why, "capped": capped}


def _box_words(b):
    s, w, n, e = b
    f = lambda v, p, m: f"{abs(v):.1f} {p if v >= 0 else m}"
    return f"{f(s, 'N', 'S')} to {f(n, 'N', 'S')}, {f(w, 'E', 'W')} to {f(e, 'E', 'W')}"


def _wms(F, layers, bbox, date, fmt="image/jpeg", size=640, timeparam=True):
    s, w, n, e = bbox
    params = {"SERVICE": "WMS", "REQUEST": "GetMap", "VERSION": "1.3.0", "LAYERS": layers, "CRS": "EPSG:4326",
              "BBOX": f"{s:.3f},{w:.3f},{n:.3f},{e:.3f}", "WIDTH": str(size), "HEIGHT": str(size), "FORMAT": fmt}
    if timeparam and date:
        params["TIME"] = date
    url = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
    body, err, _ = _img_get(F, url, params=params, cap=3_000_000, timeout=(10, 30))
    return body, err, url + "?" + "&".join(f"{k}={v}" for k, v in params.items())


def goes_crop(F, now, ext, title):
    """GOES GeoColor from NASA GIBS (near real time) cropped to the event box. East for lon -120..-20, West for -170..-120."""
    s, w, n, e = ext["bbox"]; clon = (w + e) / 2
    if -120 <= clon <= -20:
        layer, sat = "GOES-East_ABI_GeoColor", "GOES-East (GOES-19)"
    elif -170 <= clon < -120:
        layer, sat = "GOES-West_ABI_GeoColor", "GOES-West (GOES-18)"
    else:
        return None, "outside the GOES-East and GOES-West view"
    body, err, full = _wms(F, layer, ext["bbox"], None)
    if err:
        return None, err
    if _blank(body):
        return None, "GOES tile blank for that box"
    jpg = shrink(body)
    if not jpg:
        return None, "could not reach 120 KB"
    day = now.strftime("%Y-%m-%d")
    return {"jpg": jpg, "credit": f"NOAA GOES ABI GeoColor ({sat}) via NASA EOSDIS GIBS", "license": "Public domain (US government work, NOAA and NASA)",
            "src_url": full, "taken": day, "crop": "region" if ext["capped"] else "event", "crop_box": [round(v, 3) for v in ext["bbox"]],
            "caption": f"{RF if ext['capped'] else ''}Latest {sat} GeoColor satellite view cropped to the event: {title}. The box runs {_box_words(ext['bbox'])} ({ext['why']}). Taken {day}."}, None


def modis_crop(F, now, ext, title, fire=False):
    for back in (0, 1):
        date = _day(now - dt.timedelta(days=back))
        layers = "MODIS_Terra_CorrectedReflectance_TrueColor" + (",MODIS_Terra_Thermal_Anomalies_All" if fire else "")
        body, err, full = _wms(F, layers, ext["bbox"], date)
        if err:
            continue
        if _blank(body):
            err = f"no imagery for that box on {date} (blank tile)"; continue
        jpg = shrink(body)
        if not jpg:
            err = "could not reach 120 KB"; continue
        return {"jpg": jpg, "credit": f"NASA EOSDIS Global Imagery Browse Services (GIBS), MODIS Terra true colour, {date}", "license": "NASA open data; public domain per NASA media usage guidelines",
                "src_url": full, "taken": date, "crop": "region" if ext["capped"] else "event", "crop_box": [round(v, 3) for v in ext["bbox"]],
                "caption": f"{RF if ext['capped'] else ''}NASA MODIS Terra true-colour view cropped to the event: {title}. The box runs {_box_words(ext['bbox'])} ({ext['why']}). Taken {date}; cloud can hide the ground."}, None
    return None, err


def portrait(F, now):
    body, err, r = _img_get(F, PORTRAIT, cap=3_000_000)
    if err:
        return None, err
    jpg = shrink(body, 520)
    if not jpg:
        return None, "could not reach 120 KB"
    return {"jpg": jpg, "credit": "The White House, official presidential portrait", "license": "Public domain (US government work, 17 U.S.C. 105)",
            "src_url": PORTRAIT, "taken": "posted 2026-01 (portrait, not a photo of the signing)", "crop": "portrait",
            "caption": "Official portrait of the President who issues presidential documents. The White House and the Federal Register publish no photograph of an individual action, so this is the same picture on every presidential record."}, None


def _clean(s):
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", "", s or ""))).strip()


def _taken_clean(t):
    """'17 March 2014 (original upload date)' -> 'uploaded 2014-03-17'; '1805 date QS:...' -> '1805'; never a fragment."""
    up = "upload" in t.lower()
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
    if not m:
        m2 = re.search(r"(\d{1,2}) ([A-Za-z]+) (\d{4})", t)
        if m2:
            try:
                d = dt.datetime.strptime(f"{m2.group(1)} {m2.group(2)[:3]} {m2.group(3)}", "%d %b %Y"); out = d.strftime("%Y-%m-%d")
                return ("uploaded " if up else "") + out
            except Exception:
                pass
    else:
        return ("uploaded " if up else "") + m.group(0)
    y = re.search(r"\b(1[0-9]{3}|20[0-9]{2})\b", t)
    return (y.group(1) if y else "date not stated")


def wiki_places(F, now, titles):
    """{place_id: (img dict|None, why)}: lead photo of each Wikipedia article, the Commons file's own licence checked.
    Batched (two API calls for all places) because the API rate-limits per request, then one download per photo."""
    W = "https://en.wikipedia.org/w/api.php"
    out = {pid: (None, "not tried") for pid in titles}
    def api(params):
        err = "no attempt"
        for _ in range(4):
            r = F.get(W, params=params, timeout=(10, 30), tries=1, gap=1.5)
            if r.ok:
                return r.json(), None
            err = f"HTTP {r.status} {r.error or ''}".strip()
            ra = (r.headers or {}).get("retry-after", "")
            time.sleep(min(int(ra), 60) + 2 if ra.isdigit() else 8)
        return None, err
    norm, pages, cont = {}, {}, {}
    for _ in range(12):
        j, err = api({"action": "query", "titles": "|".join(titles.values()), "prop": "images", "imlimit": "500", "format": "json", "redirects": "1", **cont})
        if not j:
            if pages:
                break
            return {pid: (None, "article list " + err) for pid in titles}
        for key in ("normalized", "redirects"):
            for n in j["query"].get(key, []):
                norm[n["from"]] = n["to"]
        for p in j["query"]["pages"].values():
            q = pages.setdefault(p["title"], {"title": p["title"], "images": []}); q["images"] += p.get("images", [])
        cont = j.get("continue") or {}
        if not cont:
            break
    lead = {}
    jl, _e = api({"action": "query", "titles": "|".join(titles.values()), "prop": "pageimages", "piprop": "name", "pilimit": "50", "format": "json", "redirects": "1"})
    for p in ((jl or {}).get("query") or {}).get("pages", {}).values():
        if p.get("pageimage"):
            lead[p["title"]] = "File:" + p["pageimage"].replace("_", " ")
    pick = {}
    for pid, t in titles.items():
        t2 = norm.get(norm.get(t, t), norm.get(t, t))
        p = pages.get(t2) or pages.get(t)
        files = [i["title"] for i in (p or {}).get("images", []) if i["title"].lower().endswith((".jpg", ".jpeg")) and not SKIP_FILE.search(i["title"])]
        ld = lead.get(t2) or lead.get(t)
        if ld and ld.lower().endswith((".jpg", ".jpeg")) and not re.search(r"flag|seal|coat[_ ]of|map|locator|logo|emblem", ld, re.I):
            files = [ld] + [f for f in files if f != ld]
        files.sort(key=lambda f: 0 if SCENIC.search(f) else 1)   # stable: skyline/capitol/landscape first, lead image next
        files = files[:6]
        pick[pid] = files
        if not files:
            out[pid] = (None, "no photo file in the article")
    allf = sorted({f for fs in pick.values() for f in fs})
    meta = {}
    for i in range(0, len(allf), 40):
        j2, err = api({"action": "query", "titles": "|".join(allf[i:i + 40]), "prop": "imageinfo", "iiprop": "url|extmetadata", "iiurlwidth": "720", "format": "json"})
        if not j2:
            for pid, fs in pick.items():
                if fs and out[pid][0] is None:
                    out[pid] = (None, "imageinfo " + err)
            continue
        fn = {n["from"]: n["to"] for n in j2["query"].get("normalized", [])}
        for p in j2["query"]["pages"].values():
            if p.get("imageinfo"):
                meta[p["title"]] = p["imageinfo"][0]
    for pid, fs in pick.items():
        why = out[pid][1]
        for f in fs:
            ii = meta.get(f)
            if not ii:
                continue
            md = ii.get("extmetadata") or {}
            lic = _clean((md.get("LicenseShortName") or {}).get("value"))
            if not lic or BAD_WIKI_LICENSE.search(lic) or not OK_WIKI_LICENSE.match(lic):
                why = f"licence not accepted ({lic or 'none stated'})"; continue
            artist = _clean((md.get("Artist") or {}).get("value")) or "author not stated"
            h = len(artist) // 2
            if len(artist) % 2 == 0 and artist[:h] == artist[h:]:
                artist = artist[:h]
            taken = _clean((md.get("DateTimeOriginal") or {}).get("value")) or "date not stated"
            taken = _taken_clean(taken)
            body, err, _ = _img_get(F, ii.get("thumburl") or ii["url"], cap=5_000_000, gap=1.0, tries=2)
            if err:
                why = err; continue
            jpg = shrink(body)
            if not jpg:
                why = "could not reach 120 KB"; continue
            out[pid] = ({"jpg": jpg, "credit": f"{artist}, via Wikimedia Commons" + (", attribution required" if lic.upper().startswith("CC BY") else ""), "license": lic,
                         "src_url": ii.get("descriptionurl") or ii["url"], "taken": taken, "crop": "place",
                         "caption": f"{titles[pid].replace('_', ' ')}: {re.sub(r'\.jpe?g$', '', f.replace('File:', ''), flags=re.I)} (picture from the Wikipedia article, hosted on Wikimedia Commons)."}, None)
            break
        else:
            out[pid] = (None, why)
    return out


# ---------------------------------------------------------------- orchestration

# ---------------------------------------------------------------- R2: NASA Earth Observatory event photographs (FABLE-MIDBUILD #3)
# The old earthobservatory.nasa.gov/feeds/natural-hazards.rss now 301s to this NASA Science feed (EO + Photojournal mixed);
# only items in the "Earth Observatory" category are used. NASA imagery: public domain, credit "NASA Earth Observatory".
EO_FEED = "https://science.nasa.gov/feed/?science_org=22414%2C19791"
EO_LICENSE = "Public domain (NASA Earth Observatory image; NASA media usage guidelines)"
_EO_STOP = {"strikes", "arrive", "arrives", "past", "whips", "char", "chars", "dims", "fires", "storm", "cyclone", "typhoon", "hurricane",
            "the", "and", "with", "from", "into", "over", "near", "nasa", "views", "rainforest", "coast", "earth", "extratropical", "tropical"}


def eo_entries(xml):
    """Earth Observatory items from the NASA Science RSS -> [{title, link, pub, img, cats}]. Pure (parser over saved bytes)."""
    import html as _h
    txt = xml.decode("utf-8", "replace") if isinstance(xml, bytes) else xml
    out = []
    for it in re.findall(r"<item>(.*?)</item>", txt, re.S):
        cats = [c.strip() for c in re.findall(r"<category><!\[CDATA\[(.*?)\]\]>", it)]
        if "Earth Observatory" not in cats:
            continue
        t = re.search(r"<title>(.*?)</title>", it, re.S); l = re.search(r"<link>(.*?)</link>", it, re.S); d = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S)
        im = re.search(r'<img[^>]+src="(https://assets\.science\.nasa\.gov/[^"]+/esd/eo/images/[^"]+)"', it)
        if not (t and l and im):
            continue
        out.append({"title": _h.unescape(t.group(1).strip()), "link": l.group(1).strip(), "pub": (d.group(1).strip() if d else None),
                    "img": _h.unescape(im.group(1)).split("?")[0] + "?w=1440", "cats": cats})
    return out


def _eo_names(title):
    return {w.lower() for w in re.findall(r"[A-Z][a-zA-Z'\u2019-]{3,}", title or "") if w.lower() not in _EO_STOP}


def eo_match(item, entries):
    """The Earth Observatory entry photographing this item's event: every proper name in the photo title (storm, place) also named by the item,
    and a hazard kind the photo's categories cover. None when nothing matches (never a nearest guess)."""
    kind = item.get("kind") or ""
    names = _eo_names(item.get("title")) | _eo_names(((item.get("place") or {}).get("name")))
    if not names:
        return None
    for e in entries:
        cats = " ".join(e["cats"]).lower()
        fits = (("fire" in kind and "wildfire" in cats) or (kind in ("storm", "alert", "flood", "news", "current_event") and ("hurricane" in cats or "weather" in cats or "flood" in cats))
                or kind == "volcano" and "volcan" in cats or ("wildfire" in cats and kind in ("news", "current_event")))
        en = _eo_names(e["title"])
        if fits and en and en <= names:          # every proper name in the photo's title is named by the item (Arnab AND India)
            return e
    return None


def eo_photo(F, e):
    """Download one matched EO photo -> (image dict, None) | (None, why)."""
    r = F.get(e["img"], tries=2)
    if not r.ok or not r.body:
        return None, f"HTTP {r.status} {r.error or ''}".strip()
    jpg = shrink(r.body)
    if not jpg:
        return None, "could not reach 120 KB"
    taken = None
    if e.get("pub"):
        try:
            taken = dt.datetime.strptime(e["pub"][5:16], "%d %b %Y").strftime("%Y-%m-%d")
        except ValueError:
            taken = None
    return {"jpg": jpg, "credit": "NASA Earth Observatory", "license": EO_LICENSE, "src_url": e["link"], "taken": taken, "crop": "event", "crop_box": None,
            "caption": f"NASA Earth Observatory: {e['title']}." + (f" Published {taken}." if taken else "")}, None

def build_all(F, now, events, rec_final, place_ids, log, max_gibs=30, max_shake=6, max_crop=40):
    """-> (files [(img_id, dict)], item_img {item_id: img_id}, place_img {place_id: img_id}). `log` collects ok/failed/by_kind."""
    files, cache, item_img, place_img = {}, {}, {}, {}
    log.update({"ok": 0, "failed": [], "by_source": {}, "attached": {"events": 0, "records": 0, "places": 0}})

    def put(key, fn, src):
        """cached fetch; returns img id or None"""
        if key in cache:
            return cache[key]
        d, err = (None, "skipped")
        try:
            d, err = fn()
        except Exception as e:
            d, err = None, "error " + type(e).__name__
        if d:
            iid = "img-" + re.sub(r"[^a-z0-9]+", "-", key.lower()).strip("-")
            files[iid] = d; cache[key] = iid; log["by_source"][src] = log["by_source"].get(src, 0) + 1
        else:
            cache[key] = None; log["failed"].append({"key": key, "source": src, "why": err})
        return cache[key]

    hazards = [e for e in events if e.get("place") and e["kind"] in ("alert", "storm", "storm_report", "lsr", "flood", "tsunami", "quake", "disaster", "fire", "volcano") and e.get("layer") is not None]
    hazards.sort(key=lambda e: ((e.get("score") or {}).get("is_breaking", 0), e.get("sev", 0), (e.get("score") or {}).get("total", 0)), reverse=True)
    n_shake = n_gibs = n_crop = 0
    for e in hazards:
        p = e["place"]; lat, lon = p.get("lat"), p.get("lon"); k = e["kind"]; iid = None
        weather = k in ("alert", "storm", "storm_report", "lsr", "flood")
        if k == "quake" and n_shake < max_shake and (e.get("mag") or 0) >= 5.0:
            n_shake += 1
            iid = put("shake-" + e["id"], lambda e=e: shakemap(F, e, now), "shakemap")
        ext = event_extent(e) if not iid else None
        if ext and n_crop < max_crop:
            kb = re.sub(r"[^a-z0-9]+", "", e["id"].lower())[:40]
            ttl = e.get("title") or "event"
            gk = None
            if weather or k == "storm":
                gk = "goes-crop-" + kb + "-" + now.strftime("%Y%m%dT%H")
                if gk not in cache:
                    n_crop += 1
                iid = put(gk, lambda ext=ext, ttl=ttl: goes_crop(F, now, ext, ttl), "goes_crop")
                if iid and k == "storm":
                    log.setdefault("storms_goes", []).append(e.get("title"))
            if not iid:
                mk = "modis-crop-" + kb + "-" + _day(now)
                if mk not in cache:
                    n_crop += 1
                iid = put(mk, lambda ext=ext, ttl=ttl: modis_crop(F, now, ext, ttl), "modis_crop")
        if not iid and k == "storm":
            log.setdefault("storms_no_goes", []).append(e.get("title"))
        if not iid and weather and lat is not None and lon is not None:
            sec = nearest_sector(lat, lon)
            if sec and k in ("alert", "flood", "storm_report", "lsr") and PRECIP.search(e.get("title") or ""):
                iid = put("radar-conus-" + now.strftime("%Y%m%dT%H"), lambda: radar(F, now), "radar")
            elif sec:
                iid = put("goes-" + sec + "-" + now.strftime("%Y%m%dT%H"), lambda s=sec: goes(F, now, s), "goes")
            elif 18 <= lat <= 23 and -161 <= lon <= -154:
                iid = put("goes-hi-" + now.strftime("%Y%m%dT%H"), lambda: goes(F, now, "hi"), "goes")
        if not iid and k == "storm" and (lat is None or -140 <= (lon or -60) <= -20):
            iid = put("goes-fd-" + now.strftime("%Y%m%dT%H"), lambda: goes(F, now, "fd"), "goes")
        if not iid and lat is not None and lon is not None and n_gibs < max_gibs:
            fire = k == "fire"
            step = 5 if (fire or k == "quake") else 10
            tile = (round(lat / step) * step, round(lon / step) * step, fire)
            key = f"gibs-{'fire-' if fire else ''}{tile[0]}-{tile[1]}-{_day(now)}"
            if key not in cache:
                n_gibs += 1
            iid = put(key, lambda la=tile[0], lo=tile[1], f=fire, k=k: gibs(F, now, float(la), float(lo), f, half=3.0 if (f or k == 'quake') else 5.5), "gibs")
        if iid:
            item_img[e["id"]] = iid; e["img"] = iid; log["attached"]["events"] += 1

    # presidential records: one honest portrait, or a stated reason
    pres = rec_final.get("presidential") or []
    if pres:
        pid = put("white-house-portrait", lambda: portrait(F, now), "wh")
        for i in pres:
            if pid:
                i["img"] = pid; item_img[i["id"]] = pid; log["attached"]["records"] += 1
            else:
                i["img"] = None; i["img_note"] = "No picture: the official portrait did not download (" + (log["failed"][-1]["why"] if log["failed"] else "unknown") + ")."
    # R2: NASA Earth Observatory photo of the event itself (matched by a shared proper name; nothing matched -> nothing attached)
    try:
        rr = F.get(EO_FEED, tries=2)
        eo = eo_entries(rr.body) if rr.ok else []
        if not rr.ok:
            log["failed"].append({"key": "eo-feed", "source": "eo", "why": f"HTTP {rr.status} {rr.error or ''}".strip()})
    except Exception as e:
        eo = []; log["failed"].append({"key": "eo-feed", "source": "eo", "why": "error " + type(e).__name__})
    if eo:
        pool = list(events) + [r for v in (rec_final or {}).values() for r in v]
        for it in sorted(pool, key=lambda i: str(i.get("id"))):
            m = eo_match(it, eo)
            if not m:
                continue
            iid = put("eo-" + re.sub(r"[^a-z0-9]+", "-", m["link"].lower().rstrip("/").rsplit("/", 1)[-1])[:60], lambda m=m: eo_photo(F, m), "eo")
            if iid:
                it["img"] = iid; item_img[it["id"]] = iid
    # places (Wikimedia Commons, licence checked per file)
    titles = {pid: PLACE_TITLES[pid] for pid in place_ids if pid in PLACE_TITLES}
    res = {}
    todo = dict(titles)
    for attempt in range(2):
        try:
            got = wiki_places(F, now, todo)
        except Exception as e:
            got = {pid: (None, "error " + type(e).__name__) for pid in todo}
        res.update(got)
        todo = {pid: titles[pid] for pid, (d, err) in res.items() if not d and not str(err).startswith("licence")}
        if not todo:
            break
        time.sleep(20)
    for pid, (d, err) in list(res.items()):
        if d:
            _cache_put(pid, d)
        else:
            c = _cache_get(pid)
            if c:
                res[pid] = (c, None)
    for pid, (d, err) in res.items():
        if d:
            iid = "img-wiki-" + pid
            files[iid] = d; place_img[pid] = iid; log["attached"]["places"] += 1; log["by_source"]["wiki"] = log["by_source"].get("wiki", 0) + 1
        else:
            log["failed"].append({"key": "wiki-" + pid, "source": "wiki", "why": err})
    log["ok"] = len(files)
    return list(files.items()), item_img, place_img


CACHE_DIR = __import__("os").path.join(__import__("os").path.dirname(__import__("os").path.abspath(__file__)), "cache", "img_places")


def _cache_put(pid, d):
    """Keep the last good place photo (with its licence) so a rate-limited run still shows the same licensed picture."""
    import os
    os.makedirs(CACHE_DIR, exist_ok=True)
    open(os.path.join(CACHE_DIR, pid + ".jpg"), "wb").write(d["jpg"])
    json.dump({k: v for k, v in d.items() if k != "jpg"}, open(os.path.join(CACHE_DIR, pid + ".json"), "w"))


def _cache_get(pid):
    import os
    try:
        meta = json.load(open(os.path.join(CACHE_DIR, pid + ".json"))); meta["jpg"] = open(os.path.join(CACHE_DIR, pid + ".jpg"), "rb").read()
        meta["caption"] = meta["caption"] + " (last good copy from an earlier run)"
        return meta
    except Exception:
        return None


_LEG_CACHE = {}


def _scored(img_files):
    """{img_id: (manifest row, legibility dict)} for every fetched picture (P2: legibility_score replaces plate_score)."""
    from images import contract as C
    out = {}
    for iid, d in img_files or []:
        row = dict(manifest_rows([(iid, d)])[0], width=None, height=None)
        key = (d.get("sha256") or hashlib.sha256(d["jpg"]).hexdigest(), json.dumps(row, sort_keys=True, default=str))
        hit = _LEG_CACHE.get(key)      # M4: one score per picture per run (it was computed up to 4x: photo pass, attach x2, manifest)
        if hit is None:
            try:
                leg = C.legibility_score(d["jpg"], row)
            except Exception as e:
                leg = {"legibility": 0.0, "legible": False, "reasons": ["unscorable: " + type(e).__name__], "measured": {}}
            hit = _LEG_CACHE[key] = leg
        leg = json.loads(json.dumps(hit))
        if leg.get("measured"):
            row["width"], row["height"] = leg["measured"].get("width"), leg["measured"].get("height")
        out[iid] = (row, leg)
    return out


def attach_places(docs, place_img, img_files=None):
    """Set `img` (v3, read-only for one build) and `hero: ImageChoice` (v4; R3 portrait allowed here only) on places/<id> docs."""
    from images import contract as C
    sc = _scored(img_files)
    for coll, did, data in docs:
        if coll != "places":
            continue
        if did in place_img:
            data["img"] = place_img[did]
        iid = place_img.get(did)
        row, leg = sc.get(iid, (None, None))
        data["hero"] = C.choose_for_place(did, row, leg=leg) if row else C.plate(did, "no licensed place photograph stored")


ITEM_LISTS = ("items", "events", "wire", "breaking", "clusters", "news")


def attach_items(docs, item_img, img_files=None):
    """Every item inside events/wire/records/live/news docs gets `image: ImageChoice` (matched R1/R2 | plate | none, never R3).
    v3 `img` is still copied for one build (read-only). A picture is matched only if it is an event crop and legible."""
    from images import contract as C
    sc = _scored(img_files)
    for coll, did, data in docs:
        if coll not in ("events", "wire", "records", "live", "news"):
            continue
        for key in ITEM_LISTS:
            for it in data.get(key, []) if isinstance(data.get(key), list) else []:
                if not isinstance(it, dict):
                    continue
                if it.get("id") in item_img and not it.get("img"):
                    it["img"] = item_img[it["id"]]
                iid = it.get("img") or item_img.get(it.get("id"))
                row, leg = sc.get(iid, (None, None))
                if row is not None and leg is not None and leg.get("measured"):
                    leg = C.legibility_score(dict(img_files)[iid]["jpg"], row, it.get("kind"), it.get("title")) if it.get("kind") == "storm" else leg
                cls = "news" if (key in ("news", "clusters") or it.get("kind") == "news" or "publishers" in it) else ("record" if (coll == "records" or "stage" in it) else "event")
                it["image"] = C.choose_for_item(it, row, leg=leg, cls=cls) if row else C.choose_for_item(it, None, cls=cls)


def manifest_rows(files):
    keep = ("via", "publisher", "publisher_host", "posted", "issuer", "img_src", "sha256", "bytes", "ttl_s", "width", "height", "sharp")
    return [dict({"id": iid, "file": f"img/{iid}.jpg", "credit": d["credit"], "license": d["license"], "src_url": d["src_url"], "taken": d.get("taken"), "caption": d["caption"], "crop": d.get("crop", "region"), "crop_box": d.get("crop_box")},
                 **{k: d[k] for k in keep if k in d}) for iid, d in files]


# ================================================================ P10 (LEAP-PLAN §3): R2-pub, R2-gov, budget, honest counts
# R2-pub = the preview picture a publisher ships IN ITS OWN FEED for an article (media:content / enclosure / media:thumbnail).
# M2: plus, only for items whose feed shipped no picture, the og:image share picture from the article page's <head> (og_photos).
# Never a search, never a publisher that is blocked or declined, never a paywalled page. Credit = the publisher; the page always
# links through to the article (src_url = the article URL). R2-gov = a photograph published by the record's own issuer.
import hashlib

PUB_LICENSE = "publisher preview, linked"
# M2 sizes (measured 2026-11-01 on live feed pictures, see M2-ENGINE-NOTES): 1280 px long side = 2x a 390 pt full-bleed card at
# width; q 80 -> 60 ladder. Byte cap = smallest that keeps >= 95 % of real 1280 px feed pictures at q >= 72: measured on 101 live
# pictures from 30 publisher feeds, q72 median 107 KB, p90 167 KB, p95 180 KB (q76 p95 195 KB, q80 p95 218 KB).
PHOTO_BUDGET = 150                # picture fetches per run (hourly or morning), news first; ~1.6 per top-60 cluster incl. one larger-rendition retry
PHOTO_MAX_BYTES = 180_000         # every stored asset photo <= 180 KB JPEG (p95 at q72)
DATA_URI_MAX_BYTES = 160_000      # fetch.make_image (data URI inside a db doc): 160 KB JPEG = 213 KB base64, under fetch.DOC_LIMIT 240 KB
PHOTO_LONG = 1280                 # long side (never up-scaled: a smaller original is stored at its own size)
PHOTO_Q = (80, 76, 72, 68, 64, 60)
PHOTO_TTL_S = 7 * 86400
SHARP_LONG = 780                  # long side that is >= 2x a 390 px rendered card: `sharp` flag for hero placement
PUB_MIN_W, PUB_ASPECT, PUB_MIN_SIGMA, BANNER_MAX = 480, (1.0, 2.0), 12.0, 2
# logo / flag / seal class of SKIP_FILE, applied to the picture URL (SKIP_FILE's .png/.webp/map rules are for Wikipedia files, not previews)
PUB_SKIP = re.compile(r"logo|flag|seal|emblem|coat[_ -]?of|icon|favicon|sprite|placeholder|default[_-]?(image|img|thumb)|avatar|badge|watermark|og[_-]?default|brand", re.I)
DECLINED_FILE = __import__("os").path.join(__import__("os").path.dirname(__import__("os").path.abspath(__file__)), "data", "pub_photos_declined.txt")
REJECT_REASONS = ("declined", "blocked", "no_feed_picture", "robots", "paywall", "page_http", "no_og", "no_page", "og_small", "logo", "banner", "http", "not_image", "too_narrow", "aspect", "flat", "bytes_cap", "budget", "undecodable", "not_issuer", "no_title_match", "illegible")


def _reg(host):
    h = (host or "").lower().split(":")[0]
    h = h[4:] if h.startswith("www.") else h
    parts = h.split(".")
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "gov", "ac", "net") and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _host(url):
    from urllib.parse import urlparse
    return _reg(urlparse(url or "").netloc)


def declined_hosts(path=DECLINED_FILE):
    """Publishers that declined or block reuse of their previews (one registrable domain per line, '#' comments). Missing file = none listed."""
    try:
        return {_reg(l.split("#")[0].strip()) for l in open(path, encoding="utf-8") if l.split("#")[0].strip()}
    except OSError:
        return set()


IMG_IN_HTML = re.compile(r"<img[^>]+src=[\"']([^\"']+)[\"']", re.I)


def entry_image(e):
    """The feed's own picture for one feedparser entry -> URL | None. media:content (medium/type image) > enclosure (image/*) >
    media:thumbnail > an <img> inside the item's own feed HTML (content / summary, as the publisher shipped it in the feed);
    among several, the widest declared. Pure; reads only what the feed shipped, never the article page."""
    cands = []
    for m in (e.get("media_content") or []):
        u = m.get("url")
        if u and (m.get("medium") == "image" or str(m.get("type") or "").startswith("image") or re.search(r"\.(jpe?g|png|webp)(\?|$)", u, re.I)):
            cands.append((0, -int(m.get("width") or 0), u))
    for m in (e.get("enclosures") or e.get("links") or []):
        u = m.get("href") or m.get("url")
        if u and str(m.get("type") or "").startswith("image") and (m.get("rel") in (None, "enclosure")):
            cands.append((1, 0, u))
    for m in (e.get("media_thumbnail") or []):
        u = m.get("url")
        if u:
            cands.append((2, -int(m.get("width") or 0), u))
    if not cands:
        html = " ".join(str(c.get("value") or "") for c in (e.get("content") or [])) + " " + str(e.get("summary") or "")
        for u in IMG_IN_HTML.findall(html):
            u = u.replace("&amp;", "&")
            if u.startswith("https://") and not re.search(r"feedburner|pixel|tracker|gravatar|1x1|spacer|emoji", u, re.I):
                cands.append((3, 0, u)); break
    if not cands:
        return None
    cands.sort(key=lambda c: (c[1], c[0]))
    return cands[0][2]


# A larger rendition of the SAME feed picture on the publisher's own image CDN (only the size token changes; signed URLs are left
# alone). Tried first; the URL the feed shipped is the fallback. Never a different picture, never an article page.
UPSIZE = (
    (re.compile(r"^(https://ichef\.bbci\.co\.uk/(?:ace|news)/(?:standard|ws)/)\d{2,4}(/.*)$"), r"\g<1>1536\2"),
    (re.compile(r"^(https://static01\.nyt\.com/images/.+-)(?:mediumSquareAt3X|thumbStandard|thumbLarge|articleLarge|jumbo|square640|videoSixteenByNine\w*|threeByTwo\w*)(\.jpe?g)$"), r"\1superJumbo\2"),
    (re.compile(r"^(https://s\.france24\.com/media/display/[^/]+/)w:\d+(/.*)$"), r"\1w:1280\2"),
    (re.compile(r"^(https://[^?]+/wp-content/uploads/[^?]+?)-(?:[1-9]\d{1,2}|1[01]\d\d|12[0-7]\d)x\d{2,4}(\.jpe?g)(\?.*)?$", re.I), r"\1\2"),
    (re.compile(r"^(https://[^?]+\.jpe?g\?(?:.*&)?w=)(?:[1-9]\d{1,2}|1[01]\d\d|12[0-7]\d)((?:&.*)?)$", re.I), r"\g<1>1600\2"),
)


def upsize(url):
    """-> the larger rendition URL on the same publisher CDN, or None when no rule applies or the URL is signed."""
    if not url or re.search(r"[?&](s|sig|signature|token|itok)=", url) and "wp-content" not in url:
        return None
    for rx, rep in UPSIZE:
        if rx.match(url):
            u = rx.sub(rep, url)
            return u if u != url else None
    return None


def photo_jpeg(body, long=None, max_bytes=None):
    """Quality gates on the publisher's own pixels, then <= PHOTO_LONG (1280) px long side (never up-scaled), JPEG q 80 -> 60 until
    <= PHOTO_MAX_BYTES.
    -> (jpg, {w, h, src_w, src_h, sigma, bytes, q}) | (None, reason)."""
    import numpy as np
    from PIL import Image
    long, max_bytes = long or PHOTO_LONG, max_bytes or PHOTO_MAX_BYTES
    try:
        im = Image.open(io.BytesIO(body)); im.load()
    except Exception:
        return None, "undecodable"
    if im.mode in ("P", "LA", "RGBA"):
        im = im.convert("RGBA"); bg = Image.new("RGB", im.size, (0, 0, 0)); bg.paste(im, mask=im.split()[-1]); im = bg
    else:
        im = im.convert("RGB")
    w, h = im.size
    if w < PUB_MIN_W:
        return None, "too_narrow"
    ar = w / max(h, 1)
    if not (PUB_ASPECT[0] <= ar <= PUB_ASPECT[1]):
        return None, "aspect"
    sigma = float(np.asarray(im.convert("L"), dtype=np.float32).std())
    if sigma < PUB_MIN_SIGMA:
        return None, "flat"
    if max(w, h) > long:
        f = long / max(w, h); im = im.resize((max(1, round(w * f)), max(1, round(h * f))), Image.LANCZOS)
    for q in PHOTO_Q:
        buf = io.BytesIO(); im.save(buf, "JPEG", quality=q, optimize=True, progressive=True)
        if buf.tell() <= max_bytes:
            return buf.getvalue(), {"w": im.size[0], "h": im.size[1], "src_w": w, "src_h": h, "sigma": round(sigma, 1), "bytes": buf.tell(), "q": q,
                               "sharp": max(im.size) >= SHARP_LONG}
    return None, "bytes_cap"


class PhotoRun:
    """One run's photo budget, sha dedupe and honest counts (-> meta/run.photos {fetched, attached, rejected{reason:n}})."""

    def __init__(self, budget=PHOTO_BUDGET, declined=None, blocked=None):
        self.budget, self.fetched, self.attached = budget, 0, 0
        self.news_top, self.news_matched, self.news_feed, self.news_og = 0, 0, 0, 0
        import threading
        self.lock = threading.RLock()
        self.robots, self.og = {}, {"pages": 0, "robots_read": 0}
        self.rejected = {}
        self.files, self.by_sha, self.by_url = {}, {}, {}
        self.declined = set(declined if declined is not None else declined_hosts())
        self.blocked = set(blocked or ())

    def reject(self, why):
        with self.lock:
            self.rejected[why] = self.rejected.get(why, 0) + 1
        return None

    def spend(self):
        """Take one fetch from the run budget (thread-safe) -> False when it is spent."""
        with self.lock:
            if self.fetched >= self.budget:
                return False
            self.fetched += 1
            return True

    def legible(self, iid):
        """M4: the page's own legibility measure (images/contract.py) on a stored picture. An illegible one (letterboxed, tiny
        real area, up-scaled thumbnail) would render as no picture, so it is dropped here and the next candidate is tried."""
        with self.lock:
            d = self.files.get(iid)
            if d is None:
                return False
            if "legible" in d:
                return d["legible"]
        ok = bool(_scored([(iid, d)])[iid][1].get("legible"))
        with self.lock:
            d["legible"] = ok
            if not ok:
                self.files.pop(iid, None)
                for k, v in list(self.by_sha.items()):
                    if v == iid:
                        self.by_sha[k] = None
                for k, v in list(self.by_url.items()):
                    if v == iid:
                        self.by_url[k] = None
                self.rejected["illegible"] = self.rejected.get("illegible", 0) + 1
        return ok

    def counts(self):
        return {"fetched": self.fetched, "attached": self.attached, "stored": len(self.files), "budget": self.budget,
                "max_bytes": PHOTO_MAX_BYTES, "long_px": PHOTO_LONG, "ttl_s": PHOTO_TTL_S, "rejected": dict(sorted(self.rejected.items())),
                **({"news_top": self.news_top, "news_matched": self.news_matched, "news_feed": self.news_feed, "news_og": self.news_og,
                    "news_matched_pct": round(100.0 * self.news_matched / self.news_top, 1) if self.news_top else 0.0,
                    "og_pages": self.og["pages"], "og_robots_read": self.og["robots_read"], "blocked_hosts": sorted(self.blocked)} if self.news_top else {}),
                "sharp": sum(1 for f in self.files.values() if f.get("sharp")), "bytes_total": sum(f["bytes"] for f in self.files.values())}

    def _get(self, F, url, mark_blocked=True, strict_ct=False, min_long=0):
        """One budgeted fetch -> (jpg, info) | (None, reason). A 401/403/451 on the URL the feed shipped marks that host blocked.
        strict_ct: the answer must say image/* (og pictures). min_long: smallest accepted source long side (og: 600 px)."""
        if not self.spend():
            return None, "budget"
        r = F.get(url, timeout=(8, 20), cap=8_000_000, tries=1)
        if not r.ok or not r.body:
            if mark_blocked and getattr(r, "status", None) in (401, 403, 451):
                self.blocked.add(_host(url))
            return None, "http"
        ct = (r.headers.get("content-type") or "").lower() if getattr(r, "headers", None) else ""
        if (ct or strict_ct) and "image" not in ct:
            return None, "not_image"
        jpg, info = photo_jpeg(r.body)
        if jpg is not None and min_long and max(info["src_w"], info["src_h"]) < min_long:
            return None, "og_small"
        return jpg, info

    def take(self, F, url, meta, prefix, min_long=0, strict_ct=False, upsize_ok=True):
        """Download one feed picture under the budget -> img id | None (reason counted). The larger rendition of the same picture on
        the publisher's CDN (upsize) is tried first, the shipped URL second. Same bytes twice -> the same id (sha dedupe)."""
        with self.lock:
            if url in self.by_url:
                return self.by_url[url]
        if self.fetched >= self.budget:
            return self.reject("budget")
        jpg, info, got_url = None, "http", url
        big = upsize(url) if upsize_ok else None
        if big:
            jpg, info = self._get(F, big, mark_blocked=False, strict_ct=strict_ct, min_long=min_long); got_url = big
        if jpg is None and info != "budget":
            jpg, info = self._get(F, url, strict_ct=strict_ct, min_long=min_long); got_url = url
        if jpg is None:
            with self.lock:
                self.by_url[url] = None
            return self.reject(info)
        sha = hashlib.sha256(jpg).hexdigest()
        with self.lock:                      # M4: feed pictures are fetched in parallel; the store is serialised
            if sha in self.by_sha:
                self.by_url[url] = self.by_sha[sha]
                return self.by_sha[sha]
            iid = f"img-{prefix}-{sha[:16]}"
            self.files[iid] = dict(meta, jpg=jpg, sha256=sha, width=info["w"], height=info["h"], bytes=info["bytes"], quality=info["q"],
                                   sigma=info["sigma"], src_px=[info["src_w"], info["src_h"]], img_src=got_url, ttl_s=PHOTO_TTL_S, sharp=info["sharp"])
            self.by_sha[sha] = iid; self.by_url[url] = iid
            return iid


def _pub_meta(c, host, via="feed"):
    taken = c.get("t") if isinstance(c.get("t"), str) else (c["t"].strftime("%Y-%m-%dT%H:%M:%SZ") if c.get("t") else None)
    name = c.get("pubname") or host
    cap = (f"Preview picture {name} published in its feed with this article. Open the article for the full story." if via == "feed" else
           f"Share picture {name} supplies for links to this article (its og:image). Open the article for the full story.")
    return {"credit": name, "license": PUB_LICENSE, "src_url": c["url"], "taken": taken, "posted": taken, "via": via,
            "publisher": name, "publisher_host": host, "crop": "event", "crop_box": None, "caption": cap}


def pub_photos(F, news, pr, pubname_of=None, og=True):
    """R2-pub. `news` = ranked news items, each {id, title, t, img_candidates:[{img, url, pub, pubname, host, t}], members:[...]}
    -> {item_id: img_id}. Pass 1 (first, as before): the picture the publisher shipped in its feed. Gates: declined/blocked publisher,
    logo URL, banner (the same picture URL on > BANNER_MAX items), then photo_jpeg (width, aspect, sigma, bytes), budget, sha dedupe.
    Pass 2 (M2, og=True): items still without a picture get the publisher's own og:image / twitter:image from the article page's
    <head> (og_photos). The order of `news` is never changed and nothing is ranked by having a picture."""
    uses = Counter_(c["img"] for it in news for c in it.get("img_candidates") or [])
    out, misses = {}, []
    pr.news_top += len(news)

    def one(it):
        """-> (img id | None, had a feed picture). Every gate rejection is counted; nothing here reorders `news`."""
        cands = it.get("img_candidates") or []
        if not cands:
            pr.reject("no_feed_picture"); return None, False
        for c in cands:
            host = _reg(c.get("host") or _host(c["url"]))
            if host in pr.declined:
                pr.reject("declined"); continue
            if host in pr.blocked or _host(c["img"]) in pr.blocked:
                pr.reject("blocked"); continue
            if PUB_SKIP.search(c["img"].split("?")[0]):
                pr.reject("logo"); continue
            if uses[c["img"]] > BANNER_MAX:
                pr.reject("banner"); continue
            got = pr.take(F, c["img"], _pub_meta(c, host), "pub")
            if got and pr.legible(got):
                return got, True
        return None, True

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(FEED_WORKERS) as ex:      # M4: parallel across publishers (the fetcher keeps its per-host gap)
        res = list(ex.map(one, news))
    for it, (got, had_feed) in zip(news, res):
        if got:
            out[it["id"]] = got; pr.attached += 1; pr.news_matched += 1; pr.news_feed += 1
        elif not had_feed or pr.fetched < pr.budget:
            # M4: no feed picture, or every feed picture failed a gate (banner, logo, quality): the publisher's own og:image
            # from a member page is the next allowed source (same gates; blocked/declined/paywalled publishers never read).
            # A feed picture refused only for budget is not sent on (the og pass would refuse it again).
            misses.append(it)
    if og and misses:
        out.update(og_photos(F, misses, pr))
    return out


# ---------------------------------------------------------------- M2 (Fable, M2-JUDGE §4): the publisher's own og:image
# For a top news item whose feed shipped no usable picture: ONE GET of the linked article page, read only up to </head>
# (8 s, "Watchman/1.0 (+link preview)"), robots.txt honoured (one cached read per host), and the share picture the publisher
# itself supplies for link previews (og:image > og:image:secure_url > twitter:image). Never a declined/blocked publisher, never a
# page behind a paywall or login wall. Same gates as feed pictures (logo, banner, width, aspect, sigma, 1280 px, 180 KB, budget,
# sha dedupe) plus a 600 px long-side floor and a strict image/* content type. Credit = publisher, link = the article.
OG_UA = "Watchman/1.0 (+link preview)"
OG_TIMEOUT = 8
OG_HEAD_CAP = 400_000
OG_MIN_LONG = 600
OG_WORKERS = 8
OG_MAX_PAGES = 3          # M4: at most 3 member pages per item (one GET each, never retried)
FEED_WORKERS = 8
# Publishers the engine already refuses to read (feed blocked to automated reading: NPR "is blocked and skipped", engine.py), plus
# any host this run saw answer 401/403/451 or a bot wall. data/pub_photos_declined.txt is read on top of this.
# Al Jazeera: its robots.txt header states its terms forbid scraping by "any device, tool, or process ... using automated means"
# without written permission (read 2026-11-01), so its pages are never read for a picture.
BLOCKED_PUBLISHERS = {"npr.org", "aljazeera.com"}
# Hard / metered paywalls: their pages sit behind a subscription wall, so their og:image is never used (their FEED pictures are).
PAYWALL_HOSTS = {"nytimes.com", "wsj.com", "ft.com", "washingtonpost.com", "bloomberg.com", "economist.com", "thetimes.co.uk",
                 "telegraph.co.uk", "theatlantic.com", "newyorker.com", "barrons.com", "scmp.com", "indystar.com", "usatoday.com",
                 "latimes.com", "bostonglobe.com", "foreignpolicy.com", "politico.eu", "axios.com"}
PAYWALL_RE = re.compile(r"[\"']isAccessibleForFree[\"']\s*:\s*[\"']?false|<meta[^>]+(?:property|name)=[\"'](?:article:content_tier|cxenseparse:pay(?:wall)?|paywall)[\"'][^>]*content=[\"'](?:locked|metered|premium|true|subscriber)", re.I)
LOGIN_PATH = re.compile(r"/(?:log-?in|sign-?in|subscribe|subscription|paywall|register|auth|account/(?:login|signin))(?:[/?#.]|$)", re.I)
OG_KEYS = ("og:image", "og:image:secure_url", "og:image:url", "twitter:image", "twitter:image:src")
META_TAG = re.compile(r"<meta\b[^>]*>", re.I)
META_ATTR = re.compile(r"([a-zA-Z_:.-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'>]+))")


def head_only(body):
    """bytes/str of an HTML page -> the text up to and including </head> (the rest is never read)."""
    t = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else (body or "")
    i = t.lower().find("</head>")
    return t[: i + 7] if i >= 0 else t


def og_image(head_html, base_url=""):
    """<head> html -> the publisher's share picture URL (og:image > og:image:secure_url > og:image:url > twitter:image) | None.
    Relative URLs are resolved against the page; only http(s) pictures; http upgraded to https on the same host."""
    from urllib.parse import urljoin
    found = {}
    for tag in META_TAG.findall(head_html or ""):
        a = {m.group(1).lower(): _html.unescape(next(g for g in m.groups()[1:] if g is not None)) for m in META_ATTR.finditer(tag[5:])}
        k = (a.get("property") or a.get("name") or a.get("itemprop") or "").strip().lower()
        v = (a.get("content") or "").strip()
        if k in OG_KEYS and v and k not in found:
            found[k] = v
    for k in OG_KEYS:
        if k in found:
            u = urljoin(base_url or "", found[k])
            if u.startswith("http://"):
                u = "https://" + u[7:]
            if u.startswith("https://"):
                return u
    return None


def paywalled(head_html, final_url="", status=200):
    """-> reason | None. A page behind a paywall or login wall: HTTP 401/402, a redirect to a login/subscribe path, or the page's
    own markup saying the article is not free (isAccessibleForFree false, content_tier locked/metered)."""
    from urllib.parse import urlparse
    if status in (401, 402):
        return f"HTTP {status}"
    if final_url and LOGIN_PATH.search(urlparse(final_url).path or ""):
        return "redirected to a login/subscribe page"
    if PAYWALL_RE.search(head_html or ""):
        return "page marks the article as subscriber-only"
    return None


class _Head:
    def __init__(self, status, body, url, ct="", error=None):
        self.status, self.body, self.url, self.ct, self.error = status, body, url, ct, error
        self.ok = 200 <= (status or 0) < 300


def head_get(F, url, timeout=OG_TIMEOUT, cap=OG_HEAD_CAP):
    """ONE GET of an article page, streamed and closed as soon as </head> has arrived (or cap / timeout). -> _Head.
    A fetcher with its own `get_head` is used as is; a replay/test fetcher goes through F.get and is cut at </head>."""
    if hasattr(F, "get_head"):
        return F.get_head(url)
    hdr = {"User-Agent": OG_UA, "Accept": "text/html,application/xhtml+xml"}
    if getattr(F, "replay", None) or not hasattr(F, "_sess"):
        r = F.get(url, headers=hdr, timeout=(5, timeout), cap=cap, tries=1)
        ct = ((getattr(r, "headers", None) or {}).get("content-type") or "")
        return _Head(getattr(r, "status", 0) or 0, head_only(r.body or b""), getattr(r, "url", None) or url, ct)
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    t0 = time.time()
    with F.locks[host]:
        wait = 0.5 - (time.time() - F.last[host])
        if wait > 0:
            time.sleep(wait)
        try:
            r = F._sess().get(url, headers=hdr, timeout=(5, timeout), stream=True, allow_redirects=True)
            buf, low = bytearray(), b""
            for c in r.iter_content(16384):
                buf += c
                if b"</head>" in bytes(buf[-len(c) - 8:]).lower() or len(buf) > cap or time.time() - t0 > timeout:
                    break
            r.close()
            F.last[host] = time.time()
            return _Head(r.status_code, head_only(bytes(buf)), r.url, r.headers.get("content-type", ""))
        except Exception as e:
            F.last[host] = time.time()
            return _Head(0, "", url, "", type(e).__name__)


def robots_allows(F, pr, url):
    """robots.txt for the page's host, read once per run (counted in the budget). 200 -> its rules for "Watchman";
    401/403 -> the whole site is off limits; 404/410 -> no rules; no answer -> not fetched (conservative)."""
    import urllib.robotparser
    from urllib.parse import urlparse
    u = urlparse(url); base = f"{u.scheme}://{u.netloc}"
    import threading
    with pr.lock:                                  # M4: one robots read per host even with parallel workers (byte-stable counts)
        hl = pr.__dict__.setdefault("_robots_locks", {}).setdefault(base, threading.Lock())
    with hl:
        return _robots_locked(F, pr, url, base)


def _robots_locked(F, pr, url, base):
    import urllib.robotparser
    with pr.lock:
        rp = pr.robots.get(base, "?")
    if rp == "?":
        if not pr.spend():
            return None
        r = F.get(base + "/robots.txt", headers={"User-Agent": OG_UA}, timeout=(5, OG_TIMEOUT), cap=500_000, tries=1)
        st = getattr(r, "status", 0) or 0
        if st == 200:
            rp = urllib.robotparser.RobotFileParser(); rp.parse((r.body or b"").decode("utf-8", "replace").splitlines())
        elif st in (401, 403):
            rp = False
        elif 400 <= st < 500:
            rp = True
        else:
            rp = False
        with pr.lock:
            pr.robots[base] = rp; pr.og["robots_read"] += 1
    if rp is True or rp is False:
        return rp
    return rp.can_fetch(OG_UA, url)


def _og_member(it, pr, prio=()):
    """The cluster member whose page may be read: never declined, blocked, paywalled, or a page this run already saw blocked."""
    for a in it.get("members") or []:
        host = _reg(a.get("host") or _host(a.get("url")))
        if not a.get("url", "").startswith("https://"):
            continue
        if host in pr.declined:
            pr.reject("declined"); continue
        if host in pr.blocked or host in BLOCKED_PUBLISHERS or (a.get("receipt") or {}).get("page") == "blocked":
            pr.reject("blocked"); continue
        if host in PAYWALL_HOSTS:
            pr.reject("paywall"); continue
        yield a, host


def og_photos(F, news, pr):
    """M2 pass 2 for items with no feed picture: one page <head> each (parallel), then the og:image through the usual gates."""
    from concurrent.futures import ThreadPoolExecutor
    found = []                       # (item, member, host, og_url)

    def one(it):
        tried = 0
        for a, host in _og_member(it, pr):
            if tried >= OG_MAX_PAGES:
                return
            ok = robots_allows(F, pr, a["url"])
            if ok is None:
                pr.reject("budget"); return
            if not ok:
                pr.reject("robots"); continue                      # robots says no: try the next publisher of the same item
            if not pr.spend():
                pr.reject("budget"); return
            tried += 1
            h = head_get(F, a["url"])
            with pr.lock:
                pr.og["pages"] += 1
            if h.status in (403, 406, 451, 429) or (h.status == 0 and h.error):
                with pr.lock:
                    if h.status in (403, 406, 451):
                        pr.blocked.add(host)
                pr.reject("page_http"); continue                   # M4: this page is never retried; the next publisher's page may be read
            why = paywalled(h.body, h.url, h.status)
            if why:
                pr.reject("paywall"); continue
            if not h.ok:
                pr.reject("page_http"); continue
            u = og_image(h.body, h.url or a["url"])
            if not u:
                pr.reject("no_og"); continue
            with pr.lock:
                found.append((it, a, host, u))
            return
        if not tried:
            pr.reject("no_page")

    with ThreadPoolExecutor(OG_WORKERS) as ex:
        list(ex.map(one, news))
    uses = Counter_(u for _, _, _, u in found)
    order = {it["id"]: i for i, it in enumerate(news)}
    out = {}
    for it, a, host, u in sorted(found, key=lambda f: order[f[0]["id"]]):       # rank order kept
        if PUB_SKIP.search(u.split("?")[0]):
            pr.reject("logo"); continue
        if uses[u] > BANNER_MAX:                                   # the site's default share card, not this article's picture
            pr.reject("banner"); continue
        if _host(u) in pr.blocked:
            pr.reject("blocked"); continue
        got = pr.take(F, u, _pub_meta(a, host, via="og"), "pub", min_long=OG_MIN_LONG, strict_ct=True, upsize_ok=False)
        if got and pr.legible(got):
            out[it["id"]] = got; pr.attached += 1; pr.news_matched += 1; pr.news_og += 1
    return out


def Counter_(xs):
    from collections import Counter
    return Counter(xs)


def cluster_candidates(members, prio=()):
    """Cluster members -> img_candidates (only members whose feed shipped a picture), the house publisher order first, then earliest."""
    rank = lambda a: (prio.index(a.get("pub")) if a.get("pub") in prio else 99, str(a.get("t")))
    out = []
    for a in sorted(members, key=rank):
        if a.get("img"):
            out.append({"img": a["img"], "url": a["url"], "pub": a.get("pub"), "pubname": a.get("pubname"), "host": a.get("host"), "t": a.get("t")})
    return out


# ---------------------------------------------------------------- R2-gov: the issuer's own photograph, only the issuer
WH_FEED = "https://www.whitehouse.gov/presidential-actions/feed/"
NASA_IMAGES = "https://images-api.nasa.gov/search"
GOV_ISSUERS = {"whitehouse.gov": "The White House", "nasa.gov": "NASA", "fema.gov": "FEMA", "weather.gov": "National Weather Service",
               "noaa.gov": "NOAA", "usgs.gov": "U.S. Geological Survey"}
GOV_LICENSE = "Public domain (US government work, 17 U.S.C. 105), published by the issuer"


def record_issuer(item):
    """Registrable domain of the record's own first source (the issuer), or None."""
    s = (item.get("sources") or [{}])[0]
    return _host(s.get("url")) or None


def issuer_feed_pictures(xml_bytes, issuer):
    """{article url: picture url} from an issuer's own RSS, keeping only pictures hosted by that issuer."""
    import feedparser
    out = {}
    for e in feedparser.parse(xml_bytes).entries:
        u = entry_image(e); link = e.get("link")
        if u and link and _host(u) == issuer and _host(link) == issuer:
            out[link.rstrip("/")] = u
    return out


_TM_STOP = {"the", "and", "for", "with", "from", "into", "this", "that", "nasa", "new", "will", "after", "over", "about", "on", "of", "to", "in", "a", "an", "at", "by", "is"}


def title_match(record_title, photo_title):
    """A NASA library hit is the record's picture only when the two titles share >= 2 significant words, at least one a proper name
    (capitalised in the record title). A generic first search hit is never attached (M2: Washington items only with a true match)."""
    words = lambda t: {w.lower() for w in re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", t or "")} - _TM_STOP
    proper = {w.lower() for w in re.findall(r"\b[A-Z][A-Za-z0-9-]{2,}", record_title or "")} - _TM_STOP
    shared = words(record_title) & words(photo_title)
    return len(shared) >= 2 and bool(shared & proper)


def gov_photos(F, records, pr, wh_xml=None):
    """R2-gov. records = [record items]. A record gets a photo only from its own issuer: the White House feed picture for a
    whitehouse.gov record (matched by article link), the NASA Image Library for a nasa.gov record (title search, NASA-hosted file).
    -> {item_id: img_id}. Anything else: nothing (counted as not_issuer when a non-issuer picture would have been the only option)."""
    out = {}
    wh = None
    for it in records:
        iss = record_issuer(it)
        if iss not in GOV_ISSUERS:
            continue
        link = ((it.get("sources") or [{}])[0].get("url") or "").rstrip("/")
        pic = None
        if iss == "whitehouse.gov":
            if wh is None:
                if wh_xml is None:
                    r = F.get(WH_FEED, timeout=(8, 20), tries=1)
                    wh_xml = r.body if r.ok else b""
                wh = issuer_feed_pictures(wh_xml, "whitehouse.gov") if wh_xml else {}
            pic = wh.get(link)
        elif iss == "nasa.gov":
            r = F.get(NASA_IMAGES, params={"q": (it.get("title") or "")[:80], "media_type": "image"}, timeout=(8, 20), tries=1)
            try:
                hit = next((h for h in (r.json()["collection"]["items"] or []) if title_match(it.get("title"), " ".join(
                    str(d.get("title") or "") for d in h.get("data") or []))), None) if r.ok else None
                if hit is None:
                    pr.reject("no_title_match"); continue
                cand = next((l["href"] for l in (hit or {}).get("links") or [] if l.get("rel") == "preview"), None)
                pic = cand if cand and _host(cand) == "nasa.gov" else None
            except Exception:
                pic = None
        if not pic:
            continue
        if _host(pic) != iss:
            pr.reject("not_issuer"); continue
        meta = {"credit": GOV_ISSUERS[iss], "license": GOV_LICENSE, "src_url": link or pic, "taken": (it.get("t") or "")[:10] or None,
                "issuer": iss, "crop": "event", "crop_box": None, "caption": f"Photograph published by {GOV_ISSUERS[iss]} with this record."}
        iid = pr.take(F, pic, meta, "gov")
        if iid:
            out[it["id"]] = iid; pr.attached += 1
    return out


def photo_manifest(pr):
    """Manifest rows for this run's R2 photos (same shape as manifest_rows plus the R2 fields), sorted by id."""
    rows = []
    for iid, d in sorted(pr.files.items()):
        rows.append({"id": iid, "file": f"img/{iid}.jpg", "credit": d["credit"], "license": d["license"], "src_url": d["src_url"], "taken": d.get("taken"),
                     "caption": d["caption"], "crop": "event", "crop_box": None, "width": d["width"], "height": d["height"], "bytes": d["bytes"],
                     "sha256": d["sha256"], "img_src": d["img_src"], "ttl_s": d["ttl_s"], **({"publisher": d["publisher"], "posted": d.get("posted"), "via": d.get("via", "feed")} if "publisher" in d else {}),
                     **({"issuer": d["issuer"]} if "issuer" in d else {}), "sharp": bool(d.get("sharp"))})
    return rows


def run_photos(F, clusters, records, health=(), prio=(), budget=PHOTO_BUDGET, top=60, wh_xml=None, og=True):
    """One run's R2 photos under one budget: news clusters first (ranked as given, top `top`), then records.
    `health` rows whose feed is blocked mark that publisher blocked. -> (files [(id, dict)], {item_id: img_id}, counts)."""
    blocked = {_reg(h.get("host") or "") for h in health or () if h.get("status") == "blocked" and str(h.get("id", "")).startswith("news_")}
    pr = PhotoRun(budget=budget, blocked=(blocked | BLOCKED_PUBLISHERS) - {""})
    rank = lambda a: (prio.index(a.get("pub")) if a.get("pub") in prio else 99, str(a.get("t")))
    news = [{"id": c["id"], "img_candidates": cluster_candidates((c.get("_cluster") or {}).get("members") or [], prio),
             "members": sorted((c.get("_cluster") or {}).get("members") or [], key=rank)} for c in list(clusters)[:top]]
    m = pub_photos(F, news, pr, og=og)
    m.update(gov_photos(F, records or [], pr, wh_xml=wh_xml))
    return sorted(pr.files.items()), m, pr.counts()


def write_photo_dir(out_dir, files, counts):
    """img/<id>.jpg + img/manifest.json (+ counts) for the runner to upload (RUN.md). Returns the manifest rows."""
    import os
    d = os.path.join(out_dir, "img"); os.makedirs(d, exist_ok=True)
    for iid, f in files:
        open(os.path.join(d, iid + ".jpg"), "wb").write(f["jpg"])
    pr = PhotoRun(budget=counts.get("budget", PHOTO_BUDGET), declined=set()); pr.files = dict(files)
    rows = photo_manifest(pr)
    json.dump(rows, open(os.path.join(d, "manifest.json"), "w"), indent=1)
    json.dump(counts, open(os.path.join(d, "photos.json"), "w"), indent=1)
    return rows
