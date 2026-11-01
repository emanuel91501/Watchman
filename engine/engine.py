#!/usr/bin/env python3
"""Watchman data engine (contract v3). Deterministic, no model calls.

Fetches public sources, normalizes them to the docs in spec/CONTRACT.md, and writes
batch_NNN.json files (arrays of {"op":"set","collection","doc_id","data"}) plus report.json.
It never writes to a database, publishes, schedules or touches git. Keys come from
.keys.env (next to this file) or the environment and are never written anywhere.

  python3 engine.py --mode morning|hourly --out DIR [--data DIR] [--replay DIR] [--cache DIR]
"""
import argparse, base64, shutil, concurrent.futures as cf, csv, datetime as dt, email.utils, hashlib, html
import io, json, math, os, re, sys, threading, time
from collections import Counter, defaultdict
from html.parser import HTMLParser
from urllib.parse import urlparse, quote
from zoneinfo import ZoneInfo

import requests
import feedparser
import numpy   # noqa: F401  photos (imgfeeds.photo_jpeg) and legibility need it; import here so a missing install fails at second 0, not after 6 min of fetching
import imgfeeds
import vintages
import clock, display, freshness, gates, timeline   # P1 engine time (CONTRACT-v4 §1-4)
from display import display_value
import civic, places_extra   # r13 hooks (ROUND12-NOTES: Sonnet B civic doc, Sonnet D Marion/Indianapolis numbers)
# P1: route the hook modules' day/clock through clock.py (their own wall-clock reads are listed in app/P1-NOTES.md)
civic.today = lambda: clock.now().strftime("%Y-%m-%d")

HERE = os.path.dirname(os.path.abspath(__file__))
UA = "Watchman/0.1 (personal civic dashboard)"
V = 4                         # CONTRACT-v4
TZ = ZoneInfo("America/Indiana/Indianapolis")
UTC = dt.timezone.utc
DOC_MAX = 220 * 1024          # per doc, serialized
CHUNK_TARGET = 180 * 1024     # chunking target for list docs
BATCH_BYTES = 900 * 1024
BATCH_WRITES = 50
FIXED_COLLS = {"meta", "series", "places", "records", "clocks", "stories", "atlas", "gaps", "glossary", "civic", "time"}   # docs overwritten run after run
DATED_COLLS = {"events", "wire", "news", "brief"}   # doc ids start with a date; new each day, but an earlier date may already exist
LIVE_WINDOW_H = 2
LIVE_DOC_TARGET = 190 * 1024
CONNECT_T, READ_T = 6, 18
BACKOFF = (2, 4, 8)   # seconds before retry 1, 2, 3; timeouts, connection errors and 5xx only. A 403/406/451 block is never retried.
KEYS = {}


# ------------------------------------------------------------------ small helpers
def iso(d):
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_time(s):
    """Tolerant parse to aware UTC datetime or None."""
    if s is None or s == "":
        return None
    try:
        if isinstance(s, (int, float)):
            return dt.datetime.fromtimestamp(s / 1000 if s > 1e11 else s, UTC)
        if isinstance(s, time.struct_time):
            return dt.datetime(*s[:6], tzinfo=UTC)
        s = str(s).strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return dt.datetime.fromisoformat(s).replace(tzinfo=UTC)
        try:
            d = dt.datetime.fromisoformat(s.replace("Z", "+00:00").replace(" UTC", "+00:00"))
        except ValueError:
            d = email.utils.parsedate_to_datetime(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def local_date(d):
    return d.astimezone(TZ).strftime("%Y-%m-%d")

def sha(s, n=12):
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()[:n]

def scrub(s):
    s = str(s)
    for v in KEYS.values():
        if v and len(v) > 5:
            s = s.replace(v, "<key>")
    return s

def strip_tags(s, limit=None):
    s = re.sub(r"<(script|style).*?</\1>", " ", s or "", flags=re.S | re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        s = s[: limit - 1].rstrip() + "…"
    return s

def host_of(url):
    h = urlparse(url or "").netloc.lower()
    return h[4:] if h.startswith("www.") else h

def clip(s, n):
    s = s or ""
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"

GENERIC_PLACES = {"world", "not stated", "europe", "asia", "africa", "latin america", "middle east", "united states", "oceania", ""}

def maps_query(name, admin=None):
    """Full place name plus its state or country (never a truncated name)."""
    name = (name or "").strip()
    if not name:
        return ""
    if admin and admin.lower() not in name.lower():
        return f"{name}, {admin}"
    return name

def gmaps(name=None, lat=None, lon=None, admin=None):
    if name and name.strip().lower() not in GENERIC_PLACES:
        q = maps_query(name, admin)
    elif lat is not None and lon is not None:
        q = f"{lat:.4f},{lon:.4f}"
    else:
        q = maps_query(name, admin)
    return "https://www.google.com/maps/search/?api=1&query=" + quote(q)

def jdump(o):
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

def load_keys():
    p = os.path.join(HERE, ".keys.env")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                KEYS[k.strip()] = v.strip()
    for k in ("DATA_GOV_API_KEY", "FRED_API_KEY", "EIA_API_KEY", "OPENSTATES_API_KEY", "BEA_API_KEY", "COURTLISTENER_TOKEN"):
        if os.environ.get(k) and k not in KEYS:
            KEYS[k] = os.environ[k]


# ------------------------------------------------------------------ fetcher
class Resp:
    def __init__(self, status=0, body=b"", headers=None, error=None, ms=0, cached=False):
        self.status, self.body, self.headers, self.error, self.ms, self.cached = status, body, headers or {}, error, ms, cached
        self.attempts, self.waits = 1, []
    @property
    def ok(self):
        return 200 <= self.status < 300 and self.error is None
    def text(self):
        return self.body.decode("utf-8", "replace")
    def json(self):
        return json.loads(self.body.decode("utf-8", "replace"))


class Fetcher:
    """One request in flight per host; timeouts; 429/Retry-After respected; replay/cache for tests."""
    def __init__(self, replay=None, cache=None):
        self.replay, self.cache = replay, cache
        self.misses = set()      # replay misses (offline tests): what this run could not read, so tests can say why
        self.locks = defaultdict(threading.Lock)
        self.last = defaultdict(float)
        self.glock = threading.Lock()
        self.tl = threading.local()
        self.sleep = time.sleep
        self.session_factory = None
        if cache:
            os.makedirs(cache, exist_ok=True)

    def _sess(self):
        if not hasattr(self.tl, "s"):
            self.tl.s = self.session_factory() if self.session_factory else requests.Session()
            self.tl.s.headers["User-Agent"] = UA
        return self.tl.s

    @staticmethod
    def _resolve(v):
        if isinstance(v, str) and v.startswith("@"):
            return KEYS.get(v[1:], "")
        if isinstance(v, str) and v.startswith("Token @"):
            return "Token " + KEYS.get(v[7:], "")
        return v

    def get(self, url, params=None, headers=None, method="GET", json_body=None, timeout=(CONNECT_T, READ_T), cap=8_000_000, tries=4, gap=0.25):
        ck = sha(method + url + jdump(params) + jdump(headers) + jdump(json_body), 20)
        if self.replay:
            p = os.path.join(self.replay, ck + ".bin")
            if os.path.exists(p):
                meta = json.load(open(p + ".json"))
                return Resp(meta["status"], open(p, "rb").read(), meta.get("headers"), meta.get("error"), 0, True)
            sid_ = next((str(v) for k, v in (params.items() if isinstance(params, dict) else (params or [])) if k == "series_id"), None)
            with self.glock:
                self.misses.add(urlparse(url).netloc + urlparse(url).path + (f" series_id={sid_}" if sid_ else ""))   # no query string: never a key
            return Resp(0, b"", None, "not in replay cache", 0, True)
        host = urlparse(url).netloc
        rp = None
        if params is not None:
            rp = [(k, self._resolve(v)) for k, v in (params.items() if isinstance(params, dict) else params)]
        rh = {k: self._resolve(v) for k, v in (headers or {}).items()}
        resp = Resp(0, b"", None, "no attempt")
        waits = []
        with self.locks[host]:
            for attempt in range(max(1, tries)):
                wait = gap - (time.time() - self.last[host])
                if wait > 0:
                    self.sleep(wait)
                t0 = time.time()
                retry = False
                try:
                    r = self._sess().request(method, url, params=rp, headers=rh, json=json_body, timeout=timeout, stream=True)
                    chunks, n = [], 0
                    for c in r.iter_content(65536):
                        chunks.append(c); n += len(c)
                        if n > cap:
                            break
                    body = b"".join(chunks)
                    r.close()
                    resp = Resp(r.status_code, body, {k.lower(): v for k, v in r.headers.items() if k.lower() in ("content-type", "retry-after", "last-modified", "date")}, None, int((time.time() - t0) * 1000))
                    self.last[host] = time.time()
                    if r.status_code == 429:
                        ra = r.headers.get("Retry-After", "")
                        ra = int(ra) if ra.isdigit() else 10
                        if attempt + 1 < tries and ra <= 20:
                            waits.append(ra); self.sleep(ra); continue
                        resp.error = "429 rate limited"
                    elif r.status_code >= 500:
                        retry = True      # server error: backoff and retry. 4xx (403, 406, 451 blocks included) is never retried.
                except Exception as e:
                    self.last[host] = time.time()
                    resp = Resp(0, b"", None, scrub(type(e).__name__ + ": " + str(e))[:200], int((time.time() - t0) * 1000))
                    retry = True          # timeout / connection failure
                if retry and attempt + 1 < tries and attempt < len(BACKOFF):
                    waits.append(BACKOFF[attempt]); self.sleep(BACKOFF[attempt]); continue
                break
        resp.attempts = len(waits) + 1 if waits else 1
        resp.waits = waits
        if self.cache:
            body = resp.body
            for v in KEYS.values():
                if v and len(v) > 5:
                    body = body.replace(v.encode(), b"<key>")
            with open(os.path.join(self.cache, ck + ".bin"), "wb") as f:
                f.write(body)
            json.dump({"status": resp.status, "headers": resp.headers, "error": resp.error}, open(os.path.join(self.cache, ck + ".bin.json"), "w"))
        return resp


# ------------------------------------------------------------------ geography / text classification
US_STATES = {"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia","PR":"Puerto Rico","GU":"Guam"}
STATE_NAME_RE = re.compile(r"\b(" + "|".join(sorted([n for n in US_STATES.values() if n not in ("Washington", "Georgia", "District of Columbia", "Guam")], key=len, reverse=True)) + r")\b")
COUNTRIES = ["Afghanistan","Algeria","Argentina","Armenia","Australia","Austria","Azerbaijan","Bangladesh","Belarus","Belgium","Bolivia","Brazil","Bulgaria","Burkina Faso","Cambodia","Cameroon","Canada","Chad","Chile","China","Colombia","Cuba","Cyprus","Czech Republic","Democratic Republic of the Congo","DR Congo","Denmark","Ecuador","Egypt","Ethiopia","Finland","France","Gaza","Germany","Ghana","Greece","Guatemala","Haiti","Hungary","Iceland","India","Indonesia","Iran","Iraq","Ireland","Israel","Italy","Japan","Jordan","Kazakhstan","Kenya","Kosovo","Kuwait","Lebanon","Libya","Malaysia","Mali","Mexico","Moldova","Morocco","Mozambique","Myanmar","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","North Korea","Norway","Pakistan","Palestine","Panama","Peru","Philippines","Poland","Portugal","Qatar","Romania","Russia","Saudi Arabia","Serbia","Singapore","Slovakia","Somalia","South Africa","South Korea","South Sudan","Spain","Sri Lanka","Sudan","Sweden","Switzerland","Syria","Taiwan","Thailand","Turkey","Uganda","Ukraine","United Arab Emirates","United Kingdom","Britain","Uruguay","Venezuela","Vietnam","West Bank","Yemen","Zimbabwe","Tonga","Fiji","Vanuatu","Papua New Guinea","Solomon Islands","Chile","Costa Rica","Honduras","El Salvador","Dominican Republic","Jamaica","Bahamas","Mongolia","Laos","Kyrgyzstan","Tajikistan","Uzbekistan","Turkmenistan","Georgia (country)","Tanzania","Zambia","Angola","Rwanda","Burundi","Madagascar","Senegal","Tunisia","Eritrea","Djibouti"]
COUNTRY_RE = re.compile(r"\b(" + "|".join(sorted(set(re.escape(c) for c in COUNTRIES), key=len, reverse=True)) + r")\b")
TRACKED = {"China", "Russia", "Iran", "Saudi Arabia", "Ukraine", "Israel", "Canada", "Mexico"}
LOCAL_RE = re.compile(r"\b(Indianapolis|Marion County,? (Indiana|IN)|Indy)\b")
IN_RE = re.compile(r"\bIndiana\b|\bHoosier\b")

def find_state(text):
    m = STATE_NAME_RE.search(text or "")
    return m.group(1) if m else None

def find_country(text):
    m = COUNTRY_RE.search(text or "")
    return m.group(1) if m else None

RIGHTS_RE = re.compile(r"\b(court|courts|ruling|rules|ruled|supreme|constitution\w*|rights|vot(?:e|es|er|ers|ing)|elect(?:ion|ions|ed)|ballot\w*|arrest\w*|immigra\w*|deport\w*|asylum|privacy|speech|abortion|citizenship|detention|detain\w*|military|war|missile\w*|strikes?|struck|attack\w*|killed|kill|dead|death|deaths|died|tornado\w*|hurricane\w*|typhoon|cyclone|tsunami|earthquake\w*|evacuat\w*|outbreak\w*|emergency|flood\w*|wildfire\w*|ebola|shooting|hostage\w*|ceasefire|coup|protest\w*|nuclear|invasion|invade\w*|terror\w*|bomb\w*|casualt\w*|disaster|warning|warnings|storm|storms|injur\w*)\b", re.I)
DOLLARS_RE = re.compile(r"\b(tax|taxes|taxpayer\w*|tariff\w*|price|prices|rate|rates|wage|wages|gas|gasoline|fuel|diesel|oil|crude|inflation|mortgage\w*|loan\w*|insurance|budget|deficit|debt|stocks?|markets?|trade|sanction\w*|fees?|banks?|banking|jobs?|unemployment|economy|economic|payments?|billion|trillion|costs?|spending|benefits?|grants?|funding|appropriations?|shutdown|recession|currency|dollar|crypto\w*|bitcoin|earnings|revenue|layoffs?|hiring|housing|rent|medicare|medicaid|social security)\b", re.I)
IRREV_RE = re.compile(r"\b(final rule|signed|signs|signing|enact\w*|executive order|proclamation|sentenc\w*|convict\w*|struck down|upheld|ruled|rules|declared|declaration|deadline|expire\w*|clos(?:es|ed|ing)|killed|dead|died|deaths?|destroy\w*|sanction\w*|designat\w*|bans?|banned|terminat\w*|withdraw\w*|resign\w*|assassinat\w*|invad\w*|invasion|collapse\w*|evacuat\w*|confirmed by the senate|became public law|veto\w*|tsunami)\b", re.I)
REACH = [
    ("fuel", re.compile(r"\b(gas|gasoline|diesel|fuel|oil|crude|opec|pipeline|refiner\w*)\b", re.I)),
    ("tariff", re.compile(r"\b(tariff\w*|trade war|import dut\w*|customs)\b", re.I)),
    ("tax", re.compile(r"\b(irs|tax|taxes|taxpayer\w*|deduction\w*|estimated tax)\b", re.I)),
    ("gig", re.compile(r"\b(gig|independent contractor\w*|rideshare|ride-hailing|uber|lyft|doordash|instacart|grubhub|amazon flex|delivery driver\w*|app-based|misclassif\w*)\b", re.I)),
    ("self-employed", re.compile(r"\b(self-employ\w*|small business\w*|sole proprietor\w*|freelanc\w*|schedule c)\b", re.I)),
    ("rates", re.compile(r"\b(federal reserve|fomc|interest rate\w*|mortgage rate\w*|rate cut|rate hike|treasury yield\w*)\b", re.I)),
    ("vehicle", re.compile(r"\b(recall\w*|auto(?:mobile)? (?:sales|prices)|car prices|tires?|electric vehicle\w*|ev tax|nhtsa|truck\w*|vehicle\w*)\b", re.I)),
    ("insurance", re.compile(r"\b(health insurance|marketplace|open enrollment|affordable care|obamacare|auto insurance|insurance premium\w*)\b", re.I)),
    ("elections", re.compile(r"\b(election day|early voting|absentee|polling place|voter registration|indiana election)\b", re.I)),
    ("housing", re.compile(r"\b(rent|rents|housing|eviction\w*|landlord\w*|mortgage\w*)\b", re.I)),
    ("jobs", re.compile(r"\b(jobs report|unemployment|jobless|layoffs?|job (?:cuts|losses|openings|market|growth)|labor market|payrolls|hiring freeze)\b", re.I)),
]
DOMAIN_RULES = [  # atlas domain number by keyword (first match wins); heuristic, prov=computed
    (17, r"\b(war|military|missile|army|troops|nato|defen[cs]e|ceasefire|airstrike|drone strike|invasion|hostage|terror\w*)\b"),
    (19, r"\b(earthquake|hurricane|tornado|typhoon|cyclone|flood\w*|wildfire|storm|tsunami|volcano|weather|blizzard|heat ?wave|drought|landslide|disaster)\b"),
    (20, r"\b(outbreak|virus|disease|health|world health organization|vaccine|measles|ebola|cholera|hospital|cdc|fda|cancer|covid|flu)\b"),
    (18, r"\b(oil|gas|gasoline|diesel|fuel|opec|energy|electric(?:ity)?|power grid|pipeline|solar|nuclear plant)\b"),
    (4, r"\b(court|judge|ruling|supreme court|lawsuit|justice department|indict\w*|sentenc\w*|trial|appeals)\b"),
    (3, r"\b(election|vote|voter|ballot|campaign|primary|poll)\b"),
    (8, r"\b(tax|irs|loan|credit|mortgage|bank|savings|inflation|insurance)\b"),
    (9, r"\b(jobs?|unemployment|layoffs?|labor|union|strike|wage|gig|worker\w*|hiring)\b"),
    (10, r"\b(tariff|trade|company|companies|merger|business|industry|manufactur\w*|supply chain)\b"),
    (7, r"\b(economy|economic|recession|gdp|stocks?|markets?|fed |federal reserve|interest rate|treasury|dollar|inflation)\b"),
    (21, r"\b(ai |artificial intelligence|nasa|space|satellite|cyber\w*|technology|tech |software|quantum|rocket|launch)\b"),
    (24, r"\b(housing|rent|home prices?|real estate|eviction)\b"),
    (25, r"\b(traffic|highway|road|airline|flight|airport|rail|transit|vehicle|car |truck|nhtsa|faa)\b"),
    (22, r"\b(school|students?|university|college|education)\b"),
    (5, r"\b(budget|appropriations|deficit|spending bill|debt ceiling|shutdown)\b"),
    (1, r"\b(congress|senate|house of representatives|white house|president|executive order|federal)\b"),
    (2, r"\b(indiana|indianapolis|marion county|hoosier)\b"),
    (16, r"\b(united nations|un |nato|eu |european union|imf|world bank|g20|g7|brics)\b"),
]
DOMAIN_RULES = [(d, re.compile(p, re.I)) for d, p in DOMAIN_RULES]

def domain_for(text, default=23, fallback=None):
    for src_text in (text, fallback):
        for d, rx in DOMAIN_RULES:
            if src_text and rx.search(src_text):
                return f"d{d:02d}"
    return f"d{default:02d}"

def reach_tags(text):
    return [n for n, rx in REACH if rx.search(text or "")]


# ------------------------------------------------------------------ item construction & scoring
def mk_place(name, lat=None, lon=None, geom=None, admin=None, maps_name=None, geom_reason=None):
    p = {"name": name or "Not stated", "lat": lat, "lon": lon}
    if geom:
        p["geom"] = geom
    elif geom_reason:
        p["geom"] = None
        p["geom_reason"] = geom_reason
    p["admin"] = admin
    p["gmaps"] = gmaps(maps_name or name, lat, lon, admin)
    return p

def src(n, name, url, fetched, quote_=None, host=None):
    d = {"n": n, "name": name, "host": host or host_of(url), "url": url, "fetched": fetched}
    if quote_:
        d["quote"] = clip(strip_tags(quote_), 200)
    return d

def mk_item(idkey, kind, title, plain, t, place, sev, layer, truth, sources, domain, prov="fetched", reach=None, cutoff=None, first_seen=None, updated=None, next_=None, extra=None):
    it = {"id": kind + "-" + sha(idkey), "kind": kind, "title": strip_tags(title), "plain": plain,
          "place": place, "t": iso(t) if isinstance(t, dt.datetime) else t,
          "first_seen": iso(first_seen) if isinstance(first_seen, dt.datetime) else first_seen,
          "updated": iso(updated) if isinstance(updated, dt.datetime) else updated,
          "sev": round(max(0.0, min(1.0, sev)), 2), "layer": layer, "truth": truth, "sources": sources, "prov": prov,
          "score": None, "reach": reach or [], "domain": domain, "atlas": [], "story": None, "img": None, "next": next_}
    it["_cut"] = cutoff
    if extra:
        it.update(extra)
    return it

LAYER_PROX = {"local": 1.0, "indiana": 0.6, "us": 0.4, "world": 0.2}

WEATHER_KINDS = {"alert", "storm", "storm_report", "lsr", "space_weather", "flood", "tsunami"}
GAS_RE = re.compile(r"\b(gas prices?|gasoline|diesel|fuel prices?|pump prices?|price at the pump|gas tax)\b", re.I)
FAMILY_FED_RE = re.compile(r"\b(child tax credit|snap|food stamps|medicaid|medicare|social security|student loans?|health insurance|affordable care|tax credit|irs|wic|housing assistance|unemployment benefits|minimum wage|overtime|paid leave|tax refunds?|tax brackets?)\b", re.I)
FED_RE = re.compile(r"\b(federal reserve|fomc|powell|rate cut|rate hike|interest rates?)\b", re.I)
FED_ACTION_KINDS = {"presidential", "rule", "declaration", "sanction"}

HAZARD_KINDS = {"alert", "storm", "storm_report", "lsr", "quake", "disaster", "fire", "volcano", "flood", "tsunami"}
ADJACENT_STATES = {"Illinois", "Ohio", "Kentucky", "Michigan"}
INDY = (39.7684, -86.1581)
US_MENTION_RE = re.compile(r"\b(U\.S\.|US|United States|American|Americans)\b")
CEREMONIAL_RE = re.compile(r"\b(National [A-Z][\w ,'-]* (Month|Week|Day)\b|Day of|Anniversary|Observance|Columbus Day|Thanksgiving)", re.I)
NATIONAL_KINDS = {"presidential", "rule"}
KIND_LABEL = {"alert": "NWS alert", "storm": "Storm", "storm_report": "Storm report", "lsr": "NWS local storm report", "quake": "Earthquake", "disaster": "Disaster alert", "fire": "Fire", "volcano": "Volcano", "flood": "Flood", "tsunami": "Tsunami"}

def km_from_indy(lat, lon):
    p1, p2 = math.radians(INDY[0]), math.radians(lat)
    dphi, dl = p2 - p1, math.radians(lon - INDY[1])
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))

def touches_you(it):
    """Strict: yes only when the item plausibly affects someone living in Indianapolis, Indiana, USA. `how` is a short fact read from the item.
    Marion County / Indiana items; physical hazards in Indiana, adjacent states (IL OH KY MI) or within 300 km; national US federal actions (presidential documents, proposed rules, US sanctions);
    US prices, rates, benefits and jobs stories with a US-wide scope. Foreign-located items with no stated US effect are never yes."""
    no = lambda why="Not in Indiana or a nationwide US effect": {"yes": False, "how": why}
    place = it.get("place") or {}
    pname = place.get("name") or ""
    text = (it.get("title") or "") + " " + (it.get("plain") or "")
    layer, admin, kind = it.get("layer"), place.get("admin"), it.get("kind")
    in_indiana = layer in ("local", "indiana") or admin == "Indiana"
    lat, lon = place.get("lat"), place.get("lon")
    dist = km_from_indy(lat, lon) if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) else None
    if kind in HAZARD_KINDS:
        label = KIND_LABEL.get(kind, kind)
        if in_indiana:
            if re.search(r"\bMarion\b", pname) or LOCAL_RE.search(pname):
                return {"yes": True, "how": f"{label} covering Marion County"}
            return {"yes": True, "how": f"{label} located in Indiana"}
        if admin in ADJACENT_STATES:
            return {"yes": True, "how": f"{label} in {admin}, adjacent to Indiana" + (f", {round(dist)} km from Indianapolis" if dist is not None else "")}
        if dist is not None and dist <= 300:
            return {"yes": True, "how": f"{label} {round(dist)} km from Indianapolis"}
        return no("Hazard outside Indiana and adjacent states")
    if layer == "local" or re.search(r"\bMarion County\b", pname) or LOCAL_RE.search(pname):
        return {"yes": True, "how": "Located in Marion County, Indiana"}
    if in_indiana:
        return {"yes": True, "how": "Explicitly about Indiana" if kind in ("news", "current_event") else "Located in Indiana"}
    us_wide = layer == "us" and admin == "United States"
    foreign = layer == "world" or (admin is not None and admin != "United States" and admin not in US_STATES.values())
    if foreign:
        if GAS_RE.search(text) and US_MENTION_RE.search(text):
            return {"yes": True, "how": "States an effect on U.S. fuel prices"}
        return no("Located outside the US, no stated US effect")
    if not us_wide:
        return no("Located in another US state")
    if kind in NATIONAL_KINDS:
        if CEREMONIAL_RE.search(it.get("title") or ""):
            return no("Ceremonial or commemorative document")
        others = {c for c in COUNTRY_RE.findall(it.get("title") or "")}
        if others and not (GAS_RE.search(text) or FAMILY_FED_RE.search(text) or DOLLARS_RE.search(text)):
            return no("Concerns " + ", ".join(sorted(others)[:2]) + ", no stated US cost or rights effect")
        return {"yes": True, "how": ("Presidential document, national in scope" if kind == "presidential" else "Proposed national rule, would apply in Indiana")}
    if kind == "sanction":
        return {"yes": True, "how": "US sanctions action, binds US persons nationally"}
    if kind in ("public_inspection", "nomination", "bill", "vote", "ruling", "declaration"):
        return no("Not yet in force or not applicable in Indiana" if kind != "declaration" else "Declaration for another state")
    tags = set(it.get("reach") or [])
    if GAS_RE.search(text):
        return {"yes": True, "how": "US-wide story on gas or fuel prices"}
    if FAMILY_FED_RE.search(text):
        return {"yes": True, "how": "Nationwide US benefit or tax change: " + FAMILY_FED_RE.search(text).group(1).lower()}
    if FED_RE.search(text):
        return {"yes": True, "how": "US interest rates and Federal Reserve policy"}
    if "jobs" in tags:
        return {"yes": True, "how": "US-wide jobs and labor market story"}
    if "gig" in tags or "self-employed" in tags:
        return {"yes": True, "how": "US-wide story on gig or self-employed work"}
    return no()

def finalize(it, now):
    """Significance per Vision 6: stake x proximity x irreversibility x urgency; breaking = stake x prox x irrev x recency."""
    text = it["title"] + " " + it["plain"]
    kind = it["kind"]
    hazard = kind in ("quake", "alert", "disaster", "storm", "storm_report", "space_weather", "fire", "volcano", "flood", "lsr", "tsunami")
    if hazard:
        stake = 3 if it["sev"] >= 0.75 else (2 if it["sev"] >= 0.45 else 1)
    else:
        stake = 3 if RIGHTS_RE.search(text) else (2 if DOLLARS_RE.search(text) else 1)
    admin = (it["place"] or {}).get("admin")
    prox = LAYER_PROX.get(it["layer"], 0.2)
    if (it["reach"] or admin in TRACKED) and prox < 0.6:
        prox = 0.6
    irrev = 2 if (IRREV_RE.search(text) or it.get("_irrev")) else 1
    urg = 0.1
    cut = parse_time(it.get("_cut"))
    if cut:
        days = (cut - now).total_seconds() / 86400
        if days > -0.01:
            urg = 1.0 / max(1.0, days)
    total = round(stake * prox * irrev * urg, 3)
    t = parse_time(it["t"]) or now
    age_h = max(0.0, (now - t).total_seconds() / 3600)
    rec = 1.0 if age_h <= 3 else (0.5 if age_h <= 24 else (0.2 if age_h <= 72 else 0.05))
    brk = round(stake * prox * irrev * rec, 3)
    expired = bool(cut and cut < now and kind == "alert")
    it["score"] = {"stake": stake, "prox": prox, "irrev": irrev, "urg": round(urg, 3), "total": total, "breaking": brk,
                   "is_breaking": bool(brk >= 1.8 and it["truth"] in ("primary", "confirmed") and not expired)}
    it["touches_you"] = touches_you(it)
    return it

def public(it):
    return {k: v for k, v in it.items() if not k.startswith("_")}


# ------------------------------------------------------------------ run context and source registry
CUR = threading.local()

class Ctx:
    def __init__(self, mode, F, now, data_dir):
        self.mode, self.F, self.now, self.data = mode, F, now, data_dir
        self.events, self.wire, self.articles = [], [], []
        self.records = defaultdict(list)
        self.series = {}
        self.pnums = defaultdict(list)
        self.clocks = []
        self.pextra = defaultdict(dict)
        self.health = []
        self.lock = threading.Lock()
        self.fetched_hdr = iso(now)
        self.zone_cache = {}
        self.stats = {}

    def event(self, it):
        it["_sid"] = getattr(CUR, "sid", None)
        with self.lock:
            self.events.append(it)

    def wire_add(self, it):
        it["_sid"] = getattr(CUR, "sid", None)
        with self.lock:
            self.wire.append(it)

    def record(self, kind, it):
        it["_sid"] = getattr(CUR, "sid", None)
        with self.lock:
            self.records[kind].append(it)

    def pnum(self, place, label, value, unit, date, source, url=None, prov="fetched"):
        with self.lock:
            self.pnums[place].append({"label": label, "value": value, "display": display_value(value, unit), "url": url, "unit": unit, "date": date, "source": source + (" " + url if url else ""), "prov": prov, "fetched_at": iso(clock.now())})

    def now_iso(self):
        return iso(clock.now())


class S:
    """Per-source handle: records http status, errors, item counts."""
    def __init__(self, ctx, spec):
        self.ctx, self.spec, self.items, self.http, self.error = ctx, spec, 0, None, None
        self.blocked = False
        self.soft = False

    def get(self, url, **kw):
        r = self.ctx.F.get(url, **kw)
        self.http = r.status or self.http
        if r.status == 403 or r.status == 401:
            self.blocked = True
            self.error = f"HTTP {r.status} (blocked; not bypassed)"
        elif not r.ok:
            self.error = scrub(f"HTTP {r.status} {r.error or ''}".strip())[:200]
        return r

    def n(self, k=1):
        self.items += k


SOURCES = []

def source(id, name, host, hourly=False, keys=()):
    def deco(fn):
        SOURCES.append({"id": id, "name": name, "host": host, "hourly": hourly, "keys": keys, "fn": fn})
        return fn
    return deco

def run_source(ctx, spec):
    s = S(ctx, spec)
    CUR.sid = spec["id"]
    t0 = time.time()
    missing = [k for k in spec["keys"] if not KEYS.get(k)]
    if missing:
        status = "needs_key"; s.error = "missing key: " + ",".join(missing)
    else:
        try:
            spec["fn"](s)
        except Exception as e:
            s.error = scrub(f"parser error {type(e).__name__}: {e}")[:200]
        if s.items > 0:
            status = "data"
        elif s.blocked:
            status = "blocked"
        elif s.error and not s.soft:
            status = "error"
        else:
            status = "empty"
    return {"id": spec["id"], "name": spec["name"], "host": spec["host"], "status": status, "items": s.items,
            "http": s.http, "last_good": ctx.now_iso() if status == "data" else None,
            "error": (s.error if status != "data" else (s.error if s.error else None)), "ms": (None if clock.frozen() else int((time.time() - t0) * 1000))}


def sev_alert(level):
    return {"extreme": 1.0, "severe": 0.7, "moderate": 0.5, "minor": 0.25}.get((level or "").lower(), 0.2)

def simplify_ring(coords, maxpts=14):
    pts = [(round(c[0], 2), round(c[1], 2)) for c in coords]
    out = []
    for p in pts:
        if not out or out[-1] != p:
            out.append(p)
    if len(out) > maxpts:
        step = len(out) / maxpts
        out = [out[int(i * step)] for i in range(maxpts)]
    return [[p[0], p[1]] for p in out]

def geom_info(g):
    """-> (lat, lon, simplified polygon) from a GeoJSON geometry; polygon only for Polygon/MultiPolygon."""
    if not g:
        return None, None, None
    try:
        if g["type"] == "Point":
            return g["coordinates"][1], g["coordinates"][0], None
        ring = g["coordinates"][0] if g["type"] == "Polygon" else g["coordinates"][0][0]
        lon = sum(c[0] for c in ring) / len(ring); lat = sum(c[1] for c in ring) / len(ring)
        return round(lat, 3), round(lon, 3), {"type": "Polygon", "coordinates": [simplify_ring(ring)]}
    except Exception:
        return None, None, None


# ------------------------------------------------------------------ hazard / event sources
def geo_from_text(text):
    m = re.search(r",\s*([A-Z]{2})$", text or "")
    if m and m.group(1) in US_STATES:
        st = US_STATES[m.group(1)]
        return ("indiana" if st == "Indiana" else "us"), st
    st = find_state(text)
    if st:
        return ("indiana" if st == "Indiana" else "us"), st
    if re.search(r"\b(Alaska|Hawaii|Puerto Rico|Guam)\b", text or ""):
        return "us", find_country(text) or "United States"
    c = find_country(text)
    return "world", c

def quake_title(mag, place):
    """'M5.1 earthquake in the Balleny Islands region' / 'M4.5 earthquake 147 miles ESE of Zyryanka, Russia': no dash, US units."""
    w = (place or "").strip()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*km\s+([NSEW]{1,3})\s+of\s+(.+)$", w, re.I)
    if m:
        w = f"{round(float(m.group(1)) * 0.621371):,} miles {m.group(2)} of {m.group(3)}"
    elif re.search(r"\b(region|islands?|ridge|rise|sea|coast)\b", w, re.I):
        w = "in the " + re.sub(r"^the\s+", "", w, flags=re.I)
    elif w:
        w = "near " + w
    return f"M{mag:.1f} earthquake" + (" " + w if w else "")

@source("usgs", "USGS earthquakes (past day)", "earthquake.usgs.gov", hourly=True)
def src_usgs(s):
    r = s.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for f in r.json().get("features", []):
        p = f["properties"]; mag = p.get("mag")
        if mag is None or not f.get("geometry"):
            continue
        lon, lat = f["geometry"]["coordinates"][:2]; depth = f["geometry"]["coordinates"][2]
        region = 36.9 <= lat <= 41.9 and -88.2 <= lon <= -84.7
        if mag < 4.5 and not (region and mag >= 1.5):
            continue
        place = p.get("place") or "Unnamed location"
        layer, admin = geo_from_text(place)
        if region and layer == "world":
            layer, admin = "us", admin or "United States"
        t = parse_time(p.get("time"))
        plain = f"USGS recorded a magnitude {mag:.1f} ({p.get('magType') or 'type n/a'}) earthquake {place} at {depth:.0f} km depth, {iso(t)}. Review status: {p.get('status')}." + (" Tsunami flag set." if p.get("tsunami") else "") + (f" {p['felt']} felt reports." if p.get("felt") else "")
        it = mk_item("usgs" + f["id"], "quake", quake_title(mag, place), plain, t, mk_place(place, round(lat, 3), round(lon, 3), admin=admin),
                     (mag - 3.5) / 4.5, layer, "primary", [src(1, "USGS Earthquake Hazards Program", p.get("url") or "https://earthquake.usgs.gov", ft, p.get("title"))],
                     "d19", updated=parse_time(p.get("updated")), extra={"mag": mag})
        s.ctx.event(it); s.n()

# --- NWS geometry: alerts without a polygon get their extent from affectedZones
GEOM_MAX_POINTS = 200
ZONE_CAP = 320            # zone fetches per run
ZONE_DEADLINE_S = 110     # seconds the zone phase may take

def _dp(pts, eps):
    """Douglas-Peucker on an open list of [x, y]; iterative."""
    if len(pts) < 3:
        return list(pts)
    keep = [False] * len(pts); keep[0] = keep[-1] = True
    stack = [(0, len(pts) - 1)]
    while stack:
        i, j = stack.pop()
        (x1, y1), (x2, y2) = pts[i], pts[j]
        dx, dy = x2 - x1, y2 - y1; d2 = dx * dx + dy * dy
        best, bk = -1.0, None
        for k in range(i + 1, j):
            x, y = pts[k]
            if d2 == 0:
                d = math.hypot(x - x1, y - y1)
            else:
                d = abs(dy * (x - x1) - dx * (y - y1)) / math.sqrt(d2)
            if d > best:
                best, bk = d, k
        if best > eps and bk is not None:
            keep[bk] = True; stack += [(i, bk), (bk, j)]
    return [p for p, k in zip(pts, keep) if k]

def simplify_rings(rings, maxpts=GEOM_MAX_POINTS):
    """rings: list of closed [lon,lat] rings -> (list of simplified closed rings, total points <= maxpts)."""
    rs = []
    for r in rings:
        r = [[round(c[0], 3), round(c[1], 3)] for c in r]
        if len(r) > 1 and r[0] == r[-1]:
            r = r[:-1]
        dd = []
        for c in r:
            if not dd or dd[-1] != c:
                dd.append(c)
        if len(dd) >= 3:
            rs.append(dd)
    def bbox_area(r):
        xs = [c[0] for c in r]; ys = [c[1] for c in r]
        return (max(xs) - min(xs)) * (max(ys) - min(ys))
    rs.sort(key=bbox_area, reverse=True)
    rs = rs[: maxpts // 4]                      # every closed ring needs >= 4 points
    eps = 0.0005
    while True:
        out = [_dp(r + [r[0]], eps) for r in rs]
        out = [o if len(o) >= 4 else (o + [o[0]] * (4 - len(o))) for o in out]
        if sum(len(o) for o in out) <= maxpts or eps > 20:
            break
        eps *= 1.6
    while sum(len(o) for o in out) > maxpts and len(out) > 1:
        out.pop()
    return out

def rings_of(g):
    """Exterior rings of a GeoJSON Polygon / MultiPolygon."""
    if not g:
        return []
    if g["type"] == "Polygon":
        return [g["coordinates"][0]]
    if g["type"] == "MultiPolygon":
        return [poly[0] for poly in g["coordinates"]]
    return []

def geom_from_rings(rings):
    """-> (lat, lon, geom) with <= GEOM_MAX_POINTS points."""
    out = simplify_rings(rings)
    if not out:
        return None, None, None
    pts = [c for r in out for c in r]
    lon = sum(c[0] for c in pts) / len(pts); lat = sum(c[1] for c in pts) / len(pts)
    geom = {"type": "Polygon", "coordinates": [out[0]]} if len(out) == 1 else {"type": "MultiPolygon", "coordinates": [[r] for r in out]}
    return round(lat, 3), round(lon, 3), geom

def zone_rings(s, url, deadline):
    """Fetch one NWS zone once per run (cached in memory). -> (rings, reason)."""
    ctx = s.ctx
    with ctx.lock:
        if url in ctx.zone_cache:
            return ctx.zone_cache[url]
        if len(ctx.zone_cache) >= ZONE_CAP or time.time() > deadline:
            res = ([], "zone fetch skipped (per-run limit of %d zones or %d s reached)" % (ZONE_CAP, ZONE_DEADLINE_S))
            return res
    r = s.ctx.F.get(url, headers={"Accept": "application/geo+json"}, timeout=(CONNECT_T, 20), tries=2)
    if not r.ok:
        res = ([], scrub(f"zone fetch failed: HTTP {r.status} {r.error or ''}".strip())[:120])
    else:
        try:
            rings = rings_of(r.json().get("geometry"))
            res = (rings, None) if rings else ([], "zone has no polygon geometry")
        except Exception as e:
            res = ([], "zone parse error " + type(e).__name__)
    with ctx.lock:
        ctx.zone_cache[url] = res
    return res

def area_label(areas):
    if len(areas) <= 3:
        return ", ".join(areas)
    return f"{areas[0]} and {len(areas) - 1} more areas"

NWS_DEDUPE_H = 6
LAYER_ORDER = {"local": 0, "indiana": 1, "us": 2, "world": 3}

@source("nws", "NWS active alerts (Indiana, Marion INZ047, national severe)", "api.weather.gov", hourly=True)
def src_nws(s):
    seen = set(); ft = s.ctx.now_iso(); now = s.ctx.now
    raw = []
    for url in ("https://api.weather.gov/alerts/active?area=IN", "https://api.weather.gov/alerts/active/zone/INZ047",
                "https://api.weather.gov/alerts/active?severity=Extreme,Severe"):
        r = s.get(url, headers={"Accept": "application/geo+json"}, timeout=(CONNECT_T, 30))
        if not r.ok:
            continue
        for f in r.json().get("features", []):
            p = f["properties"]
            if p["id"] in seen:
                continue
            seen.add(p["id"])
            exp = parse_time(p.get("ends") or p.get("expires"))
            if exp and exp < now:
                continue
            raw.append((f, exp))
    deadline = time.time() + ZONE_DEADLINE_S
    recs, st = [], {"alerts_raw": len(raw), "alerts_with_feature_geometry": 0, "zones_fetched": 0, "items_without_geometry": 0}
    for f, exp in raw:
        p = f["properties"]
        ugc = (p.get("geocode") or {}).get("UGC") or []
        areas = [a.strip() for a in (p.get("areaDesc") or "").split(";") if a.strip()] or ["Area not stated"]
        rings, reason = rings_of(f.get("geometry")), None
        if rings:
            st["alerts_with_feature_geometry"] += 1
        else:
            zones = p.get("affectedZones") or []
            if not zones:
                reason = "alert has no polygon and no affectedZones"
            else:
                bad = []
                for z in zones:
                    zr, why = zone_rings(s, z, deadline)
                    if zr:
                        rings += zr
                    else:
                        bad.append(why)
                if bad and rings:
                    reason = f"partial: {len(zones) - len(bad)} of {len(zones)} zone shapes resolved ({bad[0]})"
                elif bad:
                    reason = bad[0] + (f" ({len(bad)} of {len(zones)} zones)" if len(zones) > 1 else "")
        recs.append({"p": p, "exp": exp, "ugc": ugc, "areas": areas, "rings": rings, "reason": reason, "t": parse_time(p.get("sent") or p.get("effective")) or now})
    # one item per (event type, issuing office) inside a 6 h window
    groups = {}
    for r in sorted(recs, key=lambda r: r["t"]):
        k = (r["p"].get("event"), r["p"].get("senderName"))
        gl = groups.setdefault(k, [])
        if gl and (r["t"] - gl[-1][0]["t"]).total_seconds() <= NWS_DEDUPE_H * 3600:
            gl[-1].append(r)
        else:
            gl.append([r])
    n_items = n_geom = 0
    for (event, office), gl in groups.items():
        for g in gl:
            first = g[0]; p0 = first["p"]
            areas = list(dict.fromkeys(a for r in g for a in r["areas"]))
            def lay(r):
                ugc = r["ugc"]; stc = ugc[0][:2] if ugc else ""
                if "INZ047" in ugc: return "local", "Indiana"
                if any(u.startswith("IN") for u in ugc): return "indiana", "Indiana"
                return "us", US_STATES.get(stc, "United States")
            layer, admin = min((lay(r) for r in g), key=lambda x: LAYER_ORDER[x[0]])
            rings = [c for r in g for c in r["rings"]]
            lat, lon, geom = geom_from_rings(rings)
            reasons = [r["reason"] for r in g if r["reason"]]
            greason = None
            if not geom:
                greason = reasons[0] if reasons else "no geometry resolved"
            elif reasons:
                greason = reasons[0]
            sev = max(sev_alert(r["p"].get("severity")) for r in g)
            exp = max((r["exp"] for r in g if r["exp"]), default=None)
            t = max(r["t"] for r in g)
            label = area_label(areas)
            sev_txt = max((r["p"].get("severity") or "" for r in g), key=lambda x: sev_alert(x))
            plain = f"{office} issued a {event} ({sev_txt}, certainty {p0.get('certainty')}) for {label}, effective {iso(parse_time(p0.get('effective')))} until {iso(exp)}. " + (p0.get("headline") or "")
            if len(g) > 1:
                plain += f" {len(g)} alerts of this type from this office within {NWS_DEDUPE_H} hours are merged here."
            bucket = int(first["t"].timestamp() // (NWS_DEDUPE_H * 3600))
            srcs = [src(i, "National Weather Service", "https://api.weather.gov/alerts/" + quote(r["p"]["id"], safe=""), ft, r["p"].get("headline") or r["p"].get("description")) for i, r in enumerate(g[:5], 1)]
            it = mk_item(f"nwsgrp|{event}|{office}|{bucket}", "alert", f"{event} — {label}", plain, t,
                         mk_place(label, lat, lon, geom, admin, maps_name=areas[0], geom_reason=greason if not geom else None), sev, layer, "primary",
                         srcs, "d19", updated=max(r["t"] for r in g), cutoff=iso(exp),
                         extra={"_expires": iso(exp), "count": len(g), "areas": areas, "office": office, "event": event})
            if geom and greason:
                it["place"]["geom_reason"] = greason
            s.ctx.event(it); s.n(); n_items += 1; n_geom += bool(geom)
            if not geom:
                st["items_without_geometry"] += 1
    st.update({"zones_fetched": len(s.ctx.zone_cache), "alert_items": n_items, "alert_items_with_geometry": n_geom, "deduped_away": len(raw) - n_items})
    with s.ctx.lock:
        s.ctx.stats["nws"] = st


@source("gdacs", "GDACS disaster alerts", "www.gdacs.org", hourly=True)
def src_gdacs(s):
    r = s.get("https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH?eventlist=EQ;TC;FL;VO;DR;WF&alertlevel=Green;Orange;Red", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); rows = []
    for f in r.json().get("features", []):
        p = f["properties"]
        if str(p.get("iscurrent")).lower() != "true":
            continue
        rows.append(f)
    rows.sort(key=lambda f: (f["properties"].get("alertlevel") != "Red", f["properties"].get("alertlevel") != "Orange", f["properties"].get("fromdate", "")), reverse=False)
    greens = 0
    for f in rows:
        p = f["properties"]; lvl = p.get("alertlevel")
        if lvl == "Green":
            greens += 1
            if greens > 40:
                continue
        lat, lon, geom = geom_info(f.get("geometry"))
        t = parse_time(p.get("fromdate")) or parse_time(p.get("datemodified"))
        country = p.get("country") or ""
        first_c = country.split(",")[0].strip() or None
        sevd = p.get("severitydata") or {}
        plain = strip_tags(p.get("htmldescription") or p.get("description") or "") + (f" Severity: {sevd.get('severitytext')}." if sevd.get("severitytext") else "")
        layer = "us" if "United States" in country else "world"
        it = mk_item("gdacs%s-%s" % (p.get("eventtype"), p.get("eventid")), "disaster", f"{lvl} alert: {p.get('name') or p.get('description')} ({p.get('eventtype')})", plain, t,
                     mk_place(country or "Not stated", lat, lon, admin=first_c), {"Red": 1.0, "Orange": 0.7, "Green": 0.3}.get(lvl, 0.3), layer, "primary",
                     [src(1, "GDACS (UN/EC Global Disaster Alert and Coordination System)", (p.get("url") or {}).get("report") or "https://www.gdacs.org", ft, p.get("htmldescription"))],
                     "d19", updated=parse_time(p.get("datemodified")), extra={"gdacs_type": p.get("eventtype")})
        s.ctx.event(it); s.n()

EONET_KIND = {"severeStorms": ("storm", 0.6), "volcanoes": ("volcano", 0.5), "floods": ("flood", 0.5), "wildfires": ("fire", 0.35), "earthquakes": ("quake", 0.4),
              "landslides": ("disaster", 0.4), "drought": ("disaster", 0.3), "dustHaze": ("disaster", 0.25), "snow": ("disaster", 0.3), "tempExtremes": ("disaster", 0.3), "manmade": ("disaster", 0.3)}

@source("eonet", "NASA EONET open natural events", "eonet.gsfc.nasa.gov", hourly=True)
def src_eonet(s):
    r = s.get("https://eonet.gsfc.nasa.gov/api/v3/events?status=open", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); now = s.ctx.now; out = []
    for e in r.json().get("events", []):
        cat = (e.get("categories") or [{}])[0].get("id")
        if cat not in EONET_KIND or not e.get("geometry"):
            continue
        g = e["geometry"][-1]
        t = parse_time(g.get("date"))
        if not t or (now - t).days > 10:
            continue
        out.append((e, cat, g, t))
    out.sort(key=lambda x: (x[1] not in ("severeStorms", "volcanoes"), -x[3].timestamp()))
    for e, cat, g, t in out[:90]:
        kind, sev = EONET_KIND[cat]
        mag = g.get("magnitudeValue"); unit = g.get("magnitudeUnit")
        if cat == "wildfires" and mag and unit == "acres" and mag >= 10000:
            sev = 0.55
        lat = lon = None
        if g.get("type") == "Point":
            lon, lat = g["coordinates"][:2]
        elif g.get("type") == "Polygon":
            ring = g["coordinates"][0]; lon = sum(c[0] for c in ring) / len(ring); lat = sum(c[1] for c in ring) / len(ring)
        title = e.get("title") or ""
        layer, admin = geo_from_text(title)
        if find_state(title) is None and layer == "world":
            admin = find_country(title)
        plain = f"NASA EONET lists an open {cat} event: {title}. Latest observation {iso(t)}" + (f", magnitude {mag} {unit}" if mag else "") + "."
        srcs = [src(1, "NASA EONET", e.get("link") or "https://eonet.gsfc.nasa.gov", ft, title)]
        for i, so in enumerate((e.get("sources") or [])[:2], 2):
            if so.get("url"):
                srcs.append(src(i, so.get("id") or "source", so["url"], None))
        it = mk_item("eonet" + e["id"], kind, title, plain, t, mk_place(title, round(lat, 3) if lat is not None else None, round(lon, 3) if lon is not None else None, admin=admin), sev, layer, "primary", srcs, "d19", updated=t)
        s.ctx.event(it); s.n()

def spc_time(hhmm, now):
    """SPC lists HHMM (UTC) inside a convective day that runs 12Z to 12Z, so a bare HHMM does not say which calendar day.
    The report happened at the latest such instant that is not after the fetch time (never in the future); one older than 30 h cannot be in today.csv and is dropped."""
    try:
        h, m = int(hhmm[:2]), int(hhmm[2:4])
        d = now.replace(hour=h, minute=m, second=0, microsecond=0)
    except ValueError:
        return None
    if d > now:
        d -= dt.timedelta(days=1)
    return d if (now - d) <= dt.timedelta(hours=30) else None

def convective_start(d):
    """12Z that opens the convective day containing d."""
    b = d.replace(hour=12, minute=0, second=0, microsecond=0)
    return b if d >= b else b - dt.timedelta(days=1)

@source("spc", "NOAA SPC storm reports (today)", "www.spc.noaa.gov", hourly=True)
def src_spc(s):
    r = s.get("https://www.spc.noaa.gov/climo/reports/today.csv")
    if not r.ok:
        return
    ft = s.ctx.now_iso(); now = s.ctx.now
    kind = None; agg = defaultdict(list)
    for row in csv.reader(io.StringIO(r.text())):
        if not row:
            continue
        if row[0] == "Time":
            kind = {"F_Scale": "tornado", "Speed": "wind", "Size": "hail"}.get(row[1]); continue
        if kind is None or len(row) < 7:
            continue
        hhmm = row[0].zfill(4)
        d = spc_time(hhmm, now)
        if d is None:
            continue
        try:
            lat, lon = float(row[5]), float(row[6])
        except ValueError:
            continue
        agg[(kind, row[4])].append({"t": d, "mag": row[1], "loc": row[2], "county": row[3], "lat": lat, "lon": lon, "com": row[7] if len(row) > 7 else ""})
    url = "https://www.spc.noaa.gov/climo/reports/today.html"
    for (kind, st), rs in sorted(agg.items()):
        rs.sort(key=lambda x: x["t"])
        stname = US_STATES.get(st, st); layer = "indiana" if st == "IN" else "us"
        start = convective_start(rs[0]["t"])
        if kind == "tornado":
            groups = [[x] for x in rs]
        else:
            groups = [rs]
        for g in groups:
            first = g[0]; last = g[-1]
            if kind == "tornado":
                title = f"Tornado report — {first['loc']}, {first['county']} Co., {st}"
                plain = f"SPC lists a preliminary tornado report ({first['mag']} scale) near {first['loc']}, {first['county']} County, {stname}, {iso(first['t'])}. {first['com']}"
                sev = 0.7
            elif kind == "hail":
                sizes = [int(x["mag"]) for x in g if x["mag"].isdigit()]
                mx = max(sizes) / 100 if sizes else None
                title = f"Hail reports — {stname}: {len(g)} report(s)" + (f", up to {mx:.2f} in" if mx else "")
                plain = f"SPC lists {len(g)} preliminary hail report(s) in {stname} between {iso(first['t'])} and {iso(last['t'])}" + (f"; largest {mx:.2f} inches." if mx else ".")
                sev = 0.55 if (mx or 0) >= 2.0 else 0.3
            else:
                sp = [int(x["mag"]) for x in g if x["mag"].isdigit()]
                mx = max(sp) if sp else None
                title = f"Damaging wind reports — {stname}: {len(g)} report(s)" + (f", up to {mx} mph" if mx else "")
                plain = f"SPC lists {len(g)} preliminary wind report(s) in {stname} between {iso(first['t'])} and {iso(last['t'])}" + (f"; highest measured/estimated {mx} mph." if mx else ".")
                sev = 0.5 if (mx or 0) >= 75 else 0.3
            key = f"spc-{kind}-{st}-{iso(start)}" + (f"-{first['loc']}-{iso(first['t'])}" if kind == "tornado" else "")
            it = mk_item(key, "storm_report", title, plain, last["t"], mk_place(f"{first['loc']}, {st}" if kind == "tornado" else stname, first["lat"], first["lon"], admin=stname), sev, layer, "primary",
                         [src(1, "NOAA Storm Prediction Center preliminary reports", url, ft, first["com"] or None)], "d19", updated=last["t"])
            s.ctx.event(it); s.n()

@source("mesonet", "Iowa Environmental Mesonet local storm reports", "mesonet.agron.iastate.edu", hourly=True)
def src_mesonet(s):
    r = s.get("https://mesonet.agron.iastate.edu/geojson/lsr.geojson?hours=12", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); out = []
    for f in r.json().get("features", []):
        p = f["properties"]; ty = (p.get("typetext") or "").upper(); mag = p.get("magf") or 0
        notable = ty in ("TORNADO", "FUNNEL CLOUD", "FLASH FLOOD", "HURRICANE", "STORM SURGE", "TSUNAMI", "WILDFIRE", "DEBRIS FLOW") or (ty == "HAIL" and mag >= 2) or \
                  (ty in ("TSTM WND GST", "NON-TSTM WND GST") and mag >= 75) or (ty in ("HEAVY SNOW",) and mag >= 12) or ty == "FLOOD" and p.get("st") == "IN" or p.get("st") == "IN" and ty in ("TSTM WND DMG", "TORNADO", "HAIL")
        if notable:
            out.append(f)
    out.sort(key=lambda f: f["properties"]["valid"], reverse=True)
    for f in out[:40]:
        p = f["properties"]; st = p.get("st"); stname = US_STATES.get(st, st)
        t = parse_time(p["valid"])
        title = f"{p['typetext'].title()} report — {p.get('city')}, {st}"
        plain = f"NWS local storm report ({p.get('source')}): {p['typetext'].title()}" + (f" {p.get('magnitude')} {p.get('unit')}" if p.get("magnitude") not in (None, "0", "") else "") + f" near {p.get('city')}, {p.get('county')} County, {stname}, {iso(t)}. {p.get('remark') or ''}"
        it = mk_item("lsr" + p["product_id"] + p["typetext"] + str(p.get("lat")), "lsr", title, plain, t, mk_place(f"{p.get('city')}, {st}", p.get("lat"), p.get("lon"), admin=stname),
                     0.5 if "TORNADO" in p["typetext"].upper() else 0.35, "indiana" if st == "IN" else "us", "single",
                     [src(1, "NWS local storm report via Iowa Environmental Mesonet", "https://mesonet.agron.iastate.edu/lsr/", ft, p.get("remark"))], "d19", updated=t)
        s.ctx.event(it); s.n()

@source("swpc", "NOAA SWPC space weather", "services.swpc.noaa.gov", hourly=True)
def src_swpc(s):
    ft = s.ctx.now_iso(); now = s.ctx.now
    r = s.get("https://services.swpc.noaa.gov/json/planetary_k_index_1m.json")
    if r.ok:
        rows = r.json()
        if rows:
            last = rows[-1]; kp = last.get("estimated_kp", last.get("kp_index"))
            t = parse_time(last["time_tag"])
            s.ctx.pnum("world", "Planetary K-index (1-minute estimate)", kp, "Kp 0-9", iso(t), "NOAA SWPC", "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json")
            s.n()
            if kp is not None and kp >= 5:
                it = mk_item("swpc-kp-" + local_date(t), "space_weather", f"Geomagnetic storm conditions: Kp {kp:.1f}", f"NOAA SWPC's estimated planetary K-index reached {kp:.1f} at {iso(t)} (5 or higher is G1 storm level).", t,
                             mk_place("Earth (global)"), min(1.0, 0.2 + 0.15 * (kp - 4)), "world", "primary", [src(1, "NOAA Space Weather Prediction Center", "https://www.swpc.noaa.gov/products/planetary-k-index", ft)], "d21")
                s.ctx.event(it)
    r = s.get("https://services.swpc.noaa.gov/products/alerts.json")
    if r.ok:
        for a in r.json():
            t = parse_time(a.get("issue_datetime"))
            if not t or (now - t).total_seconds() > 48 * 3600:
                continue
            msg = a.get("message", ""); m = re.search(r"NOAA Scale:\s*([GSR]\d)\s*-\s*(\w+)", msg)
            head = re.search(r"(ALERT|WARNING|WATCH|SUMMARY|EXTENDED WARNING|CONTINUED ALERT):\s*(.+)", msg)
            if not head or not m:
                continue
            title = f"{head.group(2).strip()} (NOAA scale {m.group(1)})"
            it = mk_item("swpc-" + a.get("product_id", "") + iso(t), "space_weather", title, f"NOAA SWPC {head.group(1).lower()} issued {iso(t)}: {head.group(2).strip()} NOAA scale {m.group(1)} - {m.group(2)}.", t,
                         mk_place("Earth (global)"), {"G1": 0.35, "G2": 0.5, "G3": 0.65, "G4": 0.8, "G5": 1.0}.get(m.group(1), 0.35) if m.group(1).startswith("G") else 0.35, "world", "primary",
                         [src(1, "NOAA Space Weather Prediction Center", "https://www.swpc.noaa.gov/products/alerts-watches-and-warnings", ft, head.group(2))], "d21")
            s.ctx.event(it); s.n()

@source("iss", "ISS position (wheretheiss.at)", "api.wheretheiss.at", hourly=True)
def src_iss(s):
    r = s.get("https://api.wheretheiss.at/v1/satellites/25544")
    if r.ok:
        j = r.json()
        s.ctx.pnum("world", "ISS position", f"lat {j['latitude']:.2f}, lon {j['longitude']:.2f}, altitude {j['altitude']:.0f} km", "degrees", iso(parse_time(j["timestamp"])), "wheretheiss.at", "https://api.wheretheiss.at/v1/satellites/25544")
        s.n()

@source("openmeteo", "Open-Meteo current weather (Indianapolis)", "api.open-meteo.com", hourly=True)
def src_openmeteo(s):
    r = s.get("https://api.open-meteo.com/v1/forecast", params={"latitude": 39.77, "longitude": -86.16, "current": "temperature_2m,wind_speed_10m", "temperature_unit": "fahrenheit", "wind_speed_unit": "mph"})
    if r.ok:
        c = r.json().get("current") or {}
        d = iso(parse_time(c.get("time") + ":00Z")) if c.get("time") else None
        s.ctx.pnum("marion", "Air temperature now (Indianapolis point 39.77, -86.16)", c.get("temperature_2m"), "F", d, "Open-Meteo", "https://api.open-meteo.com/v1/forecast")
        s.ctx.pnum("marion", "Wind speed now", c.get("wind_speed_10m"), "mph", d, "Open-Meteo", "https://api.open-meteo.com/v1/forecast")
        s.n(2)

@source("usgswater", "USGS streamflow, White River at Indianapolis", "waterservices.usgs.gov", hourly=True)
def src_usgswater(s):
    r = s.get("https://waterservices.usgs.gov/nwis/iv/?format=json&sites=03335500&parameterCd=00060")
    if r.ok:
        ts = r.json()["value"]["timeSeries"]
        if ts and ts[0]["values"][0]["value"]:
            v = ts[0]["values"][0]["value"][-1]
            s.ctx.pnum("marion", "White River at Indianapolis discharge (site 03335500)", float(v["value"]), "ft3/s", iso(parse_time(v["dateTime"])), "USGS Water Services", "https://waterservices.usgs.gov/nwis/iv/?sites=03335500")
            s.n()

@source("nhc", "NHC tropical weather RSS", "www.nhc.noaa.gov", hourly=True)
def src_nhc(s):
    r = s.get("https://www.nhc.noaa.gov/index-at.xml")
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for e in feedparser.parse(r.body).entries:
        title = e.get("title", "")
        if not re.search(r"\b(Hurricane|Tropical Storm|Tropical Depression|Potential Tropical Cyclone)\b", title):
            continue
        t = parse_time(e.get("published") or e.get("updated")) or s.ctx.now
        it = mk_item("nhc" + title + (e.get("link") or ""), "storm", title, strip_tags(e.get("summary", ""), 300) or title, t, mk_place("Atlantic basin"), 0.65, "world", "primary",
                     [src(1, "NOAA National Hurricane Center", e.get("link") or "https://www.nhc.noaa.gov", ft, title)], "d19")
        s.ctx.event(it); s.n()

@source("tsunami", "tsunami.gov bulletins (PHEB Atom)", "www.tsunami.gov", hourly=True)
def src_tsunami(s):
    r = s.get("https://www.tsunami.gov/events/xml/PHEBAtom.xml")
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for e in feedparser.parse(r.body).entries:
        txt = (e.get("title", "") + " " + e.get("summary", ""))
        if re.search(r"\b(tsunami (warning|advisory|watch))\b", txt, re.I) and not re.search(r"no tsunami", txt, re.I):
            t = parse_time(e.get("updated") or e.get("published")) or s.ctx.now
            it = mk_item("tsu" + e.get("id", e.get("title", "")), "tsunami", e.get("title", "Tsunami bulletin"), strip_tags(e.get("summary", ""), 300), t, mk_place("Pacific"), 0.8, "world", "primary",
                         [src(1, "NOAA Tsunami Warning Centers", e.get("link") or "https://www.tsunami.gov", ft, e.get("title"))], "d19")
            s.ctx.event(it); s.n()


# ------------------------------------------------------------------ record sources
def date_or_none(s):
    return parse_time(s) if s else None

FR_API = "https://www.federalregister.gov/api/v1/documents.json"

@source("fr_presdocu", "Federal Register presidential documents", "www.federalregister.gov", hourly=True)
def src_fr_presdocu(s):
    fields = ["title", "document_number", "publication_date", "html_url", "abstract", "executive_order_number", "signing_date", "subtype", "type"]
    r = s.get(FR_API, params=[("conditions[type][]", "PRESDOCU"), ("order", "newest"), ("per_page", "40")] + [("fields[]", f) for f in fields])
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for d in r.json().get("results", []):
        t = date_or_none(d.get("signing_date")) or date_or_none(d.get("publication_date"))
        eo = f" (Executive Order {d['executive_order_number']})" if d.get("executive_order_number") else ""
        plain = f"The Federal Register published a presidential document{eo}, signed {d.get('signing_date') or 'date not stated'}, published {d.get('publication_date')}. " + strip_tags(d.get("abstract") or "", 220)
        it = mk_item("frpres" + d["document_number"], "presidential", d["title"], plain, t, mk_place("United States", admin="United States"), 0.8, "us", "primary",
                     [src(1, "Federal Register", d["html_url"], ft, d.get("abstract") or d["title"])], domain_for(d["title"], 1), reach=reach_tags(d["title"] + " " + (d.get("abstract") or "")),
                     updated=date_or_none(d.get("publication_date")), extra={"_irrev": True})
        s.ctx.record("presidential", it); s.n()

PRORULE_TERMS = ["independent contractor", "self-employed", "gig economy", "small business", "estimated tax", "delivery driver", "rideshare", "worker classification"]

@source("fr_proposed", "Federal Register proposed rules (self-employed, gig, small business)", "www.federalregister.gov", hourly=False)
def src_fr_proposed(s):
    ft = s.ctx.now_iso(); seen = {}
    since = (s.ctx.now - dt.timedelta(days=60)).strftime("%Y-%m-%d")
    fields = ["title", "document_number", "publication_date", "html_url", "abstract", "comments_close_on", "agency_names", "type"]
    for term in PRORULE_TERMS:
        r = s.get(FR_API, params=[("conditions[type][]", "PRORULE"), ("order", "newest"), ("per_page", "12"), ("conditions[term]", term), ("conditions[publication_date][gte]", since)] + [("fields[]", f) for f in fields])
        if not r.ok:
            continue
        for d in r.json().get("results", []):
            seen.setdefault(d["document_number"], (d, []))[1].append(term)
    dropped = 0
    for dn, (d, terms) in seen.items():
        blob = (d["title"] + " " + (d.get("abstract") or "")).lower()
        terms = [t for t in terms if t in blob or t.replace("-", " ") in blob]
        if not terms:
            dropped += 1
            continue  # full-text hit only (e.g. boilerplate), not about the topic
        cc = d.get("comments_close_on")
        ag = ", ".join(d.get("agency_names") or [])
        plain = f"{ag} proposed a rule published {d.get('publication_date')}" + (f"; public comments close {cc}" if cc else "; no comment deadline stated") + ". " + strip_tags(d.get("abstract") or "", 220)
        it = mk_item("frprop" + dn, "rule", d["title"], plain, date_or_none(d.get("publication_date")), mk_place("United States", admin="United States"), 0.45, "us", "primary",
                     [src(1, "Federal Register", d["html_url"], ft, d.get("abstract") or d["title"])], domain_for(d["title"] + " " + " ".join(terms), 10),
                     reach=sorted(set(reach_tags(d["title"] + " " + (d.get("abstract") or "")) + (["self-employed"] if any(t in terms for t in ("self-employed", "small business")) else []) + (["gig"] if any(t in terms for t in ("independent contractor", "gig economy", "delivery driver", "rideshare", "worker classification")) else []) + (["tax"] if "estimated tax" in terms else []))),
                     cutoff=(iso(day_end(cc)) if cc else None), extra={"matched": terms, "comments_close_on": cc})
        s.ctx.record("rules_proposed", it); s.n()
    if dropped and not s.items:
        s.error = f"{dropped} full-text search hits dropped: topic words not in title or abstract"; s.soft = True

@source("fr_pi", "Federal Register public inspection (current)", "www.federalregister.gov", hourly=True)
def src_fr_pi(s):
    r = s.get("https://www.federalregister.gov/api/v1/public-inspection-documents/current.json", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); rows = r.json().get("results", [])
    rows.sort(key=lambda d: d.get("filed_at") or "", reverse=True)
    for d in rows[:100]:
        ag = ", ".join(d.get("agency_names") or [])
        plain = f"{ag} filed a {d.get('type')} for public inspection at {d.get('filed_at')}; scheduled for publication {d.get('publication_date')}."
        it = mk_item("frpi" + d["document_number"], "public_inspection", d.get("title") or d.get("toc_subject") or "Untitled", plain, parse_time(d.get("filed_at")), mk_place("United States", admin="United States"), 0.35, "us", "primary",
                     [src(1, "Federal Register public inspection", d["html_url"], ft, d.get("title"))], domain_for(d.get("title") or "", 1), reach=reach_tags(d.get("title") or ""), updated=parse_time(d.get("pdf_updated_at")))
        s.ctx.record("public_inspection", it); s.n()

@source("wh_rss", "White House presidential actions RSS", "www.whitehouse.gov", hourly=True)
def src_wh(s):
    r = s.get("https://www.whitehouse.gov/presidential-actions/feed/")
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for e in feedparser.parse(r.body).entries[:40]:
        t = parse_time(e.get("published") or e.get("updated"))
        it = mk_item("wh" + (e.get("link") or e.get("title")), "presidential", e.get("title", ""), "The White House posted a presidential action: " + strip_tags(e.get("summary", ""), 260), t, mk_place("United States", admin="United States"), 0.8, "us", "primary",
                     [src(1, "The White House", e.get("link") or "https://www.whitehouse.gov/presidential-actions/", ft, e.get("title"))], domain_for(e.get("title", ""), 1), reach=reach_tags(e.get("title", "")), extra={"_irrev": True})
        s.ctx.record("presidential", it); s.n()

@source("congress_rss", "Congress.gov bills presented to the President (RSS)", "www.congress.gov", hourly=True)
def src_congress_rss(s):
    r = s.get("https://www.congress.gov/rss/presented-to-president.xml")
    if not r.ok:
        return
    ft = s.ctx.now_iso(); fp = feedparser.parse(r.body)
    chan_t = parse_time(fp.feed.get("published") or fp.feed.get("updated"))
    for e in fp.entries:
        desc = strip_tags(e.get("summary", ""))
        it = mk_item("cgpres" + (e.get("id") or e.get("title")), "bill", f"{e.get('title')}: {desc}", f"Congress.gov lists {e.get('title')} as passed by both chambers and presented to the President for signature (feed dated {iso(chan_t)}).", chan_t,
                     mk_place("United States", admin="United States"), 0.6, "us", "primary", [src(1, "Congress.gov", e.get("link"), ft, desc)], domain_for(desc, 1), reach=reach_tags(desc), extra={"_irrev": True})
        s.ctx.record("bills_presented", it); s.n()

BILL_SLUG = {"HR": "house-bill", "S": "senate-bill", "HRES": "house-resolution", "SRES": "senate-resolution", "HJRES": "house-joint-resolution", "SJRES": "senate-joint-resolution", "HCONRES": "house-concurrent-resolution", "SCONRES": "senate-concurrent-resolution"}

@source("congress_api", "Congress.gov API v3 (bills, nominations, House votes)", "api.congress.gov", hourly=False, keys=("DATA_GOV_API_KEY",))
def src_congress_api(s):
    ft = s.ctx.now_iso(); now = s.ctx.now
    base = "https://api.congress.gov/v3/"
    key = {"api_key": "@DATA_GOV_API_KEY", "format": "json"}
    r = s.get(base + "bill/119", params={**key, "limit": 60, "sort": "updateDate desc"})
    if r.ok:
        for b in r.json().get("bills", []):
            la = b.get("latestAction") or {}; text = la.get("text", ""); t = date_or_none(la.get("actionDate"))
            if not t or (now - t).days > 7 or not re.search(r"Passed|Agreed to|Became Public Law|Presented to President|Signed by President|Vetoed|Cleared for White House", text):
                continue
            slug = BILL_SLUG.get(b["type"], b["type"].lower())
            url = f"https://www.congress.gov/bill/119th-congress/{slug}/{b['number']}"
            it = mk_item("cgbill%s%s" % (b["type"], b["number"]), "bill", f"{b['type']} {b['number']}: {b.get('title')}", f"Latest action {la.get('actionDate')}: {text}", t, mk_place("United States", admin="United States"), 0.55, "us", "primary",
                         [src(1, "Congress.gov API", url, ft, text)], domain_for(b.get("title", ""), 1), reach=reach_tags(b.get("title", "")), updated=date_or_none(b.get("updateDate")),
                         extra={"_irrev": bool(re.search(r"Became Public Law|Signed by President|Vetoed", text))})
            if re.search(r"Presented to President|Became Public Law|Signed by President|Vetoed|Cleared for White House", text):
                s.ctx.record("bills_presented", it)
            else:
                s.ctx.wire_add(it)
            s.n()
    r = s.get(base + "nomination/119", params={**key, "limit": 60, "sort": "updateDate desc"})
    if r.ok:
        for n in r.json().get("nominations", []):
            la = n.get("latestAction") or {}; t = date_or_none(la.get("actionDate")) or date_or_none(n.get("updateDate"))
            url = f"https://www.congress.gov/nomination/119th-congress/{n['number']}"
            it = mk_item("cgnom" + n["citation"], "nomination", f"{n['citation']}: {n.get('description', '')}", f"Latest action {la.get('actionDate')}: {la.get('text', 'not stated')} (organization: {n.get('organization') or 'not stated'}).", t,
                         mk_place("United States", admin="United States"), 0.4, "us", "primary", [src(1, "Congress.gov API", url, ft, la.get("text"))], "d01", updated=date_or_none(n.get("updateDate")),
                         extra={"_irrev": bool(re.search(r"Confirmed", la.get("text", "")))})
            s.ctx.record("nominations", it); s.n()
    r = s.get(base + "house-vote/119", params={**key, "limit": 1})
    if r.ok:
        total = (r.json().get("pagination") or {}).get("count", 0)
        r2 = s.get(base + "house-vote/119", params={**key, "limit": 25, "offset": max(0, total - 25)})
        if r2.ok:
            for v in r2.json().get("houseRollCallVotes", []):
                t = date_or_none(v.get("startDate"))
                if not t or (now - t).days > 14:
                    continue
                it = mk_item("cghv%s-%s-%s" % (v["congress"], v["sessionNumber"], v["rollCallNumber"]), "vote", f"House roll call {v['rollCallNumber']}: {v.get('legislationType')} {v.get('legislationNumber')} — {v.get('result')}",
                             f"The House held a {v.get('voteType')} vote on {v.get('legislationType')} {v.get('legislationNumber')}: result {v.get('result')} ({iso(t)}).", t, mk_place("United States", admin="United States"), 0.5, "us", "primary",
                             [src(1, "Congress.gov API (House Clerk roll call)", v.get("legislationUrl") or v.get("sourceDataURL"), ft, v.get("result"))], "d01", extra={"_irrev": True})
                s.ctx.wire_add(it); s.n()

@source("who_don", "WHO Disease Outbreak News", "www.who.int", hourly=True)
def src_who(s):
    r = s.get("https://www.who.int/api/news/diseaseoutbreaknews", params=[("$top", "25"), ("$orderby", "PublicationDateAndTime desc")], timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso()
    for d in r.json().get("value", []):
        title = strip_tags(d.get("Title") or d.get("OverrideTitle") or "")
        t = parse_time(d.get("PublicationDateAndTime") or d.get("PublicationDate"))
        country = title.split(" - ")[-1].strip() if " - " in title else None
        summ = strip_tags(d.get("Summary") or d.get("Overview") or "", 300)
        url = "https://www.who.int/emergencies/disease-outbreak-news/item/" + (d.get("UrlName") or "")
        it = mk_item("who" + d["Id"], "outbreak", title, f"WHO published Disease Outbreak News {d.get('DonId') or ''} on {iso(t)}. {summ}", t, mk_place(country or "World", admin=country), 0.7, "us" if country == "United States of America" else "world", "primary",
                     [src(1, "World Health Organization", url, ft, summ)], "d20", updated=parse_time(d.get("LastModified")))
        s.ctx.record("outbreaks", it); s.n()

DECL_TYPE = {"DR": "Major Disaster Declaration", "EM": "Emergency Declaration", "FM": "Fire Management Assistance Declaration", "FS": "Fire Suppression Authorization"}

@source("fema", "FEMA disaster declarations (OpenFEMA)", "www.fema.gov", hourly=True)
def src_fema(s):
    r = s.get("https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries", params=[("$top", "400"), ("$orderby", "declarationDate desc"), ("$select", "disasterNumber,state,declarationTitle,incidentType,declarationDate,declarationType,designatedArea")], timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); groups = {}
    for d in r.json().get("DisasterDeclarationsSummaries", []):
        g = groups.setdefault(d["disasterNumber"], {**d, "areas": 0})
        g["areas"] += 1
    for num, g in list(groups.items())[:100]:
        stname = US_STATES.get(g["state"], g["state"]); t = parse_time(g["declarationDate"])
        ty = DECL_TYPE.get(g["declarationType"], g["declarationType"])
        it = mk_item("fema%s" % num, "declaration", f"FEMA {g['declarationType']}-{num} {stname}: {g['declarationTitle'].title()}", f"FEMA recorded a {ty} for {stname} ({g['incidentType']}) dated {g['declarationDate'][:10]}, covering {g['areas']} designated area(s) in the latest 400 records.", t,
                     mk_place(stname, admin=stname), 0.6, "indiana" if g["state"] == "IN" else "us", "primary", [src(1, "OpenFEMA Disaster Declarations Summaries", f"https://www.fema.gov/disaster/{num}", ft, g["declarationTitle"])], "d19", extra={"_irrev": True})
        s.ctx.record("declarations", it)
        if (s.ctx.now - t).days <= 3:
            s.ctx.wire_add(it)
        s.n()

@source("courtlistener", "CourtListener opinions (SCOTUS, 7th Cir., Indiana courts)", "www.courtlistener.com", hourly=False, keys=("COURTLISTENER_TOKEN",))
def src_cl(s):
    ft = s.ctx.now_iso()
    for court, label, layer in (("scotus", "U.S. Supreme Court", "us"), ("ca7", "U.S. Court of Appeals, 7th Circuit", "us"), ("ind", "Indiana Supreme Court", "indiana"), ("indctapp", "Indiana Court of Appeals", "indiana")):
        r = s.get("https://www.courtlistener.com/api/rest/v4/search/", params=[("type", "o"), ("court", court), ("order_by", "dateFiled desc"), ("page_size", "15")],
                  headers={"Authorization": "Token @COURTLISTENER_TOKEN"}, timeout=(CONNECT_T, 30))
        if not r.ok:
            continue
        for d in r.json().get("results", []):
            t = date_or_none(d.get("dateFiled"))
            name = d.get("caseName") or d.get("caseNameFull") or "Untitled case"
            url = "https://www.courtlistener.com" + (d.get("absolute_url") or "")
            plain = f"{label} filed an opinion in {name} (docket {d.get('docketNumber') or 'n/a'}) on {d.get('dateFiled')}; status {d.get('status') or 'not stated'}."
            it = mk_item("cl%s" % d.get("cluster_id", d.get("id", name)), "ruling", f"{label}: {name}", plain, t, mk_place(label, admin="Indiana" if layer == "indiana" else "United States"), 0.5, layer, "single",
                         [src(1, "CourtListener (Free Law Project), court opinion record", url, ft, name)], "d04", extra={"_irrev": True})
            s.ctx.record("rulings", it); s.n()

@source("ofac", "OFAC recent actions", "ofac.treasury.gov", hourly=False)
def src_ofac(s):
    r = s.get("https://ofac.treasury.gov/recent-actions")
    if not r.ok:
        return
    ft = s.ctx.now_iso(); txt = r.text()
    for m in re.finditer(r'<a href="(/recent-actions/\d+)"[^>]*>(.*?)</a></div></div><div><div[^>]*>\s*([A-Z][a-z]+ \d{1,2}, \d{4})\s*-\s*(?:\s*<a[^>]*>(.*?)</a>)?', txt, re.S):
        href, title, date, cat = m.group(1), strip_tags(m.group(2)), m.group(3), strip_tags(m.group(4) or "")
        t = parse_time(dt.datetime.strptime(date, "%B %d, %Y").strftime("%Y-%m-%d"))
        c = find_country(title)
        it = mk_item("ofac" + href, "sanction", title, f"OFAC listed a recent action on {date}" + (f" (category: {cat})" if cat else "") + f": {title}.", t, mk_place(c or "United States", admin=c or "United States"), 0.55, "world" if c else "us", "primary",
                     [src(1, "U.S. Treasury OFAC", "https://ofac.treasury.gov" + href, ft, title)], domain_for(title, 10) if not re.search(r"Designation|Sanction|License", title) else "d16", reach=["tariff"] if False else [], extra={"_irrev": bool(re.search(r"Designation", title))})
        s.ctx.record("sanctions", it); s.n()


class CEParser(HTMLParser):
    """Wikipedia Portal:Current_events -> leaf list entries (inside div.current-events-content only) with category and cited external links."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.day, self.stack, self.entries, self.depth = None, [], [], 0
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "div":
            if re.fullmatch(r"\d{4}_[A-Za-z]+_\d{1,2}", a.get("id") or ""):
                self.day = a["id"]
            if self.depth:
                self.depth += 1
            elif "current-events-content" in (a.get("class") or ""):
                self.depth = 1
        if not self.depth:
            return
        if tag == "li":
            cats = ["".join(x["text"]).strip() for x in self.stack]
            self.stack.append({"text": [], "links": [], "ul": False, "cats": cats, "day": self.day})
        elif tag == "ul" and self.stack:
            self.stack[-1]["ul"] = True
        elif tag == "a" and self.stack and not self.stack[-1]["ul"] and "external" in (a.get("class") or "") and a.get("href", "").startswith("http"):
            self.stack[-1]["links"].append(a["href"])
    def handle_data(self, d):
        if self.depth and self.stack and not self.stack[-1]["ul"]:
            self.stack[-1]["text"].append(d)
    def handle_endtag(self, tag):
        if tag == "div" and self.depth:
            self.depth -= 1
            if self.depth == 0:
                self.stack = []
        if tag == "li" and self.stack and self.depth:
            e = self.stack.pop()
            if not e["ul"] and e["day"]:
                self.entries.append(e)

CE_SEV = {"armed": 0.5, "disaster": 0.5, "politics": 0.35, "health": 0.35, "law": 0.3, "business": 0.3, "science": 0.25}

@source("wiki_ce", "Wikipedia current events portal", "en.wikipedia.org", hourly=True)
def src_wiki(s):
    r = s.get("https://en.wikipedia.org/wiki/Portal:Current_events", timeout=(CONNECT_T, 30))
    if r.ok:
        html_text = r.text()
    else:  # fall back to the API (it rate-limits this host's IP more aggressively)
        r = s.get("https://en.wikipedia.org/w/api.php", params=[("action", "parse"), ("page", "Portal:Current_events"), ("prop", "text"), ("format", "json"), ("formatversion", "2")], timeout=(CONNECT_T, 30), tries=2)
        if not r.ok:
            return
        html_text = r.json()["parse"]["text"]
    s.error = None
    ft = s.ctx.now_iso(); now = s.ctx.now
    p = CEParser(); p.feed(html_text)
    for e in p.entries:
        try:
            d = dt.datetime.strptime(e["day"], "%Y_%B_%d").replace(tzinfo=UTC)
        except ValueError:
            continue
        if (now - d).days > 2:
            continue
        text = re.sub(r"\s+", " ", "".join(e["text"])).strip()
        if len(text) < 25:
            continue
        cat = e["cats"][0] if e["cats"] else ""
        sev = next((v for k, v in CE_SEV.items() if k in cat.lower()), 0.25)
        st = find_state(text); c = find_country(text)
        if LOCAL_RE.search(text):
            layer, admin, pn = "local", "Indiana", "Indianapolis"
        elif IN_RE.search(text):
            layer, admin, pn = "indiana", "Indiana", "Indiana"
        elif st or re.search(r"\bUnited States\b|\bU\.S\.\b", text):
            layer, admin, pn = "us", st or "United States", st or "United States"
        else:
            layer, admin, pn = "world", c, c or "World"
        srcs = [src(1, "Wikipedia: Portal:Current events (volunteer-edited compilation)", "https://en.wikipedia.org/wiki/Portal:Current_events", ft, text)]
        for i, u in enumerate(dict.fromkeys(e["links"]), 2):
            if i > 5:
                break
            srcs.append(src(i, "Cited publication (link only; not fetched by engine)", u, None))
        it = mk_item("wiki" + e["day"] + text[:80], "current_event", (sentences(text) or [text])[0].rstrip("."), text, d, mk_place(pn, admin=admin), sev, layer, "single", srcs, domain_for(cat + " " + text, 23), reach=reach_tags(text), updated=None)
        s.ctx.wire_add(it); s.n()


# ------------------------------------------------------------------ series
def kept_day(fa):
    """'Mon Oct 26' in Indianapolis time, for the 'kept from Mon Oct 26 fetch' label."""
    l = fa.astimezone(TZ)
    return f"{l.strftime('%a %b')} {l.day}"

def day_end(s):
    d = s if isinstance(s, dt.datetime) else dt.datetime.strptime(str(s)[:10], "%Y-%m-%d")
    return dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=TZ).astimezone(UTC)

TOL = {"daily": 5, "weekly": 7, "monthly": 20, "quarterly": 45}

def year_over_year(pts, latest, freq, pct_unit):
    yoy = None
    ld = parse_time(latest["date"])
    if ld:
        target = ld - dt.timedelta(days=365)
        best = min(pts, key=lambda p: abs((parse_time(p[0]) - target).days))
        if abs((parse_time(best[0]) - target).days) <= TOL.get(freq, 10) and best[1] not in (None, 0):
            if pct_unit:
                yoy = {"date": best[0], "value": best[1], "change": round(latest["value"] - best[1], 4), "change_unit": "percentage points"}
            else:
                yoy = {"date": best[0], "value": best[1], "change": round((latest["value"] / best[1] - 1) * 100, 3), "change_unit": "percent"}
    return yoy

def add_series(ctx, sid, name, plain, unit, freq, pts, source, release_next=None, flaw=None, pct_unit=False):
    pts = [[d, v] for d, v in pts if v is not None]
    pts.sort(key=lambda p: p[0])
    pts = pts[-400:]
    if not pts:
        return False
    latest = {"date": pts[-1][0], "value": pts[-1][1], "display": display_value(pts[-1][1], unit)}
    prev = {"date": pts[-2][0], "value": pts[-2][1]} if len(pts) > 1 else None
    yoy = year_over_year(pts, latest, freq, pct_unit)
    doc = {"id": sid, "name": name, "plain": plain.replace("{v}", f"{latest['value']}").replace("{d}", latest["date"]), "unit": unit, "freq": freq, "points": pts, "latest": latest, "prev": prev, "yoy": yoy,
           "release_next": release_next, "source": source, "prov": "fetched", "flaw": flaw, "fetched_at": iso(clock.now())}
    with ctx.lock:
        ctx.series[sid] = doc
    return True

FRED = [  # fred id, our id, name, unit, freq, pct_unit, flaw (known property of the series)
    ("MORTGAGE30US", "mortgage30us", "30-year fixed mortgage rate (Freddie Mac survey)", "percent", "weekly", True, "Weekly survey average across lenders; not a quote for any one loan."),
    ("ICSA", "icsa", "Initial jobless claims (seasonally adjusted)", "claims", "weekly", False, "Latest week is an advance estimate and is revised the following week."),
    ("PAYEMS", "payems", "Total nonfarm payroll employment (thousands of jobs, seasonally adjusted)", "thousand jobs", "monthly", False, "Survey estimate; the first print is revised in each of the next two months and again each year."),
    ("UNRATE", "unrate", "U.S. unemployment rate", "percent", "monthly", True, "Survey-based estimate with sampling error; revised each year."),
    ("CPIAUCSL", "cpiaucsl", "Consumer Price Index, all urban consumers (index 1982-84=100, seasonally adjusted)", "index", "monthly", False, "National average basket; not Indianapolis prices or his own costs."),
    ("CPIAUCNS", "cpiaucns", "Consumer Price Index, all urban consumers (index 1982-84=100, not seasonally adjusted)", "index", "monthly", False, "National average basket, not seasonally adjusted; not Indianapolis prices or his own costs."),
    ("SP500", "sp500", "S&P 500 stock index (daily close)", "index", "daily", False, "Daily close only; no value on market holidays."),
    ("DGS10", "dgs10", "10-year Treasury constant-maturity yield", "percent", "daily", True, "No value on market holidays."),
    ("DGS2", "dgs2", "2-year Treasury constant-maturity yield", "percent", "daily", True, "No value on market holidays."),
]
VINTAGE_FRED = ("CPIAUCSL", "CPIAUCNS", "PAYEMS", "MORTGAGE30US", "UNRATE")
FRED_STATES = [("INUR", "ur_in", "Indiana", "indiana"), ("ILUR", "ur_il", "Illinois", "illinois"), ("OHUR", "ur_oh", "Ohio", "ohio"), ("KYUR", "ur_ky", "Kentucky", "kentucky"), ("MIUR", "ur_mi", "Michigan", "michigan"), ("CAUR", "ur_ca", "California", "california")]

@source("fred", "FRED series (rates, claims, unemployment, CPI, S&P 500, yields, state unemployment)", "api.stlouisfed.org", hourly=False, keys=("FRED_API_KEY",))
def src_fred(s):
    base = "https://api.stlouisfed.org/fred/"
    key = {"api_key": "@FRED_API_KEY", "file_type": "json"}
    today = s.ctx.now.strftime("%Y-%m-%d")
    for fid, sid, name, unit, freq, pctu, flaw in FRED:
        r = s.get(base + "series/observations", params={**key, "series_id": fid, "sort_order": "desc", "limit": "400"})
        if not r.ok:
            continue
        pts = [[o["date"], float(o["value"])] for o in r.json().get("observations", []) if o.get("value") not in (".", None)]
        nxt = None
        r1 = s.get(base + "series/release", params={**key, "series_id": fid})
        if r1.ok and r1.json().get("releases"):
            rid = r1.json()["releases"][0]["id"]
            r2 = s.get(base + "release/dates", params={**key, "release_id": rid, "realtime_start": today, "realtime_end": "9999-12-31", "include_release_dates_with_no_data": "true", "sort_order": "asc", "limit": "5"})
            if r2.ok:
                ds = [d["date"] for d in r2.json().get("release_dates", []) if d["date"] >= today]
                nxt = ds[0] if ds else None
        src_ = {"name": "FRED, Federal Reserve Bank of St. Louis", "url": f"https://fred.stlouisfed.org/series/{fid}", "fetched": s.ctx.now_iso()}
        if add_series(s.ctx, sid, name, name + ". Latest {d}: {v} " + unit + ".", unit, freq, pts, src_, nxt, flaw, pctu):
            s.n()
            if fid in VINTAGE_FRED:   # r17/r18: first print -> revised (ALFRED)
                try:
                    vv = vintages.fetch_vintages(s.get, base, key, fid)
                    if vv:
                        vv[0]["revisions"] = revision_count(s.get, base, key, fid, vv[0]["date"])
                        s.ctx.series[sid]["vintages"] = vv
                except Exception:
                    pass
            lv = s.ctx.series[sid]["latest"]
            s.ctx.pnum("us", name, lv["value"], unit, lv["date"], "FRED", f"https://fred.stlouisfed.org/series/{fid}")
    for fid, sid, state, pid in FRED_STATES:
        r = s.get(base + "series/observations", params={**key, "series_id": fid, "sort_order": "desc", "limit": "400"})
        if not r.ok:
            continue
        pts = [[o["date"], float(o["value"])] for o in r.json().get("observations", []) if o.get("value") not in (".", None)]
        src_ = {"name": "FRED (BLS Local Area Unemployment Statistics, seasonally adjusted)", "url": f"https://fred.stlouisfed.org/series/{fid}", "fetched": s.ctx.now_iso()}
        if add_series(s.ctx, sid, f"{state} unemployment rate", f"{state} unemployment rate. Latest {{d}}: {{v}} percent.", "percent", "monthly", pts, src_, None, "Model-based state estimate from BLS LAUS; revised each year.", True):
            s.n()
            lv = s.ctx.series[sid]["latest"]
            s.ctx.pnum(pid, f"{state} unemployment rate", lv["value"], "percent", lv["date"], "FRED (BLS LAUS)", f"https://fred.stlouisfed.org/series/{fid}")

@source("eia", "EIA weekly retail gasoline (US, Midwest)", "api.eia.gov", hourly=False, keys=("EIA_API_KEY",))
def src_eia(s):
    for sid, name, code, pid in (("gas_us", "U.S. regular gasoline retail price", "EMM_EPMR_PTE_NUS_DPG", "us"), ("gas_midwest", "Midwest (PADD 2) regular gasoline retail price", "EMM_EPMR_PTE_R20_DPG", "indiana")):
        r = s.get(f"https://api.eia.gov/v2/seriesid/PET.{code}.W", params={"api_key": "@EIA_API_KEY", "length": "400"})
        if not r.ok:
            continue
        rows = r.json().get("response", {}).get("data", [])
        pts = [[d["period"], float(d["value"])] for d in rows if d.get("value") is not None]
        src_ = {"name": "U.S. Energy Information Administration", "url": f"https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s={code}&f=W", "fetched": s.ctx.now_iso()}
        if add_series(s.ctx, sid, name, name + ". Week of {d}: ${v} per gallon.", "$/gallon", "weekly", pts, src_, None, "Weekly survey average of retail stations; not the price at any given pump."):
            s.n()
            if sid == "gas_us":     # r18: ALFRED holds the EIA US regular series (GASREGW): first print -> revised
                try:
                    vv = vintages.fetch_vintages(s.get, "https://api.stlouisfed.org/fred/", {"api_key": "@FRED_API_KEY", "file_type": "json"}, "GASREGW")
                    if vv:
                        vv[0]["revisions"] = revision_count(s.get, "https://api.stlouisfed.org/fred/", {"api_key": "@FRED_API_KEY", "file_type": "json"}, "GASREGW", vv[0]["date"])
                        s.ctx.series[sid]["vintages"] = vv
                except Exception:
                    pass
            elif sid == "gas_midwest":   # r20: ALFRED holds no Midwest (PADD 2) series (checked: FRED has US-only GASREGW/GASREGCOVW/GASREGM), so no first print exists
                s.ctx.series[sid]["vintage_note"] = "No first-print comparison: FRED/ALFRED keeps vintages only for the U.S. gasoline series, not the Midwest (PADD 2) one, so this value is shown as read from EIA."
            lv = s.ctx.series[sid]["latest"]
            s.ctx.pnum(pid, name, lv["value"], "$/gallon", lv["date"], "EIA", src_["url"])

FRED_IDS = {sid: (fid, pctu) for fid, sid, _n, _u, _f, pctu, _fl in FRED}
FRED_IDS.update({sid: (fid, True) for fid, sid, _st, _pid in FRED_STATES})

def refresh_tails(F, ctx, series, now):
    """At publish: re-read the tail of each FRED series. If a newer dated value exists than the one fetched earlier in this run, store it (no stale-by-hours number).
    Every checked series records tail_checked_at and tail_newest (the newest dated value FRED holds right now)."""
    base = "https://api.stlouisfed.org/fred/series/observations"
    n_new = 0
    for sid, (fid, pctu) in sorted(FRED_IDS.items()):
        doc = series.get(sid)
        if not doc or doc.get("kept"):
            continue
        r = F.get(base, params={"api_key": "@FRED_API_KEY", "file_type": "json", "series_id": fid, "sort_order": "desc", "limit": "6"}, tries=2)
        doc["tail_checked_at"] = iso(clock.now())
        if not r.ok:
            doc["tail_newest"] = None; continue
        try:
            obs = [[o["date"], float(o["value"])] for o in r.json().get("observations", []) if o.get("value") not in (".", None)]
        except Exception:
            doc["tail_newest"] = None; continue
        if not obs:
            doc["tail_newest"] = None; continue
        newest = max(obs, key=lambda p: p[0])
        doc["tail_newest"] = {"date": newest[0], "value": newest[1]}
        old = doc["latest"]
        fresh = [p for p in obs if p[0] > old["date"]]
        if fresh:
            have = {d for d, _v in doc["points"]}
            pts = sorted(doc["points"] + [p for p in fresh if p[0] not in have], key=lambda p: p[0])[-400:]
            doc["points"] = pts
            doc["latest"] = {"date": pts[-1][0], "value": pts[-1][1]}
            doc["prev"] = {"date": pts[-2][0], "value": pts[-2][1]} if len(pts) > 1 else None
            doc["yoy"] = year_over_year(pts, doc["latest"], doc["freq"], pctu)
            doc["plain"] = doc["plain"].replace(str(old["value"]), str(doc["latest"]["value"])).replace(old["date"], doc["latest"]["date"])
            doc["fetched_at"] = iso(clock.now()); doc["newer_value_at"] = doc["fetched_at"]
            if isinstance(doc.get("source"), dict):
                doc["source"]["fetched"] = doc["fetched_at"]
            for lst in ctx.pnums.values():
                for n in lst:
                    if n.get("label") == doc["name"] and n.get("date") == old["date"] and str(n.get("source", "")).startswith("FRED"):
                        n.update(value=doc["latest"]["value"], date=doc["latest"]["date"], fetched_at=doc["fetched_at"])
            n_new += 1
    for sid, code in (("gas_us", "EMM_EPMR_PTE_NUS_DPG"), ("gas_midwest", "EMM_EPMR_PTE_R20_DPG")):    # r18: every series is re-read at publish, EIA ones too
        doc = series.get(sid)
        if not doc or doc.get("kept"):
            continue
        r = F.get(f"https://api.eia.gov/v2/seriesid/PET.{code}.W", params={"api_key": "@EIA_API_KEY", "length": "6"}, tries=2)
        doc["tail_checked_at"] = iso(clock.now())
        try:
            obs = [[d_["period"], float(d_["value"])] for d_ in r.json().get("response", {}).get("data", []) if d_.get("value") is not None] if r.ok else []
        except Exception:
            obs = []
        if not obs:
            doc["tail_newest"] = None; continue
        newest = max(obs, key=lambda p: p[0]); doc["tail_newest"] = {"date": newest[0], "value": newest[1]}
        old = doc["latest"]; fresh = [p for p in obs if p[0] > old["date"]]
        if fresh:
            have = {d_ for d_, _v in doc["points"]}
            pts = sorted(doc["points"] + [p for p in fresh if p[0] not in have], key=lambda p: p[0])[-400:]
            doc["points"] = pts; doc["latest"] = {"date": pts[-1][0], "value": pts[-1][1]}
            doc["prev"] = {"date": pts[-2][0], "value": pts[-2][1]} if len(pts) > 1 else None
            doc["yoy"] = year_over_year(pts, doc["latest"], doc["freq"], False)
            doc["plain"] = doc["plain"].replace(str(old["value"]), str(doc["latest"]["value"])).replace(old["date"], doc["latest"]["date"])
            doc["fetched_at"] = iso(clock.now()); doc["newer_value_at"] = doc["fetched_at"]
            if isinstance(doc.get("source"), dict): doc["source"]["fetched"] = doc["fetched_at"]
            n_new += 1
    return n_new

def _fmt_num(v):
    if isinstance(v, float) and v == int(v):
        return f"{int(v):,}" if abs(v) >= 10000 else str(int(v))
    return f"{v:.3f}".rstrip("0").rstrip(".") if isinstance(v, float) and abs(v) < 1e6 else str(v)

def revision_count(get, base, key, fid, date):
    """R20: how many times ALFRED has recorded a value for this observation date, minus the first print (0 = never revised). None when unreadable."""
    try:
        r = get(base + "series/observations", params={**key, "series_id": fid, "observation_start": date, "observation_end": date, "realtime_start": "1776-07-04", "realtime_end": "9999-12-31", "output_type": "4"})
        if not r.ok:
            return None
        vals = [o["value"] for o in r.json().get("observations", []) if o.get("value") not in (".", None)]
        return max(0, len(vals) - 1)
    except Exception:
        return None

def assign_rungs(series, now):
    """R17 Number Receipts. Every series gets number_rung {rung N1-N4, line, source_url, ...}.
    N1 first print + current (ALFRED vintages for the latest month); N2 current value re-read at publish (nothing newer dated);
    N3 value only, source date shown; N4 unverified (no source url or no dated value)."""
    counts = {"N1": 0, "N2": 0, "N3": 0, "N4": 0}
    for sid, doc in series.items():
        lv = doc.get("latest") or {}
        url = (doc.get("source") or {}).get("url") or ""
        chk = parse_time(doc.get("tail_checked_at"))
        chk_s = f"newer value checked {clock_et(chk)}" if chk else None
        vin = doc.get("vintages") or []
        k_ = 7 if doc.get("freq") == "monthly" else 10
        v0 = vin[0] if vin and str(vin[0].get("date", ""))[:k_] == str(lv.get("date", ""))[:k_] else None
        if not url or lv.get("value") is None or not lv.get("date"):
            r = {"rung": "N4", "line": "UNVERIFIED: no source link or no dated value", "source_url": url or None}
        elif v0:
            f, c = v0["first"], v0["current"]
            pub = parse_time(f["published"])
            pubs = f" ({display.display_when(pub, 'daily')})" if pub else ""   # P10: one display_when
            if f["value"] == c["value"]:
                core = f"first print {_fmt_num(f['value'])}{pubs} · unrevised"; state = "unrevised"
            else:
                core = f"first print {_fmt_num(f['value'])}{pubs} -> revised {_fmt_num(c['value'])}"; state = "revised"
            r = {"rung": "N1", "line": " · ".join([core] + ([chk_s] if chk_s else [])), "source_url": url, "first_print": f["value"], "first_published": f["published"], "current": c["value"], "state": state}
        elif chk and (doc.get("tail_newest") or {}).get("date") == lv.get("date"):
            r = {"rung": "N2", "line": f"value {_fmt_num(lv['value'])} ({lv['date']}) · re-read at publish, nothing newer dated · {chk_s}", "source_url": url}
        else:
            r = {"rung": "N3", "line": f"value {_fmt_num(lv['value'])} · source date {lv['date']}", "source_url": url}
        if r["rung"] != "N4":
            row = json.dumps([lv["date"], lv["value"]], separators=(",", ":"))
            r["source_value"] = f"{lv['date']}: {_fmt_num(lv['value'])}"; r["row"] = row
            r["sha256"] = hashlib.sha256((row + "|" + url).encode("utf-8")).hexdigest(); r["fetched_at"] = doc.get("fetched_at")
            fid_ = (FRED_IDS.get(sid) or (None,))[0] or {"gas_us": "GASREGW"}.get(sid)
            if fid_:
                r["series_url"] = f"https://fred.stlouisfed.org/series/{fid_}"     # r20: every FRED-held series, state ones included
            if r["rung"] == "N1" and fid_:
                pub_d = str(v0["first"].get("published") or "")[:10]
                r["alfred_url"] = f"https://alfred.stlouisfed.org/series?seid={fid_}" + (f"&vintage_date={pub_d}" if pub_d else "")
                r["alfred_vintage"] = pub_d or None
                rv = v0.get("revisions")
                r["revisions"] = rv
                r["revisions_line"] = None if rv is None else ("never revised since the first print" if rv == 0 else f"revised {rv} time{'s' if rv > 1 else ''} since the first print")
                if r["revisions_line"]:
                    r["line"] += " · " + r["revisions_line"]
            if doc.get("freq") == "weekly":      # same value carried forward
                pts = doc.get("points") or []; k = 1
                while k < len(pts) and pts[-1 - k][1] == lv["value"]:
                    k += 1
                if k > 1:
                    r["repeats"] = k - 1; r["line"] += f" · same value as the prior {k - 1} week{'s' if k > 2 else ''} (carried forward or unchanged)"
        if r["rung"] in ("N2", "N3"):       # R22 L-DATA: every N2/N3 row states its vintage/revision record
            rv = doc.get("revision") or {}
            if rv.get("last_revised"):
                r["vintage_line"] = (rv.get("note") or "revised by the source") ; r["last_revised"] = rv["last_revised"]
            else:
                r["vintage_line"] = "no revision record: the source publishes this value without a vintage history" + (f" ({doc['vintage_note']})" if doc.get("vintage_note") else ""); r["last_revised"] = None
            r["line"] += " · " + ("last revised " + r["last_revised"] if r["last_revised"] else "no revision record")
        doc["number_rung"] = r; counts[r["rung"]] += 1
    return counts

def flag_coincident_series(series):
    """Two different series that print the same latest value (same date, unit, frequency) get a plain note, so the page can say it is a coincidence and not a duplicate."""
    ids = sorted(series)
    for v in series.values():
        v.pop("coincides_with", None)
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            A, B = series[a], series[b]
            if A["latest"] == B["latest"] and A["unit"] == B["unit"] and A["freq"] == B["freq"] and A.get("source", {}).get("url") != B.get("source", {}).get("url"):
                pa = {d: v for d, v in A["points"]}; pb = {d: v for d, v in B["points"]}
                com = [d for d in pa if d in pb]; same = sum(1 for d in com if pa[d] == pb[d])
                for x, y, yd in ((A, B, b), (B, A, a)):
                    x["coincides_with"] = {"id": yd, "name": y["name"], "value": x["latest"]["value"], "date": x["latest"]["date"], "same_weeks": same, "compared_weeks": len(com),
                                           "note": f"{x['name']} and {y['name']} are different series; they print the same value for {x['latest']['date']} and agree on {same} of {len(com)} dates in common."}

@source("bea", "BEA real GDP growth (NIPA table 1.1.1)", "apps.bea.gov", hourly=False, keys=("BEA_API_KEY",))
def src_bea(s):
    yrs = ",".join(str(y) for y in range(s.ctx.now.year - 4, s.ctx.now.year + 1))
    r = s.get("https://apps.bea.gov/api/data/", params={"UserID": "@BEA_API_KEY", "method": "GetData", "datasetname": "NIPA", "TableName": "T10101", "Frequency": "Q", "Year": yrs, "ResultFormat": "JSON"}, timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    j = r.json().get("BEAAPI", {})
    if "Error" in j.get("Results", {}) or "Error" in j:
        s.error = "BEA API error: " + scrub(json.dumps(j.get("Results", {}).get("Error") or j.get("Error"))[:160]); return
    pts = []
    for d in j.get("Results", {}).get("Data", []):
        if d.get("LineNumber") == "1":
            m = re.match(r"(\d{4})Q(\d)", d["TimePeriod"])
            if m:
                q = int(m.group(2)); end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[q]
                try:
                    pts.append([f"{m.group(1)}-{end}", float(d["DataValue"].replace(",", ""))])
                except ValueError:
                    pass
    src_ = {"name": "U.S. Bureau of Economic Analysis", "url": "https://www.bea.gov/data/gdp/gross-domestic-product", "fetched": s.ctx.now_iso()}
    if add_series(s.ctx, "gdp_growth", "Real GDP growth, percent change from preceding quarter (annualized)", "Real GDP growth, quarter ending {d}: {v} percent at an annual rate.", "percent (annualized)", "quarterly", pts, src_, None, "Date shown is quarter end; first estimates are revised twice.", True):
        s.n(); lv = s.ctx.series["gdp_growth"]["latest"]
        s.ctx.pnum("us", "Real GDP growth (quarterly, annualized)", lv["value"], "percent", lv["date"], "BEA", src_["url"])

@source("treasury_dts", "Treasury Daily Treasury Statement: cash balance", "api.fiscaldata.treasury.gov", hourly=False)
def src_dts(s):
    r = s.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance", params=[("sort", "-record_date"), ("page[size]", "500")], timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    pts = []
    for d in r.json().get("data", []):
        if d.get("account_type", "").startswith("Treasury General Account (TGA) Closing Balance"):
            v = d.get("close_today_bal") if d.get("close_today_bal") not in (None, "null") else d.get("open_today_bal")
            try:
                pts.append([d["record_date"], float(v)])
            except (TypeError, ValueError):
                pass
    src_ = {"name": "U.S. Treasury Fiscal Data, Daily Treasury Statement", "url": "https://fiscaldata.treasury.gov/datasets/daily-treasury-statement/operating-cash-balance", "fetched": s.ctx.now_iso()}
    if add_series(s.ctx, "tga_close", "Treasury General Account closing cash balance", "Treasury cash balance at close of {d}: ${v} million.", "$ millions", "daily", pts, src_, None, "Reported for business days only; large swings follow tax dates and debt auctions."):
        s.n()

BLS_IDS = ["CUUR0000SA0", "CUSR0000SA0", "LNS14000000", "LASST180000000000003", "LASST170000000000003", "LASST390000000000003", "LASST210000000000003", "LASST260000000000003", "LASST060000000000003"]

@source("bls", "BLS public API v1 (CPI, state unemployment)", "api.bls.gov", hourly=False)
def src_bls(s):
    y = s.ctx.now.year
    r = s.get("https://api.bls.gov/publicAPI/v1/timeseries/data/", method="POST", json_body={"seriesid": BLS_IDS, "startyear": str(y - 2), "endyear": str(y)}, headers={"Content-Type": "application/json"}, timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    try:
        j = r.json()
    except ValueError:
        # HTTP 200 but not JSON: an HTML interstitial / bot-block page, not data. Report it as blocked, say what came back.
        head = re.sub(r"\s+", " ", strip_tags(r.text(), 120) if r.text() else "")[:100]
        s.blocked = True; s.error = f"HTTP 200 but not JSON (looks like a block or error page: {scrub(head)!r}); not bypassed"; return
    if j.get("status") != "REQUEST_SUCCEEDED":
        msg = scrub("; ".join(j.get("message") or [j.get("status", "unknown")]))
        if re.search(r"threshold|quota|limit|too many", msg, re.I):
            # the API answers 200 with REQUEST_NOT_PROCESSED when the unregistered daily quota is used up: that is a refusal, not data and not a parser bug
            s.blocked = True; s.error = "daily request quota of the keyless BLS v1 API reached (HTTP 200, REQUEST_NOT_PROCESSED); no data until it resets; not bypassed"
        else:
            s.error = "BLS: " + msg[:180]
        return
    names = {"CUUR0000SA0": ("cpi_bls", "CPI-U all items, U.S. city average (not seasonally adjusted)", "index", "monthly", False)}
    for ser in j["Results"]["series"]:
        pts = []
        for d in ser["data"]:
            if d["period"].startswith("M") and d["period"] != "M13":
                try:
                    pts.append([f"{d['year']}-{d['period'][1:]}-01", float(d["value"])])
                except ValueError:
                    pass
        if ser["seriesID"] in names:
            sid, nm, unit, fr, pu = names[ser["seriesID"]]
            if add_series(s.ctx, sid, nm, nm + ". {d}: {v}.", unit, fr, pts, {"name": "U.S. Bureau of Labor Statistics", "url": "https://data.bls.gov/timeseries/" + ser["seriesID"], "fetched": s.ctx.now_iso()}, None, "Not seasonally adjusted; national average."):
                s.n()


# ------------------------------------------------------------------ places data
WB_COUNTRIES = {"CHN": "china", "RUS": "russia", "IRN": "iran", "SAU": "saudi_arabia", "UKR": "ukraine", "ISR": "israel", "CAN": "canada", "MEX": "mexico"}

@source("worldbank", "World Bank population and GDP (8 tracked countries)", "api.worldbank.org", hourly=False)
def src_wb(s):
    codes = ";".join(WB_COUNTRIES)
    for ind, label, unit in (("SP.POP.TOTL", "Population, total", "people"), ("NY.GDP.MKTP.CD", "GDP (current US$)", "US dollars")):
        r = s.get(f"https://api.worldbank.org/v2/country/{codes}/indicator/{ind}", params={"format": "json", "mrnev": "1", "per_page": "50"}, timeout=(CONNECT_T, 30), tries=3)
        if not r.ok:
            continue
        j = r.json()
        if len(j) < 2 or not j[1]:
            continue
        for row in j[1]:
            pid = WB_COUNTRIES.get(row.get("countryiso3code"))
            if pid and row.get("value") is not None:
                s.ctx.pnum(pid, label, row["value"], unit, str(row["date"]), "World Bank Open Data", f"https://data.worldbank.org/indicator/{ind}?locations={row['countryiso3code'][:2]}")
                s.n()

HEAD_QIDS = {"united_states": ("Q30", "United States of America"), "canada": ("Q16", "Canada"), "mexico": ("Q96", "Mexico"), "china": ("Q148", "People's Republic of China"), "russia": ("Q159", "Russia"), "iran": ("Q794", "Iran"),
             "saudi_arabia": ("Q851", "Saudi Arabia"), "ukraine": ("Q212", "Ukraine"), "israel": ("Q801", "Israel"), "indiana": ("Q1415", "Indiana"), "illinois": ("Q1204", "Illinois"), "ohio": ("Q1397", "Ohio"),
             "kentucky": ("Q1603", "Kentucky"), "michigan": ("Q1166", "Michigan"), "california": ("Q99", "California"), "marion": ("Q6346", "Indianapolis")}

# Official head-of-government pages, from the source rows named in spec/Places.txt (Sources rows 907, 1103, 1107, 1510-1513, 1407-1412).
# Each entry: url, then a regex whose group 1 is the name as the page prints it. Names are read from the live page; nothing is typed in.
_NOT_NAME = {"Home", "Skip", "Toggle", "Our", "Legislative", "Assistance", "Explore", "Contact", "About", "Office", "The", "Announces", "Menu", "News", "Search", "Map", "Press", "Photo", "Pete", "Of", "Biography", "Meet", "First", "Lady", "Second"}
OFFICIAL_HEADS = {
    "indiana": ("https://www.in.gov/gov/", r"(?<!Lieutenant )Governor ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Governor"),
    "illinois": ("https://gov.illinois.gov/", r"\b([A-Z][A-Za-z.]+ [A-Z][a-z]+), Governor\b", "Governor"),
    "ohio": ("https://governor.ohio.gov/", r"(?<!Lieutenant )Governor ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Governor"),
    "kentucky": ("https://governor.ky.gov/", r"(?<!Lieutenant )Governor ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Governor"),
    "michigan": ("https://www.michigan.gov/whitmer", r"(?<!Lieutenant )Governor ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Governor"),
    "california": ("https://www.gov.ca.gov/", r"(?<!Lieutenant )Governor ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Governor"),
    "united_states": ("https://www.whitehouse.gov/administration/", r"(?<!Vice )President ([A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+)\b", "President"),
    "canada": ("https://www.pm.gc.ca/en", r"\b([A-Z][a-z]+ [A-Z][a-z]+) - Prime Minister of Canada", "Prime Minister"),
    "mexico": ("https://www.gob.mx/presidencia", r"Presidenta ([A-Z][a-z]+ [A-Z][a-z]+)\b", "President"),
    "china": ("https://english.www.gov.cn/", r"Premier ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Premier"),
    "russia": ("http://en.kremlin.ru/", r"President of Russia ([A-Z][a-z]+ [A-Z][a-z]+)\b", "President"),
    "iran": ("https://president.ir/en", r"President ([A-Z][a-z]+ [A-Z][a-z]+)\b", "President"),
    "saudi_arabia": ("https://www.spa.gov.sa/", r"Prime Minister ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Prime Minister"),
    "ukraine": ("https://www.president.gov.ua/en", r"President ([A-Z][a-z]+ [A-Z][a-z]+)\b", "President"),
    "israel": ("https://www.pmo.gov.il/en", r"Prime Minister ([A-Z][a-z]+ [A-Z][a-z]+)\b", "Prime Minister"),
}

def extract_head(text, rx):
    """Most frequent name matching rx in the page text; words that are page furniture are rejected."""
    cnt = Counter()
    for m in re.finditer(rx, text):
        nm = m.group(1).strip()
        if not any(w in _NOT_NAME for w in nm.split()):
            cnt[nm] += 1
    if not cnt:
        return None
    return sorted(cnt.items(), key=lambda kv: (-kv[1], -len(kv[0])))[0][0]

@source("official_heads", "Official head-of-government pages (Places.txt source rows)", "official government sites", hourly=False)
def src_official_heads(s):
    ft = s.ctx.now_iso()
    for pid, (url, rx, role) in OFFICIAL_HEADS.items():
        r = s.ctx.F.get(url, timeout=(CONNECT_T, 20), tries=2, cap=2_000_000)
        ev = {"url": url, "role": role, "readable": False, "name": None, "http": r.status or None, "fetched": ft}
        if r.ok:
            text = strip_tags(r.text())
            ev["text"] = text
            if len(text) > 400:     # a JS shell or an error page is not a readable page
                ev["readable"] = True
                ev["name"] = extract_head(text, rx)
            else:
                ev["why"] = "page returned almost no text (script shell or error page)"
        else:
            ev["why"] = scrub(f"HTTP {r.status} {r.error or ''}".strip())[:100]
        with s.ctx.lock:
            s.ctx.pextra[pid]["official"] = ev
        if ev["name"]:
            s.n()

def resolve_head(off, heads, fetched):
    return __import__("records").who_runs(off, heads, fetched)

def _resolve_head_old(off, heads, fetched):
    """Official page first. Wikidata (crowd-edited) only as a labelled fallback, and never against an official page that was read.
    -> who_runs dict {name, source, kind[, note]}"""
    heads = list(dict.fromkeys(heads or []))
    surname = lambda n: n.split()[-1].lower() if n else ""
    wd_txt = "; ".join(heads)[:120]
    if off and off.get("readable") and off.get("name"):
        who = {"name": off["name"], "source": f"Official page {off['url']}, read {fetched}", "kind": "official"}
        if heads and not any(surname(h) == surname(off["name"]) for h in heads):
            who["note"] = f"Wikidata (crowd-edited) says {wd_txt}; not used because it disagrees with the official page."
        return who
    if off and off.get("readable"):
        agree = [h for h in heads if surname(h) and surname(h) in (off.get("text") or "").lower()]
        if agree:
            return {"name": "; ".join(agree)[:120], "source": f"Wikidata P6 (crowd-edited); this name also appears on the official page {off['url']}, read {fetched}", "kind": "crowd-edited"}
        return {"name": "not measured", "source": f"Official page {off['url']} was read but no head of government could be extracted from it" + (f"; Wikidata says {wd_txt} but that name is not on the page, so it is not used" if heads else ""), "kind": "not measured"}
    why = (off or {}).get("why") or "no official page is listed for this place"
    if heads:
        return {"name": wd_txt, "source": f"Wikidata P6 (head of government), crowd-edited, fetched {fetched}; not checked against an official page ({why})", "kind": "crowd-edited"}
    return {"name": "not measured", "source": f"Official page not readable ({why}) and Wikidata returned no value", "kind": "not measured"}

@source("wikidata_heads", "Wikidata head of government (tracked places)", "query.wikidata.org", hourly=False)
def src_wikidata(s):
    vals = " ".join("wd:" + q for q, _ in HEAD_QIDS.values())
    q = "SELECT ?p ?pLabel ?h ?hLabel WHERE { VALUES ?p { %s } ?p p:P6 ?st . ?st ps:P6 ?h . FILTER NOT EXISTS { ?st pq:P582 ?end } SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" . } }" % vals
    r = s.get("https://query.wikidata.org/sparql", params={"query": q, "format": "json"}, headers={"Accept": "application/sparql-results+json"}, timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    by_q = {v[0]: k for k, v in HEAD_QIDS.items()}
    for b in r.json()["results"]["bindings"]:
        qid = b["p"]["value"].rsplit("/", 1)[-1]; pid = by_q.get(qid)
        if not pid:
            continue
        if b["pLabel"]["value"].lower() != HEAD_QIDS[pid][1].lower() and pid != "united_states":
            continue
        if re.fullmatch(r"Q\d+", b["hLabel"]["value"]):   # Wikidata has no English label for this person: not a name
            continue
        with s.ctx.lock:
            s.ctx.pextra[pid].setdefault("heads", []).append(b["hLabel"]["value"])
        s.n()

@source("openstates", "Open States: Indiana legislature bills", "v3.openstates.org", hourly=False, keys=("OPENSTATES_API_KEY",))
def src_openstates(s):
    r = s.get("https://v3.openstates.org/bills", params=[("jurisdiction", "Indiana"), ("sort", "latest_action_desc"), ("per_page", "20")], headers={"X-API-KEY": "@OPENSTATES_API_KEY"}, timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    ft = s.ctx.now_iso(); res = r.json().get("results", [])
    if res:
        top = res[0]; s.n()
        s.ctx.pnum("indiana", "Most recent bill action recorded by Open States", f"{top['identifier']} on {top.get('latest_action_date')}: {clip(top.get('latest_action_description') or '', 90)}", "text", top.get("latest_action_date"), "Open States", top.get("openstates_url") or "https://openstates.org/in/")
    for b in res:
        t = date_or_none(b.get("latest_action_date"))
        if not t or (s.ctx.now - t).days > 30:
            continue
        it = mk_item("os" + b["id"], "bill", f"Indiana {b['identifier']}: {b.get('title')}", f"Open States records the latest action on {b['identifier']} ({b.get('session')} session) as {b.get('latest_action_date')}: {b.get('latest_action_description')}", t,
                     mk_place("Indiana General Assembly", admin="Indiana"), 0.4, "indiana", "single", [src(1, "Open States (aggregates the Indiana legislature's site)", b.get("openstates_url") or "https://openstates.org/in/", ft, b.get("latest_action_description"))],
                     domain_for(b.get("title", ""), 2), reach=reach_tags(b.get("title", "")))
        s.ctx.wire_add(it); s.n()


# ------------------------------------------------------------------ clocks (dated records only)
def mk_clock(ctx, title, kind, closes_at, source_name, url, place=None, prov="fetched", note=None):
    ca = iso(closes_at) if isinstance(closes_at, dt.datetime) else closes_at
    c = {"id": "clk-" + sha(kind + title + str(ca)), "title": title, "kind": kind, "closes_at": ca, "source": source_name, "url": url, "prov": prov}
    if place:
        c["place"] = place
    if note:
        c["note"] = note
    with ctx.lock:
        ctx.clocks.append(c)
    return c

@source("fec_dates", "FEC election dates (Indiana 2026)", "api.open.fec.gov", hourly=False, keys=("DATA_GOV_API_KEY",))
def src_fec(s):
    r = s.get("https://api.open.fec.gov/v1/election-dates/", params={"api_key": "@DATA_GOV_API_KEY", "election_state": "IN", "election_year": "2026", "per_page": "20"})
    if not r.ok:
        return
    seen = set()
    for d in r.json().get("results", []):
        if d.get("election_type_id") not in ("G", "P") or not d.get("election_date"):
            continue
        ed = d["election_date"]
        if ed in seen or day_end(ed) < s.ctx.now:
            continue
        seen.add(ed)
        mk_clock(s.ctx, f"Indiana {d.get('election_type_full') or 'election'} (federal) — {ed}", "election", iso(dt.datetime.strptime(ed, "%Y-%m-%d").replace(hour=18, tzinfo=TZ)), "FEC election dates API", "https://api.open.fec.gov/v1/election-dates/?election_state=IN&election_year=2026", "Indiana", note="closes_at is 6:00 pm Eastern on election day (poll close time cited in Gaps G1)")
        s.n()

@source("fomc", "Federal Reserve FOMC calendar page", "www.federalreserve.gov", hourly=False)
def src_fomc(s):
    r = s.get("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    txt = r.text(); year = None; month = None; out = []
    for m in re.finditer(r'(\d{4}) FOMC Meetings|fomc-meeting__month[^>]*>\s*<strong>([A-Za-z/]+)</strong>|fomc-meeting__date[^>]*>\s*([0-9\-\*/ ]+)\s*<', txt):
        if m.group(1):
            year = int(m.group(1))
        elif m.group(2):
            month = m.group(2)
        elif m.group(3) and year and month:
            days = re.findall(r"\d+", m.group(3))
            if "/" in month or not days:
                continue
            try:
                end = dt.datetime.strptime(f"{year} {month[:3]} {days[-1]}", "%Y %b %d")
            except ValueError:
                continue
            out.append((end, month, "-".join(days), m.group(3).strip()))
    now_local = s.ctx.now.astimezone(TZ).replace(tzinfo=None)
    n = 0
    for end, month, days, raw in sorted(out):
        if end.date() >= now_local.date() and n < 4:
            mk_clock(s.ctx, f"FOMC meeting {month} {days}, {end.year} (rate decision on last day)", "fed", iso(dt.datetime(end.year, end.month, end.day, 14, 0, tzinfo=TZ)), "Federal Reserve FOMC meeting calendar", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
                     note="closes_at is 2:00 pm Eastern on the last day; the page lists dates only, so the time is UNVERIFIED")
            n += 1; s.n()

@source("healthcare_gov", "HealthCare.gov dates and deadlines", "www.healthcare.gov", hourly=False)
def src_hc(s):
    r = s.get("https://www.healthcare.gov/quick-guide/dates-and-deadlines/", timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    t = strip_tags(r.text())
    m = re.search(r"Open Enrollment for (\d{4}) starts (\w+ \d{1,2})", t)
    if m:
        plan = int(m.group(1))
        d = dt.datetime.strptime(f"{plan - 1} {m.group(2)}", "%Y %B %d")
        mk_clock(s.ctx, f"Marketplace Open Enrollment for {plan} coverage starts", "enrollment", iso(dt.datetime(d.year, d.month, d.day, 0, 0, tzinfo=TZ)), "HealthCare.gov dates and deadlines", "https://www.healthcare.gov/quick-guide/dates-and-deadlines/", note="date-only on the page; start of day Eastern assumed"); s.n()
        m2 = re.search(r"January 15:\s*Open Enrollment ends", t)
        if m2:
            mk_clock(s.ctx, f"Marketplace Open Enrollment for {plan} coverage ends (last day to enroll or change plans)", "enrollment", iso(day_end(f"{plan}-01-15")), "HealthCare.gov dates and deadlines", "https://www.healthcare.gov/quick-guide/dates-and-deadlines/"); s.n()

@source("in_dor_estimated", "Indiana DOR estimated tax payment page", "www.in.gov", hourly=False)
def src_dor(s):
    url = "https://www.in.gov/dor/i-need-to/make-a-payment/estimated-payments"
    r = s.get(url, timeout=(CONNECT_T, 30))
    if not r.ok:
        return
    t = strip_tags(r.text())
    m = re.search(r"due on or before:\s*April 15\s+June 15\s+Sept\. 15\s+Jan\. 15 of the following year", t)
    if not m:
        s.error = "rule text not found on page (layout changed?)"; return
    now_local = s.ctx.now.astimezone(TZ).date(); cands = []
    for y in (now_local.year, now_local.year + 1):
        cands += [dt.date(y, 4, 15), dt.date(y, 6, 15), dt.date(y, 9, 15), dt.date(y + 1, 1, 15)]
    n = 0
    for d in sorted(set(cands)):
        due = d
        while due.weekday() >= 5:
            due += dt.timedelta(days=1)
        if due >= now_local and n < 3:
            mk_clock(s.ctx, f"Estimated tax installment due (Indiana DOR rule: Apr 15, Jun 15, Sep 15, Jan 15)" + (f" — {d.isoformat()} falls on a weekend, moves to {due.isoformat()}" if due != d else f" — {d.isoformat()}"), "tax", iso(day_end(due.isoformat())), "Indiana Department of Revenue", url, "Indiana",
                     prov="computed", note="Dates computed from the fetched rule. Page also says holidays move the date to the next business day; holidays not checked (UNVERIFIED).")
            n += 1; s.n()


# ------------------------------------------------------------------ news feeds
PUB_BY_HOST = {"feeds.bbci.co.uk": ("bbc", "BBC News"), "www.theguardian.com": ("guardian", "The Guardian"), "rss.nytimes.com": ("nyt", "The New York Times"), "feeds.a.dj.com": ("wsj", "The Wall Street Journal"),
               "rss.dw.com": ("dw", "Deutsche Welle"), "www.aljazeera.com": ("aljazeera", "Al Jazeera"), "feeds.npr.org": ("npr", "NPR"), "www.pbs.org": ("pbs", "PBS NewsHour"), "www.cbsnews.com": ("cbs", "CBS News"),
               "abcnews.go.com": ("abc", "ABC News"), "news.un.org": ("un", "UN News"), "www.france24.com": ("france24", "France 24"), "www.euronews.com": ("euronews", "Euronews"), "www.scmp.com": ("scmp", "South China Morning Post"),
               "www.thehindu.com": ("hindu", "The Hindu"), "www.channelnewsasia.com": ("cna", "Channel NewsAsia"), "www.al-monitor.com": ("almonitor", "Al-Monitor"), "allafrica.com": ("allafrica", "AllAfrica"),
               "en.mercopress.com": ("mercopress", "MercoPress"), "www.cbc.ca": ("cbc", "CBC News"), "rss.politico.com": ("politico", "Politico"), "thehill.com": ("thehill", "The Hill"), "stateline.org": ("stateline", "Stateline"),
               "indianacapitalchronicle.com": ("icc", "Indiana Capital Chronicle"), "mirrorindy.org": ("mirrorindy", "Mirror Indy"), "www.wfyi.org": ("wfyi", "WFYI"), "www.who.int": ("who", "WHO"), "api.axios.com": ("axios", "Axios"),
               "www.c-span.org": ("cspan", "C-SPAN"), "travel.state.gov": ("statedept", "U.S. State Department"), "www.timesofisrael.com": ("toi", "The Times of Israel"), "statesnewsroom.com": ("statesnewsroom", "States Newsroom"),
               "www.electionguide.org": ("ifes", "IFES ElectionGuide"), "feeds.skynews.com": ("sky", "Sky News"), "feeds.nbcnews.com": ("nbc", "NBC News"),
               "www.wthr.com": ("wthr", "WTHR"), "www.wishtv.com": ("wishtv", "WISH-TV")}
FEEDS_CORE = [  # (url, region, hourly)
    ("https://feeds.npr.org/1001/rss.xml", "US", True), ("https://feeds.bbci.co.uk/news/world/rss.xml", "World", True), ("https://www.pbs.org/newshour/feeds/rss/headlines", "US", True),
    ("https://www.cbsnews.com/latest/rss/world", "World", True), ("https://www.cbsnews.com/latest/rss/main", "US", False), ("https://abcnews.go.com/abcnews/internationalheadlines", "World", True), ("https://abcnews.go.com/abcnews/usheadlines", "US", False),
    ("https://www.aljazeera.com/xml/rss/all.xml", "World", True), ("https://www.theguardian.com/world/rss", "World", True), ("https://www.theguardian.com/us-news/rss", "US", False), ("https://rss.dw.com/rdf/rss-en-all", "World", True),
    ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "World", True), ("https://rss.nytimes.com/services/xml/rss/nyt/US.xml", "US", False),
    ("https://indianacapitalchronicle.com/feed/", "Indiana", False), ("https://mirrorindy.org/feed/", "Indiana", False), ("https://www.wfyi.org/index.rss", "Indiana", False),
    # M2 (2026-11-01): picture-carrying feeds, each checked live that every item ships a media picture (Sky 8/8 and 10/10, NBC 25/25,
    # BBC US&Canada 28/28, WTHR 40/40, WISH-TV 15/15; PBS 20/20 and Guardian US 33/33 were already here but not read hourly).
    ("https://feeds.skynews.com/feeds/rss/world.xml", "World", True), ("https://feeds.nbcnews.com/nbcnews/public/news", "US", True),
    ("https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml", "US", True),
    ("https://www.wthr.com/feeds/syndication/rss/news", "Indiana", True), ("https://www.wishtv.com/feed/", "Indiana", True),
]
NON_NEWS = re.compile(r"World Bank|IMF|OECD|Our World|GDELT|Wikipedia|Wikidata|ECB|Open States|NWS|FEMA|CDC|Census|Stooq|Treasury|CoinGecko|SEC EDGAR|NASA EONET|NOAA SWPC|CISA|Polymarket|Metaculus|GDACS|Kyodo|NHK|IFES", re.I)
WIRE_RE = re.compile(r"\((AP|Reuters|AFP)\)|\b(Associated Press|Reuters|Agence France-Presse|AFP)\b")

HOURLY_NEWS_URLS = {  # the fast feeds the hourly run reads for breaking-capable news (NPR is blocked and skipped)
    "https://feeds.bbci.co.uk/news/world/rss.xml", "https://www.aljazeera.com/xml/rss/all.xml", "https://www.theguardian.com/world/rss",
    "https://www.cbsnews.com/latest/rss/world", "https://www.cbsnews.com/latest/rss/main", "https://abcnews.go.com/abcnews/internationalheadlines", "https://abcnews.go.com/abcnews/usheadlines",
    "https://rss.dw.com/rdf/rss-en-all", "https://www.france24.com/en/rss", "https://rss.politico.com/politics-news.xml", "https://thehill.com/news/feed/", "https://indianacapitalchronicle.com/feed/",
    # M2: picture-carrying feeds (World / US / Indiana), verified to ship media pictures
    "https://www.pbs.org/newshour/feeds/rss/headlines", "https://www.theguardian.com/us-news/rss", "https://feeds.skynews.com/feeds/rss/world.xml",
    "https://feeds.nbcnews.com/nbcnews/public/news", "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
    "https://www.wthr.com/feeds/syndication/rss/news", "https://www.wishtv.com/feed/"}

def load_feed_specs(data_dir):
    specs, seen = [], set()
    def add(url, region, hourly):
        if url in seen:
            return
        seen.add(url)
        hourly = url in HOURLY_NEWS_URLS
        h = urlparse(url).netloc
        pid, pname = PUB_BY_HOST.get(h, (re.sub(r"[^a-z0-9]", "", h.replace("www.", "").split(".")[0]), h))
        specs.append({"id": "news_" + sha(url, 6), "url": url, "region": region, "hourly": hourly, "pub": pid, "pubname": pname})
    for url, region, hourly in FEEDS_CORE:
        add(url, region, hourly)
    p = os.path.join(data_dir, "extra_feeds.txt")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            parts = line.strip().split("|")
            if len(parts) != 3 or NON_NEWS.search(parts[1]) or re.search(r"/api/|\.json|sparql|\.csv|format=json", parts[2]):
                continue
            add(parts[2], parts[0], parts[0] in ("World",) and False)
    return specs

def make_feed_source(fs):
    def fn(s):
        r = s.get(fs["url"], timeout=(CONNECT_T, 20), cap=3_000_000)
        if not r.ok:
            return
        fp = feedparser.parse(r.body)
        ft = s.ctx.now_iso(); now = s.ctx.now; k = 0
        for e in fp.entries[:70]:
            t = parse_time(e.get("published") or e.get("updated") or e.get("created"))
            title = strip_tags(e.get("title", ""))
            link = e.get("link") or ""
            if not t or not title or not link or (now - t).total_seconds() > 72 * 3600 or t > now + dt.timedelta(hours=2):
                continue
            summ = strip_tags(e.get("summary", "") or e.get("description", ""), 400)
            raw_links = re.findall(r'href="(https?://[^"]+)"', e.get("summary", "") or "")
            s.ctx.articles.append({"title": title, "summary": summ, "url": link, "host": host_of(link), "pub": fs["pub"], "pubname": fs["pubname"], "t": t, "region": fs["region"], "cites": sorted({host_of(u) for u in raw_links}), "fetched": ft, "img": imgfeeds.entry_image(e),
                                   "receipt": {"feed": fs["url"], "feed_at": ft, "feed_title": e.get("title", ""), "feed_summary": summ, "item_sha256": raw_item_hash(e), "item_body": raw_item_body(e), "page": "not_checked", "page_at": None, "page_http": None, "note": ""}})
            k += 1
        s.n(k)
        if k == 0 and fp.entries:
            newest = max((parse_time(e.get("published") or e.get("updated")) or dt.datetime(1970, 1, 1, tzinfo=UTC)) for e in fp.entries)
            s.error = f"feed parsed ({len(fp.entries)} entries) but none inside the 72 h window; newest {iso(newest)}"; s.soft = True
        elif k == 0:
            s.error = "feed parsed with 0 entries"; s.soft = True
    return fn


# ------------------------------------------------------------------ news clustering
STOP = set("the a an and or of to in on for with at by from as is are was were be been it its this that these those after before over under into amid says say said new news live updates update more than has have had will would could may might not but his her their they he she we you us our about up out off who what when where why how during against between while also just year years day days week one two three first last back year-old".split())

def stem(w):
    for suf in ("ing", "ed", "es", "s"):
        if w.endswith(suf) and len(w) - len(suf) >= 4:
            return w[: -len(suf)]
    return w

def toks(s):
    out = set()
    for w in re.findall(r"[a-z0-9][a-z0-9'\-]*", (s or "").lower()):
        w = re.sub(r"('s|s')$", "", w.strip("'-"))
        if len(w) < 3 or w in STOP:
            continue
        out.add(stem(w))
    return out

EVENT_WINDOW_H = 18      # two articles can only be the same event if published this close together
EVENT_SPAN_H = 30        # and a whole cluster may not stretch wider than this
LOC_RE = re.compile(r"\b([A-Z][\w'\u2019]+(?:[ -][A-Z][\w'\u2019]+)?) (Region|Province|Governorate|District|County|Prefecture|Oblast|Canton)\b", re.I)
CAPW_RE = re.compile(r"[A-Z][A-Za-z\u00C0-\u017F'\u2019]+")

def _cap_words(text):
    """capitalised words that are not the first word of a sentence; hyphen compounds keep the first part (Saudi-led -> Saudi)."""
    out = []
    for m in CAPW_RE.finditer(text or ""):
        i = m.start()
        before = text[:i].rstrip()
        if i == 0 or before == "" or before[-1] in ".!?:\u201c\"(" :
            continue
        out.append(m.group(0).replace("\u2019", "'").split("'")[0].lower())
    return out

def proper_set(texts):
    """Words that behave like names in THIS corpus: seen capitalised mid-sentence and (almost) never in lower case anywhere. Countries and states always count."""
    cap, low = Counter(), Counter()
    for t in texts:
        for w in _cap_words(t):
            cap[w] += 1
        for w in re.findall(r"\b[a-z][a-z']+\b", t or ""):
            low[w] += 1
    pr = {w for w, c in cap.items() if len(w) >= 4 and low[w] * 4 <= c and w not in STOP}
    for c in list(COUNTRIES) + list(US_STATES.values()):
        for w in re.findall(r"[A-Za-z]+", c):
            if len(w) >= 4:
                pr.add(w.lower())
    return pr

def locators(text):
    """(name, kind) pairs such as ('eastern', 'region') or ('riyadh', 'region'): the named sub-place an article says the event happened in."""
    return {(m.group(1).lower().split()[0].split("-")[0], m.group(2).lower()) for m in LOC_RE.finditer(text or "")}

def event_conflict(ta, sa, tb, sb, proper):
    """True when two articles name different places/actors for what only looks like one event. Prefers under-merging."""
    A, B = (ta or "") + ". " + (sa or "")[:300], (tb or "") + ". " + (sb or "")[:300]
    la, lb = locators(A), locators(B)
    for n1, k1 in la:
        for n2, k2 in lb:
            if k1 == k2 and n1 != n2:
                return True          # "Eastern Region" vs "Riyadh region": two different places
    fa = {w for w in re.findall(r"[a-z]{4,}", A.lower().replace("-", " "))}
    fb = {w for w in re.findall(r"[a-z]{4,}", B.lower().replace("-", " "))}
    Ta = {w for w in _cap_words(". " + ta) if w in proper}; Tb = {w for w in _cap_words(". " + tb) if w in proper}
    # sentence-case titles put the first word out of reach of _cap_words; add a leading entity word if it is a known name
    for t, T in ((ta, Ta), (tb, Tb)):
        f = re.match(r"[A-Za-z\u00C0-\u017F]+", t or "")
        if f and f.group(0).lower() in proper and f.group(0)[0].isupper():
            T.add(f.group(0).lower())
    ua, ub = Ta - fb, Tb - fa
    if ua and ub:
        return True                  # each names a place/actor the other never mentions
    if Ta and Tb and not ((Ta | Tb) & fa & fb):
        return True                  # no place/actor in common at all
    return False

CL_O = float(os.environ.get("WM_CL_O", "0.42"))
CL_W = float(os.environ.get("WM_CL_W", "6.0"))
TWO_LEVEL = {"co.uk", "com.au", "co.jp", "com.cn", "co.in", "org.uk", "com.br", "co.za"}
def reg_domain(h):
    p = h.split(".")
    if len(p) >= 3 and ".".join(p[-2:]) in TWO_LEVEL:
        return ".".join(p[-3:])
    return ".".join(p[-2:]) if len(p) >= 2 else h

PRIORITY = ["bbc", "guardian", "aljazeera", "nyt", "pbs", "cbs", "abc", "dw", "france24", "cbc", "wsj", "politico", "thehill", "un"]

def build_clusters(arts, now):
    # dedupe by url
    seen, A = set(), []
    for a in sorted(arts, key=lambda a: a["t"]):
        if a["url"] in seen:
            continue
        seen.add(a["url"]); A.append(a)
    N = len(A)
    for a in A:
        a["T"] = toks(a["title"]); a["C"] = a["T"] | toks(a["summary"][:200])
    proper = proper_set([a["title"] + ". " + (a["summary"] or "")[:300] for a in A])
    PROPER_BASE = proper_set([])
    df = Counter(tk for a in A for tk in a["C"])
    idf = {tk: math.log((N + 1) / (c + 1)) for tk, c in df.items()}
    def w(S_): return sum(idf[x] for x in S_)
    def sim(a, b):
        sh = a["T"] & b["T"]
        if len(sh) >= 2:
            sw = w(sh)
            o = sw / max(0.1, min(w(a["T"]), w(b["T"])))
            if o >= CL_O and sw >= CL_W:
                return o
        shc = a["C"] & b["C"]
        if len(shc) >= 5:
            sw = w(shc); o = sw / max(0.1, min(w(a["C"]), w(b["C"])))
            if o >= 0.42 and sw >= 14.0 and len(a["T"] & b["T"]) >= 1:
                return o * 0.9
        return 0.0
    clusters, index = [], defaultdict(set)
    for a in A:
        cand = Counter()
        for tk in a["C"]:
            if df[tk] <= 60:
                for cid in index[tk]:
                    cand[cid] += idf[tk]
        best = None
        for cid, _ in cand.most_common(10):
            c = clusters[cid]
            if (a["t"] - c["last"]).total_seconds() > EVENT_WINDOW_H * 3600 or (a["t"] - c["first"]).total_seconds() > EVENT_SPAN_H * 3600 or len(c["m"]) >= 40:
                continue
            mem = c["m"][:4] + c["m"][-8:]
            # any member naming a different place/actor vetoes the join; check ALL members, under both the
            # corpus-learned names and the corpus-free country/state set (the test's rule), so no pair can conflict
            if any(event_conflict(a["title"], a["summary"], m["title"], m["summary"], pr) for m in c["m"] for pr in (proper, PROPER_BASE)):
                continue
            sc = max(sim(a, m) for m in mem)
            if sc > 0 and (best is None or sc > best[0]):
                best = (sc, cid)
        if best:
            c = clusters[best[1]]; c["m"].append(a); c["last"] = max(c["last"], a["t"])
            cid = best[1]
        else:
            cid = len(clusters); clusters.append({"m": [a], "last": a["t"], "first": a["t"]})
        for tk in a["C"]:
            index[tk].add(cid)
    # final guard: re-check every cluster with the same text the published doc carries (title + 160-char summary)
    # and the corpus-free name set; any member conflicting with the cluster's first member leaves as its own story
    out = []
    for c in clusters:
        head, keep, loose = c["m"][0], [c["m"][0]], []
        for m in c["m"][1:]:
            if any(event_conflict(k["title"], (k["summary"] or "")[:160], m["title"], (m["summary"] or "")[:160], pr)
                   for k in keep for pr in (proper, PROPER_BASE)):
                loose.append(m)
            else:
                keep.append(m)
        out.append({"m": keep, "last": max(x["t"] for x in keep), "first": min(x["t"] for x in keep)})
        out.extend({"m": [m], "last": m["t"], "first": m["t"]} for m in loose)
    return out

def origin_of(a, by_host):
    if a["pub"] in ("stateline", "icc", "statesnewsroom"):
        return "network:statesnewsroom"
    m = WIRE_RE.search(a["title"] + " " + a["summary"])
    if m:
        return "wire:" + (m.group(1) or m.group(2)).lower().replace("associated press", "ap").replace("agence france-presse", "afp")
    for h in a["cites"]:
        rd = reg_domain(h)
        if rd in by_host and by_host[rd] != a["pub"]:
            return "pub:" + by_host[rd]
    return "pub:" + a["pub"]

_R = {"Europe": "Austria Belarus Belgium Bulgaria Cyprus Czech Republic Denmark Finland France Germany Greece Hungary Iceland Ireland Italy Kosovo Moldova Netherlands Norway Poland Portugal Romania Serbia Slovakia Spain Sweden Switzerland Ukraine Russia United Kingdom Britain Armenia Azerbaijan",
      "Asia": "Afghanistan Bangladesh Cambodia China India Indonesia Japan Kazakhstan Kyrgyzstan Laos Malaysia Mongolia Myanmar Nepal North Korea Pakistan Philippines Singapore South Korea Sri Lanka Taiwan Tajikistan Thailand Turkmenistan Uzbekistan Vietnam",
      "MidEast": "Gaza Iran Iraq Israel Jordan Kuwait Lebanon Palestine Qatar Saudi Arabia Syria Turkey United Arab Emirates West Bank Yemen",
      "Africa": "Algeria Angola Burkina Faso Burundi Cameroon Chad Democratic Republic of the Congo DR Congo Djibouti Egypt Eritrea Ethiopia Ghana Kenya Libya Madagascar Mali Morocco Mozambique Niger Nigeria Rwanda Senegal Somalia South Africa South Sudan Sudan Tanzania Tunisia Uganda Zambia Zimbabwe",
      "LatAm": "Argentina Bolivia Brazil Chile Colombia Costa Rica Cuba Dominican Republic Ecuador El Salvador Guatemala Haiti Honduras Jamaica Mexico Nicaragua Panama Peru Uruguay Venezuela Bahamas",
      "Americas": "Canada", "Oceania": "Australia New Zealand Fiji Papua New Guinea Solomon Islands Tonga Vanuatu"}
REGION_OF = {c: r for r, names in _R.items() for c in sorted(COUNTRIES, key=len, reverse=True) if re.search(r"(^| )" + re.escape(c) + r"( |$)", names) and (c in names)}

# ------------------------------------------------------------------ receipts: engine-time proof for every cited news item
BLOCK_CODES = (403, 406, 451)
PAGE_STATES = ("matched", "blocked", "mismatch", "unreadable", "not_checked")   # unreadable = asked (twice if it did not answer) and no text to compare; not_checked = not asked (hourly beats only)

def clock_et(t):
    l = t.astimezone(TZ)
    return f"{l.hour % 12 or 12}:{l.minute:02d} {'a.m.' if l.hour < 12 else 'p.m.'}"

def _norm(x):
    return re.sub(r"[^a-z0-9]+", " ", html.unescape(strip_tags(x or "")).lower()).strip()

def rung_of(r, summary=""):
    """The Proof Ladder. R1 page matched | R2 feed headline + feed summary matched (page not readable) | R3 feed headline only | R4 unverified."""
    if r.get("page") == "matched":
        return "R1"
    if r.get("page") == "mismatch" or not (r.get("feed_title") or "").strip() or not str(r.get("feed", "")).startswith("http"):
        return "R4"
    fs = _norm(r.get("feed_summary")); sm = _norm(re.sub(r"[\u2026.]+\s*$", "", summary or ""))
    if len(sm) >= 20 and fs and sm in fs:
        return "R2"
    return "R3"

def proof_line(r, rung):
    fa = parse_time(r.get("feed_at")); fat = clock_et(fa) if fa else "fetch time"
    pa = parse_time(r.get("page_at"))
    if rung == "R4":
        return "UNVERIFIED: no matching publisher proof recorded."
    if rung == "R1":
        return f"Page read at {clock_et(pa) if pa else fat} and carries the headline."
    pg = {"blocked": "Publisher page blocked automated reading; feed copy kept.", "unreadable": "page gives the engine no readable text.", "not_checked": "page not checked in the hourly beat."}.get(r.get("page"), "page not compared.")
    if rung == "R2":
        return f"Headline and summary match the publisher's feed, {fat}; {pg}"
    return f"Headline matches the publisher's feed, {fat}; no matching summary to compare; {pg}"

def receipt_out(r, summary=""):
    """Public shape: feed proof first (publisher's own RSS item), then the one polite page check, then the rung computed from both."""
    fa = parse_time(r["feed_at"]); at = clock_et(fa) if fa else "fetch time"
    note = r.get("note") or f"page not read; headline from its own feed at {at}"
    rg = rung_of(r, summary)
    ex = excerpt_of(r["feed_title"], r.get("feed_summary", ""))
    sm_ = _norm(re.sub(r"[\u2026.]+\s*$", "", summary or "")); fs_ = _norm(r.get("feed_summary"))
    drift = bool(len(sm_) >= 20 and fs_ and sm_ not in fs_)      # r20: our stored summary differs from the feed item
    return {"summary_drift": drift, "feed": r["feed"], "feed_at": r["feed_at"], "feed_title": r["feed_title"], "feed_summary": r.get("feed_summary", ""), "page": r["page"], "page_at": r.get("page_at"), "page_http": r.get("page_http"), "note": note,
            "rung": rg, "proof": proof_line(r, rg),
            "excerpt": ex, "sha256": hashlib.sha256(ex.encode("utf-8")).hexdigest(), "item_sha256": r.get("item_sha256") or "", "fetched_at": r["feed_at"],
            **({"item_body": r["item_body"], "body_sha256": hashlib.sha256(r["item_body"].encode("utf-8")).hexdigest()} if rg == "R1" and r.get("item_body") else {})}

# R19 "The receipt carries its own proof": a frozen excerpt (title + summary, <= 400 chars), its sha256, the sha256 of the raw feed item, fetched_at and the feed url.
def excerpt_of(title, summary):
    t = html.unescape(strip_tags(title or "")).strip(); sm = html.unescape(strip_tags(summary or "")).strip()
    return ((t + " | " + sm) if sm else t)[:400]

BODY_CAP = 1500
def raw_item_body(e):
    """R22 L-DATA: the feed item body as archived (title, summary or description, link, dates; canonical JSON, at most BODY_CAP chars)."""
    sm = str(e.get("summary", "") or ""); ds = str(e.get("description", "") or "")
    o = {"title": str(e.get("title", "") or ""), "summary": sm, "link": str(e.get("link", "") or ""), "published": str(e.get("published", "") or ""), "updated": str(e.get("updated", "") or "")}
    if ds and ds != sm:
        o["description"] = ds
    return json.dumps(o, sort_keys=True, ensure_ascii=False)[:BODY_CAP]

def raw_item_hash(e):
    raw = json.dumps({k: str(e.get(k, "")) for k in ("title", "summary", "description", "link", "published", "updated")}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def reprove_receipt(r, live_text=None):
    """Re-verify a stored news receipt. Hash against the stored excerpt first; then the live feed text if reachable.
    Returns (ok, state): proved_live | proved_at_fetch_rolled_off | proved_at_fetch | hash_mismatch | no_excerpt."""
    ex = r.get("excerpt")
    if not ex or not r.get("sha256"):
        return False, "no_excerpt"
    if hashlib.sha256(ex.encode("utf-8")).hexdigest() != r["sha256"]:
        return False, "hash_mismatch"
    if live_text is None:
        return True, "proved_at_fetch"
    title = ex.split(" | ")[0]
    if _norm(title) and _norm(title) in _norm(live_text):
        return True, "proved_live"
    return True, "proved_at_fetch_rolled_off"

def unify_receipts(docs):
    """One status per URL: every receipt for the same article url carries the same page state (the best proof wins, R1 over R2 over R3), so no item is both 'matched' and 'blocked'."""
    byurl = {}
    def walk(o):
        if isinstance(o, dict):
            rc = o.get("receipt")
            if isinstance(rc, dict) and "page" in rc and str(o.get("url", "")).startswith("http"):
                byurl.setdefault(o["url"], []).append(rc)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    for _c, _d, data in docs:
        walk(data)
    n = 0
    for u, rs in byurl.items():
        if len({(r["page"], r.get("rung")) for r in rs}) <= 1:
            continue
        best = min(rs, key=lambda r: (r.get("rung") or "R9", r.get("page") != "matched"))
        for r in rs:
            if r is not best:
                for k in ("page", "page_at", "page_http", "note", "rung", "proof"):
                    r[k] = best.get(k)
                n += 1
    return n

WORD_RE = re.compile(r"[a-z0-9\u00c0-\u024f]{3,}")

def page_words(html_text):
    t = re.sub(r"(?is)<(script|style|noscript)\b.*?</\1>", " ", html_text)
    return set(WORD_RE.findall(html.unescape(re.sub(r"(?s)<[^>]+>", " ", t)).lower()))

def judge_page(feed_title, body_text):
    """matched: the page carries the headline's words; mismatch: a real page that does not; None: cannot tell (not_checked)."""
    tw = set(WORD_RE.findall(html.unescape(strip_tags(feed_title)).lower())) - STOP
    if len(tw) < 3:
        return None
    pw = page_words(body_text)
    if len(pw) < 120:
        return None            # script shell or consent wall: nothing to judge
    frac = len(tw & pw) / len(tw)
    if frac >= 0.6:
        return "matched"
    if frac < 0.35:
        return "mismatch"
    return None

BLOCKED_NOTE = "Publisher page blocked automated reading; feed copy kept"
CHALLENGE_RE = re.compile(r"(?i)<title>\s*(just a moment|attention required|access denied|are you a robot|robot check|security check|please verify|verify you are human|pardon our interruption|one more step|request rejected)|cf-challenge|challenge-platform|cf_chl_|/cdn-cgi/challenge|px-captcha|captcha-delivery|geo\.captcha|datadome|g-recaptcha|h-captcha|hcaptcha\.com|perimeterx|_incapsula_resource|distil_r_captcha|enable javascript and cookies to continue|checking your browser before")

def is_challenge(status, body):
    """R22 L-DATA: a bot wall / challenge page is 'blocked', never matched or refuted. 202/429/503 with a challenge body, or a 200 page that is only a challenge (small and carrying a challenge marker)."""
    b = body or ""
    if not CHALLENGE_RE.search(b[:60000]):
        return False
    return status in (202, 429, 503) or len(page_words(b)) < 400

def check_page(F, a, now):
    """One polite GET of the article page. 403/406/451 is recorded as blocked and never retried or worked around."""
    r = a["receipt"]
    resp = F.get(a["url"], headers={"Accept": "text/html"}, timeout=(CONNECT_T, 12), cap=700_000, tries=1, gap=1.0)
    at = clock.now(); r["page_at"] = iso(at); r["page_http"] = resp.status or None
    fat = clock_et(parse_time(r["feed_at"]) or at)
    body = resp.text() if resp.ok or resp.status in (202, 429, 503) else ""
    if resp.status in BLOCK_CODES or is_challenge(resp.status, body):
        r["page"] = "blocked"; r["note"] = BLOCKED_NOTE
    elif resp.ok:
        v = judge_page(r["feed_title"], body)
        if v == "matched":
            r["page"] = "matched"; r["note"] = f"page read at {clock_et(at)} and carries the headline"
        elif v == "mismatch":
            r["page"] = "mismatch"; r["note"] = f"page read at {clock_et(at)} does not carry the headline; dropped"
        else:
            r["page"] = "not_checked"; r["note"] = f"page read at {clock_et(at)} but could not be compared; headline from its own feed at {fat}"
    else:
        r["page"] = "not_checked"; r["note"] = f"page not readable now ({resp.status or 'no answer'}); headline from its own feed at {fat}"
    return r["page"]

def apply_receipts(F, clusters, cand, now, workers=10, seconds=170, cap=300):
    """cand: indices of clusters that will be written. One page check per distinct publisher per cluster (the cited sources).
    Mismatching articles are removed from their cluster (and the cluster from the list if it empties). Returns (stats, clusters)."""
    jobs, seen = [], set()
    for ci in cand:
        per = {}
        for a in sorted(clusters[ci]["m"], key=lambda a: a["t"]):
            per.setdefault(reg_domain(a["host"]), a)
        for a in list(per.values())[:8]:
            if a["url"] not in seen:
                seen.add(a["url"]); jobs.append(a)
    jobs = jobs[:cap]
    deadline = time.time() + seconds
    def one(a):
        if time.time() > deadline:
            return
        try:
            check_page(F, a, now)
        except Exception as e:
            a["receipt"]["note"] = "page check failed: " + scrub(type(e).__name__)[:60]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, jobs))
    stats = Counter(a["receipt"]["page"] for a in jobs)
    stats["jobs"] = len(jobs)
    drop = {a["url"] for a in jobs if a["receipt"]["page"] == "mismatch"}
    out = []
    for c in clusters:
        if drop:
            c["m"] = [a for a in c["m"] if a["url"] not in drop]
        if c["m"]:
            out.append(c)
    stats["dropped"] = len(drop)
    return dict(stats), out

def emitted_articles(it):
    """Every article a news item will print: the 12 listed members (cluster doc) and the earliest article of each cited publisher."""
    m = it["_cluster"]["members"]; per = {}
    for a in m:
        per.setdefault(reg_domain(a["host"]), a)
    out, seen = [], set()
    for a in m[:12] + list(per.values())[:8]:
        if a["url"] not in seen:
            seen.add(a["url"]); out.append(a)
    return out

def settle_items(F, items, now, workers=10, seconds=240):
    """Clear every not_checked: ask the page once if never asked, ask once more if it did not answer (status 0 or 5xx), then call it unreadable.
    403/406/451 stay blocked and are never asked twice. A page that is read and does not carry the headline is removed from its cluster. Returns ids of items left empty."""
    arts = {}
    for it in items:
        for a in emitted_articles(it):
            arts.setdefault(a["url"], a)
    deadline = time.time() + seconds
    def one(a):
        r = a["receipt"]
        try:
            for _ in range(2):
                if r["page"] != "not_checked" or time.time() > deadline:
                    break
                if r["page_at"] is not None and not (r["page_http"] in (None, 0) or (r["page_http"] or 0) >= 500):
                    break      # asked and answered but nothing to compare: a second GET would give the same page
                check_page(F, a, now)
        except Exception as e:
            r["note"] = "page check failed: " + scrub(type(e).__name__)[:60]
        if r["page"] == "not_checked":
            r["page"] = "unreadable"
            r["note"] = f"page gives the engine no readable text{'' if r['page_at'] else ' (check ran out of time)'}; headline from its own feed at {clock_et(parse_time(r['feed_at']) or now)}"
    todo = [a for a in arts.values() if a["receipt"]["page"] == "not_checked"]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, todo))
    bad = {u for u, a in arts.items() if a["receipt"]["page"] == "mismatch"}
    empty = set()
    for it in items:
        c = it["_cluster"]
        if bad:
            c["members"] = [a for a in c["members"] if a["url"] not in bad]
        if not c["members"]:
            empty.add(it["id"]); continue
        seal_item(it)
    return empty

def seal_item(it):
    """Rebuild each source receipt from its article (the receipt an item was born with was a snapshot taken before the page check), set the item's rung."""
    c = it["_cluster"]; byurl = {a["url"]: a for a in c["members"]}
    srcs = [s_ for s_ in it["sources"] if s_["url"] in byurl]
    for i, s_ in enumerate(srcs, 1):
        a = byurl[s_["url"]]; s_["n"] = i
        s_["receipt"] = receipt_out(a["receipt"], a["summary"])
    it["sources"] = srcs
    hosts = {reg_domain(a["host"]) for a in c["members"]}
    c["hosts"] = sorted(hosts); it["truth"] = "confirmed" if len(hosts) >= 2 else "single"
    rungs = [s_["receipt"]["rung"] for s_ in srcs if s_.get("receipt")]
    it["rung"] = min(rungs) if rungs else "R4"
    best = next((s_["receipt"] for s_ in srcs if s_.get("receipt", {}).get("rung") == it["rung"]), None)
    it["proof"] = best["proof"] if best else "UNVERIFIED: no matching publisher proof recorded."
    it["_settled"] = True

def ladder_counts(items):
    return {"items": len(items), **{k: sum(1 for i in items if i.get("rung") == k) for k in ("R1", "R2", "R3", "R4")}}

def cluster_to_item(c, now):
    m = sorted(c["m"], key=lambda a: a["t"])
    by_host = {reg_domain(a["host"]): a["pub"] for a in m}
    origins = {origin_of(a, by_host) for a in m}
    pubs = {a["pub"] for a in m}; hosts = {reg_domain(a["host"]) for a in m}
    truth = "confirmed" if len(hosts) >= 2 else "single"   # CONTRACT: confirmed = at least 2 distinct publisher hosts (www. stripped, registrable domain); single = exactly 1
    lead = min(m, key=lambda a: (PRIORITY.index(a["pub"]) if a["pub"] in PRIORITY else 99, a["t"]))
    text = lead["title"] + " " + lead["summary"]
    # one source per distinct publisher (earliest article), cap 8
    per, srcs = {}, []
    for a in m:
        per.setdefault(reg_domain(a["host"]), a)
    for i, a in enumerate(list(per.values())[:8], 1):
        s_ = src(i, a["pubname"], a["url"], a["fetched"], a["title"])
        if a.get("receipt"):
            s_["receipt"] = receipt_out(a["receipt"], a["summary"])
        srcs.append(s_)
    st = find_state(text); ctry = find_country(text)
    if LOCAL_RE.search(text):
        layer, admin, pn = "local", "Indiana", "Indianapolis"
    elif IN_RE.search(text) or lead["region"] == "Indiana":
        layer, admin, pn = "indiana", "Indiana", "Indiana"
    elif st:
        layer, admin, pn = "us", st, st
    elif ctry and ctry != "Canada" or (ctry == "Canada" and lead["region"] != "US"):
        layer, admin, pn = ("us" if ctry == "United States" else "world"), ctry, ctry
    elif lead["region"] == "US" or re.search(r"\bU\.S\.|\bUnited States\b|\bAmerican\b|\bWashington\b", text):
        layer, admin, pn = "us", "United States", "United States"
    else:
        layer, admin, pn = "world", None, "World"
    if layer == "world" and ctry is None:
        pn = {"Europe": "Europe", "Asia": "Asia", "MidEast": "Middle East", "Africa": "Africa", "LatAm": "Latin America"}.get(lead["region"], "World")
    regs = Counter(a["region"] for a in m)
    region = REGION_OF.get(ctry) if ctry and ctry in REGION_OF else regs.most_common(1)[0][0]
    if st:
        region = "US:" + st
    if layer in ("local", "indiana"):
        region = "Indiana"
    rights = bool(RIGHTS_RE.search(lead["title"]))
    sev = min(1.0, 0.12 + 0.1 * len(origins) + (0.25 if rights else 0.0))
    names = sorted({a['pubname'] for a in m})
    who = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    lead_sum = lead["summary"] or ""
    title_s = lead["title"] if lead["title"][-1] in ".!?" else lead["title"] + "."
    plain = f"{title_s} Reported by {who}." + (f" {lead['pubname']} adds: {lead_sum}" if lead_sum and lead_sum.rstrip(". ").lower() != lead["title"].rstrip(". ").lower() else "")
    it = mk_item("news" + min(a["url"] for a in m), "news", lead["title"], plain, m[0]["t"], mk_place(pn, admin=admin), sev, layer, truth, srcs, domain_for(lead["title"], 23, lead["summary"][:160]), prov="computed", reach=reach_tags(text),
                 first_seen=m[0]["t"], updated=m[-1]["t"])
    it["_cluster"] = {"members": m, "region": region, "independent": len(origins), "hosts": sorted(hosts)}
    return it

def best_rung(m):
    rs = [receipt_out(a["receipt"], a["summary"]) for a in m if a.get("receipt")]
    if not rs:
        return "R4", "UNVERIFIED: no matching publisher proof recorded."
    b = min(rs, key=lambda r: r["rung"])
    return b["rung"], b["proof"]

def cluster_doc_entry(it, max_items=12, summ=160):
    c = it["_cluster"]; m = c["members"]
    return {"id": it["id"], "title": it["title"], "plain": it["plain"], "publishers": c["hosts"], "n": len(m), "truth": it["truth"],
            "rung": it.get("rung") or best_rung(m)[0],
            "proof": it.get("proof") or best_rung(m)[1],
            "items": [dict({"title": a["title"], "publisher": a["pubname"], "url": a["url"], "t": iso(a["t"]), "summary": clip(a["summary"], summ)}, **({"receipt": receipt_out(a["receipt"], a["summary"])} if a.get("receipt") else {})) for a in m[:max_items]],
            "region": c["region"], "domain": it["domain"], "layer": it["layer"], "t": iso(m[-1]["t"]), "touches_you": it.get("touches_you")}


# ------------------------------------------------------------------ static docs: atlas, gaps, glossary, places
def load_json(data_dir, name):
    return json.load(open(os.path.join(data_dir, name), encoding="utf-8"))

def build_atlas(data_dir):
    rows = load_json(data_dir, "Everything_to_know.json")[1:]
    domains, order, leaves = {}, [], defaultdict(list)
    for r in rows:
        m = re.match(r"(\d+)\s+(.*)", r[1]); did = f"d{int(m.group(1)):02d}"; dname = m.group(2)
        if did not in domains:
            domains[did] = {"id": did, "name": dname, "topics": [], "_t": {}}; order.append(did)
        d = domains[did]
        if r[2] not in d["_t"]:
            tid = f"{did}-t{len(d['topics']) + 1:02d}"
            d["_t"][r[2]] = {"id": tid, "name": r[2], "leaf_ids": []}; d["topics"].append(d["_t"][r[2]])
        d["_t"][r[2]]["leaf_ids"].append(r[0])
        leaves[did].append({"id": r[0], "name": r[3], "what": r[5], "why": r[6], "watch": r[7], "source": r[8], "status": r[9], "learn": r[10], "gap": r[12]})
    index = {"domains": [{"id": d, "name": domains[d]["name"], "topics": domains[d]["topics"]} for d in order]}
    return index, leaves

def link_atlas(items, leaves):
    lt = {d: [(l["id"], {t for t in toks(l["name"]) if len(t) >= 5}) for l in ls] for d, ls in leaves.items()}
    for it in items:
        cand = lt.get(it["domain"], [])
        T = {t for t in toks(it["title"] + " " + it["plain"][:200]) if len(t) >= 5}
        scored = sorted(((len(T & lt_[1]), lid) for lid, lt_ in [(c[0], c) for c in cand]), reverse=True)
        it["atlas"] = [lid for sc, lid in scored[:2] if sc >= 1]

def build_gaps(data_dir):
    rows = load_json(data_dir, "Gaps.json")[1:]
    return {"gaps": [{"id": r[0], "text": r[1], "source": r[6], "surfaces": r[3]} for r in rows]}

def page_text(raw):
    t = re.sub(r"(?is)<(script|style|noscript|nav|header|footer|aside)\b.*?</\1>", " ", raw)
    t = re.sub(r"(?i)</(p|li|h\d|div|tr)>|<br\s*/?>", "\n", t)
    t = html.unescape(re.sub(r"(?s)<[^>]+>", " ", t))
    return [re.sub(r"\s+", " ", ln).strip() for ln in t.split("\n") if ln.strip()]

def pick_quote(lines, term, plain):
    """A sentence copied exactly from the page that shares the most words with the term and its plain-words definition (min 3 shared)."""
    want = {w for w in WORD_RE.findall((term + " " + plain).lower()) if w not in STOP}
    best, bs = None, 0
    for ln in lines:
        for sn in sentences(ln):
            sn = sn.strip()
            if not 50 <= len(sn) <= 320:
                continue
            sc = len(want & set(WORD_RE.findall(sn.lower())))
            if term.lower() in sn.lower():
                sc += 2
            if sc > bs:
                best, bs = sn, sc
    return best if bs >= 3 else None

def build_glossary(data_dir, F=None, now=None):
    rows = load_json(data_dir, "Foundations.json")[1:]
    pages = {}
    def get_lines(url):
        if url in pages:
            return pages[url]
        if not F or not url.startswith("http"):
            pages[url] = ("not_checked", None, None); return pages[url]
        r = F.get(url, headers={"Accept": "text/html"}, timeout=(CONNECT_T, 15), cap=1_500_000, tries=1, gap=1.0)
        body = r.text() if r.ok or r.status in (202, 429, 503) else ""
        if r.status in BLOCK_CODES or is_challenge(r.status, body):
            pages[url] = ("blocked", None, iso(clock.now()))
        elif r.ok:
            pages[url] = ("read", page_text(body), iso(clock.now()))
        else:
            pages[url] = ("not_checked", None, iso(clock.now()))
        return pages[url]
    try:
        alt = load_json(data_dir, "gloss_alt.json")
    except Exception:
        alt = {}
    out = []
    for r in rows:
        d = {"term": r[1], "plain": r[2], "source": r[4]}
        st, lines, at = get_lines(r[4])
        q = pick_quote(lines, r[1], r[2]) if st == "read" else None
        d["quote"] = q
        d["quote_status"] = "found" if q else ("not_found" if st == "read" else st)
        d["quote_at"] = at
        if not q:      # a second source page that says it in its own words: the quote must be a whole sentence copied from that page
            for cand in alt.get(r[1], []):
                st2, lines2, at2 = get_lines(cand["url"])
                if st2 != "read":
                    continue
                hit = next((sn.strip() for ln in lines2 for sn in sentences(ln) if re.search(cand["re"], sn, re.I) and 40 <= len(sn.strip()) <= 360), None)
                if hit:
                    d.update(quote=hit, quote_status="found", quote_at=at2, source_original=r[4], source=cand["url"]); break
        d["glossed"] = d["quote_status"] == "found"      # only a found quote may print a gloss; any other term stays plain words
        if not d["glossed"]:
            d["unverified"] = True
        out.append(d)
    return {"terms": out, "glossed": sum(1 for t in out if t["glossed"]), "unverified": sum(1 for t in out if not t["glossed"])}

PLACES = [("marion", "Marion County", "Marion County"), ("indiana", "Indiana", "Indiana"), ("illinois", "Illinois", "Illinois"), ("ohio", "Ohio", "Ohio"), ("kentucky", "Kentucky", "Kentucky"), ("michigan", "Michigan", "Michigan"),
          ("california", "California", "California"), ("united_states", "United States", "United States"), ("world", "World", "World"), ("canada", "Canada", "Canada"), ("mexico", "Mexico", "Mexico"), ("china", "China", "China"),
          ("russia", "Russia", "Russia"), ("iran", "Iran", "Iran"), ("saudi_arabia", "Saudi Arabia", "Saudi Arabia"), ("ukraine", "Ukraine", "Ukraine"), ("israel", "Israel", "Israel")]
PADMIN = {"marion": "Indiana", "indiana": "Indiana", "united_states": "United States", "saudi_arabia": "Saudi Arabia"}

def build_places(ctx, data_dir, all_items, clusters_items):
    prow = {r[0]: r for r in load_json(data_dir, "Places.json")[1:]}
    docs = []
    for pid, name, key in PLACES:
        row = prow.get(key)
        admin = PADMIN.get(pid, name)
        def hit(it):
            a = (it["place"] or {}).get("admin")
            if pid == "marion": return it["layer"] == "local"
            if pid == "indiana": return it["layer"] in ("local", "indiana")
            if pid == "united_states": return it["layer"] in ("local", "indiana", "us")
            if pid == "world": return it["layer"] == "world"
            return a == admin
        alerts_n = sum(1 for it in all_items if hit(it))
        news_n = sum(1 for it in clusters_items if hit(it))
        nums = list(ctx.pnums.get({"united_states": "us"}.get(pid, pid), []))
        if pid == "united_states":
            nums += ctx.pnums.get("united_states", [])
        if pid == "indiana":
            nums += ctx.pnums.get("midwest", [])
        nums.sort(key=lambda n: (n["label"], n["source"], str(n["date"])))   # P1 byte-stable: sources finish in thread order
        heads = ctx.pextra.get(pid, {}).get("heads")
        limit = "none stated"
        if row:
            m = re.search(r"limit:\s*(.*)$", row[3])
            if m: limit = m.group(1).strip()
        who = resolve_head(ctx.pextra.get(pid, {}).get("official"), heads, ctx.fetched_hdr)
        if pid == "world":
            who = {"name": "not applicable (no single head of government)", "source": "Places.txt", "kind": "not applicable"}
        docs.append(("places", pid, {"id": pid, "name": name, "level": (row[1] if row else "unknown"), "who_runs": who, "numbers": nums, "alerts_n": alerts_n, "news_n": news_n,
                                     "why": (row[6] if row else "not stated"), "limits": limit, "gmaps": gmaps(name + (", Indiana" if pid == "marion" else ""))}))
    return docs


# ------------------------------------------------------------------ plate scoring (r20): the engine can read pixels, the page cannot
def fetch_image(F, it, now):
    """Single-event picture (kept for callers): NASA GIBS true colour around the event. The full source set lives in imgfeeds.py."""
    lat, lon = it["place"]["lat"], it["place"]["lon"]
    if lat is None or lon is None:
        return None, "no coordinates"
    return imgfeeds.gibs(F, now, lat, lon, it["kind"] == "fire")


# ------------------------------------------------------------------ brief skeleton
# A break needs end punctuation + whitespace + a capital, digit or quote, and the text before it must not end in a known
# abbreviation or initial ("U.S.", "Gov.", "Jan.", "J."), so "U.S. first lady" and "magnitude 5.1" never split.
ABBR_END = re.compile(r"(?:\b(?:[A-Z]\.){1,3}[A-Z]?\.?|\b(?:Mr|Mrs|Ms|Dr|St|Jr|Sr|Gov|Sen|Rep|Gen|Lt|Col|Sgt|Capt|Prof|No|vs|etc|Inc|Co|Corp|Ltd|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|approx|est|Dept|Mt|Ft|Ave)\.)$")
SENT_BREAK = re.compile(r"[.!?]+[\"')\]]*\s+(?=[\"'(\[]?[A-Z0-9])")

def sentences(t):
    s = re.sub(r"\s+", " ", (t or "")).strip()
    out, start = [], 0
    for m in SENT_BREAK.finditer(s):
        if ABBR_END.search(s[start:m.start() + 1]):
            continue
        out.append(s[start:m.end()].strip()); start = m.end()
    if start < len(s):
        out.append(s[start:].strip())
    return [x for x in out if x]

def first_sentence(t):
    ss = sentences(t)
    x = ss[0] if ss else (t or "").strip()
    return x if x and x[-1] in ".!?" else x + "."

STAKE_WORDS = {3: "touches rights, lives or safety", 2: "moves money: prices, taxes or spending"}

def why_here(it):
    """'Why it is here' as sentences built from the item's facts (touches you / stake, where, who says so), never its headline."""
    out = []
    tu = it.get("touches_you") or {}
    if tu.get("yes"):
        out.append(tu["how"].rstrip(".") + ".")
    elif (it.get("score") or {}).get("stake", 1) >= 2:
        out.append("Stake: " + STAKE_WORDS[it["score"]["stake"]] + ".")
    pl = it.get("place") or {}
    where = (pl.get("name") or pl.get("admin") or "").split(";")[0].strip()
    if where and where.lower() not in ("world", "not stated"):
        out.append(f"Where: {where}.")
    pubs = list(dict.fromkeys(s.get("name") or s.get("host") for s in it.get("sources") or [] if s.get("name") or s.get("host")))
    if it["truth"] == "primary":
        out.append(f"Who says so: {pubs[0] if pubs else 'the issuing body'}, the primary record.")
    elif len(pubs) > 1:
        out.append(f"Who says so: {len(pubs)} independent publishers, including {pubs[0]} and {pubs[1]}.")
    elif pubs:
        out.append(f"Who says so: {pubs[0]} alone, not yet confirmed.")
    return out

def build_brief(date, mode, all_items, health_ok, health_total, now, counts):
    """Rule-built skeleton. bottom_line is the plain sentence of the highest-scoring civic or world item, never run telemetry
    (run telemetry lives in meta/run and meta/health). Distant weather that does not touch him is not eligible."""
    def eligible(it):
        if it["truth"] not in ("primary", "confirmed", "single") or it["kind"] in ("public_inspection", "nomination"):
            return False
        if it["kind"] in WEATHER_KINDS and not (it.get("touches_you") or {}).get("yes"):
            return False
        t = parse_time(it["t"])
        return bool(t and (now - t).total_seconds() <= 47 * 3600)   # recent enough to sit in an emitted wire/events doc
    pool = [it for it in all_items if eligible(it)]
    rank = lambda it: max(it["score"]["total"], it["score"]["breaking"])
    pool.sort(key=lambda it: (rank(it), it["sev"]), reverse=True)
    chosen, kinds, ttoks = [], Counter(), []
    def ok(it):
        if kinds[it["kind"]] >= 2:
            return False
        T = toks(it["title"])
        return not any(T and len(T & x) / max(1, min(len(T), len(x))) >= 0.6 for x in ttoks)
    def take(it):
        chosen.append(it); kinds[it["kind"]] += 1; ttoks.append(toks(it["title"]))
    for it in sorted(pool, key=lambda i: (i["sev"], i["score"]["breaking"]), reverse=True):  # owner priority: worldwide first; >= 3 world-layer items (by severity/breadth) when available
        if len([c for c in chosen if c["layer"] == "world"]) >= 3:
            break
        if it["layer"] == "world" and it["truth"] in ("primary", "confirmed") and ok(it): take(it)
    for it in pool:
        if len(chosen) >= 7:
            break
        if it not in chosen and ok(it): take(it)
    top = pool[0] if pool else None          # highest-scoring eligible item
    if top is not None and top not in chosen:
        chosen.insert(0, top)
    lo = {"world": 0, "us": 1, "indiana": 2, "local": 2}
    chosen.sort(key=lambda i: (0 if i is top else 1, 0 if i["score"]["is_breaking"] else 1, lo.get(i["layer"], 3), -i["sev"], -rank(i)))
    bottom = first_sentence(top["plain"]) if top else "No civic or world item cleared the bar in this run."
    items = []
    for it in chosen:
        n = len(it["sources"])
        so = why_here(it) + [f"Label: {it['truth']}; {n} source{'s' if n != 1 else ''}."]
        wc = ("A second independent publisher reporting the same event would change the label from single to confirmed." if it["truth"] == "single"
              else "A revision or withdrawal by the issuing body would change this record." if it["truth"] == "primary" else "A retraction by one of the reporting publishers would change the label.")
        items.append({"headline": it["title"], "likelihood": None, "confidence": None, "so_what": " ".join(so), "changed": None, "would_change": wc,
                      "tags": ["FACT"] * len(so), "refs": [it["id"]], "touches_you": it.get("touches_you")})
    hero = None
    if chosen:
        hero = {**public(chosen[0]), "why_you_couldnt_ask": None}
    return {"date": date, "bottom_line": bottom, "bottom_line_tags": ["FACT"], "hero": hero, "items": items, "written_by": "engine skeleton (rules only; no model-written sentences; likelihood, confidence, why_you_couldnt_ask are null until a model fills them)", "at": iso(now)}, chosen



# ------------------------------------------------------------------ R18 "No receipt, no row": the last gate before the db write
URL_RE = re.compile(r"https?://[^\s)\]>\"']+")

def _rc_news_ok(r, url):
    """A news receipt: rung R1-R3 (R4 = no proof), an http source url, and a checked_at (page read time, else the publisher feed time)."""
    return bool(isinstance(r, dict) and r.get("rung") in ("R1", "R2", "R3") and str(url or "").startswith("http") and (parse_time(r.get("page_at")) or parse_time(r.get("feed_at"))) and reprove_receipt(r)[0])

def _item_receipt_ok(it):
    """Any contract item. News: some source carries a valid receipt. Everything else: first-hand source with an http url and a fetched time."""
    srcs = it.get("sources") or []
    if it.get("kind") == "news":
        return any(_rc_news_ok(s_.get("receipt"), s_.get("url")) for s_ in srcs)
    return any(str(s_.get("url", "")).startswith("http") and parse_time(s_.get("fetched")) for s_ in srcs)

def _num_ok(n):
    src_ = str(n.get("source", "")) + " " + str(n.get("url", ""))
    return n.get("value") is not None and bool(URL_RE.search(src_)) and bool(parse_time(n.get("fetched_at")))

def verify_all(docs):
    """Every news item, gloss and number must carry a receipt (rung, source url, checked_at); anything without one is not written.
    docs: [(collection, doc_id, data)] -> (docs, dropped). dropped = {news, gloss, numbers, items, total, why: {...}, ids: [dropped item ids]} (shown only in Settings > About)."""
    d = {"news": 0, "gloss": 0, "numbers": 0, "items": 0}; why = Counter(); gone = set(); out = []
    good_urls = {}
    unify_receipts(docs)
    for coll, did, data in docs:      # pass 1: news cluster entries keep only items that carry a receipt
        if coll != "news":
            continue
        for c in data.get("clusters", []):
            keep = []
            for i in c.get("items", []):
                if _rc_news_ok(i.get("receipt"), i.get("url")):
                    keep.append(i); good_urls[i["url"]] = i["receipt"]
                else:
                    d["news"] += 1; why["news: no receipt"] += 1
            c["items"] = keep
            ir = [i["receipt"] for i in keep if i["receipt"].get("rung") in ("R1", "R2", "R3", "R4")]
            if ir and c.get("rung") in ("R1", "R2", "R3", "R4") and min(x["rung"] for x in ir) < c["rung"]:   # r22: unify_receipts can lift an item; the cluster rung follows its best item
                b_ = min(ir, key=lambda x: x["rung"]); c["rung"] = b_["rung"]; c["proof"] = b_.get("proof") or c.get("proof")
        n0 = len(data.get("clusters", []))
        for c in data.get("clusters", []):
            if not c["items"]:
                gone.add(c.get("id"))
        data["clusters"] = [c for c in data.get("clusters", []) if c["items"]]
        d["news"] += n0 - len(data["clusters"])
    for coll, did, data in docs:
        if coll in ("wire", "events", "records") or coll == "live":
            for key in ([k for k in ("items", "events", "wire", "breaking") if k in data]):
                keep = []
                for it in data[key]:
                    if _item_receipt_ok(it):
                        keep.append(it)
                    else:
                        d["items"] += 1; gone.add(it.get("id")); why[f"{coll}: item without receipt"] += 1
                data[key] = keep
        elif coll == "series":
            r = data.get("number_rung") or {}
            if r.get("rung") not in ("N1", "N2", "N3") or not r.get("sha256") or not r.get("source_value") or not str(r.get("source_url") or "").startswith("http") or not (parse_time(data.get("tail_checked_at")) or parse_time(data.get("fetched_at"))):
                d["numbers"] += 1; why["series: no receipt"] += 1; data["_drop"] = True
        elif coll == "places":
            keep = []
            for n in data.get("numbers", []):
                if _num_ok(n):
                    keep.append(n)
                else:
                    d["numbers"] += 1; why["place number: no source url or time"] += 1
            data["numbers"] = keep
        elif coll == "glossary":
            keep = []
            for t in data.get("terms", []):
                if t.get("quote_status") == "found" and len((t.get("quote") or "").strip()) >= 40 and parse_time(t.get("quote_at")) and str(t.get("source", "")).startswith("http"):
                    keep.append(t)
                else:
                    d["gloss"] += 1; why["gloss: no found quote"] += 1
            data["terms"] = keep; data["glossed"] = sum(1 for t in keep if t.get("glossed")); data["unverified"] = sum(1 for t in keep if not t.get("glossed"))
        elif coll == "stories":
            keep = []
            for e in data.get("timeline", []):
                rc = good_urls.get(e.get("url"))
                if rc:
                    keep.append(dict(e, receipt={"rung": rc["rung"], "source_url": e["url"], "checked_at": rc.get("page_at") or rc.get("feed_at")}))
                else:
                    d["news"] += 1; why["story entry: no receipt"] += 1
            data["timeline"] = keep
    for coll, did, data in docs:
        if coll == "brief":
            data["items"] = [i for i in data.get("items", []) if not (set(i.get("refs") or []) & gone)]
            if data.get("hero") and data["hero"].get("id") in gone:
                data["hero"] = None
    for coll, did, data in docs:
        if coll == "series" and data.get("_drop"):
            gone.add("series/" + did)
    for coll, did, data in docs:
        if coll == "stories" and not data.get("timeline"):
            gone.add("stories/" + did); d["news"] += 1; why["story: no receipted entries"] += 1
    for coll, did, data in docs:
        if coll == "series" and data.get("_drop"):
            continue
        if coll == "stories" and "stories/" + did in gone:
            continue
        out.append((coll, did, data))
    total = d["news"] + d["gloss"] + d["numbers"] + d["items"]
    return out, {**d, "total": total, "why": dict(why), "ids": sorted(x for x in gone if x and "/" not in str(x))[:200]}

# ------------------------------------------------------------------ batching
def chunk_docs(coll, base, key, items, head=None):
    """Split a list into docs <= CHUNK_TARGET serialized bytes: base, base_2, base_3 ..."""
    head = head or {}
    docs, cur, size = [], [], 0
    for it in items:
        b = len(jdump(it)) + 1
        if cur and size + b > CHUNK_TARGET:
            docs.append(cur); cur, size = [], 0
        cur.append(it); size += b
    docs.append(cur)
    out = []
    for i, d in enumerate(docs, 1):
        out.append((coll, base if i == 1 else f"{base}_{i}", {**head, key: d, "chunk": i, "chunks": len(docs)}))
    return out

class GateRefused(Exception):
    def __init__(self, failed):
        super().__init__(f"{len(failed)} contract violation(s)")
        self.failed = failed

def seal_docs(docs, run, now, mode):
    """CONTRACT-v4 write path. Every doc: display_when on items, series first_print/echoes/display_when, stories states[],
    time/<date> (morning; hourly writes only new live docs), then _run/_at/v and as_of/ttl_s/stale_after; meta/health.missing
    from freshness.expect; meta/run cadence/ttl table. The gate then refuses the set on any violation."""
    tdocs = timeline.annotate(docs, now)
    docs = list(docs) + (tdocs if mode == "morning" else [])
    stamp = {"_run": run, "_at": iso(now), "v": V}
    out = [(c, d, freshness.stamp(c, {**data, **stamp}, now)) for c, d, data in docs]
    view = {f"{c}/{d}": data for c, d, data in out}
    missing = freshness.expect(view, now, scope={"live"} if mode == "hourly" else {"records", "series"})
    for i, (c, d, data) in enumerate(out):
        if c == "meta" and d == "health":
            out[i] = (c, d, freshness.stamp(c, {**data, "missing": missing}, now))
        elif c == "meta" and d == "run":
            out[i] = (c, d, freshness.stamp(c, {**data, "cadence_s": clock.CADENCE_S["hourly"], "mode_period_s": clock.CADENCE_S.get(mode), "ttl_s_by_collection": dict(sorted(freshness.TTL_S.items()))}, now))
    failed = gates.check(out, clock.now())
    if failed:
        raise GateRefused(failed)
    return out, missing

def refuse(outdir, run, failed):
    """gates.py verdict: no batches, report.json.failed[], exit 2."""
    for f in os.listdir(outdir):
        if re.fullmatch(r"batch_\d+\.json", f):
            os.remove(os.path.join(outdir, f))
    json.dump({"run": run, "failed": failed}, open(os.path.join(outdir, "report.json"), "w"), indent=1)
    print(f"[gate] refused {len(failed)} violation(s); first: {failed[0]}", file=sys.stderr)
    sys.stdout.flush(); sys.stderr.flush()
    os._exit(2)

def write_batches(docs, outdir, run, now, mode="morning"):
    os.makedirs(outdir, exist_ok=True)
    for f in os.listdir(outdir):
        if re.fullmatch(r"batch_\d+\.json", f):
            os.remove(os.path.join(outdir, f))
    try:
        sealed, _missing = seal_docs(docs, run, now, mode)
    except GateRefused as g:
        refuse(outdir, run, g.failed)
    batches, cur, size, files = [], [], 2, []
    for coll, did, data in sealed:
        w = {"op": "set", "collection": coll, "doc_id": did, "data": data}
        s = jdump(w)
        if len(s.encode("utf-8")) > DOC_MAX:
            raise ValueError(f"doc {coll}/{did} is {len(s)} bytes > {DOC_MAX}")
        b = len(s.encode("utf-8")) + 1
        if cur and (len(cur) >= BATCH_WRITES or size + b > BATCH_BYTES):
            batches.append(cur); cur, size = [], 2
        cur.append(w); size += b
    if cur:
        batches.append(cur)
    total = 0
    for i, b in enumerate(batches, 1):
        p = os.path.join(outdir, f"batch_{i:03d}.json")
        with open(p, "w", encoding="utf-8") as f:
            f.write(jdump(b))
        total += os.path.getsize(p); files.append(p)
    return files, total, {"failed": [], "missing": _missing, "docs": len(sealed), "time_docs": [d for c, d, _ in sealed if c == "time"]}


# ------------------------------------------------------------------ main pipeline
def next_due(mode, now):
    return clock.next_due(mode, now)   # hourly :51, morning 08:51 local (operator 2026-10-31)

def _sk(x):
    return json.dumps(x, sort_keys=True, default=str, ensure_ascii=False)

def settle_ctx(ctx):
    """Byte-stable runs: sources finish in thread order, so every collection the fetch threads filled is
    re-ordered here by stable keys before anything reads it (same inputs + same clock -> same bytes)."""
    item_key = lambda i: (i.get("_sid") or "", str(i.get("id") or ""), _sk(i))
    ctx.events.sort(key=item_key)
    ctx.wire.sort(key=item_key)
    ctx.articles.sort(key=lambda x: (x["t"], x.get("url") or "", x.get("title") or "", (x.get("receipt") or {}).get("feed") or "", x.get("pub") or "", _sk(x)))
    ctx.records = defaultdict(list, {k: sorted(v, key=item_key) for k, v in sorted(ctx.records.items())})
    ctx.series = {k: ctx.series[k] for k in sorted(ctx.series)}
    ctx.pnums = defaultdict(list, {k: sorted(v, key=lambda n: (str(n.get("label")), str(n.get("source")), _sk(n))) for k, v in sorted(ctx.pnums.items())})
    ctx.clocks.sort(key=lambda c: (c.get("closes_at") or "", str(c.get("id") or ""), _sk(c)))
    px = defaultdict(dict)
    for pid in sorted(ctx.pextra):
        d = ctx.pextra[pid]
        if isinstance(d.get("heads"), list):
            d["heads"] = sorted(d["heads"])
        px[pid] = {k: d[k] for k in sorted(d)}
    ctx.pextra = px
    ctx.zone_cache = {k: ctx.zone_cache[k] for k in sorted(ctx.zone_cache)}
    ctx.stats = {k: ctx.stats[k] for k in sorted(ctx.stats)}


MORNING_PHOTO_TOP = 150      # M4: clusters given a picture attempt in a morning run (Fable M3 §4: >= 80 % of the top 30 shown)
MORNING_PHOTO_BUDGET = 320
LIVE_EXTRA_PHOTO_BUDGET = 60   # live-window clusters whose article has no stored picture yet   # picture fetches (robots, pages, images) for those clusters plus issuer records


def photo_targets(news_keep, brief_news=(), newest=40, most=40):
    """Clusters in the order the page meets them: brief refs, the `newest` newest (Today sorts news newest first), the `most`
    most-reported (the lead is the cluster with the most members), then the rest by distinct publishers. Pure ordering of rows;
    a cluster's picture never moves it."""
    rank_c = lambda c: (len(c["_cluster"]["hosts"]), c["score"]["breaking"], c["score"]["total"])
    last_t = lambda c: str(max((m_["t"] for m_ in c["_cluster"]["members"]), default="")) if c.get("_cluster") else ""
    by_t = sorted(news_keep, key=lambda c: (last_t(c), c["id"]), reverse=True)   # the cluster row's t is its newest member
    by_n = sorted(news_keep, key=lambda c: (len(c["_cluster"]["members"]), str(c.get("t") or "")), reverse=True)
    out, seen = [], set()
    for c in list(brief_news) + by_t[:newest] + by_n[:most] + sorted(news_keep, key=rank_c, reverse=True):
        if c["id"] not in seen and c.get("_cluster"):
            seen.add(c["id"]); out.append(c)
    return out


def load_state(sd):
    p = os.path.join(sd, "last.json")
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return {}

def merge_prev(new, prev, run_sids):
    """new: list of items (with _sid); prev: list of {sid, item}. Keep prev items whose source did not run now."""
    ids = {i["id"] for i in new}
    keep = [dict(p["item"], _sid=p["sid"]) for p in prev if p["sid"] not in run_sids and p["item"]["id"] not in ids]
    return new + keep

def finish_hourly(a, ctx, now, run, health, cl_items, t_start, t_fetch, today):
    """Hourly mode: build the live doc, then exit."""
    def photos(news_pick):
        if getattr(a, "no_images", False):
            return [], {}, None
        return imgfeeds.run_photos(ctx.F, news_pick, [], health, PRIORITY)
    build_live(a.out, ctx, now, run, health, cl_items, t_start, t_fetch, photos, mode="hourly")
    sys.stdout.flush()
    os._exit(0)


def live_clusters(ctx, now, seen_map=None):
    """News clusters of the live window (last LIVE_WINDOW_H hours), built exactly as hourly mode builds them."""
    cut = now - dt.timedelta(hours=LIVE_WINDOW_H)
    arts = sorted((x for x in ctx.articles if x["t"] >= cut), key=lambda x: (x["t"], x.get("url") or "", x.get("title") or "", (x.get("receipt") or {}).get("feed") or ""))
    items = [cluster_to_item(c, now) for c in build_clusters(arts, now)]
    for it in items:
        it["_sid"] = "news"; finalize(it, now)
        it["first_seen"] = it.get("first_seen") or (seen_map or {}).get(it["id"]) or iso(now)
        it["updated"] = it.get("updated") or it["t"]
    return items


def reuse_photos(news_pick, files, item_img):
    """M4 morning -> live: a live cluster reuses this run's stored picture when the picture's own article (src_url) is one of the
    cluster's members -- the same publisher picture for the same article, never a guess. -> (files, {item_id: img_id}, misses)."""
    by_art = {}
    for iid, d in files:
        if iid in set(item_img.values()) and d.get("src_url"):
            by_art.setdefault(d["src_url"], iid)
    out, miss = {}, []
    for c in news_pick:
        iid = next((by_art[m_["url"]] for m_ in c["_cluster"]["members"] if m_.get("url") in by_art), None)
        if iid:
            out[c["id"]] = iid
        else:
            miss.append(c)
    used = set(out.values())
    return [(i, d) for i, d in files if i in used], out, miss


def build_live(out_dir, ctx, now, run, health, cl_items, t_start, t_fetch, photos, mode="hourly"):
    """Write ONLY new documents live/<YYYY-MM-DDTHHMM> (UTC run time) + img/ to out_dir. Stateless; never overwrites anything.
    `photos(news_pick)` -> (files, {item_id: img_id}, counts). mode = the run that wrote it ("hourly", or "morning" for the
    live doc the hourly morning-mode run also writes, M4)."""
    os.makedirs(out_dir, exist_ok=True)
    cut = now - dt.timedelta(hours=LIVE_WINDOW_H)
    recent = lambda i: bool(i.get("t")) and (parse_time(i["t"]) or cut) >= cut
    desc = lambda L: sorted(L, key=lambda i: i["t"], reverse=True)
    events = desc([public(e) for e in ctx.events if recent(e)])
    wire_src = list(ctx.wire) + [i for l in ctx.records.values() for i in l]
    # news clusters from the last 2 h go into their own `news` list below (not into wire)
    multi = [c for c in cl_items if len(c["_cluster"]["members"]) >= 2]
    rank_c = lambda c: (len(c["_cluster"]["hosts"]), c["score"]["breaking"], c["score"]["total"])
    news_pick = sorted(multi, key=rank_c, reverse=True)[:30] + sorted([c for c in cl_items if len(c["_cluster"]["members"]) == 1], key=lambda c: (c["score"]["breaking"], c["score"]["total"]), reverse=True)[:10]
    news = [cluster_doc_entry(c, max_items=5, summ=120) for c in news_pick]
    ev_ids = {e["id"] for e in events}
    seen, wire = set(), []
    for i in wire_src:
        if i["id"] not in seen and i["id"] not in ev_ids and recent(i):
            seen.add(i["id"]); wire.append(public(i))
    wire = desc(wire)
    pool = {i["id"]: i for i in list(ctx.events) + wire_src + cl_items if i["score"]["is_breaking"]}
    breaking = [public(i) for i in sorted(pool.values(), key=lambda i: i["score"]["breaking"], reverse=True)]
    n_ok = sum(1 for h in health if h["status"] == "data")
    finished = clock.now()
    hl = [dict({"id": h["id"], "status": h["status"], "items": h["items"]}, **({"error": clip(h["error"], 80)} if h["status"] != "data" and h.get("error") else {})) for h in sorted(health, key=lambda h: h["id"])]
    stamp_id = now.strftime("%Y-%m-%dT%H%M")
    head = {"run": run, "mode": "hourly", "written_by": mode, "total": len(health), "ok": n_ok, "label": f"{n_ok} of {len(health)} {'hourly' if mode == 'hourly' else 'morning'} sources returned data", "started": iso(now), "finished": iso(finished), "ok_sources": n_ok, "total_sources": len(health), "next_due": next_due("hourly", finished), "window_hours": LIVE_WINDOW_H}
    # split into live/<id>, live/<id>_2 ... when the body would pass the target
    first = {**head, "health": hl}
    parts, cur, size = [], {"events": [], "wire": [], "breaking": [], "news": []}, len(jdump(first)) + 200
    for key, lst in (("events", events), ("wire", wire), ("breaking", breaking), ("news", news)):
        for it in lst:
            b = len(jdump(it)) + 1
            if size + b > LIVE_DOC_TARGET and any(cur.values()):
                parts.append(cur); cur, size = {"events": [], "wire": [], "breaking": [], "news": []}, 200
            cur[key].append(it); size += b
    parts.append(cur)
    docs = []
    for n, body in enumerate(parts, 1):
        d = {**(first if n == 1 else head), **body, "part": n, "parts": len(parts)}
        docs.append(("live", stamp_id if n == 1 else f"{stamp_id}_{n}", d))
    # P10: R2-pub photos (publisher feed previews, linked) under the hourly budget; plates/none for the rest (CONTRACT-v4 §5)
    shutil.rmtree(os.path.join(out_dir, "img"), ignore_errors=True)
    ph_files, ph_map, ph_counts = photos(news_pick)
    for d_ in docs:
        if d_[1] == stamp_id:
            d_[2]["photos"] = ph_counts or {"fetched": 0, "attached": 0, "rejected": {}, "note": "photos not fetched (--no-images)"}
            d_[2]["cadence_s"] = clock.CADENCE_S["hourly"]
    imgfeeds.attach_items(docs, ph_map, ph_files)
    files, total_bytes, seal = write_batches(docs, out_dir, run, finished, mode="hourly")
    open(os.path.join(out_dir, "collections.txt"), "w").write("")
    if ph_files:
        imgfeeds.write_photo_dir(out_dir, ph_files, ph_counts)
    status_ct = Counter(h["status"] for h in health)
    report = {"run": run, "mode": "hourly", "written_by": mode, "started": iso(now), "finished": iso(finished), "duration_s": (None if clock.frozen() else round(time.time() - t_start, 1)), "fetch_s": (None if clock.frozen() else round(t_fetch - t_start, 1)),
              "sources": sorted(health, key=lambda h: h["id"]), "totals": {"sources": len(health), **{k: status_ct.get(k, 0) for k in ("data", "empty", "blocked", "needs_key", "error")}},
              "items": {"events": len(events), "wire": len(wire), "breaking": len(breaking), "news": len(news)}, "docs": len(docs), "failed": seal["failed"], "missing": seal["missing"], "bytes": total_bytes, "batches": len(files), "doc_ids": [d[1] for d in docs],
              "warnings": [], "ok_sources": n_ok, "total_sources": len(health), "static_once": False, "collections": [], "stats": ctx.stats,
              "news": {"clusters_in_window": len(cl_items), "multi_publisher": len(multi), "confirmed_in_list": sum(1 for n_ in news if n_["truth"] == "confirmed"), "feeds_hourly": sum(1 for h in health if h["id"].startswith("news_"))}, "photos": ph_counts}
    json.dump(report, open(os.path.join(out_dir, "report.json"), "w"), indent=1)
    print(f"[live:{mode}] run {run}: {n_ok}/{len(health)} sources with data; live/{stamp_id}: {len(events)} events, {len(wire)} wire, {len(breaking)} breaking, {len(news)} news; {len(docs)} doc(s), {total_bytes} bytes; {round(time.time() - t_start, 1)}s")
    sys.stdout.flush()
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["morning", "hourly"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--data", default=os.path.join(HERE, "data"))
    ap.add_argument("--state", default=os.path.join(HERE, "state"))
    ap.add_argument("--replay", default=None, help="reuse raw responses from this cache dir (tests)")
    ap.add_argument("--cache", default=None, help="save raw responses here (scrubbed of keys)")
    ap.add_argument("--budget", type=int, default=None, help="seconds allowed for the fetch phase")
    ap.add_argument("--no-images", action="store_true")
    ap.add_argument("--no-receipt-pages", action="store_true", help="skip the polite page check (feed proof is still recorded)")
    ap.add_argument("--no-live", action="store_true", help="morning only: skip the live/<stamp> doc (M4: written by default into OUT/hourly)")
    ap.add_argument("--static-once", action="store_true", help="morning only: also emit atlas/*, gaps/all, glossary/all (they rarely change; skipped otherwise)")
    a = ap.parse_args()
    load_keys()
    t_start = time.time(); now = clock.now(); run = a.mode[0] + "-" + now.strftime("%Y%m%dT%H%M%SZ") + "-v3"
    today = local_date(now)
    F = Fetcher(a.replay, a.cache)
    ctx = Ctx(a.mode, F, now, a.data)
    state = load_state(a.state) if a.mode == "morning" else {}   # hourly is stateless
    specs = [sp for sp in SOURCES if a.mode == "morning" or sp["hourly"]]
    for fs in load_feed_specs(a.data):
        if a.mode == "morning" or fs["hourly"]:
            specs.append({"id": fs["id"], "name": f"News feed: {fs['pubname']} ({fs['url']})", "host": urlparse(fs["url"]).netloc, "hourly": fs["hourly"], "keys": (), "fn": make_feed_source(fs)})
    budget = a.budget or (420 if a.mode == "morning" else 200)
    ex = cf.ThreadPoolExecutor(max_workers=16)
    futs = {ex.submit(run_source, ctx, sp): sp for sp in specs}
    done, pending = cf.wait(futs, timeout=budget)
    health = []
    for f in futs:
        sp = futs[f]
        if f in done:
            health.append(f.result())
        else:
            health.append({"id": sp["id"], "name": sp["name"], "host": sp["host"], "status": "error", "items": 0, "http": None, "last_good": None, "error": f"timeout after {budget}s fetch budget", "ms": budget * 1000})
    ex.shutdown(wait=False, cancel_futures=True)
    t_fetch = time.time()
    settle_ctx(ctx)
    prev_lg = state.get("last_good", {})
    for h in health:
        if h["status"] == "data":
            prev_lg[h["id"]] = h["last_good"]
        else:
            h["last_good"] = prev_lg.get(h["id"])
    run_sids = {sp["id"] for sp in specs}

    # ---- news clusters
    cutoff = now - dt.timedelta(hours=(LIVE_WINDOW_H if a.mode == "hourly" else 72))
    arts = sorted((x for x in ctx.articles if x["t"] >= cutoff), key=lambda x: (x["t"], x.get("url") or "", x.get("title") or "", (x.get("receipt") or {}).get("feed") or ""))   # P1 byte-stable: feeds finish in thread order
    clusters = build_clusters(arts, now)
    cl_items = [cluster_to_item(c, now) for c in clusters]
    for it in cl_items:
        it["_sid"] = "news"
    for it in cl_items:
        finalize(it, now)
    receipt_stats = {}
    if a.mode == "morning" and not a.replay and not a.no_receipt_pages:    # receipts: one polite page check for each cited source of what will be written (morning only)
        sing0 = sorted([i for i in range(len(clusters)) if len(clusters[i]["m"]) == 1], key=lambda i: (cl_items[i]["score"]["total"], cl_items[i]["score"]["breaking"], cl_items[i]["t"]), reverse=True)
        cand = [i for i in range(len(clusters)) if len(clusters[i]["m"]) >= 2] + sing0[:120]
        receipt_stats, clusters = apply_receipts(F, clusters, cand, now)
        if receipt_stats.get("mismatch"):
            cl_items = [cluster_to_item(c, now) for c in clusters]
            for it in cl_items:
                it["_sid"] = "news"; finalize(it, now)

    # ---- finalize everything else
    atlas_index, atlas_leaves = build_atlas(a.data)
    for coll in (ctx.events, ctx.wire):
        for it in coll: finalize(it, now)
    for kind, lst in ctx.records.items():
        for it in lst: finalize(it, now)
    all_pool = ctx.events + ctx.wire + [i for l in ctx.records.values() for i in l] + cl_items
    link_atlas(all_pool, atlas_leaves)
    seen_map = state.get("seen", {})
    for it in all_pool:  # first_seen: when this engine first saw the id (state), else this run; updated falls back to event time
        it["first_seen"] = it.get("first_seen") or seen_map.get(it["id"]) or iso(now)
        seen_map.setdefault(it["id"], it["first_seen"])
        it["updated"] = it.get("updated") or it["t"]
    seen_map = {k: v for k, v in seen_map.items() if (parse_time(v) or now) > now - dt.timedelta(days=10)}
    if a.mode == "hourly":
        finish_hourly(a, ctx, now, run, health, cl_items, t_start, t_fetch, today)

    # ---- stories from well-corroborated clusters
    stories = []
    hot = sorted([c for c in cl_items if c["truth"] == "confirmed" and len(c["_cluster"]["members"]) >= 3], key=lambda c: (c["_cluster"]["independent"], len(c["_cluster"]["members"])), reverse=True)[:12]
    for c in hot:
        mem = sorted(c["_cluster"]["members"], key=lambda x: x["t"])
        last = mem[-1]["t"]
        sid = "story-" + c["id"].split("-", 1)[1]
        c["story"] = sid
        stories.append(("stories", sid, {"id": sid, "title": c["title"], "state": "active" if (now - last).total_seconds() < 86400 else "quiet", "last_move": iso(last), "next_step": None,
                                         "timeline": [{"t": iso(x["t"]), "title": x["title"], "kind": "news", "url": x["url"], "truth": "single"} for x in mem[:30]]}))

    # ---- state merge for hourly
    warnings = []

    # ---- choose items for events / wire / records
    def doc_date_event(it):
        d = parse_time(it["t"]) or now
        ongoing = it["kind"] in ("disaster", "fire", "storm", "volcano", "flood") or (it["kind"] == "alert" and parse_time(it.get("_expires")) and parse_time(it["_expires"]) > now)
        return today if ongoing else local_date(d)
    events = ctx.events
    rec_final = {}
    for kind in ("presidential", "rules_proposed", "public_inspection", "bills_presented", "nominations", "rulings", "outbreaks", "sanctions", "declarations"):
        items = ctx.records.get(kind, [])
        items = sorted(items, key=lambda i: i["t"] or "", reverse=True)
        # dedupe by id
        seen, out = set(), []
        for i in items:
            if i["id"] not in seen:
                seen.add(i["id"]); out.append(i)
        rec_final[kind] = out[:100]
    __import__("coverage").refile_scope(rec_final)   # P10: state courts move to records/state_rulings; every record carries `scope`
    __import__("records").attach_stages(rec_final)   # P2: stage ladder on every record
    # news-based wire entries
    confirmed_n = sum(1 for c in cl_items if c["truth"] == "confirmed")
    sing = sorted([c for c in cl_items if len(c["_cluster"]["members"]) == 1], key=lambda c: (c["score"]["total"], c["score"]["breaking"], c["t"]), reverse=True)
    multi = [c for c in cl_items if len(c["_cluster"]["members"]) >= 2]
    news_keep = multi + sing[:120]
    # Proof Ladder: clear every not_checked, seal each item's receipts and rung (morning only; the hourly beat never reads pages)
    def settle_and_prune(items):
        if a.mode != "morning":
            return
        if a.no_receipt_pages:
            for it in items:
                it["_cluster"]["members"] and seal_item(it)
            return
        gone = settle_items(F, [i for i in items if not i.get("_settled")], now)
        if gone:
            for lst in (news_keep, multi, sing, wire_news_ref, cl_items, all_pool):
                lst[:] = [i for i in lst if i["id"] not in gone]
    wire_news_ref = []
    wire_news = sorted(multi, key=lambda c: (c["score"]["breaking"], c["score"]["total"], len(c["_cluster"]["members"])), reverse=True)[:80]
    wire_news_ref[:] = wire_news
    settle_and_prune(news_keep)
    wire_news = [i for i in wire_news if i in news_keep]

    # ---- images (files + manifest, never embedded in db docs; morning only). Sources and licence checks: imgfeeds.py
    img_files, img_log, place_img = [], {"ok": 0, "failed": []}, {}
    if not a.no_images and a.mode == "morning":
        img_files, item_img, place_img = imgfeeds.build_all(F, now, events, rec_final, [p[0] for p in PLACES], img_log)
    ph_files, ph_map, ph_counts = [], {}, None   # M4: R2 photos run after the brief, in the order the page shows clusters
    # ---- wire
    wire_items = list(ctx.wire)
    for kind, lst in rec_final.items():
        for i in lst:
            t = parse_time(i["t"])
            if t and (now - t).days <= 3 and kind not in ("nominations", "public_inspection"):
                wire_items.append(i)
    wire_items += wire_news
    wire_items += [e for e in events if e["score"]["is_breaking"]]
    byid = {}
    for i in wire_items:
        byid.setdefault(i["id"], i)
    wire_items = [i for i in byid.values() if i["t"]]
    wire_items.sort(key=lambda i: i["t"], reverse=True)

    force_ids = set()
    # ---- clocks (morning only; hourly leaves clocks/all untouched)
    clocks_doc = None
    if a.mode == "morning":
        for i in rec_final.get("rules_proposed", []):
            cc = i.get("comments_close_on")
            if cc and day_end(cc) > now:
                mk_clock(ctx, f"Public comments close: {i['title']}", "comment", iso(day_end(cc)), "Federal Register", i["sources"][0]["url"])
        try:
            g1 = next(r for r in load_json(a.data, "Gaps.json")[1:] if r[0] == "G1")
            m = re.search(r"Election Day is \w+ (\d{4}-\d{2}-\d{2})", g1[4]); u = re.search(r"https://www\.in\.gov/sos/[^\s;]+", g1[6])
            if m and u:
                ed = m.group(1)
                if not any(c["kind"] == "election" and c["closes_at"] and c["closes_at"][:10] in (ed, iso(dt.datetime.strptime(ed, "%Y-%m-%d").replace(hour=18, tzinfo=TZ))[:10]) for c in ctx.clocks):
                    mk_clock(ctx, f"Election Day {ed}: polls close 6:00 pm at any Marion County vote center (mail ballots must arrive by 6:00 pm)", "election", iso(dt.datetime.strptime(ed, "%Y-%m-%d").replace(hour=18, tzinfo=TZ)),
                             "Indiana Secretary of State 2026 election calendar (as cited in spec Gaps G1)", u.group(0), "Marion County", prov="typed_dated", note="Date and poll hours are typed in the spec (Gaps.txt G1) with this source; the PDF itself was not fetched by the engine (UNVERIFIED)")
        except StopIteration:
            pass
        cl = sorted({c["id"]: c for c in ctx.clocks}.values(), key=lambda c: (c["closes_at"] or "", c["id"]))   # P1 byte-stable tie order
        clocks_doc = {"items": cl}

    # ---- brief skeleton (before docs so every ref is emitted)
    n_ok = sum(1 for h in health if h["status"] == "data")
    brief_doc = None
    if a.mode == "morning":
        for _round in range(3):
            brief_doc, chosen = build_brief(today, a.mode, [i for i in all_pool], n_ok, len(health), now,
                                            {"events": len(events), "wire": len(wire_items), "clusters": len(cl_items), "confirmed": confirmed_n})
            fresh = [c for c in chosen if c["kind"] == "news" and not c.get("_settled")]
            if not fresh:
                break
            before = len(all_pool); settle_and_prune(fresh)
            wire_items[:] = [i for i in wire_items if i["id"] in {x["id"] for x in all_pool} or not i["id"].startswith("news")]
        have = {c["id"] for c in news_keep}
        for c in chosen:
            if c["kind"] == "news" and c["id"] not in have:
                news_keep.append(c); have.add(c["id"])
        in_wire = {i["id"] for i in wire_items} | {e["id"] for e in events} | {i["id"] for l in rec_final.values() for i in l}
        for c in chosen:     # every brief ref must exist in an emitted doc
            if c["kind"] != "news" and c["id"] not in {e["id"] for e in events} and c["t"]:
                force_ids.add(c["id"])
                if c["id"] not in in_wire:
                    wire_items.append(c); in_wire.add(c["id"])

    # ---- R2 photos (M4): targets in the order the reader meets them -- brief refs, the newest clusters (Today lists news newest
    # first), the most-reported clusters (the lead is the cluster with most members) -- then the rest by publishers. The order
    # comes only from the rows; nothing is ranked or chosen by having a picture.
    if not a.no_images:
        targets = photo_targets(news_keep, [c for c in (chosen if a.mode == "morning" else []) if c["kind"] == "news"])
        ph_files, ph_map, ph_counts = imgfeeds.run_photos(F, targets, [r for v in rec_final.values() for r in v], health, PRIORITY,
                                                           budget=MORNING_PHOTO_BUDGET, top=MORNING_PHOTO_TOP)
        img_files = list(img_files) + ph_files

    # ---- docs
    docs = []
    counts_items = {}
    rung_counts = None
    if a.mode == "morning":
        if a.static_once:
            docs.append(("atlas", "index", atlas_index))
            for d, ls in atlas_leaves.items():
                docs.append(("atlas", d, {"domain": d, "leaves": ls}))
            docs.append(("gaps", "all", build_gaps(a.data)))
            docs.append(("glossary", "all", build_glossary(a.data, F, now)))
        carried = []
        for sid, doc in (state.get("series") or {}).items():
            fa = parse_time(doc.get("fetched_at") or (doc.get("source") or {}).get("fetched"))
            if sid not in ctx.series and fa and (now - fa).days <= 7:
                lab = "kept from " + kept_day(fa) + " fetch"
                doc = dict(doc, stale=True, stale_note=f"not refreshed in this run; values are from the fetch at {iso(fa)}",
                           fetched_at=iso(fa), kept=True, kept_label=lab)
                ctx.series[sid] = doc; carried.append(sid)
        if carried:
            warnings.append("series carried from last good fetch: " + ", ".join(carried))
        try:
            n_tail = refresh_tails(F, ctx, ctx.series, now)
            if n_tail:
                warnings.append(f"{n_tail} series had a newer dated value at publish time and were updated")
        except Exception as ex_:
            warnings.append(f"tail check failed: {ex_!r}"[:200])
        flag_coincident_series(ctx.series)
        rung_counts = assign_rungs(ctx.series, now)
        for sid, doc in sorted(ctx.series.items()):
            docs.append(("series", sid, doc))
        counts_items["series"] = len(ctx.series)
        docs += build_places(ctx, a.data, [x for x in events if True] + (wire_items), cl_items)
        try:   # r13 hook: Census PEP + BLS LAUS numbers for Marion County (population first); nothing faked on error
            px = places_extra.fetch_places_extra()
            if px.get("errors"): warnings.append("places_extra: " + "; ".join(map(str, px["errors"]))[:300])
            for c_, d_, doc_ in docs:
                if c_ == "places" and d_ in ("marion", "indianapolis", "indiana") and px.get(d_):
                    key_ = lambda n: (n.get("label") or n.get("name") or "").lower()
                    have = {key_(n) for n in doc_["numbers"]}
                    doc_["numbers"][:0] = [dict(n, fetched_at=n.get("fetched_at") or iso(clock.now())) for n in px[d_] if key_(n) and key_(n) not in have]
        except Exception as ex:
            warnings.append(f"places_extra failed: {ex!r}"[:300])
        imgfeeds.attach_places(docs, place_img, img_files)
        docs += __import__("coverage").derived_places(list(events) + list(wire_items), {d for c, d, _ in docs if c == "places"}, get=__import__("coverage").http_json, fetched_at=iso(clock.now()), clusters=cl_items)   # P2
        docs += stories
        if clocks_doc:
            docs.append(("clocks", "all", clocks_doc)); counts_items["clocks"] = len(clocks_doc["items"])
    for kind, lst in rec_final.items():
        docs += chunk_docs("records", kind, "items", [public(i) for i in lst]); counts_items["records/" + kind] = len(lst)
    if a.mode == "morning":
        by_date = defaultdict(list)
        for c in news_keep:
            by_date[local_date(c["_cluster"]["members"][0]["t"])].append(c)
        nsum = 0
        for d, cs in sorted(by_date.items()):
            cs.sort(key=lambda c: c["score"]["total"] * 0 + len(c["_cluster"]["members"]), reverse=True)
            docs += chunk_docs("news", d, "clusters", [cluster_doc_entry(c) for c in cs], {"date": d}); nsum += len(cs)
        counts_items["news clusters"] = nsum
    by_date = defaultdict(list)
    for e in events:
        d = doc_date_event(e)
        if (dt.date.fromisoformat(today) - dt.date.fromisoformat(d)).days <= 1:
            by_date[d].append(e)
    for d, es in sorted(by_date.items()):
        es.sort(key=lambda e: e["t"], reverse=True)
        docs += chunk_docs("events", d, "items", [public(e) for e in es], {"date": d}); counts_items["events/" + d] = len(es)
    if True:
        wd = defaultdict(list)
        for i in wire_items:
            d = local_date(parse_time(i["t"]))
            if (dt.date.fromisoformat(today) - dt.date.fromisoformat(d)).days <= 2 or i["id"] in force_ids:
                wd[d].append(i)
        for d, ws in sorted(wd.items()):
            docs += chunk_docs("wire", d, "items", [public(w) for w in ws], {"date": d}); counts_items["wire/" + d] = len(ws)
    if brief_doc:
        docs.append(("brief", today, brief_doc))
    if a.mode == "morning":   # r13 hook: the Indiana Nov 3 civic doc (never raises; unreadable pages go in `unreached`)
        try:
            docs.append(("civic", "indiana-2026-general", civic.build(run=run)))
        except Exception as ex:
            warnings.append(f"civic failed: {ex!r}"[:300])

    # ---- state save (morning only; used for carried series, last_good and first_seen)
    days = {}
    records_state = {k: [{"sid": i.get("_sid"), "item": public(i)} for i in v] for k, v in rec_final.items()}
    for k, v in (state.get("records") or {}).items():
        records_state.setdefault(k, v)

    # ---- label counts across everything emitted
    emitted = list(events) + list(wire_items) + [i for v in rec_final.values() for i in v] + (news_keep if a.mode == "morning" else [])
    uniq = {i["id"]: i for i in emitted}
    labels = Counter(i["truth"] for i in uniq.values())
    t_end = time.time()
    status_ct = Counter(h["status"] for h in health)
    catalog = {}
    try:
        cat = load_json(a.data, "Sources.json")[1:]
        catalog = {"rows": len(cat), "by_verdict": dict(Counter(r[6] for r in cat)), "note": "Sources.json verdicts from the spec sheet; the engine fetched only the sources listed in `sources`."}
    except Exception:
        pass
    finished = clock.now()
    mode_word = "hourly" if a.mode == "hourly" else "morning"
    dropped = {"news": 0, "gloss": 0, "numbers": 0, "items": 0, "total": 0, "why": {}, "ids": []}
    if a.mode == "morning":
        docs, dropped = verify_all(docs)      # R18: no receipt, no row
        if dropped["total"]:
            warnings.append(f"verify_all dropped {dropped['total']} rows without a receipt: " + "; ".join(f"{k} x{v}" for k, v in dropped["why"].items()))
    meta_health = {"run": run, "mode": a.mode, "total": len(health), "ok": n_ok, "label": f"{n_ok} of {len(health)} {mode_word} sources returned data", "sources": [{k: h[k] for k in ("id", "name", "host", "status", "items", "http", "last_good", "error")} for h in sorted(health, key=lambda h: h["id"])], "catalog": catalog,
                   "totals": {"sources": len(health), **{k: status_ct.get(k, 0) for k in ("data", "empty", "blocked", "needs_key", "error")}}}
    ladder = ladder_counts([c for c in news_keep]) if a.mode == "morning" else {}
    meta_run = {"dropped": dropped, "ladder": ladder, "run": run, "mode": a.mode, "total": len(health), "ok": n_ok, "label": f"{n_ok} of {len(health)} {mode_word} sources returned data", "started": iso(now), "finished": iso(finished), "ok_sources": n_ok, "total_sources": len(health), "next_due": next_due(a.mode, finished), "photos": ph_counts or {"fetched": 0, "attached": 0, "rejected": {}, "note": "photos not fetched in this run"}}
    hb = {"at": iso(finished), "ok": n_ok, "total": len(health), "run": run}
    docs += [("meta", "health", meta_health), ("meta", "heartbeat_" + a.mode, hb), ("meta", "run", meta_run)]

    imgfeeds.attach_items(docs, {**ph_map, **{i['id']: i['img'] for i in list(events) + list(wire_items) + [r for v in rec_final.values() for r in v] if i.get('img')}}, img_files)
    # ---- fixed (overwritten) docs -> collections the runner must list for versions
    colls = sorted({c for c, d, _ in docs if c in FIXED_COLLS or c in DATED_COLLS} | {"img", "time"})   # P1: time/<date> docs are overwritten too; r13: a same-day rerun finds today's dated docs already written, so they need versions too
    files, total_bytes, seal = write_batches(docs, a.out, run, finished, mode=a.mode)
    open(os.path.join(a.out, "collections.txt"), "w").write("".join(c + "\n" for c in colls))
    img_dir = os.path.join(a.out, "img"); shutil.rmtree(img_dir, ignore_errors=True); os.makedirs(img_dir)
    for iid, d in img_files:
        open(os.path.join(img_dir, iid + ".jpg"), "wb").write(d["jpg"])
    manifest = imgfeeds.manifest_rows(img_files)
    sc_ = imgfeeds._scored(img_files)          # FABLE-MIDBUILD: plate_score retired; legibility (images/contract.py) is the one picture measure
    for m_ in manifest:
        row_, leg_ = sc_.get(m_["id"], (None, None))
        leg_ = leg_ or {}
        m_.update({"legibility": leg_.get("legibility"), "legible": bool(leg_.get("legible")), "legibility_reasons": leg_.get("reasons") or [],
                   "width": (row_ or {}).get("width"), "height": (row_ or {}).get("height")})
    img_log["legibility"] = {"scored": len(manifest), "legible": sum(1 for m_ in manifest if m_.get("legible"))}
    json.dump(manifest, open(os.path.join(img_dir, "manifest.json"), "w"), indent=1)
    dup = Counter((c, d) for c, d, _ in docs)
    assert all(v == 1 for v in dup.values()), "duplicate doc ids"
    # ---- M4: the hourly scheduled run is morning mode, so it also writes this hour's live/<stamp> doc (+ img/h<stamp>) into
    # out/<morning>/hourly, same shape as hourly mode, from the sources this run already fetched (no second fetch pass)
    live_report = None
    if a.mode == "morning" and not getattr(a, "no_live", False):
        def live_photos(news_pick):
            if a.no_images:
                return [], {}, None
            files_, map_, miss = reuse_photos(news_pick, ph_files, ph_map)
            f2, m2, cnt = imgfeeds.run_photos(F, miss, [], health, PRIORITY, budget=LIVE_EXTRA_PHOTO_BUDGET) if miss else ([], {}, None)
            files_ = sorted(dict(list(files_) + list(f2)).items())
            map_.update(m2)
            n_top = len(news_pick[:60])
            cnt = dict(cnt or {"fetched": 0, "attached": 0, "rejected": {}}, reused=len(set(map_) - set(m2)), attached=len(map_), stored=len(files_),
                       news_top=n_top, news_matched=sum(1 for c in news_pick[:60] if c["id"] in map_),
                       news_matched_pct=round(100.0 * sum(1 for c in news_pick[:60] if c["id"] in map_) / n_top, 1) if n_top else 0.0,
                       note="pictures stored by this morning-mode run are reused for the same article; the rest fetched under the live budget")
            return files_, map_, cnt
        try:
            live_report = build_live(os.path.join(a.out, "hourly"), ctx, now, run, health, live_clusters(ctx, now, seen_map), t_start, t_fetch, live_photos, mode="morning")
        except Exception as ex_:
            warnings.append(f"live doc failed: {ex_!r}"[:300])
    report = {"run": run, "mode": a.mode, "started": iso(now), "finished": iso(finished), "duration_s": (None if clock.frozen() else round(t_end - t_start, 1)), "fetch_s": (None if clock.frozen() else round(t_fetch - t_start, 1)),
              "sources": sorted(health, key=lambda h: h["id"]), "totals": {"sources": len(health), "data": status_ct.get("data", 0), "empty": status_ct.get("empty", 0), "blocked": status_ct.get("blocked", 0), "needs_key": status_ct.get("needs_key", 0), "error": status_ct.get("error", 0)},
              "items": counts_items, "item_total_unique": len(uniq), "label_counts": dict(labels), "docs": seal["docs"], "failed": seal["failed"], "missing": seal["missing"], "time_docs": seal["time_docs"], "bytes": total_bytes, "batches": len(files), "images": img_log, "static_once": bool(a.static_once), "collections": colls,
              "articles_fetched": len(ctx.articles), "clusters": {"all": len(cl_items), "multi_publisher": len(multi), "confirmed": confirmed_n, "singletons_dropped_from_docs": max(0, len(sing) - 120)} if True else None,
              "series": sorted(ctx.series), "number_rungs": (rung_counts if a.mode == "morning" else None), "clocks": len(ctx.clocks), "warnings": warnings, "ok_sources": n_ok, "total_sources": len(health),
              "stats": ctx.stats, "receipts": receipt_stats, "ladder": ladder, "official_heads": {pid: {k: v for k, v in (ex.get("official") or {}).items() if k != "text"} for pid, ex in sorted(ctx.pextra.items()) if ex.get("official")},
              "touches_you_yes": sum(1 for i in uniq.values() if (i.get("touches_you") or {}).get("yes")),
              "live": ({"doc_ids": live_report["doc_ids"], "dir": "hourly", "items": live_report["items"], "photos": live_report["photos"]} if live_report else None),
              "replay": bool(a.replay), "replay_dir": (os.path.abspath(a.replay) if a.replay else None), "replay_misses": sorted(F.misses)}
    with open(os.path.join(a.out, "report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=1)
    os.makedirs(a.state, exist_ok=True)
    json.dump({"days": days, "records": records_state, "last_good": prev_lg, "health": meta_health["sources"], "seen": seen_map,
               "series": ({k: v for k, v in ctx.series.items()} if a.mode == "morning" else state.get("series", {}))}, open(os.path.join(a.state, "last.json"), "w", encoding="utf-8"))
    print(f"[{a.mode}] run {run}: {n_ok}/{len(health)} sources with data; {len(docs)} docs, {len(files)} batches, {total_bytes} bytes; {round(t_end - t_start, 1)}s")
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
