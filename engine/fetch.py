#!/usr/bin/env python3
"""Watchman data engine (ENGINE.md sections 2, 3, 5, 9, 10, 11; ATTACK.md section 7).

Deterministic: no model calls. Fetches sources, normalizes them into ledger
documents, labels them (Primary record / Confirmed / Single source / Not fetched),
scores significance, clusters world news across publishers, and writes bulk-write
batches under out/<run_id>/. It never writes to a database, publishes, schedules,
or touches git. Keys are read from .keys.env at run time and never written anywhere.

Usage:
  python3 fetch.py --run morning|hourly [--out DIR] [--state DIR] [--replay CACHEDIR]
"""
import argparse, concurrent.futures as cf, csv, datetime as dt, email.utils, hashlib, html
import io, json, os, re, sys, threading, time, urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ENGINE_DIR = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
UA = "Watchman/0.1 (personal civic dashboard)"
TZ_NAME = "America/Indiana/Indianapolis"

# ---- hard limits measured for the target database ------------------------------------
MAX_DOCS = 25_000
MAX_DOC_BYTES = 262_144
DOC_LIMIT = 240 * 1024          # our own ceiling, under MAX_DOC_BYTES
BATCH_MAX_WRITES = 50
BATCH_MAX_BYTES = 900 * 1024    # under 1 MiB
BODY_CAP = 4 * 1024 * 1024      # B14
TIMEOUT = (8, 15)               # B14: 15 s read

LABELS = ("Primary record", "Confirmed", "Single source", "Analysis", "Conflict", "Not fetched")
STAMP_FIELDS = {"run_id", "fetched_at", "written_at", "content_sha256", "commit_sha", "last_ok_at"}

# ---- settings (ENGINE.md section 5 table defaults, plus engine-only retention keys) ------

ENGINE_SETTINGS = {
    "retention.events_per_day": 12,       # design: keeps a year inside the document and byte budget
    "retention.keep_full_days": 90,       # days kept with prose before a monthly roll-up folds them
    "retention.live_days": 7,
    "retention.max_age_days": 14,
    "cluster.window_h": 72,
    "cluster.min_shared_tokens": 3,
    "fetch.budget_min": 25,
    "fetch.workers": 16,
    "fetch.host_gap_s": 0.6,
    "live.max_breaking_per_day": 3,
    "retention.docs_ceiling": MAX_DOCS,
}


class Settings:
    def __init__(self, spec_settings, override=None):
        self.v = {}
        for k, d in spec_settings.items():
            self.v[k] = self._parse(d["default"])
        self.v.update(ENGINE_SETTINGS)
        if override:
            self.v.update(override)

    @staticmethod
    def _parse(s):
        s = str(s).strip()
        if re.fullmatch(r"\d+(\.\d+)?", s):
            return float(s) if "." in s else int(s)
        m = re.match(r"^(\d+(?:\.\d+)?)(?=\s*(\(|rows|px|$))", s)
        if m:
            x = m.group(1)
            return float(x) if "." in x else int(x)
        return s

    def __getitem__(self, k):
        return self.v[k]


def load_spec():
    with open(ENGINE_DIR / "spec" / "watchman.json") as f:
        return json.load(f)


# ---- keys: read at run time, never printed or stored ----------------------------------------

def load_keys(path=None):
    p = Path(path) if path else ENGINE_DIR / ".keys.env"
    keys = {}
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if v.strip():
                keys[k.strip()] = v.strip().strip('"').strip("'")
    return keys


class Scrubber:
    def __init__(self, keys):
        self.vals = sorted({v for v in keys.values() if len(v) >= 6}, key=len, reverse=True)

    def __call__(self, s):
        if not isinstance(s, str):
            return s
        for v in self.vals:
            if v in s:
                s = s.replace(v, "REDACTED")
        s = re.sub(r"(?i)((?:api_key|apikey|userid|token|key)=)[^&\s\"']+", r"\1REDACTED", s)
        return s


def scrub_obj(o, scrub):
    if isinstance(o, str):
        return scrub(o)
    if isinstance(o, list):
        return [scrub_obj(x, scrub) for x in o]
    if isinstance(o, dict):
        return {scrub(k) if isinstance(k, str) else k: scrub_obj(v, scrub) for k, v in o.items()}
    return o


# ---- time helpers ------------------------------------------------------------------------------

def utcnow():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso(d):
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if d else None


def parse_dt(s):
    if s is None or s == "":
        return None
    if isinstance(s, (int, float)):
        if s > 1e11:
            s = s / 1000.0
        return dt.datetime.fromtimestamp(s, dt.timezone.utc)
    s = str(s).strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
        if re.fullmatch(r"\d{8}T\d{6}Z", s):
            return dt.datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
        x = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(x)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        pass
    try:
        d = email.utils.parsedate_to_datetime(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def local_date(now):
    """Indianapolis calendar date of an instant (fixed offsets for Oct/Nov 2026 are handled by zoneinfo)."""
    try:
        from zoneinfo import ZoneInfo
        return now.astimezone(ZoneInfo(TZ_NAME)).date().isoformat()
    except Exception:
        return (now - dt.timedelta(hours=5)).date().isoformat()


def sha256_hex(b):
    return hashlib.sha256(b if isinstance(b, bytes) else b.encode()).hexdigest()


def canon(o):
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def host_of(url):
    return urllib.parse.urlsplit(url).hostname or ""


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:60]


# ====================================================================================================
# Fetching
# ====================================================================================================

KEYED_HOSTS = {
    "api.congress.gov": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.open.fec.gov": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.govinfo.gov": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.regulations.gov": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.si.edu": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.nasa.gov": ("param", "api_key", "DATA_GOV_API_KEY"),
    "api.stlouisfed.org": ("param", "api_key", "FRED_API_KEY"),
    "api.eia.gov": ("param", "api_key", "EIA_API_KEY"),
    "apps.bea.gov": ("param", "UserID", "BEA_API_KEY"),
    "v3.openstates.org": ("header", "X-API-KEY", "OPENSTATES_API_KEY"),
    "www.courtlistener.com": ("header", "Authorization", "COURTLISTENER_TOKEN"),
}
BOT_BLOCK = re.compile(r"(captcha|access denied|attention required|cf-chl|bot detection|are you a human|"
                       r"request unsuccessful|incapsula|akamai|pardon our interruption)", re.I)
HOST_GAP = {"api.gdeltproject.org": 5.5, "api.bls.gov": 1.0, "www.sec.gov": 0.5, "efts.sec.gov": 0.5}


def inject_key(url, keys):
    """Return (real_url, redacted_url, headers, need) - need names a missing key."""
    host = host_of(url)
    headers = {}
    rule = KEYED_HOSTS.get(host)
    if not rule or (host == "www.courtlistener.com" and "/api/" not in url):
        return url, redact_url(url), headers, None
    kind, name, env = rule
    val = keys.get(env)
    if not val:
        return url, redact_url(url), headers, env
    if kind == "header":
        headers[name] = ("Token " + val) if name == "Authorization" else val
        return url, redact_url(url), headers, None
    sp = urllib.parse.urlsplit(url)
    q = [(k, v) for k, v in urllib.parse.parse_qsl(sp.query, keep_blank_values=True) if k.lower() != name.lower()]
    q.append((name, val))
    real = urllib.parse.urlunsplit((sp.scheme, sp.netloc, sp.path, urllib.parse.urlencode(q, safe="[],:"), ""))
    red = urllib.parse.urlunsplit((sp.scheme, sp.netloc, sp.path,
                                   urllib.parse.urlencode([(k, ("REDACTED" if k == name else v)) for k, v in q], safe="[],:"), ""))
    return real, red, headers, None


def redact_url(url):
    return re.sub(r"(?i)((?:api_key|apikey|userid|token)=)[^&]+", r"\1REDACTED", url)


class Result:
    def __init__(self, **kw):
        self.id = kw.get("id")
        self.host = kw.get("host")
        self.url = kw.get("url")
        self.http = kw.get("http")
        self.status = kw.get("status", "not_fetched")
        self.error = kw.get("error")
        self.body = kw.get("body", b"")
        self.bytes = len(self.body)
        self.sha256 = sha256_hex(self.body) if self.body else None
        self.ms = kw.get("ms", 0)
        self.fetched_at = kw.get("fetched_at")
        self.ctype = kw.get("ctype", "")

    @property
    def ok(self):
        return self.status == "ok"

    def text(self):
        if not self.body:
            return ""
        enc = "utf-8"
        m = re.search(r"charset=([\w-]+)", self.ctype or "")
        if m:
            enc = m.group(1)
        try:
            return self.body.decode(enc, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.text().lstrip("﻿"))


class Fetcher:
    def __init__(self, keys, scrub, replay_dir=None, record_dir=None, deadline=None, host_gap=0.6):
        self.keys, self.scrub = keys, scrub
        self.replay_dir = Path(replay_dir) if replay_dir else None
        self.record_dir = Path(record_dir) if record_dir else None
        if self.record_dir:
            self.record_dir.mkdir(parents=True, exist_ok=True)
        self.deadline = deadline
        self.host_gap = host_gap
        self.last = {}
        self.lock = threading.Lock()
        self.local = threading.local()
        self.cache = {}  # url -> Result (dedupe identical URLs within a run)

    def _session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": UA, "Accept": "*/*"})
            self.local.s = s
        return s

    def _pace(self, host):
        gap = HOST_GAP.get(host, self.host_gap)
        with self.lock:
            now = time.time()
            wait = self.last.get(host, 0) + gap - now
            self.last[host] = max(now, self.last.get(host, 0) + gap)
        if wait > 0:
            time.sleep(wait)

    def _key_name(self, sid):
        return re.sub(r"[^A-Za-z0-9_.-]", "_", sid)

    def fetch(self, sid, url, extra_headers=None):
        if url in self.cache:
            r = self.cache[url]
            c = Result(id=sid, host=r.host, url=r.url, http=r.http, status=r.status, error=r.error,
                       body=r.body, ms=0, fetched_at=r.fetched_at, ctype=r.ctype)
            return c
        r = self._fetch(sid, url, extra_headers)
        self.cache[url] = r
        return r

    def _fetch(self, sid, url, extra_headers=None):
        host = host_of(url)
        if self.replay_dir:
            return self._replay(sid, url, host)
        real, red, headers, need = inject_key(url, self.keys)
        if need:
            return Result(id=sid, host=host, url=red, status="needs_key", error=f"{need} not set")
        headers.update(extra_headers or {})
        if self.deadline and time.time() > self.deadline:
            return Result(id=sid, host=host, url=red, status="skipped_budget", error="run time budget reached")
        res = None
        for attempt in range(3):
            self._pace(host)
            t0 = time.time()
            try:
                resp = self._session().get(real, headers=headers, timeout=TIMEOUT, stream=True, allow_redirects=True)
                body = b""
                too_large = False
                for chunk in resp.iter_content(65536):
                    body += chunk
                    if len(body) > BODY_CAP:
                        too_large = True
                        break
                resp.close()
                ms = int((time.time() - t0) * 1000)
                ctype = resp.headers.get("Content-Type", "")
                fa = iso(utcnow())
                if too_large:
                    res = Result(id=sid, host=host, url=red, http=resp.status_code, status="too_large",
                                 error="body over 4 MB cap", ms=ms, fetched_at=fa, ctype=ctype)
                    break
                code = resp.status_code
                if code == 429:
                    ra = resp.headers.get("Retry-After", "")
                    wait = int(ra) if ra.isdigit() else 5 * (attempt + 1)
                    res = Result(id=sid, host=host, url=red, http=429, status="rate_limited",
                                 error=f"429 Retry-After={ra or 'none'}", ms=ms, fetched_at=fa)
                    if wait > 30 or attempt == 2:
                        break
                    time.sleep(wait)
                    continue
                if code in (401, 403, 406, 451) or code == 429:
                    snippet = body[:3000].decode("utf-8", "replace")
                    status = "blocked" if (BOT_BLOCK.search(snippet) or code in (403, 406, 451)) and code != 401 else "http_error"
                    if code in (401, 403) and (headers or real != url):
                        status = "http_error"
                    res = Result(id=sid, host=host, url=red, http=code, status=status,
                                 error=f"HTTP {code}" + (" (bot block, not bypassed)" if status == "blocked" else ""),
                                 ms=ms, fetched_at=fa, ctype=ctype)
                    break  # never retry or bypass a block
                if code >= 500:
                    res = Result(id=sid, host=host, url=red, http=code, status="http_error", error=f"HTTP {code}",
                                 ms=ms, fetched_at=fa, ctype=ctype)
                    time.sleep(1.5 * (attempt + 1))
                    continue
                if code >= 400:
                    res = Result(id=sid, host=host, url=red, http=code, status="http_error", error=f"HTTP {code}",
                                 ms=ms, fetched_at=fa, ctype=ctype)
                    break
                res = Result(id=sid, host=host, url=red, http=code, status="ok", body=body, ms=ms, fetched_at=fa,
                             ctype=ctype)
                break
            except requests.exceptions.Timeout:
                res = Result(id=sid, host=host, url=red, status="timeout", error="timeout (8 s connect / 15 s read)",
                             ms=int((time.time() - t0) * 1000), fetched_at=iso(utcnow()))
            except requests.exceptions.RequestException as e:
                res = Result(id=sid, host=host, url=red, status="network_error",
                             error=self.scrub(type(e).__name__ + ": " + str(e))[:200],
                             ms=int((time.time() - t0) * 1000), fetched_at=iso(utcnow()))
                if "NameResolution" in str(e) or "Name or service" in str(e):
                    break
            if attempt < 2:
                time.sleep(1.0 + attempt)
        if self.record_dir and res is not None:
            self._record(sid, res)
        return res

    def _record(self, sid, r):
        k = self._key_name(sid)
        meta = dict(id=r.id, host=r.host, url=r.url, http=r.http, status=r.status, error=r.error, ms=r.ms,
                    fetched_at=r.fetched_at, ctype=r.ctype)
        (self.record_dir / f"{k}.json").write_text(json.dumps(meta))
        if r.body:
            (self.record_dir / f"{k}.body").write_bytes(r.body)

    def _replay(self, sid, url, host):
        k = self._key_name(sid)
        mp = self.replay_dir / f"{k}.json"
        if not mp.exists():
            return Result(id=sid, host=host, url=redact_url(url), status="not_fetched", error="absent from replay")
        m = json.loads(mp.read_text())
        bp = self.replay_dir / f"{k}.body"
        body = bp.read_bytes() if bp.exists() else b""
        return Result(id=sid, host=m["host"], url=m["url"], http=m["http"], status=m["status"], error=m["error"],
                      body=body, ms=m["ms"], fetched_at=m["fetched_at"], ctype=m.get("ctype", ""))


def norm_text(s):
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip().lower()


def field_proof(quote, text):
    """pass: quote present. schema: 'key: value' where the key (not the changing value) is present. fail. none."""
    if not quote or quote.strip() in ("-", ""):
        return "none"
    n, q = norm_text(text), norm_text(quote)
    if q in n:
        return "pass"
    m = re.match(r"^([\w\-. ]+?):\s*(.+)$", q)
    if m and m.group(1).strip() and m.group(1).strip() in n:
        return "schema"
    # the quote may have been captured from a rendering that adds spaces around markup
    if re.sub(r"[\s\W_]+", "", q) and re.sub(r"[\s\W_]+", "", q) in re.sub(r"[\s\W_]+", "", n):
        return "pass"
    return "fail"


def fetch_all(fetcher, jobs, workers, deadline):
    """jobs: list of dict(id,url,...). Sequential per host, parallel across hosts."""
    by_host = {}
    for j in jobs:
        by_host.setdefault(host_of(j["url"]), []).append(j)
    results = {}

    def work(host_jobs):
        out = {}
        for j in host_jobs:
            if deadline and time.time() > deadline:
                out[j["id"]] = Result(id=j["id"], host=host_of(j["url"]), url=redact_url(j["url"]),
                                      status="skipped_budget", error="run time budget reached")
                continue
            try:
                out[j["id"]] = fetcher.fetch(j["id"], j["url"], j.get("headers"))
            except Exception as e:  # a bug in one fetch must not kill the run
                out[j["id"]] = Result(id=j["id"], host=host_of(j["url"]), url=redact_url(j["url"]),
                                      status="network_error", error=fetcher.scrub(f"{type(e).__name__}: {e}")[:200])
        return out

    hosts = sorted(by_host, key=lambda h: -len(by_host[h]))
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for out in ex.map(work, [by_host[h] for h in hosts]):
            results.update(out)
    return results


# ====================================================================================================
# Source catalogue
# ====================================================================================================

HOURLY_IDS = ["18", "205", "17", "W404", "905", "20", "802", "903", "W403", "803", "904", "01", "02", "W418",
              "906", "03", "04", "W401", "801", "902", "34", "28", "W410", "16"]

PRIMARY_RE = re.compile(r"(\.gov$|\.gov\.[a-z]{2}$|\.mil$|\.int$|^(www\.)?un\.org$|^news\.un\.org$|gdacs\.org$|"
                        r"worldbank\.org$|oecd\.org$|ecb\.europa\.eu$|europa\.eu$|bankofcanada\.ca$|"
                        r"bankofengland\.co\.uk$|freddiemac\.com$|^(www\.)?in\.gov$|indy\.gov$|"
                        r"thespacedevs\.com$|unhcr\.org$|hungermapdata\.org$|fao\.org$|ncsl\.org$)")
# republishers and aggregators: never "Primary record"
SECONDARY_HOSTS = ("stlouisfed.org", "courtlistener.com", "openstates.org", "voteview.com", "wikipedia.org",
                   "wikidata.org", "gdeltproject.org", "stooq.com", "coingecko.com", "ourworldindata.org",
                   "polymarket.com", "metaculus.com", "githubusercontent.com", "open-meteo.com",
                   "frankfurter.dev", "electionguide.org", "humdata.org")
# gov/institution hosts whose RSS stories are news about the world, not the issuer's own act:
NEWS_CLASS_IDS = {"W410", "x-un-news", "x-who-news"}

RECORD_KINDS = {"presidential_doc", "fr_doc", "fr_inspection", "bill_presented", "nws_alert", "quake", "fema_declaration",
                "gdacs_event", "who_don", "recall", "eonet", "travel_advisory", "cisa_kev", "reg_comment",
                "central_bank", "opinion", "state_bill", "river"}


def publisher_id(host):
    h = (host or "").lower()
    h = re.sub(r"^(www\d?|m|feeds?|rss|api|news|rss2|feeds2)\.", "", h)
    aliases = {"bbci.co.uk": "bbc.co.uk", "bbc.com": "bbc.co.uk", "a.dj.com": "wsj.com", "dj.com": "wsj.com",
               "feeds.a.dj.com": "wsj.com", "rss.nytimes.com": "nytimes.com", "rss.politico.com": "politico.com"}
    if host in aliases:
        return aliases[host]
    parts = h.split(".")
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "gov", "ac") and len(parts[-1]) == 2:
        h = ".".join(parts[-3:])
    elif len(parts) >= 2:
        h = ".".join(parts[-2:])
    return aliases.get(h, h)


def load_sources(spec, extra_path):
    rows = []
    seen = set()
    for r in spec["sources"]:
        if r["verdict"] not in ("VERIFIED", "NEEDS-KEY"):
            continue
        if r["id"] in seen:  # ids are unique but be safe
            continue
        seen.add(r["id"])
        rows.append({"id": r["id"], "url": r["url"], "host": r["host"], "verdict": r["verdict"],
                     "quote": r.get("quote", ""), "origin": "spec", "region": "", "name": r["id"]})
    for line in Path(extra_path).read_text().splitlines():
        parts = line.strip().split("|", 2)
        if len(parts) != 3:
            continue
        region, name, url = parts
        sid = "x-" + slug(name)
        rows.append({"id": sid, "url": url, "host": host_of(url), "verdict": "EXTRA", "quote": "",
                     "origin": "extra", "region": region, "name": name})
    # additional keyed / computed calls the spec's keys section makes possible
    extras = [
        ("k-fred-unrate", "https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE&file_type=json&sort_order=desc&limit=14"),
        ("k-fred-cpi", "https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&file_type=json&sort_order=desc&limit=14"),
        ("k-fred-mortgage", "https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&file_type=json&sort_order=desc&limit=14"),
        ("k-fred-gas", "https://api.stlouisfed.org/fred/series/observations?series_id=GASREGW&file_type=json&sort_order=desc&limit=14"),
        ("k-eia-gas-midwest", "https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&facets[product][]=EPMR&facets[duoarea][]=R20&sort[0][column]=period&sort[0][direction]=desc&length=14"),
        ("k-eia-gas-us", "https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&facets[product][]=EPMR&facets[duoarea][]=NUS&sort[0][column]=period&sort[0][direction]=desc&length=14"),
        ("k-fr-presdocu", "https://www.federalregister.gov/api/v1/documents.json?conditions[type][]=PRESDOCU&order=newest&per_page=20"),
        ("k-fr-prorule", "https://www.federalregister.gov/api/v1/documents.json?conditions[type][]=PRORULE&order=newest&per_page=25&fields[]=title&fields[]=html_url&fields[]=document_number&fields[]=publication_date&fields[]=comments_close_on&fields[]=abstract&fields[]=agency_names&fields[]=type"),
        ("k-os-indiana", "https://v3.openstates.org/bills?jurisdiction=Indiana&sort=updated_desc&per_page=10"),
        ("k-cl-scotus", "https://www.courtlistener.com/api/rest/v4/search/?type=o&order_by=dateFiled%20desc&court=scotus&page_size=5"),
        ("k-congress-bills", "https://api.congress.gov/v3/bill?format=json&limit=10&sort=updateDate+desc"),
        ("k-regs-comments", "https://api.regulations.gov/v4/documents?filter[documentType]=Proposed%20Rule&sort=-lastModifiedDate&page[size]=10"),
        ("k-bea-gdp", "https://apps.bea.gov/api/data?method=GetData&datasetname=NIPA&TableName=T10101&Frequency=Q&Year=2025,2026&ResultFormat=JSON"),
        ("k-nws-all", "https://api.weather.gov/alerts/active?severity=Extreme,Severe"),
    ]
    have = {r["url"] for r in rows}
    for sid, url in extras:
        if url not in have:
            rows.append({"id": sid, "url": url, "host": host_of(url), "verdict": "KEYED", "quote": "",
                         "origin": "keyed", "region": "", "name": sid})
    return rows


# ====================================================================================================
# Parsing helpers and adapters
# ====================================================================================================

def strip_ns(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def parse_feed(text):
    """Small RSS 2.0 / Atom / RDF parser. Returns list of dict(title, link, published, summary, guid) or None."""
    head = text.lstrip()[:600].lower()
    if not any(t in head for t in ("<rss", "<feed", "<rdf:rdf", "<?xml")) and "<channel" not in text[:3000].lower():
        return None
    try:
        root = ET.fromstring(re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text).encode("utf-8"))
    except ET.ParseError:
        try:
            root = ET.fromstring(re.sub(r"&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;",
                                        re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)).encode("utf-8"))
        except ET.ParseError:
            return None
    if strip_ns(root.tag) not in ("rss", "feed", "RDF"):
        return None
    items = []
    for el in root.iter():
        t = strip_ns(el.tag)
        if t not in ("item", "entry"):
            continue
        d = {}
        for ch in el:
            ct = strip_ns(ch.tag)
            if ct == "title":
                d["title"] = "".join(ch.itertext()).strip()
            elif ct == "link":
                d["link"] = (ch.get("href") or (ch.text or "")).strip() if not d.get("link") or ch.get("rel") == "alternate" else d["link"]
            elif ct in ("pubDate", "published", "updated", "date", "issued") and not d.get("published"):
                d["published"] = (ch.text or "").strip()
            elif ct in ("description", "summary", "content", "encoded") and not d.get("summary"):
                d["summary"] = re.sub(r"<[^>]+>", " ", html.unescape("".join(ch.itertext()))).strip()
            elif ct == "enclosure" and (ch.get("type") or "").startswith("image") and not d.get("image"):
                d["image"] = ch.get("url")
            elif ct in ("content", "thumbnail") and ch.get("url") and not d.get("image") and (ch.get("medium") == "image" or ct == "thumbnail" or (ch.get("type") or "").startswith("image")):
                d["image"] = ch.get("url")
            elif ct in ("guid", "id") and not d.get("guid"):
                d["guid"] = (ch.text or "").strip()
        if d.get("title"):
            d["summary"] = re.sub(r"\s+", " ", d.get("summary", ""))[:600]
            items.append(d)
    return items


def mk_item(sid, kind, title, url, published, summary="", cls="record", **kw):
    it = {"source_id": sid, "kind": kind, "title": re.sub(r"\s+", " ", (title or "")).strip()[:300],
          "url": url or "", "published": iso(parse_dt(published)) if published else None,
          "summary": re.sub(r"\s+", " ", (summary or "")).strip()[:500], "cls": cls}
    it.update(kw)
    return it


def feed_kind_for(src):
    sid = src["id"]
    return {"03": ("presidential_action", "record"), "04": ("bill_presented", "record"),
            "901": ("central_bank", "record"), "W406": ("central_bank", "record"),
            "x-state-dept-travel-advisories": ("travel_advisory", "record"),
            "x-gdacs-disasters": ("gdacs_event", "record"),
            "x-noaa-swpc-space-weather": ("swpc", "record")}.get(sid, ("news", "news"))


def adapter_feed(src, r, ctx):
    items = parse_feed(r.text())
    if items is None:
        return None
    kind, cls = feed_kind_for(src)
    out = []
    for e in items[:60]:
        it = mk_item(src["id"], kind, e["title"], e.get("link"), e.get("published"), e.get("summary"), cls,
                     guid=e.get("guid"), region=src.get("region", ""), image_url=e.get("image"))
        if kind == "presidential_action":
            it["kind"] = "presidential_doc"
            it["stage"] = "signed"
        out.append(it)
    return out


def adapter_fr(src, r, ctx):
    j = r.json()
    res = j.get("results") if isinstance(j, dict) else None
    if res is None:
        raise ValueError("expected field 'results' absent")
    out = []
    for d in res[:30]:
        title = d.get("title") or ""
        typ = d.get("type") or ""
        kind = "presidential_doc" if "Presidential" in typ or src["id"] in ("01", "1204", "k-fr-presdocu") else "fr_doc"
        if src["id"] == "02":
            kind = "fr_inspection"
        it = mk_item(src["id"], kind, title, d.get("html_url"), d.get("publication_date") or d.get("filed_at"),
                     d.get("abstract") or "", "record", doc_number=d.get("document_number"),
                     stage="published (Federal Register)" if kind == "presidential_doc" else "")
        cc = d.get("comments_close_on")
        if cc:
            it["cutoff"] = cc
            it["kind"] = "fr_doc"
        out.append(it)
    return out


def adapter_usgs_quakes(src, r, ctx):
    j = r.json()
    feats = j.get("features")
    if feats is None:
        raise ValueError("expected field 'features' absent")
    out = []
    for f in feats:
        p = f.get("properties", {})
        mag = p.get("mag")
        if mag is None:
            continue
        alert = p.get("alert")
        if mag < 5.0 and not alert and src["id"] in ("17", "1201"):
            continue
        out.append(mk_item(src["id"], "quake", f"M{mag:.1f} earthquake: {p.get('place', 'unknown place')}", p.get("url"),
                           p.get("time"), "", "record", mag=mag, alert=alert, place_text=p.get("place") or ""))
    out.sort(key=lambda x: -x["mag"])
    return out[:25]


def adapter_nws(src, r, ctx):
    j = r.json()
    feats = j.get("features")
    if feats is None:
        raise ValueError("expected field 'features' absent")
    now = ctx["now"]
    out = []
    for f in feats:
        p = f.get("properties", {})
        if p.get("messageType") == "Cancel":
            continue
        end = parse_dt(p.get("ends") or p.get("expires"))
        if end and end <= now:   # expired alerts are never New (ENGINE.md section 9)
            continue
        ugc = (p.get("geocode") or {}).get("UGC") or []
        out.append(mk_item(src["id"], "nws_alert", p.get("headline") or p.get("event") or "NWS alert",
                           p.get("@id") or f.get("id"), p.get("sent") or p.get("effective"), p.get("description", "")[:300],
                           "record", severity=p.get("severity"), event=p.get("event"), area=p.get("areaDesc", "")[:160],
                           ugc=list(ugc)[:12], ends=iso(end)))
    return out[:60]


def adapter_fema(src, r, ctx):
    j = r.json()
    rows = j.get("DisasterDeclarationsSummaries")
    if rows is None:
        raise ValueError("expected field 'DisasterDeclarationsSummaries' absent")
    seen, out = set(), []
    for d in rows:
        key = (d.get("disasterNumber"), d.get("state"))
        if key in seen:
            continue
        seen.add(key)
        num = f"{d.get('declarationType', '')}-{d.get('disasterNumber')}"
        out.append(mk_item(src["id"], "fema_declaration",
                           f"{d.get('declarationType', '')}-{d.get('disasterNumber')} {d.get('state')}: {d.get('declarationTitle', '')}",
                           f"https://www.fema.gov/disaster/{d.get('disasterNumber')}", d.get("declarationDate"),
                           f"{d.get('incidentType', '')} in {d.get('state')}", "record", state=d.get("state"),
                           dtype=d.get("declarationType"), natural_key=f"fema:{d.get('disasterNumber')}:{d.get('state')}"))
    return out[:20]


def adapter_gdacs(src, r, ctx):
    j = r.json()
    feats = j.get("features")
    if feats is None:
        raise ValueError("expected field 'features' absent")
    out = []
    for f in feats[:40]:
        p = f.get("properties", {})
        lvl = (p.get("alertlevel") or "").title()
        name = p.get("name") or p.get("eventname") or "GDACS event"
        url = (p.get("url") or {}).get("report") if isinstance(p.get("url"), dict) else p.get("url")
        out.append(mk_item(src["id"], "gdacs_event", f"{lvl} alert: {name}", url, p.get("fromdate") or p.get("datemodified"),
                           p.get("description") or p.get("htmldescription", ""), "record", alert=lvl,
                           country=p.get("country"), natural_key=f"gdacs:{p.get('eventtype')}:{p.get('eventid')}"))
    return out


def adapter_who(src, r, ctx):
    j = r.json()
    val = j.get("value")
    if val is None:
        raise ValueError("expected field 'value' absent")
    out = []
    for d in val[:10]:
        url = "https://www.who.int/emergencies/disease-outbreak-news/item/" + (d.get("UrlName") or d.get("ItemDefaultUrl", "").strip("/"))
        out.append(mk_item(src["id"], "who_don", d.get("Title") or d.get("OverrideTitle") or "WHO notice", url,
                           d.get("PublicationDateAndTime") or d.get("PublicationDate"),
                           re.sub(r"<[^>]+>", " ", d.get("Summary") or ""), "record", natural_key=f"who:{d.get('DonId')}"))
    return out


def adapter_gdelt(src, r, ctx):
    t = r.text()
    if not t.lstrip().startswith("{"):
        raise ValueError("not JSON: " + t[:80].replace("\n", " "))
    j = json.loads(t)
    arts = j.get("articles")
    if arts is None:
        raise ValueError("expected field 'articles' absent")
    out = []
    for a in arts[:75]:
        out.append(mk_item(src["id"], "news", a.get("title"), a.get("url"), a.get("seendate"), "", "news",
                           pub_host=a.get("domain"), region="World", lang=a.get("language")))
    return out


def adapter_eonet(src, r, ctx):
    j = r.json()
    evs = j.get("events")
    if evs is None:
        raise ValueError("expected field 'events' absent")
    out = []
    for e in evs[:20]:
        g = (e.get("geometry") or [{}])[-1]
        out.append(mk_item(src["id"], "eonet", e.get("title"), e.get("link"), g.get("date"), "", "record",
                           natural_key="eonet:" + str(e.get("id"))))
    return out


def adapter_nhtsa(src, r, ctx):
    j = r.json()
    res = j.get("results") if "results" in j else j.get("Results")
    if res is None:
        raise ValueError("expected field 'results' absent")
    out = []
    for d in res[:5]:
        rd = d.get("ReportReceivedDate") or ""
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", rd)
        pub = f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else rd
        out.append(mk_item(src["id"], "recall", f"Recall {d.get('NHTSACampaignNumber')}: {d.get('Component', '')} ({d.get('Manufacturer', '')})",
                           f"https://www.nhtsa.gov/recalls?nhtsaId={d.get('NHTSACampaignNumber')}", pub, d.get("Summary", ""),
                           "record", natural_key="nhtsa:" + str(d.get("NHTSACampaignNumber"))))
    return out


def adapter_cpsc(src, r, ctx):
    j = r.json()
    out = []
    for d in (j if isinstance(j, list) else [])[:5]:
        out.append(mk_item(src["id"], "recall", d.get("Title"), d.get("URL"), d.get("RecallDate"), "", "record",
                           natural_key="cpsc:" + str(d.get("RecallID"))))
    return out


def adapter_openstates(src, r, ctx):
    j = r.json()
    res = j.get("results")
    if res is None:
        raise ValueError("expected field 'results' absent")
    out = []
    for b in res[:10]:
        out.append(mk_item(src["id"], "state_bill", f"{b.get('identifier')}: {b.get('title')}", b.get("openstates_url"),
                           b.get("latest_action_date") or b.get("updated_at"), (b.get("latest_action_description") or ""), "record",
                           state_hint="indiana", natural_key="os:" + str(b.get("id"))))
    return out


def adapter_courtlistener(src, r, ctx):
    j = r.json()
    res = j.get("results")
    if res is None:
        return None
    out = []
    for d in res[:5]:
        out.append(mk_item(src["id"], "opinion", d.get("caseName") or "opinion",
                           "https://www.courtlistener.com" + (d.get("absolute_url") or ""), d.get("dateFiled"), "", "record",
                           natural_key="cl:" + str(d.get("cluster_id"))))
    return out


def adapter_congress_bills(src, r, ctx):
    j = r.json()
    bills = j.get("bills")
    if bills is None:
        raise ValueError("expected field 'bills' absent")
    out = []
    for b in bills[:10]:
        la = b.get("latestAction") or {}
        out.append(mk_item(src["id"], "bill_action", f"{b.get('type')}{b.get('number')}: {b.get('title')}",
                           f"https://www.congress.gov/bill/{b.get('congress')}th-congress/{str(b.get('originChamber', '')).lower()}-bill/{b.get('number')}",
                           la.get("actionDate") or b.get("updateDate"), la.get("text", ""), "record",
                           natural_key=f"congress:{b.get('congress')}:{b.get('type')}{b.get('number')}"))
    return out


def adapter_regs(src, r, ctx):
    j = r.json()
    data = j.get("data")
    if data is None:
        raise ValueError("expected field 'data' absent")
    out = []
    for d in data[:10]:
        a = d.get("attributes", {})
        it = mk_item(src["id"], "reg_comment", a.get("title"), f"https://www.regulations.gov/document/{d.get('id')}",
                     a.get("postedDate") or a.get("lastModifiedDate"), "", "record",
                     natural_key="regs:" + str(d.get("id")), agency=a.get("agencyId"))
        if a.get("commentEndDate"):
            it["cutoff"] = a["commentEndDate"]
        out.append(it)
    return out


def adapter_cisa(src, r, ctx):
    j = r.json()
    v = j.get("vulnerabilities")
    if v is None:
        raise ValueError("expected field 'vulnerabilities' absent")
    out = []
    for d in sorted(v, key=lambda x: x.get("dateAdded", ""), reverse=True)[:5]:
        out.append(mk_item(src["id"], "cisa_kev", f"CISA KEV: {d.get('cveID')} {d.get('vendorProject')} {d.get('product')}",
                           "https://www.cisa.gov/known-exploited-vulnerabilities-catalog", d.get("dateAdded"),
                           d.get("shortDescription", ""), "record", natural_key="kev:" + str(d.get("cveID"))))
    return out


def adapter_members(src, r, ctx):
    """House Clerk MemberData.xml and congress-legislators JSON -> Indiana members (district IDs only)."""
    ents = []
    t = r.text()
    if src["id"] == "05":
        root = ET.fromstring(t.encode("utf-8"))
        for m in root.iter("member"):
            info = m.find("member-info")
            sd = (m.findtext("statedistrict") or "").strip()
            if not sd.startswith("IN"):
                continue
            if info is None:
                continue
            ents.append({"etype": "member", "eid": info.findtext("bioguideID") or sd,
                         "name": info.findtext("official-name") or info.findtext("namelist"),
                         "party": info.findtext("party"), "chamber": "House", "state": "IN", "district": sd,
                         "src": src["id"]})
    elif src["id"] == "1002":
        for p in json.loads(t):
            term = p["terms"][-1]
            if term.get("state") != "IN":
                continue
            n = p["name"]
            ents.append({"etype": "member", "eid": p["id"]["bioguide"], "name": n.get("official_full") or f"{n.get('first')} {n.get('last')}",
                         "party": term.get("party"), "chamber": "Senate" if term["type"] == "sen" else "House", "state": "IN",
                         "district": f"IN{int(term['district']):02d}" if term["type"] == "rep" and term.get("district") is not None else "IN",
                         "src": src["id"], "url": term.get("url")})
    return ents


# ---- series adapters --------------------------------------------------------------------------

def S(sid, label, unit, obs, src, freq="", extra=None):
    """obs: list of (period, value-string) newest first."""
    return {"series_id": sid, "label": label, "unit": unit, "obs": obs[:14], "src": src, "freq": freq, **(extra or {})}


def num_str(x):
    try:
        f = float(str(x).replace(",", ""))
    except Exception:
        return None
    return format(f, "f").rstrip("0").rstrip(".") if "." in format(f, "f") else format(f, "f")


def series_fred(src, r, ctx):
    j = r.json()
    obs = j.get("observations")
    if obs is None:
        raise ValueError("expected field 'observations' absent")
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(src["url"]).query)
    fid = q.get("series_id", ["?"])[0]
    names = {"UNRATE": ("Unemployment rate", "percent"), "CPIAUCSL": ("CPI, all urban consumers, SA", "index 1982-84=100"),
             "MORTGAGE30US": ("30-year mortgage rate (Freddie Mac via FRED)", "percent"),
             "GASREGW": ("US regular gasoline, weekly", "dollars per gallon")}
    lab, unit = names.get(fid, (fid, ""))
    o = [(x["date"], num_str(x["value"])) for x in obs if num_str(x.get("value")) is not None]
    o.sort(reverse=True)
    return [S("fred-" + fid.lower(), lab, unit, o, src["id"], extra={"issuer": "FRED republishes", "republisher": True})]


class SourceLimit(Exception):
    """The source answered 200 but refused the request (its own quota)."""


def series_bls(src, r, ctx):
    j = r.json()
    if str(j.get("status")) == "REQUEST_NOT_PROCESSED":
        raise SourceLimit("BLS keyless v1 daily limit: " + "; ".join(j.get("message") or [])[:120])
    ser = ((j.get("Results") or {}).get("series") or [None])[0]
    if not ser:
        raise ValueError("expected field 'Results.series' absent")
    sid = ser["seriesID"]
    o = []
    for d in ser.get("data", []):
        p = d.get("period", "")
        if not p.startswith("M") or p == "M13":
            continue
        o.append((f"{d['year']}-{p[1:]}", num_str(d.get("value"))))
    o = [x for x in o if x[1] is not None]
    o.sort(reverse=True)
    names = {"LNS14000000": ("US unemployment rate, SA (BLS)", "percent"),
             "CUUR0000SA0": ("CPI-U all items, NSA (BLS)", "index 1982-84=100")}
    lab, unit = names.get(sid, (f"BLS {sid}", "percent" if sid.startswith("LAS") else ""))
    if sid.startswith("LASST"):
        lab = f"State unemployment rate, FIPS {sid[5:7]} (BLS LAUS)"
    return [S("bls-" + sid.lower(), lab, unit, o, src["id"], "monthly",
              {"place_fips": sid[5:7] if sid.startswith("LASST") else None})]


def series_eia(src, r, ctx):
    j = r.json()
    data = ((j.get("response") or {}).get("data"))
    if data is None:
        raise ValueError("expected field 'response.data' absent")
    if not data:
        return []
    area = data[0].get("duoarea", "?")
    o = [(d["period"], num_str(d.get("value"))) for d in data if num_str(d.get("value")) is not None]
    o.sort(reverse=True)
    lab = {"R20": "Midwest regular gasoline (EIA)", "NUS": "US regular gasoline (EIA)"}.get(area, f"EIA gasoline {area}")
    return [S("eia-gas-" + area.lower(), lab, "dollars per gallon", o, src["id"], "weekly")]


def series_csv_last(src, r, ctx):
    rows = list(csv.reader(io.StringIO(r.text())))
    if len(rows) < 2:
        raise ValueError("csv has no data rows")
    head = [h.strip().strip('"') for h in rows[0]]
    sid = src["id"]
    if sid == "201":   # Freddie Mac PMMS history
        col = head.index("pmms30") if "pmms30" in head else 1
        data = [(x[0], num_str(x[col])) for x in rows[1:] if len(x) > col and num_str(x[col]) is not None]
        o = []
        for d, v in data:
            try:
                dd = parse_dt(d) or (dt.datetime.strptime(d, "%m/%d/%Y").replace(tzinfo=dt.timezone.utc) if "/" in d else None)
            except ValueError:
                dd = None
            if dd:
                o.append((dd.date().isoformat(), v))
        o.sort(reverse=True)
        o = o[:14]
        return [S("freddie-pmms30", "30-year fixed mortgage rate (Freddie Mac PMMS)", "percent", o, sid, "weekly")]
    if sid in ("1006", "1007"):
        name = head[1] if len(head) > 1 else sid
        data = [(x[0], num_str(x[1])) for x in rows[1:] if len(x) > 1 and num_str(x[1]) is not None]
        o = sorted(data[-14:], reverse=True)
        return [S("fredcsv-" + name.lower(), f"{name} (FRED graph csv)", "", o, sid, extra={"republisher": True})]
    return []


def series_worldbank(src, r, ctx):
    j = r.json()
    if not (isinstance(j, list) and len(j) == 2 and j[1] is not None):
        raise ValueError("expected World Bank [meta, data] absent")
    out = []
    groups = {}
    for d in j[1]:
        if d.get("value") is None:
            continue
        key = (d["country"]["id"], d["indicator"]["id"])
        groups.setdefault(key, (d["country"]["value"], d["indicator"]["value"], []))[2].append((d["date"], num_str(d["value"])))
    for (cid, ind), (cname, iname, obs) in groups.items():
        obs.sort(reverse=True)
        out.append(S(f"wb-{cid.lower()}-{ind.lower()}", f"{iname}, {cname} (World Bank)", "", obs, src["id"], "annual",
                     {"place_hint": cname}))
    return out


def series_treasury_fiscal(src, r, ctx):
    j = r.json()
    data = j.get("data")
    if data is None:
        raise ValueError("expected field 'data' absent")
    out = []
    if not data:
        return out
    d0 = data[0]
    pick = [k for k in ("tot_pub_debt_out_amt", "debt_held_public_amt", "close_today_bal", "open_today_bal", "avg_interest_rate_amt") if k in d0]
    for k in pick[:2]:
        o = [(d["record_date"], num_str(d.get(k))) for d in data if num_str(d.get(k)) is not None][:14]
        out.append(S(f"fiscal-{slug(src['id'])}-{k}", f"Treasury {k}", "USD (millions for DTS)" if "bal" in k else "USD", o, src["id"]))
    return out


def series_ecb(src, r, ctx):
    j = r.json()
    ds = j["dataSets"][0]["series"]
    key = next(iter(ds))
    obs = ds[key]["observations"]
    vals = j["structure"]["dimensions"]["observation"][0]["values"]
    o = [(vals[int(i)]["id"], num_str(v[0])) for i, v in obs.items()]
    o.sort(reverse=True)
    return [S("ecb-usd-eur", "US dollars per euro (ECB reference)", "USD per EUR", o, src["id"], "daily")]


def series_boc(src, r, ctx):
    j = r.json()
    obs = j.get("observations")
    if obs is None:
        raise ValueError("expected field 'observations' absent")
    out = []
    for sname in (j.get("seriesDetail") or {}):
        o = [(d["d"], num_str(d[sname]["v"])) for d in obs if sname in d and num_str(d[sname].get("v")) is not None]
        o.sort(reverse=True)
        out.append(S("boc-" + sname.lower(), f"Bank of Canada {sname}", "", o, src["id"]))
    return out


def series_frank(src, r, ctx):
    j = r.json()
    rates = j.get("rates")
    if rates is None:
        raise ValueError("expected field 'rates' absent")
    return [S("fx-usd-" + c.lower(), f"{c} per US dollar (Frankfurter, ECB-based)", f"{c} per USD", [(j.get("date"), num_str(rates[c]))], src["id"],
              extra={"republisher": True}) for c in ("EUR", "CAD", "MXN", "CNY", "JPY") if c in rates]


def series_nyfed(src, r, ctx):
    j = r.json()
    rr = j.get("refRates")
    if rr is None:
        raise ValueError("expected field 'refRates' absent")
    return [S("nyfed-" + x["type"].lower(), f"{x['type']} (NY Fed)", "percent", [(x["effectiveDate"], num_str(x["percentRate"]))], src["id"], "daily")
            for x in rr if x.get("percentRate") is not None][:4]


def series_river(src, r, ctx):
    j = r.json()
    ts = ((j.get("value") or {}).get("timeSeries"))
    if ts is None:
        raise ValueError("expected field 'value.timeSeries' absent")
    out = []
    for t in ts:
        vals = (t.get("values") or [{}])[0].get("value") or []
        if not vals:
            continue
        v = vals[-1]
        name = t["variable"]["variableName"]
        unit = (t["variable"].get("unit") or {}).get("unitCode", "")
        out.append(S("usgs-03353000-" + slug(name)[:20], f"White River at Indianapolis: {name}", unit, [(v["dateTime"], num_str(v["value"]))], src["id"], "15 min"))
    return out


def series_misc(src, r, ctx):
    sid, t = src["id"], r.text()
    if sid in ("61", "112", "207"):
        m = re.search(r"(\d{2}(?:\.\d)?)\s*cents(?: per mile|/mile)", t)
        if not m:
            raise ValueError("mileage rate phrase absent")
        return [S("irs-mileage-business", "IRS standard mileage rate, business (cents per mile)", "cents per mile",
                  [(ctx["today"], m.group(1))], sid, extra={"asof_note": "page text, no effective date parsed"})]
    if sid == "40":
        m = re.search(r"<regular>([\d.]+)</regular>", t)
        if m:
            return [S("doe-fueleconomy-regular", "US regular gasoline (fueleconomy.gov)", "dollars per gallon", [(ctx["today"], m.group(1))], sid)]
    if sid == "1010":
        j = json.loads(t)
        c = j["current"]
        return [S("aqi-indianapolis", "US AQI Indianapolis (Open-Meteo model)", "US AQI", [(c["time"], num_str(c["us_aqi"]))], sid, "hourly",
                  {"republisher": True})]
    if sid.startswith("x-stooq"):
        rows = list(csv.DictReader(io.StringIO(t)))
        if rows and rows[0].get("Close") not in (None, "N/D"):
            return [S("stooq-spx", "S&P 500 (Stooq)", "index points", [(rows[0]["Date"], num_str(rows[0]["Close"]))], sid, "daily", {"republisher": True})]
    if sid.startswith("x-coingecko"):
        j = json.loads(t)
        return [S("coingecko-btc", "Bitcoin in USD (CoinGecko)", "USD", [(ctx["today"], num_str(j["bitcoin"]["usd"]))], sid, extra={"republisher": True})]
    if sid.startswith("x-treasury-yields"):
        rows = list(csv.reader(io.StringIO(t)))
        if len(rows) > 1:
            h, last = rows[0], rows[1]
            if "10 Yr" in h:
                return [S("treasury-10y", "US Treasury 10-year par yield", "percent", [(last[0], num_str(last[h.index("10 Yr")]))], sid, "daily")]
    return []


def series_bea(src, r, ctx):
    j = r.json()
    data = ((j.get("BEAAPI") or {}).get("Results") or {}).get("Data")
    if data is None:
        raise ValueError("expected BEAAPI.Results.Data absent")
    o = [(d["TimePeriod"], num_str(d["DataValue"])) for d in data if d.get("LineNumber") == "1" and num_str(d.get("DataValue")) is not None]
    o.sort(reverse=True)
    return [S("bea-gdp-growth", "Real GDP percent change, quarterly annualized (BEA T10101)", "percent", o, src["id"], "quarterly")] if o else []


def clocks_fec(src, r, ctx):
    j = r.json()
    res = j.get("results")
    if res is None:
        raise ValueError("expected field 'results' absent")
    out = []
    for d in res:
        if d.get("election_date") and "General" in (d.get("election_type_full") or ""):
            out.append({"clock_id": "election-" + d["election_date"], "what": f"{d.get('election_type_full')}, {d.get('election_state')}",
                        "closes_on": d["election_date"], "time_known": False, "stake": 3, "prox": 1.0, "irrev": 2, "src": src["id"]})
    return out


DATA_ADAPTERS = {  # id -> (adapter, product)  product: items | series | entities | clocks
    "01": (adapter_fr, "items"), "02": (adapter_fr, "items"), "W418": (adapter_fr, "items"), "1204": (adapter_fr, "items"),
    "906": (adapter_fr, "items"), "1031": (adapter_fr, "items"), "k-fr-presdocu": (adapter_fr, "items"),
    "k-fr-prorule": (adapter_fr, "items"),
    "17": (adapter_usgs_quakes, "items"), "1201": (adapter_usgs_quakes, "items"), "W404": (adapter_usgs_quakes, "items"),
    "905": (adapter_usgs_quakes, "items"), "716": (adapter_usgs_quakes, "items"),
    "18": (adapter_nws, "items"), "205": (adapter_nws, "items"), "k-nws-all": (adapter_nws, "items"),
    "20": (adapter_fema, "items"), "802": (adapter_fema, "items"), "903": (adapter_fema, "items"),
    "W403": (adapter_gdacs, "items"), "803": (adapter_gdacs, "items"), "904": (adapter_gdacs, "items"),
    "W401": (adapter_who, "items"), "801": (adapter_who, "items"), "902": (adapter_who, "items"),
    "34": (adapter_gdelt, "items"), "28": (adapter_eonet, "items"), "21": (adapter_nhtsa, "items"),
    "22": (adapter_cpsc, "items"), "k-os-indiana": (adapter_openstates, "items"),
    "k-cl-scotus": (adapter_courtlistener, "items"), "k-congress-bills": (adapter_congress_bills, "items"),
    "k-regs-comments": (adapter_regs, "items"), "1016": (adapter_regs, "items"),
    "x-cisa-kev": (adapter_cisa, "items"),
    "05": (adapter_members, "entities"), "1002": (adapter_members, "entities"),
    "k-fred-unrate": (series_fred, "series"), "k-fred-cpi": (series_fred, "series"),
    "k-fred-mortgage": (series_fred, "series"), "k-fred-gas": (series_fred, "series"),
    "13": (series_fred, "series"), "59": (series_fred, "series"),
    "14": (series_bls, "series"), "208": (series_bls, "series"), "1102": (series_bls, "series"),
    "1401": (series_bls, "series"), "1402": (series_bls, "series"), "1404": (series_bls, "series"),
    "1406": (series_bls, "series"), "1501": (series_bls, "series"),
    "k-eia-gas-midwest": (series_eia, "series"), "k-eia-gas-us": (series_eia, "series"),
    "201": (series_csv_last, "series"), "1006": (series_csv_last, "series"), "1007": (series_csv_last, "series"),
    "32": (series_worldbank, "series"), "W402": (series_worldbank, "series"), "807": (series_worldbank, "series"),
    "1601": (series_worldbank, "series"), "1602": (series_worldbank, "series"), "1603": (series_worldbank, "series"),
    "1604": (series_worldbank, "series"), "1605": (series_worldbank, "series"), "1606": (series_worldbank, "series"),
    "08": (series_treasury_fiscal, "series"), "09": (series_treasury_fiscal, "series"), "10": (series_treasury_fiscal, "series"),
    "W405": (series_ecb, "series"), "806": (series_boc, "series"), "1017": (series_frank, "series"),
    "1004": (series_nyfed, "series"), "16": (series_river, "series"), "1203": (series_river, "series"),
    "61": (series_misc, "series"), "112": (series_misc, "series"), "207": (series_misc, "series"),
    "40": (series_misc, "series"), "1010": (series_misc, "series"), "k-bea-gdp": (series_bea, "series"),
    "35": (clocks_fec, "clocks"),
}
for _n in ("x-stooq-s-p-csv", "x-coingecko", "x-treasury-yields"):
    DATA_ADAPTERS[_n] = (series_misc, "series")


# ====================================================================================================
# Significance (ENGINE.md section 2)
# ====================================================================================================

def _w(words):
    """Short words match whole (with plural), longer ones as a word-start stem."""
    parts = []
    for w in words:
        parts.append(rf"\b{w}(?:s|es)?\b" if len(w) <= 4 and " " not in w else rf"\b{w}")
    return re.compile("|".join(parts), re.I)


STAKE3_RE = _w(["sanction", "refugee", "immigra", "citizen", "deport", "detain", "national emergenc", "martial", "invok",
                "insurrection", "voting", "election", "ballot", "war", "military force", "nuclear"])
DRAFT_RE = re.compile(r"\bdraft\b(?!\s+(guidance|environmental|report|rule|policy|plan|standard|permit|regulat|assessment|recommend|analysis|supplement))", re.I)
STAKE2_RE = _w(["tariff", "dut(?:y|ies)", "import", "export", "tax", "price", "housing", "mortgage", "wage", "benefit", "medicare",
                "medicaid", "social security", "loan", "fee", "fund", "budget", "energy", "fuel", "diesel", "oil", "gas", "health",
                "insurance", "vehicle", "trade", "bank", "currency", "dollar"])
STAKE2_RE = re.compile(STAKE2_RE.pattern.replace(r"dut(?:y|ies)(?:s|es)?\b", r"dut(?:y|ies)\b").replace(r"\bdut(?:y|ies)", r"\bdut(?:y|ies)"), re.I)
IRREV_RE = re.compile(r"\bterminat|\brevo[kc]|\bwithdraw|\bprohibit|\bban(?:s|ned|ning)?\b|\bsuspend|\bexpel|\brescind|final rule|\brepeal|\binvok", re.I)
REACH_RE = re.compile(r"\b(gas|fuel|diesel|oil|tariffs?|dut(?:y|ies)|imports?|housing|mortgages?|tax(?:es)?|wages?|prices?|health|medicare|insurance|vehicles?|food|energy|grid|inflation|rates?)\b", re.I)
NOT_BREAKING_RE = re.compile(r"(\b(month|day|week|year|observance)\s*$|\b20\d\d\s*$|continuation of the national emergency)", re.I)
TRACKED = {"canada": "Canada", "canadian": "Canada", "ottawa": "Canada", "mexico": "Mexico", "mexican": "Mexico",
           "china": "China", "chinese": "China", "beijing": "China", "russia": "Russia", "russian": "Russia", "moscow": "Russia",
           "kremlin": "Russia", "iran": "Iran", "iranian": "Iran", "tehran": "Iran", "saudi": "Saudi Arabia", "riyadh": "Saudi Arabia",
           "ukraine": "Ukraine", "ukrainian": "Ukraine", "kyiv": "Ukraine", "israel": "Israel", "israeli": "Israel", "gaza": "Israel"}
US_STATES = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
             "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
             "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
             "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
             "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington State", "West Virginia",
             "Wisconsin", "Wyoming"]
STATE_ABBR = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut",
              "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
              "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
              "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
              "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
              "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
              "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington State", "WV": "West Virginia",
              "WI": "Wisconsin", "WY": "Wyoming"}
STATE_RE = re.compile(r"\b(" + "|".join(re.escape(s) for s in sorted(US_STATES, key=len, reverse=True) if s != "Washington State") + r")\b")
WORLD_PLACES = ["Afghanistan", "Argentina", "Australia", "Bangladesh", "Brazil", "Canada", "Chile", "China", "Colombia", "Cuba", "Egypt",
                "Ethiopia", "France", "Germany", "Haiti", "India", "Indonesia", "Iran", "Iraq", "Israel", "Italy", "Japan", "Jordan",
                "Kenya", "Lebanon", "Libya", "Mexico", "Myanmar", "Nigeria", "North Korea", "South Korea", "Pakistan", "Palestine",
                "Peru", "Philippines", "Poland", "Russia", "Saudi Arabia", "Somalia", "South Africa", "Spain", "Sudan", "Syria",
                "Taiwan", "Thailand", "Turkey", "Ukraine", "United Kingdom", "Britain", "Venezuela", "Vietnam", "Yemen", "Gaza"]
WORLD_RE = re.compile(r"\b(" + "|".join(re.escape(s) for s in WORLD_PLACES) + r")\b")
US_WORDS = re.compile(r"\b(United States|U\.S\.|US|Trump|Congress|White House|Senate|Pentagon|Washington|federal)\b")
WIRES = re.compile(r"\b(Reuters|Associated Press|AP|AFP|Bloomberg|Xinhua|UPI|TASS|PTI|ANI|Kyodo)\b")


def place_slug(name):
    return slug(name)


def tag_places(text, kind, it):
    pl = set()
    for m in STATE_RE.findall(text):
        pl.add(("state", m))
    if re.search(r"\bWashington State\b", text):
        pl.add(("state", "Washington State"))
    for m in WORLD_RE.findall(text):
        pl.add(("country", "United Kingdom" if m == "Britain" else ("Israel" if m == "Gaza" else m)))
    if it.get("state"):
        sn = STATE_ABBR.get(it["state"])
        if sn:
            pl.add(("state", sn))
    if it.get("state_hint"):
        pl.add(("state", it["state_hint"].title()))
    for u in it.get("ugc") or []:
        sn = STATE_ABBR.get(u[:2])
        if sn:
            pl.add(("state", sn))
    return sorted(pl)


def stake_of(it, now, settings):
    t = f"{it.get('title', '')} {it.get('summary', '')}"
    k = it["kind"]
    if k == "quake":
        if (it.get("mag") or 0) >= 7.0 or (it.get("alert") or "").lower() in ("orange", "red"):
            return 3
    if k == "gdacs_event":
        if it.get("alert") == "Red":
            return 3
        if it.get("alert") == "Orange":
            return 2
    if k == "who_don":
        pd = parse_dt(it.get("published"))
        if pd and (now - pd).days <= 14:
            return 3
    if k == "fema_declaration":
        if it.get("dtype") == "DR" and it.get("state") == "IN":
            return 3
        return 2 if it.get("dtype") in ("DR", "EM") else 1
    if k == "nws_alert":
        if it.get("severity") in ("Severe", "Extreme"):
            return 3 if "INZ047" in (it.get("ugc") or []) else 2
        return 1
    if k == "quake":
        return 1
    if NOT_BREAKING_RE.search(it.get("title", "")):
        return 1
    if STAKE3_RE.search(t) or DRAFT_RE.search(t):
        return 3
    if STAKE2_RE.search(t):
        return 2
    return 1


def proximity_of(it, places, stake_text):
    t = stake_text
    ugc = it.get("ugc") or []
    if "INZ047" in ugc or re.search(r"\bMarion County\b|\bIndianapolis\b", t):
        base = 1.0
    elif ("state", "Indiana") in places or any(u.startswith("IN") for u in ugc) or it.get("state") == "IN" or re.search(r"\bHoosier|\bIndiana\b", t):
        base = 0.6
    elif it["kind"] in ("presidential_doc", "fr_doc", "fr_inspection", "bill_presented", "bill_action", "reg_comment", "fema_declaration",
                        "nws_alert", "recall", "opinion", "cisa_kev", "presidential_action", "state_bill") \
            or it.get("region") in ("US", "States") or US_WORDS.search(t) or any(p[0] == "state" for p in places):
        base = 0.4
    else:
        base = 0.2
    reach = bool(REACH_RE.search(t))
    tracked = any(w in t.lower().split() or re.search(rf"\b{w}\b", t.lower()) for w in TRACKED)
    if base < 0.6 and (reach or tracked):
        base = 0.6
    return base, reach, sorted({TRACKED[w] for w in TRACKED if re.search(rf"\b{w}\b", t.lower())})


def score_item(it, now, settings, first_seen=None):
    t = f"{it.get('title', '')} {it.get('summary', '')}"
    places = tag_places(t, it["kind"], it)
    it["places"] = places
    stake = stake_of(it, now, settings)
    prox, reach, tracked = proximity_of(it, places, t)
    irrev = 2 if (IRREV_RE.search(it.get("title", "")) or (it["kind"] == "quake" and (it.get("alert") or "").lower() in ("orange", "red"))) else 1
    # urgency: 1/max(days to cutoff,1) inside the horizon, else the no-clock value
    urg = settings["hero.noclock_urgency"]
    cut = parse_dt(it.get("cutoff"))
    if cut:
        days = (cut - now).total_seconds() / 86400.0
        if days >= 0 and days <= settings["hero.horizon_days"]:
            urg = 1.0 / max(int(days + 0.999), 1)
    ref = parse_dt(it.get("published")) or first_seen or now
    age_h = (now - ref).total_seconds() / 3600.0
    recency = 1.0 if age_h <= 3 else 0.3
    it.update({"stake": stake, "proximity": prox, "irreversibility": irrev, "urgency": round(urg, 4),
               "score": round(stake * prox * irrev * urg, 4), "recency": recency,
               "breaking_score": round(stake * prox * irrev * recency, 4),
               "reach_tag": reach, "tracked_countries": tracked})
    return it


# ====================================================================================================
# News clustering: Confirmed is computed, never assumed
# ====================================================================================================

STOP = set("a an the of to in on for and or at by with from as is are was were be been it its this that after over into new says say said "
           "amid about up out off than more has have had will would could may not no vs us we he she they his her their who what when how why "
           "live news update updates latest report reports today".split())


def tokens(title):
    toks = re.findall(r"[a-z0-9]+", (title or "").lower())
    return {t[:-1] if len(t) > 4 and t.endswith("s") else t for t in toks if t not in STOP and len(t) > 2}


def pub_of(it):
    h = it.get("pub_host") or host_of(it.get("url") or "") or ""
    return publisher_id(h)


def cites(a, b):
    """True when item a's text or link names publisher b (or they credit the same wire)."""
    bn = pub_of(b).split(".")[0]
    ta = f"{a.get('title', '')} {a.get('summary', '')} {a.get('url', '')}".lower()
    if bn and len(bn) > 3 and bn in ta:
        return True
    wa, wb = set(WIRES.findall(f"{a.get('title', '')} {a.get('summary', '')}")), set(WIRES.findall(f"{b.get('title', '')} {b.get('summary', '')}"))
    return bool(wa & wb)


def independent_publishers(members):
    """Largest greedy set of distinct publishers, pairwise neither citing the other."""
    chosen = []
    for m in sorted(members, key=lambda x: (x.get("published") or "", x.get("url") or "")):
        p = pub_of(m)
        if any(pub_of(c) == p for c in chosen):
            continue
        if any(cites(m, c) or cites(c, m) for c in chosen):
            continue
        chosen.append(m)
    return chosen


def cluster_news(items, settings):
    """Leader clustering on title tokens inside a time window. Returns list of clusters (lists of items)."""
    win = settings["cluster.window_h"] * 3600
    minshared = settings["cluster.min_shared_tokens"]
    order = sorted(items, key=lambda x: (x.get("published") or "", x["url"]))
    clusters, index = [], {}
    for it in order:
        tk = tokens(it["title"])
        it["_tk"] = tk
        cand = {}
        for t in tk:
            for ci in index.get(t, ()):
                cand[ci] = cand.get(ci, 0) + 1
        best, bs = None, 0.0
        for ci, shared in cand.items():
            if shared < minshared:
                continue
            c = clusters[ci]
            pd, cd = parse_dt(it.get("published")), parse_dt(c[0].get("published"))
            if pd and cd and abs((pd - cd).total_seconds()) > win:
                continue
            for m in c:
                inter = len(tk & m["_tk"])
                union = len(tk | m["_tk"]) or 1
                sim = max(inter / union, inter / (min(len(tk), len(m["_tk"])) or 1) * 0.75)
                if inter >= minshared and sim > bs and sim >= 0.5:
                    best, bs = ci, sim
        if best is None:
            clusters.append([it])
            best = len(clusters) - 1
        else:
            clusters[best].append(it)
        for t in tk:
            index.setdefault(t, set())
            if len(index[t]) < 400:
                index[t].add(best)
    for c in clusters:
        for m in c:
            m.pop("_tk", None)
    return clusters


# ====================================================================================================
# Truth labels (ATTACK.md section 7: first match wins)
# ====================================================================================================

def is_primary_host(host):
    h = (host or "").lower()
    if any(h.endswith(x) for x in SECONDARY_HOSTS):
        return False
    return bool(PRIMARY_RE.search(h))


def label_for_group(members):
    """members: items of one event. Returns (label, independent_publisher_hosts)."""
    recs = [m for m in members if m["cls"] == "record" and is_primary_host(m.get("src_host"))]
    if recs:
        return "Primary record", sorted({m["src_host"] for m in recs})
    indep = independent_publishers([m for m in members if m["cls"] == "news"])
    hosts = sorted({pub_of(m) for m in indep})
    if len(hosts) >= 2:
        return "Confirmed", hosts
    return "Single source", sorted({pub_of(m) for m in members})


# ====================================================================================================
# Ledger documents
# ====================================================================================================

COLL = {"day": "wm_day", "live": "wm_live", "event": "wm_event", "entity": "wm_entity", "series": "wm_series",
        "place": "wm_place", "source": "wm_source", "meta": "wm_meta", "heartbeat": "wm_heartbeat", "image": "wm_image"}
STAMP_COLLS = {"wm_meta", "wm_heartbeat"}   # written by every run
BREAKING_KINDS = {"presidential_doc", "presidential_action", "fema_declaration", "nws_alert", "quake", "gdacs_event", "who_don",
                  "news"}   # news only as a Confirmed cluster (ATTACK.md section 6 breaking rule)


class Ledger:
    def __init__(self, run_id, now):
        self.run_id, self.now = run_id, now
        self.docs = {}

    def put(self, family, doc_id, data, origin, asof, fetched_at, label, fact=True):
        coll = COLL[family]
        d = dict(data)
        d.update({"schema_version": SCHEMA_VERSION, "id": doc_id, "key": f"wm/{family}/{doc_id}", "run_id": self.run_id,
                  "origin": sorted(set(origin)), "asof": asof, "fetched_at": fetched_at, "written_at": iso(self.now),
                  "actor": "engine", "label": label})
        d["content_sha256"] = content_hash(d)
        self.docs[(coll, doc_id)] = d
        return d


def content_hash(d):
    core = {k: v for k, v in d.items() if k not in STAMP_FIELDS}
    return sha256_hex(canon(core))


def doc_bytes(d):
    return len(json.dumps(d, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def shrink_doc(d):
    """Keep a doc under DOC_LIMIT by trimming list fields, largest first. Never touches stamps."""
    guard = 0
    while doc_bytes(d) > DOC_LIMIT and guard < 50:
        guard += 1
        lists = sorted(((k, len(v)) for k, v in d.items() if isinstance(v, list) and len(v) > 1 and k not in ("origin",)),
                       key=lambda kv: -kv[1])
        if not lists:
            break
        k = lists[0][0]
        d[k] = d[k][: max(1, len(d[k]) // 2)]
        d["trimmed"] = True
    if doc_bytes(d) > DOC_LIMIT:
        raise ValueError(f"doc {d.get('key')} over {DOC_LIMIT} bytes")
    d["content_sha256"] = content_hash(d)
    return d


def make_batches(writes):
    batches, cur, size = [], [], 2
    for w in writes:
        wb = len(json.dumps(w, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) + 1
        if wb > BATCH_MAX_BYTES:
            raise ValueError("single write over batch byte limit")
        if cur and (len(cur) >= BATCH_MAX_WRITES or size + wb > BATCH_MAX_BYTES):
            batches.append(cur)
            cur, size = [], 2
        cur.append(w)
        size += wb
    if cur:
        batches.append(cur)
    return batches


# ---- retention ------------------------------------------------------------------------------------

def retention_plan(settings, measured_bytes=None):
    """Year budget by collection (documents, and bytes when measured). Design: nothing is deleted by the engine;
    the count is bounded by construction (events/day cap) and old Days fold into monthly roll-ups (prose dropped)."""
    ev = settings["retention.events_per_day"]
    fam = {
        "wm_day": 365, "wm_live": 365, "wm_event": ev * 365, "wm_entity": 60 + 3 * 365 // 2,
        "wm_series": 160, "wm_place": 130, "wm_source": 340, "wm_meta": 1, "wm_heartbeat": 2, "wm_image": 2 * 365,
        "wm_rollup(day)": 12,
    }
    total = sum(fam.values())
    ceiling = settings["retention.docs_ceiling"]
    cap = int(ceiling * settings["retention.writer_cap_pct"] / 100)
    plan = {"docs_per_family_year": fam, "docs_year_total": total, "docs_ceiling": ceiling, "writer_cap_docs": cap,
            "fits_year": total <= cap, "events_per_day_cap": ev,
            "rollup_rule": f"Days older than {settings['retention.keep_full_days']} days fold into one monthly roll-up doc; "
                           f"prose fields are nulled (ids kept) by update, never deleted; wm/live older than "
                           f"{settings['retention.live_days']} days stops being read."}
    if measured_bytes:
        est = {"wm_day": 365 * measured_bytes.get("wm_day", 0), "wm_event": ev * 365 * measured_bytes.get("wm_event", 0),
               "wm_live": 365 * measured_bytes.get("wm_live", 0), "wm_image": 365 * measured_bytes.get("wm_image", 0)}
        plan["bytes_year_estimate_main"] = est
        plan["bytes_year_estimate_total"] = sum(est.values()) + 3_000_000
        plan["bytes_note"] = "ESTIMATE from this run's mean doc sizes; wm_image assumes 1 image/day; +3 MB for series/entity/place/source"
    return plan


def fold_days(day_summaries, month):
    """Monthly roll-up (item ids kept, prose dropped). Returns writes: one set for the roll-up, one update per day
    that nulls prose fields and leaves a tombstone."""
    days = sorted(d for d in day_summaries if d.startswith(month))
    ids = {}
    for d in days:
        ids[d] = day_summaries[d].get("event_ids", [])
    roll = {"collection": COLL["day"], "doc_id": f"rollup-{month}", "op": "set",
            "data": {"id": f"rollup-{month}", "kind": "rollup", "month": month, "days": ids,
                     "tombstones": days, "label": "Primary record", "origin": ["engine"], "asof": month, "schema_version": SCHEMA_VERSION}}
    ups = [{"op": "update", "collection": COLL["day"], "doc_id": d,
            "data": {"ranked": None, "sections": None, "limits_line": None, "tombstone": True, "folded_into": f"rollup-{month}"}} for d in days]
    return [roll] + ups


# ====================================================================================================
# State
# ====================================================================================================

def load_state(state_dir):
    p = Path(state_dir) / "state.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {"index": {}, "first_seen": {}, "series_first": {}, "sources": {}, "heartbeat": {}, "leases": {}, "live": {}, "days": {}}


def save_state(state_dir, st):
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    p = Path(state_dir) / "state.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, sort_keys=True))
    os.replace(tmp, p)


# ====================================================================================================
# Model-run stubs (law 0A: models run only Extract, Explain, Audit). No call is made here.
# ====================================================================================================

def model_extract_stub(candidates):
    return {"step": "Extract", "model": "Haiku", "called": False,
            "would_receive": "per item: title, summary (<=500 chars), url, publisher, published; ask for who/what/where/when",
            "item_ids": [c["id"] for c in candidates], "approx_chars": sum(len(c.get("title", "")) + len(c.get("summary", "")) for c in candidates)}


def model_explain_stub(events):
    return {"step": "Explain", "model": "Sonnet", "called": False,
            "would_receive": "per event: extracted fields + ledger evidence rows (title/url/publisher/label); one analysis line tagged assumption|judgment, max explain.max_lines",
            "event_ids": [e["id"] for e in events], "approx_chars": sum(len(canon(e.get("evidence", []))) for e in events)}


def model_audit_stub(events):
    return {"step": "Audit", "model": "Opus (fresh context)", "called": False,
            "would_receive": "each analysis line plus the evidence rows it cites; return accept|reject per line with the untraceable figure",
            "event_ids": [e["id"] for e in events], "analysis_lines_present": 0}


# ====================================================================================================
# Pipeline
# ====================================================================================================

def event_key(it):
    if it.get("natural_key"):
        return it["natural_key"]
    if it.get("doc_number"):
        return "fr:" + it["doc_number"]
    return it.get("url") or it["title"]


def eid(key):
    return "e-" + hashlib.sha1(key.encode()).hexdigest()[:14]


def evidence_row(it):
    return {"source_id": it["source_id"], "host": it.get("src_host"), "publisher": pub_of(it), "url": it.get("url"),
            "title": it["title"][:160], "published": it.get("published")}


def scope_name(prox):
    return {1.0: "district", 0.6: "indiana-or-lifted", 0.4: "united-states", 0.2: "world"}.get(prox, "world")


def region_scope(it, places):
    t = f"{it.get('title', '')} {it.get('summary', '')}"
    ugc = it.get("ugc") or []
    if "INZ047" in ugc or re.search(r"\bMarion County\b|\bIndianapolis\b", t):
        return "marion-county"
    if it.get("state") == "IN" or any(u.startswith("IN") for u in ugc) or ("state", "Indiana") in places or re.search(r"\bHoosier", t):
        return "indiana"
    if it["kind"] in ("presidential_doc", "fr_doc", "fr_inspection", "bill_presented", "bill_action", "reg_comment", "recall", "opinion", "cisa_kev") \
            or US_WORDS.search(t) or any(p[0] == "state" for p in places) or it.get("region") in ("US", "States"):
        return "united-states"
    return "world"


def build_items(src_rows, results, ctx):
    """Run adapters. Returns (items, series_list, entities, clocks, per-source report info)."""
    items, series, ents, clocks, info = [], [], [], [], {}
    for src in src_rows:
        r = results.get(src["id"])
        i = {"id": src["id"], "host": src["host"], "status": "not_fetched", "http": None, "items": 0, "error": None,
             "proof": "none", "url": redact_url(src["url"]), "ms": 0, "bytes": 0}
        info[src["id"]] = i
        if r is None:
            i["error"] = "not attempted in this run tier"
            continue
        i.update(status=r.status, http=r.http, error=r.error, ms=r.ms, bytes=r.bytes)
        if not r.ok:
            continue
        i["proof"] = field_proof(src.get("quote"), r.text()) if src.get("quote") else "none"
        ad = DATA_ADAPTERS.get(src["id"])
        try:
            got = None
            if ad:
                fn, product = ad
                got = fn(src, r, ctx)
                if got is None:
                    product = "none"
            else:
                product = "items"
                got = adapter_feed(src, r, ctx) if r.body[:2000].lstrip()[:1] == b"<" else None
            if got is None:
                got, product = [], "none"
        except SourceLimit as e:
            i["status"] = "rate_limited"
            i["error"] = ctx["scrub"](str(e))[:200]
            continue
        except Exception as e:  # schema change: withhold, no guess parse (ENGINE.md section 10)
            i["status"] = "schema_changed"
            i["error"] = ctx["scrub"](f"{type(e).__name__}: {e}")[:200]
            continue
        if product == "items":
            for it in got:
                it["src_host"] = r.host
                it["src_name"] = src.get("name", src["id"])
                it["fetched_at"] = r.fetched_at
                it["region"] = it.get("region") or src.get("region", "")
            items.extend(got)
            i["items"] = len(got)
        elif product == "series":
            for s in got:
                s["host"], s["fetched_at"], s["proof"] = r.host, r.fetched_at, i["proof"]
            series.extend(got)
            i["items"] = len(got)
        elif product == "entities":
            for e in got:
                e["host"], e["fetched_at"] = r.host, r.fetched_at
            ents.extend(got)
            i["items"] = len(got)
        elif product == "clocks":
            for c in got:
                c["host"], c["fetched_at"] = r.host, r.fetched_at
            clocks.extend(got)
            i["items"] = len(got)
        # quoted-date clocks and next-release strings from reference pages
        if src["id"] == "913" and i["proof"] == "pass":
            m = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", src.get("quote", ""))
            if m:
                d = dt.datetime.strptime(m.group(1), "%B %d, %Y").date().isoformat()
                clocks.append({"clock_id": "marion-property-tax-" + d, "what": "Marion County property tax installment due",
                               "closes_on": d, "time_known": False, "stake": 2, "prox": 1.0, "irrev": 2, "src": "913",
                               "host": r.host, "fetched_at": r.fetched_at})
        if src["id"] in ("915", "39"):
            m = re.search(r"Next Release Date:\s*([A-Z][a-z]+ \d{1,2},?)", r.text())
            if m:
                ctx["next_release"]["eia-gas"] = m.group(1) + " (year not on the page)"
        if src["id"] == "916":
            ctx["next_release"]["freddie-pmms30"] = "weekly, Thursday (page text)"
    return items, series, ents, clocks, info


def process(kind, src_rows, results, state, settings, ctx, fetcher=None):
    now = ctx["now"]
    led = Ledger(ctx["run_id"], now)
    items, series, ents, clocks, info = build_items(src_rows, results, ctx)
    ctx["info"] = info
    fs = state["first_seen"]
    # -- stale items are not "new": drop those older than the age cap unless a future cutoff keeps them live
    kept, stale = [], 0
    for it in items:
        pd = parse_dt(it.get("published"))
        age = (now - pd).days if pd else 0
        lim = 30 if it["kind"] in ("presidential_doc", "presidential_action") else settings["retention.max_age_days"]
        cut = parse_dt(it.get("cutoff"))
        if age > lim and not (cut and cut >= now):
            stale += 1
            continue
        kept.append(it)
    items = kept
    ctx["stale_dropped"] = stale

    # -- score items; first_seen persists across runs
    for it in items:
        key = event_key(it)
        it["_key"] = key
        fs.setdefault(key, iso(now))
        score_item(it, now, settings, parse_dt(fs[key]))

    # -- group: news clusters; each record item is its own event
    news = [it for it in items if it["cls"] == "news" and it["title"] and it["url"]]
    recs = [it for it in items if it["cls"] != "news" and it["title"]]
    groups = []
    for c in cluster_news(news, settings):
        groups.append(("cluster", c))
    for it in recs:
        groups.append(("record", [it]))

    events = {}
    for gtype, members in groups:
        members = sorted(members, key=lambda x: (x.get("published") or "9999", x.get("url") or ""))
        best = max(members, key=lambda x: (x["breaking_score"], x["score"]))
        rep = members[0]
        key = ("cluster:" + (rep["url"] or rep["title"])) if gtype == "cluster" else rep["_key"]
        label, hosts = label_for_group(members)
        e = {"kind": "news" if gtype == "cluster" else rep["kind"], "title": best["title"], "summary": best["summary"][:300],
             "url": best["url"], "published": rep.get("published"), "stage": rep.get("stage") or "",
             "cutoff": rep.get("cutoff"), "label": label, "stake": best["stake"], "proximity": best["proximity"],
             "irreversibility": best["irreversibility"], "urgency": best["urgency"], "score": best["score"],
             "reach_tag": best["reach_tag"], "tracked_countries": best["tracked_countries"],
             "places": sorted({f"{k}:{place_slug(v)}" for it in members for k, v in it["places"]}),
             "scope": region_scope(best, best["places"]), "independent_publishers": hosts if label != "Single source" else [],
             "n_publishers": len({pub_of(m) for m in members}), "cluster_size": len(members),
             "world_significance": (len(hosts) * len(members)) if label == "Confirmed" else None,
             "world_significance_note": "independent publishers x cluster size; people affected not measured",
             "evidence": [evidence_row(m) for m in members[:6]], "first_seen": min(fs[m["_key"]] for m in members),
             "origin": sorted({m["source_id"] for m in members})}
        if rep.get("natural_key"):
            e["natural_key"] = rep["natural_key"]
        e["_bs"] = best["breaking_score"]
        e["_image"] = next((m.get("image_url") for m in members if m.get("image_url")), None)
        e["_fetched"] = max(m.get("fetched_at") or "" for m in members)
        e["_members"] = members
        e["_not_breaking"] = bool(NOT_BREAKING_RE.search(best["title"]))
        events[eid(key)] = e

    # -- retention: top N per run by (breaking_score, score); never more than the daily cap
    cap = settings["retention.events_per_day"]
    ranked_all = sorted(events.items(), key=lambda kv: (-kv[1]["_bs"], -kv[1]["score"], kv[0]))
    stored = ranked_all[:cap]
    ctx["events_all"] = events
    ctx["ranked_all"] = ranked_all
    return led, stored, series, ents, clocks, items


def hero_pool(clocks, stored, now, settings):
    today = local_date(now)
    d0 = dt.date.fromisoformat(today)
    pool = []
    for c in clocks:
        days = (dt.date.fromisoformat(c["closes_on"]) - d0).days
        if days < 0:
            continue
        urg = 1.0 / max(days, 1) if days <= settings["hero.horizon_days"] else settings["hero.noclock_urgency"]
        sc = c["stake"] * c["prox"] * c["irrev"] * urg
        pool.append({"kind": "clock", "id": c["clock_id"], "what": c["what"], "closes_on": c["closes_on"], "days": days,
                     "time_known": c["time_known"], "score": round(sc, 4), "stake": c["stake"], "source_id": c["src"],
                     "label_text": "Closes"})
    for k, e in stored:
        pool.append({"kind": "fact", "id": k, "what": e["title"], "closes_on": (e.get("cutoff") or "")[:10] or None, "days": None,
                     "score": e["score"], "stake": e["stake"], "label_text": "Most important new fact", "published": e.get("published") or ""})
    pool.sort(key=lambda p: (-p["score"], p["closes_on"] or "9999", -p["stake"], "".join(chr(255 - ord(ch)) for ch in (p.get("published") or ""))))
    return pool


def make_image(fetcher, url, scrub):
    """Fetch, downscale to JPEG (longest side <= 1280 px, never up-scaled, q 80 -> 60, <= imgfeeds.DATA_URI_MAX_BYTES 160 KB so the
    base64 data URI stays under DOC_LIMIT), return data-URI doc fields or None. Quality gates are imgfeeds.photo_jpeg (one
    implementation); `sha256` of the stored JPEG is the dedupe key."""
    import imgfeeds
    r = fetcher.fetch("img-" + hashlib.sha1(url.encode()).hexdigest()[:10], url)
    if not r.ok or not r.body:
        return None, r
    jpg, info = imgfeeds.photo_jpeg(r.body, long=imgfeeds.PHOTO_LONG, max_bytes=imgfeeds.DATA_URI_MAX_BYTES)
    if jpg is None:
        return None, r
    import base64
    return {"data_uri": "data:image/jpeg;base64," + base64.b64encode(jpg).decode(), "width": info["w"], "height": info["h"], "bytes": info["bytes"],
            "quality": info["q"], "sharp": info["sharp"], "source_url": redact_url(url), "src_sha256": r.sha256, "sha256": hashlib.sha256(jpg).hexdigest()}, r


def image_allowed(url, cls="record"):
    """news (R2-pub): the picture the publisher's own feed shipped, unless that publisher declined (imgfeeds.declined_hosts) or the URL
    is a logo/flag/seal. records (R2-gov): only public-domain government hosts (noaa/nasa/usgs/.gov/.mil), issuer checked by imgfeeds.gov_photos."""
    import imgfeeds
    h = host_of(url)
    if not h or not str(url).startswith("https://") or imgfeeds.PUB_SKIP.search(str(url).split("?")[0]):
        return False
    if cls == "news":
        return imgfeeds._reg(h) not in imgfeeds.declined_hosts()
    return is_primary_host(h) and re.search(r"(noaa|nasa|usgs|\.gov$|\.mil$)", h) is not None


def finalize_morning(kind, led, stored, series, ents, clocks, items, src_rows, state, settings, ctx, fetcher):
    now = ctx["now"]
    today = ctx["today"]
    run_id = ctx["run_id"]
    info = ctx["info"]
    rid_events = {}
    # ---- series
    sf = state["series_first"]
    sdocs = {}
    for s in series:
        sid = s["series_id"]
        if not s["obs"]:
            continue
        period, val = s["obs"][0]
        fp = sf.setdefault(sid, {})
        fp.setdefault(period, val)
        anchor = None
        if s.get("freq") == "monthly" and re.match(r"\d{4}-\d{2}", period):
            y, m = int(period[:4]), int(period[5:7])
            prev = f"{y - 1}-{m:02d}"
            for p, v in s["obs"]:
                if p == prev:
                    try:
                        anchor = {"period": p, "value": v, "change_pct": round((float(val) / float(v) - 1) * 100, 2)}
                    except Exception:
                        pass
        label = "Primary record" if (is_primary_host(s["host"]) and not s.get("republisher")) else "Single source"
        data = {"label_text": s["label"], "value": val, "unit": s["unit"], "period": period, "observations": [list(o) for o in s["obs"]],
                "first_print": fp[period], "revised_to": (val if val != fp[period] else None), "anchor": anchor,
                "next_release": ctx["next_release"].get({"eia-gas-r20": "eia-gas", "eia-gas-nus": "eia-gas"}.get(sid, sid)) or ctx["next_release"].get(sid),
                "frequency": s.get("freq", ""), "publisher_host": s["host"], "issuer_note": s.get("issuer", ""),
                "field_proof": s.get("proof"), "place_fips": s.get("place_fips"), "place_hint": s.get("place_hint")}
        sdocs[sid] = led.put("series", sid, data, [s["src"]], period, s["fetched_at"], label)
    # ---- events
    for k, e in stored:
        data = {kk: vv for kk, vv in e.items() if not kk.startswith("_") and kk not in ("origin",)}
        data["version_count"] = state.setdefault("versions", {}).get(k, {}).get("n", 1)
        led.put("event", k, data, e["origin"], e.get("published") or e["_fetched"], e["_fetched"], e["label"])
        rid_events[k] = e
    # ---- entities (Indiana members; orders; bills)
    members = {}
    for m in ents:
        cur = members.setdefault(m["eid"], {"name": m["name"], "party": m.get("party"), "chamber": m["chamber"], "state": "IN",
                                            "district": m["district"], "origin": [], "hosts": [], "fetched": m["fetched_at"]})
        cur["origin"].append(m["src"])
        cur["hosts"].append(m["host"])
        if m.get("url"):
            cur["url"] = m["url"]
    for bid, m in members.items():
        lab = "Primary record" if any(is_primary_host(h) for h in m["hosts"]) else "Single source"
        led.put("entity", f"member:{bid}", {"etype": "member", "name": m["name"], "party": m["party"], "chamber": m["chamber"],
                                           "state": "IN", "district": m["district"], "url": m.get("url"), "timeline": [],
                                           "note": "district IDs only; no address stored"},
                m["origin"], today, m["fetched"], lab)
    for k, e in stored:
        if e["kind"] in ("presidential_doc", "presidential_action"):
            dn = next((mm.get("doc_number") for mm in e["_members"] if mm.get("doc_number")), None) or k
            led.put("entity", f"order:{dn}", {"etype": "order", "title": e["title"], "stage": e.get("stage"), "timeline": [k],
                                              "url": e["url"]}, e["origin"], e.get("published") or today, e["_fetched"], e["label"])
        elif e["kind"] in ("bill_presented", "bill_action"):
            bn = re.match(r"([A-Za-z.]+\s?\d+)", e["title"])
            bid = slug(bn.group(1) if bn else e["title"][:20])
            led.put("entity", f"bill:{bid}", {"etype": "bill", "title": e["title"], "stage": "presented to the president" if e["kind"] == "bill_presented" else "action",
                                              "timeline": [k], "url": e["url"]}, e["origin"], e.get("published") or today, e["_fetched"], e["label"])
    # ---- clocks / hero
    pool = hero_pool(clocks, stored, now, settings)
    hero = pool[0] if pool else None
    clock_list = [p for p in pool if p["kind"] == "clock"]
    # ---- images (data URI, own collection; lead items that own one; public-domain government hosts only)
    image_id = None
    if fetcher is not None:
        for k, e in stored[:12]:
            if e.get("_image") and image_allowed(e["_image"]) and e["label"] == "Primary record":
                fields, r = make_image(fetcher, e["_image"], ctx["scrub"])
                if fields:
                    iid = "i-" + hashlib.sha1(e["_image"].encode()).hexdigest()[:14]
                    led.put("image", iid, {**fields, "event_id": k, "license": "US government work / public domain (host allowlist)",
                                           "credit": host_of(e["_image"])}, [e["origin"][0]], e.get("published") or today, r.fetched_at, "Primary record")
                    if image_id is None:
                        image_id = iid
                    if len([1 for kk in led.docs if kk[0] == COLL["image"]]) >= 2:
                        break
    # ---- places
    place_events = {}
    for k, e in stored:
        scopes = {e["scope"]}
        scopes.add("united-states" if e["scope"] in ("indiana", "marion-county") else e["scope"])
        if e["scope"] == "marion-county":
            scopes.add("indiana")
        for p in e["places"]:
            scopes.add(p.split(":", 1)[1])
        scopes.add("world")
        for sc in scopes:
            place_events.setdefault(sc, []).append(k)
    sp = {s["series_id"]: s for s in series}
    tracked = {p["id"]: p for p in ctx["spec"]["places"]}
    place_ids = set(tracked) | {sc for sc, ks in place_events.items() if sc not in ("world", "united-states") and ks}
    for pid in sorted(place_ids):
        ks = sorted(place_events.get(pid, []), key=lambda k: (-rid_events[k]["score"], k))
        tp = tracked.get(pid, {})
        who = None
        if tp.get("head_of_gov"):
            row = next((r for r in src_rows if r["id"] == str(tp["head_of_gov"])), None)
            if row and info.get(row["id"], {}).get("status") == "ok" and info[row["id"]]["proof"] == "pass":
                who = {"row": row["id"], "text": row["quote"][:120], "host": row["host"]}
        numbers = [sid for sid, s in sp.items() if (tp.get("fips") and s.get("place_fips") == tp["fips"]) or
                   (s.get("place_hint") and slug(s["place_hint"]) == pid)][:4]
        data = {"scope": pid, "name": tp.get("name") or pid.replace("-", " ").title(), "ptype": tp.get("type") or ("state" if any(
            slug(s) == pid for s in US_STATES) else "country"), "hero_event": ks[0] if ks else None, "top5": ks[:5], "event_count": len(ks),
            "numbers": numbers, "who_runs_it": who, "limit": tp.get("limit") or ("no verified source captured for this place" if not ks else None),
            "tracked": pid in tracked}
        led.put("place", pid, data, ["places"] if not ks else sorted({o for k in ks[:5] for o in rid_events[k]["origin"]}), today,
                iso(now), "Primary record" if who else "Single source")
    # ---- source docs
    ss = state["sources"]
    for src in src_rows:
        i = info[src["id"]]
        st = ss.setdefault(src["id"], {})
        if i["status"] == "ok":
            st["last_ok_at"] = iso(now)
            st["blocked_since"] = None
            r = ctx["results"][src["id"]]
            st["sha256"] = r.sha256
        elif i["status"] in ("blocked",) and not st.get("blocked_since"):
            st["blocked_since"] = today
        data = {"host": src["host"], "verdict": src["verdict"], "origin_table": src["origin"], "url": i["url"], "last_status": i["status"],
                "http": i["http"], "blocked_since": st.get("blocked_since"), "last_ok_at": st.get("last_ok_at"),
                "sha256_last_capture": st.get("sha256"), "proof_quote": (src.get("quote") or "")[:120], "proof": i["proof"],
                "items_last_run": i["items"], "error": i["error"]}
        led.put("source", src["id"], data, [src["id"]], st.get("last_ok_at") or today, iso(now),
                "Primary record" if i["status"] == "ok" else "Not fetched")
    return pool, hero, clock_list, image_id, rid_events


# ====================================================================================================
# Day / live / meta / heartbeat documents and the run
# ====================================================================================================

NEWS_REGIONS = {"World", "Europe", "Asia", "MidEast", "Africa", "LatAm", "Americas", "US", "States"}


def select_sources(kind, rows, only=None):
    if only:
        return [r for r in rows if r["id"] in only]
    if kind == "morning":
        return rows
    out = []
    for r in rows:
        if r["id"] in HOURLY_IDS or r["id"] in ("k-nws-all", "x-gdacs-disasters", "x-nasa-eonet-events"):
            out.append(r)
        elif r["origin"] == "extra" and r["region"] in NEWS_REGIONS and not re.search(
                r"(api\.|query\.|sparql|dataservices|sdmx|ourworldindata|\.csv|format=json|/api/|nhk.*feeds/$)", r["url"], re.I):
            out.append(r)
    return out


def build_day(led, ctx, stored, rid_events, hero, clock_list, image_id, settings, src_rows, series):
    now, today = ctx["now"], ctx["today"]
    info = ctx["info"]
    win = settings["section.happened.window_h"]
    ev_doc = {k: led.docs[(COLL["event"], k)] for k, _ in stored}
    order = sorted(stored, key=lambda kv: (-kv[1]["score"], -kv[1]["_bs"], kv[0]))
    ranked = [{"id": k, "v": ev_doc[k]["content_sha256"][:8], "score": e["score"], "label": e["label"], "kind": e["kind"],
               "scope": e["scope"], "title": e["title"][:90]} for k, e in order]

    def fresh(e):
        pd = parse_dt(e.get("published"))
        return bool(pd and (now - pd).total_seconds() <= win * 3600)

    happened_kinds = {"who_don", "fema_declaration", "gdacs_event", "quake", "news", "eonet"}
    hap = [k for k, e in order if e["kind"] in happened_kinds and fresh(e)]
    pres = [k for k, e in order if e["kind"] in ("presidential_doc", "presidential_action")]
    rules = [k for k, e in order if e["kind"] in ("fr_doc", "reg_comment") and (e["reach_tag"] or e["cutoff"])]
    reach = [k for k, e in order if e["reach_tag"] and e["proximity"] >= 0.6]
    world = [k for k, e in sorted(stored, key=lambda kv: (-kv[1]["_bs"], -kv[1]["score"], kv[0])) if e["scope"] == "world"][:12]
    indiana = [k for k, e in order if e["scope"] in ("indiana", "marion-county")][:8]
    states = {}
    for k, e in sorted(stored, key=lambda kv: -kv[1]["_bs"]):
        if e["_bs"] >= settings["live.now_threshold"]:
            for p in e["places"]:
                if p.startswith("state:"):
                    states.setdefault(p[6:], []).append(k)
    states = {s: v[:3] for s, v in states.items()}
    ok = sum(1 for r in src_rows if r["origin"] == "spec" and info[r["id"]]["status"] == "ok")
    m_total = sum(1 for r in src_rows if r["origin"] == "spec")
    ok_x = sum(1 for r in src_rows if r["origin"] != "spec" and info[r["id"]]["status"] == "ok")
    x_total = sum(1 for r in src_rows if r["origin"] != "spec")
    bad = [r for r in src_rows if info[r["id"]]["status"] != "ok"]
    hosts = sorted({r["host"] for r in bad})
    limits = ("Not fetched: " + ", ".join(hosts[:8]) + (f" and {len(hosts) - 8} more hosts" if len(hosts) > 8 else "")) if hosts else "Every source answered."
    limits += " Senate, Indiana legislature and Council: not fetched (blocked)."
    failures = [{"source_id": r["id"], "input_id": r["host"], "error": (info[r["id"]]["error"] or info[r["id"]]["status"])[:100],
                 "at": info[r["id"]].get("fetched_at") or ""} for r in bad][:60]
    for f in failures:
        res = ctx["results"].get(f["source_id"])
        f["at"] = (res.fetched_at if res and res.fetched_at else "")
    labels = {}
    for e in ctx["events_all"].values():
        labels[e["label"]] = labels.get(e["label"], 0) + 1
    sections = {"happened": hap[:settings["section.cap.happened"]], "happened_older_count": max(0, len(hap) - settings["section.cap.happened"]),
                "president": pres[:2], "president_more": max(0, len(pres) - 2), "rules": rules[:settings["section.cap.rules"]],
                "reaches": reach[:settings["section.cap.reaches"]], "world": world, "indiana": indiana, "states": states}
    alert_bar = [k for k, e in stored if e["kind"] == "nws_alert" and any(m.get("ugc") and "INZ047" in m["ugc"] for m in e["_members"])]
    data = {"date": today, "status": "done", "hero": hero, "clocks": clock_list[:5], "ranked": ranked, "sections": sections,
            "alert_bar": alert_bar, "image_id": image_id, "limits_line": limits,
            "tier_line": f"Degraded: {ok} of {m_total} sources" if ok < m_total else f"All {m_total} sources answered",
            "tier_extra": f"{ok_x} of {x_total} added feeds answered", "withheld": sum(1 for r in src_rows if info[r["id"]]["status"] == "schema_changed"),
            "reconciled": 0, "failures": failures, "label_counts_all_items": labels, "analysis": "none (model steps are stubs)"}
    return data, ok, m_total


def check_alarm(kind, state, now, settings):
    hb = state["heartbeat"].get(kind)
    if not hb:
        return None
    last = parse_dt(hb["at"])
    interval_h = 1.0 if kind == "hourly" else 24.0
    gap_h = (now - last).total_seconds() / 3600 if last else 0
    thr = (settings["alarm.missed_runs"] + 0.5) * interval_h
    if gap_h > thr:
        return f"Watchman missed {int(gap_h // interval_h)} runs since {hb['at']}"
    if hb.get("total") and 100.0 * hb["ok"] / hb["total"] < settings["alarm.min_ok_pct"]:
        return f"degraded: {hb['ok']} of {hb['total']} sources on the previous run"
    return None


def breaking_candidates(stored_all, settings, now):
    out = []
    for k, e in stored_all:
        if e["_not_breaking"] or e["kind"] not in BREAKING_KINDS:
            continue
        if e["_bs"] < settings["live.breaking_threshold"]:
            continue
        if e["label"] not in ("Primary record", "Confirmed"):   # 1 primary record or 2 independent publishers
            continue
        if e["kind"] == "news" and e["world_significance"] is None:
            continue
        out.append((k, e))
    return out


def second_capture(candidates, src_rows, fetcher, ctx, wait):
    """Re-fetch the sources behind each candidate after `wait` seconds; the item id and date must reappear unchanged."""
    if not candidates:
        return {}
    if wait:
        time.sleep(wait)
    by_id = {r["id"]: r for r in src_rows}
    fetcher.cache.clear()
    verdict = {}
    seen_src = {}
    for k, e in candidates:
        ok = False
        for m in e["_members"]:
            sid = m["source_id"]
            if sid not in seen_src:
                r = fetcher.fetch("2nd-" + sid, by_id[sid]["url"]) if sid in by_id else None
                got = []
                if r is not None and r.ok:
                    ad = DATA_ADAPTERS.get(sid)
                    try:
                        if ad and ad[1] == "items":
                            got = ad[0](by_id[sid], r, ctx) or []
                        elif not ad:
                            got = adapter_feed(by_id[sid], r, ctx) or []
                    except Exception:
                        got = []
                seen_src[sid] = {(g["url"], g["published"]) for g in got}
            if (m["url"], m["published"]) in seen_src[sid]:
                ok = True
        verdict[k] = ok
    return verdict


def build_live(ctx, state, settings, stored_all, src_rows, verdict, cands, info):
    now, today = ctx["now"], ctx["today"]
    prev = state["live"].get(today, {"items": {}, "now": {}})
    items = dict(prev.get("items", {}))
    nowl = dict(prev.get("now", {}))
    ev = dict(stored_all)
    for k, e in cands:
        if verdict.get(k):
            items.setdefault(k, {"first_seen": iso(now)})
            items[k].update({"title": e["title"][:140], "label": e["label"], "kind": e["kind"], "score": e["_bs"], "url": e["url"],
                             "published": e.get("published"), "publishers": e["independent_publishers"] or [e["evidence"][0]["host"]],
                             "evidence": e["evidence"][:3], "stage": e.get("stage", ""), "analysis": None,
                             "analysis_note": "Explanation pending (model steps are stubs)"})
    for k, e in stored_all:
        if k in items or e["_not_breaking"]:
            continue
        if settings["live.now_threshold"] <= e["_bs"] < settings["live.breaking_threshold"] or (k in verdict and not verdict[k]):
            nowl.setdefault(k, {"first_seen": iso(now)})
            nowl[k].update({"title": e["title"][:140], "label": e["label"], "kind": e["kind"], "score": e["_bs"], "url": e["url"],
                            "published": e.get("published")})
    # keep the best 12 Now items
    nowl = dict(sorted(nowl.items(), key=lambda kv: -kv[1]["score"])[:12])
    ordered = sorted(items.items(), key=lambda kv: (-kv[1]["score"], kv[1]["first_seen"]))
    cap = settings["live.max_breaking_per_day"]
    shown = dict(ordered[:cap])
    overflow = max(0, len(ordered) - cap)
    for k, v in ordered[cap:]:
        nowl.setdefault(k, {k2: v[k2] for k2 in ("title", "label", "kind", "score", "url", "published", "first_seen")})
    nws = [{"id": k, "title": e["title"][:140], "ends": next((m.get("ends") for m in e["_members"]), None), "severity": e["_members"][0].get("severity")}
           for k, e in stored_all if e["kind"] == "nws_alert"][:10]
    polls = {r["id"]: {"at": (ctx["results"].get(r["id"]).fetched_at if ctx["results"].get(r["id"]) else None), "status": info[r["id"]]["status"],
                       "http": info[r["id"]]["http"]} for r in src_rows if r["origin"] == "spec"}
    data = {"date": today, "items": shown, "overflow_count": overflow, "now": nowl, "alerts": nws, "last_poll": polls,
            "new_slot_line": ("Updated " + now.strftime("%H:%M") + "Z, nothing new") if not shown else "New " + max(v["first_seen"] for v in shown.values())[11:16] + "Z"}
    state["live"] = {today: {"items": items, "now": nowl}}
    return data


def run(args):
    t_start = time.time()
    spec = load_spec()
    settings = Settings(spec["settings"])
    keys = load_keys(args.keys)
    scrub = Scrubber(keys)
    kind = args.run
    now = parse_dt(args.now) if args.now else utcnow()
    today = local_date(now)
    run_id = f"{kind}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    out_dir = Path(args.out) / run_id
    state_dir = Path(args.state)
    state = load_state(state_dir)
    state.setdefault("index", {})
    for k in ("first_seen", "series_first", "sources", "heartbeat", "leases", "live", "days", "versions"):
        state.setdefault(k, {})
    mode = "first"
    # ---- lease (date lease; held means exit, do not wait)
    lease_key = f"{'day' if kind == 'morning' else 'live'}/{today}"
    ls = state["leases"].get(lease_key)
    if ls and ls["status"] == "pending" and (now - parse_dt(ls["at"])).total_seconds() < 1800 and not args.replay:
        print(f"lease {lease_key} held by {ls['run_id']}; exiting without waiting")
        return 3
    if ls and ls["status"] == "done":
        mode = "merge"
    state["leases"][lease_key] = {"status": "pending", "run_id": run_id, "at": iso(now)}

    alarm = check_alarm(kind, state, now, settings)
    src_all = load_sources(spec, ENGINE_DIR / "extra_feeds.txt")
    src_rows = select_sources(kind, src_all, set(args.only.split(",")) if args.only else None)
    deadline = time.time() + float(args.budget_min or settings["fetch.budget_min"]) * 60
    record_dir = None if args.replay else (ENGINE_DIR / "cache" / run_id)
    fetcher = Fetcher(keys, scrub, replay_dir=args.replay, record_dir=record_dir, deadline=deadline, host_gap=settings["fetch.host_gap_s"])
    jobs = [{"id": r["id"], "url": r["url"]} for r in src_rows]
    print(f"[{run_id}] fetching {len(jobs)} sources on {len({host_of(j['url']) for j in jobs})} hosts", flush=True)
    results = fetch_all(fetcher, jobs, settings["fetch.workers"], deadline)
    t_fetch = time.time() - t_start
    ctx = {"now": now, "today": today, "run_id": run_id, "scrub": scrub, "spec": spec, "results": results, "next_release": {}}
    led, stored, series, ents, clocks, items = process(kind, src_rows, results, state, settings, ctx, fetcher)
    info = ctx["info"]
    writes, model_steps = [], []
    batches_plan = []
    if kind == "morning":
        pool, hero, clock_list, image_id, rid_events = finalize_morning(kind, led, stored, series, ents, clocks, items, src_rows, state, settings, ctx, fetcher)
        day_data, ok, m_total = build_day(led, ctx, stored, rid_events, hero, clock_list, image_id, settings, src_rows, series)
        asof = max([e.get("published") or "" for _, e in stored] + [""]) or today
        led.put("day", today, day_data, sorted({r["id"] for r in src_rows if info[r["id"]]["status"] == "ok"})[:400], today, iso(now), "Primary record")
        plan = retention_plan(settings)
        led.put("meta", "wm", {"last_morning_run": run_id, "lease": {"key": lease_key, "status": "done"}, "tier_line": day_data["tier_line"],
                               "source_health": {"ok": ok, "total": m_total, "extra_ok": sum(1 for r in src_rows if r["origin"] != "spec" and info[r["id"]]["status"] == "ok")},
                               "retention": {k: plan[k] for k in ("docs_year_total", "writer_cap_docs", "fits_year", "events_per_day_cap")}},
                [], today, iso(now), "Primary record")
        total, ok_n = len(src_rows), sum(1 for r in src_rows if info[r["id"]]["status"] == "ok")
        led.put("heartbeat", "morning", {"at": iso(now), "ok": ok_n, "total": total, "noteworthy": alarm}, [], iso(now), iso(now), "Primary record")
        state["heartbeat"]["morning"] = {"at": iso(now), "ok": ok_n, "total": total, "run_id": run_id}
        state["days"][today] = {"event_ids": [k for k, _ in stored]}
        top = [{"id": k, "title": e["title"], "summary": e["summary"], "url": e["url"], "publisher": pub_of(e["_members"][0]), "published": e.get("published")} for k, e in stored[:20]]
        model_steps = [model_extract_stub([{"id": t["id"], **t} for t in top]), model_explain_stub([dict(id=k, **{"evidence": e["evidence"]}) for k, e in stored[:20]]),
                       model_audit_stub([{"id": k} for k, _ in stored[:20]])]
        families_order = ["source", "series", "entity", "event", "image", "place"]
    else:
        cands = breaking_candidates(ctx["ranked_all"], settings, now)
        verdict = second_capture(cands, src_rows, fetcher, ctx, args.second_capture_wait)
        live = build_live(ctx, state, settings, ctx["ranked_all"], src_rows, verdict, cands, info)
        total, ok_n = len(src_rows), sum(1 for r in src_rows if info[r["id"]]["status"] == "ok")
        led.put("live", today, live, sorted({r["id"] for r in src_rows if info[r["id"]]["status"] == "ok"})[:400], today, iso(now), "Primary record")
        led.put("heartbeat", "hourly", {"at": iso(now), "ok": ok_n, "total": total, "noteworthy": alarm}, [], iso(now), iso(now), "Primary record")
        state["heartbeat"]["hourly"] = {"at": iso(now), "ok": ok_n, "total": total, "run_id": run_id}
        ok = ok_n
        m_total = total
        if cands:
            model_steps = [model_extract_stub([{"id": k, "title": e["title"], "summary": e["summary"]} for k, e in cands]),
                           model_explain_stub([dict(id=k, evidence=e["evidence"]) for k, e in cands]), model_audit_stub([{"id": k} for k, _ in cands])]
        families_order = []

    # ---- compose writes (set when new, update when content changed, nothing when identical)
    idx = state["index"]
    unchanged = changed = new = 0
    ordered_keys = []
    for fam in families_order:
        ordered_keys += sorted(k for k in led.docs if k[0] == COLL[fam])
    final_keys = [k for k in led.docs if k[0] in (COLL["day"], COLL["live"], COLL["meta"], COLL["heartbeat"])]
    final_order = sorted(final_keys, key=lambda k: {COLL["day"]: 0, COLL["live"]: 0, COLL["meta"]: 1, COLL["heartbeat"]: 2}[k[0]])
    docs_checked = {}
    for k in ordered_keys + final_order:
        d = shrink_doc(led.docs[k])
        docs_checked[k] = d
    mid_writes, fin_writes = [], []
    stats = {}
    for k in ordered_keys + final_order:
        d = docs_checked[k]
        ikey = f"{k[0]}/{k[1]}"
        always = k[0] in STAMP_COLLS
        prev = idx.get(ikey)
        if prev == d["content_sha256"] and not always:
            unchanged += 1
            continue
        op = "update" if prev else "set"
        if prev:
            changed += 1
        else:
            new += 1
        w = {"op": op, "collection": k[0], "doc_id": k[1], "data": d}
        (fin_writes if k in final_order else mid_writes).append(w)
        stats.setdefault(k[0], {"n": 0, "bytes": 0})
        stats[k[0]]["n"] += 1
        stats[k[0]]["bytes"] += doc_bytes(d)
    pending = []
    if kind == "morning":
        pend = {"schema_version": SCHEMA_VERSION, "id": "wm", "key": "wm/meta/wm", "run_id": run_id, "origin": [], "asof": today,
                "fetched_at": iso(now), "written_at": iso(now), "actor": "engine", "label": "Primary record",
                "lease": {"key": lease_key, "status": "pending", "run_id": run_id, "at": iso(now)}}
        pend["content_sha256"] = content_hash(pend)
        pending = [{"op": "update" if idx.get("wm_meta/wm") else "set", "collection": COLL["meta"], "doc_id": "wm", "data": pend}]
    batches = (make_batches(pending) if pending else []) + make_batches(mid_writes) + make_batches(fin_writes)

    # ---- retention check and measured projection
    measured = {c: v["bytes"] / max(v["n"], 1) for c, v in stats.items()}
    plan = retention_plan(settings, measured)
    # ---- report
    label_all, label_stored = {}, {}
    for e in ctx["events_all"].values():
        label_all[e["label"]] = label_all.get(e["label"], 0) + 1
    for _, e in stored:
        label_stored[e["label"]] = label_stored.get(e["label"], 0) + 1
    srep = []
    for r in src_rows:
        i = info[r["id"]]
        srep.append({"id": r["id"], "host": r["host"], "status": i["status"], "http": i["http"], "items": i["items"], "error": i["error"],
                     "proof": i["proof"], "origin": r["origin"]})
    by_status = {}
    for s in srep:
        by_status[s["status"]] = by_status.get(s["status"], 0) + 1
    reasons = {}
    for s in srep:
        if s["status"] != "ok":
            reasons[(s["error"] or s["status"])[:60]] = reasons.get((s["error"] or s["status"])[:60], 0) + 1
    label_all["Not fetched (sources)"] = sum(1 for s in srep if s["status"] not in ("ok", "schema_changed"))
    conf = [dict(id=k, title=e["title"][:80], publishers=e["independent_publishers"]) for k, e in ctx["ranked_all"] if e["label"] == "Confirmed"][:5]
    report = {
        "run_id": run_id, "kind": kind, "mode": mode, "started": iso(now), "duration_s": round(time.time() - t_start, 1), "fetch_s": round(t_fetch, 1),
        "alarm": alarm, "sources": srep,
        "totals": {"sources_attempted": len(srep), "sources_ok": by_status.get("ok", 0), "by_status": by_status, "failure_reasons": reasons,
                   "proof": {p: sum(1 for s in srep if s["proof"] == p) for p in ("pass", "schema", "fail", "none")},
                   "items_normalized": len(items), "events_all": len(ctx["events_all"]), "events_stored": len(stored), "stale_dropped": ctx.get("stale_dropped", 0),
                   "series": len(series), "entities": len(ents), "clocks": len(clocks),
                   "items_per_label_all": label_all, "items_per_label_stored": label_stored,
                   "docs_new": new, "docs_changed": changed, "docs_unchanged_skipped": unchanged, "writes": len(pending) + len(mid_writes) + len(fin_writes),
                   "batches": len(batches), "doc_stats": stats},
        "confirmed_sample": conf, "retention": plan, "model_steps": model_steps,
        "limits_applied": {"max_docs": MAX_DOCS, "max_doc_bytes": MAX_DOC_BYTES, "batch_max_writes": BATCH_MAX_WRITES, "batch_max_bytes": BATCH_MAX_BYTES},
    }
    if kind == "hourly":
        report["breaking"] = {"candidates": [k for k, _ in cands], "second_capture": verdict, "live_items": len(live["items"]), "now_items": len(live["now"])}
    # ---- write outputs (scrubbed), then verify no key value is present
    out_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for n, b in enumerate(batches, 1):
        files.append((f"batch_{n:03d}.json", scrub_obj(b, scrub)))
    files.append(("report.json", scrub_obj(report, scrub)))
    files.append(("model_inputs.json", scrub_obj(model_steps, scrub)))
    for name, obj in files:
        txt = json.dumps(obj, ensure_ascii=False, indent=None if name.startswith("batch") else 1)
        for v in scrub.vals:
            if v in txt:
                print("FATAL: key value found in output; aborting")
                return 5
        (out_dir / name).write_text(txt)
    # ---- commit state (a done lease and the index of what this run emitted)
    if not args.no_state_commit:
        for w in pending + mid_writes + fin_writes:
            if w["collection"] != COLL["meta"] or w["data"].get("lease", {}).get("status") != "pending":
                idx[f"{w['collection']}/{w['doc_id']}"] = w["data"]["content_sha256"]
        for k, d in docs_checked.items():
            idx[f"{k[0]}/{k[1]}"] = d["content_sha256"]
        for k, _ in stored:
            v = state["versions"].setdefault(k, {"n": 0, "sha": None})
            sha = docs_checked.get((COLL["event"], k), {}).get("content_sha256")
            if sha and sha != v["sha"]:
                v["n"], v["sha"] = v["n"] + 1, sha
        state["leases"][lease_key] = {"status": "done", "run_id": run_id, "at": iso(now)}
        save_state(state_dir, state)
    print(f"[{run_id}] done in {report['duration_s']}s: sources ok {by_status.get('ok', 0)}/{len(srep)}, events {len(stored)}/{len(ctx['events_all'])}, "
          f"writes {report['totals']['writes']} in {len(batches)} batches (new {new}, changed {changed}, unchanged {unchanged}); out={out_dir}")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Watchman data engine")
    ap.add_argument("--run", choices=["morning", "hourly"], required=True)
    ap.add_argument("--out", default=str(ENGINE_DIR / "out"))
    ap.add_argument("--state", default=str(ENGINE_DIR / "state"))
    ap.add_argument("--keys", default=None)
    ap.add_argument("--replay", default=None, help="replay captured bodies from a cache/<run_id> directory (no network)")
    ap.add_argument("--only", default=None, help="comma-separated source ids (dev)")
    ap.add_argument("--now", default=None, help="override the clock (tests)")
    ap.add_argument("--budget-min", type=float, default=None)
    ap.add_argument("--second-capture-wait", type=float, default=60)
    ap.add_argument("--no-state-commit", action="store_true")
    a = ap.parse_args(argv)
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
